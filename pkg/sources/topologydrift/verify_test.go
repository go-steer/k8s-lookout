// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package topologydrift

import (
	"errors"
	"fmt"
	"log"
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// verifyHarness is a State plus the pod cache the verifier rebuilds from, held
// separately so a test can make the two disagree — which is the only thing this
// file is about.
type verifyHarness struct {
	*harness
	cache   []*corev1.Pod
	err     error
	kinds   []leeway.SubjectKind
	logged  []string
	shards  int
	verifer *Verifier
}

func newVerifyHarness(t *testing.T, shards int) *verifyHarness {
	t.Helper()
	vh := &verifyHarness{harness: newHarness(t), shards: shards}
	vh.verifer = NewVerifier(VerifyOptions{
		State:  vh.State,
		Pods:   func() ([]*corev1.Pod, error) { return vh.cache, vh.err },
		Shards: shards,
		OnMismatch: func(k leeway.SubjectKind) {
			vh.kinds = append(vh.kinds, k)
		},
		Logf: func(format string, args ...any) {
			vh.logged = append(vh.logged, fmt.Sprintf(format, args...))
		},
	})
	return vh
}

// add feeds a pod to both sides: the incremental path and the cache the
// verifier will rebuild from. This is the honest state, and every test starts
// from it before pulling the two apart.
func (vh *verifyHarness) add(pods ...*corev1.Pod) {
	for _, p := range pods {
		vh.OnPodAdd(p)
		vh.cache = append(vh.cache, p)
	}
}

// verifyAll runs one full pass and returns the totals across every shard.
func (vh *verifyHarness) verifyAll() Report {
	var all Report
	for i := 0; i < vh.shards; i++ {
		rep := vh.verifer.VerifyShard(i)
		all.Subjects += rep.Subjects
		all.Drifted += rep.Drifted
		if rep.Err != nil {
			all.Err = rep.Err
		}
	}
	return all
}

func (vh *verifyHarness) logContains(t *testing.T, want string) bool {
	t.Helper()
	for _, line := range vh.logged {
		if strings.Contains(line, want) {
			return true
		}
	}
	return false
}

func TestVerifier_AgreesWithItselfOnAHealthyState(t *testing.T) {
	vh := newVerifyHarness(t, 4)
	vh.OnNodeUpsert(node("n1", "zone-a"))
	vh.OnNodeUpsert(node("n2", "zone-b"))
	vh.add(
		webPod("web-1", "n1"),
		webPod("web-2", "n2"),
		pod("ds-1", "kube-system", "n1", ownedBy("DaemonSet", "fluentd")),
		pod("ds-2", "kube-system", "n2", ownedBy("DaemonSet", "fluentd")),
		pod("job-1", "batch", "n1", ownedBy("Job", "nightly")),
	)
	vh.drain()

	rep := vh.verifyAll()
	if rep.Err != nil {
		t.Fatalf("verify: %v", rep.Err)
	}
	if rep.Subjects != 3 {
		t.Errorf("compared %d subjects, want 3 (web, fluentd, nightly)", rep.Subjects)
	}
	if rep.Drifted != 0 {
		t.Errorf("drifted = %d on a state built entirely through the delta rules; log:\n%s",
			rep.Drifted, strings.Join(vh.logged, "\n"))
	}
	if len(vh.kinds) != 0 {
		t.Errorf("OnMismatch fired %v on a healthy state", vh.kinds)
	}
	// A clean pass must also be a silent one: the verifier runs every five
	// minutes forever, and a line per pass is how the one that matters gets
	// scrolled past.
	if len(vh.logged) != 0 {
		t.Errorf("clean pass logged:\n%s", strings.Join(vh.logged, "\n"))
	}
	if q := vh.drain(); len(q) != 0 {
		t.Errorf("clean pass enqueued %v; nothing changed, so nothing should re-evaluate", q)
	}
}

// TestVerifier_RepairsAMissedUpdate is the headline case: the pod moved and the
// delta rules never heard about it.
func TestVerifier_RepairsAMissedUpdate(t *testing.T) {
	vh := newVerifyHarness(t, 1)
	vh.OnNodeUpsert(node("n1", "zone-a"))
	vh.OnNodeUpsert(node("n2", "zone-b"))
	vh.add(webPod("web-1", "n1"))

	// The cache now holds the pod on n2; the incremental path was never told.
	// Same UID, so this is one pod that moved, not two pods.
	vh.cache = []*corev1.Pod{webPod("web-1", "n2")}
	vh.drain()

	rep := vh.verifyAll()
	if rep.Drifted != 1 {
		t.Fatalf("drifted = %d, want 1; log:\n%s", rep.Drifted, strings.Join(vh.logged, "\n"))
	}
	if got := vh.totals(webSubject); !maps.Equal(got, map[leeway.Domain]int64{"zone-b": 1}) {
		t.Errorf("after repair totals = %v, want zone-b:1", got)
	}
	if !slices.Equal(vh.kinds, []leeway.SubjectKind{leeway.SubjectDeployment}) {
		t.Errorf("OnMismatch kinds = %v, want [Deployment]", vh.kinds)
	}
	if !vh.logContains(t, "COUNTER DRIFT on Deployment/prod/web") {
		t.Errorf("no drift line naming the subject:\n%s", strings.Join(vh.logged, "\n"))
	}
	// A moved pod has the same total on both sides. The line has to name the
	// domains or it reports the commonest bug here as a difference of nothing.
	if !vh.logContains(t, "zone-a 1→0, zone-b 0→1") {
		t.Errorf("drift line does not say which domains moved:\n%s", strings.Join(vh.logged, "\n"))
	}
	// The repair changed the distribution, so whatever reads it has to be told.
	if q := vh.drain(); !slices.Contains(q, webSubject) {
		t.Errorf("repair enqueued %v, want the repaired subject", q)
	}
}

// TestVerifier_RepairReseatsThePlacementIndex is the reason Repair rebuilds the
// pod indexes and not only the counts.
//
// Overwriting counts alone passes the very next verification and then
// re-corrupts on the first real event: placements is what a delta decrements
// FROM, so a pod left at its stale placement subtracts from a domain it is no
// longer counted in — leaving the old domain's count stranded and the subject
// with one more replica than it has.
func TestVerifier_RepairReseatsThePlacementIndex(t *testing.T) {
	vh := newVerifyHarness(t, 1)
	vh.OnNodeUpsert(node("n1", "zone-a"))
	vh.OnNodeUpsert(node("n2", "zone-b"))
	vh.OnNodeUpsert(node("n3", "zone-c"))
	vh.add(webPod("web-1", "n1"))

	vh.cache = []*corev1.Pod{webPod("web-1", "n2")}
	if rep := vh.verifyAll(); rep.Drifted != 1 {
		t.Fatalf("drifted = %d, want 1", rep.Drifted)
	}

	// Now a perfectly ordinary move, delivered the ordinary way.
	vh.OnPodUpdate(nil, webPod("web-1", "n3"))

	got := vh.totals(webSubject)
	want := map[leeway.Domain]int64{"zone-c": 1}
	if !maps.Equal(got, want) {
		t.Errorf("after repair-then-move totals = %v, want %v — the repair left a stale placement, so the move decremented the wrong domain", got, want)
	}
}

// TestVerifier_RepairReseatsTheNodeIndex is the same argument for byNode: a pod
// filed under its old node is invisible to that node's relabel.
func TestVerifier_RepairReseatsTheNodeIndex(t *testing.T) {
	vh := newVerifyHarness(t, 1)
	vh.OnNodeUpsert(node("n1", "zone-a"))
	vh.OnNodeUpsert(node("n2", "zone-b"))
	vh.add(webPod("web-1", "n1"))

	vh.cache = []*corev1.Pod{webPod("web-1", "n2")}
	if rep := vh.verifyAll(); rep.Drifted != 1 {
		t.Fatalf("drifted = %d, want 1", rep.Drifted)
	}

	vh.OnNodeUpsert(node("n2", "zone-b2"))

	got := vh.totals(webSubject)
	want := map[leeway.Domain]int64{"zone-b2": 1}
	if !maps.Equal(got, want) {
		t.Errorf("after repair-then-relabel totals = %v, want %v — the repaired pod was still filed under its old node", got, want)
	}
}

// TestVerifier_DropsALeakedPod covers the failure the counters cannot recover
// from on their own: a delete that never arrived.
func TestVerifier_DropsALeakedPod(t *testing.T) {
	vh := newVerifyHarness(t, 1)
	vh.OnNodeUpsert(node("n1", "zone-a"))
	vh.add(webPod("web-1", "n1"), webPod("web-2", "n1"))

	// web-2 is gone from the cluster; its delete event was never delivered.
	vh.cache = []*corev1.Pod{webPod("web-1", "n1")}
	vh.drain()

	rep := vh.verifyAll()
	if rep.Drifted != 1 {
		t.Fatalf("drifted = %d, want 1; log:\n%s", rep.Drifted, strings.Join(vh.logged, "\n"))
	}
	if got := vh.totals(webSubject); !maps.Equal(got, map[leeway.Domain]int64{"zone-a": 1}) {
		t.Errorf("after repair totals = %v, want zone-a:1", got)
	}
	if got := vh.Len(); got != 1 {
		t.Errorf("Len = %d, want 1: the leaked pod must leave the placement index too", got)
	}
}

// TestVerifier_ForgetsASubjectWithNoPodsLeft is the whole-subject version of the
// leak. It only works because VerifyShard unions the rebuild with the subjects
// the state already holds — a subject that has vanished from the cluster
// appears in the rebuild as nothing at all.
func TestVerifier_ForgetsASubjectWithNoPodsLeft(t *testing.T) {
	vh := newVerifyHarness(t, 1)
	vh.OnNodeUpsert(node("n1", "zone-a"))
	vh.add(webPod("web-1", "n1"))

	vh.cache = nil
	rep := vh.verifyAll()
	if rep.Subjects != 1 || rep.Drifted != 1 {
		t.Fatalf("subjects=%d drifted=%d, want 1/1", rep.Subjects, rep.Drifted)
	}
	if snap := vh.Snapshot(webSubject); snap != nil {
		t.Errorf("subject still tracked after every pod left: %v", snap)
	}
	if got := vh.Len(); got != 0 {
		t.Errorf("Len = %d, want 0", got)
	}
	// And the second pass has nothing to say.
	vh.logged = nil
	if rep := vh.verifyAll(); rep.Subjects != 0 || rep.Drifted != 0 {
		t.Errorf("second pass subjects=%d drifted=%d, want 0/0", rep.Subjects, rep.Drifted)
	}
}

// TestVerifier_UsesTheCachedSubject pins the deliberate decision in
// RebuildShard. A pod whose ReplicaSet has left the owner cache is supposed to
// keep counting under the subject it was first assigned — re-resolving would
// fail to find one and report correct behaviour as drift, on an SLI whose whole
// value is that any non-zero rate means a bug.
func TestVerifier_UsesTheCachedSubject(t *testing.T) {
	vh := newVerifyHarness(t, 1)
	vh.OnNodeUpsert(node("n1", "zone-a"))
	vh.add(webPod("web-1", "n1"))

	// The Deployment is being deleted: its ReplicaSet is already out of cache,
	// so a fresh resolution of this pod would yield no subject at all.
	vh.owners = staticOwners(nil)

	rep := vh.verifyAll()
	if rep.Drifted != 0 {
		t.Errorf("drifted = %d after the owner left the cache, want 0; log:\n%s",
			rep.Drifted, strings.Join(vh.logged, "\n"))
	}
	if got := vh.totals(webSubject); !maps.Equal(got, map[leeway.Domain]int64{"zone-a": 1}) {
		t.Errorf("totals = %v, want zone-a:1 — the cached subject must survive", got)
	}
}

// TestVerifier_PodCacheErrorRepairsNothing. An unreadable cache means the pass
// learned nothing, and a verifier that treated "no pods" as "no pods exist"
// would repair the entire cluster's counters to zero.
func TestVerifier_PodCacheErrorRepairsNothing(t *testing.T) {
	vh := newVerifyHarness(t, 1)
	vh.OnNodeUpsert(node("n1", "zone-a"))
	vh.add(webPod("web-1", "n1"))

	boom := errors.New("lister exploded")
	vh.err = boom

	rep := vh.verifyAll()
	if !errors.Is(rep.Err, boom) {
		t.Errorf("Err = %v, want %v", rep.Err, boom)
	}
	if rep.Drifted != 0 || len(vh.kinds) != 0 {
		t.Errorf("drifted=%d kinds=%v, want nothing reported on an unread cache", rep.Drifted, vh.kinds)
	}
	if got := vh.totals(webSubject); !maps.Equal(got, map[leeway.Domain]int64{"zone-a": 1}) {
		t.Errorf("totals = %v, want zone-a:1 untouched", got)
	}
	if !vh.logContains(t, "counters unverified this pass") {
		t.Errorf("no line saying the pass was skipped:\n%s", strings.Join(vh.logged, "\n"))
	}
}

// TestVerifier_TickWalksEveryShardExactlyOnce is what makes the one-hour figure
// in DefaultVerifyInterval's doc true.
func TestVerifier_TickWalksEveryShardExactlyOnce(t *testing.T) {
	vh := newVerifyHarness(t, 5)
	vh.OnNodeUpsert(node("n1", "zone-a"))
	for i := 0; i < 40; i++ {
		vh.add(pod(fmt.Sprintf("ds-%d", i), "kube-system", "n1",
			ownedBy("DaemonSet", fmt.Sprintf("agent-%d", i))))
	}

	seen := map[int]int{}
	total := 0
	for i := 0; i < vh.shards; i++ {
		rep := vh.verifer.Tick()
		seen[rep.Shard]++
		total += rep.Subjects
		if rep.Drifted != 0 {
			t.Fatalf("shard %d drifted: %s", rep.Shard, strings.Join(vh.logged, "\n"))
		}
	}
	if len(seen) != vh.shards {
		t.Errorf("one full cycle visited shards %v, want all %d exactly once", seen, vh.shards)
	}
	if total != 40 {
		t.Errorf("a full cycle compared %d subjects, want all 40 — a subject in no shard is never checked", total)
	}
	// And it wraps rather than running off the end.
	if got := vh.verifer.Tick().Shard; got != 0 {
		t.Errorf("shard after a full cycle = %d, want 0", got)
	}
}

// TestShardOf_IsStableUnderChurn. Hashing the subject rather than slicing a
// sorted list is what stops a subject from repeatedly landing just behind a
// moving boundary and going years without a check.
func TestShardOf_IsStableUnderChurn(t *testing.T) {
	sub := leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "web"}
	want := shardOf(sub, 12)
	for i := 0; i < 1000; i++ {
		other := leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: fmt.Sprintf("svc-%d", i)}
		_ = shardOf(other, 12)
		if got := shardOf(sub, 12); got != want {
			t.Fatalf("shard moved to %d after %d other subjects, want a stable %d", got, i, want)
		}
	}
}

func TestShardOf_SpreadsAndStaysInRange(t *testing.T) {
	const shards = 12
	hist := make([]int, shards)
	for i := 0; i < 2000; i++ {
		s := shardOf(leeway.SubjectRef{
			Kind:      leeway.SubjectDeployment,
			Namespace: fmt.Sprintf("ns-%d", i%37),
			Name:      fmt.Sprintf("app-%d", i),
		}, shards)
		if s < 0 || s >= shards {
			t.Fatalf("shard %d out of range [0,%d)", s, shards)
		}
		hist[s]++
	}
	// Not a uniformity proof — just enough to catch a hash that collapses, which
	// would silently leave most of the fleet unverified.
	for i, n := range hist {
		if n == 0 {
			t.Errorf("shard %d got nothing of 2000 subjects: %v", i, hist)
		}
	}
}

func TestShardOf_SingleShardTakesEverything(t *testing.T) {
	for _, shards := range []int{0, 1} {
		if got := shardOf(webSubject, shards); got != 0 {
			t.Errorf("shardOf(_, %d) = %d, want 0", shards, got)
		}
	}
}

func TestVerifyOptions_Defaults(t *testing.T) {
	v := NewVerifier(VerifyOptions{})
	if v.Interval() != DefaultVerifyInterval {
		t.Errorf("Interval = %v, want %v", v.Interval(), DefaultVerifyInterval)
	}
	if v.Shards() != DefaultVerifyShards {
		t.Errorf("Shards = %d, want %d", v.Shards(), DefaultVerifyShards)
	}
}

func TestSameDistributions(t *testing.T) {
	full := func(dom leeway.Domain, n int) map[leeway.TopologyKey]*leeway.Distribution {
		d := leeway.NewDistribution()
		for i := 0; i < n; i++ {
			d.Add(dom, leeway.StateRunning, false)
		}
		return map[leeway.TopologyKey]*leeway.Distribution{zoneKey: d}
	}
	tests := []struct {
		name string
		a, b map[leeway.TopologyKey]*leeway.Distribution
		want bool
	}{
		{"both empty", nil, nil, true},
		{"same", full("zone-a", 2), full("zone-a", 2), true},
		{"different count", full("zone-a", 2), full("zone-a", 3), false},
		{"different domain", full("zone-a", 2), full("zone-b", 2), false},
		{"axis only on the left", full("zone-a", 1), nil, false},
		{"axis only on the right", nil, full("zone-a", 1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sameDistributions(tt.a, tt.b); got != tt.want {
				t.Errorf("sameDistributions = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestDescribeDrift_IsOrderedAndNamesBothSides. The log line is the only place a
// reader learns WHAT drifted, and an unordered one cannot be diffed between two
// occurrences of the same bug.
func TestDescribeDrift_IsOrderedAndNamesBothSides(t *testing.T) {
	dist := func(n int) *leeway.Distribution {
		d := leeway.NewDistribution()
		for i := 0; i < n; i++ {
			d.Add("zone-a", leeway.StateRunning, false)
		}
		return d
	}
	want := map[leeway.TopologyKey]*leeway.Distribution{"b.example/rack": dist(3), zoneKey: dist(2)}
	got := map[leeway.TopologyKey]*leeway.Distribution{zoneKey: dist(1)}

	line := describeDrift(want, got)
	expect := "b.example/rack: zone-a 0→3; topology.kubernetes.io/zone: zone-a 1→2"
	if line != expect {
		t.Errorf("describeDrift =\n %q\nwant\n %q", line, expect)
	}
	if got := describeDrift(nil, nil); got != "no per-domain difference" {
		t.Errorf("describeDrift(nil, nil) = %q", got)
	}
}

// TestDescribeDrift_ElidesAWideDrift. A systematic bug drifts every domain of
// every subject at once, and the log reporting it has to stay readable then.
func TestDescribeDrift_ElidesAWideDrift(t *testing.T) {
	d := leeway.NewDistribution()
	for i := 0; i < maxDriftDomains+3; i++ {
		d.Add(leeway.Domain(fmt.Sprintf("zone-%02d", i)), leeway.StateRunning, false)
	}
	line := describeDrift(map[leeway.TopologyKey]*leeway.Distribution{zoneKey: d}, nil)

	if n := strings.Count(line, "→"); n != maxDriftDomains {
		t.Errorf("line names %d domains, want %d:\n %s", n, maxDriftDomains, line)
	}
	if !strings.Contains(line, "and 3 more domain(s)") {
		t.Errorf("elision not reported:\n %s", line)
	}
}

// TestDescribeAxis_SilentWhenTheAxisAgrees keeps an axis that is fine out of a
// line about an axis that is not.
func TestDescribeAxis_SilentWhenTheAxisAgrees(t *testing.T) {
	d := leeway.NewDistribution()
	d.Add("zone-a", leeway.StateRunning, false)
	other := d.Clone()
	if got := describeAxis(d, other); got != "" {
		t.Errorf("describeAxis on identical distributions = %q, want empty", got)
	}
}

// TestVerifier_RepairsDroppedEventsAtScale is the §6.5 half of Phase 2's exit
// criterion, and the counterpart to TestState_CountsMatchARecountAfterChurn.
//
// That test proves the delta rules are right when every event is delivered.
// This one assumes they are not: the same churn runs with one event in twelve
// silently discarded — which is every failure mode this component exists for,
// a missed add, a missed move, a missed delete — and then asks whether one full
// verification pass puts the counters back to what a recount says.
//
// It also asserts the pass is *idempotent*, and that the repaired state still
// takes deltas. A repair that rewrote the counts but left the pod indexes stale
// would pass a second verification and then re-corrupt on the first real event;
// the relabel at the end is what catches that.
func TestVerifier_RepairsDroppedEventsAtScale(t *testing.T) {
	const (
		pods     = 10_000
		nodes    = 200
		subjects = 40
		events   = 60_000
		shards   = 12
		dropOne  = 12 // 1 event in 12 never reaches the delta rules
	)

	inv := NewInventory([]leeway.TopologyKey{zoneKey, poolKey})
	owners := map[string][2]string{}
	for i := range subjects {
		owners[fmt.Sprintf("prod/ReplicaSet/rs-%d", i)] = [2]string{"Deployment", fmt.Sprintf("dep-%d", i)}
	}
	lookup := staticOwners(owners)
	state := NewState(StateOptions{Inventory: inv, Owners: lookup})

	for i := range nodes {
		state.OnNodeUpsert(node(fmt.Sprintf("n%03d", i), fmt.Sprintf("zone-%d", i%4),
			withLabel(string(poolKey), fmt.Sprintf("pool-%d", i%3))))
	}

	live := map[types.UID]*corev1.Pod{}
	rng := rand.New(rand.NewPCG(7, 11)) //nolint:gosec // deterministic by design

	makePod := func(i int) *corev1.Pod {
		p := pod(fmt.Sprintf("pod-%04d", i), "prod",
			fmt.Sprintf("n%03d", rng.IntN(nodes)),
			ownedBy("ReplicaSet", fmt.Sprintf("rs-%d", i%subjects)))
		switch rng.IntN(10) {
		case 0:
			phase(corev1.PodPending)(p)
		case 1:
			unschedulablePod()(p)
		case 2:
			terminating()(p)
		}
		return p
	}

	dropped := 0
	for step := range events {
		// The cluster's truth always moves; the delta rules only sometimes hear
		// about it. `deliver` is the whole fault injection.
		deliver := step%dropOne != 0
		if !deliver {
			dropped++
		}
		switch rng.IntN(10) {
		case 0, 1, 2, 3, 4, 5:
			p := makePod(rng.IntN(pods))
			live[p.UID] = p
			if deliver {
				state.OnPodUpdate(nil, p)
			}
		case 6:
			p := makePod(rng.IntN(pods))
			phase(corev1.PodSucceeded)(p)
			live[p.UID] = p
			if deliver {
				state.OnPodUpdate(nil, p)
			}
		case 7, 8:
			p := makePod(rng.IntN(pods))
			delete(live, p.UID)
			if deliver {
				state.OnPodDelete(p)
			}
		default:
			// Node events are always delivered: a relabel the source never saw
			// is a different bug, and the inventory is the shared input both
			// sides of this comparison read.
			state.OnNodeUpsert(node(fmt.Sprintf("n%03d", rng.IntN(nodes)),
				fmt.Sprintf("zone-%d", rng.IntN(5)),
				withLabel(string(poolKey), fmt.Sprintf("pool-%d", rng.IntN(4)))))
		}
	}
	if dropped == 0 {
		t.Fatal("no events were dropped; this test proves nothing")
	}

	cache := slices.Collect(maps.Values(live))
	v := NewVerifier(VerifyOptions{
		State:  state,
		Pods:   func() ([]*corev1.Pod, error) { return cache, nil },
		Shards: shards,
		Logf:   func(string, ...any) {},
	})

	drifted := 0
	for i := range shards {
		drifted += v.VerifyShard(i).Drifted
	}
	if drifted == 0 {
		t.Fatalf("%d dropped events produced no detectable drift; the verifier is not checking what it claims to", dropped)
	}
	t.Logf("%d dropped events -> %d drifted subject(s) repaired", dropped, drifted)

	want := recountPods(inv, live, lookup)
	got := map[leeway.SubjectRef]map[leeway.TopologyKey]*leeway.Distribution{}
	for _, sub := range state.Subjects() {
		got[sub] = state.Snapshot(sub)
	}
	if len(got) != len(want) {
		t.Fatalf("after repair %d subjects counted, recount says %d", len(got), len(want))
	}
	for sub, wantByKey := range want {
		gotByKey, ok := got[sub]
		if !ok {
			t.Fatalf("%s: missing after repair", sub)
		}
		for key, wantDist := range wantByKey {
			if !wantDist.Equal(gotByKey[key]) {
				t.Errorf("%s on %s:\n  repaired %s\n  recount  %s",
					sub, key, render(gotByKey[key]), render(wantDist))
			}
		}
	}

	for i := range shards {
		if rep := v.VerifyShard(i); rep.Drifted != 0 {
			t.Errorf("second pass over shard %d found %d drifted subject(s); the repair did not stick", i, rep.Drifted)
		}
	}

	// A relabel moves every pod on one node through the delta path, from the
	// placements and byNode entries the repair re-seated.
	state.OnNodeUpsert(node("n000", "zone-relabelled",
		withLabel(string(poolKey), "pool-relabelled")))
	for i := range shards {
		if rep := v.VerifyShard(i); rep.Drifted != 0 {
			t.Errorf("after a post-repair relabel, shard %d found %d drifted subject(s); Repair left a stale index",
				i, rep.Drifted)
		}
	}
}

// TestVerifier_LoggerDefaultsToTheStandardLog. The drift line is the only thing
// that turns the SLI into an actionable report, so a nil Logf must not silently
// mean "discard".
func TestVerifier_LoggerDefaultsToTheStandardLog(t *testing.T) {
	v := NewVerifier(VerifyOptions{})
	if got := reflect.ValueOf(v.logger()).Pointer(); got != reflect.ValueOf(log.Printf).Pointer() {
		t.Error("logger() with no Logf is not log.Printf")
	}
}

// TestRebuildShard_SkipsNilPods. A lister never returns one, but a rebuild that
// panicked would take down the sentinel to report a counting bug.
func TestRebuildShard_SkipsNilPods(t *testing.T) {
	vh := newVerifyHarness(t, 1)
	vh.OnNodeUpsert(node("n1", "zone-a"))
	vh.add(webPod("web-1", "n1"))

	got := vh.RebuildShard([]*corev1.Pod{nil, webPod("web-1", "n1"), nil}, 0, 1)
	if len(got) != 1 {
		t.Fatalf("rebuilt %d subjects, want 1", len(got))
	}
	if n := len(got[webSubject].Pods); n != 1 {
		t.Errorf("rebuilt from %d pods, want 1", n)
	}
}

// TestRepair_SkipsUncountablePods exercises a guard no verification pass can
// reach — RebuildShard only ever hands Repair pods that resolvePlacement
// accepted. It exists so that a future caller cannot make a completed pod
// occupy a domain, and testing it directly is the only way to know it works.
func TestRepair_SkipsUncountablePods(t *testing.T) {
	vh := newVerifyHarness(t, 1)
	vh.OnNodeUpsert(node("n1", "zone-a"))
	vh.add(webPod("web-1", "n1"))

	vh.Repair(webSubject, []*corev1.Pod{
		webPod("web-1", "n1"),
		webPod("web-2", "n1", phase(corev1.PodSucceeded)),
	})

	if got := vh.totals(webSubject); !maps.Equal(got, map[leeway.Domain]int64{"zone-a": 1}) {
		t.Errorf("totals = %v, want zone-a:1 — a completed pod occupies no domain", got)
	}
	if got := vh.Len(); got != 1 {
		t.Errorf("Len = %d, want 1", got)
	}
}
