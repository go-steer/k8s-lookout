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
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// regionKey is the second axis, for the per-axis suppression test.
const regionKey = leeway.TopologyKey(corev1.LabelTopologyRegion)

// cordonedAtTaint sets the unschedulable taint with the API server's own
// TimeAdded, which is what a real `kubectl cordon` writes.
func cordonedAtTaint(at time.Time) nodeOpt {
	return func(n *corev1.Node) {
		n.Spec.Unschedulable = true
		stamp := metav1.NewTime(at)
		n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{
			Key:       corev1.TaintNodeUnschedulable,
			Effect:    corev1.TaintEffectNoSchedule,
			TimeAdded: &stamp,
		})
	}
}

// evenlyEligible is an eligible set of equal domains, which is every shape
// these tests want and none of them are about.
func evenlyEligible(domains ...leeway.Domain) leeway.Eligibility {
	e := leeway.Eligibility{Domains: domains}
	for range domains {
		e.NodeCount = append(e.NodeCount, 4)
		e.Capacity = append(e.Capacity, 4)
	}
	return e
}

// fixedInventory is an inventory whose clock is under the test's control.
func fixedInventory(now *time.Time, keys ...leeway.TopologyKey) *Inventory {
	inv := NewInventory(keys)
	inv.now = func() time.Time { return *now }
	return inv
}

func TestInventory_ReadyCountsNotReadyNodesSeparatelyFromCordonedOnes(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("n-1", "us-central1-a"))
	inv.Upsert(node("n-2", "us-central1-a", cordoned()))
	inv.Upsert(node("n-3", "us-central1-a", notReady()))
	inv.Upsert(node("n-4", "us-central1-a", cordoned(), notReady()))

	got := inv.Stats(zoneKey)["us-central1-a"]
	// Four nodes; two Ready (n-1, n-2); one usable (n-1 alone, since a cordon
	// costs usability and readiness costs both).
	if got.Nodes != 4 || got.Ready != 2 || got.Usable != 1 {
		t.Fatalf("stats = %+v, want Nodes 4, Ready 2, Usable 1", got)
	}
}

// The distinction Ready exists for: a drain and an outage look identical
// through Usable and must not be judged identically.
func TestInventory_ACordonDoesNotLookLikeAnOutage(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	for _, n := range []string{"n-1", "n-2", "n-3", "n-4"} {
		inv.Upsert(node(n, "us-central1-a"))
	}
	inv.SampleReady(t0, 0)
	for _, n := range []string{"n-1", "n-2", "n-3"} {
		inv.Upsert(node(n, "us-central1-a", cordoned()))
	}
	inv.SampleReady(t0.Add(time.Minute), 0)

	st := inv.Stats(zoneKey)["us-central1-a"]
	if st.Usable != 1 {
		t.Fatalf("Usable = %d, want 1: three of four nodes are cordoned", st.Usable)
	}
	c := leeway.DefaultTransientConfig()
	if c.DomainOutage(inv.ReadyHistory(zoneKey, "us-central1-a"), st.Ready, t0.Add(time.Minute)) {
		t.Error("a cordoned zone read as an outage; nothing went NotReady")
	}
}

func TestInventory_SampleReadyCarriesThePeakIntoTheWindow(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	for _, n := range []string{"n-1", "n-2", "n-3", "n-4", "n-5", "n-6"} {
		inv.Upsert(node(n, "us-central1-a"))
	}
	// Ten minutes of a healthy zone, sampled every minute.
	for i := range 10 {
		inv.SampleReady(t0.Add(time.Duration(i)*time.Minute), 0)
	}
	// Then it dies, all at once — the case an append-on-change log cannot see.
	for _, n := range []string{"n-1", "n-2", "n-3", "n-4", "n-5", "n-6"} {
		inv.Upsert(node(n, "us-central1-a", notReady()))
	}
	now := t0.Add(10 * time.Minute)
	inv.SampleReady(now, 0)

	st := inv.Stats(zoneKey)["us-central1-a"]
	if st.Ready != 0 {
		t.Fatalf("Ready = %d, want 0", st.Ready)
	}
	if !leeway.DefaultTransientConfig().DomainOutage(inv.ReadyHistory(zoneKey, "us-central1-a"), st.Ready, now) {
		t.Error("a zone that lost every ready node did not read as an outage")
	}
}

func TestInventory_SampleReadyForgetsWhatTheWindowNoLongerCovers(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("n-1", "us-central1-a"))
	for i := range 20 {
		inv.SampleReady(t0.Add(time.Duration(i)*time.Minute), 5*time.Minute)
	}

	h := inv.ReadyHistory(zoneKey, "us-central1-a")
	if len(h) == 0 || len(h) > 6 {
		t.Fatalf("history holds %d samples, want at most the six a five-minute retention covers", len(h))
	}
	if oldest := h[0].At; oldest.Before(t0.Add(14 * time.Minute)) {
		t.Errorf("oldest sample is %v, want nothing before %v", oldest, t0.Add(14*time.Minute))
	}
	// The newest sample is always kept, whatever the retention.
	if newest := h[len(h)-1].At; !newest.Equal(t0.Add(19 * time.Minute)) {
		t.Errorf("newest sample is %v, want the one just taken", newest)
	}
}

// A domain that loses its last node stops being sampled, so its series has to
// be dropped by age rather than by the next sample — but not before it has
// aged out, because a zone that emptied a minute ago is the interesting one.
func TestInventory_AVanishedDomainKeepsItsHistoryUntilItAgesOut(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("n-1", "us-central1-a"))
	inv.Upsert(node("n-2", "us-central1-b"))
	inv.SampleReady(t0, 5*time.Minute)

	inv.Remove("n-1")
	inv.SampleReady(t0.Add(time.Minute), 5*time.Minute)
	if got := inv.ReadyHistory(zoneKey, "us-central1-a"); len(got) != 1 {
		t.Fatalf("history for the emptied zone = %d samples, want the one taken while it lived", len(got))
	}

	inv.SampleReady(t0.Add(10*time.Minute), 5*time.Minute)
	if got := inv.ReadyHistory(zoneKey, "us-central1-a"); got != nil {
		t.Errorf("history for the emptied zone = %v, want it forgotten once it aged out", got)
	}
	if got := inv.ReadyHistory(zoneKey, "us-central1-b"); len(got) == 0 {
		t.Error("the surviving zone lost its history too")
	}
}

func TestInventory_ReadyHistoryAndLastDrainRefuseAnUnknownAxis(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("n-1", "us-central1-a", cordoned()))
	inv.SampleReady(t0, 0)

	if got := inv.ReadyHistory("nope/key", "us-central1-a"); got != nil {
		t.Errorf("ReadyHistory on an untracked axis = %v, want nil", got)
	}
	if got := inv.LastDrainIn("nope/key", []leeway.Domain{"us-central1-a"}); !got.IsZero() {
		t.Errorf("LastDrainIn on an untracked axis = %v, want the zero time", got)
	}
	if got := inv.LastDrainIn(zoneKey, nil); !got.IsZero() {
		t.Errorf("LastDrainIn over no domains = %v, want the zero time", got)
	}
}

func TestInventory_LastDrainInDatesACordonFromTheTaint(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)
	cordonedAt := t0.Add(-3 * time.Minute)
	inv.Upsert(node("n-1", "us-central1-a", cordonedAtTaint(cordonedAt)))
	inv.Upsert(node("n-2", "us-central1-b"))

	if got := inv.LastDrainIn(zoneKey, []leeway.Domain{"us-central1-a"}); !got.Equal(cordonedAt) {
		t.Errorf("LastDrainIn = %v, want the taint's TimeAdded %v", got, cordonedAt)
	}
	// Scoped by domain: a drain somewhere this subject cannot reach is not its
	// drain.
	if got := inv.LastDrainIn(zoneKey, []leeway.Domain{"us-central1-b"}); !got.IsZero() {
		t.Errorf("LastDrainIn for an untouched zone = %v, want the zero time", got)
	}
}

func TestInventory_AnUndatedCordonIsDatedFromTheFlipWeWatched(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)
	inv.Upsert(node("n-1", "us-central1-a"))

	now = t0.Add(time.Minute)
	inv.Upsert(node("n-1", "us-central1-a", cordoned()))
	if got := inv.LastDrainIn(zoneKey, []leeway.Domain{"us-central1-a"}); !got.Equal(now) {
		t.Fatalf("LastDrainIn = %v, want the moment of the flip %v", got, now)
	}

	// Still cordoned five minutes later: the clock started when it went, not
	// at every subsequent status update.
	flip := now
	now = t0.Add(6 * time.Minute)
	inv.Upsert(node("n-1", "us-central1-a", cordoned(), withLabel("rev", "2")))
	if got := inv.LastDrainIn(zoneKey, []leeway.Domain{"us-central1-a"}); !got.Equal(flip) {
		t.Errorf("LastDrainIn = %v, want it still reading the original flip %v", got, flip)
	}

	// An uncordon does not un-drain: the pods the drain evicted are still
	// landing.
	now = t0.Add(7 * time.Minute)
	inv.Upsert(node("n-1", "us-central1-a"))
	if got := inv.LastDrainIn(zoneKey, []leeway.Domain{"us-central1-a"}); !got.Equal(flip) {
		t.Errorf("LastDrainIn after an uncordon = %v, want the drain still dated %v", got, flip)
	}
}

// A node first seen already cordoned gets the zero time. Guessing "now" would
// relax every subject in the zone for a whole drain window after each restart,
// which is a suppression that switches itself on at startup.
func TestInventory_ANodeFirstSeenCordonedIsNotDatedNow(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)
	inv.Upsert(node("n-1", "us-central1-a", cordoned()))

	if got := inv.LastDrainIn(zoneKey, []leeway.Domain{"us-central1-a"}); !got.IsZero() {
		t.Errorf("LastDrainIn = %v, want the zero time for an undatable cordon", got)
	}
	// It is still not dated when the same undated object comes round again.
	inv.Upsert(node("n-1", "us-central1-a", cordoned(), withLabel("rev", "2")))
	if got := inv.LastDrainIn(zoneKey, []leeway.Domain{"us-central1-a"}); !got.IsZero() {
		t.Errorf("LastDrainIn = %v, want it still undated", got)
	}
	// A taint appearing later does date it.
	inv.Upsert(node("n-1", "us-central1-a", cordonedAtTaint(t0.Add(-time.Hour))))
	if got := inv.LastDrainIn(zoneKey, []leeway.Domain{"us-central1-a"}); !got.Equal(t0.Add(-time.Hour)) {
		t.Errorf("LastDrainIn = %v, want the taint's answer once one arrived", got)
	}
}

// LastDrainIn reports the most recent cordon across the domains, not the first
// one it happens to walk past. Map iteration order makes the difference
// invisible in a one-node test.
func TestInventory_LastDrainInTakesTheMostRecentCordon(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)
	inv.Upsert(node("n-1", "us-central1-a", cordonedAtTaint(t0.Add(-20*time.Minute))))
	inv.Upsert(node("n-2", "us-central1-a", cordonedAtTaint(t0.Add(-2*time.Minute))))
	inv.Upsert(node("n-3", "us-central1-b", cordonedAtTaint(t0.Add(-9*time.Minute))))

	want := t0.Add(-2 * time.Minute)
	got := inv.LastDrainIn(zoneKey, []leeway.Domain{"us-central1-a", "us-central1-b"})
	if !got.Equal(want) {
		t.Errorf("LastDrainIn = %v, want the most recent cordon %v", got, want)
	}
}

func TestConfig_ReadyRetentionIsTwiceTheOutageWindow(t *testing.T) {
	if got := (Config{}).readyRetention(); got != 30*time.Minute {
		t.Errorf("readyRetention() = %v, want twice the default 15m window", got)
	}
	if got := (Config{Transient: leeway.TransientConfig{OutageWindow: time.Minute}}).readyRetention(); got != 2*time.Minute {
		t.Errorf("readyRetention() = %v, want 2m", got)
	}
}

func TestScoreAxis_ASuppressionIsCarriedAndAppliedToTheVerdict(t *testing.T) {
	e := evenlyEligible("us-central1-a", "us-central1-b", "us-central1-c")
	sup := leeway.Suppression{State: leeway.TransientDomainOutage, Suppress: true, Reason: "a zone is out"}

	got := ScoreAxis(zoneKey, nil, e, running(e, 8, 0, 0), leeway.DefaultThresholds(), sup)
	if got.Suppression != sup {
		t.Errorf("Suppression = %+v, want it carried through", got.Suppression)
	}
	if got.Verdict.Breached {
		t.Errorf("verdict = %+v, want the breach suppressed", got.Verdict)
	}
	if !got.Verdict.Suppressed || got.Verdict.Transient != leeway.TransientDomainOutage {
		t.Errorf("verdict = %+v, want it to say why", got.Verdict)
	}
	// The scores themselves are measured regardless: they are the evidence the
	// suppression was right.
	if got.Scores.Drift == 0 {
		t.Error("drift scored zero under a suppression; the measurement is not suppressed, the judgement is")
	}
}

func TestScoreSubject_ASuppressorIsAskedPerAxisAndNilMeansNoTransient(t *testing.T) {
	res := Resolution{
		Eligible: map[leeway.TopologyKey]leeway.Eligibility{
			zoneKey:   evenlyEligible("us-central1-a", "us-central1-b"),
			regionKey: evenlyEligible("us-central1"),
		},
	}
	var asked []leeway.TopologyKey
	got := ScoreSubject(res, nil, leeway.DefaultThresholds(), func(key leeway.TopologyKey, _ leeway.Eligibility) leeway.Suppression {
		asked = append(asked, key)
		if key != zoneKey {
			return leeway.Suppression{}
		}
		return leeway.Suppression{State: leeway.TransientWarmup, Suppress: true, Reason: "warming"}
	})
	if len(asked) != 2 {
		t.Fatalf("the suppressor was asked about %v, want both axes", asked)
	}
	for _, ev := range got {
		want := leeway.TransientNone
		if ev.Key == zoneKey {
			want = leeway.TransientWarmup
		}
		if ev.Suppression.State != want {
			t.Errorf("%s: transient %v, want %v", ev.Key, ev.Suppression.State, want)
		}
	}

	for _, ev := range ScoreSubject(res, nil, leeway.DefaultThresholds(), nil) {
		if ev.Suppression != (leeway.Suppression{}) {
			t.Errorf("%s: nil suppressor produced %+v, want the zero Suppression", ev.Key, ev.Suppression)
		}
	}
}

// armedSource is a source whose caches are declared synced without running
// informers, so a test can drive the inventory by hand and ask §7.6 directly.
// Without this every answer is cluster-warmup, which is correct and useless.
func armedSource(cfg Config, nodes ...*corev1.Node) *Source {
	s := New(fake.NewSimpleClientset(), cfg)
	for _, n := range nodes {
		s.inv.Upsert(n)
	}
	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()
	return s
}

func TestSource_WarmupSuppressesUntilTheSentinelIsArmed(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	s.inv.Upsert(node("n-a", "us-central1-a"))

	got := s.suppression(t0)(zoneKey, evenlyEligible("us-central1-a"))
	if got.State != leeway.TransientWarmup || !got.Suppress {
		t.Fatalf("suppression before arming = %+v, want a suppressing cluster-warmup", got)
	}

	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()
	if got := s.suppression(t0)(zoneKey, evenlyEligible("us-central1-a")); got.State != leeway.TransientNone {
		t.Errorf("suppression after arming = %+v, want no transient", got)
	}
}

func TestSource_ADomainOutageSuppressesOnlyTheSubjectsThatCouldReachIt(t *testing.T) {
	cfg := Config{TopologyKeys: []leeway.TopologyKey{zoneKey}}
	s := armedSource(cfg,
		node("n-a1", "us-central1-a"), node("n-a2", "us-central1-a"), node("n-a3", "us-central1-a"),
		node("n-b", "us-central1-b"), node("n-c", "us-central1-c"))
	s.inv.SampleReady(t0, cfg.readyRetention())

	// Two of zone a's three nodes go NotReady: one left of three is strictly
	// more than half gone.
	s.inv.Upsert(node("n-a1", "us-central1-a", notReady()))
	s.inv.Upsert(node("n-a2", "us-central1-a", notReady()))
	now := t0.Add(time.Minute)
	s.inv.SampleReady(now, cfg.readyRetention())

	sup := s.suppression(now)
	if got := sup(zoneKey, evenlyEligible("us-central1-a", "us-central1-b", "us-central1-c")); !got.Suppress || got.State != leeway.TransientDomainOutage {
		t.Errorf("a subject eligible for the dying zone = %+v, want a suppressing domain-outage", got)
	}
	// A subject pinned away from zone a is unaffected: the counts it is scored
	// against describe a cluster nothing happened to.
	if got := sup(zoneKey, evenlyEligible("us-central1-b", "us-central1-c")); got.State != leeway.TransientNone {
		t.Errorf("a subject that cannot reach the dying zone = %+v, want no transient", got)
	}
}

func TestSource_ARecentDrainRelaxesRatherThanSuppresses(t *testing.T) {
	cfg := Config{TopologyKeys: []leeway.TopologyKey{zoneKey}}
	s := armedSource(cfg,
		node("n-a", "us-central1-a", cordonedAtTaint(t0.Add(-2*time.Minute))),
		node("n-b", "us-central1-b"))
	s.inv.SampleReady(t0, cfg.readyRetention())

	got := s.suppression(t0)(zoneKey, evenlyEligible("us-central1-a", "us-central1-b"))
	if got.Suppress || !got.Relax || got.State != leeway.TransientDrain {
		t.Fatalf("suppression = %+v, want a relaxing node-drain", got)
	}
	if got.Multiplier != leeway.DefaultTransientConfig().Multiplier {
		t.Errorf("multiplier = %v, want the configured default", got.Multiplier)
	}

	// Past the settle window it stops mattering. The cordon is still there;
	// what has expired is the claim that the pods are still moving.
	late := t0.Add(leeway.DefaultTransientConfig().DrainSettleWindow + time.Minute)
	if got := s.suppression(late)(zoneKey, evenlyEligible("us-central1-a", "us-central1-b")); got.State != leeway.TransientNone {
		t.Errorf("suppression after the settle window = %+v, want no transient", got)
	}
}

// §14's exit criterion, at the mechanism level: one dead zone is one cluster
// event, not one finding per workload that had pods in it.
func TestSource_AZoneOutageIsOneClusterEventNotAFindingPerWorkload(t *testing.T) {
	// Zone a has three nodes so that losing two is strictly more than half.
	// Every Deployment starts evenly spread across the three zones, which is
	// the configuration nobody is paged about.
	objs := []runtime.Object{
		node("n-a1", "us-central1-a"), node("n-a2", "us-central1-a"), node("n-a3", "us-central1-a"),
		node("n-b", "us-central1-b"), node("n-c", "us-central1-c"),
	}
	const workloads = 20
	for w := range workloads {
		name := fmt.Sprintf("app-%d", w)
		objs = append(objs, replicaSet(name+"-7c9f", "prod", name))
		for i, n := range []string{"n-a1", "n-a2", "n-b", "n-b", "n-c", "n-c"} {
			objs = append(objs, pod(fmt.Sprintf("%s-%d", name, i), "prod", n, ownedBy("ReplicaSet", name+"-7c9f")))
		}
	}
	s, _ := runAlerting(t, quickAlerts(), nil, objs...)

	waitFor(t, "every workload to be scored", func() bool { return scoredAxes(s) == workloads })
	if n := s.alerts.Len(); n != 0 {
		t.Fatalf("%d episode(s) open before anything happened", n)
	}

	// The zone loses two of its three nodes, and every workload's pods there
	// are rescheduled into the survivors. Each of the twenty is now 0/3/3
	// against an expectation of 2/2/2 — a third of its pods in the wrong
	// place, twenty times over, in the same second.
	s.inv.Upsert(node("n-a1", "us-central1-a", notReady()))
	s.inv.Upsert(node("n-a2", "us-central1-a", notReady()))
	for w := range workloads {
		name := fmt.Sprintf("app-%d", w)
		for i, n := range []string{"n-b", "n-c"} {
			s.state.OnPodAdd(pod(fmt.Sprintf("%s-%d", name, i), "prod", n, ownedBy("ReplicaSet", name+"-7c9f")))
		}
	}

	waitFor(t, "the outage to reach every evaluation", func() bool {
		return suppressedAxes(s) == workloads
	})
	// Long enough for several alert ticks at the five-millisecond interval, so
	// that "no episodes" means the machine declined rather than had not run.
	time.Sleep(50 * time.Millisecond)
	if n := s.alerts.Len(); n != 0 {
		t.Errorf("%d episode(s) opened for one dead zone, want none: §14 asks for one finding, not four hundred", n)
	}
}

// The control for the test above. The same movement, with the zone perfectly
// healthy, is exactly the finding leeway exists to raise.
func TestSource_TheSameSkewWithoutAnOutageIsStillAFinding(t *testing.T) {
	objs := []runtime.Object{
		node("n-a1", "us-central1-a"), node("n-a2", "us-central1-a"), node("n-a3", "us-central1-a"),
		node("n-b", "us-central1-b"), node("n-c", "us-central1-c"),
		replicaSet("web-7c9f", "prod", "web"),
	}
	for i, n := range []string{"n-a1", "n-a2", "n-b", "n-b", "n-c", "n-c"} {
		objs = append(objs, pod(fmt.Sprintf("web-%d", i), "prod", n, ownedBy("ReplicaSet", "web-7c9f")))
	}
	s, _ := runAlerting(t, quickAlerts(), nil, objs...)

	for i, n := range []string{"n-b", "n-c"} {
		s.state.OnPodAdd(pod(fmt.Sprintf("web-%d", i), "prod", n, ownedBy("ReplicaSet", "web-7c9f")))
	}

	waitFor(t, "the episode to fire", func() bool {
		st, ok := s.alerts.StateOf(webSub, zoneKey)
		return ok && st.Phase == leeway.PhaseFiring
	})
}

// scoredAxes counts the subject-axes the source has an evaluation for.
func scoredAxes(s *Source) int {
	n := 0
	s.state.EachEvaluation(func(leeway.SubjectRef, *Evaluation) { n++ })
	return n
}

// suppressedAxes counts the subject-axes §7.6 is currently holding back.
func suppressedAxes(s *Source) int {
	n := 0
	s.state.EachEvaluation(func(_ leeway.SubjectRef, ev *Evaluation) {
		if ev.Verdict.Suppressed {
			n++
		}
	})
	return n
}
