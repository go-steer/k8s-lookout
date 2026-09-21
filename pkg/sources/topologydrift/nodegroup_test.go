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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

const (
	classLabel = "cloud.google.com/compute-class"
	poolLabel  = "cloud.google.com/gke-nodepool"
)

// inPool labels a node with a GKE node pool, which is the last entry of the
// default precedence list that a plain GKE cluster actually carries.
func inPool(name string) nodeOpt { return withLabel(poolLabel, name) }

// nodeGroupSource builds a Source driven directly rather than through Run: the
// FR-3 pass is a timer tick over the inventory, so a test that seeds the
// inventory and calls the tick is testing the shipped pass with nothing
// simulated. armed is set because §7.6's warming row suppresses every axis on
// a source whose informers have not synced, and these tests are about what the
// scoring says, not about whether warmup silences it.
func nodeGroupSource(t *testing.T, cfg Config, nodes ...*corev1.Node) (*Source, *logSink) {
	t.Helper()
	if cfg.TopologyKeys == nil {
		cfg.TopologyKeys = []leeway.TopologyKey{zoneKey}
	}
	s := New(fake.NewSimpleClientset(), cfg)
	sink := &logSink{}
	s.logf = sink.logf
	s.armed = true
	for _, n := range nodes {
		s.inv.Upsert(n)
	}
	return s, sink
}

// logSink records what the source logged, so a test can assert both the text
// and how many times it appeared.
type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (l *logSink) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logSink) matching(substr string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			out = append(out, line)
		}
	}
	return out
}

// groupEval is one node group's evaluation on one axis, or nil if the group is
// not tracked at all.
func groupEval(s *Source, name string, key leeway.TopologyKey) *Evaluation {
	sub := leeway.SubjectRef{Kind: leeway.SubjectNodeGroup, Name: name}
	for i, ev := range s.state.EvaluationsOf(sub) {
		if ev.Key == key {
			return &s.state.EvaluationsOf(sub)[i]
		}
	}
	return nil
}

func TestNodeGroupOf_PrecedenceIsOrdered(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
		wantOK bool
	}{{
		// The reason the list is ordered rather than a set. A node autoscaling
		// created under a compute class carries both labels, and the pool name
		// is the ephemeral one — grouping by it would produce a new subject
		// every time NAP rebuilds the pool.
		name:   "compute class outranks the pool it created",
		labels: map[string]string{classLabel: "high-throughput", poolLabel: "nap-b2c7f1"},
		want:   "high-throughput",
		wantOK: true,
	}, {
		name:   "the pool answers where there is no class",
		labels: map[string]string{poolLabel: "np-default"},
		want:   "np-default",
		wantOK: true,
	}, {
		// An empty value is not an answer. A node carrying the key with no
		// value would otherwise collect every such node into one group named
		// "", which is the shape of the cardinality bug in reverse.
		name:   "an empty value falls through to the next key",
		labels: map[string]string{classLabel: "", poolLabel: "np-default"},
		want:   "np-default",
		wantOK: true,
	}, {
		name:   "no key matches",
		labels: map[string]string{"kubernetes.io/hostname": "n-1"},
		wantOK: false,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := nodeGroupOf(tc.labels, DefaultNodeGroupLabelKeys)
			if ok != tc.wantOK || got != tc.want {
				t.Errorf("nodeGroupOf = (%q, %v), want (%q, %v)", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestInventoryNodeGroups_GroupsAndStates(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey, poolKey})
	inv.Upsert(node("a-1", "zone-a", inPool("np-default")))
	inv.Upsert(node("a-2", "zone-a", inPool("np-default"), notReady()))
	inv.Upsert(node("b-1", "zone-b", inPool("np-default"), cordoned()))
	inv.Upsert(node("b-2", "zone-b", inPool("np-general")))
	inv.Upsert(node("c-1", "zone-c")) // unlabelled: not a group member

	got := inv.NodeGroups(DefaultNodeGroupLabelKeys)
	if len(got) != 2 {
		t.Fatalf("NodeGroups returned %d groups (%v), want np-default and np-general", len(got), groupNames(got))
	}

	// One distribution per configured axis, for every group, always. A group
	// present on the zone axis and absent on the pool axis would make the two
	// evaluations of the same subject disagree about whether it exists.
	for name, byKey := range got {
		if len(byKey) != 2 {
			t.Errorf("group %q has %d axes, want one per configured key", name, len(byKey))
		}
	}

	// Usable nodes count Running and the rest count Unschedulable, because
	// DomainStats sums allocatable over usable nodes only: a NotReady node
	// counted into the actual while its capacity is absent from the
	// expectation manufactures drift out of a node reboot.
	zones := []leeway.Domain{"zone-a", "zone-b", "zone-c"}
	def := got["np-default"][zoneKey]
	// zone-c holds only the unlabelled node, which is nobody's group member.
	if running := def.Counts(zones, leeway.StateRunning); !slices.Equal(running, []int64{1, 0, 0}) {
		t.Errorf("np-default running = %v, want [1 0 0]", running)
	}
	if out := def.Counts(zones, leeway.StateUnschedulable); !slices.Equal(out, []int64{1, 1, 0}) {
		t.Errorf("np-default unschedulable = %v, want [1 1 0] (NotReady in zone-a, cordoned in zone-b)", out)
	}

	// An empty precedence list is the off switch, and it has to be the off
	// switch here as well as at the flag: --topology-node-group-keys= is how an
	// operator whose estate labels nodes per-node turns the subjects off
	// without turning the source off.
	if got := inv.NodeGroups(nil); got != nil {
		t.Errorf("NodeGroups(nil) = %v, want nil", got)
	}
}

// groupNames is the sorted group names, for a failure message that has to say
// what it found and not just how many.
func groupNames(m map[string]map[leeway.TopologyKey]*leeway.Distribution) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func TestStateGroups_AreTrackedButNotSweptOrVerified(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("a-1", "zone-a", inPool("np-default")))
	st := NewState(StateOptions{Inventory: inv})

	sub := leeway.SubjectRef{Kind: leeway.SubjectNodeGroup, Name: "np-default"}
	dist := leeway.NewDistribution()
	dist.Add(leeway.Domain("zone-a"), leeway.StateRunning, false)
	st.SetGroups(map[leeway.SubjectRef]map[leeway.TopologyKey]*leeway.Distribution{
		sub: {zoneKey: dist},
	})

	if snap := st.Snapshot(sub); snap[zoneKey] == nil {
		t.Error("Snapshot of a node group is nil, want the distribution SetGroups adopted")
	}
	if got := st.Groups(); !slices.Equal(got, []leeway.SubjectRef{sub}) {
		t.Errorf("Groups() = %v, want [%v]", got, sub)
	}
	if got := st.SubjectCounts()[leeway.SubjectNodeGroup]; got != 1 {
		t.Errorf("SubjectCounts()[NodeGroup] = %d, want 1", got)
	}

	// The load-bearing negative. §6.5's verifier rebuilds each shard of
	// Subjects() from the pod cache and Repairs anything that disagrees; a node
	// group has no pods, so listing it there would have the verifier find it
	// empty, call it counter drift and erase it on the next sweep.
	if got := st.Subjects(); len(got) != 0 {
		t.Errorf("Subjects() = %v, want no node groups — the sweep and the verifier read this", got)
	}

	// SetEvaluations only records for a tracked subject, so this also asserts
	// tracked() consults the group map.
	st.SetEvaluations(sub, []Evaluation{{Key: zoneKey}})
	if got := st.EvaluationsOf(sub); len(got) != 1 {
		t.Fatalf("EvaluationsOf after SetEvaluations = %v, want one evaluation", got)
	}

	// A pool that is deleted stops being a subject, evaluations and all. The
	// pass hands over the whole map each tick, so departure is "absent from the
	// new map" rather than an explicit delete.
	st.SetGroups(nil)
	if got := st.Groups(); len(got) != 0 {
		t.Errorf("Groups() after SetGroups(nil) = %v, want empty", got)
	}
	if got := st.EvaluationsOf(sub); got != nil {
		t.Errorf("EvaluationsOf a departed group = %v, want nil", got)
	}
	if got := st.SubjectCounts()[leeway.SubjectNodeGroup]; got != 0 {
		t.Errorf("SubjectCounts()[NodeGroup] = %d, want 0", got)
	}
}

// TestNodeGroupSubjects_ImbalanceIsAttributedToThePool is §14's phase 7 exit
// criterion, stated in issue #469: "a pool with nodes in one of the three zones
// it is configured for produces one finding naming the pool, and the workloads
// riding it do not each produce their own".
//
// The cluster is three zones. np-default has two nodes in each, np-general has
// six all in zone-a, and every node is four cores. Both halves of the criterion
// are asserted here rather than split, because the claim is comparative: it is
// not enough that the pool breaches, the things that ride it have to stay
// quiet, and the balanced pool alongside it has to stay quiet too.
func TestNodeGroupSubjects_ImbalanceIsAttributedToThePool(t *testing.T) {
	var nodes []*corev1.Node
	for _, zone := range []string{"zone-a", "zone-b", "zone-c"} {
		for i := range 2 {
			nodes = append(nodes, node(fmt.Sprintf("%s-def-%d", zone, i), zone, inPool("np-default")))
		}
	}
	for i := range 6 {
		nodes = append(nodes, node(fmt.Sprintf("zone-a-gen-%d", i), "zone-a", inPool("np-general")))
	}
	s, _ := nodeGroupSource(t, Config{}, nodes...)
	s.evaluateNodeGroups(time.Now())

	t.Run("the concentrated pool breaches and is named", func(t *testing.T) {
		ev := groupEval(s, "np-general", zoneKey)
		if ev == nil {
			t.Fatal("np-general was not evaluated")
		}
		if !slices.Equal(ev.Scores.Actual, []int64{6, 0, 0}) {
			t.Errorf("actual = %v, want [6 0 0]", ev.Scores.Actual)
		}
		// Equal, not capacity: zone-a holds 32 of the cluster's 48 cores, and a
		// capacity-weighted expectation would be [4 1 1] — the pool partly
		// excusing its own concentration with the capacity that concentration
		// is made of. See EligibilityOptions.UndeclaredWeighting.
		if !slices.Equal(ev.Scores.Expected, []int64{2, 2, 2}) {
			t.Errorf("expected = %v, want [2 2 2] — an even expectation, not a capacity-weighted one", ev.Scores.Expected)
		}
		if !ev.Verdict.Breached {
			t.Errorf("np-general did not breach: %+v", ev.Scores)
		}
		if ev.Scores.Relocation != 4 || ev.Scores.Drift < 0.66 {
			t.Errorf("R = %d, rho = %.3f, want 4 nodes to move and rho = 2/3", ev.Scores.Relocation, ev.Scores.Drift)
		}
		// Tier C, and that is the point of giving these subjects no intent:
		// promoting every pool in an estate to a tier that emits by default is
		// how a cardinality bound becomes an alert storm.
		if ev.Verdict.Tier != leeway.TierC {
			t.Errorf("tier = %v, want C — a node group declares no intent", ev.Verdict.Tier)
		}
		if ev.Intent != nil {
			t.Errorf("intent = %+v, want nil", ev.Intent)
		}
	})

	t.Run("the balanced pool alongside it is quiet", func(t *testing.T) {
		ev := groupEval(s, "np-default", zoneKey)
		if ev == nil {
			t.Fatal("np-default was not evaluated")
		}
		if ev.Verdict.Breached {
			t.Errorf("np-default breached: %+v", ev.Scores)
		}
		if ev.Scores.Drift != 0 {
			t.Errorf("rho = %.3f, want 0 — two nodes in each of three zones is the expectation", ev.Scores.Drift)
		}
	})

	t.Run("a workload riding the pool does not produce its own finding", func(t *testing.T) {
		// The other half of the criterion, and it needs no new machinery: §7.1
		// narrows a pinned workload to the domains it can actually reach, so a
		// Deployment selecting np-general is eligible for zone-a alone and
		// scores a perfect zero against it. Without that, every workload on a
		// lopsided pool would restate the pool's imbalance once each.
		rep := pod("web-0", "prod", "zone-a-gen-0", func(p *corev1.Pod) {
			p.Spec.NodeSelector = map[string]string{poolLabel: "np-general"}
		})
		res := Resolve(rep, s.inv, ResolveConfig{ClusterDefaults: noClusterDefaults})
		eligible := res.Eligible[zoneKey]
		if !slices.Equal(eligible.Domains, []leeway.Domain{"zone-a"}) {
			t.Fatalf("eligible domains = %v, want [zone-a] — §7.1 should narrow to the pool's zone", eligible.Domains)
		}

		dist := leeway.NewDistribution()
		for i := range 6 {
			dist.Add(domainOf(s.inv, zoneKey, fmt.Sprintf("zone-a-gen-%d", i)), leeway.StateRunning, false)
		}
		ev := ScoreAxis(zoneKey, res.Intents[zoneKey], eligible, dist, leeway.DefaultThresholds(), leeway.Suppression{})
		if ev.Verdict.Breached {
			t.Errorf("the workload breached as well as the pool: %+v", ev.Scores)
		}
	})
}

func TestEvaluateNodeGroups_CardinalityBound(t *testing.T) {
	// A precedence list that resolved to something per-node is the failure this
	// bound exists for, so the fixture is that: the hostname label, three
	// nodes, three groups, a bound of two.
	cfg := Config{
		NodeGroupLabelKeys: []string{corev1.LabelHostname},
		MaxNodeGroups:      2,
	}
	s, sink := nodeGroupSource(t, cfg,
		node("a-1", "zone-a", withLabel(corev1.LabelHostname, "a-1")),
		node("a-2", "zone-a", withLabel(corev1.LabelHostname, "a-2")),
		node("b-1", "zone-b", withLabel(corev1.LabelHostname, "b-1")),
	)

	s.evaluateNodeGroups(time.Now())
	if got := s.state.Groups(); len(got) != 0 {
		t.Errorf("tracked %v over the bound, want none of them", got)
	}
	// The count keeps being reported. It is the reading that sends somebody to
	// the precedence list, and a gauge that went to zero when the bound tripped
	// would look exactly like a cluster with no node pools.
	if got := s.nodeGroupsDiscovered(); got != 3 {
		t.Errorf("nodeGroupsDiscovered() = %d, want 3", got)
	}

	// Once, not once per tick: this pass runs on a timer, and a log line per
	// tick for a misconfiguration nobody is watching for is its own outage.
	s.evaluateNodeGroups(time.Now())
	if got := sink.matching("over the --topology-max-node-groups bound"); len(got) != 1 {
		t.Errorf("logged the bound warning %d times, want once: %v", len(got), got)
	}

	// And it warns again after recovering, because the second occurrence is
	// news even though the text is the same.
	s.cfg.MaxNodeGroups = 10
	s.evaluateNodeGroups(time.Now())
	if got := len(s.state.Groups()); got != 3 {
		t.Fatalf("tracked %d groups under a raised bound, want 3", got)
	}
	s.cfg.MaxNodeGroups = 2
	s.evaluateNodeGroups(time.Now())
	if got := sink.matching("over the --topology-max-node-groups bound"); len(got) != 2 {
		t.Errorf("logged the bound warning %d times across two trips, want twice", len(got))
	}
	if got := s.state.Groups(); len(got) != 0 {
		t.Errorf("tracked %v after the bound tripped again, want none — SetGroups(nil) must forget the lot", got)
	}
}

func TestEvaluateNodeGroups_NegativeBoundIsTheOffSwitch(t *testing.T) {
	s, _ := nodeGroupSource(t, Config{MaxNodeGroups: -1},
		node("a-1", "zone-a", inPool("np-default")),
	)
	s.evaluateNodeGroups(time.Now())

	if got := s.state.Groups(); len(got) != 0 {
		t.Errorf("Groups() = %v, want none — a negative bound turns the pass off", got)
	}
	// Off means off, including the gauge: the discovery count is a product of
	// the pass, and reporting one while tracking nothing would read as the
	// bound having tripped.
	if got := s.nodeGroupsDiscovered(); got != 0 {
		t.Errorf("nodeGroupsDiscovered() = %d, want 0", got)
	}
}
