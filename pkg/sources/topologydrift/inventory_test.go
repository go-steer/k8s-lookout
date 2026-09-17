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
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

const (
	zoneKey = leeway.TopologyKey(corev1.LabelTopologyZone)
	poolKey = leeway.TopologyKey("cloud.google.com/gke-nodepool")
)

type nodeOpt func(*corev1.Node)

func withLabel(k, v string) nodeOpt {
	return func(n *corev1.Node) { n.Labels[k] = v }
}

func notReady() nodeOpt {
	return func(n *corev1.Node) { n.Status.Conditions[0].Status = corev1.ConditionFalse }
}

func cordoned() nodeOpt {
	return func(n *corev1.Node) { n.Spec.Unschedulable = true }
}

func withTaint(key, value string, effect corev1.TaintEffect) nodeOpt {
	return func(n *corev1.Node) {
		n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: key, Value: value, Effect: effect})
	}
}

func withAllocatable(cpu, mem string) nodeOpt {
	return func(n *corev1.Node) {
		n.Status.Allocatable = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpu),
			corev1.ResourceMemory: resource.MustParse(mem),
		}
	}
}

// node builds a ready, schedulable node in the given zone, plus whatever the
// options change.
func node(name, zone string, opts ...nodeOpt) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{
				Type:   corev1.NodeReady,
				Status: corev1.ConditionTrue,
			}},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("16Gi"),
			},
		},
	}
	if zone != "" {
		n.Labels[corev1.LabelTopologyZone] = zone
	}
	for _, o := range opts {
		o(n)
	}
	return n
}

func TestNewInventory_OrdinalsAreStableAndDeduplicated(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey, poolKey, zoneKey, ""})

	keys := inv.Keys()
	if !slices.Equal(keys, []leeway.TopologyKey{zoneKey, poolKey}) {
		t.Fatalf("Keys() = %v, want [zone pool]", keys)
	}
	if i, ok := inv.Ordinal(zoneKey); !ok || i != 0 {
		t.Errorf("Ordinal(zone) = %d, %v; want 0, true", i, ok)
	}
	if i, ok := inv.Ordinal(poolKey); !ok || i != 1 {
		t.Errorf("Ordinal(pool) = %d, %v; want 1, true", i, ok)
	}
	if _, ok := inv.Ordinal("nope"); ok {
		t.Error("Ordinal reported an unconfigured key")
	}

	// Keys() must not hand out the internal slice. An ordinal is baked into
	// every Placement in the index, so a caller that overwrote an entry would
	// silently re-label an axis under counts already taken against it.
	keys[0] = "injected"
	if inv.Keys()[0] != zoneKey {
		t.Error("Keys() aliased the inventory's own slice")
	}
}

func TestInventory_UpsertAddsAndMaps(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey, poolKey})

	ch := inv.Upsert(node("n1", "us-central1-a", withLabel(string(poolKey), "pool-1")))
	if !ch.Added || !ch.DomainsChanged || !ch.EligibilityChanged {
		t.Errorf("first Upsert returned %+v, want everything set", ch)
	}
	if inv.Len() != 1 {
		t.Errorf("Len = %d, want 1", inv.Len())
	}

	domains := inv.DomainsOf("n1")
	if !slices.Equal(domains, []leeway.Domain{"us-central1-a", "pool-1"}) {
		t.Errorf("DomainsOf(n1) = %v", domains)
	}
	if got := inv.DomainsOf("unknown-node"); got != nil {
		t.Errorf("DomainsOf(unknown) = %v, want nil", got)
	}
}

func TestInventory_UnlabelledNodeIsUnknownNotAbsent(t *testing.T) {
	// A node pool created without the topology label is the failure leeway
	// exists to notice, so it must show up as a visible domain rather than
	// vanish from the distribution.
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("n1", ""))

	if got := inv.DomainsOf("n1"); !slices.Equal(got, []leeway.Domain{leeway.DomainUnknown}) {
		t.Errorf("DomainsOf = %v, want [%s]", got, leeway.DomainUnknown)
	}
	stats := inv.Stats(zoneKey)
	if s := stats[leeway.DomainUnknown]; s.Nodes != 1 || s.Usable != 1 {
		t.Errorf("unknown domain stats = %+v, want 1 node / 1 usable", s)
	}
}

func TestInventory_BetaTopologyLabelFallback(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey, leeway.TopologyKey(corev1.LabelTopologyRegion)})

	beta := node("n1", "")
	beta.Labels[corev1.LabelFailureDomainBetaZone] = "us-central1-a"
	beta.Labels[corev1.LabelFailureDomainBetaRegion] = "us-central1"
	inv.Upsert(beta)

	if got := inv.DomainsOf("n1"); !slices.Equal(got, []leeway.Domain{"us-central1-a", "us-central1"}) {
		t.Errorf("beta-only node resolved to %v", got)
	}

	// Where both spellings are present the GA one wins, which is what the
	// cloud providers that write both intend.
	both := node("n2", "ga-zone")
	both.Labels[corev1.LabelFailureDomainBetaZone] = "beta-zone"
	inv.Upsert(both)
	if got := inv.DomainsOf("n2")[0]; got != "ga-zone" {
		t.Errorf("GA label lost to beta: %q", got)
	}
}

func TestInventory_InertUpdateChangesNothing(t *testing.T) {
	// The case that decides whether leeway is affordable on a shared informer:
	// a node status update whose only difference is a heartbeat must produce a
	// zero Change, so no pods are re-mapped and the generation does not move.
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("n1", "us-central1-a"))
	gen := inv.Generation()

	churned := node("n1", "us-central1-a")
	churned.Status.Conditions[0].LastHeartbeatTime = metav1.NewTime(time.Now())
	churned.ResourceVersion = "99999"
	churned.Status.NodeInfo.KubeletVersion = "v1.33.1"

	if ch := inv.Upsert(churned); ch.Any() {
		t.Errorf("heartbeat churn reported %+v, want no change", ch)
	}
	if inv.Generation() != gen {
		t.Error("heartbeat churn bumped the generation")
	}
}

func TestInventory_UpsertChangeKinds(t *testing.T) {
	tests := []struct {
		name string
		to   *corev1.Node
		want Change
	}{
		{
			// Both flags, and not by accident: the pods on this node are now
			// counted in the wrong domain (the narrow re-map), and the domain
			// composition of the cluster moved too — zone-a may have just
			// emptied (the broad generation bump).
			"relabelled to another zone",
			node("n1", "us-central1-b"),
			Change{DomainsChanged: true, EligibilityChanged: true},
		},
		{
			"went NotReady",
			node("n1", "us-central1-a", notReady()),
			Change{EligibilityChanged: true},
		},
		{
			"cordoned",
			node("n1", "us-central1-a", cordoned()),
			Change{EligibilityChanged: true},
		},
		{
			"taint added",
			node("n1", "us-central1-a", withTaint("dedicated", "gpu", corev1.TaintEffectNoSchedule)),
			Change{EligibilityChanged: true},
		},
		{
			"allocatable shrank",
			node("n1", "us-central1-a", withAllocatable("2", "8Gi")),
			Change{EligibilityChanged: true},
		},
		{
			// A deviation from §6.3's five-field list, and deliberate: a
			// non-topology label can be the thing a subject's nodeSelector
			// matches on, so it moves the eligible set even though no domain
			// moved.
			"non-topology label added",
			node("n1", "us-central1-a", withLabel("workload", "batch")),
			Change{EligibilityChanged: true},
		},
		{
			"relabelled and cordoned at once",
			node("n1", "us-central1-b", cordoned()),
			Change{DomainsChanged: true, EligibilityChanged: true},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv := NewInventory([]leeway.TopologyKey{zoneKey})
			inv.Upsert(node("n1", "us-central1-a"))
			gen := inv.Generation()

			if got := inv.Upsert(tc.to); got != tc.want {
				t.Errorf("Change = %+v, want %+v", got, tc.want)
			}
			if inv.Generation() <= gen {
				t.Error("a real change did not bump the generation")
			}
		})
	}
}

func TestInventory_TaintTimeAddedIsNotAChange(t *testing.T) {
	// A node controller re-applying an identical taint moves TimeAdded.
	// Treating that as a change would bump the generation fleet-wide for
	// nothing.
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	first := node("n1", "us-central1-a", withTaint("node.kubernetes.io/unreachable", "", corev1.TaintEffectNoExecute))
	first.Spec.Taints[0].TimeAdded = ptrTime(time.Now().Add(-time.Hour))
	inv.Upsert(first)

	again := node("n1", "us-central1-a", withTaint("node.kubernetes.io/unreachable", "", corev1.TaintEffectNoExecute))
	again.Spec.Taints[0].TimeAdded = ptrTime(time.Now())
	if ch := inv.Upsert(again); ch.Any() {
		t.Errorf("re-applied taint reported %+v, want no change", ch)
	}
}

func ptrTime(t time.Time) *metav1.Time {
	mt := metav1.NewTime(t)
	return &mt
}

func TestInventory_TaintValueChangeIsAChange(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("n1", "zone-a", withTaint("dedicated", "gpu", corev1.TaintEffectNoSchedule)))

	if ch := inv.Upsert(node("n1", "zone-a", withTaint("dedicated", "tpu", corev1.TaintEffectNoSchedule))); !ch.EligibilityChanged {
		t.Errorf("retargeted taint reported %+v, want EligibilityChanged", ch)
	}
}

func TestInventory_KeyWithNoValueAndNoBetaFallback(t *testing.T) {
	// A pool label the node simply does not carry: no beta spelling exists for
	// a vendor key, so the answer is the sentinel and not a lookup miss.
	inv := NewInventory([]leeway.TopologyKey{zoneKey, poolKey})
	inv.Upsert(node("n1", "zone-a"))

	if got := inv.DomainsOf("n1"); !slices.Equal(got, []leeway.Domain{"zone-a", leeway.DomainUnknown}) {
		t.Errorf("DomainsOf = %v, want [zone-a %s]", got, leeway.DomainUnknown)
	}
}

// The next two exercise guards that the public API cannot reach — an ordinal
// outside the configured key set, and a decrement against a domain with no
// stats row. Both exist so that a key-set change or a double delete degrades
// into a counting inaccuracy the §6.5 verifier repairs, rather than a panic in
// an informer handler. Testing them directly is the only way to know the guard
// works, since by construction no sequence of Upsert and Remove gets there.
func TestNodeFacts_DomainOutOfRange(t *testing.T) {
	n := &nodeFacts{domains: []leeway.Domain{"zone-a"}}
	for _, ordinal := range []int{-1, 1, 99} {
		if got := n.domain(ordinal); got != leeway.DomainUnknown {
			t.Errorf("domain(%d) = %q, want %q", ordinal, got, leeway.DomainUnknown)
		}
	}
	var empty nodeFacts
	if got := empty.domain(0); got != leeway.DomainUnknown {
		t.Errorf("zero-value domain(0) = %q", got)
	}
}

func TestInventory_DecrementAgainstMissingDomainIsIgnored(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.addStats(&nodeFacts{name: "ghost", domains: []leeway.Domain{"zone-a"}, ready: true, schedulable: true}, -1)

	if got := inv.Stats(zoneKey); len(got) != 0 {
		t.Errorf("a decrement conjured a domain row: %+v", got)
	}
}

func TestInventory_StatsTrackMembershipAndUsability(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("a1", "zone-a", withAllocatable("4", "16Gi")))
	inv.Upsert(node("a2", "zone-a", withAllocatable("8", "32Gi")))
	inv.Upsert(node("b1", "zone-b", withAllocatable("4", "16Gi")))

	stats := inv.Stats(zoneKey)
	if got := stats["zone-a"]; got.Nodes != 2 || got.Usable != 2 || got.AllocatableCPUMilli != 12_000 {
		t.Errorf("zone-a = %+v", got)
	}

	// A cordoned node still exists; it is just not somewhere a pod can land.
	// Keeping the two counts apart is what lets a finding say "the zone is
	// there but unusable" instead of "the zone is gone".
	inv.Upsert(node("a2", "zone-a", withAllocatable("8", "32Gi"), cordoned()))
	stats = inv.Stats(zoneKey)
	if got := stats["zone-a"]; got.Nodes != 2 || got.Usable != 1 || got.AllocatableCPUMilli != 4_000 {
		t.Errorf("after cordon, zone-a = %+v, want 2 nodes / 1 usable / 4000m", got)
	}

	if inv.Stats("unconfigured") != nil {
		t.Error("Stats returned a map for an unconfigured key")
	}
}

func TestInventory_StatsFollowARelabel(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("n1", "zone-a"))
	inv.Upsert(node("n1", "zone-b"))

	stats := inv.Stats(zoneKey)
	if _, present := stats["zone-a"]; present {
		t.Error("zone-a survived as an empty row after its only node moved")
	}
	if got := stats["zone-b"]; got.Nodes != 1 {
		t.Errorf("zone-b = %+v, want 1 node", got)
	}
}

func TestInventory_RemoveEmptiesTheDomain(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("n1", "zone-a"))
	gen := inv.Generation()

	ch := inv.Remove("n1")
	if !ch.Removed || !ch.DomainsChanged || !ch.EligibilityChanged {
		t.Errorf("Remove returned %+v, want everything set", ch)
	}
	if inv.Len() != 0 {
		t.Errorf("Len = %d after removing the only node", inv.Len())
	}
	if inv.Generation() <= gen {
		t.Error("Remove did not bump the generation")
	}
	if len(inv.Stats(zoneKey)) != 0 {
		t.Errorf("stats kept a row for an emptied domain: %+v", inv.Stats(zoneKey))
	}
	if got := inv.DomainsOf("n1"); got != nil {
		t.Errorf("DomainsOf after Remove = %v, want nil", got)
	}

	// Removing an unknown node is a no-op: informer deletes can arrive twice.
	if ch := inv.Remove("n1"); ch.Any() {
		t.Errorf("second Remove reported %+v", ch)
	}
}

func TestInventory_UpsertIgnoresNilAndUnnamed(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	if ch := inv.Upsert(nil); ch.Any() {
		t.Errorf("Upsert(nil) = %+v", ch)
	}
	if ch := inv.Upsert(&corev1.Node{}); ch.Any() {
		t.Errorf("Upsert(unnamed) = %+v", ch)
	}
	if inv.Len() != 0 {
		t.Error("a nil or unnamed node entered the inventory")
	}
}

func TestInventory_UnknownTuple(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey, poolKey})
	tuple := inv.UnknownTuple()
	want := []leeway.Domain{leeway.DomainUnknown, leeway.DomainUnknown}
	if !slices.Equal(tuple, want) {
		t.Errorf("UnknownTuple = %v, want %v", tuple, want)
	}
	// It is interned like any other, so a cluster where the node cache is
	// still filling does not allocate a tuple per straggler pod.
	if &inv.UnknownTuple()[0] != &tuple[0] {
		t.Error("UnknownTuple allocated a fresh slice per call")
	}
}

func TestInventory_NodeViews(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("n3", "zone-b", withAllocatable("8", "32Gi")))
	inv.Upsert(node("n1", "zone-a", withAllocatable("4", "16Gi")))
	inv.Upsert(node("n2", "zone-a", cordoned()))

	views := inv.NodeViews(zoneKey, leeway.WeightAllocatableCPU)
	if len(views) != 3 {
		t.Fatalf("got %d views, want 3", len(views))
	}
	// Sorted by name, because eligibility feeds scoring and a score that
	// depends on map iteration order is a flake waiting for a release.
	for i, want := range []string{"n1", "n2", "n3"} {
		if views[i].Name != want {
			t.Fatalf("views[%d] = %q, want %q", i, views[i].Name, want)
		}
	}
	if views[0].Domain != "zone-a" || views[0].Capacity != 4000 {
		t.Errorf("n1 view = %+v", views[0])
	}
	if views[1].Schedulable {
		t.Error("the cordoned node came back schedulable")
	}
	if !views[0].MatchesSelector || !views[0].Tolerated {
		t.Error("Phase 2 must present every node as matching; the predicate is Phase 3")
	}

	if got := inv.NodeViews(zoneKey, leeway.WeightAllocatableMemory)[0].Capacity; got != 16*1024*1024*1024 {
		t.Errorf("memory-weighted capacity = %v", got)
	}
	// Equal weighting ignores capacity, so carrying a number there would be
	// noise the engine has to remember to ignore.
	if got := inv.NodeViews(zoneKey, leeway.WeightEqual)[0].Capacity; got != 0 {
		t.Errorf("equal-weighted capacity = %v, want 0", got)
	}
	if inv.NodeViews("unconfigured", leeway.WeightEqual) != nil {
		t.Error("NodeViews returned views for an unconfigured key")
	}
}

func TestInventory_NodeViewsFeedEligibility(t *testing.T) {
	// The join that matters: the inventory's output has to be something
	// pkg/leeway can score without adaptation, and a NotReady zone has to drop
	// out with a reason rather than silently.
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("a1", "zone-a"))
	inv.Upsert(node("b1", "zone-b", notReady()))

	el := leeway.EligibleDomains(inv.NodeViews(zoneKey, leeway.WeightEqual), leeway.DefaultEligibilityOptions())
	if !slices.Equal(el.Domains, []leeway.Domain{"zone-a"}) {
		t.Errorf("eligible domains = %v, want [zone-a]", el.Domains)
	}
	if _, explained := el.Excluded["zone-b"]; !explained {
		t.Error("zone-b was dropped without a reason")
	}
}

func TestInventory_ConcurrentReadsAndWrites(t *testing.T) {
	// Under a shared factory the Node handler writes while the Pod handler
	// reads DomainsOf on every placement. Run under -race, this is the whole
	// claim in the type's doc comment.
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	for n := range 50 {
		inv.Upsert(node(fmt.Sprintf("n%02d", n), fmt.Sprintf("zone-%d", n%3)))
	}

	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range 200 {
				name := fmt.Sprintf("n%02d", n%50)
				switch w {
				case 0:
					inv.Upsert(node(name, fmt.Sprintf("zone-%d", n%4)))
				case 1:
					inv.Remove(name)
				case 2:
					_ = inv.DomainsOf(name)
					_ = inv.Generation()
				default:
					_ = inv.Stats(zoneKey)
					_ = inv.NodeViews(zoneKey, leeway.WeightAllocatableCPU)
				}
			}
		}()
	}
	wg.Wait()
}

func TestInventory_StatsAreConsistentAfterChurn(t *testing.T) {
	// The property Phase 2's exit criteria care about, at node scale: after an
	// arbitrary sequence of adds, relabels and removes, the incremental stats
	// must equal a from-scratch recount. Incremental counters are exactly the
	// code that is right in a unit test and wrong sixty days in.
	inv := NewInventory([]leeway.TopologyKey{zoneKey, poolKey})
	live := map[string]*corev1.Node{}

	for step := range 10_000 {
		name := fmt.Sprintf("n%03d", step%400)
		switch step % 7 {
		case 0, 1, 2, 3:
			n := node(name, fmt.Sprintf("zone-%d", step%5),
				withLabel(string(poolKey), fmt.Sprintf("pool-%d", step%3)))
			if step%11 == 0 {
				cordoned()(n)
			}
			if step%13 == 0 {
				notReady()(n)
			}
			inv.Upsert(n)
			live[name] = n
		case 4:
			inv.Remove(name)
			delete(live, name)
		default:
			// Re-apply the identical object: the inert path must not corrupt
			// the counts either.
			if n, ok := live[name]; ok {
				inv.Upsert(n)
			}
		}
	}

	if inv.Len() != len(live) {
		t.Fatalf("Len = %d, want %d", inv.Len(), len(live))
	}
	for _, key := range []leeway.TopologyKey{zoneKey, poolKey} {
		want := recount(live, key)
		got := inv.Stats(key)
		if len(got) != len(want) {
			t.Fatalf("%s: %d domains, want %d", key, len(got), len(want))
		}
		for d, w := range want {
			if got[d] != w {
				t.Errorf("%s/%s: incremental %+v, recount %+v", key, d, got[d], w)
			}
		}
	}
}

// recount derives the stats from scratch, the way §6.5's verifier does.
func recount(nodes map[string]*corev1.Node, key leeway.TopologyKey) map[leeway.Domain]DomainStats {
	out := map[leeway.Domain]DomainStats{}
	for _, n := range nodes {
		d := leeway.NormaliseDomain(leeway.Domain(topologyValue(n.Labels, key)))
		s := out[d]
		s.Nodes++

		ready := false
		for _, c := range n.Status.Conditions {
			if c.Type == corev1.NodeReady {
				ready = c.Status == corev1.ConditionTrue
			}
		}
		if ready && !n.Spec.Unschedulable {
			s.Usable++
			s.AllocatableCPUMilli += n.Status.Allocatable.Cpu().MilliValue()
			s.AllocatableMemBytes += n.Status.Allocatable.Memory().Value()
		}
		out[d] = s
	}
	return out
}
