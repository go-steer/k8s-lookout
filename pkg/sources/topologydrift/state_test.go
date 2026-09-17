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
	"math/rand/v2"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

type podOpt func(*corev1.Pod)

func pod(name, namespace, nodeName string, opts ...podOpt) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(namespace + "/" + name),
		},
		Spec:   corev1.PodSpec{NodeName: nodeName},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

func phase(p corev1.PodPhase) podOpt {
	return func(pod *corev1.Pod) { pod.Status.Phase = p }
}

func terminating() podOpt {
	return func(pod *corev1.Pod) {
		now := metav1.Now()
		pod.DeletionTimestamp = &now
	}
}

func unschedulablePod() podOpt {
	return func(pod *corev1.Pod) {
		pod.Status.Phase = corev1.PodPending
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:   corev1.PodScheduled,
			Status: corev1.ConditionFalse,
			Reason: corev1.PodReasonUnschedulable,
		})
	}
}

// harness is a State plus the inventory behind it and a record of what was
// enqueued.
type harness struct {
	*State
	inv      *Inventory
	enqueued []leeway.SubjectRef
	mu       sync.Mutex
}

func newHarness(t *testing.T, keys ...leeway.TopologyKey) *harness {
	t.Helper()
	if len(keys) == 0 {
		keys = []leeway.TopologyKey{zoneKey}
	}
	h := &harness{inv: NewInventory(keys)}
	h.State = NewState(StateOptions{
		Inventory: h.inv,
		Owners:    staticOwners(map[string][2]string{"prod/ReplicaSet/web-7c9f": {"Deployment", "web"}}),
		Enqueue: func(sub leeway.SubjectRef) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.enqueued = append(h.enqueued, sub)
		},
	})
	return h
}

func (h *harness) drain() []leeway.SubjectRef {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := h.enqueued
	h.enqueued = nil
	return out
}

// counts returns the per-domain running/pending/etc totals for a subject on the
// zone axis, flattened to totals for terse assertions.
func (h *harness) totals(sub leeway.SubjectRef) map[leeway.Domain]int64 {
	snap := h.Snapshot(sub)
	dist, ok := snap[zoneKey]
	if !ok {
		return nil
	}
	out := map[leeway.Domain]int64{}
	for dom, c := range dist.ByDomain {
		out[dom] = c.Total()
	}
	return out
}

var webSubject = leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "web"}

func webPod(name, nodeName string, opts ...podOpt) *corev1.Pod {
	return pod(name, "prod", nodeName, append([]podOpt{ownedBy("ReplicaSet", "web-7c9f")}, opts...)...)
}

func TestState_AddCountsAndEnqueues(t *testing.T) {
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	h.OnPodAdd(webPod("web-1", "n1"))

	if got := h.totals(webSubject); got[leeway.Domain("zone-a")] != 1 {
		t.Errorf("totals = %v, want one pod in zone-a", got)
	}
	if got := h.drain(); len(got) != 1 || got[0] != webSubject {
		t.Errorf("enqueued %v, want one %v", got, webSubject)
	}
	if h.Len() != 1 {
		t.Errorf("Len = %d, want 1", h.Len())
	}
}

func TestState_InertUpdateDoesNothing(t *testing.T) {
	// The line the whole subsystem's affordability rests on: leeway sees every
	// pod event every other source sees, and almost none of them move a pod.
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	h.OnPodAdd(webPod("web-1", "n1"))
	h.drain()

	churned := webPod("web-1", "n1")
	churned.ResourceVersion = "12345"
	churned.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", RestartCount: 7}}
	churned.Status.Conditions = append(churned.Status.Conditions, corev1.PodCondition{
		Type: corev1.PodReady, Status: corev1.ConditionTrue,
	})
	h.OnPodUpdate(nil, churned)

	if got := h.drain(); len(got) != 0 {
		t.Errorf("status churn enqueued %v", got)
	}
	if got := h.totals(webSubject); got[leeway.Domain("zone-a")] != 1 {
		t.Errorf("totals = %v, want one pod still in zone-a", got)
	}
}

func TestState_RelistDoesNotDoubleCount(t *testing.T) {
	// After a watch break the informer replays its whole cache as adds. A
	// handler that incremented unconditionally would double every count in the
	// cluster on a dropped connection.
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	p := webPod("web-1", "n1")

	for range 3 {
		h.OnPodAdd(p)
	}
	if got := h.totals(webSubject); got[leeway.Domain("zone-a")] != 1 {
		t.Errorf("totals = %v after three adds, want one pod", got)
	}
	if h.Len() != 1 {
		t.Errorf("Len = %d, want 1", h.Len())
	}
}

func TestState_PodMovesBetweenZones(t *testing.T) {
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	h.OnNodeUpsert(node("n2", "zone-b"))
	h.OnPodAdd(webPod("web-1", "n1"))

	h.OnPodUpdate(nil, webPod("web-1", "n2"))

	got := h.totals(webSubject)
	if got[leeway.Domain("zone-b")] != 1 {
		t.Errorf("totals = %v, want the pod in zone-b", got)
	}
	// The emptied domain is deleted rather than kept as a zero row, so a
	// long-lived subject cannot accumulate a row per domain it ever touched.
	if _, present := got[leeway.Domain("zone-a")]; present {
		t.Errorf("zone-a survived as a zero row: %v", got)
	}
}

func TestState_DecrementUsesTheStoredPlacementNotTheObject(t *testing.T) {
	// The §6.3 decision, tested against the case that motivates it: after a
	// missed event the object handed to the delete says the pod was somewhere
	// it was never counted. Decrementing from the object would leave zone-a
	// permanently one high and zone-b permanently one low.
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	h.OnNodeUpsert(node("n2", "zone-b"))
	h.OnPodAdd(webPod("web-1", "n1"))

	h.OnPodDelete(webPod("web-1", "n2"))

	if got := h.totals(webSubject); len(got) != 0 {
		t.Errorf("totals = %v, want everything back to zero", got)
	}
	if h.Len() != 0 {
		t.Errorf("Len = %d, want 0", h.Len())
	}
}

func TestState_DeleteUsesTheCachedSubject(t *testing.T) {
	// A Deployment's deletion takes its ReplicaSets with it, and the order the
	// events arrive in is not ours to choose. Re-resolving the owner chain here
	// would miss and decrement nothing, leaving the subject permanently high.
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	table := map[string][2]string{"prod/ReplicaSet/web-7c9f": {"Deployment", "web"}}
	s := NewState(StateOptions{
		Inventory: inv,
		Owners:    staticOwners(table),
	})
	inv.Upsert(node("n1", "zone-a"))
	s.OnPodAdd(webPod("web-1", "n1"))

	// The ReplicaSet goes first.
	delete(table, "prod/ReplicaSet/web-7c9f")
	s.OnPodDelete(webPod("web-1", "n1"))

	if snap := s.Snapshot(webSubject); snap != nil {
		t.Errorf("subject still holds counts after its pod was deleted: %v", snap)
	}
	if len(s.Subjects()) != 0 {
		t.Errorf("Subjects = %v, want none", s.Subjects())
	}
}

func TestState_DeleteViaTombstone(t *testing.T) {
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	h.OnPodAdd(webPod("web-1", "n1"))

	h.OnPodDelete(cache.DeletedFinalStateUnknown{Key: "prod/web-1", Obj: webPod("web-1", "n1")})

	if h.Len() != 0 {
		t.Errorf("Len = %d after a tombstone delete, want 0", h.Len())
	}
}

func TestState_DeleteIgnoresRubbish(t *testing.T) {
	h := newHarness(t)
	h.OnPodDelete("not a pod")
	h.OnPodDelete(cache.DeletedFinalStateUnknown{Key: "x", Obj: "not a pod either"})
	h.OnNodeDelete(42)
	h.OnNodeDelete(cache.DeletedFinalStateUnknown{Key: "x", Obj: &corev1.Pod{}})
	h.OnPodUpdate(nil, nil)

	if h.Len() != 0 {
		t.Error("rubbish entered the state")
	}
}

func TestState_DeleteOfAnUnknownPodIsANoOp(t *testing.T) {
	h := newHarness(t)
	h.OnPodDelete(webPod("never-seen", "n1"))
	if got := h.drain(); len(got) != 0 {
		t.Errorf("deleting an unknown pod enqueued %v", got)
	}
}

func TestState_CountStates(t *testing.T) {
	tests := []struct {
		name    string
		opts    []podOpt
		want    leeway.CountState
		counted bool
	}{
		{"running", nil, leeway.StateRunning, true},
		{"pending", []podOpt{phase(corev1.PodPending)}, leeway.StatePending, true},
		{"unschedulable", []podOpt{unschedulablePod()}, leeway.StateUnschedulable, true},
		{
			// Pending with a PodScheduled condition that is not the
			// unschedulable one — the scheduler has bound it and the kubelet
			// has not started it yet.
			"pending but scheduled",
			[]podOpt{phase(corev1.PodPending), func(p *corev1.Pod) {
				p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
					Type: corev1.PodScheduled, Status: corev1.ConditionTrue,
				})
			}},
			leeway.StatePending, true,
		},
		{"terminating", []podOpt{terminating()}, leeway.StateTerminating, true},
		{
			// Terminating outranks the phase: §7.6 needs these apart so a
			// rollout is not read as drift.
			"terminating outranks pending",
			[]podOpt{phase(corev1.PodPending), terminating()},
			leeway.StateTerminating, true,
		},
		{
			// Still bound to its node and still occupying that domain. The
			// node being unreachable is a fact the inventory records.
			"unknown phase counts as running",
			[]podOpt{phase(corev1.PodUnknown)},
			leeway.StateRunning, true,
		},
		{"succeeded is not counted", []podOpt{phase(corev1.PodSucceeded)}, 0, false},
		{"failed is not counted", []podOpt{phase(corev1.PodFailed)}, 0, false},
		{
			// A terminal phase wins over the deletion timestamp: the pod is
			// finished, not going.
			"succeeded while terminating is not counted",
			[]podOpt{phase(corev1.PodSucceeded), terminating()},
			0, false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, counted := podState(pod("p", "prod", "n1", tc.opts...))
			if counted != tc.counted {
				t.Fatalf("counted = %v, want %v", counted, tc.counted)
			}
			if counted && got != tc.want {
				t.Errorf("state = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestState_TerminalPodIsUncounted(t *testing.T) {
	// A Job's pods complete rather than being deleted. Leaving them counted
	// would make a batch namespace look permanently fuller than it is.
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	h.OnPodAdd(webPod("web-1", "n1"))
	h.drain()

	h.OnPodUpdate(nil, webPod("web-1", "n1", phase(corev1.PodSucceeded)))

	if h.Len() != 0 {
		t.Errorf("Len = %d, want the completed pod gone", h.Len())
	}
	if got := h.totals(webSubject); len(got) != 0 {
		t.Errorf("totals = %v, want empty", got)
	}
	if got := h.drain(); len(got) != 1 {
		t.Errorf("completing a pod enqueued %v, want one notification", got)
	}
}

func TestState_StateChangeIsCountedSeparately(t *testing.T) {
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	h.OnPodAdd(webPod("web-1", "n1"))

	h.OnPodUpdate(nil, webPod("web-1", "n1", terminating()))

	dist := h.Snapshot(webSubject)[zoneKey]
	c := dist.ByDomain["zone-a"]
	if c.Running != 0 || c.Terminating != 1 {
		t.Errorf("counts = %+v, want 0 running / 1 terminating", *c)
	}
	if dist.Total != 1 {
		t.Errorf("Total = %d, want 1 — the pod moved state, it did not multiply", dist.Total)
	}
}

func TestState_UncountedPodsAreNotTracked(t *testing.T) {
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))

	h.OnPodAdd(pod("bare", "prod", "n1"))                               // no owner
	h.OnPodAdd(pod("crd", "prod", "n1", ownedBy("FooSet", "foo")))      // unsupported kind
	h.OnPodAdd(webPod("done", "n1", phase(corev1.PodSucceeded)))        // terminal
	h.OnPodAdd(pod("orphan", "prod", "n1", ownedBy("ReplicaSet", "?"))) // unresolvable

	if h.Len() != 0 {
		t.Errorf("Len = %d, want 0", h.Len())
	}
	if got := h.drain(); len(got) != 0 {
		t.Errorf("uncounted pods enqueued %v", got)
	}
}

func TestState_PodOnAnUnseenNodeIsUnknownThenRemapped(t *testing.T) {
	// Pod events routinely beat their node's. Recording DomainUnknown is
	// honest, and the node's own add is what fixes it.
	h := newHarness(t)
	h.OnPodAdd(webPod("web-1", "n1"))

	if got := h.totals(webSubject); got[leeway.DomainUnknown] != 1 {
		t.Fatalf("totals = %v, want the pod in the unknown domain", got)
	}

	h.OnNodeUpsert(node("n1", "zone-a"))

	got := h.totals(webSubject)
	if got[leeway.Domain("zone-a")] != 1 {
		t.Errorf("totals = %v, want the pod re-mapped into zone-a", got)
	}
	if _, present := got[leeway.DomainUnknown]; present {
		t.Errorf("the pod is still counted as unknown: %v", got)
	}
}

func TestState_UnscheduledPodIsUnknown(t *testing.T) {
	h := newHarness(t)
	h.OnPodAdd(webPod("web-1", "", phase(corev1.PodPending)))

	dist := h.Snapshot(webSubject)[zoneKey]
	if c := dist.ByDomain[leeway.DomainUnknown]; c == nil || c.Pending != 1 {
		t.Errorf("counts = %+v, want one pending pod in the unknown domain", dist.ByDomain)
	}
}

func TestState_NodeRelabelRemapsItsPods(t *testing.T) {
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	for i := range 3 {
		h.OnPodAdd(webPod(fmt.Sprintf("web-%d", i), "n1"))
	}
	h.OnPodAdd(pod("db-0", "prod", "n1", ownedBy("StatefulSet", "db")))
	h.drain()

	h.OnNodeUpsert(node("n1", "zone-b"))

	if got := h.totals(webSubject); got[leeway.Domain("zone-b")] != 3 {
		t.Errorf("totals = %v, want three pods in zone-b", got)
	}
	// One notification per affected subject, not one per pod: a 200-pod node
	// usually carries a handful of subjects, and the workqueue should not be
	// handed two hundred duplicates under the lock.
	if got := h.drain(); len(got) != 2 {
		t.Errorf("relabel enqueued %d subjects, want 2 (the Deployment and the StatefulSet)", len(got))
	}
}

func TestState_NodeDeleteLeavesItsPodsUnknown(t *testing.T) {
	// The pods' own deletions are coming but are not here yet, and in the
	// meantime they genuinely exist and are genuinely nowhere.
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	h.OnPodAdd(webPod("web-1", "n1"))

	h.OnNodeDelete(node("n1", "zone-a"))

	if got := h.totals(webSubject); got[leeway.DomainUnknown] != 1 {
		t.Errorf("totals = %v, want the pod in the unknown domain", got)
	}
	// And then the pod's delete lands.
	h.OnPodDelete(webPod("web-1", "n1"))
	if h.Len() != 0 {
		t.Errorf("Len = %d, want 0", h.Len())
	}
}

func TestState_NodeDeleteViaTombstone(t *testing.T) {
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	h.OnPodAdd(webPod("web-1", "n1"))

	h.OnNodeDelete(cache.DeletedFinalStateUnknown{Key: "n1", Obj: node("n1", "zone-a")})

	if got := h.totals(webSubject); got[leeway.DomainUnknown] != 1 {
		t.Errorf("totals = %v, want the pod in the unknown domain", got)
	}
}

func TestState_InertNodeUpdateDoesNotRemap(t *testing.T) {
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	h.OnPodAdd(webPod("web-1", "n1"))
	h.drain()

	// Cordoning changes eligibility, not domains, so no pod is re-mapped and
	// nothing is enqueued — the generation bump is what re-evaluation hangs
	// off, and that is a cheap read rather than 20,000 workqueue entries.
	ch := h.OnNodeUpsert(node("n1", "zone-a", cordoned()))
	if !ch.EligibilityChanged || ch.DomainsChanged {
		t.Fatalf("Change = %+v, want eligibility only", ch)
	}
	if got := h.drain(); len(got) != 0 {
		t.Errorf("a cordon enqueued %v", got)
	}
}

func TestState_MultipleAxes(t *testing.T) {
	h := newHarness(t, zoneKey, poolKey)
	h.OnNodeUpsert(node("n1", "zone-a", withLabel(string(poolKey), "pool-1")))
	h.OnNodeUpsert(node("n2", "zone-a", withLabel(string(poolKey), "pool-2")))
	h.OnPodAdd(webPod("web-1", "n1"))
	h.OnPodAdd(webPod("web-2", "n2"))

	snap := h.Snapshot(webSubject)
	if got := snap[zoneKey].ByDomain["zone-a"].Total(); got != 2 {
		t.Errorf("zone-a holds %d, want both pods", got)
	}
	if got := snap[poolKey].ByDomain["pool-1"].Total(); got != 1 {
		t.Errorf("pool-1 holds %d, want 1", got)
	}
	if got := snap[poolKey].ByDomain["pool-2"].Total(); got != 1 {
		t.Errorf("pool-2 holds %d, want 1", got)
	}
}

func TestState_SnapshotIsADeepCopy(t *testing.T) {
	// The verifier compares a snapshot against counts that are still moving
	// underneath it.
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	h.OnPodAdd(webPod("web-1", "n1"))

	snap := h.Snapshot(webSubject)
	h.OnPodAdd(webPod("web-2", "n1"))

	if got := snap[zoneKey].ByDomain["zone-a"].Running; got != 1 {
		t.Errorf("the snapshot moved with the state: running = %d, want 1", got)
	}
	if h.Snapshot(leeway.SubjectRef{Kind: leeway.SubjectDeployment, Name: "nope"}) != nil {
		t.Error("Snapshot invented a subject")
	}
}

func TestState_PlacementOf(t *testing.T) {
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	p := webPod("web-1", "n1")
	h.OnPodAdd(p)

	got, ok := h.PlacementOf(p.UID)
	if !ok || got.NodeName != "n1" || got.Domain(0) != "zone-a" || got.State != leeway.StateRunning {
		t.Errorf("PlacementOf = %+v, %v", got, ok)
	}
	if _, ok := h.PlacementOf("no-such-uid"); ok {
		t.Error("PlacementOf invented a placement")
	}
}

func TestState_PinPredicate(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	s := NewState(StateOptions{
		Inventory: inv,
		Owners:    staticOwners(map[string][2]string{"prod/ReplicaSet/web-7c9f": {"Deployment", "web"}}),
		Pinned:    func(p *corev1.Pod) bool { return p.Name == "web-1" },
	})
	inv.Upsert(node("n1", "zone-a"))
	s.OnPodAdd(webPod("web-1", "n1"))
	s.OnPodAdd(webPod("web-2", "n1"))

	c := s.Snapshot(webSubject)[zoneKey].ByDomain["zone-a"]
	if c.Running != 2 || c.Pinned != 1 {
		t.Errorf("counts = %+v, want 2 running / 1 pinned", *c)
	}

	// Pinned overlaps the state counters rather than sitting beside them, so
	// the decrement has to mirror the increment exactly or the count drifts a
	// little on every move.
	s.OnPodDelete(webPod("web-1", "n1"))
	c = s.Snapshot(webSubject)[zoneKey].ByDomain["zone-a"]
	if c.Running != 1 || c.Pinned != 0 {
		t.Errorf("counts = %+v after deleting the pinned pod, want 1 running / 0 pinned", *c)
	}
}

// TestState_CountsMatchARecountAfterChurn is Phase 2's exit criterion: counters
// provably correct under a property test at 10k pods. It drives ten thousand
// mixed events through the delta rules — adds, moves, state transitions,
// completions, deletions and node relabels — and then compares every subject's
// incremental distribution against a recount over the pods that are still
// live, which is what §6.5's verifier does in production.
func TestState_CountsMatchARecountAfterChurn(t *testing.T) {
	const (
		pods     = 10_000
		nodes    = 200
		subjects = 40
		events   = 60_000
	)

	h := newHarness(t, zoneKey, poolKey)
	owners := map[string][2]string{}
	for i := range subjects {
		owners[fmt.Sprintf("prod/ReplicaSet/rs-%d", i)] = [2]string{"Deployment", fmt.Sprintf("dep-%d", i)}
	}
	h.State = NewState(StateOptions{Inventory: h.inv, Owners: staticOwners(owners)})

	for i := range nodes {
		h.OnNodeUpsert(node(fmt.Sprintf("n%03d", i), fmt.Sprintf("zone-%d", i%4),
			withLabel(string(poolKey), fmt.Sprintf("pool-%d", i%3))))
	}

	// live is the ground truth: the pod objects a lister would return.
	live := map[types.UID]*corev1.Pod{}
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // deterministic by design

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

	for step := range events {
		switch rng.IntN(10) {
		case 0, 1, 2, 3, 4:
			p := makePod(rng.IntN(pods))
			h.OnPodUpdate(nil, p)
			live[p.UID] = p
		case 5:
			p := makePod(rng.IntN(pods))
			h.OnPodAdd(p)
			live[p.UID] = p
		case 6:
			// Completion: the pod is still in the cache but occupies nothing.
			p := makePod(rng.IntN(pods))
			phase(corev1.PodSucceeded)(p)
			h.OnPodUpdate(nil, p)
			live[p.UID] = p
		case 7:
			p := makePod(rng.IntN(pods))
			h.OnPodDelete(p)
			delete(live, p.UID)
		case 8:
			// A relist replaying an object already counted.
			for uid := range live {
				h.OnPodAdd(live[uid])
				break
			}
		default:
			if step%3 == 0 {
				h.OnNodeUpsert(node(fmt.Sprintf("n%03d", rng.IntN(nodes)),
					fmt.Sprintf("zone-%d", rng.IntN(5)),
					withLabel(string(poolKey), fmt.Sprintf("pool-%d", rng.IntN(4)))))
			} else {
				h.OnNodeDelete(node(fmt.Sprintf("n%03d", rng.IntN(nodes)), ""))
			}
		}
	}

	want := recountPods(h.inv, live, staticOwners(owners))
	got := map[leeway.SubjectRef]map[leeway.TopologyKey]*leeway.Distribution{}
	for _, sub := range h.Subjects() {
		got[sub] = h.Snapshot(sub)
	}

	if len(got) != len(want) {
		t.Fatalf("%d subjects counted, recount says %d", len(got), len(want))
	}
	for sub, wantByKey := range want {
		gotByKey, ok := got[sub]
		if !ok {
			t.Fatalf("%s: missing from the incremental state", sub)
		}
		for key, wantDist := range wantByKey {
			if !wantDist.Equal(gotByKey[key]) {
				t.Errorf("%s on %s:\n  incremental %s\n  recount     %s",
					sub, key, render(gotByKey[key]), render(wantDist))
			}
		}
	}
}

// recountPods rebuilds every subject's distributions from scratch, the way
// §6.5's verifier will.
func recountPods(inv *Inventory, pods map[types.UID]*corev1.Pod, owners OwnerLookup) map[leeway.SubjectRef]map[leeway.TopologyKey]*leeway.Distribution {
	out := map[leeway.SubjectRef]map[leeway.TopologyKey]*leeway.Distribution{}
	keys := inv.Keys()
	for _, p := range pods {
		state, counted := podState(p)
		if !counted {
			continue
		}
		sub, ok := resolveSubject(p, owners)
		if !ok {
			continue
		}
		domains := inv.DomainsOf(p.Spec.NodeName)
		byKey, ok := out[sub]
		if !ok {
			byKey = map[leeway.TopologyKey]*leeway.Distribution{}
			out[sub] = byKey
		}
		for ordinal, key := range keys {
			dist, ok := byKey[key]
			if !ok {
				dist = leeway.NewDistribution()
				byKey[key] = dist
			}
			domain := leeway.DomainUnknown
			if ordinal < len(domains) {
				domain = domains[ordinal]
			}
			dist.Add(domain, state, false)
		}
	}
	return out
}

func render(d *leeway.Distribution) string {
	if d == nil {
		return "<absent>"
	}
	out := fmt.Sprintf("total=%d", d.Total)
	for _, dom := range d.Domains() {
		out += fmt.Sprintf(" %s=%+v", dom, *d.ByDomain[dom])
	}
	return out
}

// TestState_DecrementGuards exercises the two guards no event sequence can
// reach — a decrement against a subject or axis that holds no counts, and an
// unlink from a node with no pod set. Both exist so that a bookkeeping slip
// degrades into a count the §6.5 verifier repairs rather than a negative number
// every downstream score inherits, or a panic in an informer handler. Testing
// them directly is the only way to know they work.
func TestState_DecrementGuards(t *testing.T) {
	// A plain State rather than the harness, so that reaching past the public
	// surface into the lock and the indexes reads as the deliberate thing it is.
	inv := NewInventory([]leeway.TopologyKey{zoneKey, poolKey})
	s := NewState(StateOptions{
		Inventory: inv,
		Owners:    staticOwners(map[string][2]string{"prod/ReplicaSet/web-7c9f": {"Deployment", "web"}}),
	})

	s.mu.Lock()
	s.applyLocked(webSubject, leeway.Placement{NodeName: "n1", Domains: []leeway.Domain{"zone-a", "pool-1"}}, -1)
	s.mu.Unlock()
	if got := s.Subjects(); len(got) != 0 {
		t.Errorf("a decrement conjured a subject: %v", got)
	}

	// The subject exists but one axis has already been emptied.
	s.OnNodeUpsert(node("n1", "zone-a", withLabel(string(poolKey), "pool-1")))
	s.OnPodAdd(webPod("web-1", "n1"))
	s.mu.Lock()
	delete(s.counts[webSubject], poolKey)
	s.applyLocked(webSubject, s.placements["prod/web-1"], -1)
	s.mu.Unlock()
	if got := s.Subjects(); len(got) != 0 {
		t.Errorf("Subjects = %v, want the emptied subject forgotten", got)
	}

	s.mu.Lock()
	s.unlinkNode("a-node-with-no-pods", "prod/web-1")
	s.mu.Unlock()
}

func TestState_ConcurrentPodAndNodeEvents(t *testing.T) {
	// Under a shared factory the Pod and Node handlers run on different
	// goroutines against the same indexes. Run under -race.
	h := newHarness(t)
	for i := range 20 {
		h.OnNodeUpsert(node(fmt.Sprintf("n%02d", i), fmt.Sprintf("zone-%d", i%3)))
	}

	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				name := fmt.Sprintf("n%02d", i%20)
				switch w {
				case 0:
					h.OnPodAdd(webPod(fmt.Sprintf("web-%d", i%100), name))
				case 1:
					h.OnPodDelete(webPod(fmt.Sprintf("web-%d", i%100), name))
				case 2:
					h.OnNodeUpsert(node(name, fmt.Sprintf("zone-%d", i%4)))
				default:
					_ = h.Snapshot(webSubject)
					_ = h.Subjects()
					_ = h.Len()
				}
			}
		}()
	}
	wg.Wait()
}
