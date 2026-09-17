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
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// PinPredicate reports whether a pod cannot be moved — today, whether it is
// bound to a zonal volume.
//
// Phase 2 ships without one, so every pod is unpinned. That is not a stub left
// to be found later: deciding pinning means resolving each pod's PVCs to PVs
// and reading their node affinity, which is volume-pinning inference and
// belongs with the rest of intent inference in Phase 3. Until then the
// conservative answer is "not pinned", which counts a pinned pod as
// relocatable — and since Phase 2 emits nothing at all, no finding can be
// wrong because of it.
type PinPredicate func(*corev1.Pod) bool

// StateOptions configures a State.
type StateOptions struct {
	// Inventory is the node-side state. Required.
	Inventory *Inventory

	// Owners resolves a ReplicaSet to its Deployment. A nil lookup means
	// Deployment-owned pods resolve to no subject and go uncounted, which is
	// why the wiring always supplies one.
	Owners OwnerLookup

	// Pinned decides Placement.Pinned. Nil means never pinned.
	Pinned PinPredicate

	// Enqueue is called with each subject whose distribution moved. Nil
	// discards, which is what the counting tests use.
	Enqueue func(leeway.SubjectRef)
}

// State holds the pod-side indexes of §6.2 and applies the §6.3 delta rules.
//
// Everything here exists to avoid recomputing a distribution from the pod cache
// on every evaluation, which is O(pods) per subject and the cost §6 is written
// to avoid. What it buys in speed it owes in correctness: incremental counters
// are the kind of code that is right in tests and quietly wrong in production
// two months in, which is why §6.5's verifier is a shipped component and not a
// debugging aid.
//
// Safe for concurrent use. One mutex covers every index, because the delta
// rules read and write several of them per event and a finer-grained scheme
// would buy contention for correctness we would then have to prove. The
// critical sections are map operations on a handful of entries.
type State struct {
	mu sync.Mutex

	inv    *Inventory
	owners OwnerLookup
	pinned PinPredicate
	notify func(leeway.SubjectRef)

	// keys is the inventory's key set, held here because applyLocked walks it
	// twice per pod event and Inventory.Keys allocates a defensive copy. The
	// set is fixed for the inventory's life, so caching it cannot go stale.
	keys []leeway.TopologyKey

	// placements is podUID → where that pod was last counted. This index, not
	// the event's old object, is what deltas decrement from.
	placements map[types.UID]leeway.Placement

	// subjects is podUID → the subject it was counted under, cached at the
	// same moment. See OnPodDelete for why re-resolving instead would corrupt
	// the counts.
	subjects map[types.UID]leeway.SubjectRef

	// byNode is nodeName → the pods counted on it, for node-event fanout.
	byNode map[string]map[types.UID]struct{}

	// counts is the distributions themselves, per subject per axis.
	counts map[leeway.SubjectRef]map[leeway.TopologyKey]*leeway.Distribution
}

// NewState returns a State ready to receive deltas.
func NewState(opts StateOptions) *State {
	return &State{
		inv:        opts.Inventory,
		owners:     opts.Owners,
		pinned:     opts.Pinned,
		notify:     opts.Enqueue,
		keys:       opts.Inventory.Keys(),
		placements: make(map[types.UID]leeway.Placement),
		subjects:   make(map[types.UID]leeway.SubjectRef),
		byNode:     make(map[string]map[types.UID]struct{}),
		counts:     make(map[leeway.SubjectRef]map[leeway.TopologyKey]*leeway.Distribution),
	}
}

// OnPodAdd applies an add.
//
// It routes through the same path as an update rather than assuming the pod is
// new, because an informer add is not a guarantee of novelty: after a watch
// break the relist replays the whole cache as adds, and a handler that
// incremented unconditionally would double every count in the cluster on a
// dropped connection.
func (s *State) OnPodAdd(pod *corev1.Pod) {
	s.OnPodUpdate(nil, pod)
}

// OnPodUpdate applies an update, doing nothing at all when the placement has
// not moved.
//
// The early return is the load-bearing line in this file. Most pod updates are
// status churn — a restart count, a probe transition, a condition heartbeat —
// and none of it changes where the pod is. Under a shared informer leeway sees
// every event every other source sees, so filtering here is what decides
// whether the subsystem is affordable at all. Comparing derived placements
// rather than objects is what makes the filter cheap and, more importantly,
// correct: two objects can differ in a hundred fields and be the same
// placement.
//
// The previous object is ignored entirely, which is why the parameter is
// unnamed. See OnPodDelete.
func (s *State) OnPodUpdate(_, cur *corev1.Pod) {
	if cur == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	uid := cur.UID
	next, counted := s.resolvePlacement(cur)
	if !counted {
		// A pod that has stopped counting — it reached a terminal phase — is
		// removed rather than left at its last placement. A completed pod
		// occupies no domain, and leaving it counted would make a batch
		// namespace look permanently fuller than it is.
		s.uncount(uid)
		return
	}

	prev, known := s.placements[uid]
	if known && prev.Equal(next) {
		return
	}

	sub, ok := s.subjectOf(cur, uid)
	if !ok {
		s.uncount(uid)
		return
	}

	if known {
		s.applyLocked(sub, prev, -1)
		if prev.NodeName != next.NodeName {
			s.unlinkNode(prev.NodeName, uid)
		}
	}
	s.placements[uid] = next
	s.subjects[uid] = sub
	s.linkNode(next.NodeName, uid)
	s.applyLocked(sub, next, +1)
	s.enqueue(sub)
}

// OnPodDelete applies a delete, accepting the tombstone an informer hands over
// when it missed the deletion itself.
//
// The subject is taken from the cache rather than re-resolved from the object,
// which is a deliberate departure from §6.3's sketch. Re-resolving at delete
// time asks the owner chain a question it may no longer be able to answer: when
// a Deployment is deleted, its ReplicaSets and pods go with it, and whether the
// ReplicaSet lookup still succeeds depends on which delete event arrives first.
// A lookup that misses would decrement nothing and leave the subject's counts
// permanently high — until the next verifier pass, but the point of the delta
// rules is not to need one. It is the same argument as decrementing from the
// stored placement instead of the old object, applied to the other half of the
// key.
func (s *State) OnPodDelete(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		tomb, isTomb := obj.(cache.DeletedFinalStateUnknown)
		if !isTomb {
			return
		}
		if pod, ok = tomb.Obj.(*corev1.Pod); !ok {
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uncount(pod.UID)
}

// OnNodeUpsert applies a node event and fans out according to what changed.
//
// The two branches are asymmetric on purpose (§6.3). A topology relabel is
// narrow and expensive: the pods on this one node are now counted in the wrong
// domain, so each is re-mapped. An eligibility change is broad and cheap: it
// can move the eligible domain set for every subject in the cluster, and
// enqueuing twenty thousand subjects because one node went NotReady is the
// stampede the design is written to avoid, so it bumps the inventory generation
// and lets subjects re-evaluate lazily.
func (s *State) OnNodeUpsert(node *corev1.Node) Change {
	ch := s.inv.Upsert(node)
	if ch.DomainsChanged {
		// Including on an add. A new node is not an empty one as far as this
		// index is concerned: pod events routinely arrive before their node's,
		// and a node that was deleted and re-created keeps its name, so
		// byNode can already hold pods the moment the node appears. Skipping
		// the re-map here strands them in DomainUnknown until they next
		// change, which on a stable workload is never.
		s.remapNode(node.Name)
	}
	return ch
}

// OnNodeDelete applies a node deletion.
//
// The pods on a deleted node are re-mapped to DomainUnknown rather than
// dropped. Their own deletions are coming, but they are not here yet, and in
// the meantime the pods genuinely exist and are genuinely nowhere — which is a
// true statement about a cluster that just lost a node, and one that vanishing
// them would hide.
func (s *State) OnNodeDelete(obj any) Change {
	node, ok := obj.(*corev1.Node)
	if !ok {
		tomb, isTomb := obj.(cache.DeletedFinalStateUnknown)
		if !isTomb {
			return Change{}
		}
		if node, ok = tomb.Obj.(*corev1.Node); !ok {
			return Change{}
		}
	}
	ch := s.inv.Remove(node.Name)
	if ch.Removed {
		s.remapNode(node.Name)
	}
	return ch
}

// Snapshot returns a deep copy of one subject's distributions, or nil if the
// subject is not tracked.
func (s *State) Snapshot(sub leeway.SubjectRef) map[leeway.TopologyKey]*leeway.Distribution {
	s.mu.Lock()
	defer s.mu.Unlock()
	byKey, ok := s.counts[sub]
	if !ok {
		return nil
	}
	out := make(map[leeway.TopologyKey]*leeway.Distribution, len(byKey))
	for key, dist := range byKey {
		out[key] = dist.Clone()
	}
	return out
}

// Subjects returns every subject currently holding a count.
func (s *State) Subjects() []leeway.SubjectRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]leeway.SubjectRef, 0, len(s.counts))
	for sub := range s.counts {
		out = append(out, sub)
	}
	return out
}

// SubjectCounts tallies tracked subjects by kind.
//
// Separate from Subjects because this one is called on every metrics scrape:
// returning the whole subject slice to count it would allocate ~1 MiB per
// scrape at §6.6's baseline row, for a handful of integers.
func (s *State) SubjectCounts() map[leeway.SubjectKind]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[leeway.SubjectKind]int64, 4)
	for sub := range s.counts {
		out[sub.Kind]++
	}
	return out
}

// Len reports how many pods are counted.
func (s *State) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.placements)
}

// PlacementOf returns where a pod was last counted.
func (s *State) PlacementOf(uid types.UID) (leeway.Placement, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.placements[uid]
	return p, ok
}

// resolvePlacement derives where a pod counts, reporting false for one that
// should not count at all. Caller holds the lock: the inventory read is
// independently synchronised, but the tuple must match the placement stored
// under the same critical section.
func (s *State) resolvePlacement(pod *corev1.Pod) (leeway.Placement, bool) {
	state, counted := podState(pod)
	if !counted {
		return leeway.Placement{}, false
	}
	domains := s.inv.DomainsOf(pod.Spec.NodeName)
	if domains == nil {
		// Either the pod is unscheduled, or its node's event has not arrived
		// yet. Both are honestly "unknown on every axis" — and for the second,
		// the node's own add re-maps the pod when it lands, which is exactly
		// what remapNode is for.
		domains = s.inv.UnknownTuple()
	}
	pinned := false
	if s.pinned != nil {
		pinned = s.pinned(pod)
	}
	return leeway.Placement{
		NodeName: pod.Spec.NodeName,
		Domains:  domains,
		State:    state,
		Pinned:   pinned,
	}, true
}

// subjectOf returns a pod's subject, using the cached answer when there is one.
// Caller holds the lock.
func (s *State) subjectOf(pod *corev1.Pod, uid types.UID) (leeway.SubjectRef, bool) {
	if sub, ok := s.subjects[uid]; ok {
		return sub, true
	}
	return resolveSubject(pod, s.owners)
}

// uncount removes a pod from every index. Caller holds the lock.
func (s *State) uncount(uid types.UID) {
	p, known := s.placements[uid]
	if !known {
		return
	}
	sub, hadSubject := s.subjects[uid]
	s.applyLocked(sub, p, -1)
	delete(s.placements, uid)
	delete(s.subjects, uid)
	s.unlinkNode(p.NodeName, uid)
	if hadSubject {
		s.enqueue(sub)
	}
}

// applyLocked folds a placement into (sign +1) or out of (sign -1) its
// subject's distributions, on every configured axis. Caller holds the lock.
func (s *State) applyLocked(sub leeway.SubjectRef, p leeway.Placement, sign int) {
	byKey, ok := s.counts[sub]
	if !ok {
		if sign < 0 {
			return
		}
		byKey = make(map[leeway.TopologyKey]*leeway.Distribution)
		s.counts[sub] = byKey
	}
	for ordinal, key := range s.keys {
		dist, ok := byKey[key]
		if !ok {
			if sign < 0 {
				continue
			}
			dist = leeway.NewDistribution()
			byKey[key] = dist
		}
		if sign > 0 {
			dist.Add(p.Domain(ordinal), p.State, p.Pinned)
			continue
		}
		dist.Remove(p.Domain(ordinal), p.State, p.Pinned)
		if dist.Total == 0 {
			delete(byKey, key)
		}
	}
	// A subject with nothing left is forgotten. A Deployment that was deleted
	// months ago must not keep a map entry alive for the life of the process,
	// and an empty row would also make Subjects() — which the verifier shards
	// over — grow without bound on a cluster with heavy Job churn.
	if len(byKey) == 0 {
		delete(s.counts, sub)
	}
}

func (s *State) linkNode(nodeName string, uid types.UID) {
	set, ok := s.byNode[nodeName]
	if !ok {
		set = make(map[types.UID]struct{})
		s.byNode[nodeName] = set
	}
	set[uid] = struct{}{}
}

func (s *State) unlinkNode(nodeName string, uid types.UID) {
	set, ok := s.byNode[nodeName]
	if !ok {
		return
	}
	delete(set, uid)
	if len(set) == 0 {
		delete(s.byNode, nodeName)
	}
}

// remapNode re-counts every pod on a node whose domains moved.
//
// It never consults the pod objects. The stored placement already holds
// everything that is not changing — node name, state, pinning — and the new
// domain tuple comes from the inventory, so the new placement is the old one
// with one field replaced. That is not merely an optimisation over reaching for
// a lister: it means the re-map cannot disagree with what was counted, because
// it is derived from it.
func (s *State) remapNode(nodeName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pods := s.byNode[nodeName]
	if len(pods) == 0 {
		return
	}
	domains := s.inv.DomainsOf(nodeName)
	if domains == nil {
		domains = s.inv.UnknownTuple()
	}

	// Subjects are collected and notified once each. A 200-pod node usually
	// carries only a handful of distinct subjects, and enqueuing per pod would
	// hand the workqueue two hundred duplicates to dedupe under the lock.
	touched := make(map[leeway.SubjectRef]struct{})
	for uid := range pods {
		prev := s.placements[uid]
		next := prev
		next.Domains = domains
		if prev.Equal(next) {
			continue
		}
		sub := s.subjects[uid]
		s.applyLocked(sub, prev, -1)
		s.placements[uid] = next
		s.applyLocked(sub, next, +1)
		touched[sub] = struct{}{}
	}
	for sub := range touched {
		s.enqueue(sub)
	}
}

func (s *State) enqueue(sub leeway.SubjectRef) {
	if s.notify != nil {
		s.notify(sub)
	}
}

// podState maps a pod to the state it is counted under, reporting false for a
// pod that occupies no domain at all.
//
// Succeeded and Failed pods are both uncounted, which is a narrower rule than
// the §6.1 field selector that drops only Succeeded server-side. The two
// answer different questions: the selector keeps Failed pods in the shared
// cache because objectstate's eviction-burst detector reads exactly that phase,
// while leeway is asking where a workload's replicas are, and a failed pod is
// not a replica. Counting them would make a zone that has just evicted five
// hundred pods look like the most populated one in the cluster.
func podState(pod *corev1.Pod) (leeway.CountState, bool) {
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return 0, false
	}

	// Terminating outranks the phase: a pod with a deletion timestamp is on its
	// way out whatever its status says, and §7.6 needs those counted apart so
	// that a rollout is not mistaken for drift.
	if pod.DeletionTimestamp != nil {
		return leeway.StateTerminating, true
	}

	if pod.Status.Phase == corev1.PodPending {
		if unschedulable(pod) {
			return leeway.StateUnschedulable, true
		}
		return leeway.StatePending, true
	}

	// Running, and the legacy Unknown phase — a node the control plane has lost
	// contact with. The pod is still bound to it and still occupies that
	// domain; the node being unreachable is a fact about the node, which the
	// inventory already records, not about where the pod is.
	return leeway.StateRunning, true
}

// unschedulable reports the PodScheduled=False/Unschedulable condition the
// scheduler sets when it cannot place a pod.
func unschedulable(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled {
			return c.Status == corev1.ConditionFalse && c.Reason == corev1.PodReasonUnschedulable
		}
	}
	return false
}
