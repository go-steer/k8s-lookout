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
	"hash/fnv"
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

	// groups is FR-3's node-group subjects: the same shape as counts and read
	// by the same accessors, but derived rather than accumulated.
	//
	// A second map rather than more rows in counts, because everything around
	// counts is pod machinery that would be wrong here. The §6.3 delta rules
	// key off placements and byNode, which a node group has neither of;
	// §6.5's verifier rebuilds a subject from the pod cache and would find a
	// node group empty, report it as counter drift and Repair it to nothing;
	// and the sweep enqueues Subjects() onto the coalescer, which resolves
	// intent from a representative pod. So the two live side by side:
	// tracked, Snapshot and SubjectCounts answer for both, Subjects() stays
	// pod-only and Groups() is the other half.
	//
	// Replaced wholesale by SetGroups rather than maintained incrementally.
	// A node group is a property of the node inventory, which is small,
	// already indexed and re-derivable in one pass — the cost the delta rules
	// exist to avoid is O(pods), and this is O(nodes) on a timer.
	groups map[leeway.SubjectRef]map[leeway.TopologyKey]*leeway.Distribution

	// representatives is subject → one of its pods, by UID and name.
	//
	// Intent is inferred from an *admitted pod* and never from a controller's
	// PodTemplateSpec (§13 S3: GKE injects tolerations at admission that the
	// template does not carry, and inferring from the weaker document computes
	// too small an eligible set, concludes the workload is pinned and
	// suppresses real drift). Evaluation is per subject, so something has to
	// get from a subject back to a pod, and this is it.
	//
	// It holds a name, not a pod: §6.6.2 budgets this process to absorb a
	// ~66k-pod reschedule, and retaining pod objects here would put the whole
	// cache in a second place. The cost is one string per *subject* — thousands,
	// not hundreds of thousands — and a lister Get on the read side.
	//
	// It is a hint and not an index. The entry is overwritten by every counted
	// pod event, so it converges on the most recently admitted pod, which is
	// the freshest spec; it is dropped when that pod is uncounted; and a reader
	// that misses re-elects. Nothing downstream may assume it is populated.
	representatives map[leeway.SubjectRef]representative

	// intents is subject → the intent set its last evaluation resolved (§5.1),
	// one entry per axis that expressed one.
	//
	// It lives here rather than on the Source because the eviction rule it
	// needs is the one this file already runs: a subject is forgotten the
	// moment its last pod stops counting, and a resolved intent that outlived
	// its subject is a metric series for a Deployment that was deleted months
	// ago. One owner, one lock, one lifetime.
	intents map[leeway.SubjectRef]map[leeway.TopologyKey]*leeway.Intent

	// evaluations is subject → what its last evaluation scored (§7.3), one
	// entry per axis, in canonical key order.
	//
	// Here for the same reason as intents, and with the same lifetime: a
	// drift figure for a Deployment that was deleted an hour ago is a series
	// that never goes away on its own. It is read by the scrape callback and
	// written once per evaluation; it is not the path a finding is built on,
	// which uses the live Evaluation rather than reading it back out.
	evaluations map[leeway.SubjectRef][]Evaluation
}

// representative identifies the pod a subject's intent is read from.
//
// The UID is carried alongside the name so that uncount can tell "the pod this
// hint names has gone" from "a pod with the same name was recreated", which is
// not a hypothetical: a StatefulSet replaces web-0 with a new object under the
// old name, and dropping the hint on that event would send the next evaluation
// down the un-indexed list path for no reason.
type representative struct {
	uid  types.UID
	name string
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
		groups:     make(map[leeway.SubjectRef]map[leeway.TopologyKey]*leeway.Distribution),

		representatives: make(map[leeway.SubjectRef]representative),
		intents:         make(map[leeway.SubjectRef]map[leeway.TopologyKey]*leeway.Intent),
		evaluations:     make(map[leeway.SubjectRef][]Evaluation),
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
	// After applyLocked, never before: that call is what creates the subject's
	// row, and the hint is evicted with that row, so setting it for a subject
	// applyLocked declined to track would leak an entry nothing ever removes.
	if s.tracked(sub) {
		s.representatives[sub] = representative{uid: uid, name: cur.Name}
	}
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
		if byKey, ok = s.groups[sub]; !ok {
			return nil
		}
	}
	out := make(map[leeway.TopologyKey]*leeway.Distribution, len(byKey))
	for key, dist := range byKey {
		out[key] = dist.Clone()
	}
	return out
}

// Subjects returns every *pod* subject currently holding a count.
//
// Node groups are deliberately absent, and the two callers are why. The sweep
// re-enqueues this list onto the §6.4 coalescer, whose callback resolves
// intent from a representative pod; and §6.5's verifier shards over
// SubjectsInShard and rebuilds each subject from the pod cache, which for a
// node group would find nothing, call it counter drift and repair it away.
// See Groups for the other half, and groups for why they are separate.
func (s *State) Subjects() []leeway.SubjectRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]leeway.SubjectRef, 0, len(s.counts))
	for sub := range s.counts {
		out = append(out, sub)
	}
	return out
}

// Groups returns every node-group subject currently registered (FR-3).
func (s *State) Groups() []leeway.SubjectRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]leeway.SubjectRef, 0, len(s.groups))
	for sub := range s.groups {
		out = append(out, sub)
	}
	return out
}

// SetGroups replaces the node-group registry with a freshly derived one.
//
// Wholesale rather than merged, because the input is a complete re-derivation
// from the node inventory: a group that is absent from it has no nodes left,
// and merging would keep a decommissioned pool's last distribution — and its
// evaluations, and its metric series — for the life of the process. Departed
// groups are forgotten the same way a subject whose last pod went is, so an
// episode open against a pool that no longer exists resolves as
// object_deleted on the next pass rather than sitting firing forever.
//
// The distributions are adopted, not copied. The caller derived them and must
// not retain them.
func (s *State) SetGroups(groups map[leeway.SubjectRef]map[leeway.TopologyKey]*leeway.Distribution) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for sub := range s.groups {
		if _, still := groups[sub]; !still {
			s.forgetLocked(sub)
		}
	}
	if groups == nil {
		groups = make(map[leeway.SubjectRef]map[leeway.TopologyKey]*leeway.Distribution)
	}
	s.groups = groups
}

// Tracked reports whether this subject currently holds a count.
//
// It is the clearance observer's "does this workload still exist" test (§7.4),
// and it answers from the same index every other question here answers from
// rather than from a lister: an incident whose subject left the pod cache is
// one whose objects are gone, which is what object_deleted means.
func (s *State) Tracked(sub leeway.SubjectRef) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tracked(sub)
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
	for sub := range s.groups {
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

// SubjectOfPod returns the subject a pod was counted under.
//
// This is the index a caller holding pod UIDs from somewhere else — the
// `capacity` source's refused-pod table, in §8.5's case — uses to find out
// which of them belong to the workload it is asking about. Answering from
// here rather than re-reading the pod's owner references is the whole point:
// two subsystems that resolve ownership separately will eventually disagree,
// and this source's counts are already keyed on the answer it reached.
//
// False means the pod is not counted, which includes a pod this source has
// deliberately excluded (Succeeded, Failed) as well as one it has not seen.
func (s *State) SubjectOfPod(uid types.UID) (leeway.SubjectRef, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subjects[uid]
	return sub, ok
}

// PlacementOf returns where a pod was last counted.
func (s *State) PlacementOf(uid types.UID) (leeway.Placement, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.placements[uid]
	return p, ok
}

// Representative returns the name of a pod belonging to sub, in sub's
// namespace, for intent inference to read.
//
// False means "no hint", not "no such pod": the subject may be untracked, or
// the pod the hint named may have been the one that just went away. A true
// answer is not a guarantee either — the name is from an index this package
// maintains, and the pod informer's cache is a different index that may have
// moved on. Callers must handle a lister miss, and re-elect through
// SetRepresentative when they do.
func (s *State) Representative(sub leeway.SubjectRef) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rep, ok := s.representatives[sub]
	return rep.name, ok
}

// SetRepresentative records a pod as sub's representative, which is how a
// reader that found one the hard way stops the next evaluation paying for it
// again.
//
// Ignored for a subject that holds no counts. The hint is evicted with the
// counts, so accepting one without them is how the map would grow without
// bound on a cluster with heavy Job churn.
func (s *State) SetRepresentative(sub leeway.SubjectRef, uid types.UID, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.tracked(sub) {
		return
	}
	s.representatives[sub] = representative{uid: uid, name: name}
}

// SetIntents records what a subject's evaluation resolved (§5.1). A nil or
// empty set clears the entry, so a workload that drops its spread constraints
// stops exporting intent for them on the next evaluation rather than at the
// next restart.
//
// Ignored for an untracked subject, for the same reason as SetRepresentative.
func (s *State) SetIntents(sub leeway.SubjectRef, intents map[leeway.TopologyKey]*leeway.Intent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.tracked(sub) {
		return
	}
	if len(intents) == 0 {
		delete(s.intents, sub)
		return
	}
	s.intents[sub] = intents
}

// IntentsOf returns the intent set a subject's last evaluation resolved, or nil
// if it has not been evaluated since it was last tracked.
//
// The map and the intents in it are shared, not copied, and callers must treat
// both as read-only. They are written once per evaluation and replaced rather
// than mutated, so a reader holding the previous map sees a consistent older
// answer rather than a torn newer one.
func (s *State) IntentsOf(sub leeway.SubjectRef) map[leeway.TopologyKey]*leeway.Intent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.intents[sub]
}

// EachIntent calls yield for every tracked subject's resolved intent, one call
// per axis, under the lock.
//
// Used by the intent_info scrape callback. It walks rather than copying because
// the alternative — handing back a map of maps — allocates the whole intent
// table on every scrape to read a handful of labels off it, which is the same
// mistake SubjectCounts exists to avoid.
func (s *State) EachIntent(yield intentObserver) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for sub, byKey := range s.intents {
		for _, key := range leeway.SortedKeys(byKey) {
			yield(sub, key, byKey[key])
		}
	}
}

// SetEvaluations records what a subject's evaluation scored (§7.3). An empty
// set clears the entry, mirroring SetIntents.
//
// Ignored for an untracked subject, for the same reason as SetRepresentative.
func (s *State) SetEvaluations(sub leeway.SubjectRef, evals []Evaluation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.tracked(sub) {
		return
	}
	if len(evals) == 0 {
		delete(s.evaluations, sub)
		return
	}
	s.evaluations[sub] = evals
}

// EvaluationsOf returns what a subject's last evaluation scored, or nil if it
// has not been scored since it was last tracked.
//
// The slice is shared, not copied, and callers must treat it as read-only. It
// is replaced rather than mutated, so a reader holding the previous slice sees
// a consistent older answer rather than a torn newer one.
func (s *State) EvaluationsOf(sub leeway.SubjectRef) []Evaluation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.evaluations[sub]
}

// EachEvaluation calls yield for every scored subject, one call per axis, under
// the lock. Used by the §7.3 scrape callbacks, and it walks rather than copying
// for the reason SubjectCounts gives.
func (s *State) EachEvaluation(yield evalObserver) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for sub, evals := range s.evaluations {
		for i := range evals {
			yield(sub, &evals[i])
		}
	}
}

// DriftOf returns a subject's highest drift across its axes, and false when it
// has not been scored.
//
// Highest rather than per-axis, because §8.4's per-domain series are gated per
// *subject*: one that is drifting on its zone axis is one somebody is about to
// go and look at, and serving them the region breakdown while withholding the
// zone one would be the wrong half.
func (s *State) DriftOf(sub leeway.SubjectRef) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	evals, ok := s.evaluations[sub]
	if !ok || len(evals) == 0 {
		return 0, false
	}
	worst := evals[0].Scores.Drift
	for _, e := range evals[1:] {
		if e.Scores.Drift > worst {
			worst = e.Scores.Drift
		}
	}
	return worst, true
}

// Rebuilt is one subject's distributions computed straight from the pod cache,
// alongside the pods they were computed from. The pods are kept because a
// mismatch is repaired by re-seating them, not by overwriting the counts: see
// Repair.
type Rebuilt struct {
	Pods   []*corev1.Pod
	Counts map[leeway.TopologyKey]*leeway.Distribution
}

// RebuildShard computes, from scratch, the distributions of every subject that
// falls in the given shard, for the pods handed to it.
//
// The subject of each pod is taken from the cache first (subjectOf), not
// resolved fresh. That looks like it weakens the check — the rebuild trusts one
// of the things it is checking — and the alternative is worse. A pod whose
// ReplicaSet has since left the informer cache is *supposed* to keep counting
// under the subject it was first assigned; re-resolving would fail to find one
// and report correct behaviour as drift, on an SLI whose whole value is that any
// non-zero rate means a bug. What this pass verifies is the arithmetic — the
// increments, decrements and re-maps of §6.3 — which is where drift actually
// comes from.
func (s *State) RebuildShard(pods []*corev1.Pod, shard, shards int) map[leeway.SubjectRef]*Rebuilt {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[leeway.SubjectRef]*Rebuilt)
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		p, counted := s.resolvePlacement(pod)
		if !counted {
			continue
		}
		sub, ok := s.subjectOf(pod, pod.UID)
		if !ok || shardOf(sub, shards) != shard {
			continue
		}
		r, ok := out[sub]
		if !ok {
			r = &Rebuilt{Counts: make(map[leeway.TopologyKey]*leeway.Distribution, len(s.keys))}
			out[sub] = r
		}
		r.Pods = append(r.Pods, pod)
		for ordinal, key := range s.keys {
			dist, ok := r.Counts[key]
			if !ok {
				dist = leeway.NewDistribution()
				r.Counts[key] = dist
			}
			dist.Add(p.Domain(ordinal), p.State, p.Pinned)
		}
	}
	return out
}

// SubjectsInShard returns the tracked subjects that fall in the given shard.
func (s *State) SubjectsInShard(shard, shards int) []leeway.SubjectRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []leeway.SubjectRef
	for sub := range s.counts {
		if shardOf(sub, shards) == shard {
			out = append(out, sub)
		}
	}
	return out
}

// Repair replaces one subject's state with a rebuild of it.
//
// It re-seats the pod indexes and not only the counts. Overwriting counts alone
// would look like it worked and re-corrupt on the next event: the placements
// index is what deltas decrement from, so a pod left at a stale placement
// subtracts from a domain it was never counted in the moment it moves, putting
// the subject back where it started with a negative in it.
//
// Pods belonging to this subject that are absent from the rebuild are dropped
// from every index. That is the leak case — a pod the cache no longer has and
// whose delete event never reached us — and it is the one the counts cannot
// recover from on their own.
func (s *State) Repair(sub leeway.SubjectRef, pods []*corev1.Pod) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for uid, cached := range s.subjects {
		if cached != sub {
			continue
		}
		if p, known := s.placements[uid]; known {
			s.unlinkNode(p.NodeName, uid)
		}
		delete(s.placements, uid)
		delete(s.subjects, uid)
	}
	// Dropped whole rather than decremented back to zero: the counts are known
	// wrong, so unwinding them with the same arithmetic that produced them is
	// not a repair. Nothing else keys off this map.
	delete(s.counts, sub)
	// The representative goes with them. A repair is driven by a rebuild from
	// the pod cache, and the leak case it exists to fix — a pod whose delete
	// event never arrived — is exactly the case where the hint names a pod that
	// is not there. Re-elected below from the pods the rebuild actually saw.
	s.forgetLocked(sub)

	for _, pod := range pods {
		p, counted := s.resolvePlacement(pod)
		if !counted {
			continue
		}
		s.placements[pod.UID] = p
		s.subjects[pod.UID] = sub
		s.linkNode(p.NodeName, pod.UID)
		s.applyLocked(sub, p, +1)
		if s.tracked(sub) {
			s.representatives[sub] = representative{uid: pod.UID, name: pod.Name}
		}
	}
	s.enqueue(sub)
}

// shardOf assigns a subject to one of shards slices, stably across restarts and
// across processes. Hashing the subject rather than, say, ranging over a sorted
// subject list means a shard's membership does not shift every time an unrelated
// Deployment is created — so a subject cannot repeatedly land just behind the
// moving boundary and go years without being checked.
func shardOf(sub leeway.SubjectRef, shards int) int {
	if shards <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(sub.String()))
	return int(h.Sum32() % uint32(shards)) //nolint:gosec // shards is a small positive count
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
	switch rep, hasRep := s.representatives[sub]; {
	case !s.tracked(sub):
		// That was the subject's last counted pod. This is the one decrement
		// that is final, so it is where everything else keyed by the subject
		// goes too — see forgetLocked.
		s.forgetLocked(sub)
	case hasRep && rep.uid == uid:
		// Other pods remain, but the hint named this one, so it is now a name
		// the lister will not resolve. Dropping it here rather than leaving it
		// to fail on read costs nothing and keeps the miss path for genuine
		// surprises.
		delete(s.representatives, sub)
	}
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

// forgetLocked drops the per-subject state that is not a count but shares the
// counts' lifetime. Caller holds the lock.
//
// Both maps are keyed by subject and neither is rebuilt from anything, so a
// subject that stops being tracked and is never seen again would keep its
// entries for the life of the process — and, for intents, keep exporting an
// intent_info series for a workload that no longer exists.
//
// It is deliberately not called from applyLocked, even though applyLocked is
// where the counts row is dropped. A decrement to zero there is not the same
// event as a subject going away: the move path in OnPodUpdate and the re-map in
// remapNode both take a subject's last pod out and put it straight back under
// the same lock, and a single-pod subject passes through zero every time either
// one runs. Forgetting on that transient would clear the representative and the
// resolved intents of a healthy workload whose node was merely relabelled.
// Only the callers that know their decrement is final call this.
func (s *State) forgetLocked(sub leeway.SubjectRef) {
	delete(s.representatives, sub)
	delete(s.intents, sub)
	delete(s.evaluations, sub)
}

// tracked reports whether the subject has counts, from either half. Caller
// holds the lock.
func (s *State) tracked(sub leeway.SubjectRef) bool {
	if _, ok := s.counts[sub]; ok {
		return true
	}
	_, ok := s.groups[sub]
	return ok
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
