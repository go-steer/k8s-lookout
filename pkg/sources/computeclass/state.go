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

package computeclass

import (
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// Source implements sources.Source for the compute-class row of §7.2.
//
// Three caches feed one piece of arithmetic. A ComputeClass gives an axis, a
// Node gives a rank on that axis, and a Pod inherits its node's rank for as
// long as it occupies it. Every mutation is a delta on the same three maps
// under one lock — there is no periodic rebuild, because the thing being
// accumulated is time, and a rebuild would have to reconstruct when each pod
// arrived.
type Source struct {
	client kubernetes.Interface
	dyn    dynamic.Interface
	cfg    Config

	extractor *leeway.ProfileExtractor
	resolver  *leeway.RankResolver
	tracker   *leeway.RankTracker

	factory     informers.SharedInformerFactory
	nodeFactory informers.SharedInformerFactory

	mu sync.Mutex
	// synced flips after the three caches cross their barrier.
	synced bool
	// classes holds every class that decoded, by name. A class that failed to
	// decode is absent here and counted in decodeFails: a half-read axis is
	// worse than no axis, so the nodes on it report as unreadable rather than
	// being ranked against rules we are not sure we understood.
	classes map[string]*leeway.ComputeClass
	// decodeFails counts refusals per class name. Kept after the class is
	// deleted, because an error counter that disappears when the broken object
	// does hides the very edit that broke it.
	decodeFails map[string]int64
	nodes       map[string]*nodeState
	pods        map[podRef]*podState
	// podsByNode indexes the pods a node change has to retarget. Without it
	// every node event would be O(pods in the cluster).
	podsByNode  map[string]map[podRef]struct{}
	transitions map[transitionKey]int64

	in *instruments

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
	// logf overrides log.Printf for testing. nil = log.Printf.
	logf func(format string, args ...any)
}

// podRef identifies a pod. Namespace/name rather than UID, because the
// tombstone path and the informer both always have the former, and a pod's
// identity for this purpose is the slot it occupies.
type podRef struct {
	Namespace string
	Name      string
}

// nodeState is what one node contributed at its last resolution.
type nodeState struct {
	// class is the value of the class label, empty if the node carries none.
	class string
	// known records that the class was decoded — so an unreadable class and a
	// node outside every class are distinguishable, which they are not from
	// `place` alone.
	known bool
	place leeway.RankPlacement
	axis  leeway.AxisKey
	// profile and annotation are the two inputs to the next resolution. Kept
	// because a class edit has to re-resolve every node on it, and the node
	// objects are the informer's, not ours.
	profile    leeway.NodeProfile
	annotation string
}

// placed reports whether this node contributes a rank pods can be charged to.
func (n *nodeState) placed() bool { return n != nil && n.known }

// podState is where a pod is currently charged.
type podState struct {
	node string
	axis leeway.AxisKey
	rank leeway.Rank
	// counted is true while this pod is inside the tracker. A pod on a node
	// whose class has not synced yet is tracked here but not counted, so that
	// it can be charged the moment the class arrives rather than waiting for
	// its own next event — which for a Running pod may never come.
	counted bool
}

// transitionKey is one node's move between two placements.
type transitionKey struct {
	Axis leeway.AxisKey
	From leeway.Rank
	To   leeway.Rank
	// Lateral marks a move between two rules sharing a tier — capacity churn
	// within a preference level, not a fallback. Counted, and excluded from
	// every degradation signal, because an equal-score alternative is not a
	// demotion (§7.7.3).
	Lateral bool
}

// UpsertClass decodes a ComputeClass spec and re-ranks everything on it.
//
// Exported so tests can drive the state machine without an informer, which is
// how every behavioural test in here runs.
func (s *Source) UpsertClass(name string, spec map[string]any, at time.Time) {
	class, err := leeway.DecodeComputeClass(name, spec)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		s.decodeFails[name]++
		// Drop the previous good decode rather than keep serving it. The
		// object in the cluster is not the one we last understood, and ranking
		// nodes against a superseded spec produces confident wrong answers —
		// exactly what the annotation-primary design exists to avoid.
		if old, ok := s.classes[name]; ok {
			delete(s.classes, name)
			s.resetAxis(old.Axis.Key)
		}
		s.logPrintf("%s: ComputeClass %q did not decode, its nodes are now unranked: %v", Name, name, err)
		s.rerankClass(name, at)
		return
	}

	prev, had := s.classes[name]
	s.classes[name] = class
	if had && prev.Axis.SpecHash != class.Axis.SpecHash {
		// A re-tiering. Rank 1 no longer means what it meant an hour ago, so
		// the accumulated pod-seconds are discarded rather than averaged
		// across two different questions. spec_hash is a label on the
		// counter, so this reads downstream as a new series, not as a counter
		// that went backwards.
		s.resetAxis(prev.Axis.Key)
	}
	s.rerankClass(name, at)
}

// DeleteClass drops a class and unranks its nodes.
func (s *Source) DeleteClass(name string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	class, ok := s.classes[name]
	if !ok {
		return
	}
	delete(s.classes, name)
	s.resetAxis(class.Axis.Key)
	s.rerankClass(name, at)
}

// resetAxis discards an axis's buckets and marks its pods uncharged.
//
// The pods are NOT left via the tracker: their buckets no longer exist, and a
// Leave against a recreated empty bucket would register as an underflow — an
// SLI whose whole value is that it reads zero.
func (s *Source) resetAxis(key leeway.AxisKey) {
	s.tracker.Forget(key)
	for _, ps := range s.pods {
		if ps.counted && ps.axis == key {
			ps.counted = false
		}
	}
}

// rerankClass re-resolves every node carrying one class label and retargets
// the pods on them.
func (s *Source) rerankClass(name string, at time.Time) {
	for nodeName, ns := range s.nodes {
		if ns.class != name {
			continue
		}
		s.resolveNode(nodeName, ns, at)
	}
}

// UpsertNode records a node's profile and resolves its rank.
func (s *Source) UpsertNode(node *corev1.Node, at time.Time) {
	// Capacity, not allocatable: a priority rule's minCores and minMemoryGb
	// describe the machine the rule would admit, and allocatable is that
	// machine minus whatever GKE reserved on it.
	//
	// Memory converts at 10^9, not 2^30. GKE writes minMemoryGb against the
	// machine type's nominal size, and a node's reported capacity is already
	// nominal-minus-kernel-reservation; dividing by 2^30 would land below the
	// nominal figure and miss a rule that asks for exactly it, while 10^9
	// lands a few percent above and matches. Near a boundary this is an
	// estimate, which is one more reason the annotation stays primary and the
	// disagreement gauge exists.
	cores := node.Status.Capacity.Cpu().Value()
	memGB := float64(node.Status.Capacity.Memory().Value()) / 1e9
	profile := s.extractor.Extract(node.Labels, cores, memGB)

	class := node.Labels[s.cfg.ClassLabel]
	annotation := node.Annotations[s.cfg.PriorityIndexAnnotation]

	s.mu.Lock()
	defer s.mu.Unlock()

	ns, had := s.nodes[node.Name]
	if !had {
		ns = &nodeState{}
		s.nodes[node.Name] = ns
	}
	ns.class = class
	ns.profile = profile
	ns.annotation = annotation
	s.resolveNode(node.Name, ns, at)
}

// DeleteNode forgets a node and uncharges the pods still indexed on it.
func (s *Source) DeleteNode(name string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.nodes[name]; !ok {
		return
	}
	delete(s.nodes, name)
	// The pods are not deleted: their own delete events may not have arrived,
	// and a pod whose node vanished is genuinely unplaced rather than gone.
	// retarget reads s.nodes, which no longer has this one, so they uncharge.
	for ref := range s.podsByNode[name] {
		s.retarget(ref, at)
	}
}

// resolveNode recomputes one node's placement and moves its pods if it landed
// somewhere new. Caller holds s.mu.
func (s *Source) resolveNode(name string, ns *nodeState, at time.Time) {
	beforePlace, beforeAxis, wasKnown := ns.place, ns.axis, ns.known

	class, ok := s.classes[ns.class]
	if ns.class == "" || !ok {
		// Outside every class, or on one we could not read. Either way there
		// is no axis to rank against, and pretending otherwise would file the
		// node under a rank it was never assigned.
		ns.known = false
		ns.axis = leeway.AxisKey{}
		ns.place = leeway.RankPlacement{Rank: leeway.RankUnknown, RuleIndex: -1}
	} else {
		ns.known = true
		ns.axis = class.Axis.Key
		ns.place = s.resolver.Resolve(ns.annotation, ns.profile, class.Axis)
	}

	// A transition is a node that was somewhere and is now somewhere else on
	// the SAME axis. A node's first resolution is not a transition, and a node
	// that changed class did not move within a preference order — it left one.
	if wasKnown && ns.known && beforeAxis == ns.axis &&
		(beforePlace.Rank != ns.place.Rank || beforePlace.RuleIndex != ns.place.RuleIndex) {
		s.transitions[transitionKey{
			Axis:    ns.axis,
			From:    beforePlace.Rank,
			To:      ns.place.Rank,
			Lateral: beforePlace.Rank == ns.place.Rank,
		}]++
	}

	for ref := range s.podsByNode[name] {
		s.retarget(ref, at)
	}
}

// UpsertPod charges a pod to its node's rank, or moves it if that changed.
func (s *Source) UpsertPod(pod *corev1.Pod, at time.Time) {
	ref := podRef{pod.Namespace, pod.Name}
	node := pod.Spec.NodeName

	s.mu.Lock()
	defer s.mu.Unlock()

	if !occupies(pod) {
		s.forgetPod(ref, at)
		return
	}

	ps, had := s.pods[ref]
	if had && ps.node != node {
		// A pod is bound once and never rebound, so this is a name reused
		// after a delete we missed. Uncharge the old node before re-indexing.
		s.forgetPod(ref, at)
		had = false
	}
	if !had {
		ps = &podState{node: node}
		s.pods[ref] = ps
		byNode, ok := s.podsByNode[node]
		if !ok {
			byNode = map[podRef]struct{}{}
			s.podsByNode[node] = byNode
		}
		byNode[ref] = struct{}{}
	}
	s.retarget(ref, at)
}

// DeletePod uncharges a pod.
func (s *Source) DeletePod(pod *corev1.Pod, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forgetPod(podRef{pod.Namespace, pod.Name}, at)
}

// forgetPod uncharges and de-indexes one pod. Caller holds s.mu.
func (s *Source) forgetPod(ref podRef, at time.Time) {
	ps, ok := s.pods[ref]
	if !ok {
		return
	}
	if ps.counted {
		s.tracker.Leave(ps.axis, ps.rank, at)
	}
	delete(s.pods, ref)
	if byNode, ok := s.podsByNode[ps.node]; ok {
		delete(byNode, ref)
		if len(byNode) == 0 {
			delete(s.podsByNode, ps.node)
		}
	}
}

// retarget moves one pod to wherever its node now says it belongs. Caller
// holds s.mu.
func (s *Source) retarget(ref podRef, at time.Time) {
	ps, ok := s.pods[ref]
	if !ok {
		return
	}
	ns := s.nodes[ps.node]
	place := ns.placed()

	if ps.counted && (!place || ps.axis != ns.axis || ps.rank != ns.place.Rank) {
		s.tracker.Leave(ps.axis, ps.rank, at)
		ps.counted = false
	}
	if place && !ps.counted {
		ps.axis, ps.rank = ns.axis, ns.place.Rank
		s.tracker.Enter(ps.axis, ps.rank, at)
		ps.counted = true
	}
}

// reconcile re-resolves every node once the caches have synced.
//
// The three informers sync independently, so a node handled before its class
// arrived resolved against an axis that did not exist. Its own next event
// would fix it — but a stable node in a stable cluster has no next event, so
// without this pass a fleet that was quiet at startup stays unranked forever.
func (s *Source) reconcile(at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, ns := range s.nodes {
		s.resolveNode(name, ns, at)
	}
}

// occupies reports whether a pod is holding a place on a node right now.
//
// Bound and not terminal. A Pending unscheduled pod has no node and therefore
// no rank — which is a real condition and the subject of §7.7.4's
// leeway.rank_wedged, but it is not occupancy and charging it to a rank would
// put a pod that got nothing into the same bucket as one that got its first
// choice.
func occupies(pod *corev1.Pod) bool {
	if pod.Spec.NodeName == "" {
		return false
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return false
	default:
		return true
	}
}
