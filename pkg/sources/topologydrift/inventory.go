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
	"maps"
	"slices"
	"sort"
	"sync"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// betaFallback maps a topology key to the deprecated beta label carrying the
// same value.
//
// Reading the fallback is not optional politeness. A node labelled only with
// the beta key would otherwise resolve to DomainUnknown, and because
// DomainUnknown is a real, visible domain (pkg/leeway's sentinel, deliberately
// not a drop), the result would not be a missing node — it would be a
// confident, wrong distribution showing a chunk of the fleet in a nonexistent
// zone. Three other places in this repo already read both spellings
// (pkg/sources/capacity/forecast.go, pkg/checks/state/volumes.go,
// pkg/graph/derive.go); leeway matching them is the conservative choice.
var betaFallback = map[leeway.TopologyKey]string{
	corev1.LabelTopologyZone:   corev1.LabelFailureDomainBetaZone,
	corev1.LabelTopologyRegion: corev1.LabelFailureDomainBetaRegion,
}

// DomainStats is the per-domain aggregate of the nodes in it (§6.2).
type DomainStats struct {
	// Nodes is every node in the domain, whatever its condition.
	Nodes int64

	// Usable is the nodes that could actually accept a pod: Ready and not
	// cordoned. Kept apart from Nodes because a zone whose nodes have all gone
	// NotReady still exists — it is simply not somewhere the scheduler can
	// place anything, which is a finding rather than an absence.
	Usable int64

	// Allocatable sums over usable nodes only, for capacity weighting (§7.2).
	// Summing over all nodes would expect pods into capacity that cannot
	// receive them.
	AllocatableCPUMilli int64
	AllocatableMemBytes int64
}

// nodeFacts is everything the inventory retains about one node. It is
// deliberately not a *corev1.Node: holding the API object would keep the whole
// (already trimmed) object alive per node and would make the relevance
// comparison in Upsert a deep compare of fields nothing here reads.
type nodeFacts struct {
	name        string
	domains     []leeway.Domain // by topology-key ordinal, interned tuple
	ready       bool
	schedulable bool
	taints      []corev1.Taint
	labels      map[string]string
	cpuMilli    int64
	memBytes    int64
}

// Change describes what a node event actually altered, so the caller can do the
// cheapest correct thing (§6.3).
type Change struct {
	// Added and Removed report inventory membership changes.
	Added, Removed bool

	// DomainsChanged means this node's topology labels moved, so every pod on
	// it is now counted in the wrong domain and must be re-mapped. Narrow and
	// expensive: the caller walks byNode for this one node.
	DomainsChanged bool

	// EligibilityChanged means the node's contribution to the eligible domain
	// set moved — readiness, schedulability, taints, allocatable or labels.
	// Broad and cheap: the caller bumps the inventory generation and lets
	// subjects re-evaluate lazily, because eagerly enqueuing every subject in
	// the cluster on one zone-wide node event is the stampede §6.3 exists to
	// avoid.
	//
	// A topology relabel always sets this as well as DomainsChanged, which is
	// correct rather than redundant: the pods on the node need re-mapping and
	// the cluster's domain composition moved — the node's old zone may have
	// just emptied. The two flags select two different pieces of work, and a
	// relabel needs both.
	EligibilityChanged bool
}

// Any reports whether the event changed anything worth acting on.
func (c Change) Any() bool {
	return c.Added || c.Removed || c.DomainsChanged || c.EligibilityChanged
}

// Inventory is the node-derived half of the state: which domains exist on each
// configured axis, what is in them, and which nodes map to which domain (§6.2).
//
// Safe for concurrent use. It has to be: the Node handler writes to it while
// the Pod handler reads DomainsOf on every placement, and under a shared
// informer factory those are different goroutines.
type Inventory struct {
	mu       sync.RWMutex
	keys     []leeway.TopologyKey
	ordinals map[leeway.TopologyKey]int
	intern   *interner
	nodes    map[string]*nodeFacts
	stats    []map[leeway.Domain]*DomainStats // indexed by ordinal
	gen      uint64
}

// NewInventory returns an inventory tracking the given topology keys.
//
// The key set is fixed for the inventory's life, which is what lets a Placement
// address its domains by ordinal instead of carrying a map (§5). Duplicates are
// dropped, preserving first-seen order, so that an ordinal always means the
// same axis.
func NewInventory(keys []leeway.TopologyKey) *Inventory {
	inv := &Inventory{
		ordinals: make(map[leeway.TopologyKey]int, len(keys)),
		intern:   newInterner(),
		nodes:    make(map[string]*nodeFacts),
		gen:      1,
	}
	for _, k := range keys {
		if k == "" {
			continue
		}
		if _, dup := inv.ordinals[k]; dup {
			continue
		}
		inv.ordinals[k] = len(inv.keys)
		inv.keys = append(inv.keys, k)
		inv.stats = append(inv.stats, make(map[leeway.Domain]*DomainStats))
	}
	return inv
}

// Keys returns the configured topology keys in ordinal order.
func (inv *Inventory) Keys() []leeway.TopologyKey {
	return slices.Clone(inv.keys)
}

// Ordinal returns the index of a topology key in a Placement's Domains slice.
func (inv *Inventory) Ordinal(key leeway.TopologyKey) (int, bool) {
	i, ok := inv.ordinals[key]
	return i, ok
}

// Generation is bumped whenever the eligible domain set may have moved. A
// subject whose cached evaluation was computed at an older generation is stale
// (§6.3).
func (inv *Inventory) Generation() uint64 {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	return inv.gen
}

// Len is the number of nodes in the inventory.
func (inv *Inventory) Len() int {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	return len(inv.nodes)
}

// Upsert records a node and reports what changed.
//
// The comparison is against the retained facts, not against the previous
// object. That is what makes the common case free: a node status update whose
// only difference is a condition's heartbeat timestamp produces identical facts
// and returns a zero Change, so neither the pod re-map nor the generation bump
// happens. Comparing objects would make every heartbeat look like a change.
func (inv *Inventory) Upsert(node *corev1.Node) Change {
	if node == nil || node.Name == "" {
		return Change{}
	}
	inv.mu.Lock()
	defer inv.mu.Unlock()

	next := inv.factsOf(node)
	prev, known := inv.nodes[next.name]
	if !known {
		inv.nodes[next.name] = next
		inv.addStats(next, +1)
		inv.gen++
		return Change{Added: true, DomainsChanged: true, EligibilityChanged: true}
	}

	var ch Change
	if !slices.Equal(prev.domains, next.domains) {
		ch.DomainsChanged = true
	}
	if prev.ready != next.ready ||
		prev.schedulable != next.schedulable ||
		prev.cpuMilli != next.cpuMilli ||
		prev.memBytes != next.memBytes ||
		!taintsEqual(prev.taints, next.taints) ||
		!maps.Equal(prev.labels, next.labels) {
		ch.EligibilityChanged = true
	}
	if !ch.Any() {
		return ch
	}

	// The stats swap has to happen whenever either half moved: a domain change
	// moves the node's counts between buckets, and an eligibility change moves
	// its contribution within one.
	inv.addStats(prev, -1)
	inv.nodes[next.name] = next
	inv.addStats(next, +1)
	inv.gen++
	return ch
}

// Remove drops a node from the inventory.
func (inv *Inventory) Remove(name string) Change {
	inv.mu.Lock()
	defer inv.mu.Unlock()

	prev, known := inv.nodes[name]
	if !known {
		return Change{}
	}
	inv.addStats(prev, -1)
	delete(inv.nodes, name)
	inv.intern.forget(prev.name)
	inv.gen++
	return Change{Removed: true, DomainsChanged: true, EligibilityChanged: true}
}

// DomainsOf returns the interned domain tuple for a node, or nil if the node is
// unknown.
//
// A nil result is the honest answer for a pod scheduled onto a node whose event
// has not arrived yet, and the caller records it as such rather than guessing:
// see State's handling, where an unknown node yields DomainUnknown on every
// axis and the node's own event re-maps the pod when it lands.
//
// The returned slice is shared and must not be mutated.
func (inv *Inventory) DomainsOf(nodeName string) []leeway.Domain {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	if n, ok := inv.nodes[nodeName]; ok {
		return n.domains
	}
	return nil
}

// UnknownTuple is the domain tuple for a node the inventory has never seen: the
// sentinel on every configured axis. It is interned like any other tuple.
func (inv *Inventory) UnknownTuple() []leeway.Domain {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	unknown := make([]leeway.Domain, len(inv.keys))
	for i := range unknown {
		unknown[i] = leeway.DomainUnknown
	}
	return inv.intern.tuple(unknown)
}

// Stats returns a copy of the per-domain aggregates for one axis.
func (inv *Inventory) Stats(key leeway.TopologyKey) map[leeway.Domain]DomainStats {
	ordinal, ok := inv.Ordinal(key)
	if !ok {
		return nil
	}
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	out := make(map[leeway.Domain]DomainStats, len(inv.stats[ordinal]))
	for d, s := range inv.stats[ordinal] {
		out[d] = *s
	}
	return out
}

// NodeViews projects the inventory onto the shape pkg/leeway's eligibility
// computation consumes, for one axis (§7.1).
//
// MatchesSelector and Tolerated are set true for every node. That is the
// correct Phase 2 answer and not a placeholder to be forgotten: this package
// owns the predicate and pkg/leeway owns the rule, and the predicate needs the
// subject's nodeSelector, affinity and tolerations, which is Phase 3's intent
// inference. Until then every node matches, which is exactly the behaviour of a
// workload that constrains nothing — the common case.
//
// The result is sorted by node name so that eligibility, and therefore every
// score derived from it, does not depend on Go's map iteration order.
func (inv *Inventory) NodeViews(key leeway.TopologyKey, weighting leeway.Weighting) []leeway.NodeView {
	ordinal, ok := inv.Ordinal(key)
	if !ok {
		return nil
	}
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	views := make([]leeway.NodeView, 0, len(inv.nodes))
	for _, n := range inv.nodes {
		views = append(views, leeway.NodeView{
			Name:            n.name,
			Domain:          n.domain(ordinal),
			Ready:           n.ready,
			Schedulable:     n.schedulable,
			MatchesSelector: true,
			Tolerated:       true,
			Capacity:        n.capacity(weighting),
		})
	}
	sort.Slice(views, func(a, b int) bool { return views[a].Name < views[b].Name })
	return views
}

// factsOf extracts the retained facts from a Node. Caller holds the lock,
// because interning writes.
func (inv *Inventory) factsOf(node *corev1.Node) *nodeFacts {
	domains := make([]leeway.Domain, len(inv.keys))
	for i, key := range inv.keys {
		domains[i] = leeway.NormaliseDomain(leeway.Domain(topologyValue(node.Labels, key)))
	}

	f := &nodeFacts{
		name:        inv.intern.name(node.Name),
		domains:     inv.intern.tuple(domains),
		schedulable: !node.Spec.Unschedulable,
		taints:      node.Spec.Taints,
		labels:      node.Labels,
	}
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			f.ready = c.Status == corev1.ConditionTrue
			break
		}
	}
	if cpu, ok := node.Status.Allocatable[corev1.ResourceCPU]; ok {
		f.cpuMilli = cpu.MilliValue()
	}
	if mem, ok := node.Status.Allocatable[corev1.ResourceMemory]; ok {
		f.memBytes = mem.Value()
	}
	return f
}

// addStats folds a node's contribution into (sign +1) or out of (sign -1) the
// per-domain aggregates. Caller holds the lock.
func (inv *Inventory) addStats(n *nodeFacts, sign int64) {
	for ordinal := range inv.keys {
		d := n.domain(ordinal)
		s, ok := inv.stats[ordinal][d]
		if !ok {
			if sign < 0 {
				continue
			}
			s = &DomainStats{}
			inv.stats[ordinal][d] = s
		}
		s.Nodes += sign
		if n.ready && n.schedulable {
			s.Usable += sign
			s.AllocatableCPUMilli += sign * n.cpuMilli
			s.AllocatableMemBytes += sign * n.memBytes
		}
		// A domain with no nodes left is gone, not a zero row. Keeping it
		// would make an eligible-domain list that includes somewhere no pod
		// can ever be placed, and the expectation would reserve a share for
		// it forever.
		if s.Nodes <= 0 {
			delete(inv.stats[ordinal], d)
		}
	}
}

func (n *nodeFacts) domain(ordinal int) leeway.Domain {
	if ordinal < 0 || ordinal >= len(n.domains) {
		return leeway.DomainUnknown
	}
	return n.domains[ordinal]
}

func (n *nodeFacts) capacity(w leeway.Weighting) float64 {
	switch w {
	case leeway.WeightAllocatableCPU:
		return float64(n.cpuMilli)
	case leeway.WeightAllocatableMemory:
		return float64(n.memBytes)
	default:
		return 0
	}
}

// topologyValue reads a topology key from a node's labels, falling back to the
// deprecated beta spelling where one exists.
func topologyValue(labels map[string]string, key leeway.TopologyKey) string {
	if v, ok := labels[string(key)]; ok && v != "" {
		return v
	}
	if beta, ok := betaFallback[key]; ok {
		return labels[beta]
	}
	return ""
}

// taintsEqual compares taints by the fields that affect scheduling. TimeAdded
// is excluded deliberately: it moves when a node controller re-applies an
// identical taint, and treating that as a change would bump the generation
// across the fleet for nothing.
func taintsEqual(a, b []corev1.Taint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key || a[i].Value != b[i].Value || a[i].Effect != b[i].Effect {
			return false
		}
	}
	return true
}
