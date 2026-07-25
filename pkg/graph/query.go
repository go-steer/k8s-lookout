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

package graph

// Query surface per DESIGN.md §6.4. All queries run on one immutable
// Snapshot (grab it once, query many times — consistent by
// construction), return compact NodeIDs, carry visited-sets (the
// topology is a graph, not a DAG), and are deterministic: results
// are sorted, so the same snapshot always yields byte-identical
// answers.

import (
	"slices"
	"sort"
)

// Hit is one node reached by a traversal, with the BFS depth at
// which it was first reached (1 = direct neighbor).
type Hit struct {
	ID    NodeID
	Depth int
	// Via is the kind of the edge that first reached this node: how
	// the hit attaches to the chain (`triage radius` renders it as the
	// neighbor's relation). For Up hits it is the edge from the hit
	// toward the origin's chain (Ingress --RoutesTo--> Service); for
	// Down hits the edge from the chain toward the hit (Pod
	// --Mounts--> ConfigMap); for Lateral hits the co-tenant's own
	// edge to the shared node (RunsOn or Mounts).
	Via EdgeKind
	// Anchor is set on Lateral hits only: the shared-infrastructure
	// node (Node, Zone, ConfigMap, Secret, PVC) that makes the hit a
	// co-tenant. NoNode on Up/Down hits.
	Anchor NodeID
}

// RadiusResult is the blast-radius neighborhood of an origin node
// (`triage radius`, §7.5 storm correlation, §7.6 enrichment).
type RadiusResult struct {
	Origin NodeID
	// Up: BFS over inbound edges — everything that points at the
	// origin, transitively: the routing chain (EndpointSlice,
	// Service, Ingress), owners, governing policies; for infra
	// origins (Node, ConfigMap) this is the dependent workloads.
	Up []Hit
	// Down: BFS over outbound edges — everything the origin points
	// at: owned children, containers, mounted configs/secrets/PVCs,
	// the node, its zone.
	Down []Hit
	// Lateral: co-tenants — for every shared-infrastructure node in
	// Down (Node, Zone, ConfigMap, Secret, PVC), the *other* objects
	// that run on or mount it. Depth is the infra node's depth + 1.
	Lateral []Hit
}

// lateralKinds are the shared-infrastructure kinds whose co-tenants
// constitute lateral blast radius.
var lateralKinds = [numNodeKinds]bool{
	KindNode:                  true,
	KindZone:                  true,
	KindConfigMap:             true,
	KindSecret:                true,
	KindPersistentVolumeClaim: true,
}

// Radius computes the blast-radius neighborhood of origin with the
// given depth limit (edges traversed per direction). Unknown origin
// or maxDepth <= 0 yields an empty result.
func (s *Snapshot) Radius(origin NodeID, maxDepth int) RadiusResult {
	res := RadiusResult{Origin: origin}
	if maxDepth <= 0 {
		return res
	}
	if _, ok := s.nodes[origin]; !ok {
		return res
	}
	res.Up = s.directedBFS(origin, maxDepth, s.in)
	res.Down = s.directedBFS(origin, maxDepth, s.out)

	seen := make(map[NodeID]struct{}, 1+len(res.Up)+len(res.Down))
	seen[origin] = struct{}{}
	for _, h := range res.Up {
		seen[h.ID] = struct{}{}
	}
	for _, h := range res.Down {
		seen[h.ID] = struct{}{}
	}
	for _, h := range res.Down {
		if !lateralKinds[s.nodes[h.ID].kind] {
			continue
		}
		for _, e := range s.in[h.ID] {
			if e.Kind != EdgeRunsOn && e.Kind != EdgeMounts {
				continue
			}
			if _, dup := seen[e.To]; dup {
				continue
			}
			seen[e.To] = struct{}{}
			res.Lateral = append(res.Lateral, Hit{ID: e.To, Depth: h.Depth + 1, Via: e.Kind, Anchor: h.ID})
		}
	}
	sortHits(res.Up)
	sortHits(res.Down)
	sortHits(res.Lateral)
	return res
}

// directedBFS walks adj (either s.in or s.out) from origin to
// maxDepth with a visited-set; origin itself is not reported.
func (s *Snapshot) directedBFS(origin NodeID, maxDepth int, adj map[NodeID][]Edge) []Hit {
	visited := map[NodeID]struct{}{origin: {}}
	var hits []Hit
	frontier := []NodeID{origin}
	for depth := 1; depth <= maxDepth && len(frontier) > 0; depth++ {
		var next []NodeID
		for _, n := range frontier {
			for _, e := range adj[n] {
				if _, dup := visited[e.To]; dup {
					continue
				}
				visited[e.To] = struct{}{}
				hits = append(hits, Hit{ID: e.To, Depth: depth, Via: e.Kind})
				next = append(next, e.To)
			}
		}
		frontier = next
	}
	return hits
}

func sortHits(hits []Hit) {
	slices.SortFunc(hits, func(a, b Hit) int {
		if a.Depth != b.Depth {
			return a.Depth - b.Depth
		}
		return int(a.ID) - int(b.ID)
	})
}

// ownerChainMax caps owner-chain length; real chains are ≤3
// (Pod→ReplicaSet→Deployment), the cap only guards degenerate input.
const ownerChainMax = 16

// OwnerChain resolves the controller chain of id, immediate owner
// first (e.g. pod → [ReplicaSet, Deployment]). When an object has
// several owners the lowest NodeID wins — deterministic, and real
// controller ownership is single. Cycle-safe; empty for unowned or
// unknown nodes.
func (s *Snapshot) OwnerChain(id NodeID) []NodeID {
	var chain []NodeID
	visited := map[NodeID]struct{}{id: {}}
	cur := id
	for len(chain) < ownerChainMax {
		owner := NoNode
		for _, e := range s.in[cur] {
			if e.Kind != EdgeOwns {
				continue
			}
			if owner == NoNode || e.To < owner {
				owner = e.To
			}
		}
		if owner == NoNode {
			break
		}
		if _, dup := visited[owner]; dup {
			break // ownerReference cycle — degenerate but must terminate
		}
		visited[owner] = struct{}{}
		chain = append(chain, owner)
		cur = owner
	}
	return chain
}

// ancestorMaxDepth bounds correlation-ancestor searches. The longest
// meaningful chain is pod→job→cronjob or pod→node→zone plus slack.
const ancestorMaxDepth = 8

// CommonAncestors answers the storm-correlation question (§7.5):
// given N incident objects, what do they share? Ancestors of a node
// are its transitive owners (inbound Owns), its placement (outbound
// RunsOn: node, zone), its mounted configuration (outbound Mounts),
// and its namespace. The result is every ancestor common to *all*
// inputs, nearest first (ranked by the maximum distance any input
// has to it, ties by NodeID). Empty when the inputs share nothing.
func (s *Snapshot) CommonAncestors(ids ...NodeID) []NodeID {
	if len(ids) == 0 {
		return nil
	}
	common := s.ancestorDepths(ids[0])
	for _, id := range ids[1:] {
		if len(common) == 0 {
			return nil
		}
		next := s.ancestorDepths(id)
		for n, d := range common {
			nd, ok := next[n]
			if !ok {
				delete(common, n)
				continue
			}
			if nd > d {
				common[n] = nd
			}
		}
	}
	if len(common) == 0 {
		return nil
	}
	out := make([]NodeID, 0, len(common))
	for n := range common {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool {
		di, dj := common[out[i]], common[out[j]]
		if di != dj {
			return di < dj
		}
		return out[i] < out[j]
	})
	return out
}

// ancestorDepths BFS-walks the correlation-ancestor relation from
// id (depth 0 = id itself) up to ancestorMaxDepth.
func (s *Snapshot) ancestorDepths(id NodeID) map[NodeID]int {
	depths := make(map[NodeID]int)
	if _, ok := s.nodes[id]; !ok {
		return depths
	}
	depths[id] = 0
	frontier := []NodeID{id}
	for depth := 1; depth <= ancestorMaxDepth && len(frontier) > 0; depth++ {
		var next []NodeID
		for _, n := range frontier {
			visit := func(m NodeID) {
				if m == NoNode {
					return
				}
				if _, seen := depths[m]; seen {
					return
				}
				depths[m] = depth
				next = append(next, m)
			}
			for _, e := range s.in[n] {
				if e.Kind == EdgeOwns {
					visit(e.To)
				}
			}
			for _, e := range s.out[n] {
				if e.Kind == EdgeRunsOn || e.Kind == EdgeMounts {
					visit(e.To)
				}
			}
			visit(s.nodes[n].ns)
		}
		frontier = next
	}
	return depths
}

// PodsUnder resolves a workload to its pods by walking outbound Owns
// edges (Deployment→ReplicaSet→Pod, CronJob→Job→Pod, …). A Pod input
// resolves to itself. Sorted; empty for unknown nodes or workloads
// with no pods. Cycle-safe.
func (s *Snapshot) PodsUnder(id NodeID) []NodeID {
	info, ok := s.nodes[id]
	if !ok {
		return nil
	}
	if info.kind == KindPod {
		return []NodeID{id}
	}
	var pods []NodeID
	visited := map[NodeID]struct{}{id: {}}
	stack := []NodeID{id}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, e := range s.out[n] {
			if e.Kind != EdgeOwns {
				continue
			}
			if _, dup := visited[e.To]; dup {
				continue
			}
			visited[e.To] = struct{}{}
			if s.nodes[e.To].kind == KindPod {
				pods = append(pods, e.To)
			} else {
				stack = append(stack, e.To)
			}
		}
	}
	slices.Sort(pods)
	return pods
}

// WorkloadEdge is one aggregated edge in a WorkloadEdges answer.
type WorkloadEdge struct {
	From, To NodeID
	Kind     EdgeKind
}

// WorkloadEdges is the `state edges` query: the dependency edges of
// a workload, aggregated across its pods. For each pod under id
// (via PodsUnder; id itself when it has no pods):
//
//   - outbound pod edges except Contains are lifted to the workload
//     (From = id): the nodes it runs on, the configs/secrets/PVCs it
//     mounts — deduplicated across pods.
//   - inbound Selects/Governs/RoutesTo sources are reported against
//     the workload (To = id): services selecting its pods, policies
//     governing them, endpoint slices targeting them.
//   - one further northbound hop is included for those sources:
//     their own inbound RoutesTo edges (Ingress→Service,
//     Service→EndpointSlice), so the full traffic chain appears in
//     one call.
//
// Deterministically sorted by (Kind, From, To).
func (s *Snapshot) WorkloadEdges(id NodeID) []WorkloadEdge {
	if _, ok := s.nodes[id]; !ok {
		return nil
	}
	pods := s.PodsUnder(id)
	if len(pods) == 0 {
		pods = []NodeID{id}
	}
	set := make(map[WorkloadEdge]struct{})
	add := func(e WorkloadEdge) { set[e] = struct{}{} }
	for _, p := range pods {
		for _, e := range s.out[p] {
			if e.Kind == EdgeContains {
				continue
			}
			add(WorkloadEdge{From: id, To: e.To, Kind: e.Kind})
		}
		for _, e := range s.in[p] {
			if e.Kind != EdgeSelects && e.Kind != EdgeGoverns && e.Kind != EdgeRoutesTo {
				continue
			}
			src := e.To
			add(WorkloadEdge{From: src, To: id, Kind: e.Kind})
			for _, e2 := range s.in[src] {
				if e2.Kind == EdgeRoutesTo {
					add(WorkloadEdge{From: e2.To, To: src, Kind: EdgeRoutesTo})
				}
			}
		}
	}
	out := make([]WorkloadEdge, 0, len(set))
	for e := range set {
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b WorkloadEdge) int {
		if a.Kind != b.Kind {
			return int(a.Kind) - int(b.Kind)
		}
		if a.From != b.From {
			return int(a.From) - int(b.From)
		}
		return int(a.To) - int(b.To)
	})
	return out
}
