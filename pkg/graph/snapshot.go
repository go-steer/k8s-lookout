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

import "sync"

// Snapshot is one immutable, internally consistent view of the
// topology. Readers obtain it from Graph.Snapshot and query it
// lock-free; the writer never mutates a published snapshot (map
// values, including edge slices, are replaced copy-on-write, never
// edited in place).
//
// Slices returned by Out and In alias snapshot-internal storage and
// MUST NOT be modified by callers.
type Snapshot struct {
	nodes map[NodeID]nodeInfo
	out   map[NodeID][]Edge // edges sorted by (Kind, To)
	in    map[NodeID][]Edge // reverse adjacency, same ordering
	edges int

	// keys is the interner view at swap time (len fixed); ids is the
	// shared append-only key→ID map — Lookup validates hits against
	// nodes so identities interned after this swap are invisible.
	keys []string
	ids  *sync.Map

	// watched is the ingest's Options.WatchedKinds as a set (shared,
	// never mutated after construction). Nil means every supported
	// kind is observed — the full-List one-shot posture, and the
	// posture of snapshots restored from the §6.6 history encoding
	// (which predates this field and carries no watched set).
	watched map[NodeKind]bool

	generation uint64
}

// Generation is the snapshot's swap counter: 1 for the first
// published snapshot, monotonically increasing. Useful for tests,
// metrics, and cheap "did topology change?" checks.
func (s *Snapshot) Generation() uint64 { return s.generation }

// NumNodes returns the number of nodes in the snapshot (observed
// objects plus referenced-but-unobserved identities).
func (s *Snapshot) NumNodes() int { return len(s.nodes) }

// NumEdges returns the number of directed edges.
func (s *Snapshot) NumEdges() int { return s.edges }

// Lookup resolves a (kind, namespace, name) identity to its NodeID
// in this snapshot. Cluster-scoped kinds pass namespace "".
func (s *Snapshot) Lookup(kind NodeKind, namespace, name string) (NodeID, bool) {
	v, ok := s.ids.Load(nodeKey(kind, namespace, name))
	if !ok {
		return NoNode, false
	}
	id := v.(NodeID)
	if _, present := s.nodes[id]; !present {
		// Interned after this swap, or not part of this snapshot's
		// topology (deleted and garbage-collected).
		return NoNode, false
	}
	return id, true
}

// Resolve returns the Ref behind a NodeID, or ok=false if the node
// is not part of this snapshot.
func (s *Snapshot) Resolve(id NodeID) (Ref, bool) {
	info, ok := s.nodes[id]
	if !ok {
		return Ref{}, false
	}
	namespace, name := splitKey(s.keys[id-1])
	return Ref{
		ID:        id,
		Kind:      info.kind,
		Namespace: namespace,
		Name:      name,
		Observed:  info.observed,
	}, true
}

// Watches reports whether this snapshot's ingest actually observes
// objects of the given kind from the API server (Options.
// WatchedKinds). It is the honesty predicate behind Ref.Observed:
//
//   - Watches(k) && !Observed → the object is genuinely absent (a
//     dangling reference — real triage signal).
//   - !Watches(k) && !Observed → UNKNOWN: the node exists only as a
//     referenced identity because this index never watches the kind;
//     nothing may be claimed about the object's existence.
//
// A snapshot built without WatchedKinds (one-shot full-List graphs,
// history-restored snapshots) watches everything.
func (s *Snapshot) Watches(kind NodeKind) bool {
	if s.watched == nil {
		return true
	}
	return s.watched[kind]
}

// Out returns the outbound edges of id ("id --Kind--> Edge.To"),
// sorted by (Kind, To). The slice aliases snapshot storage — do not
// modify. Nil for unknown nodes or nodes without outbound edges.
func (s *Snapshot) Out(id NodeID) []Edge { return s.out[id] }

// In returns the inbound edges of id ("Edge.To --Kind--> id"),
// sorted by (Kind, To). The slice aliases snapshot storage — do not
// modify.
func (s *Snapshot) In(id NodeID) []Edge { return s.in[id] }
