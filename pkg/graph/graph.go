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

// Package graph is the in-memory topology index of DESIGN.md §6: a
// directed, typed graph centered on the Pod, connecting the
// traffic/policy layers above it (Ingress → Service/EndpointSlice →
// NetworkPolicy → Pod) to the infrastructure below (Containers,
// ConfigMaps/Secrets, PVCs, Node, Zone).
//
// It is a *graph*, not a DAG — selector relationships and shared
// mounts create cycles across layers; every traversal here carries a
// visited-set and nothing assumes acyclicity.
//
// # Concurrency model (§6.2)
//
// Single-writer, copy-on-write readers. One ingest loop owns a
// Writer; it batches ADD/UPDATE/DELETE deltas of typed Kubernetes
// objects and publishes an immutable *Snapshot through an
// atomic.Pointer at most once per swap interval. Readers call
// Graph.Snapshot and query it without ever taking a lock; a snapshot
// is internally consistent forever (the writer never mutates
// published state). The interface is shaped for the compact
// representation (uint32 NodeIDs, typed edge kinds); the v1
// implementation behind it is plain Go maps — CSR packing is a
// profile-triggered rewrite (§15 Q5) gated on the benchmarks in this
// package (see docs/graph-q5-gate.md).
//
// # What is stored
//
// Only identities and typed edges. Object specs, statuses and — in
// particular — secret *values* are never stored: ingesting a
// *corev1.Secret reads nothing but ObjectMeta (§6.5).
//
// # History (§6.6)
//
// The live graph carries no persistence of its own, but it produces
// everything history needs: snapshots serialize to a versioned
// compressed binary (history.go) and — when Options.OnChange is set —
// every applied delta emits a ChangeRecord carrying a changed-field
// summary for `triage changes` plus a replayable effect for `--at`
// point-in-time reconstruction (changes.go, replay.go). Storing and
// querying those artifacts is pkg/store's job.
package graph

import (
	"errors"
	"sync/atomic"
	"time"
)

// ErrNotReady is returned by Graph.Snapshot before the first swap:
// initial sync has not completed, and per §6.3 the read path serves
// nothing (rather than a misleading empty graph) until it has.
var ErrNotReady = errors.New("graph: not ready: no snapshot published yet (initial sync pending)")

// DefaultSwapInterval is the default upper bound on snapshot publish
// frequency (§6.3: "swap at most every few hundred ms — readers
// don't need per-event freshness").
const DefaultSwapInterval = 300 * time.Millisecond

// Options configures a Graph.
type Options struct {
	// SwapInterval bounds how often the writer publishes a new
	// snapshot: deltas arriving via Writer.Apply are batched and
	// swapped in at most once per interval. Zero means
	// DefaultSwapInterval. Negative disables the timer entirely —
	// snapshots are published only by explicit Writer.Flush /
	// Writer.FromObjects calls (used by tests and benchmarks for
	// deterministic swaps).
	SwapInterval time.Duration

	// OnChange, when set, turns on the §6.6 delta log: every delta
	// applied via Writer.Apply produces one ChangeRecord (identity,
	// changed-field summary, replay effect), delivered here right
	// after the snapshot containing it is published. Called on the
	// writer goroutine under the writer mutex — the hook must be
	// fast and non-blocking (the sentinel hands records to the
	// store's buffered writer). Initial sync (FromObjects) seeds
	// change tracking but emits no records: the first stored
	// snapshot covers that state.
	OnChange func(ChangeRecord)

	// Now is the clock stamped into ChangeRecord.At. Nil means
	// time.Now; tests inject a fake for deterministic history.
	Now func() time.Time
}

// Graph is the shared handle: the writer publishes snapshots into
// it, readers take them out. Safe for concurrent use; readers never
// block the writer or each other.
type Graph struct {
	snap   atomic.Pointer[Snapshot]
	writer *Writer
}

// New constructs an empty Graph. The graph is not ready (Snapshot
// returns ErrNotReady) until the writer publishes the first
// snapshot — normally via FromObjects at the end of initial sync.
func New(opts Options) *Graph {
	interval := opts.SwapInterval
	if interval == 0 {
		interval = DefaultSwapInterval
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	g := &Graph{}
	g.writer = newWriter(g, interval, opts.OnChange, now)
	return g
}

// Snapshot returns the current immutable topology snapshot, or
// ErrNotReady if the writer has not completed initial sync. The
// returned snapshot never changes; hold it for as long as a
// consistent view is needed.
func (g *Graph) Snapshot() (*Snapshot, error) {
	s := g.snap.Load()
	if s == nil {
		return nil, ErrNotReady
	}
	return s, nil
}

// Writer returns the graph's single writer. There is exactly one per
// Graph; the ingest loop that owns it is the only party that may
// call its mutating methods (they are internally serialized, but the
// design is single-writer — see §6.2).
func (g *Graph) Writer() *Writer {
	return g.writer
}
