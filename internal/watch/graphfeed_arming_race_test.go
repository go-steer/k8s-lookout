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

package watch

// Issue #107 — graphfeed arming race.
//
// graphFeed.Run does its arming in two steps (graphfeed.go, the block
// that today reads lines 183-192):
//
//	g.mu.Lock()
//	buffered := g.buf
//	g.buf = nil
//	g.armed = true          // <-- armed becomes observable to enqueue
//	g.mu.Unlock()           // <-- lock dropped
//	if len(buffered) > 0 {
//	    w.ApplyInitial(buffered...)   // <-- replay happens AFTER the drop
//	}
//
// enqueue takes g.mu, and once g.armed is true it drops the lock and
// calls g.graph.Writer().Apply(d) directly. So a live delta that
// arrives in the window between the Unlock above and the ApplyInitial
// replay is queued into the writer's pending list BEFORE the buffered
// (initial-sync) deltas. Because the writer folds pending deltas in
// FIFO order on Flush and an upsert is last-writer-wins, a STALE
// buffered delta for object X then clobbers a NEWER live delta for the
// same X — the graph ends up describing pre-outage topology.
//
// Invariant under test: every buffered (initial-sync) delta must be
// applied to the writer BEFORE any live delta, so no live change for an
// object can be overwritten by that object's replayed buffered state.
//
// ---------------------------------------------------------------------
// armAndReplay contract (the production seam this test drives).
//
// The coder is expected to extract the arm+replay block above into:
//
//	func (g *graphFeed) armAndReplay(w *graph.Writer) error
//
// Contract:
//   - Drains the buffer and replays it under a single critical section:
//     the buffered deltas MUST be handed to w.ApplyInitial BEFORE g.armed
//     becomes observable to a concurrent enqueue (i.e. the drain, the
//     ApplyInitial replay, and the arm are not separated by a window in
//     which enqueue can take its live Apply path). Concretely: hold g.mu
//     across the ApplyInitial replay, or otherwise guarantee that no
//     enqueue can Apply a live delta until every buffered delta has
//     already been queued into the writer.
//   - Replays ALL buffered deltas, in buffer (FIFO) order, before
//     returning; drains until the buffer is empty.
//   - Returns any error from w.ApplyInitial (propagated as today via the
//     "storm: replay buffered graph deltas" wrap at the call site).
//   - A no-op replay (empty buffer) still arms.
//
// The verbatim extraction of lines 183-192 satisfies "replays all
// buffered deltas" but NOT the ordering guarantee — it arms and drops
// the lock before replaying — so this test fails against it and passes
// only once the arm+replay is made atomic w.r.t. enqueue.
//
// NOTE ON DETERMINISM: the losing interleave lives entirely inside
// armAndReplay (between its lock-drop and its replay), and there is no
// seam the test can wedge open there without touching production. So
// this is a concurrency STRESS test, not a strictly deterministic one:
// it widens the race window by padding the buffer with many filler
// deltas (making the ApplyInitial replay validate/queue for a while
// after arming) and races a live update against it over many
// iterations, failing if the stale buffered state wins even once. It is
// run under `go test -race`, though the defect is a logical
// apply-ordering race (both paths funnel through the writer's own mutex,
// so there is no memory data race for -race to flag) — the assertion
// below is the detector.

import (
	"sync"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/graph"
)

// podNodeFromAncestors returns the Node ancestor name for a pod, i.e.
// the node the winning delta placed it on ("" if none resolved).
func podNodeFromAncestors(g *graphFeed, ns, name string) string {
	for _, a := range g.Ancestors(engine.ObjectRef{Kind: "Pod", Namespace: ns, Name: name}) {
		if a.Kind == "Node" {
			return a.Name
		}
	}
	return ""
}

// TestGraphFeed_ArmAndReplay_LiveDeltaNotClobberedByBufferedReplay
// reproduces issue #107: a live delta for pod X (placing it on the NEW
// node) that lands while Run is arming must not be overwritten by pod
// X's STALE buffered delta (the OLD node) replayed on top.
//
// Drives the armAndReplay seam directly. Fails to COMPILE until the
// coder adds `func (g *graphFeed) armAndReplay(w *graph.Writer) error`;
// once added as a verbatim extraction it fails the assertion (stale
// node wins in the race window); it passes only when arm+replay is
// atomic w.r.t. enqueue.
func TestGraphFeed_ArmAndReplay_LiveDeltaNotClobberedByBufferedReplay(t *testing.T) {
	t.Parallel()

	const (
		ns         = "shop"
		podName    = "pay-1"
		oldNode    = "gke-old" // stale placement, in the buffered replay
		newNode    = "gke-new" // live placement, must win
		filler     = 2000      // pad the buffer to widen the replay window
		iterations = 60
	)

	for i := 0; i < iterations; i++ {
		g := &graphFeed{graph: graph.New(graph.Options{SwapInterval: -1})}
		w := g.graph.Writer()

		// Buffer (pre-arm, so enqueue appends to g.buf): the STALE
		// placement of pod X plus filler nodes that lengthen the
		// ApplyInitial replay so the live delta can slip in after arming.
		g.enqueue(graph.OpAdd, testPod(ns, podName, oldNode, "", ""))
		for j := 0; j < filler; j++ {
			g.enqueue(graph.OpAdd, testNode(fillerNodeName(j)))
		}

		// Live writer: as soon as arming makes g.armed observable, apply
		// the NEWER placement of pod X (node = newNode) via the live path.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				g.mu.Lock()
				armed := g.armed
				g.mu.Unlock()
				if armed {
					break
				}
			}
			g.enqueue(graph.OpUpdate, testPod(ns, podName, newNode, "", ""))
		}()

		if err := g.armAndReplay(w); err != nil {
			t.Fatalf("iter %d: armAndReplay: %v", i, err)
		}
		wg.Wait()

		if err := w.Flush(); err != nil {
			t.Fatalf("iter %d: Flush: %v", i, err)
		}

		if got := podNodeFromAncestors(g, ns, podName); got != newNode {
			t.Fatalf("iter %d: pod %s/%s placed on %q; want %q — a stale "+
				"buffered delta replayed on top of the newer live delta "+
				"(issue #107 arming race)", i, ns, podName, got, newNode)
		}
	}
}

func fillerNodeName(i int) string {
	// Distinct node names so each is its own graph node (widens replay).
	const digits = "0123456789"
	b := []byte("fill-0000")
	b[8] = digits[i%10]
	b[7] = digits[(i/10)%10]
	b[6] = digits[(i/100)%10]
	b[5] = digits[(i/1000)%10]
	return string(b)
}
