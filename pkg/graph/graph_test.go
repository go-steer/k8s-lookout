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

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// buildGraph constructs a manually flushed graph from objs via the
// initial-sync path and returns it with its first snapshot.
func buildGraph(t testing.TB, objs []any) (*Graph, *Snapshot) {
	t.Helper()
	g := New(Options{SwapInterval: -1})
	if err := g.Writer().FromObjects(slices.Values(objs)); err != nil {
		t.Fatalf("FromObjects: %v", err)
	}
	s, err := g.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot after FromObjects: %v", err)
	}
	return g, s
}

// refString renders a node as "Kind/ns/name" (with a "!" prefix when
// unobserved) for edge-set comparisons that are stable across graph
// instances with different NodeID assignments.
func refString(s *Snapshot, id NodeID) string {
	r, ok := s.Resolve(id)
	if !ok {
		return fmt.Sprintf("?<%d>", id)
	}
	prefix := ""
	if !r.Observed {
		prefix = "!"
	}
	return prefix + r.Kind.String() + "/" + r.Namespace + "/" + r.Name
}

// dumpEdges renders every edge as "From -Kind-> To" strings.
func dumpEdges(s *Snapshot) map[string]bool {
	m := make(map[string]bool)
	for id := range s.nodes {
		for _, e := range s.Out(id) {
			m[refString(s, id)+" -"+e.Kind.String()+"-> "+refString(s, e.To)] = true
		}
	}
	return m
}

// dumpObserved renders every observed node.
func dumpObserved(s *Snapshot) map[string]bool {
	m := make(map[string]bool)
	for id, info := range s.nodes {
		if info.observed {
			m[refString(s, id)] = true
		}
	}
	return m
}

// checkInvariants asserts snapshot-internal consistency: out/in
// symmetry, sorted adjacency, edge count, and that every edge
// endpoint resolves.
func checkInvariants(t testing.TB, s *Snapshot) {
	t.Helper()
	count := 0
	for id, edges := range s.out {
		if !slices.IsSortedFunc(edges, func(a, b Edge) int {
			if a.Kind != b.Kind {
				return int(a.Kind) - int(b.Kind)
			}
			return int(a.To) - int(b.To)
		}) {
			t.Fatalf("out[%d] not sorted: %v", id, edges)
		}
		for _, e := range edges {
			count++
			if _, ok := s.nodes[id]; !ok {
				t.Fatalf("edge source %d not in nodes", id)
			}
			if _, ok := s.nodes[e.To]; !ok {
				t.Fatalf("edge target %d (%s from %s) not in nodes", e.To, e.Kind, refString(s, id))
			}
			if _, present := findEdge(s.in[e.To], Edge{To: id, Kind: e.Kind}); !present {
				t.Fatalf("missing reverse edge for %s -%s-> %s", refString(s, id), e.Kind, refString(s, e.To))
			}
		}
	}
	inCount := 0
	for id, edges := range s.in {
		for _, e := range edges {
			inCount++
			if _, present := findEdge(s.out[e.To], Edge{To: id, Kind: e.Kind}); !present {
				t.Fatalf("in edge %s <-%s- %s has no out counterpart", refString(s, id), e.Kind, refString(s, e.To))
			}
		}
	}
	if count != s.NumEdges() || inCount != s.NumEdges() {
		t.Fatalf("edge count mismatch: out=%d in=%d NumEdges=%d", count, inCount, s.NumEdges())
	}
}

func TestSnapshotNotReadyBeforeFirstSwap(t *testing.T) {
	g := New(Options{SwapInterval: -1})
	if _, err := g.Snapshot(); !errors.Is(err, ErrNotReady) {
		t.Fatalf("Snapshot before first swap: want ErrNotReady, got %v", err)
	}
	// Queued deltas alone must not make the graph ready.
	if err := g.Writer().Apply(Delta{Op: OpAdd, Object: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "a"}}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, err := g.Snapshot(); !errors.Is(err, ErrNotReady) {
		t.Fatalf("Snapshot with pending deltas: want ErrNotReady, got %v", err)
	}
	if err := g.Writer().Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	s, err := g.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot after flush: %v", err)
	}
	if s.Generation() != 1 || s.NumNodes() != 1 {
		t.Fatalf("got generation=%d nodes=%d, want 1/1", s.Generation(), s.NumNodes())
	}
}

func TestFromObjectsEmptyIsReadyAndExplicit(t *testing.T) {
	g := New(Options{SwapInterval: -1})
	if err := g.Writer().FromObjects(slices.Values([]any(nil))); err != nil {
		t.Fatalf("FromObjects: %v", err)
	}
	s, err := g.Snapshot()
	if err != nil {
		t.Fatalf("empty initial sync must still publish: %v", err)
	}
	if s.NumNodes() != 0 || s.Generation() != 1 {
		t.Fatalf("got nodes=%d gen=%d, want 0/1", s.NumNodes(), s.Generation())
	}
}

func TestFromObjectsRejectsUnsupportedTypeWithoutSwap(t *testing.T) {
	g := New(Options{SwapInterval: -1})
	objs := []any{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "a"}},
		"not a k8s object",
	}
	if err := g.Writer().FromObjects(slices.Values(objs)); err == nil {
		t.Fatal("FromObjects with unsupported type: want error")
	}
	if _, err := g.Snapshot(); !errors.Is(err, ErrNotReady) {
		t.Fatalf("failed initial sync must not publish: got %v", err)
	}
}

func TestFromObjectsPublishesExactlyOneSwap(t *testing.T) {
	_, s := buildGraph(t, synthCluster(1, 200))
	if s.Generation() != 1 {
		t.Fatalf("initial sync generation = %d, want exactly 1 swap", s.Generation())
	}
	checkInvariants(t, s)
}

func TestApplyValidatesBeforeQueueing(t *testing.T) {
	g := New(Options{SwapInterval: -1})
	w := g.Writer()
	err := w.Apply(
		Delta{Op: OpAdd, Object: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "a"}}},
		Delta{Op: OpAdd, Object: 42},
	)
	if err == nil {
		t.Fatal("Apply with unsupported object: want error")
	}
	if err := w.Apply(Delta{Op: 99, Object: &corev1.Namespace{}}); err == nil {
		t.Fatal("Apply with invalid op: want error")
	}
	// Nothing was queued: flush publishes an empty snapshot.
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	s, _ := g.Snapshot()
	if s.NumNodes() != 0 {
		t.Fatalf("invalid Apply leaked state: %d nodes", s.NumNodes())
	}
}

func TestBatchingWindowSwapsAtMostOncePerInterval(t *testing.T) {
	g := New(Options{SwapInterval: 30 * time.Millisecond})
	w := g.Writer()
	defer w.Close()
	if err := w.FromObjects(slices.Values([]any(nil))); err != nil {
		t.Fatalf("FromObjects: %v", err)
	}
	base, _ := g.Snapshot()

	// Burst of deltas: nothing may be visible before the window
	// elapses, then everything arrives in one swap.
	for i := range 10 {
		err := w.Apply(Delta{Op: OpAdd, Object: &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("ns-%d", i)},
		}})
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	s, _ := g.Snapshot()
	if s.Generation() != base.Generation() {
		t.Fatalf("swap happened synchronously inside the batching window")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		s, _ = g.Snapshot()
		if s.Generation() > base.Generation() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timer flush never fired")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if s.Generation() != base.Generation()+1 {
		t.Fatalf("burst produced %d swaps, want 1", s.Generation()-base.Generation())
	}
	if s.NumNodes() != 10 {
		t.Fatalf("got %d nodes after batched swap, want 10", s.NumNodes())
	}
}

func TestWriterCloseRejectsFurtherWork(t *testing.T) {
	g := New(Options{SwapInterval: -1})
	w := g.Writer()
	w.Close()
	if err := w.Apply(Delta{Op: OpAdd, Object: &corev1.Namespace{}}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Apply after Close: want ErrClosed, got %v", err)
	}
	if err := w.Flush(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Flush after Close: want ErrClosed, got %v", err)
	}
	if err := w.FromObjects(slices.Values([]any(nil))); !errors.Is(err, ErrClosed) {
		t.Fatalf("FromObjects after Close: want ErrClosed, got %v", err)
	}
}

func TestNodeIDsStableAcrossDeleteAndRecreate(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "p1"}}
	g, s := buildGraph(t, []any{pod})
	id1, ok := s.Lookup(KindPod, "ns", "p1")
	if !ok {
		t.Fatal("pod not found")
	}
	w := g.Writer()
	if err := w.Apply(Delta{Op: OpDelete, Object: pod}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	s2, _ := g.Snapshot()
	if _, ok := s2.Lookup(KindPod, "ns", "p1"); ok {
		t.Fatal("deleted, unreferenced pod still present")
	}
	if err := w.Apply(Delta{Op: OpAdd, Object: pod}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	s3, _ := g.Snapshot()
	id2, ok := s3.Lookup(KindPod, "ns", "p1")
	if !ok {
		t.Fatal("re-created pod not found")
	}
	if id1 != id2 {
		t.Fatalf("NodeID changed across delete/re-create: %d → %d", id1, id2)
	}
}

func TestOldSnapshotsRemainConsistentAfterSwaps(t *testing.T) {
	objs := synthCluster(2, 300)
	g, s1 := buildGraph(t, objs)
	before := dumpEdges(s1)
	w := g.Writer()
	// Churn: delete a swath of pods.
	deleted := 0
	for _, o := range objs {
		if p, ok := o.(*corev1.Pod); ok && deleted < 50 {
			if err := w.Apply(Delta{Op: OpDelete, Object: p}); err != nil {
				t.Fatal(err)
			}
			deleted++
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	s2, _ := g.Snapshot()
	if s2.NumEdges() >= s1.NumEdges() {
		t.Fatalf("deletes did not shrink the new snapshot: %d → %d edges", s1.NumEdges(), s2.NumEdges())
	}
	// The old snapshot must be byte-for-byte what it was.
	after := dumpEdges(s1)
	if len(before) != len(after) {
		t.Fatalf("published snapshot mutated by later swap: %d → %d edges", len(before), len(after))
	}
	for e := range before {
		if !after[e] {
			t.Fatalf("edge vanished from old snapshot: %s", e)
		}
	}
	checkInvariants(t, s1)
	checkInvariants(t, s2)
}

func TestSecretValuesNeverStored(t *testing.T) {
	// §6.5: the graph holds names only, never secret payloads. The
	// synthetic cluster plants a canary value in every Secret's
	// data; it must not appear in any graph storage: interner keys
	// (the only string storage there is).
	_, s := buildGraph(t, synthCluster(3, 500))
	for _, key := range s.keys {
		if strings.Contains(key, secretCanary) {
			t.Fatalf("secret value leaked into graph storage: %q", key)
		}
	}
	// And no node identity of kind Secret carries anything beyond
	// namespace/name.
	for id, info := range s.nodes {
		if info.kind != KindSecret {
			continue
		}
		if r, _ := s.Resolve(id); strings.Contains(r.Name, secretCanary) {
			t.Fatalf("secret value in node name: %q", r.Name)
		}
	}
}

func TestDanglingReferenceLifecycle(t *testing.T) {
	// A pod mounting a ConfigMap that doesn't exist keeps a
	// referenced-but-unobserved node alive (that IS the finding);
	// deleting the pod garbage-collects it.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "p1"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "cfg",
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "ghost"},
				}},
			}},
		},
	}
	g, s := buildGraph(t, []any{pod})
	id, ok := s.Lookup(KindConfigMap, "ns", "ghost")
	if !ok {
		t.Fatal("referenced ConfigMap node missing")
	}
	r, _ := s.Resolve(id)
	if r.Observed {
		t.Fatal("never-synced ConfigMap reported as observed")
	}
	w := g.Writer()
	if err := w.Apply(Delta{Op: OpDelete, Object: pod}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	s2, _ := g.Snapshot()
	if _, ok := s2.Lookup(KindConfigMap, "ns", "ghost"); ok {
		t.Fatal("unreferenced, unobserved node not garbage-collected")
	}
	if _, ok := s2.Lookup(KindPod, "ns", "p1"); ok {
		t.Fatal("deleted pod not garbage-collected")
	}
}
