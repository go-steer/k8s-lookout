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

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// TestGraphSnapshotIntervalFlag: default 5m (§6.6 "every ~5 min"),
// non-positive rejected in every mode.
func TestGraphSnapshotIntervalFlag(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--dry-run"})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.validate(); err != nil {
		t.Fatal(err)
	}
	if f.graphSnapshotInterval != 5*time.Minute {
		t.Errorf("default --graph-snapshot-interval = %v, want 5m (§6.6)", f.graphSnapshotInterval)
	}
	for _, bad := range []string{"--graph-snapshot-interval=0", "--graph-snapshot-interval=-1m"} {
		f, err := parseFlags([]string{"--dry-run", bad})
		if err != nil {
			t.Fatal(err)
		}
		if err := f.validate(); err == nil {
			t.Errorf("%s must be rejected", bad)
		}
	}
}

// fakeSnapshotter records PutGraphSnapshot calls.
type fakeSnapshotter struct {
	mu   sync.Mutex
	gens []uint64
}

func (f *fakeSnapshotter) PutGraphSnapshot(_ context.Context, snap *graph.Snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gens = append(f.gens, snap.Generation())
	return nil
}

func (f *fakeSnapshotter) generations() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.gens...)
}

// TestRunGraphHistoryLoop_BaselineAndDedupe: the loop waits out
// ErrNotReady, stores a baseline as soon as the graph is ready, and
// skips ticks whose generation it already stored.
func TestRunGraphHistoryLoop_BaselineAndDedupe(t *testing.T) {
	t.Parallel()
	g := graph.New(graph.Options{SwapInterval: -1})
	fs := &fakeSnapshotter{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runGraphHistoryLoop(ctx, g.Snapshot, fs, 5*time.Millisecond)
	}()

	// Not ready yet: nothing may be stored.
	time.Sleep(30 * time.Millisecond)
	if gens := fs.generations(); len(gens) != 0 {
		t.Fatalf("stored %v before the graph was ready", gens)
	}

	// Publish generation 1 → baseline stored exactly once despite
	// many ticks (generation-deduped).
	if err := g.Writer().Flush(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "baseline snapshot", func() bool { return len(fs.generations()) >= 1 })
	time.Sleep(30 * time.Millisecond)
	if gens := fs.generations(); len(gens) != 1 || gens[0] != 1 {
		t.Fatalf("baseline: got %v, want exactly [1]", gens)
	}

	// A new generation is picked up on the interval cadence.
	if err := g.Writer().Apply(graph.Delta{Op: graph.OpAdd, Object: testNode("n-late")}); err != nil {
		t.Fatal(err)
	}
	if err := g.Writer().Flush(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "second snapshot", func() bool {
		gens := fs.generations()
		return len(gens) >= 2 && gens[len(gens)-1] == 2
	})

	cancel()
	<-done
}

// TestGraphHistory_FeedToStore is the sentinel integration: the REAL
// informer-driven graph feed wired to a REAL SQLite store the way
// realMain wires them — steady-state deltas land in graph_changes,
// the snapshot loop stores a baseline, and GraphAt answers from the
// combination.
func TestGraphHistory_FeedToStore(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "lookout.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()

	client := fake.NewSimpleClientset(testNode("gke-a"), testRS("shop", "pay-7b9d", "pay"))
	factory := informers.NewSharedInformerFactory(client, 0)
	feed := newGraphFeed(factory, st.RecordGraphChange)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = feed.Run(ctx) }()
	go runGraphHistoryLoop(ctx, feed.snapshot, st, 10*time.Millisecond)

	waitFor(t, "initial snapshot", func() bool {
		_, err := feed.snapshot()
		return err == nil
	})

	// Steady-state delta: a pod appears.
	if _, err := client.CoreV1().Pods("shop").Create(ctx,
		testPod("shop", "pay-1", "gke-a", "pay-7b9d", ""), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "graph_changes row", func() bool {
		st.Flush()
		rows, err := st.GraphChanges(ctx, time.Time{}, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			if r.Kind == "Pod" && r.Name == "pay-1" && r.Op == "add" {
				return true
			}
		}
		return false
	})

	// The snapshot loop has stored a baseline; GraphAt(now) resolves
	// a topology containing the pod once its change is replayable.
	waitFor(t, "GraphAt resolves the pod", func() bool {
		snap, err := st.GraphAt(ctx, time.Now())
		if err != nil {
			return false
		}
		_, ok := snap.Lookup(graph.KindPod, "shop", "pay-1")
		return ok
	})
}
