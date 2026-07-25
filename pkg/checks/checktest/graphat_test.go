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

package checktest

import (
	"bytes"
	"context"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// TestGraphAtCommand_EndToEnd drives the ENTIRE §6.6 read path the
// way the next milestone's commands will: a sentinel-shaped writer
// builds history into a real SQLite store, then the hidden
// graph-backed probe resolves --at through emit.Run — flag gating,
// Scope resolution, read-only open, nearest-snapshot + replay.
func TestGraphAtCommand_EndToEnd(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "lookout.db")
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

	st, err := store.Open(path, store.WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	g := graph.New(graph.Options{
		SwapInterval: -1,
		OnChange:     st.RecordGraphChange,
		Now:          func() time.Time { return now },
	})
	w := g.Writer()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}}
	pod := func(name string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: name},
			Spec:       corev1.PodSpec{NodeName: "n1", Containers: []corev1.Container{{Name: "app", Image: "img:v1"}}},
		}
	}
	if err := w.FromObjects(slices.Values([]any{node, pod("pay-1")})); err != nil {
		t.Fatal(err)
	}
	snap, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutGraphSnapshot(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	snapshotAt := now

	now = now.Add(time.Minute)
	if err := w.Apply(graph.Delta{Op: graph.OpAdd, Object: pod("pay-2")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	afterChange, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	changeAt := now
	if err := st.Close(); err != nil { // flushes the buffered change row
		t.Fatal(err)
	}

	run := func(args ...string) (int, string, string) {
		var out, errBuf bytes.Buffer
		code := emit.Run(context.Background(), GraphAtCommand().RunConfig(&out, &errBuf), args)
		return code, out.String(), errBuf.String()
	}

	// Point-in-time at the change instant: snapshot + one replayed
	// delta.
	code, stdout, stderr := run("--at="+changeAt.Format(time.RFC3339), "--store="+path)
	if code != emit.ExitData {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	wantGen := "generation=" + itoa(afterChange.Generation())
	if !strings.Contains(stdout, "kind=graph.at") || !strings.Contains(stdout, wantGen) {
		t.Errorf("stdout missing resolved finding (%s):\n%s", wantGen, stdout)
	}
	if !strings.Contains(stdout, "\nscanned=") {
		t.Errorf("stdout missing summary line:\n%s", stdout)
	}

	// At the snapshot's own instant: the baseline, no replay.
	code, stdout, stderr = run("--at="+snapshotAt.Format(time.RFC3339), "--store="+path)
	if code != emit.ExitData {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	if want := "generation=" + itoa(snap.Generation()); !strings.Contains(stdout, want) {
		t.Errorf("snapshot-instant resolution: want %s in\n%s", want, stdout)
	}

	// Before all history: runtime error carrying the no-history
	// diagnostic, clean stdout.
	code, stdout, stderr = run("--at="+snapshotAt.Add(-time.Hour).Format(time.RFC3339), "--store="+path)
	if code != emit.ExitRuntime {
		t.Fatalf("pre-history --at: exit %d (stdout %q)", code, stdout)
	}
	if !strings.Contains(stderr, "no graph history") {
		t.Errorf("stderr: %q", stderr)
	}
	if stdout != "" {
		t.Errorf("stdout must stay clean on failure, got %q", stdout)
	}

	// Live invocation (no --at): runs without touching the store.
	code, stdout, stderr = run()
	if code != emit.ExitData {
		t.Fatalf("live run: exit %d, stderr %q", code, stderr)
	}
	if !strings.HasPrefix(stdout, "scanned=0 findings=0") {
		t.Errorf("live run summary: %q", stdout)
	}

	// --at without --store: usage error naming the requirement.
	code, _, stderr = run("--at=20m")
	if code != emit.ExitUsage || !strings.Contains(stderr, "--at requires --store") {
		t.Errorf("gating: exit %d, stderr %q", code, stderr)
	}
}

func itoa(v uint64) string { return strconv.FormatUint(v, 10) }
