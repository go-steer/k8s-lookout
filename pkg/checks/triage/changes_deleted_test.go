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

package triage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

func canary() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "canary"},
		Spec: corev1.PodSpec{
			NodeName:   "n1",
			Containers: []corev1.Container{{Name: "app", Image: "img:canary"}},
		},
	}
}

// seedDeletionLog is #393's reproduction: a pod sharing the target's
// node is created at 10:05 and deleted at 10:12, both inside a window
// ending 10:30, and a pod in another namespace is deleted at 10:14.
func seedDeletionLog(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lookout.db")
	cur := changesT0
	clock := func() time.Time { return cur }

	st, err := store.Open(path, store.WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	g := graph.New(graph.Options{SwapInterval: -1, OnChange: st.RecordGraphChange, Now: clock})
	w := g.Writer()
	replicas := int32(2)
	if err := w.FromObjects(slices.Values([]any{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web"},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		},
		webRS(2),
		webPodV("img:v1", map[string]string{"app": "web"}),
		webCM("v1"),
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}},
		bystander("img:v1"),
	})); err != nil {
		t.Fatal(err)
	}
	snap, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutGraphSnapshot(context.Background(), snap); err != nil {
		t.Fatal(err)
	}

	step := func(minutes int, op graph.Op, obj any) {
		cur = changesT0.Add(time.Duration(minutes) * time.Minute)
		if err := w.Apply(graph.Delta{Op: op, Object: obj}); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	step(5, graph.OpAdd, canary())
	step(12, graph.OpDelete, canary())
	step(14, graph.OpDelete, bystander("img:v1"))
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

var canaryRow = regexp.MustCompile(`(?m)^.*name=canary reason=(\w+).*relation=(\w+).*$`)

func canaryRows(t *testing.T, path, at string) map[string]string {
	t.Helper()
	res := checktest.Run(t, ChangesCommand(noClusterDeps(t)),
		"Deployment/prod/web", "--at="+at, "--store="+path)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr %q", res.Code, res.Stderr)
	}
	if strings.Contains(res.Stdout, "bystander") {
		t.Errorf("a deletion in another namespace leaked into the window:\n%s", res.Stdout)
	}
	out := map[string]string{}
	for _, m := range canaryRow.FindAllStringSubmatch(res.Stdout, -1) {
		out[m[1]] = m[2]
	}
	return out
}

// TestChanges_AtReportsADeletion (#393): a window containing an
// object's creation AND its deletion reports both, with the relation
// the object had while it lived — before the fix it reported neither,
// because the object is absent from the graph at --at.
func TestChanges_AtReportsADeletion(t *testing.T) {
	path := seedDeletionLog(t)

	// Before the delete, the add is visible and says where the pod
	// sat: that relation is what the deletion must keep.
	before := canaryRows(t, path, "2026-07-25T10:08:00Z")
	rel, ok := before["Added"]
	if !ok || rel == "" {
		t.Fatalf("the canary's add is missing before its delete: %v", before)
	}

	after := canaryRows(t, path, "2026-07-25T10:30:00Z")
	if after["Added"] != rel {
		t.Errorf("Added relation = %q after the delete, want %q as before it", after["Added"], rel)
	}
	if after["Deleted"] != rel {
		t.Errorf("Deleted relation = %q, want %q — the relation the pod had while it lived", after["Deleted"], rel)
	}
}

// fakeHistory answers GraphAt from a function and counts the calls.
type fakeHistory struct {
	at    func(time.Time) (*graph.Snapshot, error)
	calls int
}

func (f *fakeHistory) GraphAt(_ context.Context, at time.Time) (*graph.Snapshot, error) {
	f.calls++
	return f.at(at)
}

func deleteRow(ns, name string, at time.Time) store.GraphChange {
	return store.GraphChange{Op: "delete", Kind: "Pod", Namespace: ns, Name: name, At: at}
}

// TestPlaceDeleted_LeavesWhatNoGraphCanPlace: a delete that predates
// the first snapshot, or one from a moment the target did not exist,
// stays unplaced rather than guessed at; a store error is an error.
func TestPlaceDeleted_LeavesWhatNoGraphCanPlace(t *testing.T) {
	wl := emit.WorkloadRef{Kind: "Deployment", Namespace: "prod", Name: "web"}
	rows := []store.GraphChange{deleteRow("prod", "a", changesT0)}

	for _, tc := range []struct {
		name string
		at   func(time.Time) (*graph.Snapshot, error)
	}{
		{"before the first snapshot", func(time.Time) (*graph.Snapshot, error) {
			return nil, fmt.Errorf("%w (asked for then)", store.ErrNoHistory)
		}},
		{"the target did not exist", func(time.Time) (*graph.Snapshot, error) {
			g := graph.New(graph.Options{SwapInterval: -1})
			if err := g.Writer().FromObjects(slices.Values([]any{
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}},
			})); err != nil {
				return nil, err
			}
			return g.Snapshot()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hood := map[string]string{}
			if err := placeDeleted(context.Background(), &fakeHistory{at: tc.at}, rows, hood, wl, 2); err != nil {
				t.Fatalf("placeDeleted: %v", err)
			}
			if len(hood) != 0 {
				t.Errorf("hood = %v, want the delete left unplaced", hood)
			}
		})
	}

	boom := errors.New("disk on fire")
	err := placeDeleted(context.Background(), &fakeHistory{at: func(time.Time) (*graph.Snapshot, error) { return nil, boom }}, rows, map[string]string{}, wl, 2)
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want the store's error", err)
	}
}

// TestPlaceDeleted_ReadsOnlyWhatItMustRead: deletes elsewhere in the
// cluster, and deletes already in the neighborhood, cost no graph
// read; and one read places every pending delete that graph holds.
func TestPlaceDeleted_ReadsOnlyWhatItMustRead(t *testing.T) {
	path := seedDeletionLog(t)
	st, err := store.OpenRead(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	h := &fakeHistory{at: func(at time.Time) (*graph.Snapshot, error) {
		return st.GraphAt(context.Background(), at)
	}}
	wl := emit.WorkloadRef{Kind: "Deployment", Namespace: "prod", Name: "web"}
	at := changesT0.Add(12 * time.Minute)
	rows := []store.GraphChange{
		deleteRow("other", "bystander", at),
		deleteRow("prod", "web-1", at),
		deleteRow("prod", "canary", at),
		{Op: "delete", Kind: "ConfigMap", Namespace: "prod", Name: "cm-app", At: at.Add(time.Minute)},
	}
	hood := map[string]string{refKey("Pod", "prod", "web-1"): "self"}
	if err := placeDeleted(context.Background(), h, rows, hood, wl, 2); err != nil {
		t.Fatal(err)
	}
	if h.calls != 1 {
		t.Errorf("GraphAt calls = %d, want one read to place both the canary and the ConfigMap", h.calls)
	}
	for _, key := range []string{refKey("Pod", "prod", "canary"), refKey("ConfigMap", "prod", "cm-app")} {
		if hood[key] == "" {
			t.Errorf("%s was not placed: %v", key, hood)
		}
	}
	if _, ok := hood[refKey("Pod", "other", "bystander")]; ok {
		t.Errorf("a delete in another namespace was placed: %v", hood)
	}
}
