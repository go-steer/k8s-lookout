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
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestSnapshot_Watches pins the watched-kind honesty predicate:
// without Options.WatchedKinds every kind reads as watched (the
// full-List one-shot posture, and the posture of history-restored
// snapshots); with it, only the declared kinds do — the input the
// radius unknown-vs-missing distinction is built on.
func TestSnapshot_Watches(t *testing.T) {
	t.Parallel()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "p1"}}

	full := New(Options{SwapInterval: -1})
	if err := full.Writer().FromObjects(slices.Values([]any{pod})); err != nil {
		t.Fatal(err)
	}
	fs, err := full.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []NodeKind{KindPod, KindConfigMap, KindNode, KindSecret} {
		if !fs.Watches(k) {
			t.Errorf("no-WatchedKinds snapshot must watch everything; %v reads unwatched", k)
		}
	}

	partial := New(Options{
		SwapInterval: -1,
		WatchedKinds: []NodeKind{KindPod, KindNode, KindReplicaSet},
	})
	if err := partial.Writer().FromObjects(slices.Values([]any{pod})); err != nil {
		t.Fatal(err)
	}
	ps, err := partial.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []NodeKind{KindPod, KindNode, KindReplicaSet} {
		if !ps.Watches(k) {
			t.Errorf("declared kind %v must read watched", k)
		}
	}
	for _, k := range []NodeKind{KindConfigMap, KindSecret, KindPersistentVolumeClaim, KindDeployment} {
		if ps.Watches(k) {
			t.Errorf("undeclared kind %v must read unwatched", k)
		}
	}

	// The declaration survives incremental publishes, not just the
	// initial sync.
	if err := partial.Writer().Apply(Delta{Op: OpAdd, Object: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}}}); err != nil {
		t.Fatal(err)
	}
	if err := partial.Writer().Flush(); err != nil {
		t.Fatal(err)
	}
	ps2, err := partial.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if ps2.Generation() == ps.Generation() {
		t.Fatal("expected a new snapshot generation")
	}
	if ps2.Watches(KindConfigMap) || !ps2.Watches(KindPod) {
		t.Error("WatchedKinds lost across an incremental publish")
	}
}
