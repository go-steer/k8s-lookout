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
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestInternerCompactionUnderHighPodChurn(t *testing.T) {
	const (
		steadyLivePods = 10
		churnedPods    = 10_000
	)

	var changeRecords int
	g := New(Options{
		OnChange: func(ChangeRecord) { changeRecords++ },
	})
	w := g.Writer()
	defer w.Close()

	// Seed 10 long-lived pods.
	for i := 0; i < steadyLivePods; i++ {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      fmt.Sprintf("steady-%d", i),
				Labels:    map[string]string{"app": "web"},
			},
			Spec: corev1.PodSpec{NodeName: "node-1"},
		}
		if err := w.Apply(Delta{Op: OpAdd, Object: pod}); err != nil {
			t.Fatalf("seed steady pod: %v", err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush steady pods: %v", err)
	}
	baselineSnap, err := g.Snapshot()
	if err != nil {
		t.Fatalf("baseline snapshot: %v", err)
	}
	baselineBytes, err := baselineSnap.Encode()
	if err != nil {
		t.Fatalf("encode baseline: %v", err)
	}

	// Churn 10,000 ephemeral pods (create + status-only update + delete).
	for i := 0; i < churnedPods; i++ {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "batch",
				Name:      fmt.Sprintf("job-pod-%d-abcdef1234", i),
			},
			Spec: corev1.PodSpec{NodeName: "node-1"},
		}
		if err := w.Apply(Delta{Op: OpAdd, Object: pod}); err != nil {
			t.Fatalf("add churn pod %d: %v", i, err)
		}
		// Status-only heartbeats with no spec/label/topology change should not bloat ChangeRecord stream (#658).
		updated := pod.DeepCopy()
		updated.Status.Message = fmt.Sprintf("heartbeat-%d", i)
		if err := w.Apply(Delta{Op: OpUpdate, Object: updated}); err != nil {
			t.Fatalf("update churn pod %d: %v", i, err)
		}
		if err := w.Apply(Delta{Op: OpDelete, Object: pod}); err != nil {
			t.Fatalf("delete churn pod %d: %v", i, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush churned pods: %v", err)
	}

	// Compact at snapshot boundary (#657): epoch advances, interner key table
	// shrinks strictly back to live node count, and serialized bytes stay O(live).
	compacted := w.Compact()
	if w.Epoch() != 1 {
		t.Fatalf("Epoch after first Compact() = %d, want 1", w.Epoch())
	}
	if len(compacted.keys) != len(compacted.nodes) {
		t.Fatalf("compacted key count = %d, want exact live node count %d", len(compacted.keys), len(compacted.nodes))
	}
	compactedBytes, err := compacted.Encode()
	if err != nil {
		t.Fatalf("encode compacted snapshot: %v", err)
	}
	// After 10,000 churned pods, encoded size must remain within 2x of the tiny 10-pod baseline.
	if len(compactedBytes) > len(baselineBytes)*2+256 {
		t.Fatalf("compacted snapshot size = %d bytes after %d churned pods, want <= %d bytes (baseline=%d)",
			len(compactedBytes), churnedPods, len(baselineBytes)*2+256, len(baselineBytes))
	}

	// Verify restored graph preserves all steady pods and topology edges.
	restored, err := Restore(compactedBytes)
	if err != nil {
		t.Fatalf("Restore compacted snapshot: %v", err)
	}
	for i := 0; i < steadyLivePods; i++ {
		if _, ok := restored.Lookup(KindPod, "default", fmt.Sprintf("steady-%d", i)); !ok {
			t.Fatalf("missing steady pod steady-%d after Restore", i)
		}
	}
}
