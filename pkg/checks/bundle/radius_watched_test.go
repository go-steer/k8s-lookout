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

package bundle

import (
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/graph"
)

// TestRadiusFindings_UnwatchedKindUnknownNotMissing pins the M2 drill
// observation 3 rule at the finding producer: over a partial-ingest
// snapshot (the sentinel's live graph watches pods/nodes/replicasets
// only), an unobserved neighbor is claimed missing
// (ReferencedNotFound) ONLY when its kind is watched — a
// referenced-but-unwatched ConfigMap renders as radius.neighbor
// observed=unknown, and a full-List snapshot (no WatchedKinds) keeps
// the missing claim for everything, so CLI bundles are unchanged.
func TestRadiusFindings_UnwatchedKindUnknownNotMissing(t *testing.T) {
	t.Parallel()
	// Pod owned by a ReplicaSet that was never ingested (watched kind,
	// genuinely absent) and mounting a ConfigMap that was never
	// ingested (unwatched kind: existence unknown by construction).
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "fixlab",
			Name:            "payment-1",
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "payment-rs"}},
		},
		Spec: corev1.PodSpec{
			NodeName: "worker-1",
			Volumes: []corev1.Volume{{
				Name: "config",
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "payment-config"},
				}},
			}},
		},
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}}

	findingsOver := func(watched []graph.NodeKind) map[string]emit.Finding {
		t.Helper()
		g := graph.New(graph.Options{SwapInterval: -1, WatchedKinds: watched})
		if err := g.Writer().FromObjects(slices.Values([]any{pod, node})); err != nil {
			t.Fatal(err)
		}
		snap, err := g.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		id, ok := snap.Lookup(graph.KindPod, "fixlab", "payment-1")
		if !ok {
			t.Fatal("pod not in snapshot")
		}
		byName := map[string]emit.Finding{}
		for _, f := range RadiusFindings(snap, id, 2) {
			byName[f.KindOfObject+"/"+f.Name] = f
		}
		return byName
	}

	hasDetail := func(f emit.Finding, key, val string) bool {
		for _, d := range f.Details {
			if d.Key == key && d.Value == val {
				return true
			}
		}
		return false
	}

	// Live-graph posture: pods/nodes/replicasets watched.
	live := findingsOver([]graph.NodeKind{graph.KindPod, graph.KindNode, graph.KindReplicaSet})
	cm, ok := live["ConfigMap/payment-config"]
	if !ok {
		t.Fatalf("ConfigMap neighbor missing from findings: %v", live)
	}
	if cm.Kind != "radius.neighbor" || cm.Reason == "ReferencedNotFound" || cm.Severity != emit.SeverityInfo {
		t.Errorf("unwatched ConfigMap mislabeled: %+v", cm)
	}
	if !hasDetail(cm, "observed", "unknown") {
		t.Errorf("unwatched ConfigMap must carry observed=unknown: %+v", cm)
	}
	rs, ok := live["ReplicaSet/payment-rs"]
	if !ok {
		t.Fatalf("ReplicaSet neighbor missing from findings: %v", live)
	}
	if rs.Kind != "radius.missing" || rs.Reason != "ReferencedNotFound" || rs.Severity != emit.SeverityWarning {
		t.Errorf("watched-but-absent ReplicaSet must keep the missing claim: %+v", rs)
	}
	if nd := live["Node/worker-1"]; nd.Kind != "radius.neighbor" || hasDetail(nd, "observed", "unknown") {
		t.Errorf("observed Node neighbor must stay a plain neighbor: %+v", nd)
	}

	// Full-List posture (CLI one-shot): everything watched → the
	// absent ConfigMap IS a real dangling reference. Unchanged.
	full := findingsOver(nil)
	cm = full["ConfigMap/payment-config"]
	if cm.Kind != "radius.missing" || cm.Reason != "ReferencedNotFound" {
		t.Errorf("full-List snapshot must keep ReferencedNotFound for the absent ConfigMap: %+v", cm)
	}
}
