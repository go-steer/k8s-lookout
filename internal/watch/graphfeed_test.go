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
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/graph"
)

func testNode(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		UID:    types.UID("node-" + name),
		Labels: map[string]string{corev1.LabelTopologyZone: "us-east1-b"},
	}}
}

func testRS(ns, name, deploy string) *appsv1.ReplicaSet {
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: name, UID: types.UID("rs-" + name),
	}}
	if deploy != "" {
		rs.OwnerReferences = []metav1.OwnerReference{{Kind: "Deployment", Name: deploy}}
	}
	return rs
}

func testPod(ns, name, node, rs, configMap string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID("pod-" + name)},
		Spec:       corev1.PodSpec{NodeName: node},
	}
	if rs != "" {
		pod.OwnerReferences = []metav1.OwnerReference{{Kind: "ReplicaSet", Name: rs}}
	}
	if configMap != "" {
		pod.Spec.Volumes = []corev1.Volume{{
			Name:         "cfg",
			VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: configMap}}},
		}}
	}
	return pod
}

// buildFeedGraph seeds a graphFeed's graph directly (no informers) —
// the resolver-side tests exercise Ancestors over a real pkg/graph
// topology built from typed objects.
func buildFeedGraph(t *testing.T, objs ...any) *graphFeed {
	t.Helper()
	g := &graphFeed{graph: graph.New(graph.Options{SwapInterval: -1})}
	if err := g.graph.Writer().FromObjects(slices.Values(objs)); err != nil {
		t.Fatalf("FromObjects: %v", err)
	}
	return g
}

// TestGraphAncestors_Priority pins the §7.5 key priority over a REAL
// topology: node > owner chain (nearest owner first) > shared
// config > namespace — and Zone excluded (fleet tier, AX's join).
func TestGraphAncestors_Priority(t *testing.T) {
	t.Parallel()
	g := buildFeedGraph(t,
		testNode("gke-a"),
		testRS("shop", "pay-7b9d", "pay"),
		testPod("shop", "pay-1", "gke-a", "pay-7b9d", "db-config"),
		testPod("shop", "pay-2", "gke-a", "pay-7b9d", "db-config"),
	)
	got := g.Ancestors(engine.ObjectRef{Kind: "Pod", Namespace: "shop", Name: "pay-1"})
	want := []engine.Ancestor{
		{Kind: "Node", Name: "gke-a"},
		{Kind: "ReplicaSet", Namespace: "shop", Name: "pay-7b9d"},
		{Kind: "Deployment", Namespace: "shop", Name: "pay"},
		{Kind: "ConfigMap", Namespace: "shop", Name: "db-config"},
		{Kind: "Namespace", Name: "shop"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("Ancestors(pod) =\n %+v, want\n %+v", got, want)
	}
	for _, a := range got {
		if a.Kind == "Zone" {
			t.Error("Zone must never be a storm key (fleet tier is AX's join)")
		}
	}
}

// TestGraphAncestors_SelfAncestor: a Node incident's FIRST candidate
// is the node itself — that is how the root incident joins the storm
// its evicted pods key on (§7.5: "Node X NotReady; 30 pods affected").
func TestGraphAncestors_SelfAncestor(t *testing.T) {
	t.Parallel()
	g := buildFeedGraph(t,
		testNode("gke-a"),
		testPod("shop", "pay-1", "gke-a", "", ""),
	)
	got := g.Ancestors(engine.ObjectRef{Kind: "Node", Name: "gke-a"})
	if len(got) == 0 || got[0] != (engine.Ancestor{Kind: "Node", Name: "gke-a"}) {
		t.Errorf("Ancestors(node) = %+v, want the node itself first", got)
	}
	// And it equals the pods' best key, so they group together.
	pods := g.Ancestors(engine.ObjectRef{Kind: "Pod", Namespace: "shop", Name: "pay-1"})
	if len(pods) == 0 || pods[0] != got[0] {
		t.Errorf("pod best key %+v != node self key %+v", pods, got)
	}
}

// TestGraphAncestors_SharedConfigWithoutInformer: the ConfigMap is
// never watched (documented informer-set decision) yet still keys the
// storm — the Mounts edge is declared by the pod spec, so the
// ancestor exists as a referenced identity.
func TestGraphAncestors_SharedConfigWithoutInformer(t *testing.T) {
	t.Parallel()
	g := buildFeedGraph(t,
		testPod("shop", "a-1", "", "rs-a", "db-config"),
		testPod("shop", "b-1", "", "rs-b", "db-config"),
	)
	a := g.Ancestors(engine.ObjectRef{Kind: "Pod", Namespace: "shop", Name: "a-1"})
	b := g.Ancestors(engine.ObjectRef{Kind: "Pod", Namespace: "shop", Name: "b-1"})
	cm := engine.Ancestor{Kind: "ConfigMap", Namespace: "shop", Name: "db-config"}
	if !slices.Contains(a, cm) || !slices.Contains(b, cm) {
		t.Errorf("both pods must carry the shared ConfigMap key:\n a=%+v\n b=%+v", a, b)
	}
	// Different owners → the config key is what they share.
	if a[0] == b[0] {
		t.Errorf("distinct owners must not share the best key: %+v", a[0])
	}
}

// TestGraphAncestors_NotReadyOrUnknown: before the first snapshot, or
// for objects outside the topology, resolution yields nil — incidents
// proceed per-incident (correlation optimizes session count, never
// gates paging).
func TestGraphAncestors_NotReadyOrUnknown(t *testing.T) {
	t.Parallel()
	cold := &graphFeed{graph: graph.New(graph.Options{SwapInterval: -1})}
	if got := cold.Ancestors(engine.ObjectRef{Kind: "Pod", Namespace: "shop", Name: "pay-1"}); got != nil {
		t.Errorf("not-ready graph must resolve nil, got %+v", got)
	}
	g := buildFeedGraph(t, testNode("gke-a"))
	if got := g.Ancestors(engine.ObjectRef{Kind: "Pod", Namespace: "shop", Name: "missing"}); got != nil {
		t.Errorf("unknown object must resolve nil, got %+v", got)
	}
	if got := g.Ancestors(engine.ObjectRef{Kind: "PodDisruptionBudget", Namespace: "shop", Name: "pdb"}); got != nil {
		t.Errorf("unmapped kind must resolve nil, got %+v", got)
	}
}

// TestGraphFeed_InformerIngest runs the REAL informer path over a
// fake clientset: initial sync via FromObjects (one swap), then
// steady-state Apply deltas — and confirms the feed resolves storm
// keys from live topology.
func TestGraphFeed_InformerIngest(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset(
		testNode("gke-a"),
		testRS("shop", "pay-7b9d", "pay"),
		testPod("shop", "pay-1", "gke-a", "pay-7b9d", ""),
	)
	factory := informers.NewSharedInformerFactory(client, 0)
	feed := newGraphFeed(factory)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- feed.Run(ctx) }()

	waitFor := func(desc string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", desc)
	}
	waitFor("initial snapshot", func() bool {
		_, err := feed.graph.Snapshot()
		return err == nil
	})
	got := feed.Ancestors(engine.ObjectRef{Kind: "Pod", Namespace: "shop", Name: "pay-1"})
	want := []engine.Ancestor{
		{Kind: "Node", Name: "gke-a"},
		{Kind: "ReplicaSet", Namespace: "shop", Name: "pay-7b9d"},
		{Kind: "Deployment", Namespace: "shop", Name: "pay"},
		{Kind: "Namespace", Name: "shop"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("post-sync Ancestors =\n %+v, want\n %+v", got, want)
	}

	// Steady state: a pod created after arming flows through Apply.
	if _, err := client.CoreV1().Pods("shop").Create(ctx, testPod("shop", "pay-2", "gke-a", "pay-7b9d", ""), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	waitFor("delta ingestion of pay-2", func() bool {
		a := feed.Ancestors(engine.ObjectRef{Kind: "Pod", Namespace: "shop", Name: "pay-2"})
		return len(a) > 0 && a[0] == (engine.Ancestor{Kind: "Node", Name: "gke-a"})
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("feed.Run returned %v on clean shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("feed.Run did not return after cancel")
	}
}
