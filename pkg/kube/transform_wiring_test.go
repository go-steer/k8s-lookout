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

package kube

import (
	"context"
	"reflect"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

// The transform is attached to a shared factory (§6.1, Phase 2). The tests in
// transform_test.go check what trimPod and trimNode do to an object; these
// check that the objects actually reach them.
//
// That distinction is the whole point of this file. Every strip in the registry
// is dead code if the option is missing from the factory, and nothing else in
// the suite would notice: the unit tests call the trim functions directly, so
// they stay green on a build where no informer has a transform at all.
//
// The sentinel's namespace-filtered factory is the one construction site this
// package cannot see; internal/watch/factories_test.go covers it.

func TestTransform_Dispatches(t *testing.T) {
	pod := fullPod(t)
	if _, err := Transform(pod); err != nil {
		t.Fatalf("Transform(Pod): %v", err)
	}
	if pod.ManagedFields != nil {
		t.Error("Pod was not routed to trimPod")
	}

	node := fullNode(t)
	if _, err := Transform(node); err != nil {
		t.Fatalf("Transform(Node): %v", err)
	}
	if node.Status.Images != nil {
		t.Error("Node was not routed to trimNode")
	}
}

// TestTransform_PassesThroughEverythingElse: a factory serves far more than
// Pods and Nodes, and one transform covers all of them. A type the registry has
// no opinion about must come back byte-identical — not a copy, the same pointer
// — because anything else is an unreviewed mutation of an object some other
// source depends on.
func TestTransform_PassesThroughEverythingElse(t *testing.T) {
	others := []any{
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "svc"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cm"}},
		&corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "ev"}},
		&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "rs"}},
		nil,
	}
	for _, in := range others {
		out, err := Transform(in)
		if err != nil {
			t.Errorf("%T: unexpected error %v", in, err)
		}
		if out != in {
			t.Errorf("%T: transform did not pass the object through unchanged", in)
		}
	}
}

// TestTransform_IsIdempotent pins a client-go requirement, not a preference:
// objects already in the cache can be passed back to Replace(), where a second
// pass would mutate an object other goroutines are reading
// (delta_fifo.go:501-506). A transform that appended to a slice or accumulated
// into a map instead of assigning would violate this, and the symptom would be
// a data race under load rather than a test failure here.
func TestTransform_IsIdempotent(t *testing.T) {
	tests := []struct {
		name string
		obj  func(*testing.T) any
	}{
		{"Pod", func(t *testing.T) any { return fullPod(t) }},
		{"Node", func(t *testing.T) any { return fullNode(t) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			once, err := Transform(tc.obj(t))
			if err != nil {
				t.Fatalf("first pass: %v", err)
			}
			twice, err := Transform(once)
			if err != nil {
				t.Fatalf("second pass: %v", err)
			}
			if !reflect.DeepEqual(once, twice) {
				t.Errorf("a second pass changed the object:\n once: %+v\ntwice: %+v", once, twice)
			}
		})
	}
}

// TestTransform_Tombstone documents a case the transform cannot currently
// reach. DeltaFIFO skips the transformer for a DeletedFinalStateUnknown and for
// a Sync, because the object has already been through it
// (delta_fifo.go:507-516). Pinned rather than assumed: if that changes
// upstream, a tombstone must pass through rather than be mistaken for an object
// and mutated, and the failure otherwise would be a nil-deref in a delete path.
func TestTransform_Tombstone(t *testing.T) {
	tomb := cache.DeletedFinalStateUnknown{Key: "ns/pod", Obj: fullPod(t)}
	out, err := Transform(tomb)
	if err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	got, ok := out.(cache.DeletedFinalStateUnknown)
	if !ok {
		t.Fatalf("tombstone came back as %T", out)
	}
	if got.Key != "ns/pod" {
		t.Errorf("Key = %q, want ns/pod", got.Key)
	}
}

// TestNewTransformingFactory_TrimsOnTheWayIntoTheCache is the wiring proof, and
// the only test in this package that would fail if informers.WithTransform were
// dropped from the constructor. It runs a real informer over a fake API server
// and reads the cache the way a source does — through the lister — rather than
// calling the transform itself.
//
// A secret value is the assertion worth having: §6.1's security goal is that
// resolved secret values never enter process memory, and this is the only test
// that observes that at the cache boundary.
func TestNewTransformingFactory_TrimsOnTheWayIntoTheCache(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:          "api",
			Namespace:     "prod",
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubectl"}},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "app",
				Env: []corev1.EnvVar{
					{Name: "DB_PASSWORD", Value: "hunter2"},
					{Name: "DB_HOST", ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{Key: "host"},
					}},
				},
			}},
		},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node-1",
			Annotations: map[string]string{"ccc_priority_index": "0", "kubectl.kubernetes.io/last-applied": "{}"},
		},
		Status: corev1.NodeStatus{
			Images:     []corev1.ContainerImage{{Names: []string{"gcr.io/x/y:1"}, SizeBytes: 1 << 20}},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady}, {Type: corev1.NodeMemoryPressure}},
		},
	}

	factory := NewTransformingFactory(fake.NewSimpleClientset(pod, node))
	podLister := factory.Core().V1().Pods().Lister()
	nodeLister := factory.Core().V1().Nodes().Lister()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	cached, err := podLister.Pods("prod").Get("api")
	if err != nil {
		t.Fatalf("pod not in cache: %v", err)
	}
	if got := cached.Spec.Containers[0].Env[0].Value; got != "" {
		t.Errorf("secret env value reached the shared cache: %q", got)
	}
	// The reference has to survive, or pkg/graph loses its Secret edges. It
	// is the half of the strip that is easy to get wrong.
	if cached.Spec.Containers[0].Env[1].ValueFrom == nil {
		t.Error("ValueFrom was stripped along with the value — graph loses its Secret edge")
	}
	if cached.ManagedFields != nil {
		t.Error("ManagedFields reached the shared cache")
	}

	cachedNode, err := nodeLister.Get("node-1")
	if err != nil {
		t.Fatalf("node not in cache: %v", err)
	}
	if cachedNode.Status.Images != nil {
		t.Error("status.images reached the shared cache — the largest single line item")
	}
	if _, ok := cachedNode.Annotations["ccc_priority_index"]; !ok {
		t.Error("ccc_priority_index was filtered out — §7.7 preference-rank tracking goes silent")
	}
	if _, ok := cachedNode.Annotations["kubectl.kubernetes.io/last-applied"]; ok {
		t.Error("an unretained annotation survived")
	}
	if !hasCondition(cachedNode.Status.Conditions, corev1.NodeReady) {
		t.Error("NodeReady was dropped")
	}
	if hasCondition(cachedNode.Status.Conditions, corev1.NodeMemoryPressure) {
		t.Error("an unretained condition survived")
	}
}
