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

package objectstate_test

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/kube"
	"github.com/go-steer/k8s-lookout/pkg/sources/objectstate"
)

// TestObjectStateBehindTransformingFactory_NodePressureFires exercises the real
// kube.NewTransformingFactory cache seam end-to-end (#622): a Node experiencing
// MemoryPressure=False -> True through the client-go informer cache must survive
// trimNode and fire objectstate.node_pressure without silence.
func TestObjectStateBehindTransformingFactory_NodePressureFires(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t0 := time.Now().Add(-15 * time.Minute)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gke-node-pressure-1",
			UID:  "uid-node-pressure-1",
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(t0)},
				{
					Type:               corev1.NodeMemoryPressure,
					Status:             corev1.ConditionFalse,
					LastTransitionTime: metav1.NewTime(t0),
				},
			},
		},
	}
	client := fake.NewClientset(node)
	factory := kube.NewTransformingFactory(client)

	var (
		mu   sync.Mutex
		sigs []engine.Signal
	)
	src := objectstate.New(client, objectstate.Config{})
	src.WithFactory(factory)

	errCh := make(chan error, 1)
	go func() {
		errCh <- src.Run(ctx, func(sig engine.Signal) {
			mu.Lock()
			sigs = append(sigs, sig)
			mu.Unlock()
		})
	}()

	syncDeadline := time.Now().Add(3 * time.Second)
	for !src.HasSynced() {
		if time.Now().After(syncDeadline) {
			t.Fatal("timed out waiting for objectstate.HasSynced()")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Transition NodeMemoryPressure False -> True via real watch update (#622).
	updated := node.DeepCopy()
	updated.Status.Conditions[1] = corev1.NodeCondition{
		Type:               corev1.NodeMemoryPressure,
		Status:             corev1.ConditionTrue,
		Reason:             "KubeletHasInsufficientMemory",
		Message:            "memory available below eviction threshold",
		LastTransitionTime: metav1.NewTime(time.Now()),
	}
	if _, err := client.CoreV1().Nodes().UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(sigs)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	cancel()
	<-errCh

	mu.Lock()
	defer mu.Unlock()
	if len(sigs) == 0 {
		t.Fatal("objectstate emitted 0 signals when backed by kube.NewTransformingFactory; Node pressure conditions were stripped (#622)")
	}
	if sigs[0].Kind != objectstate.KindNodePressure {
		t.Fatalf("got signal kind %q, want %q", sigs[0].Kind, objectstate.KindNodePressure)
	}
}
