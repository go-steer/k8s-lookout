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

package k8sevents

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

func controllerOf(apiVersion, kind, name string, uid types.UID) []metav1.OwnerReference {
	yes := true
	return []metav1.OwnerReference{{APIVersion: apiVersion, Kind: kind, Name: name, UID: uid, Controller: &yes}}
}

func ownedPod(name string, uid types.UID, owners []metav1.OwnerReference) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "checkout", UID: uid, OwnerReferences: owners,
	}}
}

func ownedRS(name string, uid types.UID, owners []metav1.OwnerReference) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "checkout", UID: uid, OwnerReferences: owners,
	}}
}

func podEvent(name string, uid types.UID) *corev1.Event {
	ev := crashEvent(name + ".evt")
	ev.InvolvedObject.Name = name
	ev.InvolvedObject.UID = uid
	return ev
}

// resolverOver builds the resolver over static caches — the listers
// the informers would back, minus the informers.
func resolverOver(t *testing.T, withRS bool, objs ...any) *ownerResolver {
	t.Helper()
	pods := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	rss := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, o := range objs {
		var err error
		switch o.(type) {
		case *corev1.Pod:
			err = pods.Add(o)
		case *appsv1.ReplicaSet:
			err = rss.Add(o)
		}
		if err != nil {
			t.Fatalf("index %T: %v", o, err)
		}
	}
	r := &ownerResolver{pods: corev1listers.NewPodLister(pods)}
	if withRS {
		r.replicaSets = appsv1listers.NewReplicaSetLister(rss)
	}
	return r
}

func TestOwnerResolver_ControllerRef(t *testing.T) {
	t.Parallel()
	rsOwned := ownedPod("checkout-5d9f8c7b6-q2x7p", "pod-1", controllerOf("apps/v1", "ReplicaSet", "checkout-5d9f8c7b6", "rs-1"))
	byDeployment := ownedRS("checkout-5d9f8c7b6", "rs-1", controllerOf("apps/v1", "Deployment", "checkout", "dep-1"))

	cases := []struct {
		name   string
		withRS bool
		objs   []any
		ev     *corev1.Event
		want   string
	}{{
		name:   "ReplicaSet pod walks to its Deployment",
		withRS: true,
		objs:   []any{rsOwned, byDeployment},
		ev:     podEvent(rsOwned.Name, "pod-1"),
		want:   "Deployment/checkout",
	}, {
		name:   "ReplicaSet not in the cache reports the ReplicaSet",
		withRS: true,
		objs:   []any{rsOwned},
		ev:     podEvent(rsOwned.Name, "pod-1"),
		want:   "ReplicaSet/checkout-5d9f8c7b6",
	}, {
		name:   "no ReplicaSet grant reports the ReplicaSet",
		withRS: false,
		objs:   []any{rsOwned, byDeployment},
		ev:     podEvent(rsOwned.Name, "pod-1"),
		want:   "ReplicaSet/checkout-5d9f8c7b6",
	}, {
		name:   "a same-name ReplicaSet with another UID is not the owner",
		withRS: true,
		objs:   []any{rsOwned, ownedRS("checkout-5d9f8c7b6", "rs-other", controllerOf("apps/v1", "Deployment", "checkout", "dep-1"))},
		ev:     podEvent(rsOwned.Name, "pod-1"),
		want:   "ReplicaSet/checkout-5d9f8c7b6",
	}, {
		name:   "a bare ReplicaSet stays the owner",
		withRS: true,
		objs:   []any{rsOwned, ownedRS("checkout-5d9f8c7b6", "rs-1", nil)},
		ev:     podEvent(rsOwned.Name, "pod-1"),
		want:   "ReplicaSet/checkout-5d9f8c7b6",
	}, {
		name:   "a CRD calling itself ReplicaSet is not walked",
		withRS: true,
		objs: []any{
			ownedPod("p", "pod-1", controllerOf("example.com/v1", "ReplicaSet", "checkout-5d9f8c7b6", "rs-1")),
			byDeployment,
		},
		ev:   podEvent("p", "pod-1"),
		want: "ReplicaSet/checkout-5d9f8c7b6",
	}, {
		name:   "StatefulSet pod reports its StatefulSet",
		withRS: true,
		objs:   []any{ownedPod("db-0", "pod-1", controllerOf("apps/v1", "StatefulSet", "db", "sts-1"))},
		ev:     podEvent("db-0", "pod-1"),
		want:   "StatefulSet/db",
	}, {
		name:   "Job pod reports its Job",
		withRS: true,
		objs:   []any{ownedPod("migrate-x7", "pod-1", controllerOf("batch/v1", "Job", "migrate", "job-1"))},
		ev:     podEvent("migrate-x7", "pod-1"),
		want:   "Job/migrate",
	}, {
		name:   "bare pod has no owner",
		withRS: true,
		objs:   []any{ownedPod("debug", "pod-1", nil)},
		ev:     podEvent("debug", "pod-1"),
		want:   "",
	}, {
		name:   "pod not in the cache has no owner — never guessed from its name",
		withRS: true,
		objs:   []any{byDeployment},
		ev:     podEvent(rsOwned.Name, "pod-1"),
		want:   "",
	}, {
		name:   "a same-name pod with another UID is a different pod",
		withRS: true,
		objs:   []any{rsOwned, byDeployment},
		ev:     podEvent(rsOwned.Name, "pod-old"),
		want:   "",
	}, {
		name:   "non-pod events carry no owner",
		withRS: true,
		objs:   []any{rsOwned, byDeployment},
		ev: func() *corev1.Event {
			ev := podEvent("checkout-5d9f8c7b6", "rs-1")
			ev.InvolvedObject.Kind = "ReplicaSet"
			return ev
		}(),
		want: "",
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolverOver(t, tc.withRS, tc.objs...).controllerRef(tc.ev); got != tc.want {
				t.Errorf("controllerRef = %q, want %q", got, tc.want)
			}
		})
	}
	var off *ownerResolver
	if got := off.controllerRef(podEvent(rsOwned.Name, "pod-1")); got != "" {
		t.Errorf("nil resolver controllerRef = %q, want empty", got)
	}
}

// TestSource_OwnerResolutionFillsControllerRef runs the source on a
// shared factory the way the sentinel wires it: a crash-loop event on
// a Deployment's pod comes out naming the Deployment, and the
// fingerprint-relevant fields are untouched.
func TestSource_OwnerResolutionFillsControllerRef(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset(
		ownedPod("checkout-svc-7b9d-x4kzq", "abc-123", controllerOf("apps/v1", "ReplicaSet", "checkout-svc-7b9d", "rs-1")),
		ownedRS("checkout-svc-7b9d", "rs-1", controllerOf("apps/v1", "Deployment", "checkout-svc", "dep-1")),
	)
	factory := informers.NewSharedInformerFactory(client, 0)
	src := New(client, 0)
	src.WithFactory(factory)
	src.WithOwnerResolution(true)
	rec := newSignalRecorder()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- src.Run(ctx, rec.emit) }()
	waitArmed(ctx, t, src)
	// Arming does not wait for the owner caches; the test does.
	for typ, ok := range factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			t.Fatalf("cache %v never synced", typ)
		}
	}

	if _, err := client.CoreV1().Events("checkout").Create(ctx, crashEvent("checkout-svc.evt"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create event: %v", err)
	}
	select {
	case <-rec.first:
	case <-ctx.Done():
		t.Fatal("no signal within timeout")
	}
	cancel()
	<-done

	got := rec.snapshot()[0]
	if got.ControllerRef != "Deployment/checkout-svc" {
		t.Errorf("ControllerRef = %q, want Deployment/checkout-svc", got.ControllerRef)
	}
	if got.Kind != engine.KindK8sEvent || got.KindOfObject != "Pod" || got.Key.Reason != "CrashLoopBackOff" {
		t.Errorf("fingerprint inputs changed: kind=%q object=%q reason=%q", got.Kind, got.KindOfObject, got.Key.Reason)
	}
}
