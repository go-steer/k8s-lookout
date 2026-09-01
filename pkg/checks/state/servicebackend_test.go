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

package state

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// backendDeployment builds a Deployment/ReplicaSet pair carrying
// labels, with replicas pods — zero of them for the scale-to-zero
// case #366 is actually about.
func backendDeployment(name string, labels map[string]string, replicas int) []runtime.Object {
	owner := func(kind, oname, uid string) metav1.OwnerReference {
		return metav1.OwnerReference{APIVersion: "apps/v1", Kind: kind, Name: oname, UID: types.UID(uid), Controller: ptr(true)}
	}
	tpl := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}}
	objs := []runtime.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: name, UID: types.UID("d-" + name)},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr(int32(replicas)),
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: tpl,
			},
		},
		&appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod", Name: name + "-rs", UID: types.UID("r-" + name),
				OwnerReferences: []metav1.OwnerReference{owner("Deployment", name, "d-"+name)},
			},
			Spec: appsv1.ReplicaSetSpec{Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: tpl},
		},
	}
	for i := range replicas {
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod", Name: name + "-rs-p" + string(rune('a'+i)), Labels: labels,
				OwnerReferences: []metav1.OwnerReference{owner("ReplicaSet", name+"-rs", "r-"+name)},
			},
		})
	}
	return objs
}

func backendService(name string, selector map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: name},
		Spec:       corev1.ServiceSpec{Selector: selector, Ports: []corev1.ServicePort{{Port: 80}}},
	}
}

func loadBackendCluster(t *testing.T, objs ...runtime.Object) *Cluster {
	t.Helper()
	c, err := LoadCluster(context.Background(), fake.NewClientset(objs...), metav1.NamespaceAll)
	if err != nil {
		t.Fatalf("LoadCluster: %v", err)
	}
	return c
}

// TestServiceBackend is the resolver behind issue #366: a Service is
// not a workload, but its selector names one, and that is the
// workload an endpoints_empty signal is about. The case the issue was
// filed on is the one with no pods left — the selector still names
// the workload, and the pod TEMPLATES are what proves it.
func TestServiceBackend(t *testing.T) {
	t.Parallel()
	api := map[string]string{"app": "api"}

	t.Run("through the live pods", func(t *testing.T) {
		t.Parallel()
		objs := append(backendDeployment("api", api, 2), backendService("api", api))
		c := loadBackendCluster(t, objs...)

		wl, ok := c.ServiceBackend("prod", "api")
		if !ok {
			t.Fatal("ServiceBackend: not resolved")
		}
		// The pods roll up past their ReplicaSet: the bundle target
		// is the thing an operator scales, not the generation of it.
		if want := (emit.WorkloadRef{Kind: "Deployment", Namespace: "prod", Name: "api"}); wl != want {
			t.Errorf("ServiceBackend = %v, want %v", wl, want)
		}
	})

	t.Run("scaled to zero, through the templates", func(t *testing.T) {
		t.Parallel()
		// The #366 repro exactly: scale the backing Deployment to
		// zero, endpoints_empty fires, and there is not one pod left
		// carrying the labels the selector matches.
		objs := append(backendDeployment("api", api, 0), backendService("api", api))
		c := loadBackendCluster(t, objs...)

		wl, ok := c.ServiceBackend("prod", "api")
		if !ok {
			t.Fatal("ServiceBackend with no pods: not resolved — this is the case #366 is about")
		}
		// The Deployment and its ReplicaSet BOTH carry the template,
		// so this only has one answer because the rollup happens
		// before the ambiguity check.
		if want := (emit.WorkloadRef{Kind: "Deployment", Namespace: "prod", Name: "api"}); wl != want {
			t.Errorf("ServiceBackend = %v, want %v", wl, want)
		}
	})

	t.Run("a bare pod is its own backend", func(t *testing.T) {
		t.Parallel()
		// No controller, so no pod template anywhere: only the live
		// pods can answer this, which is why that branch exists and
		// runs first.
		c := loadBackendCluster(t,
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "solo", Labels: api}},
			backendService("api", api))

		wl, ok := c.ServiceBackend("prod", "api")
		if !ok {
			t.Fatal("ServiceBackend: not resolved")
		}
		if want := (emit.WorkloadRef{Kind: "Pod", Namespace: "prod", Name: "solo"}); wl != want {
			t.Errorf("ServiceBackend = %v, want %v", wl, want)
		}
	})

	t.Run("two workloads behind one selector is no answer", func(t *testing.T) {
		t.Parallel()
		// A blue/green cutover: both halves carry app=api. Naming one
		// would attach the bundle to the wrong half half the time.
		objs := append(backendDeployment("api-blue", api, 1), backendDeployment("api-green", api, 1)...)
		objs = append(objs, backendService("api", api))
		c := loadBackendCluster(t, objs...)

		if wl, ok := c.ServiceBackend("prod", "api"); ok {
			t.Errorf("ServiceBackend resolved an ambiguous Service to %v, want no answer", wl)
		}
	})

	t.Run("two workloads and no pods is also no answer", func(t *testing.T) {
		t.Parallel()
		objs := append(backendDeployment("api-blue", api, 0), backendDeployment("api-green", api, 0)...)
		objs = append(objs, backendService("api", api))
		c := loadBackendCluster(t, objs...)

		if wl, ok := c.ServiceBackend("prod", "api"); ok {
			t.Errorf("ServiceBackend resolved an ambiguous template match to %v, want no answer", wl)
		}
	})

	t.Run("a selector that matches nothing", func(t *testing.T) {
		t.Parallel()
		// The typo'd selector `state edges` reports as
		// edge.selector_empty: there is no backend, and guessing the
		// best fit is that check's job, not a bundle target's.
		objs := append(backendDeployment("api", api, 1), backendService("api", map[string]string{"app": "ap1"}))
		c := loadBackendCluster(t, objs...)

		if wl, ok := c.ServiceBackend("prod", "api"); ok {
			t.Errorf("ServiceBackend = %v, want no answer for a selector that matches nothing", wl)
		}
	})

	t.Run("a partial label match is not a match", func(t *testing.T) {
		t.Parallel()
		// Selector semantics are AND: app=api,tier=web selects only
		// pods carrying both. A workload with just app=api is not the
		// backend.
		objs := append(backendDeployment("api", api, 0),
			backendService("api", map[string]string{"app": "api", "tier": "web"}))
		c := loadBackendCluster(t, objs...)

		if wl, ok := c.ServiceBackend("prod", "api"); ok {
			t.Errorf("ServiceBackend = %v, want no answer: the selector needs tier=web too", wl)
		}
	})

	t.Run("a selectorless Service has no backend to name", func(t *testing.T) {
		t.Parallel()
		// An ExternalName or manually-managed-endpoints Service. Its
		// endpoints are someone else's business by construction.
		objs := append(backendDeployment("api", api, 1), backendService("api", nil))
		c := loadBackendCluster(t, objs...)

		if wl, ok := c.ServiceBackend("prod", "api"); ok {
			t.Errorf("ServiceBackend = %v, want no answer for a selectorless Service", wl)
		}
	})

	t.Run("a Service in another namespace does not match", func(t *testing.T) {
		t.Parallel()
		objs := append(backendDeployment("api", api, 0), backendService("api", api))
		c := loadBackendCluster(t, objs...)

		if wl, ok := c.ServiceBackend("staging", "api"); ok {
			t.Errorf("ServiceBackend = %v, want no answer for a Service that does not exist", wl)
		}
	})
}
