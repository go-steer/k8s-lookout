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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// What the §6.1 transform DOES is pkg/kube's business and is tested there.
// What this file covers is the one thing pkg/kube cannot see: the sentinel
// builds two factories when a namespace deny list is in force, and only one of
// them comes from kube.NewTransformingFactory. The filtered half has to attach
// kube.Transform by hand, because a constructor that takes no scope cannot
// express one — so it is the half that can silently lose the transform.

// TestNewSharedFactories_BothHalvesTrim runs real informers over a fake API
// server for each shape of the pair and reads the caches through the listers,
// the way a source does.
//
// The split case is the point. Dropping WithTransform from the filtered factory
// would leave every test in pkg/kube green, every scoping test in this package
// green, and the sentinel caching resolved secret values on any cluster that
// passes --exclude-namespace. A secret value is therefore the assertion: §6.1's
// goal is that those never enter process memory, and the cache boundary is
// where that becomes observable.
func TestNewSharedFactories_BothHalvesTrim(t *testing.T) {
	for _, tc := range []struct {
		name    string
		exclude []string
		// Which namespace the fixture pod lives in. It must be one the deny
		// list keeps, or the split case would pass by the pod being absent
		// rather than by it being trimmed.
		namespace string
	}{
		{name: "one factory", namespace: "prod"},
		{name: "split by a deny list", exclude: []string{"kube-system"}, namespace: "prod"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:          "api",
					Namespace:     tc.namespace,
					ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubectl"}},
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name: "app",
					Env: []corev1.EnvVar{
						{Name: "DB_PASSWORD", Value: "hunter2"},
						{Name: "DB_HOST", ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{Key: "host"},
						}},
					},
				}}},
			}
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
				Status: corev1.NodeStatus{
					Images: []corev1.ContainerImage{{Names: []string{"gcr.io/x/y:1"}, SizeBytes: 1 << 20}},
				},
			}

			client := fake.NewSimpleClientset(pod, node)
			factories := newSharedFactories(client, tc.exclude)
			if got := factories.Split(); got != (len(tc.exclude) > 0) {
				t.Fatalf("Split() = %v with exclude=%v", got, tc.exclude)
			}
			podLister := factories.Namespaced.Core().V1().Pods().Lister()
			nodeLister := factories.Cluster.Core().V1().Nodes().Lister()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			factories.Start(ctx.Done())
			factories.Namespaced.WaitForCacheSync(ctx.Done())
			factories.Cluster.WaitForCacheSync(ctx.Done())

			cached, err := podLister.Pods(tc.namespace).Get("api")
			if err != nil {
				t.Fatalf("pod not in cache: %v", err)
			}
			if got := cached.Spec.Containers[0].Env[0].Value; got != "" {
				t.Errorf("secret env value reached the namespaced cache: %q", got)
			}
			// The reference has to survive, or pkg/graph loses its Secret
			// edges. It is the half of the strip that is easy to get wrong.
			if cached.Spec.Containers[0].Env[1].ValueFrom == nil {
				t.Error("ValueFrom was stripped along with the value — graph loses its Secret edge")
			}
			if cached.ManagedFields != nil {
				t.Error("ManagedFields reached the namespaced cache")
			}

			cachedNode, err := nodeLister.Get("node-1")
			if err != nil {
				t.Fatalf("node not in cache: %v", err)
			}
			if cachedNode.Status.Images != nil {
				t.Error("status.images reached the cluster cache — the largest single line item")
			}
		})
	}
}
