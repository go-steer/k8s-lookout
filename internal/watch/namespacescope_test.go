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
	k8stesting "k8s.io/client-go/testing"
)

// --exclude-namespace as a watch scope (issue #407). These tests are about
// where the deny list is applied, not what it selects: the API server does the
// filtering, and the fake clientset's tracker ignores field selectors
// entirely — so asserting on cache contents here would pass on a build that
// sends no selector at all. What can be proven locally is the request, and
// that is what the operator's audit log shows too.

func TestNamespaceExclusionSelector(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want string
	}{
		{"nil is no selector", nil, ""},
		{"empty is no selector", []string{}, ""},
		{
			// A trailing comma in --exclude-namespace produces an empty
			// element, and `metadata.namespace!=` is a legal selector that
			// matches nothing — it would silently empty the cache.
			"blanks are dropped, not rendered",
			[]string{"", "   "},
			"",
		},
		{"one", []string{"kube-system"}, "metadata.namespace!=kube-system"},
		{
			"sorted regardless of input order",
			[]string{"gmp-system", "kube-system", "argocd"},
			"metadata.namespace!=argocd,metadata.namespace!=gmp-system,metadata.namespace!=kube-system",
		},
		{
			"deduped and trimmed",
			[]string{" kube-system ", "kube-system"},
			"metadata.namespace!=kube-system",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := namespaceExclusionSelector(tc.in); got != tc.want {
				t.Errorf("namespaceExclusionSelector(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestNewSharedFactories_UnsetIsOneFactory pins the no-regression half: without
// the flag the process must keep the exact stream count PR #390 established,
// and the cheapest way to guarantee that is for both roles to be the SAME
// object rather than two equivalent ones.
func TestNewSharedFactories_UnsetIsOneFactory(t *testing.T) {
	f := newSharedFactories(fake.NewSimpleClientset(), nil)
	if f.Namespaced != f.Cluster {
		t.Error("unset deny list built two factories; the node watch is now a second stream for nothing")
	}
	if f.Split() {
		t.Error("Split() is true with no deny list")
	}
}

func TestNewSharedFactories_DenyListSplits(t *testing.T) {
	f := newSharedFactories(fake.NewSimpleClientset(), []string{"kube-system"})
	if f.Namespaced == f.Cluster {
		t.Fatal("deny list did not split the factories; the node LIST would carry metadata.namespace and be rejected")
	}
	if !f.Split() {
		t.Error("Split() is false with a deny list")
	}
}

// TestNewSharedFactories_SelectorReachesNamespacedListsOnly is the wiring proof.
// It is the only test that would fail if WithTweakListOptions were dropped, or
// if the Node informer were moved back onto the filtered factory — and the
// latter is the dangerous one, because the API server answers a node LIST
// carrying metadata.namespace with a BadRequest the reflector retries forever,
// so the symptom in production is an informer that never syncs.
func TestNewSharedFactories_SelectorReachesNamespacedListsOnly(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "prod"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
	)
	f := newSharedFactories(client, []string{"kube-system", "gmp-system"})

	// Registering the informers is what makes the factory list them.
	podInf := f.Namespaced.Core().V1().Pods().Informer()
	nodeInf := f.Cluster.Core().V1().Nodes().Informer()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f.Start(ctx.Done())
	f.Namespaced.WaitForCacheSync(ctx.Done())
	f.Cluster.WaitForCacheSync(ctx.Done())
	_, _ = podInf, nodeInf

	const want = "metadata.namespace!=gmp-system,metadata.namespace!=kube-system"
	var sawPods, sawNodes bool
	for _, a := range client.Actions() {
		la, ok := a.(k8stesting.ListAction)
		if !ok {
			continue
		}
		fields := la.GetListRestrictions().Fields
		got := ""
		if fields != nil {
			got = fields.String()
		}
		switch a.GetResource().Resource {
		case "pods":
			sawPods = true
			if got != want {
				t.Errorf("pod LIST field selector = %q, want %q", got, want)
			}
		case "nodes":
			sawNodes = true
			if got != "" {
				t.Errorf("node LIST carried field selector %q; nodes are cluster-scoped and the API server rejects metadata.namespace on them", got)
			}
		}
	}
	if !sawPods || !sawNodes {
		t.Fatalf("expected both a pod and a node LIST (pods=%v nodes=%v)", sawPods, sawNodes)
	}
}
