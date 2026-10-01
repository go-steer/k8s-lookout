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
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// partialFixture is a minimal healthy topology: one Deployment→Pod
// plus a Secret and a ConfigMap, enough that a Secrets-denied load
// still has a workload, a pod, and a non-secret section to render.
func partialFixture() []runtime.Object {
	labels := map[string]string{"app": "api"}
	owner := func(kind, name, uid string) metav1.OwnerReference {
		return metav1.OwnerReference{APIVersion: "apps/v1", Kind: kind, Name: name, UID: types.UID(uid), Controller: ptr(true)}
	}
	return []runtime.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api", UID: "d1"},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}},
			},
		},
		&appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api-xyz", UID: "r1", OwnerReferences: []metav1.OwnerReference{owner("Deployment", "api", "d1")}},
			Spec:       appsv1.ReplicaSetSpec{Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}}},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api-xyz-p1", Labels: labels, OwnerReferences: []metav1.OwnerReference{owner("ReplicaSet", "api-xyz", "r1")}},
			Spec:       corev1.PodSpec{ServiceAccountName: "api"},
		},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "db-creds"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api-config"}},
	}
}

func ptr[T any](v T) *T { return &v }

// denyList makes the fake client return the given apierror for a
// `list` on resource, mimicking an RBAC gap for exactly that kind.
func denyList(cs *fake.Clientset, resource string, err error) {
	cs.PrependReactor("list", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, err
	})
}

func forbidden(resource string) error {
	return apierrors.NewForbidden(schema.GroupResource{Resource: resource}, "", errors.New("denied by test"))
}

// TestListClusterToleratesForbidden is issue #192's core invariant: a
// Secrets-only Forbidden under Tolerate() yields a non-nil partial
// Cluster whose non-secret sections still populate, with secrets
// recorded in Skipped() — the least-privilege posture (a deploy that
// omits `secrets: list` still produces a useful, secret-free bundle).
func TestListClusterToleratesForbidden(t *testing.T) {
	cs := fake.NewClientset(partialFixture()...)
	denyList(cs, "secrets", forbidden("secrets"))

	c, err := LoadCluster(context.Background(), cs, metav1.NamespaceAll, Tolerate())
	if err != nil {
		t.Fatalf("LoadCluster with Tolerate() should not error on a Secrets Forbidden: %v", err)
	}
	if c == nil {
		t.Fatal("LoadCluster returned a nil Cluster")
	}

	// The skipped resource is recorded and nothing else is.
	if got := c.SkippedNote(); got != "secrets" {
		t.Errorf("SkippedNote() = %q, want %q", got, "secrets")
	}

	// The non-secret topology still resolved: the workload and its pod
	// are present in the snapshot.
	wl := emit.WorkloadRef{Kind: "Deployment", Namespace: "prod", Name: "api"}
	if _, err := c.WorkloadNode(wl); err != nil {
		t.Errorf("workload should resolve from the partial load: %v", err)
	}
	pods, err := c.WorkloadPods(wl)
	if err != nil {
		t.Fatalf("WorkloadPods: %v", err)
	}
	if len(pods) != 1 {
		t.Errorf("got %d pods from partial load, want 1", len(pods))
	}
	if c.ix.secrets["prod/db-creds"] != nil {
		t.Error("a denied Secret must not appear in the index")
	}
	if c.ix.configMaps["prod/api-config"] == nil {
		t.Error("the readable ConfigMap should still be indexed")
	}
}

// TestListClusterToleratesNotFound covers the IsNotFound arm (an
// optional CRD-ish resource whose group is absent): skipped, not fatal.
func TestListClusterToleratesNotFound(t *testing.T) {
	cs := fake.NewClientset(partialFixture()...)
	denyList(cs, "ingresses", apierrors.NewNotFound(schema.GroupResource{Group: "networking.k8s.io", Resource: "ingresses"}, ""))

	c, err := LoadCluster(context.Background(), cs, metav1.NamespaceAll, Tolerate())
	if err != nil {
		t.Fatalf("Tolerate() should skip a NotFound list: %v", err)
	}
	if got := c.SkippedNote(); got != "ingresses.networking.k8s.io" {
		t.Errorf("SkippedNote() = %q, want %q", got, "ingresses.networking.k8s.io")
	}
}

// TestLoadClusterUntolerantStillAborts guards the non-Forbidden and
// no-option paths: without Tolerate() a Forbidden aborts (the original
// all-or-nothing contract every non-bundle caller relies on), and even
// with Tolerate() a non-Forbidden/NotFound error still aborts.
func TestLoadClusterUntolerantStillAborts(t *testing.T) {
	t.Run("no option, forbidden aborts", func(t *testing.T) {
		cs := fake.NewClientset(partialFixture()...)
		denyList(cs, "secrets", forbidden("secrets"))
		if _, err := LoadCluster(context.Background(), cs, metav1.NamespaceAll); err == nil {
			t.Fatal("LoadCluster without Tolerate() must still abort on Forbidden")
		}
	})
	t.Run("tolerate, other error aborts", func(t *testing.T) {
		cs := fake.NewClientset(partialFixture()...)
		denyList(cs, "pods", apierrors.NewInternalError(errors.New("boom")))
		if _, err := LoadCluster(context.Background(), cs, metav1.NamespaceAll, Tolerate()); err == nil {
			t.Fatal("a non-Forbidden/NotFound error must abort even under Tolerate()")
		}
	})
}

// TestLoadClusterListsDeselection: Lists() drops a resource up front
// (no List call at all) and records it as skipped, exactly like a
// denial — the --lists=all,-secrets posture.
func TestLoadClusterListsDeselection(t *testing.T) {
	cs := fake.NewClientset(partialFixture()...)
	listed := map[string]bool{}
	cs.PrependReactor("list", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		listed["secrets"] = true
		return false, nil, nil // fall through to tracker
	})

	sel, err := ParseListSelection("all,-secrets")
	if err != nil {
		t.Fatalf("ParseListSelection: %v", err)
	}
	c, err := LoadCluster(context.Background(), cs, metav1.NamespaceAll, Lists(sel))
	if err != nil {
		t.Fatalf("LoadCluster: %v", err)
	}
	if listed["secrets"] {
		t.Error("a deselected resource must not be listed at all")
	}
	if got := c.SkippedNote(); got != "secrets" {
		t.Errorf("SkippedNote() = %q, want %q", got, "secrets")
	}
}

// TestLoadClusterPreflightDropsDenied: the SSAR preflight drops a
// resource the review denies before listing it, and falls back to the
// reactive skip when SSAR itself errors.
func TestLoadClusterPreflightDropsDenied(t *testing.T) {
	t.Run("ssar denies secrets", func(t *testing.T) {
		cs := fake.NewClientset(partialFixture()...)
		cs.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
			r := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview)
			allowed := r.Spec.ResourceAttributes.Resource != "secrets"
			return true, &authorizationv1.SelfSubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: allowed}}, nil
		})
		listedSecrets := false
		cs.PrependReactor("list", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
			listedSecrets = true
			return true, nil, forbidden("secrets")
		})
		c, err := LoadCluster(context.Background(), cs, metav1.NamespaceAll, Tolerate(), Preflight())
		if err != nil {
			t.Fatalf("LoadCluster: %v", err)
		}
		if listedSecrets {
			t.Error("preflight should have dropped secrets before the List call")
		}
		if got := c.SkippedNote(); got != "secrets" {
			t.Errorf("SkippedNote() = %q, want %q", got, "secrets")
		}
	})
	t.Run("ssar not permitted falls back to reactive", func(t *testing.T) {
		cs := fake.NewClientset(partialFixture()...)
		cs.PrependReactor("create", "selfsubjectaccessreviews", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, forbidden("selfsubjectaccessreviews")
		})
		denyList(cs, "secrets", forbidden("secrets"))
		c, err := LoadCluster(context.Background(), cs, metav1.NamespaceAll, Tolerate(), Preflight())
		if err != nil {
			t.Fatalf("SSAR being forbidden must not abort the load: %v", err)
		}
		if got := c.SkippedNote(); got != "secrets" {
			t.Errorf("SkippedNote() = %q, want %q (reactive fallback)", got, "secrets")
		}
	})
}

func TestParseListSelection(t *testing.T) {
	all := LoadClusterListRequirements()
	tests := []struct {
		name string
		spec string
		want []string // requirement.String() in canonical order
		err  bool
	}{
		{name: "all", spec: "all", want: names(all)},
		{name: "empty is all", spec: "", want: names(all)},
		{name: "subtract secrets", spec: "all,-secrets", want: without(all, "secrets")},
		{name: "bare allowlist", spec: "pods,deployments", want: []string{"pods", "deployments.apps"}},
		{name: "grouped name", spec: "endpointslices.discovery.k8s.io", want: []string{"endpointslices.discovery.k8s.io"}},
		{name: "spaces tolerated", spec: " all , -secrets ", want: without(all, "secrets")},
		{name: "unknown errors", spec: "widgets", err: true},
		{name: "unknown subtraction errors", spec: "all,-widgets", err: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseListSelection(tc.spec)
			if tc.err {
				if err == nil {
					t.Fatalf("ParseListSelection(%q) = nil error, want error", tc.spec)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseListSelection(%q): %v", tc.spec, err)
			}
			if g := names(got); !equal(g, tc.want) {
				t.Errorf("ParseListSelection(%q) = %v, want %v", tc.spec, g, tc.want)
			}
		})
	}
}

// TestEdgesStaySilentOnUnreadKinds is #149's correctness half: a
// least-privilege deployment that drops `secrets`/`serviceaccounts`
// (or any other name-lookup list) must get a partial bundle, not one
// that reports every reference to the unread kind as missing. The
// first row loads everything and proves each reference really is
// dangling, so silence in the other rows is the skip, not the fixture.
func TestEdgesStaySilentOnUnreadKinds(t *testing.T) {
	fixture := func() []runtime.Object {
		objs := partialFixture()
		p := objs[2].(*corev1.Pod)
		p.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "gone-pull"}}
		p.Spec.Containers = []corev1.Container{{
			Name:    "c",
			EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "gone-secret"}}}},
		}}
		p.Spec.Volumes = []corev1.Volume{{Name: "cfg", VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "gone-cm"}},
		}}}
		subject := []rbacv1.Subject{{Kind: "ServiceAccount", Name: "api", Namespace: "prod"}}
		return append(objs,
			&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "rb-role"}, Subjects: subject, RoleRef: rbacv1.RoleRef{Kind: "Role", Name: "gone-role"}},
			&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "rb-cr"}, Subjects: subject, RoleRef: rbacv1.RoleRef{Kind: "ClusterRole", Name: "gone-cr"}},
			&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "crb"}, Subjects: subject, RoleRef: rbacv1.RoleRef{Kind: "ClusterRole", Name: "gone-cr"}},
		)
	}
	// What each dangling reference reports when its kind IS read,
	// keyed by finding subject name.
	dangling := map[string]string{
		"gone-pull":   "edge.missing_ref",
		"gone-secret": "edge.missing_ref",
		"gone-cm":     "edge.missing_ref",
		"api":         "edge.missing_ref", // the ServiceAccount
		"rb-role":     "edge.rbac_dangling",
		"rb-cr":       "edge.rbac_dangling",
		"crb":         "edge.rbac_dangling",
	}
	for _, tc := range []struct {
		name   string
		denied []string
		silent []string // subjects that must not be reported
	}{
		{"everything read", nil, nil},
		{"no secrets", []string{"secrets"}, []string{"gone-pull", "gone-secret"}},
		{"no secrets or serviceaccounts", []string{"secrets", "serviceaccounts"}, []string{"gone-pull", "gone-secret", "api"}},
		{"no configmaps", []string{"configmaps"}, []string{"gone-cm"}},
		{"no roles", []string{"roles"}, []string{"rb-role"}},
		{"no clusterroles", []string{"clusterroles"}, []string{"rb-cr", "crb"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := fake.NewClientset(fixture()...)
			for _, r := range tc.denied {
				denyList(cs, r, forbidden(r))
			}
			c, err := LoadCluster(context.Background(), cs, metav1.NamespaceAll, Tolerate())
			if err != nil {
				t.Fatalf("LoadCluster: %v", err)
			}
			fs, err := c.EdgeFindings(emit.WorkloadRef{Kind: "Deployment", Namespace: "prod", Name: "api"}, 0, time.Now())
			if err != nil {
				t.Fatalf("EdgeFindings: %v", err)
			}
			got := map[string]string{}
			for _, f := range fs {
				got[f.Name] = f.Kind
			}
			silent := map[string]bool{}
			for _, n := range tc.silent {
				silent[n] = true
				if k, ok := got[n]; ok {
					t.Errorf("%s reported as %s, but its kind was never read", n, k)
				}
			}
			for n, want := range dangling {
				if !silent[n] && got[n] != want {
					t.Errorf("%s: got %q, want %s — a kind that was read must still be judged", n, got[n], want)
				}
			}
		})
	}
}

func names(reqs []ListRequirement) []string {
	out := make([]string, len(reqs))
	for i, r := range reqs {
		out[i] = r.String()
	}
	return out
}

func without(reqs []ListRequirement, drop string) []string {
	var out []string
	for _, r := range reqs {
		if r.Resource != drop {
			out = append(out, r.String())
		}
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
