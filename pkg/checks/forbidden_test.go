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

package checks_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"

	"github.com/go-steer/k8s-lookout/pkg/checks"
)

// TestRefusalWording pins the one sentence every refused read renders
// as (#546): what was refused, its scope, whether the built-in view
// role is the reason (only when it really is), and the grant that
// fixes it (naming the shipped ClusterRole only when it really grants
// it).
func TestRefusalWording(t *testing.T) {
	for _, tc := range []struct {
		name string
		r    checks.Refusal
		want string
	}{
		{"cluster-scoped, outside view, shipped",
			checks.Refused("list", "", "nodes"),
			"forbidden: list nodes — cluster-scoped, not granted by the built-in view role; grant list on nodes (core) via a ClusterRole, as lookout's shipped ClusterRole does"},
		{"namespaced, outside view, shipped",
			checks.Refused("list", "", "secrets"),
			"forbidden: list secrets — namespaced, not granted by the built-in view role; grant list on secrets (core) via a ClusterRole or Role, as lookout's shipped ClusterRole does"},
		{"inside view: a custom role is to blame, not view",
			checks.Refused("list", "", "pods"),
			"forbidden: list pods — namespaced, this identity lacks it; grant list on pods (core) via a ClusterRole or Role, as lookout's shipped ClusterRole does"},
		{"inside view, not shipped",
			checks.Refused("list", "", "resourcequotas"),
			"forbidden: list resourcequotas — namespaced, this identity lacks it; grant list on resourcequotas (core) via a ClusterRole or Role"},
		{"grouped, cluster-scoped, not shipped",
			checks.Refused("list", "storage.k8s.io", "volumeattachments"),
			"forbidden: list volumeattachments.storage.k8s.io — cluster-scoped, not granted by the built-in view role; grant list on volumeattachments (storage.k8s.io) via a ClusterRole"},
		{"a verb the shipped role lacks",
			checks.Refused("get", "", "nodes"),
			"forbidden: get nodes — cluster-scoped, not granted by the built-in view role; grant get on nodes (core) via a ClusterRole"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.r.String(); got != tc.want {
				t.Errorf("String()\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

// TestListForbiddenReadsTheServerVerb: the verb comes from the API
// server's own message, so a refused get is not reported as a list.
func TestListForbiddenReadsTheServerVerb(t *testing.T) {
	gr := schema.GroupResource{Resource: "nodes"}
	get := apierrors.NewForbidden(gr, "n1", errors.New(`User "u" cannot get resource "nodes" in API group "" at the cluster scope`))
	r, ok := checks.ForbiddenRefusal(fmt.Errorf("fetching Node n1: %w", get))
	if !ok || r != checks.Refused("get", "", "nodes") {
		t.Fatalf("ForbiddenRefusal = %+v, %v; want get nodes", r, ok)
	}
	bare := apierrors.NewForbidden(gr, "", errors.New("x"))
	if got, ok := checks.ListForbidden(bare); !ok || got != checks.Refused("list", "", "nodes").String() {
		t.Errorf("ListForbidden(no verb in message) = %q, %v; want the list wording", got, ok)
	}
	if _, ok := checks.ListForbidden(apierrors.NewInternalError(errors.New("x"))); ok {
		t.Error("a non-Forbidden error classified as a refusal")
	}
}

// TestShippedRoleGrantsMatchManifest pins checks.ShippedRoleGrants to
// deploy/12-clusterrole-watcher.yaml, so "as lookout's shipped
// ClusterRole does" is never a false promise and never a missed one.
func TestShippedRoleGrantsMatchManifest(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "12-clusterrole-watcher.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var role rbacv1.ClusterRole
	if err := yaml.Unmarshal(raw, &role); err != nil {
		t.Fatal(err)
	}
	fromYAML := map[schema.GroupResource][]string{}
	for _, rule := range role.Rules {
		for _, g := range rule.APIGroups {
			for _, res := range rule.Resources {
				gr := schema.GroupResource{Group: g, Resource: res}
				fromYAML[gr] = append(fromYAML[gr], rule.Verbs...)
			}
		}
	}
	for gr, verbs := range fromYAML {
		slices.Sort(verbs)
		got := slices.Clone(checks.ShippedRoleGrants[gr])
		slices.Sort(got)
		if !slices.Equal(got, verbs) {
			t.Errorf("%s: ShippedRoleGrants has %v, the manifest grants %v", gr, got, verbs)
		}
	}
	for gr := range checks.ShippedRoleGrants {
		if _, ok := fromYAML[gr]; !ok {
			t.Errorf("%s: in ShippedRoleGrants but not granted by the manifest", gr)
		}
	}
}

// TestWordRefusalKeepsTheCallersContext: rewording a wrapped Forbidden
// keeps the wrapper's text ahead of the shared wording, so the
// diagnostic still says which read was refused, and the result is
// still a *RefusalError wrapping the API error (#584).
func TestWordRefusalKeepsTheCallersContext(t *testing.T) {
	gr := schema.GroupResource{Resource: "pods"}
	api := apierrors.NewForbidden(gr, "", errors.New(`User "u" cannot list resource "pods" in API group ""`))
	tail := checks.Refused("list", "", "pods").String() + " — the command cannot answer without it"
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"wrapped", fmt.Errorf("workload Deployment/prod/api: listing pods: %w", api), "workload Deployment/prod/api: listing pods: " + tail},
		{"bare", api, tail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := checks.WordRefusal(tc.err)
			if got.Error() != tc.want {
				t.Errorf("Error()\n got: %s\nwant: %s", got, tc.want)
			}
			var re *checks.RefusalError
			if !errors.As(got, &re) || !apierrors.IsForbidden(got) {
				t.Errorf("lost the RefusalError or the API error: %#v", got)
			}
		})
	}
}
