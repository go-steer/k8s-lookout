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

package mcpserver

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/delta"
	"github.com/go-steer/k8s-lookout/pkg/checks/health"
	"github.com/go-steer/k8s-lookout/pkg/checks/stab"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// TestCallTool_ForbiddenReadDegradesOnMCPSurface is #546 on the MCP
// surface: under the built-in `view` role (no Secrets, no RBAC
// objects) health and state edges answer as a successful tool result
// carrying the explicit unavailable records — not a tool error, which
// is what an agent connected over MCP used to get instead of an
// answer.
func TestCallTool_ForbiddenReadDegradesOnMCPSurface(t *testing.T) {
	cs := fake.NewClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "api-0",
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "api"}},
		}},
	)
	for _, gr := range []schema.GroupResource{
		{Resource: "secrets"},
		{Group: "rbac.authorization.k8s.io", Resource: "rolebindings"},
		{Group: "rbac.authorization.k8s.io", Resource: "roles"},
		{Group: "rbac.authorization.k8s.io", Resource: "clusterrolebindings"},
		{Group: "rbac.authorization.k8s.io", Resource: "clusterroles"},
	} {
		cs.PrependReactor("list", gr.Resource, func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(gr, "", errors.New("denied by test"))
		})
	}
	client := func(context.Context) (kubernetes.Interface, error) { return cs, nil }
	now := func() time.Time { return time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC) }

	reg := checks.NewRegistry()
	reg.Register(health.New(health.Deps{
		Client:   client,
		Provider: func(context.Context) (cloud.Provider, error) { return cloud.NoProvider, nil },
		Now:      now,
	}))
	reg.Register(state.EdgesCommand(state.Deps{Client: client, Now: now}))
	session := connect(t, New(reg, "test"))

	for _, tc := range []struct {
		tool string
		args map[string]any
		want []string
	}{
		{"k8s_cluster_health", nil, []string{
			`message="forbidden: list secrets — namespaced, not granted by the built-in view role; grant list on secrets (core) via a ClusterRole or Role, as lookout's shipped ClusterRole does — certificate expiry`,
			"category=certs status=unavailable",
		}},
		{"k8s_state_edges", map[string]any{"workload": "Deployment/prod/api"}, []string{
			`kind=read.unavailable severity=info reason=ListForbidden message="forbidden: list secrets — namespaced, not granted by the built-in view role; grant list on secrets (core) via a ClusterRole or Role, as lookout's shipped ClusterRole does`,
			"resource=rolebindings.rbac.authorization.k8s.io",
		}},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			res, err := callTool(t, session, tc.tool, tc.args)
			if err != nil {
				t.Fatalf("tools/call: %v", err)
			}
			got := text(t, res)
			if res.IsError {
				t.Fatalf("a forbidden read became a tool error:\n%s", got)
			}
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("payload missing %q:\n%s", w, got)
				}
			}
			lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
			if !strings.HasPrefix(lines[len(lines)-1], "scanned=") {
				t.Errorf("payload not terminated by the summary line:\n%s", got)
			}
		})
	}
}

// TestCallTool_ExactViewRoleDegradesOnMCPSurface: the commands that
// gained a degraded answer in #546's third round — whole-cluster
// triage delta, state webhooks, state storage and stab drain — answer
// over MCP under exactly the built-in `view` role as a successful tool
// result, each refusal one read.unavailable record naming the grant
// that fixes it, exactly as on the CLI.
func TestCallTool_ExactViewRoleDegradesOnMCPSurface(t *testing.T) {
	cs := fake.NewClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "api-0",
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "api"}},
		}, Spec: corev1.PodSpec{NodeName: "n1"}},
	)
	checktest.ViewRole(cs)
	client := func(context.Context) (kubernetes.Interface, error) { return cs, nil }
	now := func() time.Time { return time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC) }

	reg := checks.NewRegistry()
	reg.Register(delta.New(client))
	reg.Register(state.WebhooksCommand(state.Deps{Client: client, Now: now}))
	reg.Register(state.StorageCommand(state.Deps{Client: client, Now: now}))
	reg.Register(stab.DrainCommand(stab.Deps{Client: client, Now: now,
		Provider: func(context.Context) (cloud.Provider, error) { return cloud.NoProvider, nil }}))
	session := connect(t, New(reg, "test"))

	nodes := `kind=read.unavailable severity=info reason=ListForbidden message="forbidden: list nodes — cluster-scoped, not granted by the built-in view role; grant list on nodes (core) via a ClusterRole, as lookout's shipped ClusterRole does`
	for _, tc := range []struct {
		tool string
		args map[string]any
		want []string
	}{
		{"k8s_triage_delta", nil, []string{nodes + ` — node.* findings not checked" resource=nodes`}},
		{"k8s_admission_webhooks", nil, []string{
			`message="forbidden: list validatingwebhookconfigurations.admissionregistration.k8s.io — cluster-scoped, not granted by the built-in view role; grant list on validatingwebhookconfigurations (admissionregistration.k8s.io) via a ClusterRole, as lookout's shipped ClusterRole does — no admission webhook was audited`,
		}},
		{"k8s_storage_binding", nil, []string{
			"resource=storageclasses.storage.k8s.io",
			"resource=persistentvolumes",
		}},
		{"k8s_drain_blockers", map[string]any{"all_namespaces": true}, []string{nodes, "nodes=1"}},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			res, err := callTool(t, session, tc.tool, tc.args)
			if err != nil {
				t.Fatalf("tools/call: %v", err)
			}
			got := text(t, res)
			if res.IsError {
				t.Fatalf("a forbidden read became a tool error:\n%s", got)
			}
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("payload missing %q:\n%s", w, got)
				}
			}
			lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
			if !strings.HasPrefix(lines[len(lines)-1], "scanned=") {
				t.Errorf("payload not terminated by the summary line:\n%s", got)
			}
		})
	}
}
