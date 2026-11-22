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

package audit

import (
	"bytes"
	"context"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func TestComposedCommandsCoversEveryNonMetaAuditCommand(t *testing.T) {
	meta := map[string]bool{
		"audit all":        true,
		"audit exemptions": true,
		"audit policy":     true,
	}

	registered := checks.Default().GroupCommands("audit")
	if len(registered) == 0 {
		t.Fatal("expected registered commands in the audit group")
	}

	var wantWithoutExemptions []string
	for _, c := range registered {
		if meta[c.Name] {
			continue
		}
		wantWithoutExemptions = append(wantWithoutExemptions, c.Name)
	}

	gotWithout := composedCommands(Deps{}, false)
	var gotWithoutNames []string
	seen := map[string]bool{}
	for _, c := range gotWithout {
		if seen[c.Name] {
			t.Errorf("duplicate command %q in composedCommands(false)", c.Name)
		}
		seen[c.Name] = true
		gotWithoutNames = append(gotWithoutNames, c.Name)
		if meta[c.Name] {
			t.Errorf("meta command %q must not appear in composedCommands(false)", c.Name)
		}
	}
	for _, name := range wantWithoutExemptions {
		if !slices.Contains(gotWithoutNames, name) {
			t.Errorf("non-meta audit command %q missing from composedCommands(false); got %v", name, gotWithoutNames)
		}
	}

	gotWith := composedCommands(Deps{}, true)
	var gotWithNames []string
	for _, c := range gotWith {
		gotWithNames = append(gotWithNames, c.Name)
	}
	for _, name := range append(slices.Clone(wantWithoutExemptions), "audit exemptions") {
		if !slices.Contains(gotWithNames, name) {
			t.Errorf("audit command %q missing from composedCommands(true); got %v", name, gotWithNames)
		}
	}
	for _, forbidden := range []string{"audit all", "audit policy"} {
		if slices.Contains(gotWithNames, forbidden) {
			t.Errorf("meta command %q must not appear in composedCommands(true)", forbidden)
		}
	}
}

func TestSnapshotSlicesStayImmutableUnderSkipSystemNamespaces(t *testing.T) {
	cs := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ml"}},
		&networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "sys-deny"}},
		&networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "isolate-vllm"}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "sys-role"}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "ml-role"}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "sys-rb"}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "ml-rb"}},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "a-sys-crb"},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Namespace: "kube-system", Name: "default"}},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "z-ml-crb"},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Namespace: "ml", Name: "default"}},
		},
	)
	deps := Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return cs, nil },
	}
	snap := NewSnapshot(cs, metav1.NamespaceAll)
	ctx := WithSnapshot(context.Background(), snap)

	for _, cmd := range []checks.Command{RBACCommand(deps), NetpolCommand(deps)} {
		flags, err := cmd.DefaultFlags(emit.Field{Key: "skip-system-namespaces", Value: "true"})
		if err != nil {
			t.Fatalf("%s DefaultFlags: %v", cmd.Name, err)
		}
		var buf bytes.Buffer
		w, err := emit.NewWriter(&buf, emit.FormatLogfmt)
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if _, err := cmd.Run(ctx, emit.Invocation{
			Scope: emit.Scope{AllNamespaces: true},
			Flags: flags,
			Out:   w,
		}); err != nil {
			t.Fatalf("%s Run: %v", cmd.Name, err)
		}
	}

	nps, _, err := snap.NetworkPolicies(ctx)
	if err != nil || len(nps) != 2 || nps[0].Name != "sys-deny" || nps[1].Name != "isolate-vllm" {
		t.Errorf("Snapshot.NetworkPolicies mutated in place: got %+v, err=%v", nps, err)
	}
	roles, _, err := snap.Roles(ctx)
	if err != nil || len(roles) != 2 || roles[0].Name != "sys-role" || roles[1].Name != "ml-role" {
		t.Errorf("Snapshot.Roles mutated in place: got %+v, err=%v", roles, err)
	}
	rbs, _, err := snap.RoleBindings(ctx)
	if err != nil || len(rbs) != 2 || rbs[0].Name != "sys-rb" || rbs[1].Name != "ml-rb" {
		t.Errorf("Snapshot.RoleBindings mutated in place: got %+v, err=%v", rbs, err)
	}
	crbs, _, err := snap.ClusterRoleBindings(ctx)
	if err != nil || len(crbs) != 2 || crbs[0].Name != "a-sys-crb" || crbs[1].Name != "z-ml-crb" {
		t.Errorf("Snapshot.ClusterRoleBindings mutated in place: got %+v, err=%v", crbs, err)
	}
}
