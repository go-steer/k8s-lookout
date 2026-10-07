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

package state_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// viewRoleGaps are the Lists the built-in `view` ClusterRole refuses
// that `state edges` makes: Secrets and every RBAC object (#546).
var viewRoleGaps = []schema.GroupResource{
	{Resource: "secrets"},
	{Group: "rbac.authorization.k8s.io", Resource: "rolebindings"},
	{Group: "rbac.authorization.k8s.io", Resource: "roles"},
	{Group: "rbac.authorization.k8s.io", Resource: "clusterrolebindings"},
	{Group: "rbac.authorization.k8s.io", Resource: "clusterroles"},
}

// viewRoleCommand is testCommand under a credential bound to `view`:
// every List in viewRoleGaps answers 403 the way the API server does.
func viewRoleCommand(objs ...runtime.Object) checks.Command {
	cs := fake.NewClientset(objs...)
	for _, gr := range viewRoleGaps {
		cs.PrependReactor("list", gr.Resource, func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(gr, "", errors.New(`User "system:serviceaccount:lookout:agent" cannot list resource`))
		})
	}
	return state.EdgesCommand(state.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Now:    func() time.Time { return fixedNow },
	})
}

// TestEdgesDegradeUnderViewRole is #546: Secrets and RBAC objects
// forbidden, the command still exits 0, names each refused List as
// read.unavailable with the reason, stays silent about the Secret it
// could not read (rather than calling it missing), and verifies every
// other edge exactly as it would with full access.
func TestEdgesDegradeUnderViewRole(t *testing.T) {
	c := healthy(t)
	c.cmApp = nil                                           // a readable edge, broken: must still be reported
	c.secDB.Data = map[string][]byte{"passwd": []byte("x")} // an unreadable edge, broken: must not be guessed at

	res := checktest.Run(t, viewRoleCommand(c.objects()...), "--workload="+wl)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0; stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	summary := lines[len(lines)-1]
	if !strings.HasPrefix(summary, "scanned=") || !strings.Contains(summary, " findings=7 ") {
		t.Errorf("summary = %q, want findings=7", summary)
	}
	wantFindings(t, lines[:len(lines)-1], []string{
		`kind=read.unavailable severity=info reason=ListForbidden message="forbidden: list secrets — namespaced, not granted by the built-in view role; grant list on secrets (core) via a ClusterRole or Role, as lookout's shipped ClusterRole does — Secret references (env, envFrom and volume keys, imagePullSecrets, Ingress TLS secrets and their certificate expiry) not verified" workload=Deployment/prod/api resource=secrets`,
		`kind=read.unavailable severity=info reason=ListForbidden message="forbidden: list rolebindings.rbac.authorization.k8s.io — namespaced, not granted by the built-in view role; grant list on rolebindings (rbac.authorization.k8s.io) via a ClusterRole or Role, as lookout's shipped ClusterRole does — RoleBindings naming the workload's ServiceAccount, and their roleRefs, not verified" workload=Deployment/prod/api resource=rolebindings.rbac.authorization.k8s.io`,
		`kind=read.unavailable severity=info reason=ListForbidden message="forbidden: list roles.rbac.authorization.k8s.io — namespaced, not granted by the built-in view role; grant list on roles (rbac.authorization.k8s.io) via a ClusterRole or Role, as lookout's shipped ClusterRole does — RoleBinding roleRefs to Roles not verified" workload=Deployment/prod/api resource=roles.rbac.authorization.k8s.io`,
		`kind=read.unavailable severity=info reason=ListForbidden message="forbidden: list clusterrolebindings.rbac.authorization.k8s.io — cluster-scoped, not granted by the built-in view role; grant list on clusterrolebindings (rbac.authorization.k8s.io) via a ClusterRole, as lookout's shipped ClusterRole does — ClusterRoleBindings naming the workload's ServiceAccount, and their roleRefs, not verified" workload=Deployment/prod/api resource=clusterrolebindings.rbac.authorization.k8s.io`,
		`kind=read.unavailable severity=info reason=ListForbidden message="forbidden: list clusterroles.rbac.authorization.k8s.io — cluster-scoped, not granted by the built-in view role; grant list on clusterroles (rbac.authorization.k8s.io) via a ClusterRole, as lookout's shipped ClusterRole does — roleRefs to ClusterRoles not verified" workload=Deployment/prod/api resource=clusterroles.rbac.authorization.k8s.io`,
		`kind=edge.missing_ref severity=critical namespace=prod kind_of_object=ConfigMap name=app-config reason=CreateContainerConfigError message="configmap app-config not found (env LOG_LEVEL in container api)" workload=Deployment/prod/api container=api env=LOG_LEVEL key=log.level pods=2`,
		`kind=edge.missing_ref severity=critical namespace=prod kind_of_object=ConfigMap name=app-config reason=FailedMount message="configmap app-config not found (volume config)" workload=Deployment/prod/api volume=config key=config.yaml pods=2`,
	})
	checktest.VerifyContract(t, viewRoleCommand(c.objects()...), "--workload="+wl)
}

// TestEdgesServiceEntryUnderViewRole: entered from the Service side
// the pass checks nothing workload-scoped, so only the Secret gap (an
// Ingress's TLS Secret) touches the answer — the RBAC refusals get no
// record, because no check of this mode would have read them.
func TestEdgesServiceEntryUnderViewRole(t *testing.T) {
	c := healthy(t)
	res := checktest.Run(t, viewRoleCommand(c.objects()...), "--workload=Service/prod/api")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0; stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	wantFindings(t, lines[:len(lines)-1], []string{
		`kind=read.unavailable severity=info reason=ListForbidden message="forbidden: list secrets — namespaced, not granted by the built-in view role; grant list on secrets (core) via a ClusterRole or Role, as lookout's shipped ClusterRole does — Ingress TLS secret references and their certificate expiry not verified" workload=Service/prod/api resource=secrets`,
	})
}

// TestEdgesOtherListErrorsStillFail: only a permission gap degrades.
// An API server that errors for any other reason still fails the
// command — exit 1, not a quietly partial answer.
func TestEdgesOtherListErrorsStillFail(t *testing.T) {
	cs := fake.NewClientset(healthy(t).objects()...)
	cs.PrependReactor("list", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("etcd is on fire"))
	})
	cmd := state.EdgesCommand(state.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Now:    func() time.Time { return fixedNow },
	})
	if res := checktest.Run(t, cmd, "--workload="+wl); res.Code != emit.ExitRuntime {
		t.Fatalf("exit %d, want %d (runtime); stdout: %s", res.Code, emit.ExitRuntime, res.Stdout)
	}
}

func TestListForbidden(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
		ok   bool
	}{
		{"core", apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "", errors.New("x")), "forbidden: list secrets — namespaced, not granted by the built-in view role; grant list on secrets (core) via a ClusterRole or Role, as lookout's shipped ClusterRole does", true},
		{"grouped", apierrors.NewForbidden(schema.GroupResource{Group: "rbac.authorization.k8s.io", Resource: "rolebindings"}, "", errors.New("x")), "forbidden: list rolebindings.rbac.authorization.k8s.io — namespaced, not granted by the built-in view role; grant list on rolebindings (rbac.authorization.k8s.io) via a ClusterRole or Role, as lookout's shipped ClusterRole does", true},
		{"wrapped", errors.Join(errors.New("listing secrets"), apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "", errors.New("x"))), "forbidden: list secrets — namespaced, not granted by the built-in view role; grant list on secrets (core) via a ClusterRole or Role, as lookout's shipped ClusterRole does", true},
		{"internal", apierrors.NewInternalError(errors.New("x")), "", false},
		{"nil", nil, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := state.ListForbidden(tc.err)
			if got != tc.want || ok != tc.ok {
				t.Errorf("ListForbidden() = %q, %v; want %q, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}

// TestEdgesUnderExactViewRole pins the records under exactly the
// built-in `view` role (checktest.ViewRole, an allow list transcribed
// from the upstream bootstrap policy): one per refused list that
// touches an edge, none for Nodes, which edge validity never reads.
func TestEdgesUnderExactViewRole(t *testing.T) {
	cs := fake.NewClientset(healthy(t).objects()...)
	checktest.ViewRole(cs)
	cmd := state.EdgesCommand(state.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Now:    func() time.Time { return fixedNow },
	})
	res := checktest.Run(t, cmd, "--workload="+wl)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0; stderr: %s", res.Code, res.Stderr)
	}
	var resources []string
	for _, line := range strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n") {
		if !strings.HasPrefix(line, "kind=read.unavailable ") {
			if !strings.HasPrefix(line, "scanned=") {
				t.Errorf("a healthy workload reported an edge under view: %s", line)
			}
			continue
		}
		resources = append(resources, line[strings.LastIndex(line, "resource=")+len("resource="):])
	}
	want := "ingressclasses.networking.k8s.io,secrets,rolebindings.rbac.authorization.k8s.io,roles.rbac.authorization.k8s.io,clusterrolebindings.rbac.authorization.k8s.io,clusterroles.rbac.authorization.k8s.io,storageclasses.storage.k8s.io"
	if got := strings.Join(resources, ","); got != want {
		t.Errorf("read.unavailable resources = %s\nwant %s", got, want)
	}
}
