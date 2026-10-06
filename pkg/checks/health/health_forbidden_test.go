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

package health_test

import (
	"context"
	"errors"
	"slices"
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
	"github.com/go-steer/k8s-lookout/pkg/checks/health"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// viewRole is what the built-in `view` ClusterRole refuses among the
// reads health makes: Secrets, and every RBAC object (#546).
var viewRole = []schema.GroupResource{
	{Resource: "secrets"},
	{Group: "rbac.authorization.k8s.io", Resource: "rolebindings"},
	{Group: "rbac.authorization.k8s.io", Resource: "roles"},
	{Group: "rbac.authorization.k8s.io", Resource: "clusterrolebindings"},
	{Group: "rbac.authorization.k8s.io", Resource: "clusterroles"},
}

// deniedCommand is testCommand with each listed resource's List
// answering err the way the API server would.
func deniedCommand(deny []schema.GroupResource, mkErr func(schema.GroupResource) error, objs ...runtime.Object) checks.Command {
	cs := fake.NewClientset(objs...)
	for _, gr := range deny {
		cs.PrependReactor("list", gr.Resource, func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, mkErr(gr)
		})
	}
	return health.New(health.Deps{
		Client:   func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Provider: func(context.Context) (cloud.Provider, error) { return cloud.NoProvider, nil },
		Now:      func() time.Time { return fixedNow },
	})
}

func forbidden(gr schema.GroupResource) error {
	return apierrors.NewForbidden(gr, "", errors.New(`User "system:serviceaccount:lookout:agent" cannot list resource`))
}

// categoryLines indexes a run's scorecard lines by category.
func categoryLines(t *testing.T, stdout string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, line := range strings.Split(strings.TrimSuffix(stdout, "\n"), "\n") {
		if rec := parseLine(t, line); rec["kind"] == "health.category" {
			out[rec["category"]] = line
		}
	}
	return out
}

// TestViewRoleDegradesCertsOnly is #546's health half: under `view`
// the scan exits 0, certs answers unavailable with the forbidden
// reason, services still scores and names its one blind spot, and
// every other category's line — and its details — is exactly what a
// full-access credential sees.
func TestViewRoleDegradesCertsOnly(t *testing.T) {
	objs := brokenObjects(t)
	view := checktest.Run(t, deniedCommand(viewRole, forbidden, objs...))
	if view.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0; stderr: %s", view.Code, view.Stderr)
	}
	full := checktest.Run(t, testCommand(objs...))

	got, want := categoryLines(t, view.Stdout), categoryLines(t, full.Stdout)
	if len(got) != 12 {
		t.Fatalf("want 12 scorecard lines, got %d:\n%s", len(got), view.Stdout)
	}
	if line := `kind=health.category severity=info reason=Unavailable message="forbidden: list secrets — certificate expiry is read from the tls.crt of each kubernetes.io/tls Secret; grant list on secrets to score it" category=certs status=unavailable`; got["certs"] != line {
		t.Errorf("certs line:\n got: %s\nwant: %s", got["certs"], line)
	}
	if line := want["services"] + ` unverified="Ingress TLS secret references (forbidden: list secrets)"`; got["services"] != line {
		t.Errorf("services line:\n got: %s\nwant: %s", got["services"], line)
	}
	for cat, line := range want {
		if cat == "certs" || cat == "services" {
			continue
		}
		if got[cat] != line {
			t.Errorf("%s changed under view:\n got: %s\nwant: %s", cat, got[cat], line)
		}
	}

	// Details: everything but the certs category's, line for line.
	viewLines := strings.Split(view.Stdout, "\n")
	for _, line := range strings.Split(strings.TrimSuffix(full.Stdout, "\n"), "\n") {
		rec := parseLine(t, line)
		if rec["kind"] == "health.category" || rec["category"] == "certs" || strings.HasPrefix(line, "scanned=") {
			continue
		}
		if !slices.Contains(viewLines, line) {
			t.Errorf("detail lost under view: %s", line)
		}
	}
	if strings.Contains(view.Stdout, "kind=cert.") {
		t.Errorf("a cert finding survived a refused Secrets list:\n%s", view.Stdout)
	}
	checktest.VerifyContract(t, deniedCommand(viewRole, forbidden, objs...))
}

// TestForbiddenCoreReadMakesCategoryUnavailable: the same rule for the
// other categories with a read of their own — a refused webhook
// configuration list, or a refused Services list, answers unavailable
// with the reason; the scan still exits 0.
func TestForbiddenCoreReadMakesCategoryUnavailable(t *testing.T) {
	for _, tc := range []struct {
		cat  string
		deny schema.GroupResource
		want string
	}{
		{"webhooks", schema.GroupResource{Group: "admissionregistration.k8s.io", Resource: "validatingwebhookconfigurations"},
			"forbidden: list validatingwebhookconfigurations.admissionregistration.k8s.io — the webhook audit reads the admission webhook configurations and the Services behind them"},
		{"services", schema.GroupResource{Resource: "services"},
			"forbidden: list services — Service routing is judged from Services and the pods their selectors match"},
		{"nodes", schema.GroupResource{Resource: "nodes"},
			"forbidden: list nodes — node conditions are read from the cluster-scoped Node objects, which the built-in view role does not grant"},
		{"quota", schema.GroupResource{Resource: "resourcequotas"},
			"forbidden: list resourcequotas — quota pressure is read from the ResourceQuotas in scope"},
		{"disruption", schema.GroupResource{Group: "policy", Resource: "poddisruptionbudgets"},
			"forbidden: list poddisruptionbudgets.policy — disruption readiness is read from the PodDisruptionBudgets in scope"},
		{"rollouts", schema.GroupResource{Group: "batch", Resource: "cronjobs"},
			"forbidden: list cronjobs.batch — rollouts are read from the Deployments, StatefulSets, DaemonSets, Jobs and CronJobs in scope"},
		{"storage", schema.GroupResource{Resource: "persistentvolumeclaims"},
			"forbidden: list persistentvolumeclaims — PVC health needs the PersistentVolumeClaims in scope"},
	} {
		t.Run(tc.cat, func(t *testing.T) {
			status, kinds, reasons := statusesAndKinds(t, deniedCommand([]schema.GroupResource{tc.deny}, forbidden, brokenObjects(t)...))
			if status[tc.cat] != "unavailable" || reasons[tc.cat] != tc.want {
				t.Errorf("%s = %s %q; want unavailable %q", tc.cat, status[tc.cat], reasons[tc.cat], tc.want)
			}
			if len(kinds[tc.cat]) > 0 {
				t.Errorf("an unavailable category carried details: %v", kinds[tc.cat])
			}
			if status["certs"] != "degraded" {
				t.Errorf("certs = %s; an unrelated refusal must not touch it", status["certs"])
			}
		})
	}
}

// TestNonForbiddenReadErrorStillFails: only a permission gap degrades.
// Any other error from a category's read still fails the scan (exit 1)
// — a broken API server is not a coverage note.
func TestNonForbiddenReadErrorStillFails(t *testing.T) {
	internal := func(schema.GroupResource) error { return apierrors.NewInternalError(errors.New("etcd is on fire")) }
	for _, gr := range []schema.GroupResource{{Resource: "secrets"}, {Resource: "services"}} {
		res := checktest.Run(t, deniedCommand([]schema.GroupResource{gr}, internal, brokenObjects(t)...))
		if res.Code != emit.ExitRuntime {
			t.Errorf("%s internal error: exit %d, want %d", gr.Resource, res.Code, emit.ExitRuntime)
		}
	}
	for _, gr := range []schema.GroupResource{{Resource: "nodes"}, {Group: "policy", Resource: "poddisruptionbudgets"}} {
		res := checktest.Run(t, deniedCommand([]schema.GroupResource{gr}, internal, brokenObjects(t)...))
		if res.Code != emit.ExitRuntime {
			t.Errorf("%s internal error: exit %d, want %d", gr.Resource, res.Code, emit.ExitRuntime)
		}
	}
}

// viewCommand is testCommand under a credential bound to exactly the
// built-in `view` ClusterRole (checktest.ViewRole: everything `view`
// does not grant is refused — nodes, secrets, the RBAC kinds,
// IngressClasses, webhook configurations, any other cluster-scoped
// read).
func viewCommand(objs ...runtime.Object) (checks.Command, func() []schema.GroupResource) {
	cs := fake.NewClientset(objs...)
	refused := checktest.ViewRole(cs)
	return health.New(health.Deps{
		Client:   func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Provider: func(context.Context) (cloud.Provider, error) { return cloud.NoProvider, nil },
		Now:      func() time.Time { return fixedNow },
	}), refused
}

// TestHealthUnderExactViewRole is the #546 follow-up: `lookout health`
// under nothing but `view` exits 0 with a useful scorecard. The four
// categories whose reads `view` refuses answer unavailable with the
// reason (control-plane is unavailable for want of a provider, as on
// any vanilla build); the other eight score exactly as they do with
// full access, details included, and services names its blind spots.
func TestHealthUnderExactViewRole(t *testing.T) {
	objs := brokenObjects(t)
	cmd, refused := viewCommand(objs...)
	view := checktest.Run(t, cmd)
	if view.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0; stderr: %s", view.Code, view.Stderr)
	}
	full := checktest.Run(t, testCommand(objs...))
	got, want := categoryLines(t, view.Stdout), categoryLines(t, full.Stdout)

	unavailable := map[string]string{
		"nodes":    "forbidden: list nodes — node conditions are read from the cluster-scoped Node objects, which the built-in view role does not grant",
		"certs":    "forbidden: list secrets — certificate expiry is read from the tls.crt of each kubernetes.io/tls Secret; grant list on secrets to score it",
		"webhooks": "forbidden: list validatingwebhookconfigurations.admissionregistration.k8s.io — the webhook audit reads the admission webhook configurations and the Services behind them",
	}
	for cat, reason := range unavailable {
		rec := parseLine(t, got[cat])
		if rec["status"] != "unavailable" || rec["message"] != reason {
			t.Errorf("%s:\n got: %s\nwant: status=unavailable message=%q", cat, got[cat], reason)
		}
	}
	if line := want["services"] + ` unverified="Ingress class references (forbidden: list ingressclasses.networking.k8s.io); Ingress TLS secret references (forbidden: list secrets)"`; got["services"] != line {
		t.Errorf("services line:\n got: %s\nwant: %s", got["services"], line)
	}
	for cat, line := range want {
		if _, ok := unavailable[cat]; ok || cat == "services" {
			continue
		}
		if got[cat] != line {
			t.Errorf("%s changed under view:\n got: %s\nwant: %s", cat, got[cat], line)
		}
	}
	viewLines := strings.Split(view.Stdout, "\n")
	for _, line := range strings.Split(strings.TrimSuffix(full.Stdout, "\n"), "\n") {
		rec := parseLine(t, line)
		if rec["kind"] == "health.category" || strings.HasPrefix(line, "scanned=") {
			continue
		}
		if _, ok := unavailable[rec["category"]]; ok {
			continue
		}
		if !slices.Contains(viewLines, line) {
			t.Errorf("detail lost under view: %s", line)
		}
	}
	// What health still asks for and is refused — pinned, so a new
	// read `view` does not grant is a reviewed decision.
	var names []string
	for _, gr := range refused() {
		names = append(names, gr.String())
	}
	if got, want := strings.Join(names, ","), "nodes,secrets,validatingwebhookconfigurations.admissionregistration.k8s.io,ingressclasses.networking.k8s.io"; got != want {
		t.Errorf("refused reads = %s\nwant %s", got, want)
	}
	cmd, _ = viewCommand(objs...)
	checktest.VerifyContract(t, cmd)
}
