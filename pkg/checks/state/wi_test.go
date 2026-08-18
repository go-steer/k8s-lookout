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

// §13 conventions for `state wi`: fake.Clientset fixture clusters on
// the k8s side, a capability-backed fake provider (cloud.NoProvider
// plus a fixture-driven WorkloadIdentityAPI — the cloudcheck
// embedding trick) on the cloud side, exact findings per broken
// link, a golden mixed cluster, the §2 unavailable path, and the
// checktest contract round-trip.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// wiFakeProvider is cloud.NoProvider plus exactly the
// workload-identity capability.
type wiFakeProvider struct {
	cloud.Provider
	api cloud.WorkloadIdentityAPI
}

func (p wiFakeProvider) WorkloadIdentity() (cloud.WorkloadIdentityAPI, bool) { return p.api, true }

// wiFakeAPI serves canned verification results keyed ns/ksa/gsa and
// records the verifications asked for. Unexpected keys error, so a
// scoping bug cannot silently verify out-of-scope identities.
type wiFakeAPI struct {
	bindings map[string]cloud.WIBinding
	calls    []string
}

func (f *wiFakeAPI) VerifyBinding(_ context.Context, ns, ksa, gsa string) (cloud.WIBinding, error) {
	k := ns + "/" + ksa + "/" + gsa
	f.calls = append(f.calls, k)
	b, ok := f.bindings[k]
	if !ok {
		return cloud.WIBinding{}, fmt.Errorf("unexpected VerifyBinding(%s)", k)
	}
	return b, nil
}

const (
	wiGSAAPI   = "api@my-project.iam.gserviceaccount.com"
	wiGSAGhost = "ghost@my-project.iam.gserviceaccount.com"
	wiGSAGood  = "good@my-project.iam.gserviceaccount.com"
)

// wiBindings is the cloud-side fixture matching the mixed cluster:
// api-sa's claim lacks the IAM binding, ghost-sa's GSA is gone,
// bound-sa verifies clean.
func wiBindings() *wiFakeAPI {
	return &wiFakeAPI{bindings: map[string]cloud.WIBinding{
		"prod/api-sa/" + wiGSAAPI: {
			Namespace: "prod", ServiceAccount: "api-sa", CloudIdentity: wiGSAAPI,
			Problems: []string{cloud.WIProblemNoBinding +
				": serviceAccount:my-project.svc.id.goog[prod/api-sa] lacks roles/iam.workloadIdentityUser on " + wiGSAAPI},
		},
		"prod/ghost-sa/" + wiGSAGhost: {
			Namespace: "prod", ServiceAccount: "ghost-sa", CloudIdentity: wiGSAGhost,
			Problems: []string{cloud.WIProblemIdentityMissing + ": service account " + wiGSAGhost + " not found in IAM"},
		},
		"prod/bound-sa/" + wiGSAGood: {
			Namespace: "prod", ServiceAccount: "bound-sa", CloudIdentity: wiGSAGood,
			Bound: true,
		},
	}}
}

func wiSA(ns, name, gsa string) *corev1.ServiceAccount {
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	if gsa != "" {
		sa.Annotations = map[string]string{"iam.gke.io/gcp-service-account": gsa}
	}
	return sa
}

func wiPod(ns, name, sa string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: corev1.PodSpec{
			ServiceAccountName: sa,
			Containers:         []corev1.Container{{Name: "app"}},
		},
	}
}

// wiMixed is the golden fixture: one unbound claim (2 pods), one
// claim on a deleted GSA, one healthy bound claim, and one
// unannotated pod on a credential file in another namespace.
func wiMixed() []runtime.Object {
	keyfilePod := wiPod("legacy", "legacy-keyfile", "default")
	keyfilePod.Spec.Containers[0].Env = []corev1.EnvVar{
		{Name: "GOOGLE_APPLICATION_CREDENTIALS", Value: "/var/secrets/google/key.json"},
	}
	return []runtime.Object{
		wiSA("prod", "api-sa", wiGSAAPI),
		wiSA("prod", "ghost-sa", wiGSAGhost),
		wiSA("prod", "bound-sa", wiGSAGood),
		wiSA("legacy", "default", ""),
		wiPod("prod", "api-1", "api-sa"),
		wiPod("prod", "api-2", "api-sa"),
		wiPod("prod", "ghost-1", "ghost-sa"),
		wiPod("prod", "good-1", "bound-sa"),
		keyfilePod,
	}
}

func wiTestCommand(api cloud.WorkloadIdentityAPI, objs ...runtime.Object) checks.Command {
	cs := fake.NewClientset(objs...)
	return state.WICommand(state.WIDeps{
		Client: func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Provider: func(context.Context) (cloud.Provider, error) {
			return wiFakeProvider{Provider: cloud.NoProvider, api: api}, nil
		},
	})
}

// wiFindings returns the finding lines of a successful run (summary
// stripped), failing the test on non-zero exit.
func wiFindings(t *testing.T, api cloud.WorkloadIdentityAPI, objs []runtime.Object, args ...string) []string {
	t.Helper()
	res := checktest.Run(t, wiTestCommand(api, objs...), args...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	if len(lines) == 0 || !strings.HasPrefix(lines[len(lines)-1], "scanned=") {
		t.Fatalf("no summary line: %q", res.Stdout)
	}
	return lines[:len(lines)-1]
}

const (
	wiUnboundLine = `kind=wi.unbound severity=critical namespace=prod kind_of_object=ServiceAccount name=api-sa reason=BindingMissing message="annotation claims api@my-project.iam.gserviceaccount.com but the workload identity binding is missing: serviceAccount:my-project.svc.id.goog[prod/api-sa] lacks roles/iam.workloadIdentityUser on api@my-project.iam.gserviceaccount.com" gsa=api@my-project.iam.gserviceaccount.com pods=2 problem=no-workload-identity-binding`
	wiGhostLine   = `kind=wi.gsa_missing severity=critical namespace=prod kind_of_object=ServiceAccount name=ghost-sa reason=GSAMissing message="annotated GSA ghost@my-project.iam.gserviceaccount.com does not exist — every GCP call from these pods fails" gsa=ghost@my-project.iam.gserviceaccount.com pods=1`
	wiKeyfileLine = `kind=wi.unannotated_use severity=info namespace=legacy kind_of_object=Pod name=legacy-keyfile reason=CredentialFileWithoutWI message="pod points at a mounted credential file instead of Workload Identity — works, but key rotation/leakage risk" container=app env=GOOGLE_APPLICATION_CREDENTIALS`
)

func TestWIMixedFindings(t *testing.T) {
	api := wiBindings()
	got := wiFindings(t, api, wiMixed())
	// Deterministic order: namespace, then name, then kind.
	wantFindings(t, got, []string{wiKeyfileLine, wiUnboundLine, wiGhostLine})
	// The healthy bound-sa claim was verified — silence means
	// "checked and fine", not "skipped".
	found := false
	for _, c := range api.calls {
		if c == "prod/bound-sa/"+wiGSAGood {
			found = true
		}
	}
	if !found {
		t.Errorf("bound-sa was never verified, calls: %v", api.calls)
	}
}

func TestWIHealthyIsSilent(t *testing.T) {
	objs := []runtime.Object{
		wiSA("prod", "bound-sa", wiGSAGood),
		wiPod("prod", "good-1", "bound-sa"),
	}
	res := checktest.Run(t, wiTestCommand(wiBindings(), objs...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	// scanned = 1 pod + 1 serviceaccount.
	want := "scanned=2 findings=0 elapsed=100ms\n"
	if res.Stdout != want {
		t.Errorf("healthy cluster must emit only the summary:\ngot:  %qwant: %q", res.Stdout, want)
	}
}

func TestWINamespaceScope(t *testing.T) {
	got := wiFindings(t, wiBindings(), wiMixed(), "--namespace=prod")
	wantFindings(t, got, []string{wiUnboundLine, wiGhostLine})

	got = wiFindings(t, wiBindings(), wiMixed(), "--namespace=legacy")
	wantFindings(t, got, []string{wiKeyfileLine})
}

func TestWIWorkloadScope(t *testing.T) {
	tmpl := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}},
		Spec:       corev1.PodSpec{ServiceAccountName: "api-sa"},
	}
	pod := func(name string) *corev1.Pod {
		p := wiPod("prod", name, "api-sa")
		p.OwnerReferences = []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "api-99xyz"}}
		return p
	}
	objs := []runtime.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api"},
			Spec:       appsv1.DeploymentSpec{Template: tmpl},
		},
		&appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod", Name: "api-99xyz",
				OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "api"}},
			},
			Spec: appsv1.ReplicaSetSpec{Template: tmpl},
		},
		pod("api-99xyz-aaaaa"),
		pod("api-99xyz-bbbbb"),
		wiPod("prod", "ghost-1", "ghost-sa"), // outside the workload
		wiSA("prod", "api-sa", wiGSAAPI),
		wiSA("prod", "ghost-sa", wiGSAGhost),
	}
	api := wiBindings()
	got := wiFindings(t, api, objs, "--workload=Deployment/prod/api")
	wantFindings(t, got, []string{wiUnboundLine})
	// ghost-sa's pod is outside the workload: its claim must never
	// have been verified.
	for _, c := range api.calls {
		if strings.HasPrefix(c, "prod/ghost-sa/") {
			t.Errorf("out-of-workload identity verified: %v", api.calls)
		}
	}
}

func TestWIUnavailable(t *testing.T) {
	cmd := state.WICommand(state.WIDeps{
		// The client must never be dialed on the unavailable path:
		// the capability check comes before any List.
		Client: func(context.Context) (kubernetes.Interface, error) {
			return nil, fmt.Errorf("client must not be built when the capability is unavailable")
		},
		Provider: func(context.Context) (cloud.Provider, error) { return cloud.NoProvider, nil },
	})
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("unavailable path must exit 0 (explicit, not broken), got %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want exactly the cloud.unavailable finding + summary, got: %q", res.Stdout)
	}
	finding := lines[0]
	for _, want := range []string{
		"kind=cloud.unavailable",
		"severity=info",
		"reason=CapabilityUnavailable",
		"capability=" + string(cloud.CapabilityWorkloadIdentity),
		"provider=" + cloud.NoProviderName,
		cloud.NoProviderReason,
	} {
		if !strings.Contains(finding, want) {
			t.Errorf("unavailable finding %q missing %q", finding, want)
		}
	}
	summary := lines[1]
	if !strings.HasPrefix(summary, "scanned=0 findings=1") ||
		!strings.Contains(summary, `unavailable="`+cloud.NoProviderReason+`"`) {
		t.Errorf("summary = %q, want scanned=0 with the §2 unavailable marker", summary)
	}
}

func TestWIVerifyErrorFailsLoudly(t *testing.T) {
	// An SA claiming an identity the fake has no fixture for: the
	// provider errors, and the command must exit 1 — never a partial
	// verdict.
	objs := []runtime.Object{
		wiSA("prod", "api-sa", "unknown@my-project.iam.gserviceaccount.com"),
		wiPod("prod", "api-1", "api-sa"),
	}
	res := checktest.Run(t, wiTestCommand(wiBindings(), objs...))
	if res.Code != emit.ExitRuntime {
		t.Fatalf("exit %d, want %d (stderr: %s)", res.Code, emit.ExitRuntime, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "unexpected VerifyBinding") {
		t.Errorf("stderr %q does not surface the provider error", res.Stderr)
	}
	if strings.Contains(res.Stdout, "scanned=") {
		t.Errorf("failed run must not emit a summary, stdout: %q", res.Stdout)
	}
}

func TestWIUsageErrors(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		stderr string
	}{
		{"unsupported kind", []string{"--workload=Service/prod/api"}, "unsupported workload kind"},
		{"namespace contradiction", []string{"--workload=Deployment/prod/api", "--namespace=other"}, "contradicts"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := checktest.Run(t, wiTestCommand(wiBindings(), wiMixed()...), tt.args...)
			// Both cases are malformed invocations: §4.2 exit 2.
			if res.Code != emit.ExitUsage {
				t.Fatalf("exit %d, want %d (stderr: %s)", res.Code, emit.ExitUsage, res.Stderr)
			}
			if !strings.Contains(res.Stderr, tt.stderr) {
				t.Errorf("stderr %q does not mention %q", res.Stderr, tt.stderr)
			}
		})
	}
}

func TestWIMixedGolden(t *testing.T) {
	res := checktest.Run(t, wiTestCommand(wiBindings(), wiMixed()...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/wi-mixed.golden", res.Stdout)
}

func TestWIContract(t *testing.T) {
	checktest.VerifyContract(t, wiTestCommand(wiBindings(), wiMixed()...))
}

func TestWIRegisteredInDefaultRegistry(t *testing.T) {
	c, ok := checks.Lookup("state wi")
	if !ok {
		t.Fatal("state wi is not registered in the default registry")
	}
	if c.MCPName != "k8s_workload_identity" {
		t.Errorf("MCP tool name = %q, want k8s_workload_identity", c.MCPName)
	}
	if !strings.Contains(c.Help(), "Workload Identity") {
		t.Error("generated help does not mention Workload Identity")
	}
}
