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

// §13 testing conventions for `state webhooks`: fake.Clientset
// fixture clusters, exact findings per broken class (the failure
// policy × backend matrix, scope analysis, timeout risk, CA-bundle
// expiry), a golden mixed cluster, and the checktest contract
// round-trip. The healthy fixture proves zero nominal state. CA
// bundles reuse newTestCert (edges_test.go): generated per run,
// never committed. All helpers here are wh-prefixed; fixedNow,
// newTestCert, and wantFindings are shared with edges_test.go.

import (
	"context"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func whPtr[T any](v T) *T { return &v }

func whCommand(objs ...runtime.Object) checks.Command {
	cs := fake.NewClientset(objs...)
	return state.WebhooksCommand(state.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Now:    func() time.Time { return fixedNow },
	})
}

// whWebhook is the well-formed webhook entry: service-backed
// (infra/policy-webhook, default port 443), default failurePolicy
// (Fail), timeoutSeconds 5 (below the slow_risk threshold), one
// CREATE/UPDATE pods rule. Tests break exactly one property each.
func whWebhook(mutate func(*admissionv1.ValidatingWebhook)) admissionv1.ValidatingWebhook {
	w := admissionv1.ValidatingWebhook{
		Name: "validate.example.com",
		ClientConfig: admissionv1.WebhookClientConfig{
			Service: &admissionv1.ServiceReference{Namespace: "infra", Name: "policy-webhook"},
		},
		TimeoutSeconds: whPtr(int32(5)),
		Rules: []admissionv1.RuleWithOperations{{
			Operations: []admissionv1.OperationType{admissionv1.Create, admissionv1.Update},
			Rule:       admissionv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods"}},
		}},
	}
	if mutate != nil {
		mutate(&w)
	}
	return w
}

func whConfig(mutate func(*admissionv1.ValidatingWebhook)) *admissionv1.ValidatingWebhookConfiguration {
	return &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "policy"},
		Webhooks:   []admissionv1.ValidatingWebhook{whWebhook(mutate)},
	}
}

func whService(ports ...int32) *corev1.Service {
	if len(ports) == 0 {
		ports = []int32{443}
	}
	svcPorts := make([]corev1.ServicePort, 0, len(ports))
	for _, p := range ports {
		svcPorts = append(svcPorts, corev1.ServicePort{Port: p})
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "policy-webhook"},
		Spec:       corev1.ServiceSpec{Ports: svcPorts},
	}
}

func whEndpointSlice(ready bool) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "infra", Name: "policy-webhook-abc12",
			Labels: map[string]string{discoveryv1.LabelServiceName: "policy-webhook"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{"10.0.0.9"},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
		}},
	}
}

func whNamespace(name string, labels map[string]string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

// whHealthy is the fully wired fixture: webhook + backing service +
// one ready endpoint.
func whHealthy(mutate func(*admissionv1.ValidatingWebhook)) []runtime.Object {
	return []runtime.Object{whConfig(mutate), whService(), whEndpointSlice(true)}
}

// whFindings returns the finding lines of a successful run (summary
// stripped), failing the test on non-zero exit.
func whFindings(t *testing.T, objs []runtime.Object, args ...string) []string {
	t.Helper()
	res := checktest.Run(t, whCommand(objs...), args...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	if len(lines) == 0 || !strings.HasPrefix(lines[len(lines)-1], "scanned=") {
		t.Fatalf("no summary line: %q", res.Stdout)
	}
	return lines[:len(lines)-1]
}

func TestWebhooksHealthyIsSilent(t *testing.T) {
	res := checktest.Run(t, whCommand(whHealthy(nil)...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	want := "scanned=1 findings=0 elapsed=100ms\n"
	if res.Stdout != want {
		t.Errorf("healthy webhook must emit only the summary:\ngot:  %qwant: %q", res.Stdout, want)
	}
}

// TestWebhooksFailurePolicyMatrix crosses backend death (missing
// service, no ready endpoints, port mismatch) with the effective
// failure policy: Fail (including the nil default) fails closed at
// critical, Ignore means the policy is silently not enforced.
func TestWebhooksFailurePolicyMatrix(t *testing.T) {
	const scope = ` webhook=policy/validate.example.com service=infra/policy-webhook`
	tests := []struct {
		name string
		objs []runtime.Object
		want []string
	}{
		{
			name: "Fail + missing service",
			objs: []runtime.Object{whConfig(nil)},
			want: []string{
				`kind=webhook.failing_closed severity=critical kind_of_object=ValidatingWebhookConfiguration name=policy reason=FailingClosed message="backend service infra/policy-webhook not found — matching admissions are REJECTED (failurePolicy Fail)"` + scope + ` backend="service missing" gates="all namespaces" rules="CREATE,UPDATE pods"`,
			},
		},
		{
			name: "Fail + no ready endpoints",
			objs: []runtime.Object{whConfig(nil), whService(), whEndpointSlice(false)},
			want: []string{
				`kind=webhook.failing_closed severity=critical kind_of_object=ValidatingWebhookConfiguration name=policy reason=FailingClosed message="backend service infra/policy-webhook has no ready endpoints — matching admissions are REJECTED (failurePolicy Fail)"` + scope + ` backend="no ready endpoints" gates="all namespaces" rules="CREATE,UPDATE pods"`,
			},
		},
		{
			name: "Fail + port mismatch",
			objs: []runtime.Object{whConfig(nil), whService(8443), whEndpointSlice(true)},
			want: []string{
				`kind=webhook.failing_closed severity=critical kind_of_object=ValidatingWebhookConfiguration name=policy reason=FailingClosed message="backend service infra/policy-webhook does not expose port 443 — matching admissions are REJECTED (failurePolicy Fail)"` + scope + ` backend="port 443 not on service" gates="all namespaces" rules="CREATE,UPDATE pods"`,
			},
		},
		{
			name: "Ignore + dead backend",
			objs: []runtime.Object{whConfig(func(w *admissionv1.ValidatingWebhook) {
				w.FailurePolicy = whPtr(admissionv1.Ignore)
			})},
			want: []string{
				`kind=webhook.dead_backend severity=warning kind_of_object=ValidatingWebhookConfiguration name=policy reason=DeadBackend message="backend service infra/policy-webhook not found — webhook silently passes everything; the policy it enforces is NOT running (failurePolicy Ignore)"` + scope + ` backend="service missing" gates="all namespaces" rules="CREATE,UPDATE pods"`,
			},
		},
		{
			name: "Fail + alive backend",
			objs: whHealthy(nil),
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantFindings(t, whFindings(t, tt.objs), tt.want)
		})
	}
}

// TestWebhooksScopeAnalysis: the gates detail evaluates
// namespaceSelector against the listed namespaces, and
// object_selector appears only when one is set.
func TestWebhooksScopeAnalysis(t *testing.T) {
	namespaces := []runtime.Object{
		whNamespace("team-a", map[string]string{"tier": "web"}),
		whNamespace("team-b", map[string]string{"tier": "web"}),
		whNamespace("ops", nil),
		whNamespace("kube-system", nil),
	}

	t.Run("selector matches 2 of 4", func(t *testing.T) {
		cfg := whConfig(func(w *admissionv1.ValidatingWebhook) {
			w.NamespaceSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "web"}}
			w.ObjectSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "payments"}}
		})
		got := whFindings(t, append([]runtime.Object{cfg}, namespaces...))
		wantFindings(t, got, []string{
			`kind=webhook.failing_closed severity=critical kind_of_object=ValidatingWebhookConfiguration name=policy reason=FailingClosed message="backend service infra/policy-webhook not found — matching admissions are REJECTED (failurePolicy Fail)" webhook=policy/validate.example.com service=infra/policy-webhook backend="service missing" gates="2/4 namespaces: team-a, team-b" rules="CREATE,UPDATE pods" object_selector="app=payments"`,
		})
	})

	t.Run("nil selector gates all namespaces", func(t *testing.T) {
		got := whFindings(t, append([]runtime.Object{whConfig(nil)}, namespaces...))
		wantFindings(t, got, []string{
			`kind=webhook.failing_closed severity=critical kind_of_object=ValidatingWebhookConfiguration name=policy reason=FailingClosed message="backend service infra/policy-webhook not found — matching admissions are REJECTED (failurePolicy Fail)" webhook=policy/validate.example.com service=infra/policy-webhook backend="service missing" gates="all namespaces" rules="CREATE,UPDATE pods"`,
		})
	})
}

// TestWebhooksSlowRisk: a long timeout on a failing-closed webhook
// with a LIVE backend is a stall risk, not a failure — INFO only,
// and never doubled with failing_closed on a dead backend.
func TestWebhooksSlowRisk(t *testing.T) {
	got := whFindings(t, whHealthy(func(w *admissionv1.ValidatingWebhook) {
		w.TimeoutSeconds = whPtr(int32(30))
	}))
	wantFindings(t, got, []string{
		`kind=webhook.slow_risk severity=info kind_of_object=ValidatingWebhookConfiguration name=policy reason=SlowWebhookRisk message="timeoutSeconds 30 with failurePolicy Fail: a slow or hung backend stalls every matching admission for up to 30s before rejecting it" webhook=policy/validate.example.com service=infra/policy-webhook timeout=30s`,
	})
}

func TestWebhooksCABundle(t *testing.T) {
	t.Run("expired", func(t *testing.T) {
		got := whFindings(t, whHealthy(func(w *admissionv1.ValidatingWebhook) {
			w.ClientConfig.CABundle = newTestCert(t, "policy-ca", fixedNow.Add(-16*24*time.Hour))
		}))
		wantFindings(t, got, []string{
			`kind=webhook.ca_expired severity=critical kind_of_object=ValidatingWebhookConfiguration name=policy reason=CABundleExpired message="CA bundle expired 16d ago — TLS to the backend fails; matching admissions are REJECTED (failurePolicy Fail)" webhook=policy/validate.example.com service=infra/policy-webhook subject=policy-ca not_after=2026-06-15T00:00:00Z days_left=-16`,
		})
	})
	t.Run("expiring within cert-warn", func(t *testing.T) {
		got := whFindings(t, whHealthy(func(w *admissionv1.ValidatingWebhook) {
			w.ClientConfig.CABundle = newTestCert(t, "policy-ca", fixedNow.Add(15*24*time.Hour))
		}))
		wantFindings(t, got, []string{
			`kind=webhook.ca_expiring severity=warning kind_of_object=ValidatingWebhookConfiguration name=policy reason=CABundleExpiringSoon message="CA bundle expires in 15d" webhook=policy/validate.example.com service=infra/policy-webhook subject=policy-ca not_after=2026-07-16T00:00:00Z days_left=15`,
		})
	})
	t.Run("tighter cert-warn window is silent", func(t *testing.T) {
		got := whFindings(t, whHealthy(func(w *admissionv1.ValidatingWebhook) {
			w.ClientConfig.CABundle = newTestCert(t, "policy-ca", fixedNow.Add(15*24*time.Hour))
		}), "--cert-warn=240h")
		wantFindings(t, got, nil)
	})
	t.Run("unparseable bundle is skipped", func(t *testing.T) {
		// Injected CAs (cert-manager ca-injector, service-ca) make an
		// empty or odd bundle common — not provably broken.
		got := whFindings(t, whHealthy(func(w *admissionv1.ValidatingWebhook) {
			w.ClientConfig.CABundle = []byte("not a certificate bundle")
		}))
		wantFindings(t, got, nil)
	})
}

// TestWebhooksURLBackedSkipsBackendChecks: an external endpoint
// cannot be verified from a List pass — no dead-backend findings —
// but the spec-only checks (timeout risk) still apply.
func TestWebhooksURLBackedSkipsBackendChecks(t *testing.T) {
	url := "https://hooks.example.com/validate"
	t.Run("short timeout is silent", func(t *testing.T) {
		cfg := whConfig(func(w *admissionv1.ValidatingWebhook) {
			w.ClientConfig = admissionv1.WebhookClientConfig{URL: &url}
		})
		got := whFindings(t, []runtime.Object{cfg})
		wantFindings(t, got, nil)
	})
	t.Run("default timeout still carries slow risk", func(t *testing.T) {
		cfg := whConfig(func(w *admissionv1.ValidatingWebhook) {
			w.ClientConfig = admissionv1.WebhookClientConfig{URL: &url}
			w.TimeoutSeconds = nil // API default: 10s
		})
		got := whFindings(t, []runtime.Object{cfg})
		wantFindings(t, got, []string{
			`kind=webhook.slow_risk severity=info kind_of_object=ValidatingWebhookConfiguration name=policy reason=SlowWebhookRisk message="timeoutSeconds 10 with failurePolicy Fail: a slow or hung backend stalls every matching admission for up to 10s before rejecting it" webhook=policy/validate.example.com timeout=10s`,
		})
	})
}

func TestWebhooksUsageErrors(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		stderr string
	}{
		{"namespace rejected", []string{"--namespace=prod"}, "cluster-scoped"},
		{"all-namespaces rejected", []string{"-A"}, "cluster-scoped"},
		{"workload rejected", []string{"--workload=Deployment/prod/api"}, "--workload does not apply"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := checktest.Run(t, whCommand(whHealthy(nil)...), tt.args...)
			if res.Code != emit.ExitUsage {
				t.Fatalf("exit %d, want %d (stderr: %s)", res.Code, emit.ExitUsage, res.Stderr)
			}
			if !strings.Contains(res.Stderr, tt.stderr) {
				t.Errorf("stderr %q does not mention %q", res.Stderr, tt.stderr)
			}
			if strings.Contains(res.Stdout, "scanned=") {
				t.Errorf("failed run must not emit a summary, stdout: %q", res.Stdout)
			}
		})
	}
}

// whMixed breaks nearly every class at once across both
// configuration kinds — the golden pins ordering and formatting.
func whMixed(t *testing.T) []runtime.Object {
	t.Helper()
	// "audit": Ignore + missing service.
	audit := &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "audit"},
		Webhooks: []admissionv1.ValidatingWebhook{whWebhook(func(w *admissionv1.ValidatingWebhook) {
			w.Name = "audit.example.com"
			w.ClientConfig.Service.Name = "ghost"
			w.FailurePolicy = whPtr(admissionv1.Ignore)
		})},
	}
	// "gatekeeper": Fail + missing service, namespace-scoped, plus an
	// expired CA bundle.
	gatekeeper := &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "gatekeeper"},
		Webhooks: []admissionv1.ValidatingWebhook{whWebhook(func(w *admissionv1.ValidatingWebhook) {
			w.Name = "deny.example.com"
			w.ClientConfig.Service.Name = "gone"
			w.ClientConfig.CABundle = newTestCert(t, "gatekeeper-ca", fixedNow.Add(-16*24*time.Hour))
			w.NamespaceSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "web"}}
			w.Rules = []admissionv1.RuleWithOperations{{
				Operations: []admissionv1.OperationType{admissionv1.Create, admissionv1.Update},
				Rule:       admissionv1.Rule{APIGroups: []string{"", "apps"}, APIVersions: []string{"v1"}, Resources: []string{"pods", "deployments"}},
			}}
		})},
	}
	// "sidecar-injector" (mutating): live backend but a long timeout,
	// and an expiring CA bundle.
	v := whWebhook(func(w *admissionv1.ValidatingWebhook) {
		w.Name = "inject.example.com"
		w.TimeoutSeconds = whPtr(int32(30))
		w.ClientConfig.CABundle = newTestCert(t, "injector-ca", fixedNow.Add(15*24*time.Hour))
	})
	injector := &admissionv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "sidecar-injector"},
		Webhooks: []admissionv1.MutatingWebhook{{
			Name:              v.Name,
			ClientConfig:      v.ClientConfig,
			FailurePolicy:     v.FailurePolicy,
			NamespaceSelector: v.NamespaceSelector,
			ObjectSelector:    v.ObjectSelector,
			Rules:             v.Rules,
			TimeoutSeconds:    v.TimeoutSeconds,
		}},
	}
	// "policy": fully healthy — must stay silent.
	return []runtime.Object{
		audit, gatekeeper, injector, whConfig(nil),
		whService(), whEndpointSlice(true),
		whNamespace("team-a", map[string]string{"tier": "web"}),
		whNamespace("team-b", map[string]string{"tier": "web"}),
		whNamespace("ops", nil),
		whNamespace("kube-system", nil),
	}
}

func TestWebhooksMixedGolden(t *testing.T) {
	res := checktest.Run(t, whCommand(whMixed(t)...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/webhooks-mixed.golden", res.Stdout)
}

func TestWebhooksContract(t *testing.T) {
	checktest.VerifyContract(t, whCommand(whMixed(t)...))
}

func TestWebhooksRegisteredInDefaultRegistry(t *testing.T) {
	c, ok := checks.Lookup("state webhooks")
	if !ok {
		t.Fatal("state webhooks is not registered in the default registry")
	}
	if c.MCPName != "k8s_admission_webhooks" {
		t.Errorf("MCP tool name = %q, want k8s_admission_webhooks", c.MCPName)
	}
	if !strings.Contains(c.Help(), "--cert-warn") {
		t.Error("generated help does not document --cert-warn")
	}
}
