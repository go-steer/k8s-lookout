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

// §13 testing conventions: fake.Clientset fixture clusters, the
// all-healthy cluster proving the scorecard answers explicitly (ten
// category findings, never silence), one broken fixture per
// category, a golden mixed cluster, and the checktest contract
// round-trip. TLS fixtures are generated in-test (self-signed,
// ECDSA); no key material is ever serialized.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/health"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

var fixedNow = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

func testCommand(objs ...runtime.Object) checks.Command {
	cs := fake.NewClientset(objs...)
	return health.New(health.Deps{
		Client:   func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Provider: func(context.Context) (cloud.Provider, error) { return cloud.NoProvider, nil },
		Now:      func() time.Time { return fixedNow },
	})
}

func ago(d time.Duration) metav1.Time { return metav1.Time{Time: fixedNow.Add(-d)} }

func ptr[T any](v T) *T { return &v }

// --- healthy fixtures ------------------------------------------------------

func healthyPod(ns, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, CreationTimestamp: ago(time.Hour)},
		Status: corev1.PodStatus{
			Phase:     corev1.PodRunning,
			StartTime: ptr(ago(time.Hour)),
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "app", Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ago(time.Hour)}},
			}},
		},
	}
}

func deployment(ns, name string, desired, ready int32, labels map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: labels},
		Spec:       appsv1.DeploymentSpec{Replicas: ptr(desired)},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas: ready, UpdatedReplicas: desired, AvailableReplicas: ready,
		},
	}
}

func healthyNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
		}},
	}
}

func boundPVC(ns, name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, CreationTimestamp: ago(2 * time.Hour)},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
}

func quota(ns, name string, used, hard int64) *corev1.ResourceQuota {
	return &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{corev1.ResourcePods: *resource.NewQuantity(hard, resource.DecimalSI)},
			Used: corev1.ResourceList{corev1.ResourcePods: *resource.NewQuantity(used, resource.DecimalSI)},
		},
	}
}

// webhook keeps timeoutSeconds below the delegated check's 10s
// slow-risk threshold so a live backend stays silent; failurePolicy
// is left nil (the v1 default, Fail).
func webhook(name, svcNS, svcName string) *admissionv1.ValidatingWebhookConfiguration {
	return &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Webhooks: []admissionv1.ValidatingWebhook{{
			Name: "validate.example.com",
			ClientConfig: admissionv1.WebhookClientConfig{
				Service: &admissionv1.ServiceReference{Namespace: svcNS, Name: svcName},
			},
			TimeoutSeconds: ptr(int32(5)),
		}},
	}
}

// service exposes 443, the webhook ServiceReference port default.
func service(ns, name string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 443}}},
	}
}

// readySlice backs a service with one ready endpoint so the
// delegated webhook backend check sees it alive.
func readySlice(ns, svcName string) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: svcName + "-abc12",
			Labels: map[string]string{discoveryv1.LabelServiceName: svcName},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{"10.0.0.9"},
			Conditions: discoveryv1.EndpointConditions{Ready: ptr(true)},
		}},
	}
}

// newTestCert returns a PEM self-signed certificate expiring at
// notAfter. SYNTHETIC TEST FIXTURE — private key discarded, never
// serialized.
func newTestCert(t *testing.T, cn string, notAfter time.Time) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    notAfter.Add(-365 * 24 * time.Hour),
		NotAfter:     notAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func tlsSecret(t *testing.T, ns, name, cn string, notAfter time.Time) *corev1.Secret {
	t.Helper()
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       newTestCert(t, cn, notAfter),
			corev1.TLSPrivateKeyKey: []byte("SYNTHETIC-FIXTURE-NOT-A-KEY"),
		},
	}
}

func healthyObjects(t *testing.T) []runtime.Object {
	t.Helper()
	return []runtime.Object{
		healthyPod("prod", "web-0"),
		deployment("prod", "web", 1, 1, nil),
		deployment("kube-system", "coredns", 2, 2, map[string]string{"k8s-app": "kube-dns"}),
		healthyNode("node-1"),
		boundPVC("prod", "data-web-0"),
		quota("prod", "compute", 1, 10),
		tlsSecret(t, "prod", "api-tls", "api.example.com", fixedNow.Add(92*24*time.Hour)),
		webhook("policy", "infra", "policy-webhook"),
		service("infra", "policy-webhook"),
		readySlice("infra", "policy-webhook"),
	}
}

// --- broken fixtures, one per category --------------------------------------

func crashloopPod(ns, name string) *corev1.Pod {
	p := healthyPod(ns, name)
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "app", Ready: false, RestartCount: 12,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
			Reason: "CrashLoopBackOff", Message: "back-off 5m0s restarting failed container",
		}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason: "Error", ExitCode: 1,
		}},
	}}
	return p
}

func pendingPod(ns, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, CreationTimestamp: ago(30 * time.Minute)},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionFalse,
				Reason: corev1.PodReasonUnschedulable, Message: "0/3 nodes are available: 3 Insufficient cpu.",
			}},
		},
	}
}

func brokenNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionFalse,
			Reason: "KubeletNotReady", LastTransitionTime: ago(10 * time.Minute),
		}}},
	}
}

func pendingPVC(ns, name string) *corev1.PersistentVolumeClaim {
	pvc := boundPVC(ns, name)
	pvc.Status.Phase = corev1.ClaimPending
	return pvc
}

func lostPVC(ns, name string) *corev1.PersistentVolumeClaim {
	pvc := boundPVC(ns, name)
	pvc.Status.Phase = corev1.ClaimLost
	return pvc
}

// brokenObjects breaks every live category at once: crashloop +
// aged-Pending pods, a dead rollout, a NotReady node, a degraded
// system add-on, an exhausted quota, Pending and Lost PVCs, an
// expired certificate, and a webhook pointing at a ghost service.
func brokenObjects(t *testing.T) []runtime.Object {
	t.Helper()
	return []runtime.Object{
		crashloopPod("prod", "api-0"),
		pendingPod("prod", "batch-7"),
		deployment("prod", "web", 2, 0, nil),
		deployment("kube-system", "coredns", 2, 1, map[string]string{"k8s-app": "kube-dns"}),
		brokenNode("node-1"),
		pendingPVC("prod", "data-new"),
		lostPVC("prod", "data-old"),
		quota("prod", "compute", 10, 10),
		tlsSecret(t, "prod", "old-tls", "old.example.com", fixedNow.Add(-16*24*time.Hour)),
		webhook("policy", "infra", "ghost"),
	}
}

// --- tests -------------------------------------------------------------------

func TestRegisteredInDefaultRegistry(t *testing.T) {
	c, ok := checks.Lookup("health")
	if !ok {
		t.Fatal("health is not registered in the default registry")
	}
	if c.MCPName != "k8s_cluster_health" {
		t.Errorf("MCP name = %q, want k8s_cluster_health", c.MCPName)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("registered command invalid: %v", err)
	}
}

// TestDeltaGlossaryReachedHealth asserts the delegated delta fields
// made it into health's declaration — Verify below depends on it.
func TestDeltaGlossaryReachedHealth(t *testing.T) {
	h, ok := checks.Lookup("health")
	if !ok {
		t.Fatal("health not registered")
	}
	declared := map[string]bool{}
	for _, f := range h.Output {
		declared[f.Name] = true
	}
	d, ok := checks.Lookup("triage delta")
	if !ok {
		t.Fatal("triage delta not registered")
	}
	for _, f := range d.Output {
		if !declared[f.Name] {
			t.Errorf("delta field %q missing from health's glossary", f.Name)
		}
	}
}

// parseLine understands exactly what emit's logfmt encoder produces.
func parseLine(t *testing.T, line string) map[string]string {
	t.Helper()
	out := map[string]string{}
	rest := line
	for rest != "" {
		eq := strings.IndexByte(rest, '=')
		if eq <= 0 {
			t.Fatalf("bad logfmt line %q", line)
		}
		key := rest[:eq]
		rest = rest[eq+1:]
		var val string
		if strings.HasPrefix(rest, `"`) {
			q, err := strconv.QuotedPrefix(rest)
			if err != nil {
				t.Fatalf("bad quoted value in %q: %v", line, err)
			}
			val, _ = strconv.Unquote(q)
			rest = rest[len(q):]
		} else if sp := strings.IndexByte(rest, ' '); sp >= 0 {
			val, rest = rest[:sp], rest[sp:]
		} else {
			val, rest = rest, ""
		}
		out[key] = val
		rest = strings.TrimPrefix(rest, " ")
	}
	return out
}

// TestAllHealthyClusterAnswersExplicitly: ten category findings,
// nine healthy plus the M1 control-plane unavailable marker — never
// silence.
func TestAllHealthyClusterAnswersExplicitly(t *testing.T) {
	res := checktest.Run(t, testCommand(healthyObjects(t)...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	findings := lines[:len(lines)-1]
	if len(findings) != 10 {
		t.Fatalf("want exactly 10 category findings, got %d:\n%s", len(findings), res.Stdout)
	}
	status := map[string]string{}
	for _, line := range findings {
		rec := parseLine(t, line)
		if rec["kind"] != "health.category" {
			t.Errorf("unexpected non-category finding on a healthy cluster: %s", line)
		}
		status[rec["category"]] = rec["status"]
	}
	for _, cat := range []string{"nodes", "crashloops", "pending", "rollouts", "storage", "addons", "quota", "certs", "webhooks"} {
		if status[cat] != "healthy" {
			t.Errorf("category %s = %q, want healthy", cat, status[cat])
		}
	}
	if status["control-plane"] != "unavailable" {
		t.Errorf("control-plane = %q, want unavailable (metrics land M4)", status["control-plane"])
	}
}

// TestBrokenClusterPerCategory: every live category degrades, each
// carrying its diagnosis in the details block.
func TestBrokenClusterPerCategory(t *testing.T) {
	res := checktest.Run(t, testCommand(brokenObjects(t)...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	status := map[string]string{}
	details := map[string][]string{} // category → finding kinds
	for _, line := range lines[:len(lines)-1] {
		rec := parseLine(t, line)
		if rec["kind"] == "health.category" {
			status[rec["category"]] = rec["status"]
			continue
		}
		details[rec["category"]] = append(details[rec["category"]], rec["kind"])
	}
	wantKinds := map[string][]string{
		"nodes":      {"node.notready"},
		"crashloops": {"pod.crashloop"},
		"pending":    {"pod.pending"},
		"rollouts":   {"workload.rollout"},
		"storage":    {"pvc.pending", "pvc.lost"},
		"addons":     {"addon.degraded"},
		"quota":      {"quota.exhausted"},
		"certs":      {"cert.expired"},
		"webhooks":   {"webhook.failing_closed"},
	}
	for cat, kinds := range wantKinds {
		if status[cat] != "degraded" {
			t.Errorf("category %s = %q, want degraded", cat, status[cat])
		}
		got := strings.Join(details[cat], ",")
		for _, k := range kinds {
			if !strings.Contains(got, k) {
				t.Errorf("category %s details = %q, want to contain %s", cat, got, k)
			}
		}
	}
	if status["control-plane"] != "unavailable" {
		t.Errorf("control-plane = %q, want unavailable", status["control-plane"])
	}
}

func TestBrokenClusterGolden(t *testing.T) {
	res := checktest.Run(t, testCommand(brokenObjects(t)...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	// The certificate is generated per run: normalize nothing — the
	// finding carries only subject/notAfter/days, all pinned by the
	// fixture parameters.
	golden := filepath.Join("testdata", "health-broken.golden")
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(golden, []byte(res.Stdout), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("%v (run with UPDATE_GOLDEN=1 to create)", err)
	}
	if res.Stdout != string(want) {
		t.Errorf("golden mismatch:\ngot:\n%s\nwant:\n%s", res.Stdout, want)
	}
}

// TestWebhooksCategoryLineStable pins the scorecard shape across the
// delegation to state.CheckWebhooks: one ghost-service webhook must
// yield exactly the same category line format as before —
// status/total/top rendering untouched, only the finding kind
// changed (webhook.backend_missing → webhook.failing_closed).
func TestWebhooksCategoryLineStable(t *testing.T) {
	res := checktest.Run(t, testCommand(webhook("policy", "infra", "ghost")))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	want := `kind=health.category severity=critical category=webhooks status=degraded total=1 top="webhook.failing_closed policy"`
	for _, line := range strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n") {
		rec := parseLine(t, line)
		if rec["kind"] == "health.category" && rec["category"] == "webhooks" {
			if line != want {
				t.Errorf("webhooks category line changed shape:\n got: %s\nwant: %s", line, want)
			}
			return
		}
	}
	t.Fatal("webhooks category line not found")
}

// TestNamespaceScopedMarksClusterCategoriesUnavailable: a namespaced
// scan cannot see nodes, kube-system add-ons, or cluster-scoped
// webhook configurations — those categories say so instead of lying
// with "healthy".
func TestNamespaceScopedMarksClusterCategoriesUnavailable(t *testing.T) {
	res := checktest.Run(t, testCommand(healthyObjects(t)...), "--namespace=prod")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	status := map[string]string{}
	for _, line := range lines[:len(lines)-1] {
		rec := parseLine(t, line)
		if rec["kind"] == "health.category" {
			status[rec["category"]] = rec["status"]
		}
	}
	for _, cat := range []string{"nodes", "addons", "webhooks", "control-plane"} {
		if status[cat] != "unavailable" {
			t.Errorf("category %s = %q under --namespace, want unavailable", cat, status[cat])
		}
	}
	for _, cat := range []string{"crashloops", "pending", "rollouts", "storage", "quota", "certs"} {
		if status[cat] != "healthy" {
			t.Errorf("category %s = %q under --namespace, want healthy", cat, status[cat])
		}
	}
}

func TestTopCapsInlineFindings(t *testing.T) {
	objs := []runtime.Object{
		crashloopPod("prod", "api-0"),
		crashloopPod("prod", "api-1"),
		crashloopPod("prod", "api-2"),
	}
	res := checktest.Run(t, testCommand(objs...), "--top=2")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	for _, line := range strings.Split(res.Stdout, "\n") {
		rec := parseLine(t, strings.TrimSpace(line))
		if rec["category"] == "crashloops" && rec["kind"] == "health.category" {
			if rec["total"] != "3" {
				t.Errorf("total = %q, want 3", rec["total"])
			}
			if n := strings.Count(rec["top"], ";") + 1; n != 2 {
				t.Errorf("top names %d findings, want 2 (%q)", n, rec["top"])
			}
			return
		}
	}
	t.Fatal("crashloops category finding not found")
}

func TestUsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"workload rejected", []string{"--workload=Deployment/prod/api"}, "bundle"},
		{"bad top", []string{"--top=0"}, "--top"},
		{"bad cert-warn", []string{"--cert-warn=0s"}, "--cert-warn"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, testCommand(healthyObjects(t)...), tc.args...)
			if res.Code != emit.ExitUsage {
				t.Fatalf("exit = %d, want %d (stderr %q)", res.Code, emit.ExitUsage, res.Stderr)
			}
			if !strings.Contains(res.Stderr, tc.want) {
				t.Errorf("stderr %q should contain %q", res.Stderr, tc.want)
			}
		})
	}
}

func TestVerifyContract(t *testing.T) {
	checktest.VerifyContract(t, testCommand(healthyObjects(t)...))
	checktest.VerifyContract(t, testCommand(brokenObjects(t)...))
	checktest.VerifyContract(t, testCommand(brokenObjects(t)...), "--namespace=prod")
}
