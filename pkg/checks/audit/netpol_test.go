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

package audit_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// --- fixture builders ---

// netpol builds a NetworkPolicy with an explicit policyTypes, the shape
// the API server stores. Pass a nil selector for the empty one that
// selects every pod in the namespace — the default-deny idiom.
func netpol(ns, name string, sel *metav1.LabelSelector, types ...networkingv1.PolicyType) *networkingv1.NetworkPolicy {
	if sel == nil {
		sel = &metav1.LabelSelector{}
	}
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: *sel,
			PolicyTypes: types,
		},
	}
}

// undefaulted drops policyTypes, the state a policy is in before the
// API server defaults it — and the state a manifest read straight off
// disk is always in.
func undefaulted(p *networkingv1.NetworkPolicy) *networkingv1.NetworkPolicy {
	p.Spec.PolicyTypes = nil
	return p
}

// withEgressRule adds an egress rule, which is what makes an
// undefaulted policy cover egress as well as ingress.
func withEgressRule(p *networkingv1.NetworkPolicy) *networkingv1.NetworkPolicy {
	p.Spec.Egress = append(p.Spec.Egress, networkingv1.NetworkPolicyEgressRule{})
	return p
}

func onHostNetwork(t corev1.PodTemplateSpec) corev1.PodTemplateSpec {
	t.Spec.HostNetwork = true
	return t
}

// netpolCluster is the shared fixture: one namespace per coverage shape
// the command has to tell apart.
//
//	prod       default-deny ingress over everything, egress only for checkout
//	  Deployment/checkout   covered both ways
//	  Deployment/payments   covered inbound, unselected outbound — one finding
//	  DaemonSet/cni         hostNetwork, outside NetworkPolicy entirely
//	platform   no policies at all — one namespace finding per direction
//	  Deployment/mesh
//	  CronJob/nightly
//	  DaemonSet/logging     hostNetwork, so the namespace claims count 2, not 3
//	legacy     one ingress policy whose selector matches nothing
//	  Deployment/vendor
//	edge       nothing but a hostNetwork DaemonSet — no claim is possible
//	  DaemonSet/edge-cni
func netpolCluster() []runtime.Object {
	return []runtime.Object{
		nsObj("prod", nil),
		nsObj("platform", nil),
		nsObj("legacy", nil),
		nsObj("edge", nil),

		netpol("prod", "default-deny-ingress", nil, networkingv1.PolicyTypeIngress),
		netpol("prod", "checkout-egress", matching(map[string]string{"app": "checkout"}), networkingv1.PolicyTypeEgress),
		netpol("legacy", "ghost", matching(map[string]string{"app": "ghost"}), networkingv1.PolicyTypeIngress),

		deploy("prod", "checkout", 3, app("checkout")),
		deploy("prod", "payments", 2, app("payments")),
		daemon("prod", "cni", onHostNetwork(app("cni"))),

		deploy("platform", "mesh", 2, app("mesh")),
		cronJob("platform", "nightly", app("nightly")),
		daemon("platform", "logging", onHostNetwork(app("logging"))),

		deploy("legacy", "vendor", 1, app("vendor")),

		daemon("edge", "edge-cni", onHostNetwork(app("edge-cni"))),
	}
}

func TestNetpolContract(t *testing.T) {
	checktest.VerifyContract(t, audit.NetpolCommand(testDeps(netpolCluster()...)), "-A")
}

func TestNetpolGolden(t *testing.T) {
	res := checktest.Run(t, audit.NetpolCommand(testDeps(netpolCluster()...)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	path := filepath.Join("testdata", "netpol.golden")
	if *update {
		if err := os.WriteFile(path, []byte(res.Stdout), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading golden (run 'go test ./pkg/checks/audit -update'): %v", err)
	}
	if !bytes.Equal([]byte(res.Stdout), want) {
		t.Errorf("output does not match %s:\ngot:\n%s\nwant:\n%s", path, res.Stdout, want)
	}
}

// The acceptance criterion of #185: a namespace with a default-deny —
// podSelector {}, which selects every pod in it — is covered, and none
// of its workloads may be reported. An empty selector is the one shape
// a naive "does any policy name this workload" implementation gets
// backwards, because it names nothing and matches everything.
func TestNetpolDefaultDenyCoversEveryone(t *testing.T) {
	objs := []runtime.Object{
		nsObj("prod", nil),
		netpol("prod", "default-deny-all", nil,
			networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress),
		deploy("prod", "checkout", 3, app("checkout")),
		deploy("prod", "payments", 2, app("payments")),
	}
	res := checktest.Run(t, audit.NetpolCommand(testDeps(objs...)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.HasPrefix(res.Stdout, "scanned=2 findings=0 ") {
		t.Errorf("a default-deny namespace is covered in both directions, got:\n%s", res.Stdout)
	}
}

// A namespace with no policies is one decision with one remedy, so it
// is one finding per direction against the NAMESPACE — not one per
// workload, which would turn a single missing object into as many
// records as the namespace has Deployments.
func TestNetpolUnpolicedNamespaceIsReportedOnce(t *testing.T) {
	var objs []runtime.Object
	objs = append(objs, nsObj("prod", nil))
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		objs = append(objs, deploy("prod", name, 1, app(name)))
	}
	res := checktest.Run(t, audit.NetpolCommand(testDeps(objs...)), "-A")
	recs := findingLines(t, res.Stdout)
	if len(recs) != 2 {
		t.Fatalf("want one finding per direction over 5 workloads, got %d:\n%s", len(recs), res.Stdout)
	}
	for _, r := range recs {
		if r["kind_of_object"] != "Namespace" || r["name"] != "prod" {
			t.Errorf("subject should be the namespace: %v", r)
		}
		if r["workloads"] != "5" {
			t.Errorf("workloads = %q, want 5 — the claim covers all of them: %v", r["workloads"], r)
		}
		if r["policies"] != "0" {
			t.Errorf("policies = %q, want 0: %v", r["policies"], r)
		}
	}
	byReason := map[string]string{}
	for _, r := range recs {
		byReason[r["reason"]] = r["severity"]
	}
	if got := byReason["NoIngressPolicies"]; got != "warning" {
		t.Errorf("unrestricted ingress is the lateral-movement path, want warning, got %q", got)
	}
	// Egress policy is far less widely adopted and breaks DNS when
	// applied carelessly. The claim is still made — a fleet that has
	// chosen not to adopt it should not read as a fleet of warnings.
	if got := byReason["NoEgressPolicies"]; got != "info" {
		t.Errorf("absent egress policy is a posture choice, want info, got %q", got)
	}
}

// Policies that exist and select nothing are a different defect from
// policies that do not exist: nobody chose it, and the namespace reads
// as policed to anyone counting objects. Separate reason, and a warning
// in BOTH directions, unlike the absence claims.
func TestNetpolPoliciesThatSelectNothing(t *testing.T) {
	objs := []runtime.Object{
		nsObj("legacy", nil),
		netpol("legacy", "typo", matching(map[string]string{"app": "vender"}),
			networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress),
		deploy("legacy", "vendor", 1, app("vendor")),
	}
	res := checktest.Run(t, audit.NetpolCommand(testDeps(objs...)), "-A")
	recs := findingLines(t, res.Stdout)
	if len(recs) != 2 {
		t.Fatalf("want one finding per direction, got %d:\n%s", len(recs), res.Stdout)
	}
	for _, r := range recs {
		if r["kind_of_object"] != "Namespace" {
			t.Errorf("nothing in the namespace is covered, so the namespace is the subject: %v", r)
		}
		if r["severity"] != "warning" {
			t.Errorf("a selector that matches nothing is a mistake in either direction, got %q: %v", r["severity"], r)
		}
		if r["policies"] != "1" {
			t.Errorf("policies = %q, want 1 — the policy exists, that is the point: %v", r["policies"], r)
		}
	}
	wantReasons := map[string]bool{
		"IngressPoliciesSelectNothing": true,
		"EgressPoliciesSelectNothing":  true,
	}
	for _, r := range recs {
		if !wantReasons[r["reason"]] {
			t.Errorf("reason = %q, want one of %v", r["reason"], wantReasons)
		}
	}
}

// The high-value half: the namespace is policed and one workload fell
// out of the selectors. The subject is that workload, because the fix
// is its labels or a policy naming them — not a namespace decision.
func TestNetpolUnselectedWorkloadIsItsOwnSubject(t *testing.T) {
	res := checktest.Run(t, audit.NetpolCommand(testDeps(netpolCluster()...)), "--namespace=prod")
	recs := findingLines(t, res.Stdout)
	if len(recs) != 1 {
		t.Fatalf("only payments is unselected, got %d findings:\n%s", len(recs), res.Stdout)
	}
	r := recs[0]
	if r["reason"] != "UnselectedEgress" {
		t.Errorf("reason = %q, want UnselectedEgress", r["reason"])
	}
	if r["kind_of_object"] != "Deployment" || r["name"] != "payments" {
		t.Errorf("subject should be the workload that fell through: %v", r)
	}
	if r["covered_workloads"] != "1" {
		t.Errorf("covered_workloads = %q, want 1 (checkout): %v", r["covered_workloads"], r)
	}
	// The labels are the evidence: they are what the selector missed.
	if r["pod_labels"] != "app=payments" {
		t.Errorf("pod_labels = %q, want app=payments", r["pod_labels"])
	}
}

// policyTypes is defaulted by the API server, but the default has to be
// implemented rather than assumed — a policy read from a manifest or
// stored before the field existed arrives with it empty, and treating
// that as "covers nothing" would report a locked-down namespace as wide
// open.
func TestNetpolPolicyTypesDefaulting(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy *networkingv1.NetworkPolicy
		// wantReason is the one finding expected, or "" for a namespace
		// that comes out covered in both directions.
		wantReason string
	}{
		{
			name:       "unset-with-no-rules-is-ingress-only",
			policy:     undefaulted(netpol("prod", "p", nil)),
			wantReason: "NoEgressPolicies",
		},
		{
			name:       "unset-with-an-egress-rule-covers-both",
			policy:     withEgressRule(undefaulted(netpol("prod", "p", nil))),
			wantReason: "",
		},
		{
			name:       "explicit-egress-only-leaves-ingress-open",
			policy:     netpol("prod", "p", nil, networkingv1.PolicyTypeEgress),
			wantReason: "NoIngressPolicies",
		},
		{
			name: "explicit-both",
			policy: netpol("prod", "p", nil,
				networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress),
			wantReason: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objs := []runtime.Object{
				nsObj("prod", nil),
				tc.policy,
				deploy("prod", "checkout", 1, app("checkout")),
			}
			res := checktest.Run(t, audit.NetpolCommand(testDeps(objs...)), "-A")
			recs := findingLines(t, res.Stdout)
			if tc.wantReason == "" {
				if len(recs) != 0 {
					t.Fatalf("want both directions covered, got:\n%s", res.Stdout)
				}
				return
			}
			if len(recs) != 1 {
				t.Fatalf("want exactly %s, got:\n%s", tc.wantReason, res.Stdout)
			}
			if recs[0]["reason"] != tc.wantReason {
				t.Errorf("reason = %q, want %q", recs[0]["reason"], tc.wantReason)
			}
		})
	}
}

// NetworkPolicy selects pods; a pod on the node's network stack is not
// one it can reach. Such templates are left out of the arithmetic
// rather than reported as uncovered — no policy an operator could write
// would fix them — and the omission is visible in the output rather
// than silent.
func TestNetpolHostNetworkIsOutsideThePolicyModel(t *testing.T) {
	res := checktest.Run(t, audit.NetpolCommand(testDeps(netpolCluster()...)), "--namespace=platform")
	recs := findingLines(t, res.Stdout)
	if len(recs) != 2 {
		t.Fatalf("want one finding per direction, got %d:\n%s", len(recs), res.Stdout)
	}
	for _, r := range recs {
		if r["workloads"] != "2" {
			t.Errorf("workloads = %q, want 2 — the hostNetwork DaemonSet is not one of them: %v", r["workloads"], r)
		}
		if r["host_network_workloads"] != "1" {
			t.Errorf("the exclusion must be visible in the record, got %q: %v", r["host_network_workloads"], r)
		}
	}
	if strings.Contains(res.Stdout, "logging") {
		t.Errorf("a hostNetwork template must not be reported as uncovered:\n%s", res.Stdout)
	}
}

// A namespace whose only templates are hostNetwork has nothing
// NetworkPolicy could protect, so it makes no claim at all — as opposed
// to a claim about zero workloads, which would be noise in every
// cluster that runs a CNI.
func TestNetpolNamespaceWithNothingToProtectIsSilent(t *testing.T) {
	objs := []runtime.Object{
		nsObj("edge", nil),
		daemon("edge", "edge-cni", onHostNetwork(app("edge-cni"))),
		nsObj("blank", nil),
	}
	res := checktest.Run(t, audit.NetpolCommand(testDeps(objs...)), "-A")
	if !strings.HasPrefix(res.Stdout, "scanned=1 findings=0 ") {
		t.Errorf("neither an empty namespace nor a hostNetwork-only one makes a claim, got:\n%s", res.Stdout)
	}
}

// The same objects the sibling detectors judge, judged once: a Job or
// Pod carrying an ownerReference is a copy of a template that belongs
// to the object an operator would edit.
func TestNetpolJudgesOwnedObjectsOnceAtTheOwner(t *testing.T) {
	objs := []runtime.Object{
		nsObj("platform", nil),
		cronJob("platform", "nightly", app("nightly")),
		owned(job("platform", "nightly-28", app("nightly")), "CronJob", "nightly"),
		owned(barePod("platform", "nightly-28-x9", app("nightly")), "Job", "nightly-28"),
	}
	res := checktest.Run(t, audit.NetpolCommand(testDeps(objs...)), "-A")
	if !strings.Contains(res.Stdout, "scanned=1 ") {
		t.Errorf("want scanned=1 (the CronJob only):\n%s", res.Stdout)
	}
	for _, unwanted := range []string{"nightly-28", "nightly-28-x9"} {
		if strings.Contains(res.Stdout, unwanted) {
			t.Errorf("owned object %s should be judged at its owner:\n%s", unwanted, res.Stdout)
		}
	}
}

// The posture recipe: the fingerprint is the CLASS, so the same gap in
// two namespaces shares it and a fleet rollup can count them together.
func TestNetpolFingerprintIsClassNotInstance(t *testing.T) {
	objs := []runtime.Object{
		nsObj("a", nil), nsObj("b", nil),
		deploy("a", "one", 1, app("one")),
		deploy("b", "two", 1, app("two")),
	}
	res := checktest.Run(t, audit.NetpolCommand(testDeps(objs...)), "-A")
	byReason := map[string][]string{}
	for _, r := range findingLines(t, res.Stdout) {
		byReason[r["reason"]] = append(byReason[r["reason"]], r["fingerprint"])
	}
	if len(byReason) != 2 {
		t.Fatalf("want both directional reasons, got %v", byReason)
	}
	for reason, fps := range byReason {
		if len(fps) != 2 {
			t.Fatalf("%s: want one finding per namespace, got %v", reason, fps)
		}
		if fps[0] != fps[1] {
			t.Errorf("%s: the same gap in two namespaces is one class: %s vs %s", reason, fps[0], fps[1])
		}
		if !strings.HasPrefix(fps[0], "sha256:") {
			t.Errorf("%s: no fingerprint: %s", reason, fps[0])
		}
	}
}

func TestNetpolScopes(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"all-namespaces", []string{"-A"}, "scanned=8 findings=5 elapsed=100ms namespaces=4"},
		{"one-namespace", []string{"--namespace=legacy"}, "scanned=1 findings=2 elapsed=100ms namespaces=1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, audit.NetpolCommand(testDeps(netpolCluster()...)), tc.args...)
			if res.Code != emit.ExitData {
				t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
			}
			if !strings.Contains(res.Stdout, tc.want) {
				t.Errorf("summary should be %q:\n%s", tc.want, res.Stdout)
			}
		})
	}
}

func TestNetpolScopeErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"no-scope", nil, "no scope"},
		{"workload", []string{"--workload=Deployment/prod/checkout"}, "scoped by namespace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, audit.NetpolCommand(testDeps(netpolCluster()...)), tc.args...)
			if res.Code != emit.ExitUsage {
				t.Fatalf("exit %d, want %d (usage); stderr: %s", res.Code, emit.ExitUsage, res.Stderr)
			}
			if !strings.Contains(res.Stderr, tc.want) {
				t.Errorf("stderr = %q, want it to mention %q", res.Stderr, tc.want)
			}
		})
	}
}

// A --namespace that does not exist is a runtime error, not an empty
// clean scan: "scanned=0 findings=0" would read as "this namespace is
// fully policed".
func TestNetpolUnknownNamespaceIsAnError(t *testing.T) {
	res := checktest.Run(t, audit.NetpolCommand(testDeps(netpolCluster()...)), "--namespace=ghost")
	if res.Code != emit.ExitRuntime {
		t.Fatalf("exit %d, want %d (runtime); stderr: %s", res.Code, emit.ExitRuntime, res.Stderr)
	}
	if res.Stdout != "" {
		t.Errorf("a failed run must not emit a summary line, got %q", res.Stdout)
	}
}
