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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func resourceQuota(namespace, name string, used, hard corev1.ResourceList) *corev1.ResourceQuota {
	return &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Status: corev1.ResourceQuotaStatus{
			Used: used,
			Hard: hard,
		},
	}
}

func limitRange(namespace, name string) *corev1.LimitRange {
	return &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
	}
}

func unboundedContainer(name string) corev1.Container {
	c := container(name, true, true)
	c.Resources = corev1.ResourceRequirements{}
	return c
}

func boundedContainer(name string) corev1.Container {
	return container(name, true, true)
}

func findByReason(records []map[string]string, reason string) map[string]string {
	for _, r := range records {
		if r["reason"] == reason {
			return r
		}
	}
	return nil
}

func TestNamespacesNoResourceQuotaAndNoLimitRange(t *testing.T) {
	cmd := audit.NamespacesCommand(testDeps(
		nsObj("prod", nil),
		nsObj("empty", nil),
		deploy("prod", "api", 2, template(nil, unboundedContainer("api"))),
	))
	res := checktest.Run(t, cmd, "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	records := findingLines(t, res.Stdout)

	q := findByReason(records, "NoResourceQuota")
	if q == nil {
		t.Fatalf("expected NoResourceQuota finding, got: %v", records)
	}
	if q["kind"] != "audit.no_resource_quota" || q["severity"] != "info" || q["namespace"] != "prod" || q["kind_of_object"] != "Namespace" || q["name"] != "prod" || q["workloads"] != "1" {
		t.Errorf("unexpected NoResourceQuota fields: %v", q)
	}

	lr := findByReason(records, "NoLimitRange")
	if lr == nil {
		t.Fatalf("expected NoLimitRange finding, got: %v", records)
	}
	if lr["kind"] != "audit.no_limit_range" || lr["severity"] != "info" || lr["namespace"] != "prod" || lr["kind_of_object"] != "Namespace" || lr["name"] != "prod" || lr["workloads"] != "1" {
		t.Errorf("unexpected NoLimitRange fields: %v", lr)
	}

	for _, r := range records {
		if r["namespace"] == "empty" {
			t.Errorf("non-workload-bearing namespace 'empty' should not be flagged under -A: %v", r)
		}
	}
}

func TestNamespacesResourceQuotaUsedNearHard(t *testing.T) {
	cmd := audit.NamespacesCommand(testDeps(
		nsObj("prod", nil),
		deploy("prod", "api", 2, template(nil, boundedContainer("api"))),
		limitRange("prod", "mem-cpu-defaults"),
		resourceQuota("prod", "compute-quota",
			corev1.ResourceList{
				corev1.ResourceRequestsCPU:    resource.MustParse("900m"),
				corev1.ResourceRequestsMemory: resource.MustParse("500Mi"),
			},
			corev1.ResourceList{
				corev1.ResourceRequestsCPU:    resource.MustParse("1000m"),
				corev1.ResourceRequestsMemory: resource.MustParse("1000Mi"),
			},
		),
	))
	res := checktest.Run(t, cmd, "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	records := findingLines(t, res.Stdout)
	if len(records) != 1 {
		t.Fatalf("expected 1 finding (ResourceQuotaUsedNearHard), got %d: %v", len(records), records)
	}
	r := records[0]
	if r["kind"] != "audit.no_resource_quota" || r["reason"] != "ResourceQuotaUsedNearHard" || r["severity"] != "warning" {
		t.Errorf("unexpected finding header: %v", r)
	}
	if r["quota"] != "compute-quota" || r["resource"] != "requests.cpu" || r["used"] != "900m" || r["hard"] != "1" || r["pct"] != "90" {
		t.Errorf("unexpected quota details: %v", r)
	}
}

func TestNamespacesLimitRangeSuppressedWhenAllContainersSetRequestsAndLimits(t *testing.T) {
	cmd := audit.NamespacesCommand(testDeps(
		nsObj("prod", nil),
		deploy("prod", "api", 2, template(nil, boundedContainer("api"))),
		resourceQuota("prod", "q",
			corev1.ResourceList{corev1.ResourcePods: resource.MustParse("2")},
			corev1.ResourceList{corev1.ResourcePods: resource.MustParse("10")},
		),
	))
	res := checktest.Run(t, cmd, "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	records := findingLines(t, res.Stdout)
	if len(records) != 0 {
		t.Errorf("expected 0 findings when quota exists and all containers have requests/limits, got: %v", records)
	}
}

func TestNamespacesOnboardingRequiredLabelsFlagAndPolicy(t *testing.T) {
	cmd := audit.NamespacesCommand(testDeps(
		nsObj("prod", map[string]string{"owner": "payments"}),
		nsObj("staging", map[string]string{"owner": "payments", "team": "core", "cost-center": "cc-123"}),
		deploy("prod", "api", 1, template(nil, boundedContainer("api"))),
		deploy("staging", "api", 1, template(nil, boundedContainer("api"))),
		resourceQuota("prod", "q", nil, nil),
		resourceQuota("staging", "q", nil, nil),
	))

	res := checktest.Run(t, cmd, "-A", "--required-labels=owner,team,cost-center")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	records := findingLines(t, res.Stdout)
	if len(records) != 1 {
		t.Fatalf("expected 1 onboarding finding for prod, got %d: %v", len(records), records)
	}
	r := records[0]
	if r["kind"] != "audit.namespace_onboarding_gaps" || r["reason"] != "MissingRequiredLabels" || r["severity"] != "info" {
		t.Errorf("unexpected finding kind/reason/severity: %v", r)
	}
	if r["namespace"] != "prod" || r["missing_labels"] != "team,cost-center" || r["workloads"] != "1" {
		t.Errorf("unexpected onboarding details: %v", r)
	}

	policyYAML := `checks:
  audit.namespace_onboarding_gaps:
    params:
      required_labels: "owner,team"
`
	polRes := checktest.Run(t, cmd, "-A", "--policy="+write(t, policyYAML))
	if polRes.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", polRes.Code, polRes.Stderr)
	}
	polRecords := findingLines(t, polRes.Stdout)
	if len(polRecords) != 1 || polRecords[0]["missing_labels"] != "team" {
		t.Errorf("expected missing_labels=team from policy param, got: %v", polRecords)
	}
}

func TestNamespacesSystemNamespacesSkippedUnderAllNamespacesUnlessTargeted(t *testing.T) {
	cmd := audit.NamespacesCommand(testDeps(
		nsObj("kube-system", nil),
		nsObj("gke-managed-cim", nil),
		nsObj("gmp-system", nil),
		deploy("kube-system", "coredns", 2, template(nil, container("coredns", true, true))),
		deploy("gke-managed-cim", "agent", 1, template(nil, container("agent", true, true))),
		deploy("gmp-system", "collector", 1, template(nil, container("collector", true, true))),
	))

	allRes := checktest.Run(t, cmd, "-A")
	if allRes.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", allRes.Code, allRes.Stderr)
	}
	if got := findingLines(t, allRes.Stdout); len(got) != 0 {
		t.Errorf("system namespaces should be skipped by default under -A, got: %v", got)
	}

	skipFlagRes := checktest.Run(t, cmd, "-A", "--skip-system-namespaces")
	if skipFlagRes.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", skipFlagRes.Code, skipFlagRes.Stderr)
	}
	if !strings.Contains(skipFlagRes.Stdout, "skipped_system_namespaces=3") {
		t.Errorf("expected skipped_system_namespaces=3 note, got:\n%s", skipFlagRes.Stdout)
	}

	targetedRes := checktest.Run(t, cmd, "--namespace=kube-system")
	if targetedRes.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", targetedRes.Code, targetedRes.Stderr)
	}
	if got := findingLines(t, targetedRes.Stdout); len(got) == 0 {
		t.Errorf("explicit --namespace=kube-system should audit the targeted namespace")
	}
}

func TestNamespacesScopeValidation(t *testing.T) {
	cmd := audit.NamespacesCommand(testDeps())
	if res := checktest.Run(t, cmd); res.Code != emit.ExitUsage {
		t.Errorf("no scope: exit %d, want %d", res.Code, emit.ExitUsage)
	}
	if res := checktest.Run(t, cmd, "--workload=Deployment/prod/api"); res.Code != emit.ExitUsage {
		t.Errorf("--workload scope: exit %d, want %d", res.Code, emit.ExitUsage)
	}
}
