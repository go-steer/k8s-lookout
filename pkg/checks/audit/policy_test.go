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
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func TestAuditPolicyContract(t *testing.T) {
	checktest.VerifyContract(t, audit.PolicyCommand())
}

func TestAuditPolicyDefaultAndProfiles(t *testing.T) {
	res := checktest.Run(t, audit.PolicyCommand(), "--profile=strict")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "policy=strict suppressed=0") {
		t.Errorf("stdout missing policy=strict suppressed=0:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "target_kind=audit.no_liveness_probe enabled=true effective_severity=warning default_severity=info source=profile") {
		t.Errorf("stdout missing promoted audit.no_liveness_probe rule:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "target_kind=audit.liveness_preempts_readiness/LivenessDeadlineTiesReadiness enabled=true effective_severity=warning default_severity=info source=profile") {
		t.Errorf("stdout missing promoted LivenessDeadlineTiesReadiness rule:\n%s", res.Stdout)
	}
}

func TestAuditPolicyFileAndFlagOverrides(t *testing.T) {
	dir := t.TempDir()
	polFile := filepath.Join(dir, "policy.yaml")
	err := os.WriteFile(polFile, []byte(`
name: team-sec
profile: gke-autopilot
checks:
  audit.no_pdb:
    enabled: false
  audit.suspended_cronjob:
    severity: warning
    params:
      suspended_for: 72h
`), 0o600)
	if err != nil {
		t.Fatalf("write policy.yaml: %v", err)
	}

	res := checktest.Run(t, audit.PolicyCommand(),
		"--policy="+polFile,
		"--skip=audit.single_replica",
		"--set=audit.pdb_blocks_drain.min_replicas=4",
	)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "policy=team-sec suppressed=0") {
		t.Errorf("stdout missing policy=team-sec suppressed=0:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "target_kind=audit.privileged_container enabled=false effective_severity=critical default_severity=critical source=profile") {
		t.Errorf("stdout missing disabled privileged_container (from gke-autopilot):\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "target_kind=audit.no_pdb enabled=false effective_severity=warning default_severity=warning source=file") {
		t.Errorf("stdout missing disabled no_pdb (from file):\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "target_kind=audit.single_replica enabled=false effective_severity=warning default_severity=warning source=flag") {
		t.Errorf("stdout missing disabled single_replica (from flag):\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, `target_kind=audit.suspended_cronjob enabled=true effective_severity=warning default_severity=info source=file params="suspended_for=72h"`) {
		t.Errorf("stdout missing suspended_cronjob override:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, `target_kind=audit.pdb_blocks_drain enabled=true effective_severity=warning default_severity=warning source=flag params="min_replicas=4"`) {
		t.Errorf("stdout missing pdb_blocks_drain --set override:\n%s", res.Stdout)
	}
}

func TestAuditPolicyUsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{
			name: "unknown profile",
			args: []string{"--profile=invalid-profile"},
		},
		{
			name: "missing policy file",
			args: []string{"--policy=/nonexistent/policy.yaml"},
		},
		{
			name: "invalid min-severity",
			args: []string{"--min-severity=super-critical"},
		},
		{
			name: "malformed set",
			args: []string{"--set=bad_set_expr"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, audit.PolicyCommand(), tc.args...)
			if res.Code != emit.ExitUsage {
				t.Errorf("Code = %d, want %d (ExitUsage); stderr=%s", res.Code, emit.ExitUsage, res.Stderr)
			}
			if res.Stdout != "" {
				t.Errorf("Stdout = %q, want empty on usage error", res.Stdout)
			}
		})
	}
}

func TestDetectorPolicyFilteringAndSummary(t *testing.T) {
	objs := []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "prod"}},
		deploy("prod", "api", 1, template(lbl("api"), container("api", false, false))),
	}
	cmd := audit.WorkloadsCommand(testDeps(objs...))

	res := checktest.Run(t, cmd, "--namespace=prod", "--profile=strict", "--skip=audit.single_replica")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "policy=strict suppressed=1") {
		t.Errorf("expected policy=strict suppressed=1 in summary:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "kind=audit.single_replica") {
		t.Errorf("expected audit.single_replica to be suppressed:\n%s", res.Stdout)
	}
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "audit.no_liveness_probe" && r["severity"] != "warning" {
			t.Errorf("expected audit.no_liveness_probe promoted to warning under strict, got %q", r["severity"])
		}
	}
}

func TestSkipSystemNamespaces(t *testing.T) {
	objs := []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "prod"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "gke-gmp-system"}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "default"}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "default"}},
		deploy("prod", "api", 1, template(lbl("api"), container("api", false, false))),
		deploy("kube-system", "coredns", 1, template(lbl("coredns"), container("coredns", false, false))),
		deploy("gke-gmp-system", "collector", 1, template(lbl("collector"), container("collector", false, false))),
	}
	res := checktest.Run(t, audit.AllCommand(testDeps(objs...)), "-A", "--skip-system-namespaces")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "skipped_system_namespaces=2") {
		t.Errorf("expected skipped_system_namespaces=2 in summary:\n%s", res.Stdout)
	}
	for _, r := range findingLines(t, res.Stdout) {
		if r["namespace"] == "kube-system" || r["namespace"] == "gke-gmp-system" {
			t.Errorf("unexpected finding in system namespace %q: %v", r["namespace"], r)
		}
	}
}

func TestRBACHolderWorkloadAttributionAndPerWorkload(t *testing.T) {
	tpl := template(lbl("runner"), container("runner", true, true))
	tpl.Spec.ServiceAccountName = "deployer"
	objs := append(bootstrapRBAC(),
		clusterRole("platform-superuser", nil, fullWildcard()),
		clusterBinding("superuser-ci", nil, "platform-superuser", sa("ci", "deployer")),
		deploy("ci", "runner-a", 1, tpl),
		deploy("ci", "runner-b", 1, tpl),
	)

	cmd := audit.RBACCommand(testDeps(objs...))
	res := checktest.Run(t, cmd, "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	bindingRec := findRecord(t, res.Stdout, "audit.cluster_admin_binding", "ClusterRoleBinding", "", "superuser-ci")
	if bindingRec["workloads"] != "2" || bindingRec["workload_names"] != "Deployment/runner-a,Deployment/runner-b" {
		t.Errorf("binding workload attribution wrong: workloads=%q workload_names=%q", bindingRec["workloads"], bindingRec["workload_names"])
	}
	roleRec := findRecord(t, res.Stdout, "audit.wildcard_rbac", "ClusterRole", "", "platform-superuser")
	if roleRec["workloads"] != "2" || roleRec["workload_names"] != "Deployment/runner-a,Deployment/runner-b" {
		t.Errorf("role workload attribution wrong: workloads=%q workload_names=%q", roleRec["workloads"], roleRec["workload_names"])
	}

	perWl := checktest.Run(t, cmd, "-A", "--per-workload")
	if perWl.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", perWl.Code, perWl.Stderr)
	}
	findRecord(t, perWl.Stdout, "audit.cluster_admin_binding", "Deployment", "ci", "runner-a")
	findRecord(t, perWl.Stdout, "audit.cluster_admin_binding", "Deployment", "ci", "runner-b")
	findRecord(t, perWl.Stdout, "audit.wildcard_rbac", "Deployment", "ci", "runner-a")
	findRecord(t, perWl.Stdout, "audit.wildcard_rbac", "Deployment", "ci", "runner-b")
}

func TestHardeningDefaultSAPerWorkload(t *testing.T) {
	objs := []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "prod"}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "default"}},
		deploy("prod", "web", 1, template(lbl("web"), container("web", true, true))),
		deploy("prod", "worker", 1, template(lbl("worker"), container("worker", true, true))),
	}
	cmd := audit.HardeningCommand(testDeps(objs...))
	res := checktest.Run(t, cmd, "--namespace=prod", "--per-workload")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	findRecord(t, res.Stdout, "audit.default_sa_automount", "Deployment", "prod", "web")
	findRecord(t, res.Stdout, "audit.default_sa_automount", "Deployment", "prod", "worker")
}
