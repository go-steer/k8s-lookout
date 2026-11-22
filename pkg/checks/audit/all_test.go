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
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func TestAllContract(t *testing.T) {
	objs := append(postureCluster(),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "prod"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "batch"}},
	)
	checktest.VerifyContract(t, audit.AllCommand(testDeps(objs...)), "-A")
}

func TestAllSharesSnapshotAcrossDetectors(t *testing.T) {
	objs := []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "prod"}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "default"}},
		deploy("prod", "api", 2, template(lbl("api"), container("api", true, true))),
		svc("prod", "api", lbl("api"), port("http", 80, "http")),
	}
	client := fake.NewClientset(objs...)

	var (
		mu     sync.Mutex
		counts = map[string]int{}
	)
	client.PrependReactor("list", "*", func(a k8stesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		counts[a.GetResource().Resource]++
		mu.Unlock()
		return false, nil, nil
	})

	deps := audit.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return client, nil },
	}
	res := checktest.Run(t, audit.AllCommand(deps), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}

	shared := []string{
		"deployments",
		"statefulsets",
		"daemonsets",
		"cronjobs",
		"jobs",
		"pods",
		"services",
		"namespaces",
		"serviceaccounts",
	}
	for _, r := range shared {
		if got := counts[r]; got != 1 {
			t.Errorf("resource %q listed %d times across audit all, want 1", r, got)
		}
	}
}

func TestAllStampsSubcommandAndRunsExemptionsWhenProvided(t *testing.T) {
	objs := []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "prod"}},
		deploy("prod", "api", 1, template(lbl("api"), container("api", false, false))),
	}
	cmd := audit.AllCommand(testDeps(objs...))

	res := checktest.Run(t, cmd, "--namespace=prod")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	seenChecks := map[string]bool{}
	for _, r := range findingLines(t, res.Stdout) {
		if r["check"] == "" {
			t.Errorf("finding missing check= stamp: %v", r)
		}
		seenChecks[r["check"]] = true
	}
	for _, want := range []string{"audit workloads", "audit hardening", "audit netpol", "audit namespaces", "audit cluster", "audit upgrades"} {
		if !seenChecks[want] {
			t.Errorf("missing findings from %q; saw %v\nstdout:\n%s", want, seenChecks, res.Stdout)
		}
	}
	if !strings.Contains(res.Stdout, "checks=9") {
		t.Errorf("summary should report checks=9 without --exemptions:\n%s", res.Stdout)
	}

	withExemptions := checktest.Run(t, cmd, "-A", "--exemptions="+write(t, fixture))
	if withExemptions.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", withExemptions.Code, withExemptions.Stderr)
	}
	if !strings.Contains(withExemptions.Stdout, "checks=10") {
		t.Errorf("summary should report checks=10 with --exemptions:\n%s", withExemptions.Stdout)
	}
}

func TestAllRunsEveryNonMetaAuditDetector(t *testing.T) {
	meta := map[string]bool{
		"audit all":        true,
		"audit exemptions": true,
		"audit policy":     true,
	}
	var wantDetectors []string
	for _, c := range checks.Default().GroupCommands("audit") {
		if meta[c.Name] {
			continue
		}
		wantDetectors = append(wantDetectors, c.Name)
	}
	if len(wantDetectors) == 0 {
		t.Fatal("expected non-meta commands in audit group")
	}

	aiContainer := container("vllm", false, false)
	aiContainer.Image = "vllm/vllm-openai:latest"
	aiTpl := template(lbl("vllm"), aiContainer)
	aiTpl.Spec.NodeSelector = map[string]string{
		"cloud.google.com/compute-class": "missing-cc",
	}

	clusterAdmin := clusterRole("cluster-admin", map[string]string{"kubernetes.io/bootstrapping": "rbac-defaults"}, fullWildcard())
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "danger-binding"},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "cluster-admin"},
		Subjects:   []rbacv1.Subject{{Kind: "User", Name: "bob@example.com"}},
	}

	deps := testDepsWithComputeClasses(
		[]*unstructured.Unstructured{
			makeComputeClassCR("existing-cc", []map[string]any{{"machineFamily": "n2", "spot": false}}, false),
		},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "prod"}},
		deploy("prod", "vllm", 1, aiTpl),
		svc("prod", "vllm", lbl("vllm"), port("http", 8000, "http")),
		clusterAdmin,
		binding,
	)

	cmd := audit.AllCommand(deps)
	res := checktest.Run(t, cmd, "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if err := checktest.Verify(cmd, res.Stdout, emit.FormatLogfmt); err != nil {
		t.Fatalf("contract: %v\n%s", err, res.Stdout)
	}

	wantChecksNote := fmt.Sprintf("checks=%d", len(wantDetectors))
	if !strings.Contains(res.Stdout, wantChecksNote) {
		t.Errorf("summary missing %s:\n%s", wantChecksNote, res.Stdout)
	}

	seenChecks := map[string]bool{}
	sawComputeClassMissing := false
	sawAIExposure := false
	for _, r := range findingLines(t, res.Stdout) {
		seenChecks[r["check"]] = true
		if r["check"] == "audit computeclass" && r["kind"] == "audit.compute_class_posture" && r["reason"] == "ComputeClassMissing" {
			sawComputeClassMissing = true
		}
		if r["check"] == "audit ai" && r["kind"] == "audit.ai_workload_exposure" {
			sawAIExposure = true
		}
	}
	for _, want := range wantDetectors {
		if !seenChecks[want] {
			t.Errorf("detector %q did not emit findings during audit all; saw %v\nstdout:\n%s", want, seenChecks, res.Stdout)
		}
	}
	if !sawComputeClassMissing {
		t.Errorf("expected audit computeclass to run with caller Deps and emit ComputeClassMissing:\n%s", res.Stdout)
	}
	if !sawAIExposure {
		t.Errorf("expected audit ai to run and emit audit.ai_workload_exposure:\n%s", res.Stdout)
	}
}

func TestAllScopeValidation(t *testing.T) {
	cmd := audit.AllCommand(testDeps())
	if res := checktest.Run(t, cmd); res.Code != emit.ExitUsage {
		t.Errorf("no scope: exit %d, want %d", res.Code, emit.ExitUsage)
	}
	if res := checktest.Run(t, cmd, "--workload=Deployment/prod/api"); res.Code != emit.ExitUsage {
		t.Errorf("--workload: exit %d, want %d", res.Code, emit.ExitUsage)
	}
}

func TestAllSkipSystemNamespacesHonoredAcrossStages(t *testing.T) {
	aiContainer := container("vllm", false, false)
	aiContainer.Image = "vllm/vllm-openai:latest"
	aiTpl := template(lbl("vllm"), aiContainer)
	aiTpl.Spec.HostNetwork = true

	cmd := audit.AllCommand(testDeps(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system"}},
		deploy("kube-system", "vllm", 1, aiTpl),
	))
	res := checktest.Run(t, cmd, "--namespace=kube-system", "--skip-system-namespaces")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "skipped_system_namespaces=1") {
		t.Errorf("expected skipped_system_namespaces=1:\n%s", res.Stdout)
	}
	for _, r := range findingLines(t, res.Stdout) {
		if r["namespace"] == "kube-system" {
			t.Errorf("expected zero findings in skipped system namespace kube-system, got: %v", r)
		}
	}
}

func TestAllSharesSnapshotWithProductionNilClientDeps(t *testing.T) {
	var (
		mu     sync.Mutex
		counts = map[string]int{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		counts[r.URL.Path]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/version":
			_, _ = w.Write([]byte(`{"major":"1","minor":"32","gitVersion":"v1.32.1"}`))
		default:
			_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"List","items":[]}`))
		}
	}))
	defer srv.Close()

	kc := filepath.Join(t.TempDir(), "kubeconfig")
	body := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: local
  cluster:
    server: %s
contexts:
- name: local
  context:
    cluster: local
    user: local
current-context: local
users:
- name: local
  user: {}
`, srv.URL)
	if err := os.WriteFile(kc, []byte(body), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}

	// Production wiring has Client == nil. Every sub-check inside audit all
	// must still reuse the single Snapshot on ctx instead of minting a new
	// client per stage and re-listing pods/deployments 5 times.
	cmd := audit.AllCommand(audit.Deps{
		Provider: func(context.Context) (cloud.Provider, error) { return cloud.NoProvider, nil },
	})
	res := checktest.Run(t, cmd, "-A", "--kubeconfig="+kc)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}

	for _, path := range []string{
		"/apis/apps/v1/deployments",
		"/apis/apps/v1/statefulsets",
		"/apis/apps/v1/daemonsets",
		"/apis/batch/v1/jobs",
		"/apis/batch/v1/cronjobs",
		"/api/v1/pods",
		"/api/v1/services",
		"/api/v1/namespaces",
		"/api/v1/serviceaccounts",
	} {
		if got := counts[path]; got != 1 {
			t.Errorf("HTTP GET %s count = %d across production audit all, want 1", path, got)
		}
	}
}
