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
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
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
	for _, want := range []string{"audit workloads", "audit hardening", "audit netpol", "audit cluster", "audit upgrades"} {
		if !seenChecks[want] {
			t.Errorf("missing findings from %q; saw %v\nstdout:\n%s", want, seenChecks, res.Stdout)
		}
	}
	if !strings.Contains(res.Stdout, "checks=6") {
		t.Errorf("summary should report checks=6 without --exemptions:\n%s", res.Stdout)
	}

	withExemptions := checktest.Run(t, cmd, "-A", "--exemptions="+write(t, fixture))
	if withExemptions.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", withExemptions.Code, withExemptions.Stderr)
	}
	if !strings.Contains(withExemptions.Stdout, "checks=7") {
		t.Errorf("summary should report checks=7 with --exemptions:\n%s", withExemptions.Stdout)
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
