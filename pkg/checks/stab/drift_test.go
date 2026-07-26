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

package stab_test

import (
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/stab"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// ManagedFieldsEntry fixtures, built inline per §13: raw FieldsV1
// JSON exactly as the API server records it.

// gitopsSpec is a server-side-apply manager's typical footprint: 6
// spec leaf fields, always the majority in these fixtures.
const gitopsSpec = `{"f:metadata":{"f:labels":{"f:app":{}}},"f:spec":{"f:replicas":{},"f:selector":{},"f:template":{"f:spec":{"f:containers":{"k:{\"name\":\"app\"}":{".":{},"f:image":{},"f:name":{},"f:resources":{}}}}}}}`

// kubectlSpec is a kubectl edit touching replicas + image: 2 leaves,
// both high-blast-radius.
const kubectlSpec = `{"f:spec":{"f:replicas":{},"f:template":{"f:spec":{"f:containers":{"k:{\"name\":\"app\"}":{"f:image":{}}}}}}}`

// statusOnly is a controller co-manager owning nothing under f:spec.
const statusOnly = `{"f:status":{"f:availableReplicas":{},"f:conditions":{}}}`

// termGraceSpec is a low-blast-radius spec field: plain warning.
const termGraceSpec = `{"f:spec":{"f:template":{"f:spec":{"f:terminationGracePeriodSeconds":{}}}}}`

// envSpec owns one env var of one container: the .env[ path class.
const envSpec = `{"f:spec":{"f:template":{"f:spec":{"f:containers":{"k:{\"name\":\"app\"}":{"f:env":{"k:{\"name\":\"FOO\"}":{".":{},"f:name":{},"f:value":{}}}}}}}}}`

// tenFieldSpec owns 10 spec leaves — exercises the 8-path cap.
const tenFieldSpec = `{"f:spec":{"f:minReadySeconds":{},"f:paused":{},"f:progressDeadlineSeconds":{},"f:replicas":{},"f:revisionHistoryLimit":{},"f:strategy":{"f:type":{},"f:rollingUpdate":{"f:maxSurge":{},"f:maxUnavailable":{}}},"f:template":{"f:spec":{"f:dnsPolicy":{},"f:restartPolicy":{}}}}}`

func entry(manager string, op metav1.ManagedFieldsOperationType, t *metav1.Time, raw string) metav1.ManagedFieldsEntry {
	return metav1.ManagedFieldsEntry{
		Manager:    manager,
		Operation:  op,
		APIVersion: "apps/v1",
		Time:       t,
		FieldsType: "FieldsV1",
		FieldsV1:   &metav1.FieldsV1{Raw: []byte(raw)},
	}
}

func statusEntry(manager, subresource string) metav1.ManagedFieldsEntry {
	e := entry(manager, metav1.ManagedFieldsOperationUpdate, ptr(ago(time.Minute)), statusOnly)
	e.Subresource = subresource
	return e
}

func deployment(ns, name string, entries ...metav1.ManagedFieldsEntry) *appsv1.Deployment {
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: name, ManagedFields: entries,
	}}
}

func statefulSet(ns, name string, entries ...metav1.ManagedFieldsEntry) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: name, ManagedFields: entries,
	}}
}

// driftMixed is the golden fixture: one kubectl-edit drift
// (critical), one out-of-band co-manager without a timestamp
// (warning), status-only controllers (ignored), and a clean
// StatefulSet. argocd-controller owns the majority of spec leaves.
func driftMixed() []runtime.Object {
	return []runtime.Object{
		deployment("prod", "api",
			entry("argocd-controller", metav1.ManagedFieldsOperationApply, ptr(ago(240*time.Hour)), gitopsSpec),
			entry("kubectl-edit", metav1.ManagedFieldsOperationUpdate, ptr(ago(3*time.Hour+20*time.Minute)), kubectlSpec),
			statusEntry("kube-controller-manager", "status"),
		),
		deployment("prod", "worker",
			entry("argocd-controller", metav1.ManagedFieldsOperationApply, ptr(ago(240*time.Hour)), gitopsSpec),
			entry("helm-legacy", metav1.ManagedFieldsOperationUpdate, nil, termGraceSpec),
		),
		statefulSet("db", "postgres",
			entry("argocd-controller", metav1.ManagedFieldsOperationApply, ptr(ago(240*time.Hour)), gitopsSpec),
			statusEntry("kube-controller-manager", ""),
		),
	}
}

func TestDriftKubectlEdit(t *testing.T) {
	cmd := stab.DriftCommand(testDeps(
		deployment("prod", "api",
			entry("argocd-controller", metav1.ManagedFieldsOperationApply, ptr(ago(240*time.Hour)), gitopsSpec),
			entry("kubectl-edit", metav1.ManagedFieldsOperationUpdate, ptr(ago(3*time.Hour+20*time.Minute)), kubectlSpec),
		),
	))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 1 {
		t.Fatalf("want 1 finding, got %d: %v", len(recs), recs)
	}
	r := recs[0]
	want := map[string]string{
		"kind":           "drift.manual_edit",
		"severity":       "critical", // image + replicas are high blast radius
		"namespace":      "prod",
		"kind_of_object": "Deployment",
		"name":           "api",
		"reason":         "KubectlManualEdit",
		"manager":        "kubectl-edit",
		"operation":      "Update",
		"tool":           "kubectl",
		"fields":         "spec.replicas,spec.template.spec.containers[app].image",
		"field_count":    "2",
		"age":            "3h20m",
	}
	for k, v := range want {
		if r[k] != v {
			t.Errorf("finding[%s] = %q, want %q (finding: %v)", k, r[k], v, r)
		}
	}
	if strings.Contains(r["message"], "user") && !strings.Contains(r["message"], "manager") {
		t.Errorf("message must never claim a user identity: %q", r["message"])
	}
	sum := summaryLine(t, res.Stdout)
	if sum["scanned"] != "1" || sum["manager"] != "argocd-controller" || sum["detection"] != "majority" {
		t.Errorf("summary = %v, want scanned=1 manager=argocd-controller detection=majority", sum)
	}
}

// TestDriftStatusOnlyCoManagersIgnored: controllers that own only
// status — via the status subresource or a plain f:status entry —
// are the nominal state, never drift.
func TestDriftStatusOnlyCoManagersIgnored(t *testing.T) {
	cmd := stab.DriftCommand(testDeps(
		deployment("prod", "api",
			entry("argocd-controller", metav1.ManagedFieldsOperationApply, ptr(ago(time.Hour)), gitopsSpec),
			statusEntry("kube-controller-manager", "status"),
			statusEntry("deployment-controller", ""),
		),
	))
	res := checktest.Run(t, cmd)
	if recs := findingLines(t, res.Stdout); len(recs) != 0 {
		t.Fatalf("status-only co-managers must emit nothing, got %v", recs)
	}
	sum := summaryLine(t, res.Stdout)
	if sum["manager"] != "argocd-controller" || sum["detection"] != "majority" {
		t.Errorf("summary = %v, want manager=argocd-controller detection=majority", sum)
	}
}

// TestDriftMajorityTieBreak: equal spec-leaf totals resolve to the
// lexicographically smallest manager, deterministically, and the
// resolved manager is never reported as drift.
func TestDriftMajorityTieBreak(t *testing.T) {
	cmd := stab.DriftCommand(testDeps(
		deployment("prod", "api",
			entry("b-manager", metav1.ManagedFieldsOperationApply, ptr(ago(time.Hour)), termGraceSpec),
			entry("a-manager", metav1.ManagedFieldsOperationApply, ptr(ago(time.Hour)), `{"f:spec":{"f:paused":{}}}`),
		),
	))
	res := checktest.Run(t, cmd)
	recs := findingLines(t, res.Stdout)
	if len(recs) != 1 || recs[0]["manager"] != "b-manager" {
		t.Fatalf("want exactly one finding for b-manager (a-manager wins the tie), got %v", recs)
	}
	sum := summaryLine(t, res.Stdout)
	if sum["manager"] != "a-manager" || sum["detection"] != "majority" {
		t.Errorf("summary = %v, want manager=a-manager detection=majority", sum)
	}
}

// TestDriftManagerOverride: --manager reassigns who is foreign — the
// auto-detected majority manager itself becomes drift.
func TestDriftManagerOverride(t *testing.T) {
	cmd := stab.DriftCommand(testDeps(driftMixed()...))
	res := checktest.Run(t, cmd, "--manager=flux-controller")
	recs := findingLines(t, res.Stdout)
	managers := map[string]bool{}
	for _, r := range recs {
		managers[r["manager"]] = true
		if r["manager"] == "argocd-controller" && r["reason"] != "OutOfBandManager" {
			t.Errorf("argocd finding reason = %q, want OutOfBandManager", r["reason"])
		}
	}
	// argocd on 3 objects + kubectl-edit + helm-legacy = 5 findings.
	if len(recs) != 5 || !managers["argocd-controller"] || !managers["kubectl-edit"] || !managers["helm-legacy"] {
		t.Errorf("got %d findings for managers %v, want 5 across argocd-controller/kubectl-edit/helm-legacy", len(recs), managers)
	}
	sum := summaryLine(t, res.Stdout)
	if sum["manager"] != "flux-controller" || sum["detection"] != "declared" {
		t.Errorf("summary = %v, want manager=flux-controller detection=declared", sum)
	}
}

// TestDriftSeverity: image/replicas/env escalate to critical; a
// plain spec field stays a warning.
func TestDriftSeverity(t *testing.T) {
	for _, tc := range []struct {
		name, raw, severity string
	}{
		{"image", `{"f:spec":{"f:template":{"f:spec":{"f:containers":{"k:{\"name\":\"app\"}":{"f:image":{}}}}}}}`, "critical"},
		{"replicas", `{"f:spec":{"f:replicas":{}}}`, "critical"},
		{"env", envSpec, "critical"},
		{"termination_grace", termGraceSpec, "warning"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := stab.DriftCommand(testDeps(
				deployment("prod", "api",
					entry("argocd-controller", metav1.ManagedFieldsOperationApply, ptr(ago(time.Hour)), gitopsSpec),
					entry("rogue-operator", metav1.ManagedFieldsOperationUpdate, ptr(ago(time.Minute)), tc.raw),
				),
			))
			recs := findingLines(t, checktest.Run(t, cmd).Stdout)
			if len(recs) != 1 || recs[0]["severity"] != tc.severity {
				t.Fatalf("want one %s finding, got %v", tc.severity, recs)
			}
			if recs[0]["reason"] != "OutOfBandManager" {
				t.Errorf("reason = %q, want OutOfBandManager", recs[0]["reason"])
			}
		})
	}
}

// TestDriftEnvListKeyRendering: k:{"name":...} list keys render as
// [name] segments for containers and env entries alike.
func TestDriftEnvListKeyRendering(t *testing.T) {
	cmd := stab.DriftCommand(testDeps(
		deployment("prod", "api",
			entry("argocd-controller", metav1.ManagedFieldsOperationApply, ptr(ago(time.Hour)), gitopsSpec),
			entry("kubectl-patch", metav1.ManagedFieldsOperationUpdate, ptr(ago(45*time.Second)), envSpec),
		),
	))
	recs := findingLines(t, checktest.Run(t, cmd).Stdout)
	if len(recs) != 1 {
		t.Fatalf("want 1 finding, got %v", recs)
	}
	r := recs[0]
	want := "spec.template.spec.containers[app].env[FOO],spec.template.spec.containers[app].env[FOO].name,spec.template.spec.containers[app].env[FOO].value"
	if r["fields"] != want {
		t.Errorf("fields = %q, want %q", r["fields"], want)
	}
	if r["tool"] != "kubectl" || r["reason"] != "KubectlManualEdit" || r["age"] != "45s" {
		t.Errorf("kubectl-patch must be recognized as the kubectl tool with age 45s, got %v", r)
	}
}

// TestDriftMultiManagerObject: one finding per foreign manager on the
// same object; an entry without a recorded time omits the age detail.
func TestDriftMultiManagerObject(t *testing.T) {
	cmd := stab.DriftCommand(testDeps(
		deployment("prod", "api",
			entry("argocd-controller", metav1.ManagedFieldsOperationApply, ptr(ago(time.Hour)), gitopsSpec),
			entry("kubectl-edit", metav1.ManagedFieldsOperationUpdate, ptr(ago(time.Minute)), kubectlSpec),
			entry("legacy-script", metav1.ManagedFieldsOperationUpdate, nil, termGraceSpec),
		),
	))
	recs := findingLines(t, checktest.Run(t, cmd).Stdout)
	if len(recs) != 2 {
		t.Fatalf("want one finding per foreign manager (2), got %v", recs)
	}
	byManager := map[string]map[string]string{}
	for _, r := range recs {
		byManager[r["manager"]] = r
	}
	if _, hasAge := byManager["legacy-script"]["age"]; hasAge {
		t.Errorf("entry without a time must omit age: %v", byManager["legacy-script"])
	}
	if byManager["kubectl-edit"]["age"] != "1m" {
		t.Errorf("kubectl-edit age = %q, want 1m", byManager["kubectl-edit"]["age"])
	}
}

// TestDriftFieldCap: rendered paths cap at 8 with an explicit tail;
// field_count stays the full total.
func TestDriftFieldCap(t *testing.T) {
	cmd := stab.DriftCommand(testDeps(
		deployment("prod", "api",
			// argocd owns 6+3+2=11 leaves so the 10-leaf kubectl entry
			// stays in the minority.
			entry("argocd-controller", metav1.ManagedFieldsOperationApply, ptr(ago(time.Hour)), gitopsSpec),
			entry("argocd-controller", metav1.ManagedFieldsOperationApply, ptr(ago(time.Hour)), envSpec),
			entry("argocd-controller", metav1.ManagedFieldsOperationApply, ptr(ago(time.Hour)), `{"f:spec":{"f:minReadySeconds":{},"f:paused":{}}}`),
			entry("kubectl-client-side-apply", metav1.ManagedFieldsOperationUpdate, ptr(ago(time.Hour)), tenFieldSpec),
		),
	))
	recs := findingLines(t, checktest.Run(t, cmd).Stdout)
	if len(recs) != 1 {
		t.Fatalf("want 1 finding, got %v", recs)
	}
	r := recs[0]
	if r["field_count"] != "10" {
		t.Errorf("field_count = %q, want 10", r["field_count"])
	}
	if !strings.HasSuffix(r["fields"], ",+2 more") || strings.Count(r["fields"], ",") != 8 {
		t.Errorf("fields must cap at 8 paths with a +2 more tail, got %q", r["fields"])
	}
}

// TestDriftDetectionNone: a scope owning no spec fields emits no
// findings and says detection=none rather than guessing a manager.
func TestDriftDetectionNone(t *testing.T) {
	for name, deps := range map[string]stab.Deps{
		"empty_cluster": testDeps(),
		"no_managed_fields": testDeps(
			deployment("prod", "api"),
		),
	} {
		t.Run(name, func(t *testing.T) {
			res := checktest.Run(t, stab.DriftCommand(deps))
			if recs := findingLines(t, res.Stdout); len(recs) != 0 {
				t.Fatalf("want no findings, got %v", recs)
			}
			sum := summaryLine(t, res.Stdout)
			if sum["detection"] != "none" {
				t.Errorf("summary = %v, want detection=none", sum)
			}
			if _, ok := sum["manager"]; ok {
				t.Errorf("no manager note when nothing was resolved, got %v", sum)
			}
		})
	}
}

// TestDriftWorkloadScope: --workload narrows the scan (and the
// majority election) to one object; unsupported kinds are usage
// errors.
func TestDriftWorkloadScope(t *testing.T) {
	deps := testDeps(driftMixed()...)
	res := checktest.Run(t, stab.DriftCommand(deps), "--workload=Deployment/prod/api")
	recs := findingLines(t, res.Stdout)
	if len(recs) != 1 || recs[0]["manager"] != "kubectl-edit" {
		t.Fatalf("want only prod/api's kubectl-edit drift, got %v", recs)
	}
	if sum := summaryLine(t, res.Stdout); sum["scanned"] != "1" {
		t.Errorf("summary = %v, want scanned=1", sum)
	}

	res = checktest.Run(t, stab.DriftCommand(deps), "--workload=Job/prod/api")
	if res.Code != emit.ExitUsage || !strings.Contains(res.Stderr, "Deployment|StatefulSet|DaemonSet") {
		t.Errorf("unsupported kind: exit %d stderr %q, want usage error naming the kinds", res.Code, res.Stderr)
	}

	res = checktest.Run(t, stab.DriftCommand(deps), "--workload=Deployment/prod/api", "--namespace=other")
	if res.Code != emit.ExitUsage {
		t.Errorf("contradictory namespace: exit %d, want %d", res.Code, emit.ExitUsage)
	}
}

func TestDriftContract(t *testing.T) {
	deps := testDeps(driftMixed()...)
	checktest.VerifyContract(t, stab.DriftCommand(deps))
	checktest.VerifyContract(t, stab.DriftCommand(deps), "--manager=flux-controller")
	checktest.VerifyContract(t, stab.DriftCommand(deps), "--workload=Deployment/prod/worker")
	checktest.VerifyContract(t, stab.DriftCommand(testDeps()))
}

func TestDriftGolden(t *testing.T) {
	res := checktest.Run(t, stab.DriftCommand(testDeps(driftMixed()...)))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checkGolden(t, "drift-mixed.golden", res.Stdout)
}
