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

package delta

import (
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	msgNoMetrics     = "the HPA was unable to compute the replica count: failed to get cpu utilization: unable to get metrics for resource cpu: no metrics returned from resource metrics API"
	msgMissingReq    = "the HPA was unable to compute the replica count: failed to get cpu utilization: missing request for cpu in container app of Pod web-7d4b9-x2x"
	msgTargetMissing = `the HPA controller was unable to get the target's current scale: deployments/scale.apps "web" not found`
)

// hpaCond is one status condition that transitioned d before testNow.
func hpaCond(t autoscalingv2.HorizontalPodAutoscalerConditionType, status corev1.ConditionStatus, reason, msg string, d time.Duration) autoscalingv2.HorizontalPodAutoscalerCondition {
	return autoscalingv2.HorizontalPodAutoscalerCondition{
		Type: t, Status: status, Reason: reason, Message: msg, LastTransitionTime: ago(d),
	}
}

// hpa builds an autoscaler of Deployment/web holding 3 replicas. With
// no conditions it is the state before the controller's first sync.
func hpa(ns, name string, conds ...autoscalingv2.HorizontalPodAutoscalerCondition) *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, CreationTimestamp: ago(24 * time.Hour)},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"},
			MaxReplicas:    10,
		},
		Status: autoscalingv2.HorizontalPodAutoscalerStatus{CurrentReplicas: 3, DesiredReplicas: 3, Conditions: conds},
	}
}

func healthyHPA(ns, name string) *autoscalingv2.HorizontalPodAutoscaler {
	return hpa(ns, name,
		hpaCond(autoscalingv2.AbleToScale, corev1.ConditionTrue, "ReadyForNewScale", "recommended size matches current size", time.Hour),
		hpaCond(autoscalingv2.ScalingActive, corev1.ConditionTrue, "ValidMetricFound", "the HPA was able to successfully calculate a replica count from cpu resource utilization", time.Hour),
	)
}

func metricsDeadHPA(ns, name, msg string, d time.Duration) *autoscalingv2.HorizontalPodAutoscaler {
	return hpa(ns, name,
		hpaCond(autoscalingv2.AbleToScale, corev1.ConditionTrue, "SucceededGetScale", "the HPA controller was able to get the target's current scale", time.Hour),
		hpaCond(autoscalingv2.ScalingActive, corev1.ConditionFalse, "FailedGetResourceMetric", msg, d),
	)
}

func TestHPAClass(t *testing.T) {
	cmd := testCommand(
		healthyHPA("prod", "ok"),
		metricsDeadHPA("prod", "no-metrics", msgNoMetrics, 20*time.Minute),
		hpa("prod", "no-target",
			hpaCond(autoscalingv2.AbleToScale, corev1.ConditionFalse, "FailedGetScale", msgTargetMissing, time.Hour),
			// Stale from before the target vanished: AbleToScale is
			// judged first and the pair is ONE finding.
			hpaCond(autoscalingv2.ScalingActive, corev1.ConditionFalse, "FailedGetResourceMetric", msgNoMetrics, 2*time.Hour),
		),
		hpa("prod", "bad-selector",
			hpaCond(autoscalingv2.ScalingActive, corev1.ConditionFalse, "InvalidSelector", "the HPA target's scale is missing a selector", time.Hour),
		),
		// Deliberate: the target was scaled to zero.
		hpa("prod", "scaled-to-zero",
			hpaCond(autoscalingv2.ScalingActive, corev1.ConditionFalse, reasonScalingDisabled, "scaling is disabled since the replica count of the target is zero", time.Hour),
		),
		// A metrics-server blip inside the grace.
		metricsDeadHPA("prod", "blip", msgNoMetrics, 2*time.Minute),
		// Never synced: no conditions to judge.
		hpa("prod", "fresh"),
		// Failing since this very sync.
		metricsDeadHPA("prod", "just-now", msgNoMetrics, 0),
	)
	got, scanned := runFindings(t, cmd, "--only=hpa")
	assertFindings(t, got, []finding{
		{"hpa.scaling_inactive", "no-metrics", "warning"},
		{"hpa.scale_failed", "no-target", "warning"},
		{"hpa.scaling_inactive", "bad-selector", "warning"},
	})
	if scanned != 8 {
		t.Errorf("scanned = %d, want 8", scanned)
	}
}

// TestHPAUndatedConditionIsNotJudged: a False condition with no
// transition time cannot be aged, so it is not judged.
func TestHPAUndatedConditionIsNotJudged(t *testing.T) {
	h := metricsDeadHPA("prod", "undated", msgNoMetrics, time.Hour)
	h.Status.Conditions[1].LastTransitionTime = metav1.Time{}
	got, _ := runFindings(t, testCommand(h), "--only=hpa")
	assertFindings(t, got, nil)
}

// TestHPAFindingFields pins the claim: the controller's own reason
// (so the fingerprint is the one its Warning event carries), the
// False condition, the target and the held replica count.
func TestHPAFindingFields(t *testing.T) {
	res := checktest.Run(t, testCommand(metricsDeadHPA("prod", "web", msgNoMetrics, 20*time.Minute)), "--only=hpa")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	rec := parseLogfmtLine(t, strings.SplitN(res.Stdout, "\n", 2)[0])
	want := map[string]string{
		"kind":           "hpa.scaling_inactive",
		"severity":       "warning",
		"namespace":      "prod",
		"kind_of_object": "HorizontalPodAutoscaler",
		"name":           "web",
		"reason":         "FailedGetResourceMetric",
		"condition":      "ScalingActive=False",
		"scale_target":   "Deployment/web",
		"replicas":       "3",
		"age":            "20m0s",
		"fingerprint":    engine.ScanFingerprint("FailedGetResourceMetric", "HorizontalPodAutoscaler", ""),
	}
	for k, v := range want {
		if rec[k] != v {
			t.Errorf("%s = %q, want %q", k, rec[k], v)
		}
	}
	if !strings.HasPrefix(rec["message"], "replicas held at 3: the HPA was unable to compute the replica count") {
		t.Errorf("message = %q, want the held count then the controller's message", rec["message"])
	}
	if _, ok := rec["audit_reason"]; ok {
		t.Errorf("audit_reason = %q on a runtime failure with no posture counterpart", rec["audit_reason"])
	}
}

// TestHPAStructuralCauseNamesTheAuditClaim: the two posture defects
// the controller trips over carry audit_reason, so the incident and
// audit.hpa_cannot_scale read as one problem.
func TestHPAStructuralCauseNamesTheAuditClaim(t *testing.T) {
	for _, tc := range []struct {
		name string
		h    *autoscalingv2.HorizontalPodAutoscaler
		want string
	}{
		{"missing requests", metricsDeadHPA("prod", "h", msgMissingReq, time.Hour), "HPATargetMissingRequests"},
		{"target missing", hpa("prod", "h",
			hpaCond(autoscalingv2.AbleToScale, corev1.ConditionFalse, "FailedGetScale", msgTargetMissing, time.Hour)), "HPATargetMissing"},
		{"scale forbidden", hpa("prod", "h",
			hpaCond(autoscalingv2.AbleToScale, corev1.ConditionFalse, "FailedGetScale", `deployments.apps "web" is forbidden`, time.Hour)), ""},
		{"update failed", hpa("prod", "h",
			hpaCond(autoscalingv2.AbleToScale, corev1.ConditionFalse, "FailedUpdateScale", "the HPA controller was unable to update the target scale: conflict", time.Hour)), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, testCommand(tc.h), "--only=hpa")
			rec := parseLogfmtLine(t, strings.SplitN(res.Stdout, "\n", 2)[0])
			if rec["audit_reason"] != tc.want {
				t.Errorf("audit_reason = %q, want %q", rec["audit_reason"], tc.want)
			}
		})
	}
}

func TestHPAGraceFlag(t *testing.T) {
	cmd := testCommand(metricsDeadHPA("prod", "blip", msgNoMetrics, 2*time.Minute))
	got, _ := runFindings(t, cmd, "--only=hpa", "--hpa-grace=1m")
	assertFindings(t, got, []finding{{"hpa.scaling_inactive", "blip", "warning"}})
	got, _ = runFindings(t, cmd, "--only=hpa", "--hpa-grace=0s")
	assertFindings(t, got, []finding{{"hpa.scaling_inactive", "blip", "warning"}})

	res := checktest.Run(t, testCommand(), "--hpa-grace=-1m")
	if res.Code != emit.ExitUsage || !strings.Contains(res.Stderr, "--hpa-grace must not be negative") {
		t.Errorf("exit = %d stderr = %q, want a usage error", res.Code, res.Stderr)
	}
}

func TestHPANamespaceScope(t *testing.T) {
	cmd := testCommand(
		metricsDeadHPA("prod", "a", msgNoMetrics, time.Hour),
		metricsDeadHPA("dev", "b", msgNoMetrics, time.Hour),
	)
	got, scanned := runFindings(t, cmd, "--only=hpa", "--namespace=prod")
	assertFindings(t, got, []finding{{"hpa.scaling_inactive", "a", "warning"}})
	if scanned != 1 {
		t.Errorf("scanned = %d, want 1", scanned)
	}
}
