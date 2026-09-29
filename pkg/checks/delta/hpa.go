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

// The hpa class: an autoscaler failing to autoscale RIGHT NOW, read
// from the HPA controller's own status conditions.
//
// # Why this is not audit.hpa_cannot_scale
//
// `audit workloads` makes the posture claim: the spec alone makes the
// HPA incapable of scaling (min==max, a dangling scaleTargetRef, a
// utilization target over a template with no requests), whether or
// not anything is wrong yet. This class makes the incident claim: the
// controller reports it cannot scale, which self-clears when the
// metrics pipeline recovers.
//
// Two of the posture defects also surface here, because the
// controller trips over them on every reconcile. Those findings carry
// audit_reason naming the structural claim, so the pair reads as one
// problem, its symptom and its cause, rather than two. The finding is
// still made: `scan` leaves audit out by default, and a live failure
// must not depend on an opt-in group to be seen.

import (
	"fmt"
	"strings"

	"github.com/go-steer/k8s-lookout/pkg/emit"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
)

// reasonScalingDisabled is the ScalingActive=False reason for a
// target scaled to zero replicas: deliberate, so not a failure.
const reasonScalingDisabled = "ScalingDisabled"

// hpaObjectClass is the subject of every finding here. The reason is
// the controller's own condition reason (FailedGetResourceMetric,
// FailedGetScale, …), so the fingerprint matches the Warning event
// the controller emits under the same reason.
const hpaObjectClass = "HorizontalPodAutoscaler"

// The audit.hpa_cannot_scale reasons whose symptom the controller's
// condition message spells out.
const (
	auditTargetMissing   = "HPATargetMissing"
	auditMissingRequests = "HPATargetMissingRequests"
)

// checkHPAs derives hpa.scale_failed and hpa.scaling_inactive, one
// finding per HPA. AbleToScale is judged first: when the controller
// cannot even read the target's scale it stops before computing
// metrics, so a stale ScalingActive beside it says nothing new.
func (s *scanner) checkHPAs(hpas []autoscalingv2.HorizontalPodAutoscaler) {
	for i := range hpas {
		h := &hpas[i]
		kind, c := "hpa.scale_failed", hpaCondition(h, autoscalingv2.AbleToScale)
		if c == nil || c.Status != corev1.ConditionFalse {
			kind, c = "hpa.scaling_inactive", hpaCondition(h, autoscalingv2.ScalingActive)
			if c == nil || c.Status != corev1.ConditionFalse || c.Reason == reasonScalingDisabled {
				continue
			}
		}
		// The controller rewrites the condition every sync, but
		// lastTransitionTime moves only when the status flips, so it
		// dates the failure. A zero time cannot be aged and is not
		// judged; a metrics-server blip shorter than the grace is not
		// an incident.
		if c.LastTransitionTime.IsZero() || s.now.Sub(c.LastTransitionTime.Time) < s.th.hpaGrace {
			continue
		}
		reason := c.Reason
		if reason == "" {
			reason = string(c.Type) + "False"
		}
		details := []emit.Field{
			{Key: "condition", Value: string(c.Type) + "=False"},
			{Key: "scale_target", Value: h.Spec.ScaleTargetRef.Kind + "/" + h.Spec.ScaleTargetRef.Name},
			{Key: "replicas", Value: itoa32(h.Status.CurrentReplicas)},
			{Key: "age", Value: s.age(c.LastTransitionTime.Time)},
		}
		if ar := structuralCause(c); ar != "" {
			details = append(details, emit.Field{Key: "audit_reason", Value: ar})
		}
		s.add(emit.Finding{
			Kind:         kind,
			Severity:     emit.SeverityWarning,
			Namespace:    h.Namespace,
			KindOfObject: hpaObjectClass,
			Name:         h.Name,
			Reason:       reason,
			Message:      fmt.Sprintf("replicas held at %d: %s", h.Status.CurrentReplicas, c.Message),
			Details:      details,
		})
	}
}

// structuralCause names the audit.hpa_cannot_scale reason behind a
// condition when the controller's message identifies it. The markers
// are the controller's fixed error text: a scale subresource GET that
// returned NotFound, and the replica calculator's "missing request
// for <resource> in container …". Anything else is a runtime failure
// with no posture counterpart.
func structuralCause(c *autoscalingv2.HorizontalPodAutoscalerCondition) string {
	switch {
	case c.Reason == "FailedGetScale" && strings.Contains(c.Message, "not found"):
		return auditTargetMissing
	case strings.HasPrefix(c.Reason, "FailedGet") && strings.Contains(c.Message, "missing request for"):
		return auditMissingRequests
	}
	return ""
}

func hpaCondition(h *autoscalingv2.HorizontalPodAutoscaler, t autoscalingv2.HorizontalPodAutoscalerConditionType) *autoscalingv2.HorizontalPodAutoscalerCondition {
	for i := range h.Status.Conditions {
		if h.Status.Conditions[i].Type == t {
			return &h.Status.Conditions[i]
		}
	}
	return nil
}
