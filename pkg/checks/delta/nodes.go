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

// The nodes class: NotReady, kubelet pressure, NPD-style custom
// conditions (kernel-sentry), cordons holding pods, and reclaim
// countdowns visible on the node object itself (spot-countdown).
// Detection is portable — only taints/conditions on the Node are
// read; no cloud API calls.

import (
	"github.com/go-steer/k8s-lookout/pkg/emit"

	corev1 "k8s.io/api/core/v1"
)

// pressureConditions are the kubelet's resource-pressure signals.
var pressureConditions = map[corev1.NodeConditionType]bool{
	corev1.NodeMemoryPressure: true,
	corev1.NodeDiskPressure:   true,
	corev1.NodePIDPressure:    true,
}

// criticalCustomConditions are the non-standard (typically
// node-problem-detector) conditions that mean the node cannot be
// trusted at all when True. Everything else non-standard and True
// is a warning: we cannot know an arbitrary NPD condition's
// semantics, only that it is abnormal.
var criticalCustomConditions = map[corev1.NodeConditionType]bool{
	"KernelDeadlock":              true,
	"ReadonlyFilesystem":          true,
	corev1.NodeNetworkUnavailable: true, // standard type, custom-critical handling
}

// reclaimTaints are the taint keys announcing that the node is
// going away, with severity and a machine-matchable reason. The GKE
// key is the portable on-node trace of a spot/preemptible reclaim;
// the two cluster-autoscaler keys are upstream.
var reclaimTaints = map[string]struct {
	severity string
	reason   string
}{
	"cloud.google.com/impending-node-termination": {emit.SeverityCritical, "PreemptionImminent"},
	"ToBeDeletedByClusterAutoscaler":              {emit.SeverityWarning, "AutoscalerDraining"},
	"DeletionCandidateOfClusterAutoscaler":        {emit.SeverityInfo, "AutoscalerCandidate"},
}

// checkNodes derives the node.* findings. pods is the scope's pod
// list, used to count what a cordoned node is still holding.
func (s *scanner) checkNodes(nodes []corev1.Node, pods []corev1.Pod) {
	occupancy := map[string]int{}
	for i := range pods {
		p := &pods[i]
		if p.Spec.NodeName == "" || ownedBy(p.OwnerReferences, "DaemonSet") {
			continue // DaemonSet pods never block a drain
		}
		if p.Status.Phase == corev1.PodRunning || p.Status.Phase == corev1.PodPending {
			occupancy[p.Spec.NodeName]++
		}
	}

	for i := range nodes {
		s.checkNode(&nodes[i], occupancy[nodes[i].Name])
	}
}

func (s *scanner) checkNode(node *corev1.Node, podCount int) {
	base := emit.Finding{KindOfObject: "Node", Name: node.Name}

	for _, c := range node.Status.Conditions {
		switch {
		case c.Type == corev1.NodeReady:
			if c.Status != corev1.ConditionTrue {
				f := base
				f.Kind = "node.notready"
				f.Severity = emit.SeverityCritical
				f.Reason = conditionReason(c, "NodeNotReady")
				f.Message = c.Message
				f.Details = []emit.Field{{Key: "age", Value: s.age(c.LastTransitionTime.Time)}}
				s.add(f)
			}
		case pressureConditions[c.Type]:
			if c.Status == corev1.ConditionTrue {
				f := base
				f.Kind = "node.pressure"
				f.Severity = emit.SeverityCritical
				f.Reason = conditionReason(c, string(c.Type))
				f.Message = c.Message
				f.Details = []emit.Field{{Key: "condition", Value: string(c.Type)}}
				s.add(f)
			}
		default:
			// Anything else True is abnormal by convention:
			// NPD and its cousins publish problem conditions
			// with Status=True (KernelDeadlock,
			// FrequentKubeletRestart, …).
			if c.Status == corev1.ConditionTrue {
				f := base
				f.Kind = "node.condition"
				f.Severity = emit.SeverityWarning
				if criticalCustomConditions[c.Type] {
					f.Severity = emit.SeverityCritical
				}
				f.Reason = conditionReason(c, string(c.Type))
				f.Message = c.Message
				f.Details = []emit.Field{{Key: "condition", Value: string(c.Type)}}
				s.add(f)
			}
		}
	}

	// A cordon with nothing non-DaemonSet on it is a nearly-done
	// drain — nominal. A cordon holding pods is a stuck drain or a
	// forgotten maintenance step.
	if node.Spec.Unschedulable && podCount > 0 {
		f := base
		f.Kind = "node.cordoned"
		f.Severity = emit.SeverityWarning
		f.Reason = "Cordoned"
		f.Details = []emit.Field{{Key: "pods", Value: itoa(podCount)}}
		s.add(f)
	}

	for _, t := range node.Spec.Taints {
		if r, ok := reclaimTaints[t.Key]; ok {
			f := base
			f.Kind = "node.preempt"
			f.Severity = r.severity
			f.Reason = r.reason
			f.Details = []emit.Field{
				{Key: "taint", Value: t.Key},
				{Key: "pods", Value: itoa(podCount)},
			}
			s.add(f)
		}
	}
}

func conditionReason(c corev1.NodeCondition, fallback string) string {
	if c.Reason != "" {
		return c.Reason
	}
	return fallback
}
