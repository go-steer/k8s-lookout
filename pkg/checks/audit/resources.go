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

package audit

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

const (
	kindNoResourceRequests = "audit.no_resource_requests"
	kindNoResourceLimits   = "audit.no_resource_limits"
)

const (
	reasonNoCPURequest    = "NoCPURequest"
	reasonNoMemoryRequest = "NoMemoryRequest"
	reasonNoMemoryLimit   = "NoMemoryLimit"
	reasonNoCPULimit      = "NoCPULimit"
)

type namespaceResourceDefaults struct {
	cpuRequest    bool
	memoryRequest bool
	cpuLimit      bool
	memoryLimit   bool
}

func computeNamespaceResourceDefaults(lrs []*corev1.LimitRange, ns string) namespaceResourceDefaults {
	var d namespaceResourceDefaults
	for _, lr := range lrs {
		if lr.Namespace != ns {
			continue
		}
		for _, item := range lr.Spec.Limits {
			if item.Type != corev1.LimitTypeContainer {
				continue
			}
			if hasResourceQty(item.Default, corev1.ResourceCPU) {
				d.cpuLimit = true
				d.cpuRequest = true
			}
			if hasResourceQty(item.DefaultRequest, corev1.ResourceCPU) {
				d.cpuRequest = true
			}
			if hasResourceQty(item.Default, corev1.ResourceMemory) {
				d.memoryLimit = true
				d.memoryRequest = true
			}
			if hasResourceQty(item.DefaultRequest, corev1.ResourceMemory) {
				d.memoryRequest = true
			}
		}
	}
	return d
}

func hasResourceQty(list corev1.ResourceList, name corev1.ResourceName) bool {
	if list == nil {
		return false
	}
	q, ok := list[name]
	return ok && !q.IsZero()
}

// resourceFindings judges template-level CPU and memory requests and limits
// (L-03).
//
// Kubernetes defaults an unset container request from its limit in that
// dimension, and a namespace LimitRange with a Container defaultRequest or
// default limit supplies the missing dimension at pod admission. Both
// mechanisms satisfy the check for that dimension.
//
// Limits carry lookout's documented CPU/memory asymmetry: missing memory
// limits are a warning (memory has no throttle; a leak OOMs the node), while
// missing CPU limits are info (CPU is compressible and CFS limits can introduce
// tail-latency throttling).
func (ix *workloadIndex) resourceFindings(w workload) []emit.Finding {
	if ix.limitRangesRefused != nil {
		return nil
	}
	cs := w.template.Spec.Containers
	if len(cs) == 0 {
		return nil
	}
	nsDef := computeNamespaceResourceDefaults(ix.limitRanges, w.namespace)

	var (
		noCPUReq, noMemReq []string
		noMemLim, noCPULim []string
	)
	for _, c := range cs {
		hasCPULim := hasResourceQty(c.Resources.Limits, corev1.ResourceCPU)
		hasMemLim := hasResourceQty(c.Resources.Limits, corev1.ResourceMemory)
		hasCPUReq := hasResourceQty(c.Resources.Requests, corev1.ResourceCPU) || hasCPULim
		hasMemReq := hasResourceQty(c.Resources.Requests, corev1.ResourceMemory) || hasMemLim

		if !nsDef.cpuRequest && !hasCPUReq {
			noCPUReq = append(noCPUReq, c.Name)
		}
		if !nsDef.memoryRequest && !hasMemReq {
			noMemReq = append(noMemReq, c.Name)
		}
		if !nsDef.memoryLimit && !hasMemLim {
			noMemLim = append(noMemLim, c.Name)
		}
		if !nsDef.cpuLimit && !hasCPULim {
			noCPULim = append(noCPULim, c.Name)
		}
	}

	var out []emit.Finding
	total := len(cs)
	if len(noCPUReq) > 0 {
		out = append(out, resourceFinding(
			kindNoResourceRequests, emit.SeverityWarning, reasonNoCPURequest, "cpu",
			"the scheduler places the pod with zero reserved CPU and no namespace LimitRange supplies a default",
			noCPUReq, total,
		))
	}
	if len(noMemReq) > 0 {
		out = append(out, resourceFinding(
			kindNoResourceRequests, emit.SeverityWarning, reasonNoMemoryRequest, "memory",
			"the scheduler places the pod with zero reserved memory and kubelet eviction ranks it first under node pressure",
			noMemReq, total,
		))
	}
	if len(noMemLim) > 0 {
		out = append(out, resourceFinding(
			kindNoResourceLimits, emit.SeverityWarning, reasonNoMemoryLimit, "memory",
			"memory is incompressible; an unbounded leak grows until the node OOM-kills pods",
			noMemLim, total,
		))
	}
	if len(noCPULim) > 0 {
		out = append(out, resourceFinding(
			kindNoResourceLimits, emit.SeverityInfo, reasonNoCPULimit, "cpu",
			"no upper bound on CPU burst (often intentional to avoid CFS throttling, recorded as info)",
			noCPULim, total,
		))
	}
	return out
}

func resourceFinding(kind, severity, reason, resourceName, consequence string, missing []string, total int) emit.Finding {
	return emit.Finding{
		Kind:     kind,
		Severity: severity,
		Reason:   reason,
		Message: fmt.Sprintf("%d of %d %s missing %s %s: %s",
			len(missing), total, plural(total, "container"), resourceName, resourceClaimNoun(kind), consequence),
		Details: []emit.Field{
			{Key: "containers", Value: itoa(len(missing))},
			{Key: "container_names", Value: cappedList(missing)},
			{Key: "total_containers", Value: itoa(total)},
			{Key: "resource", Value: resourceName},
		},
	}
}

func resourceClaimNoun(kind string) string {
	if kind == kindNoResourceLimits {
		return "limit"
	}
	return "request"
}
