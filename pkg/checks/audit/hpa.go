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
	"strings"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// kindHPACannotScale is the structural-autoscaling posture kind: an
// HPA object exists, and the spec makes it incapable of ever changing
// a replica count.
//
// # Why this does not overlap autoscaling.hpa_pinned
//
// The `autoscaling` sentinel source (pkg/sources/autoscaling) reports
// two HPA faults, hpa_pinned and hpa_metrics_dead, and both are
// SUSTAINED states: they need 10-30 minute windows and episode memory,
// and its package doc says so plainly — invisible to a point-in-time
// scan without history. A one-shot audit cannot reproduce either, so
// there is nothing here to divide.
//
// What it can do is the complement. Every claim below is derived from
// the spec alone, is permanent until someone edits the object, and is
// invisible to the sentinel BY DESIGN — the source deliberately
// declines the min==max case, because such an HPA reports
// ScalingLimited=False and must not fire its pinned predicate. The
// result today is that nothing in the tree reports an autoscaler that
// cannot autoscale. This closes that.
const kindHPACannotScale = "audit.hpa_cannot_scale"

// The three structural defects, each with its own remedy and so its
// own fingerprint class.
const (
	reasonMinEqualsMax   = "HPAMinEqualsMax"
	reasonMissingRequest = "HPATargetMissingRequests"
	reasonTargetMissing  = "HPATargetMissing"
)

// hpaObjectClass is the KindOfObject every finding here carries: the
// subject is the autoscaler, not the workload behind it. An operator
// fixes these by editing the HPA (or the template it points at), and a
// fleet rollup should count them as autoscaler defects.
const hpaObjectClass = "HorizontalPodAutoscaler"

// scalableAPIGroup is the apiVersion prefix of the workload kinds this
// command indexes. A scaleTargetRef into any other group — an Argo
// Rollout, a CRD with a scale subresource — is a target this pass
// never listed, so it makes no claim about it.
const scalableAPIGroup = "apps/"

// hpaFindings judges one HPA. The claims are independent: an HPA can
// be both pinned min==max and pointed at a template with no requests,
// and an operator fixing one has not fixed the other.
func (ix *workloadIndex) hpaFindings(h *autoscalingv2.HorizontalPodAutoscaler) []emit.Finding {
	var out []emit.Finding
	ref := h.Spec.ScaleTargetRef

	if min := hpaMinReplicas(h); min == h.Spec.MaxReplicas {
		out = append(out, emit.Finding{
			Kind:     kindHPACannotScale,
			Severity: emit.SeverityWarning,
			Reason:   reasonMinEqualsMax,
			Message: fmt.Sprintf("minReplicas and maxReplicas are both %d: the autoscaler is wired up but can never change the replica count, and because it reports ScalingLimited=False no sustained-state detector will ever flag it either",
				min),
			Details: []emit.Field{
				{Key: "min_replicas", Value: itoa(int(min))},
				{Key: "max_replicas", Value: itoa(int(h.Spec.MaxReplicas))},
				{Key: "scale_target", Value: scaleTargetString(ref)},
			},
		})
	}

	target, known := ix.scaleTarget(h.Namespace, ref)
	switch {
	case !known:
		// The ref points outside the kinds this pass listed. Silence is
		// the honest answer: "target missing" would be a claim about
		// something never looked for.
	case target == nil:
		out = append(out, emit.Finding{
			Kind:     kindHPACannotScale,
			Severity: emit.SeverityWarning,
			Reason:   reasonTargetMissing,
			Message: fmt.Sprintf("scaleTargetRef names %s, which does not exist in this namespace: the autoscaler has nothing to scale and every reconcile fails",
				scaleTargetString(ref)),
			Details: []emit.Field{{Key: "scale_target", Value: scaleTargetString(ref)}},
		})
	default:
		if f, ok := unmeasurableFinding(h, *target); ok {
			out = append(out, f)
		}
	}

	for i := range out {
		out[i].Namespace = h.Namespace
		out[i].KindOfObject = hpaObjectClass
		out[i].Name = h.Name
		out[i].Fingerprint = engine.PostureFingerprint(out[i].Kind, out[i].Reason, hpaObjectClass)
	}
	return out
}

// hpaMinReplicas resolves spec.minReplicas the way the API server
// does: a nil value means 1.
func hpaMinReplicas(h *autoscalingv2.HorizontalPodAutoscaler) int32 {
	if h.Spec.MinReplicas == nil {
		return 1
	}
	return *h.Spec.MinReplicas
}

func scaleTargetString(ref autoscalingv2.CrossVersionObjectReference) string {
	return ref.Kind + "/" + ref.Name
}

// scaleTarget resolves a scaleTargetRef against the indexed workloads.
// The second result says whether the ref is even a kind this pass
// listed; only then is a nil workload evidence of a dangling ref.
func (ix *workloadIndex) scaleTarget(ns string, ref autoscalingv2.CrossVersionObjectReference) (*workload, bool) {
	if ref.APIVersion != "" && !strings.HasPrefix(ref.APIVersion, scalableAPIGroup) {
		return nil, false
	}
	if _, ok := canonicalWorkloadKinds[strings.ToLower(ref.Kind)]; !ok {
		return nil, false
	}
	for i := range ix.workloads {
		w := &ix.workloads[i]
		if w.namespace == ns && w.kind == ref.Kind && w.name == ref.Name {
			return w, true
		}
	}
	return nil, true
}

// targets reports whether this HPA scales the named workload, so that
// --workload scope shows the autoscaler attached to it.
func hpaTargets(h *autoscalingv2.HorizontalPodAutoscaler, kind, ns, name string) bool {
	ref := h.Spec.ScaleTargetRef
	return h.Namespace == ns && ref.Kind == kind && ref.Name == name
}

// utilTarget is one resource the HPA reads as a PERCENTAGE OF the
// container's request. container is the single container it applies
// to, or "" for the pod-wide form, which sums across all of them.
type utilTarget struct {
	resource  corev1.ResourceName
	container string
}

// utilizationTargets returns the metrics whose arithmetic divides by a
// request. AverageValue and Value targets are absolute and need no
// request, so they are not here; External and Object metrics never
// touch the pod spec at all.
func utilizationTargets(h *autoscalingv2.HorizontalPodAutoscaler) []utilTarget {
	// An HPA with no metrics is not a no-op: the API server defaults it
	// to 80% CPU utilization, which is also what every autoscaling/v1
	// object converts to. Judging it as CPU-utilization is what the
	// cluster will actually do.
	if len(h.Spec.Metrics) == 0 {
		return []utilTarget{{resource: corev1.ResourceCPU}}
	}
	var out []utilTarget
	for _, m := range h.Spec.Metrics {
		switch m.Type {
		case autoscalingv2.ResourceMetricSourceType:
			if m.Resource != nil && m.Resource.Target.Type == autoscalingv2.UtilizationMetricType {
				out = append(out, utilTarget{resource: m.Resource.Name})
			}
		case autoscalingv2.ContainerResourceMetricSourceType:
			if m.ContainerResource != nil && m.ContainerResource.Target.Type == autoscalingv2.UtilizationMetricType {
				out = append(out, utilTarget{
					resource:  m.ContainerResource.Name,
					container: m.ContainerResource.Container,
				})
			}
		}
	}
	return out
}

// unmeasurableFinding reports the HPA whose utilization target cannot
// be computed because the pod template carries no matching request.
//
// One finding per HPA rather than one per metric: the subject is the
// autoscaler, it is either able to compute a replica count or it is
// not, and two rows saying "cpu" and "memory" about the same broken
// object would share a fingerprint anyway.
func unmeasurableFinding(h *autoscalingv2.HorizontalPodAutoscaler, w workload) (emit.Finding, bool) {
	cs := w.template.Spec.Containers
	if len(cs) == 0 {
		return emit.Finding{}, false
	}
	var resources []string
	seenResource := map[corev1.ResourceName]bool{}
	var missing []string
	seenContainer := map[string]bool{}

	for _, ut := range utilizationTargets(h) {
		short := containersMissingRequest(cs, ut)
		if len(short) == 0 {
			continue
		}
		if !seenResource[ut.resource] {
			seenResource[ut.resource] = true
			resources = append(resources, string(ut.resource))
		}
		for _, name := range short {
			if !seenContainer[name] {
				seenContainer[name] = true
				missing = append(missing, name)
			}
		}
	}
	if len(missing) == 0 {
		return emit.Finding{}, false
	}

	list := strings.Join(resources, ",")
	return emit.Finding{
		Kind:     kindHPACannotScale,
		Severity: emit.SeverityWarning,
		Reason:   reasonMissingRequest,
		Message: fmt.Sprintf("targets %s utilization, but %d of %d %s in %s carry no matching request: utilization is a percentage OF the request, so the autoscaler can never compute a replica count",
			list, len(missing), len(cs), plural(len(cs), "container"), scaleTargetString(h.Spec.ScaleTargetRef)),
		Details: []emit.Field{
			{Key: "metric", Value: list},
			{Key: "containers", Value: itoa(len(missing))},
			{Key: "container_names", Value: cappedList(missing)},
			{Key: "total_containers", Value: itoa(len(cs))},
			{Key: "scale_target", Value: scaleTargetString(h.Spec.ScaleTargetRef)},
		},
	}, true
}

// containersMissingRequest returns the containers that leave the
// utilization target's arithmetic undefined, in template order.
//
// The pod-wide form needs the request on EVERY container: the
// controller sums requests across the pod and fails the whole metric
// if one is absent, so a single bare sidecar disables autoscaling for
// the workload. The per-container form needs it only on the container
// it names — and a name that matches nothing in the template is
// reported too, since that HPA cannot compute anything either.
func containersMissingRequest(cs []corev1.Container, ut utilTarget) []string {
	var missing []string
	found := false
	for _, c := range cs {
		if ut.container != "" && c.Name != ut.container {
			continue
		}
		found = true
		if q, ok := c.Resources.Requests[ut.resource]; !ok || q.IsZero() {
			missing = append(missing, c.Name)
		}
	}
	if ut.container != "" && !found {
		return []string{ut.container}
	}
	return missing
}
