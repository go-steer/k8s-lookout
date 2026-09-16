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

package watch

import (
	corev1 "k8s.io/api/core/v1"
)

// Object trimming for the shared informer factory.
//
// These functions are NOT attached to the factory yet. `topology-drift` ships
// default-on, which puts this transform in every deployment, so the maintainer
// decision was explicitly "source on, transform off" until the preserved-field
// registry in transform_registry.go exists and its guard test passes. The
// registry is that gate; wiring these into NewSharedInformerFactory is a Phase 2
// change (docs/leeway-design.md §6.1, §14).
//
// Read transform_registry.go before editing anything here. A transform on a
// shared factory mutates objects every other consumer sees, and a nil slice is
// indistinguishable from an empty one downstream — so a field stripped here
// does not fail loudly in the source that needed it, it just makes that source
// quietly stop finding things. The registry names every field this code touches
// and every field it deliberately does not, with the reader that requires it.

// retainedNodeAnnotations is load-bearing, not cosmetic. GKE records the
// provisioned compute-class priority in the `ccc_priority_index` annotation
// (docs/leeway-design.md §7.7.2), so dropping it here would silently disable
// preference-rank tracking with no error anywhere. Anything §7.7 depends on
// must be listed.
//
// The key is unprefixed and GKE-internal: it is not in any published API, and
// spike S1 measured it carrying non-numeric sentinels (`ccc_no_rule_matching`,
// `ccc_scale_up_anyway`) and being absent for the first 33-44s of a node's life.
// It is retained verbatim; interpreting it is the compute-class source's job.
var retainedNodeAnnotations = map[string]bool{
	"ccc_priority_index": true,
}

// trimPod drops fields no informer-reachable consumer reads, before the object
// enters the shared cache.
//
// The security goal is that secret VALUES never enter process memory or a heap
// dump. That is satisfied by nulling Env[i].Value while preserving Name and
// ValueFrom — the reference, not the resolved secret — which is precisely what
// pkg/graph needs to build its ConfigMap and Secret edges. Container statuses
// stay, because four sources read them.
func trimPod(obj any) (any, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return obj, nil // tombstones and other types pass through untouched
	}
	pod.ManagedFields = nil
	trimContainers(pod.Spec.Containers)
	trimContainers(pod.Spec.InitContainers)

	// Ephemeral containers carry the same EphemeralContainerCommon shape as a
	// regular container, so a debug container started with `kubectl debug
	// --env` puts literal secret values in the cache by exactly the route this
	// transform exists to close. Their Name is preserved: pkg/checks/logs reads
	// it to enumerate the containers it can fetch logs from, and that check is
	// informer-reachable through the enricher.
	for i := range pod.Spec.EphemeralContainers {
		trimContainerCommon(&pod.Spec.EphemeralContainers[i].EphemeralContainerCommon)
	}
	return pod, nil
}

// trimContainers strips each container in place. It takes the slice rather than
// returning one so that callers cannot accidentally drop the result and leave
// the originals untrimmed.
func trimContainers(cs []corev1.Container) {
	for i := range cs {
		c := &cs[i]
		for j := range c.Env {
			c.Env[j].Value = "" // drop literal values; keep Name + ValueFrom
		}
		c.Command, c.Args, c.Lifecycle = nil, nil, nil
		c.ReadinessProbe, c.LivenessProbe, c.StartupProbe = nil, nil, nil
	}
}

// trimContainerCommon applies the same strips to an ephemeral container's
// embedded common fields. Kept separate from trimContainers because
// EphemeralContainerCommon is a distinct type with the same field set, and Go
// will not let one function span both.
func trimContainerCommon(c *corev1.EphemeralContainerCommon) {
	for j := range c.Env {
		c.Env[j].Value = ""
	}
	c.Command, c.Args, c.Lifecycle = nil, nil, nil
	c.ReadinessProbe, c.LivenessProbe, c.StartupProbe = nil, nil, nil
}

// trimNode drops the node-side fields nothing in the repo reads. status.images
// is the single largest line item — 10-40 KiB per node, roughly 2 GiB at 50k
// nodes — and no consumer touches it.
func trimNode(obj any) (any, error) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return obj, nil
	}
	node.ManagedFields = nil
	node.Annotations = filterKeys(node.Annotations, retainedNodeAnnotations)
	node.Status.Images = nil
	node.Status.VolumesInUse, node.Status.VolumesAttached = nil, nil
	node.Status.NodeInfo = corev1.NodeSystemInfo{}
	node.Status.Conditions = retainConditions(node.Status.Conditions, corev1.NodeReady)
	return node, nil
}

// filterKeys returns m reduced to the keys in keep, or nil if none survive.
func filterKeys(m map[string]string, keep map[string]bool) map[string]string {
	if len(m) == 0 {
		return m
	}
	var out map[string]string
	for k, v := range m {
		if !keep[k] {
			continue
		}
		if out == nil {
			out = make(map[string]string, len(keep))
		}
		out[k] = v
	}
	return out
}

// retainConditions returns only the conditions whose type is in keep, in their
// original order.
func retainConditions(cs []corev1.NodeCondition, keep ...corev1.NodeConditionType) []corev1.NodeCondition {
	if len(cs) == 0 {
		return cs
	}
	wanted := make(map[corev1.NodeConditionType]bool, len(keep))
	for _, k := range keep {
		wanted[k] = true
	}
	var out []corev1.NodeCondition
	for i := range cs {
		if wanted[cs[i].Type] {
			out = append(out, cs[i])
		}
	}
	return out
}
