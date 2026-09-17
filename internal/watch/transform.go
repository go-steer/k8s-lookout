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
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

// Object trimming for the shared informer factory.
//
// sharedTransform is attached to the one shared factory in wiring.go, so these
// functions run on every Pod and Node entering the process (§6.1). The gate the
// maintainer decision put on that — "source on, transform off" until the
// preserved-field registry in transform_registry.go exists and its guard test
// passes — is satisfied; the registry is the standing control, not a one-time
// review.
//
// Read transform_registry.go before editing anything here. A transform on a
// shared factory mutates objects every other consumer sees, and a nil slice is
// indistinguishable from an empty one downstream — so a field stripped here
// does not fail loudly in the source that needed it, it just makes that source
// quietly stop finding things. The registry names every field this code touches
// and every field it deliberately does not, with the reader that requires it.

// sharedTransform is the single cache.TransformFunc the shared factory applies
// to every informer it serves. A factory takes one transform for all of them,
// so the dispatch has to happen here, and anything that is not a Pod or a Node
// must pass through untouched — the factory also serves Deployments,
// ReplicaSets, Events, HPAs, Ingresses and Services, and the registry has no
// opinion about those.
//
// Two contract points from client-go, both load-bearing:
//
//   - It must be IDEMPOTENT. Objects already in the cache can be handed back to
//     Replace(), and a second pass over an object other goroutines are reading
//     must not change it (delta_fifo.go:501-506). trimPod and trimNode are
//     idempotent by construction — they assign zero values and filter to a fixed
//     key set, never append or accumulate — and TestSharedTransform_IsIdempotent
//     pins that.
//   - It never sees a DeletedFinalStateUnknown tombstone, and never runs on a
//     Sync: DeltaFIFO skips the transformer for both, because in each case the
//     object has already been through it (delta_fifo.go:507-516). The type
//     switch would pass a tombstone through anyway, which is the right answer if
//     that ever changes.
//
// It is also on the hot path for every watch event in the process, so it stays
// a type switch and two field-assignment passes. No allocation beyond what the
// annotation filter needs.
func sharedTransform(obj any) (any, error) {
	switch obj.(type) {
	case *corev1.Pod:
		return trimPod(obj)
	case *corev1.Node:
		return trimNode(obj)
	default:
		return obj, nil
	}
}

// newSharedFactory builds the one shared informer factory, with the transform
// attached.
//
// This exists so that there is exactly one place the factory is constructed.
// The alternative — wiring.go builds its own and the test builds a matching one
// — passes happily on the day someone drops the option from wiring, because the
// test is still constructing a factory that has it. The test calls this.
func newSharedFactory(client kubernetes.Interface) informers.SharedInformerFactory {
	return informers.NewSharedInformerFactoryWithOptions(client, 0,
		informers.WithTransform(sharedTransform))
}

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
