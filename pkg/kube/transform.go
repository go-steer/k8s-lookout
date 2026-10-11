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

package kube

import (
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

// Object trimming for shared informer factories.
//
// Transform is attached to every factory this repo constructs, so these
// functions run on every Pod, Node and workload controller entering any process
// built from it (§6.1). The gate the maintainer decision put on that — "source
// on, transform off" until the preserved-field registry in transform_registry.go
// exists and its guard test passes — is satisfied; the registry is the standing
// control, not a one-time review.
//
// # Why this lives in pkg/kube
//
// It began in internal/watch, next to the sentinel's runner, which is where
// the only factory used to be built. It is here now because it is not the
// sentinel's property: cmd/leeway runs a single source outside the runner and
// needs the identical transform, and §2.4 discipline 4 says that binary may not
// import internal/watch. A transform reachable from only one of the two
// binaries is how the standalone one came to cache resolved secret env values.
// pkg/kube is what both may import, so this is where the one definition goes.
//
// Read transform_registry.go before editing anything here. A transform on a
// shared factory mutates objects every other consumer sees, and a nil slice is
// indistinguishable from an empty one downstream — so a field stripped here
// does not fail loudly in the source that needed it, it just makes that source
// quietly stop finding things. The registry names every field this code touches
// and every field it deliberately does not, with the reader that requires it.

// Transform is the single cache.TransformFunc every factory in this repo
// applies to every informer it serves. A factory takes one transform for all of
// them, so the dispatch has to happen here, and anything the registry has no
// opinion about passes through untouched.
//
// Two contract points from client-go, both load-bearing:
//
//   - It must be IDEMPOTENT. Objects already in the cache can be handed back to
//     Replace(), and a second pass over an object other goroutines are reading
//     must not change it (delta_fifo.go:501-506). Every trim function is
//     idempotent by construction — assigning zero values and filtering to fixed
//     key sets without mutating retained maps — and TestTransform_IsIdempotent
//     pins that.
//   - It never sees a DeletedFinalStateUnknown tombstone, and never runs on a
//     Sync: DeltaFIFO skips the transformer for both, because in each case the
//     object has already been through it (delta_fifo.go:507-516). The type
//     switch would pass a tombstone through anyway, which is the right answer if
//     that ever changes.
//
// It is also on the hot path for every watch event in the process, so it stays
// a type switch and field-assignment passes. No allocation beyond what an
// annotation filter actually changes.
func Transform(obj any) (any, error) {
	switch obj.(type) {
	case *corev1.Pod:
		return trimPod(obj)
	case *corev1.Node:
		return trimNode(obj)
	case *appsv1.Deployment:
		return trimDeployment(obj)
	case *appsv1.ReplicaSet:
		return trimReplicaSet(obj)
	case *appsv1.StatefulSet:
		return trimStatefulSet(obj)
	case *appsv1.DaemonSet:
		return trimDaemonSet(obj)
	case *batchv1.Job:
		return trimJob(obj)
	case *batchv1.CronJob:
		return trimCronJob(obj)
	default:
		return obj, nil
	}
}

// NewTransformingFactory builds an unfiltered shared informer factory with the
// §6.1 transform attached.
//
// Every factory this repo constructs comes from here or from the sentinel
// runner's namespace-filtered sibling, which attaches the same Transform. The
// reason that matters is §6.1's security property rather than its memory one:
// resolved secret env values are not supposed to be in the process at all, and
// a factory built without the transform put them there.
//
// What this guarantees is the part that must not diverge: there is one
// definition of what enters a cache, and no caller can construct a factory that
// skips it without writing informers.NewSharedInformerFactory themselves.
func NewTransformingFactory(client kubernetes.Interface) informers.SharedInformerFactory {
	return informers.NewSharedInformerFactoryWithOptions(client, 0,
		informers.WithTransform(Transform))
}

// strippedAnnotations lists full-manifest applied-state annotations that duplicate
// raw YAML/JSON manifests (including literal secret env values and args) without
// being read by any informer consumer (#628). Non-listed annotations (such as
// rollout revision and custom metadata) survive intact.
var strippedAnnotations = map[string]bool{
	"kubectl.kubernetes.io/last-applied-configuration":       true,
	"kapp.k14s.io/original":                                  true,
	"field.cattle.io/publicEndpoints":                        true,
	"fluxcd.io/last-applied-configuration":                   true,
	"kustomize.toolkit.fluxcd.io/last-applied-configuration": true,
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
	pod.Annotations = stripAppliedAnnotations(pod.Annotations)
	trimPodSpec(&pod.Spec)
	return pod, nil
}

func trimDeployment(obj any) (any, error) {
	d, ok := obj.(*appsv1.Deployment)
	if !ok {
		return obj, nil
	}
	d.ManagedFields = nil
	d.Annotations = stripAppliedAnnotations(d.Annotations)
	trimPodTemplateSpec(&d.Spec.Template)
	return d, nil
}

func trimReplicaSet(obj any) (any, error) {
	rs, ok := obj.(*appsv1.ReplicaSet)
	if !ok {
		return obj, nil
	}
	rs.ManagedFields = nil
	rs.Annotations = stripAppliedAnnotations(rs.Annotations)
	trimPodTemplateSpec(&rs.Spec.Template)
	return rs, nil
}

func trimStatefulSet(obj any) (any, error) {
	sts, ok := obj.(*appsv1.StatefulSet)
	if !ok {
		return obj, nil
	}
	sts.ManagedFields = nil
	sts.Annotations = stripAppliedAnnotations(sts.Annotations)
	trimPodTemplateSpec(&sts.Spec.Template)
	return sts, nil
}

func trimDaemonSet(obj any) (any, error) {
	ds, ok := obj.(*appsv1.DaemonSet)
	if !ok {
		return obj, nil
	}
	ds.ManagedFields = nil
	ds.Annotations = stripAppliedAnnotations(ds.Annotations)
	trimPodTemplateSpec(&ds.Spec.Template)
	return ds, nil
}

func trimJob(obj any) (any, error) {
	j, ok := obj.(*batchv1.Job)
	if !ok {
		return obj, nil
	}
	j.ManagedFields = nil
	j.Annotations = stripAppliedAnnotations(j.Annotations)
	trimPodTemplateSpec(&j.Spec.Template)
	return j, nil
}

func trimCronJob(obj any) (any, error) {
	cj, ok := obj.(*batchv1.CronJob)
	if !ok {
		return obj, nil
	}
	cj.ManagedFields = nil
	cj.Annotations = stripAppliedAnnotations(cj.Annotations)
	trimPodTemplateSpec(&cj.Spec.JobTemplate.Spec.Template)
	return cj, nil
}

func trimPodTemplateSpec(tpl *corev1.PodTemplateSpec) {
	tpl.Annotations = stripAppliedAnnotations(tpl.Annotations)
	trimPodSpec(&tpl.Spec)
}

func trimPodSpec(spec *corev1.PodSpec) {
	trimContainers(spec.Containers)
	trimContainers(spec.InitContainers)

	// Ephemeral containers carry the same EphemeralContainerCommon shape as a
	// regular container, so a debug container started with `kubectl debug
	// --env` puts literal secret values in the cache by exactly the route this
	// transform exists to close. Their Name is preserved: pkg/checks/logs reads
	// it to enumerate the containers it can fetch logs from, and that check is
	// informer-reachable through the enricher.
	for i := range spec.EphemeralContainers {
		trimContainerCommon(&spec.EphemeralContainers[i].EphemeralContainerCommon)
	}
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
	node.Status.Conditions = retainConditions(node.Status.Conditions,
		corev1.NodeReady,
		corev1.NodeMemoryPressure,
		corev1.NodeDiskPressure,
		corev1.NodePIDPressure,
	)
	return node, nil
}

// stripAppliedAnnotations removes full-manifest applied-state keys from m
// without mutating m in place. If none of strippedAnnotations are present, m is
// returned directly without allocating.
func stripAppliedAnnotations(m map[string]string) map[string]string {
	if len(m) == 0 {
		return m
	}
	drop := 0
	for k := range m {
		if strippedAnnotations[k] {
			drop++
		}
	}
	if drop == 0 {
		return m
	}
	if drop == len(m) {
		return nil
	}
	out := make(map[string]string, len(m)-drop)
	for k, v := range m {
		if !strippedAnnotations[k] {
			out[k] = v
		}
	}
	return out
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
