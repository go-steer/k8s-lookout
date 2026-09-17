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
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// sharedFactories are the informer factories one runner owns.
//
// Without --exclude-namespace both fields hold THE SAME factory, which is the
// shape the process has always had: one factory, one informer per object type,
// the 13-stream floor PR #390 established. The pair only splits when the deny
// list turns into a watch scope, and it has to split then — see
// newSharedFactories.
type sharedFactories struct {
	// Namespaced serves every namespaced informer: Pods, Events, Deployments,
	// ReplicaSets, StatefulSets, EndpointSlices, PDBs, Jobs, CronJobs and HPAs.
	Namespaced informers.SharedInformerFactory

	// Cluster serves the cluster-scoped informers. Nodes is the only one, and
	// the only reason this field exists.
	Cluster informers.SharedInformerFactory
}

// sameFactory presents one factory in both roles: the unscoped shape, and the
// only shape tests that are not about scoping should use.
func sameFactory(f informers.SharedInformerFactory) sharedFactories {
	return sharedFactories{Namespaced: f, Cluster: f}
}

// Split reports whether the two roles are served by different factories, which
// is exactly "a namespace deny list is in force".
func (sf sharedFactories) Split() bool { return sf.Cluster != sf.Namespaced }

// Start starts every informer registered so far, once per factory. Callers must
// not reach for Namespaced.Start directly: when the pair is split, the node
// informer lives on the other one and would never be started.
func (sf sharedFactories) Start(stopCh <-chan struct{}) {
	sf.Namespaced.Start(stopCh)
	if sf.Split() {
		sf.Cluster.Start(stopCh)
	}
}

// newSharedFactories builds the runner's informer factories, with the transform
// attached, excluding the named namespaces at the API server when any are given.
//
// This exists so that there is exactly one place a factory is constructed. The
// alternative — wiring.go builds its own and the test builds a matching one —
// passes happily on the day someone drops an option from wiring, because the
// test is still constructing a factory that has it. The tests call this.
//
// # Why two factories
//
// A field selector set through WithTweakListOptions applies to every informer
// the factory serves, and `metadata.namespace` is not a selectable field on a
// cluster-scoped resource. The API server does not ignore it, it refuses:
//
//	$ kubectl get nodes --field-selector='metadata.namespace!=kube-system'
//	Error from server (BadRequest): Unable to find "/v1, Resource=nodes" that
//	match label selector "", field selector "metadata.namespace!=kube-system":
//	field label not supported: metadata.namespace
//
// So a single filtered factory would not filter the node watch, it would break
// it — and break it in the worst way, since the failure surfaces as a reflector
// that never syncs rather than as a startup error. Nodes therefore ride an
// unfiltered factory of their own, which costs nothing: they are cluster-scoped,
// so there is nothing for a namespace deny list to remove from them anyway.
//
// Verified against every namespaced type the factory serves (Events,
// EndpointSlices, Deployments, ReplicaSets, StatefulSets, Jobs, CronJobs, PDBs,
// HorizontalPodAutoscalers): all accept the selector, only Nodes rejects it.
func newSharedFactories(client kubernetes.Interface, excludeNamespaces []string) sharedFactories {
	selector := namespaceExclusionSelector(excludeNamespaces)
	if selector == "" {
		// One factory, used for both roles. Deliberately the same object and
		// not two equivalent ones: Start and Shutdown are then idempotent
		// across the pair for free, and the unset-flag path keeps exactly the
		// stream count it had before this option existed.
		return sameFactory(informers.NewSharedInformerFactoryWithOptions(client, 0,
			informers.WithTransform(sharedTransform)))
	}
	return sharedFactories{
		Namespaced: informers.NewSharedInformerFactoryWithOptions(client, 0,
			informers.WithTransform(sharedTransform),
			informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
				opts.FieldSelector = selector
			})),
		Cluster: informers.NewSharedInformerFactoryWithOptions(client, 0,
			informers.WithTransform(sharedTransform)),
	}
}

// namespaceExclusionSelector renders a deny list as a field selector, or ""
// when there is nothing to exclude.
//
// Field-selector requirements are ANDed, and `!=` is supported, so an
// exclusion of any length is one selector on one stream. An *inclusion* list
// is not: field selectors have no OR, so `metadata.namespace=a` can name
// exactly one namespace and watching M of them needs M factories. That
// asymmetry is the whole reason the deny list can become a watch scope cheaply
// and the allow list cannot (issue #407).
func namespaceExclusionSelector(excludeNamespaces []string) string {
	seen := make(map[string]struct{}, len(excludeNamespaces))
	for _, ns := range excludeNamespaces {
		if ns = strings.TrimSpace(ns); ns != "" {
			seen[ns] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return ""
	}
	// Sorted so the selector is stable across restarts: it is visible in API
	// server audit logs and in the reflector's own error messages, and a string
	// that reorders per process start cannot be diffed against the last one.
	terms := make([]string, 0, len(seen))
	for ns := range seen {
		terms = append(terms, "metadata.namespace!="+ns)
	}
	slices.Sort(terms)
	return strings.Join(terms, ",")
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
