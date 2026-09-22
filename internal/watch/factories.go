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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// The runner's informer factories.
//
// What enters a cache is kube.Transform's business and lives in pkg/kube,
// because cmd/leeway needs the identical trimming and may not import this
// package (§2.4 discipline 4). What is in SCOPE is this file's business,
// because only the sentinel has a namespace deny list.

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
// This exists so that there is exactly one place a factory is constructed for
// the sentinel. The alternative — wiring.go builds its own and the test builds a
// matching one — passes happily on the day someone drops an option from wiring,
// because the test is still constructing a factory that has it. The tests call
// this.
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
		return sameFactory(kube.NewTransformingFactory(client))
	}
	return sharedFactories{
		// The filtered factory is the one construction site in the repo that
		// cannot go through kube.NewTransformingFactory, because that one
		// cannot express a scope. It therefore has to attach kube.Transform
		// itself, and TestNewSharedFactories_BothHalvesTrim is what stops the
		// two from drifting.
		Namespaced: informers.NewSharedInformerFactoryWithOptions(client, 0,
			informers.WithTransform(kube.Transform),
			informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
				opts.FieldSelector = selector
			})),
		Cluster: kube.NewTransformingFactory(client),
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
