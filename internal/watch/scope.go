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
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/sources"
	"github.com/go-steer/k8s-lookout/pkg/sources/saturation"
)

// --watch-scope (issue #407): watching a list of namespaces instead of the
// cluster.
//
// The pieces, and where each lives:
//
//   - admission (admitNamespaces): a listed namespace whose Event grant is
//     refused is skipped and counted, never fatal unless all are;
//   - the namespaced informers come from one informers.WithNamespace
//     factory per admitted namespace — a read shard — presented to the
//     sources as one factory (newSharedFactories, union.go). No source
//     changes, and still one instance of each source and one brain;
//   - the §11 probe asks every declared requirement in each namespace when
//     the resource has a namespaced form (newAccessReviewer, the one
//     construction site for every reviewer the sentinel uses), so namespaced
//     Roles pass and cluster-scoped needs — nodes above all — are still
//     asked cluster-wide;
//   - the handful of reads that are not informers on the shared factory
//     (saturation's polled LISTs, expiry's scans, the gateway and
//     LeewayPolicy dynamic informers, node-incident enrichment) are told the
//     namespaces in wiring.go, because a probe that passes in-namespace over
//     a read that is still cluster-wide would be a 403 at runtime, or for an
//     informer a sync that never completes.
//
// The scope is per run: in multi-cluster mode every runner watches the same
// namespace names in its own cluster.

const (
	watchScopeCluster   = "cluster"
	watchScopeNamespace = "namespace"
)

// validateWatchScope checks --watch-scope and its pairing with --namespace.
// Every failure is a usage error (exit 2).
func (f *flags) validateWatchScope() error {
	switch f.watchScope {
	case watchScopeCluster:
		return nil
	case watchScopeNamespace:
	default:
		return emit.UsageErrorf("--watch-scope must be cluster or namespace (got %q)", f.watchScope)
	}
	nss := uniqueCSV(f.namespaces)
	if len(nss) == 0 {
		return emit.UsageErrorf("--watch-scope=namespace needs at least one --namespace value: the namespaces to watch, comma-separated")
	}
	for _, ns := range nss {
		if slices.Contains(splitCSV(f.excludeNamespaces), ns) {
			return emit.UsageErrorf("--exclude-namespace names %q, a namespace --watch-scope=namespace watches — drop it from one of the two lists", ns)
		}
	}
	return nil
}

// uniqueCSV is splitCSV with duplicates removed, first occurrence kept: a
// namespace named twice is one namespace, never two factories watching it.
func uniqueCSV(s string) []string {
	var out []string
	for _, v := range splitCSV(s) {
		if !slices.Contains(out, v) {
			out = append(out, v)
		}
	}
	return out
}

// scopeNamespaces is the namespaces the informers are confined to, or nil
// for the cluster scope. Once a run has admitted its namespaces (see
// admitNamespaces) it is the admitted subset: a namespace skipped for a
// missing grant is not watched, probed or scanned. Only meaningful after
// validate.
func (f *flags) scopeNamespaces() []string {
	if f.watchScope != watchScopeNamespace {
		return nil
	}
	if f.admittedNamespaces != nil {
		return f.admittedNamespaces
	}
	return uniqueCSV(f.namespaces)
}

// expiryNamespaceList is what the expiry source scans: --expiry-namespaces
// when given, else the watch-scope namespaces, else every namespace (nil).
// Used by both the real source and auto's throwaway, so the probe asks
// exactly what the source will read.
func (f *flags) expiryNamespaceList() []string {
	if nss := splitCSV(f.expiryNamespaces); len(nss) > 0 {
		return nss
	}
	return f.scopeNamespaces()
}

// newAccessReviewer is the one place the sentinel builds an AccessReviewer.
// In the cluster scope it is the plain SelfSubjectAccessReview reviewer,
// exactly as before. In the namespace scope it is wrapped so that every
// requirement on a namespaced resource is asked in each scope namespace,
// using discovery's APIResource.Namespaced to decide which resources have
// that form.
func newAccessReviewer(f *flags, client kubernetes.Interface) sources.AccessReviewer {
	base := sources.NewAccessReviewer(client)
	nss := f.scopeNamespaces()
	if len(nss) == 0 {
		return base
	}
	d := &scopeDiscovery{client: client}
	return sources.NewNamespaceScopedReviewer(discoveryGate{inner: base, d: d}, nss, d.namespaced)
}

// namespaceAdmission is the floor a namespace must clear to be watched at
// all: the Event list+watch every sentinel needs (k8s-events is the one
// source auto never skips).
var namespaceAdmission = []sources.Requirement{
	{Resource: "events", Verb: "list"},
	{Resource: "events", Verb: "watch"},
}

// admitNamespaces decides, once per run, which scope namespaces this
// sentinel watches (#407's skip-vs-abort question). A namespace whose
// Event grant is refused is SKIPPED, not fatal — the kubeconfig fleet's
// precedent (#388): one namespace's missing Role is not every namespace's
// problem. Skipping is loud three ways: one line naming the namespace and
// the refused grant, lookout_namespace_errors_total{namespace,cause}, and
// the admitted list in the scope line that follows. A coverage gap must not
// read as a quiet namespace.
//
// Every namespace refused is fatal, with the same refusal the single
// namespace form gives: a sentinel watching nothing is misdeployed. A
// probe that cannot be evaluated is fatal too (§11).
//
// The admitted list is recorded on f for the rest of the run; a supervisor
// restart re-admits from the full list, so a Role granted later is picked
// up then.
func admitNamespaces(ctx context.Context, f *flags, client kubernetes.Interface, m *metrics) error {
	f.admittedNamespaces = nil
	nss := f.scopeNamespaces()
	if len(nss) == 0 {
		return nil
	}
	base := sources.NewAccessReviewer(client)
	admitted := make([]string, 0, len(nss))
	var lastDenied error
	for _, ns := range nss {
		var refused *sources.Requirement
		var why sources.Decision
		for _, req := range namespaceAdmission {
			req.Namespace = ns
			d, err := base.Allowed(ctx, req)
			if err != nil {
				return fmt.Errorf("--watch-scope=namespace: capability probe for %q failed: %w", req, err)
			}
			if !d.Allowed {
				r := req
				refused, why = &r, d
				break
			}
		}
		if refused == nil {
			admitted = append(admitted, ns)
			continue
		}
		if m != nil {
			m.namespaceErrors.WithLabelValues(ns, "access_denied").Inc()
		}
		lastDenied = fmt.Errorf("--watch-scope=namespace: no namespace can be watched — the last refusal: %q, %s; a sentinel that cannot watch events is misdeployed%.0w", *refused, sources.DenialDetail(why), sources.ErrAccessDenied)
		log.Printf("watch: namespace %q NOT watched — %q denied (%s); grant the namespaced Role there (deploy-namespaced/) and restart. The other namespaces are watched; lookout_namespace_errors_total{namespace=%q} counts this gap (#407)", ns, *refused, sources.DenialDetail(why), ns)
	}
	if len(admitted) == 0 {
		return lastDenied
	}
	f.admittedNamespaces = admitted
	return nil
}

// scopeDiscovery answers "does this resource have a namespaced form" from
// one discovery pass, loaded on first use.
type scopeDiscovery struct {
	client kubernetes.Interface

	once sync.Once
	byGR map[schema.GroupResource]bool
	err  error
}

func (d *scopeDiscovery) load() {
	d.once.Do(func() {
		// ServerGroupsAndResources, not ServerPreferredResources: scope is
		// a property of the resource, the same in every version, and this
		// one also reports a partial answer when an aggregated API is down
		// — a broken metrics-server must not stop the probe asking about
		// pods.
		_, lists, err := d.client.Discovery().ServerGroupsAndResources()
		d.byGR = make(map[schema.GroupResource]bool)
		for _, l := range lists {
			if l == nil {
				continue
			}
			gv, perr := schema.ParseGroupVersion(l.GroupVersion)
			if perr != nil {
				continue
			}
			for _, r := range l.APIResources {
				if strings.Contains(r.Name, "/") {
					continue // subresources are scoped by their parent
				}
				group := gv.Group
				if r.Group != "" {
					group = r.Group
				}
				d.byGR[schema.GroupResource{Group: group, Resource: r.Name}] = r.Namespaced
			}
		}
		if err != nil && len(d.byGR) == 0 {
			d.err = fmt.Errorf("--watch-scope=namespace: discovery failed, so the probe cannot tell which resources are namespaced: %w", err)
		}
	})
}

// namespaced implements sources.NamespacedFunc.
func (d *scopeDiscovery) namespaced(group, resource string) (namespaced, known bool) {
	d.load()
	namespaced, known = d.byGR[schema.GroupResource{Group: group, Resource: resource}]
	return namespaced, known
}

// discoveryGate turns a failed discovery pass into a probe error. Without it
// every requirement would be asked cluster-wide and denied, and the operator
// would read an RBAC gap where the real fault is an unreachable API server —
// §11's "could not verify" must not read as "denied" any more than as
// "assumed fine".
type discoveryGate struct {
	inner sources.AccessReviewer
	d     *scopeDiscovery
}

func (g discoveryGate) Allowed(ctx context.Context, req sources.Requirement) (sources.Decision, error) {
	g.d.load()
	if g.d.err != nil {
		return sources.Decision{}, g.d.err
	}
	return g.inner.Allowed(ctx, req)
}

// scopedPodFetcher is saturation's pod-usage fetcher over the watch scope:
// the cluster-wide fetcher when nss is empty, one namespaced fetcher per
// scope namespace otherwise, their samples concatenated. Polled reads do
// not ride the informer shards, so the scope has to reach them here.
func scopedPodFetcher(metricsClient metricsv.Interface, client kubernetes.Interface, nss []string) saturation.PodUsageFetcher {
	if len(nss) == 0 {
		return saturation.NewMetricsPodFetcher(metricsClient, client)
	}
	fs := make(multiPodFetcher, len(nss))
	for i, ns := range nss {
		fs[i] = saturation.NewScopedMetricsPodFetcher(metricsClient, client, ns)
	}
	return fs
}

type multiPodFetcher []saturation.PodUsageFetcher

func (m multiPodFetcher) FetchPodUsage(ctx context.Context) ([]saturation.ContainerSample, error) {
	var out []saturation.ContainerSample
	for _, f := range m {
		s, err := f.FetchPodUsage(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, s...)
	}
	return out, nil
}
