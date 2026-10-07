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
	"slices"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// --watch-scope (issue #407): watching one namespace instead of the cluster.
//
// The pieces, and where each lives:
//
//   - the namespaced informers come from a factory built with
//     informers.WithNamespace (newSharedFactories) — no source changes,
//     because a namespaced factory hands out namespaced informers through
//     the same call sites;
//   - the §11 probe asks every declared requirement in that namespace when
//     the resource has a namespaced form (newAccessReviewer, the one
//     construction site for every reviewer the sentinel uses), so a
//     namespaced Role passes and cluster-scoped needs — nodes above all —
//     are still asked cluster-wide and denied;
//   - the handful of reads that are not informers on the shared factory
//     (saturation's polled LISTs, expiry's scans, the gateway and
//     LeewayPolicy dynamic informers, node-incident enrichment) are told the
//     namespace in wiring.go, because a probe that passes in-namespace over
//     a read that is still cluster-wide would be a 403 at runtime, or for an
//     informer a sync that never completes.
//
// The scope is per run: in multi-cluster mode every runner watches the same
// namespace name in its own cluster.

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
	nss := splitCSV(f.namespaces)
	if len(nss) != 1 {
		return emit.UsageErrorf("--watch-scope=namespace needs exactly one --namespace value (got %d: %q) — one namespace per sentinel; watching several is not supported", len(nss), f.namespaces)
	}
	if slices.Contains(splitCSV(f.excludeNamespaces), nss[0]) {
		return emit.UsageErrorf("--exclude-namespace names %q, the namespace --watch-scope=namespace watches — nothing would be left to watch", nss[0])
	}
	return nil
}

// scopeNamespace is the namespace the informers are confined to, or "" for
// the cluster scope. Only meaningful after validate.
func (f *flags) scopeNamespace() string {
	if f.watchScope != watchScopeNamespace {
		return ""
	}
	if nss := splitCSV(f.namespaces); len(nss) == 1 {
		return nss[0]
	}
	return ""
}

// expiryNamespaceList is what the expiry source scans: --expiry-namespaces
// when given, else the watch-scope namespace, else every namespace (nil).
// Used by both the real source and auto's throwaway, so the probe asks
// exactly what the source will read.
func (f *flags) expiryNamespaceList() []string {
	if nss := splitCSV(f.expiryNamespaces); len(nss) > 0 {
		return nss
	}
	if ns := f.scopeNamespace(); ns != "" {
		return []string{ns}
	}
	return nil
}

// newAccessReviewer is the one place the sentinel builds an AccessReviewer.
// In the cluster scope it is the plain SelfSubjectAccessReview reviewer,
// exactly as before. In the namespace scope it is wrapped so that every
// requirement on a namespaced resource is asked in the scope namespace,
// using discovery's APIResource.Namespaced to decide which resources have
// that form.
func newAccessReviewer(f *flags, client kubernetes.Interface) sources.AccessReviewer {
	base := sources.NewAccessReviewer(client)
	ns := f.scopeNamespace()
	if ns == "" {
		return base
	}
	d := &scopeDiscovery{client: client}
	return sources.NewNamespaceScopedReviewer(discoveryGate{inner: base, d: d}, ns, d.namespaced)
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
