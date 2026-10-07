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

package sources

import "context"

// Namespace-scoped watching (issue #407).
//
// A sentinel run with a single-namespace watch scope builds its namespaced
// informers with informers.WithNamespace, so every LIST and WATCH they issue
// is a namespaced request. The requirements sources declare do not know
// that: they were written for the cluster tier and leave Namespace empty,
// which the probe reads as "cluster-wide". Asked as written, every
// requirement would be denied to a namespaced Role, and the probe would
// refuse a deployment that would in fact run.
//
// Rather than thread the namespace through every source's configuration,
// the scope is applied to the requirements here, at the probe seam, by
// wrapping the reviewer every probe asks. That is one transform in one
// place, and it covers every caller that asks the same question: the §11
// startup probe, auto resolution, the storm and routing probes, the
// recovery fallback and the periodic access re-check.
//
// Only resources that have a namespaced form are stamped. Nodes,
// PersistentVolumes, webhook configurations, ComputeClasses and the like
// stay cluster-wide, so a namespaced Role is still denied them — which is
// what makes the sources that need them degrade through the probe (skipped
// under --sources=auto, fatal when named) instead of starting an informer
// that would retry a 403 forever.

// NamespacedFunc reports whether a resource has a namespaced form. known is
// false when the answer is not available (the resource is not served);
// an unknown resource is left as declared.
type NamespacedFunc func(group, resource string) (namespaced, known bool)

// RequirementScoper is implemented by an AccessReviewer that narrows a
// requirement before asking about it. Callers that print a requirement
// next to the answer use Effective so that the line names what was
// actually asked.
type RequirementScoper interface {
	ScopeRequirement(Requirement) Requirement
}

// Effective returns req as reviewer will ask it: scoped when reviewer is a
// RequirementScoper, unchanged otherwise. Asking Allowed with the result is
// the same question as asking with req, since scoping is idempotent.
func Effective(reviewer AccessReviewer, req Requirement) Requirement {
	if s, ok := reviewer.(RequirementScoper); ok {
		return s.ScopeRequirement(req)
	}
	return req
}

// NewNamespaceScopedReviewer wraps inner so that every requirement on a
// namespaced resource that does not name a namespace of its own is asked in
// namespace. Requirements that already carry a namespace (the capacity
// source's kube-system ConfigMap, explicit --expiry-namespaces) are left
// alone: the source declared exactly what it reads.
//
// namespaced is consulted per requirement; a resource it does not know is
// left cluster-wide. That errs toward a denial, never toward an informer
// that cannot sync.
func NewNamespaceScopedReviewer(inner AccessReviewer, namespace string, namespaced NamespacedFunc) AccessReviewer {
	return namespaceScopedReviewer{inner: inner, namespace: namespace, namespaced: namespaced}
}

type namespaceScopedReviewer struct {
	inner      AccessReviewer
	namespace  string
	namespaced NamespacedFunc
}

// ScopeRequirement implements RequirementScoper. A subresource
// ("pods/log", "nodes/proxy") is scoped by its parent resource.
func (r namespaceScopedReviewer) ScopeRequirement(req Requirement) Requirement {
	if req.Namespace != "" || r.namespace == "" {
		return req
	}
	if ns, known := r.namespaced(req.Group, req.Resource); known && ns {
		req.Namespace = r.namespace
	}
	return req
}

// Allowed implements AccessReviewer.
func (r namespaceScopedReviewer) Allowed(ctx context.Context, req Requirement) (Decision, error) {
	return r.inner.Allowed(ctx, r.ScopeRequirement(req))
}
