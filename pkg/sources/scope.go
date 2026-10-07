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
// A sentinel run with a namespace watch scope builds its namespaced
// informers with informers.WithNamespace, one factory per scope namespace,
// so every LIST and WATCH they issue is a namespaced request. The
// requirements sources declare do not know that: they were written for the
// cluster tier and leave Namespace empty, which the probe reads as
// "cluster-wide". Asked as written, every requirement would be denied to a
// namespaced Role, and the probe would refuse a deployment that would in
// fact run.
//
// Rather than thread the namespaces through every source's configuration,
// the scope is applied to the requirements here, at the probe seam, by
// wrapping the reviewer every probe asks. That is one transform in one
// place, and it covers every caller that asks the same question: the §11
// startup probe, auto resolution, the storm and routing probes, the
// recovery fallback and the periodic access re-check.
//
// With several scope namespaces a requirement fans out: one copy per
// namespace, every one of which must be allowed, because the sentinel
// watches every one of them.
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

// RequirementScoper is implemented by an AccessReviewer that rewrites a
// requirement before asking about it — possibly into several. Callers that
// print a requirement next to the answer iterate Expand so that every line
// names what was actually asked.
type RequirementScoper interface {
	ScopeRequirement(Requirement) []Requirement
}

// Expand returns the requirements reviewer will actually ask for req: the
// per-namespace copies when reviewer is a RequirementScoper, req alone
// otherwise. Each result is already scoped, so asking Allowed with it asks
// exactly that one question again.
func Expand(reviewer AccessReviewer, req Requirement) []Requirement {
	if s, ok := reviewer.(RequirementScoper); ok {
		return s.ScopeRequirement(req)
	}
	return []Requirement{req}
}

// NewNamespaceScopedReviewer wraps inner so that every requirement on a
// namespaced resource that does not name a namespace of its own is asked in
// each of namespaces. Requirements that already carry a namespace (the
// capacity source's kube-system ConfigMap, explicit --expiry-namespaces)
// are left alone: the source declared exactly what it reads.
//
// namespaced is consulted per requirement; a resource it does not know is
// left cluster-wide. That errs toward a denial, never toward an informer
// that cannot sync.
func NewNamespaceScopedReviewer(inner AccessReviewer, namespaces []string, namespaced NamespacedFunc) AccessReviewer {
	return namespaceScopedReviewer{inner: inner, namespaces: namespaces, namespaced: namespaced}
}

type namespaceScopedReviewer struct {
	inner      AccessReviewer
	namespaces []string
	namespaced NamespacedFunc
}

// ScopeRequirement implements RequirementScoper. A subresource
// ("pods/log", "nodes/proxy") is scoped by its parent resource.
func (r namespaceScopedReviewer) ScopeRequirement(req Requirement) []Requirement {
	if req.Namespace != "" || len(r.namespaces) == 0 {
		return []Requirement{req}
	}
	if ns, known := r.namespaced(req.Group, req.Resource); !known || !ns {
		return []Requirement{req}
	}
	out := make([]Requirement, 0, len(r.namespaces))
	for _, ns := range r.namespaces {
		scoped := req
		scoped.Namespace = ns
		out = append(out, scoped)
	}
	return out
}

// Allowed implements AccessReviewer: allowed only when every expansion is.
// The first refusal is returned as is; a caller that needs to know WHICH
// namespace refused iterates Expand and asks each copy itself.
func (r namespaceScopedReviewer) Allowed(ctx context.Context, req Requirement) (Decision, error) {
	var d Decision
	for _, scoped := range r.ScopeRequirement(req) {
		var err error
		if d, err = r.inner.Allowed(ctx, scoped); err != nil || !d.Allowed {
			return d, err
		}
	}
	return d, nil
}

// ExpandAll is Expand over a requirement list, in order: what a probe loop
// iterates so that each question it asks — and each line it prints — names
// the namespace it was asked in.
func ExpandAll(reviewer AccessReviewer, reqs []Requirement) []Requirement {
	out := make([]Requirement, 0, len(reqs))
	for _, req := range reqs {
		out = append(out, Expand(reviewer, req)...)
	}
	return out
}
