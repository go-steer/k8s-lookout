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

import (
	"context"
	"fmt"

	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Requirement is one RBAC capability a source needs to do its job:
// a (verb, resource) pair, optionally namespace-scoped. Sources
// declare these via AccessDeclarer; Probe checks them at startup.
type Requirement struct {
	// Group is the API group ("" = core).
	Group string
	// Resource is the lowercase plural resource ("events", "pods").
	Resource string
	// Subresource is the optional subresource — the RBAC rule form is
	// "resource/subresource" (e.g. "nodes/proxy" for the saturation
	// source's kubelet stats summary reads).
	Subresource string
	// Verb is the RBAC verb ("list", "watch", "get", …).
	Verb string
	// Namespace scopes the check; empty means cluster-wide (which
	// a namespaced Role cannot satisfy — exactly the §11 tier
	// mismatch the probe exists to catch).
	Namespace string
	// Name scopes the check to one named object, for sources that
	// only ever read a single well-known resource (the capacity
	// source's cluster-autoscaler-status ConfigMap). A named
	// requirement can be satisfied by a `resourceNames`-pinned Role
	// rule — and MUST be declared named when that is all the source
	// reads, or the SSAR would demand the broader all-names grant
	// the deployment deliberately did not make. Empty = any name.
	Name string
}

// String renders the requirement the way an operator would write it
// in a (Cluster)Role rule, for use in fail-loudly startup errors.
func (r Requirement) String() string {
	res := r.Resource
	if r.Subresource != "" {
		res += "/" + r.Subresource
	}
	if r.Group != "" {
		res += "." + r.Group
	}
	if r.Name != "" {
		res += " " + r.Name
	}
	scope := "cluster-wide"
	if r.Namespace != "" {
		scope = "in namespace " + r.Namespace
	}
	return fmt.Sprintf("%s %s %s", r.Verb, res, scope)
}

// AccessDeclarer is the optional side of Source: a source that needs
// RBAC declares it here so the sentinel can verify capabilities at
// startup instead of running a silently empty watch (§11).
type AccessDeclarer interface {
	RequiredAccess() []Requirement
}

// AccessReviewer answers "can I?" for one requirement. The real
// implementation asks the API server via SelfSubjectAccessReview;
// tests substitute a fake that denies.
type AccessReviewer interface {
	Allowed(ctx context.Context, req Requirement) (bool, error)
}

// NewAccessReviewer returns the client-go backed AccessReviewer:
// each Allowed call creates a SelfSubjectAccessReview (authorization
// v1), asking the API server whether the sentinel's own credentials
// hold the requirement. SSAR creation is granted to every
// authenticated subject by the default system:basic-user binding, so
// the probe itself needs no RBAC beyond being able to authenticate.
func NewAccessReviewer(client kubernetes.Interface) AccessReviewer {
	return ssarReviewer{client: client}
}

type ssarReviewer struct {
	client kubernetes.Interface
}

func (r ssarReviewer) Allowed(ctx context.Context, req Requirement) (bool, error) {
	review := &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Group:       req.Group,
				Resource:    req.Resource,
				Subresource: req.Subresource,
				Verb:        req.Verb,
				Namespace:   req.Namespace,
				Name:        req.Name,
			},
		},
	}
	resp, err := r.client.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, review, metav1.CreateOptions{})
	if err != nil {
		return false, err
	}
	return resp.Status.Allowed, nil
}

// Probe verifies every requirement declared by every source before
// anything starts watching. The failure mode it exists to prevent is
// the silent empty watch (§11): an informer without list/watch
// permission logs a warning and then reports nothing forever, which
// reads as "cluster healthy". So a deployment whose RBAC cannot
// support a source fails loudly at startup, naming the source and
// the missing permission, and the operator either grants the
// permission or disables the source explicitly.
//
// Sources that do not implement AccessDeclarer are skipped. An error
// from the reviewer itself (API unreachable, SSAR rejected) is also
// fatal: "could not verify" must not degrade into "assumed fine".
func Probe(ctx context.Context, reviewer AccessReviewer, srcs ...Source) error {
	for _, s := range srcs {
		decl, ok := s.(AccessDeclarer)
		if !ok {
			continue
		}
		for _, req := range decl.RequiredAccess() {
			allowed, err := reviewer.Allowed(ctx, req)
			if err != nil {
				return fmt.Errorf("source %q: capability probe for %q failed: %w", s.Name(), req, err)
			}
			if !allowed {
				return fmt.Errorf("source %q requires permission to %q (scope: %s) and this ServiceAccount does not have it; grant it or disable the source — refusing to run a silently empty watch", s.Name(), req, s.Scope())
			}
		}
	}
	return nil
}
