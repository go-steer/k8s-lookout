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
	"context"
	"sync"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// CallerVisibility decides which findings an invocation running as its
// caller (#699) may show them. Live reads already run with the caller's
// token, so the API server has applied their RBAC; rows from lookout's own
// store or a report file have not been vetted by anyone, and this is the
// check that vets them. It satisfies emit.Visibility.
//
// The rule, using the caller's own token (SelfSubjectAccessReview, so no
// extra grant for lookout):
//
//   - A finding about an object in a namespace is visible if the caller
//     may list pods in that namespace: the common signal of access to a
//     namespace, and what a caller needs to investigate anything there.
//   - A finding about a cluster-scoped object (no namespace) is visible
//     only if the caller may list pods in every namespace: cluster-wide
//     read.
//   - A caller with cluster-wide read sees everything, so that one review
//     settles the common case.
//
// On GKE a review answers for the caller's RBAC and IAM alike. Answers are
// cached for the invocation. A review that fails hides the row: the check
// fails closed.
type CallerVisibility struct {
	ctx    context.Context
	client func() (kubernetes.Interface, error)

	once   sync.Once
	cl     kubernetes.Interface
	clErr  error
	mu     sync.Mutex
	all    *bool
	byName map[string]bool
}

// NewCallerVisibility returns the visibility check for the invocation ctx
// describes: its cluster selection and caller token. It contacts the API
// server only when first asked.
func NewCallerVisibility(ctx context.Context) *CallerVisibility {
	return &CallerVisibility{
		ctx:    ctx,
		client: func() (kubernetes.Interface, error) { return BuildClient(OptionsFrom(ctx)) },
		byName: map[string]bool{},
	}
}

// reviewTimeout bounds one access review.
const reviewTimeout = 10 * time.Second

// Visible reports whether the caller may see a finding about an object in
// namespace ns ("" for cluster-scoped).
func (v *CallerVisibility) Visible(ns string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.all == nil {
		all := v.canListPods("")
		v.all = &all
	}
	if *v.all {
		return true
	}
	if ns == "" {
		return false
	}
	if ok, cached := v.byName[ns]; cached {
		return ok
	}
	ok := v.canListPods(ns)
	v.byName[ns] = ok
	return ok
}

// canListPods asks the API server, as the caller, whether they may list
// pods in ns ("" means every namespace). Any error is a no.
func (v *CallerVisibility) canListPods(ns string) bool {
	v.once.Do(func() { v.cl, v.clErr = v.client() })
	if v.clErr != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(v.ctx, reviewTimeout)
	defer cancel()
	review, err := v.cl.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{Namespace: ns, Verb: "list", Resource: "pods"},
		},
	}, metav1.CreateOptions{})
	return err == nil && review.Status.Allowed
}

type callerNameKey struct{}

// WithCallerName records who the invocation runs as (an email, in
// Google-token mode), for the records it writes (triage status
// recorded_by). An empty name leaves ctx unchanged.
func WithCallerName(ctx context.Context, name string) context.Context {
	if name == "" {
		return ctx
	}
	return context.WithValue(ctx, callerNameKey{}, name)
}

// CallerNameFrom returns the name WithCallerName recorded, or "".
func CallerNameFrom(ctx context.Context) string {
	s, _ := ctx.Value(callerNameKey{}).(string)
	return s
}
