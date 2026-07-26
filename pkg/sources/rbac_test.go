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
	"errors"
	"strings"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// scriptedReviewer answers Allowed from a script keyed by
// Requirement.String(); anything not scripted is denied.
type scriptedReviewer struct {
	allowed map[string]bool
	err     error
}

func (s scriptedReviewer) Allowed(_ context.Context, req Requirement) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	return s.allowed[req.String()], nil
}

func TestProbe_FailsLoudlyNamingSourceAndPermission(t *testing.T) {
	t.Parallel()
	src := &fakeSource{
		name:  "k8s-events",
		scope: ScopeCluster,
		reqs: []Requirement{
			{Resource: "events", Verb: "list"},
			{Resource: "events", Verb: "watch"},
		},
	}
	// list allowed, watch denied → the error must name the source
	// AND the exact missing permission (§11: never a silent empty
	// watch).
	reviewer := scriptedReviewer{allowed: map[string]bool{
		Requirement{Resource: "events", Verb: "list"}.String(): true,
	}}
	err := Probe(context.Background(), reviewer, src)
	if err == nil {
		t.Fatal("Probe should fail when a declared permission is denied")
	}
	for _, want := range []string{`source "k8s-events"`, "watch events cluster-wide", "disable the source"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("fail-loudly error missing %q; got: %v", want, err)
		}
	}
}

func TestProbe_PassesWhenAllAllowed(t *testing.T) {
	t.Parallel()
	src := &fakeSource{
		name: "k8s-events",
		reqs: []Requirement{{Resource: "events", Verb: "list"}},
	}
	reviewer := scriptedReviewer{allowed: map[string]bool{
		Requirement{Resource: "events", Verb: "list"}.String(): true,
	}}
	if err := Probe(context.Background(), reviewer, src); err != nil {
		t.Errorf("Probe with all permissions granted: %v", err)
	}
}

func TestProbe_ReviewerErrorIsFatal(t *testing.T) {
	t.Parallel()
	src := &fakeSource{
		name: "k8s-events",
		reqs: []Requirement{{Resource: "events", Verb: "list"}},
	}
	reviewer := scriptedReviewer{err: errors.New("apiserver unreachable")}
	err := Probe(context.Background(), reviewer, src)
	if err == nil {
		t.Fatal(`"could not verify" must not degrade into "assumed fine"`)
	}
	if !strings.Contains(err.Error(), `source "k8s-events"`) {
		t.Errorf("probe error should name the source; got %v", err)
	}
}

func TestProbe_SkipsSourcesWithoutDeclaration(t *testing.T) {
	t.Parallel()
	// A Source that does not implement AccessDeclarer is skipped —
	// nothing to verify. Use a bare struct, not fakeSource (which
	// declares).
	if err := Probe(context.Background(), scriptedReviewer{}, bareSource{}); err != nil {
		t.Errorf("Probe over a non-declaring source: %v", err)
	}
}

type bareSource struct{}

func (bareSource) Name() string                                  { return "bare" }
func (bareSource) Scope() Scope                                  { return ScopeNamespace }
func (bareSource) Run(ctx context.Context, _ func(Signal)) error { <-ctx.Done(); return nil }

// TestSelfSubjectAccessReviewer verifies the client-go backed
// implementation end to end against a fake clientset: the SSAR the
// reviewer creates must carry the requirement's attributes, and the
// API's verdict must round-trip.
func TestSelfSubjectAccessReviewer(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	var gotAttrs *authorizationv1.ResourceAttributes
	client.PrependReactor("create", "selfsubjectaccessreviews",
		func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
			review := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview)
			gotAttrs = review.Spec.ResourceAttributes
			// Grant list on events only; deny everything else.
			allowed := gotAttrs.Resource == "events" && gotAttrs.Verb == "list"
			review = review.DeepCopy()
			review.Status.Allowed = allowed
			return true, review, nil
		})

	reviewer := NewAccessReviewer(client)
	ok, err := reviewer.Allowed(context.Background(), Requirement{Resource: "events", Verb: "list", Namespace: "prod"})
	if err != nil || !ok {
		t.Errorf("Allowed(list events) = %v, %v; want true, nil", ok, err)
	}
	if gotAttrs == nil || gotAttrs.Verb != "list" || gotAttrs.Resource != "events" || gotAttrs.Namespace != "prod" {
		t.Errorf("SSAR attributes = %+v; want verb=list resource=events namespace=prod", gotAttrs)
	}

	ok, err = reviewer.Allowed(context.Background(), Requirement{Resource: "nodes", Verb: "watch"})
	if err != nil || ok {
		t.Errorf("Allowed(watch nodes) = %v, %v; want false, nil", ok, err)
	}
}

// TestProbe_DeniedSSAR_EndToEnd is the startup fail-loudly scenario
// through the real reviewer: a fake API server that denies watch →
// Probe error naming source + permission.
func TestProbe_DeniedSSAR_EndToEnd(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	client.PrependReactor("create", "selfsubjectaccessreviews",
		func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
			review := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview).DeepCopy()
			review.Status.Allowed = false // deny everything
			return true, review, nil
		})
	src := &fakeSource{
		name:  "k8s-events",
		scope: ScopeCluster,
		reqs:  []Requirement{{Resource: "events", Verb: "list"}},
	}
	err := Probe(context.Background(), NewAccessReviewer(client), src)
	if err == nil {
		t.Fatal("Probe should fail against a denying API server")
	}
	for _, want := range []string{`source "k8s-events"`, "list events cluster-wide"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q; got: %v", want, err)
		}
	}
}

// TestRequirement_Subresource pins the M3 addition for the saturation
// source's nodes/proxy requirement: the String rendering matches the
// (Cluster)Role rule form and the SSAR carries the subresource.
func TestRequirement_Subresource(t *testing.T) {
	t.Parallel()
	req := Requirement{Resource: "nodes", Subresource: "proxy", Verb: "get"}
	if got, want := req.String(), "get nodes/proxy cluster-wide"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}

	client := fake.NewClientset()
	var gotAttrs *authorizationv1.ResourceAttributes
	client.PrependReactor("create", "selfsubjectaccessreviews",
		func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
			review := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview)
			gotAttrs = review.Spec.ResourceAttributes
			review = review.DeepCopy()
			review.Status.Allowed = true
			return true, review, nil
		})
	if ok, err := NewAccessReviewer(client).Allowed(context.Background(), req); err != nil || !ok {
		t.Fatalf("Allowed = %v, %v; want true, nil", ok, err)
	}
	if gotAttrs == nil || gotAttrs.Subresource != "proxy" || gotAttrs.Resource != "nodes" {
		t.Errorf("SSAR attributes = %+v; want resource=nodes subresource=proxy", gotAttrs)
	}
}
