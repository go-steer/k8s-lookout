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
	reasons map[string]string
	err     error
}

func (s scriptedReviewer) Allowed(_ context.Context, req Requirement) (Decision, error) {
	if s.err != nil {
		return Decision{}, s.err
	}
	return Decision{Allowed: s.allowed[req.String()], Reason: s.reasons[req.String()]}, nil
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
	_, err := Probe(context.Background(), reviewer, src)
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
	if _, err := Probe(context.Background(), reviewer, src); err != nil {
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
	_, err := Probe(context.Background(), reviewer, src)
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
	if _, err := Probe(context.Background(), scriptedReviewer{}, bareSource{}); err != nil {
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
	d, err := reviewer.Allowed(context.Background(), Requirement{Resource: "events", Verb: "list", Namespace: "prod"})
	if err != nil || !d.Allowed {
		t.Errorf("Allowed(list events) = %+v, %v; want allowed, nil", d, err)
	}
	if gotAttrs == nil || gotAttrs.Verb != "list" || gotAttrs.Resource != "events" || gotAttrs.Namespace != "prod" {
		t.Errorf("SSAR attributes = %+v; want verb=list resource=events namespace=prod", gotAttrs)
	}

	d, err = reviewer.Allowed(context.Background(), Requirement{Resource: "nodes", Verb: "watch"})
	if err != nil || d.Allowed {
		t.Errorf("Allowed(watch nodes) = %+v, %v; want denied, nil", d, err)
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
	_, err := Probe(context.Background(), NewAccessReviewer(client), src)
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
	if d, err := NewAccessReviewer(client).Allowed(context.Background(), req); err != nil || !d.Allowed {
		t.Fatalf("Allowed = %+v, %v; want allowed, nil", d, err)
	}
	if gotAttrs == nil || gotAttrs.Subresource != "proxy" || gotAttrs.Resource != "nodes" {
		t.Errorf("SSAR attributes = %+v; want resource=nodes subresource=proxy", gotAttrs)
	}
}

// TestRequirement_Name pins the M4 addition for the capacity source's
// status-ConfigMap requirement: a name-scoped check renders the named
// object and the SSAR carries ResourceAttributes.Name, so a
// `resourceNames`-pinned kube-system Role satisfies exactly what the
// source reads (and nothing broader).
func TestRequirement_Name(t *testing.T) {
	t.Parallel()
	req := Requirement{Resource: "configmaps", Verb: "get", Namespace: "kube-system", Name: "cluster-autoscaler-status"}
	if got, want := req.String(), "get configmaps cluster-autoscaler-status in namespace kube-system"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}

	client := fake.NewClientset()
	var gotAttrs *authorizationv1.ResourceAttributes
	client.PrependReactor("create", "selfsubjectaccessreviews",
		func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
			review := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview)
			gotAttrs = review.Spec.ResourceAttributes
			review = review.DeepCopy()
			// Grant ONLY the named object — the resourceNames-pinned
			// Role's behavior.
			review.Status.Allowed = gotAttrs.Name == "cluster-autoscaler-status"
			return true, review, nil
		})
	reviewer := NewAccessReviewer(client)
	if d, err := reviewer.Allowed(context.Background(), req); err != nil || !d.Allowed {
		t.Fatalf("Allowed(named) = %+v, %v; want allowed, nil", d, err)
	}
	if gotAttrs == nil || gotAttrs.Name != "cluster-autoscaler-status" || gotAttrs.Namespace != "kube-system" {
		t.Errorf("SSAR attributes = %+v; want the named, namespaced check", gotAttrs)
	}
	unnamed := Requirement{Resource: "configmaps", Verb: "get", Namespace: "kube-system"}
	if d, err := reviewer.Allowed(context.Background(), unnamed); err != nil || d.Allowed {
		t.Errorf("Allowed(unnamed) = %+v, %v; want denied against a resourceNames-pinned grant", d, err)
	}
}

// wardenReason is the shape of a real GKE Autopilot denial (#145) —
// the probe must repeat the authorizer's words, never claim a plain
// RBAC gap.
const wardenReason = `GKE Warden authz [denied by managed-namespaces-limitation]: cluster scoped resource "nodes/proxy" is managed and access is denied`

// TestProbe_DenialCarriesAuthorizerReason pins #145: when the SSAR
// response carries a reason, the fatal error repeats it verbatim and
// stops claiming "this ServiceAccount does not have it".
func TestProbe_DenialCarriesAuthorizerReason(t *testing.T) {
	t.Parallel()
	src := &fakeSource{
		name:  "k8s-events",
		scope: ScopeCluster,
		reqs:  []Requirement{{Resource: "events", Verb: "list"}},
	}
	_, err := Probe(context.Background(), scriptedReviewer{
		allowed: map[string]bool{},
		reasons: map[string]string{"list events cluster-wide": wardenReason},
	}, src)
	if err == nil {
		t.Fatal("Probe must fail on the denial")
	}
	if !strings.Contains(err.Error(), wardenReason) {
		t.Errorf("error must carry the authorizer's reason verbatim; got: %v", err)
	}
	if strings.Contains(err.Error(), "this ServiceAccount does not have it") {
		t.Errorf("error claims an RBAC gap despite an authorizer reason: %v", err)
	}
	if !strings.Contains(err.Error(), "platform policy") {
		t.Errorf("error must warn that a platform policy denial cannot be granted away: %v", err)
	}
}

// TestProbe_OptionalRequirementDegrades pins the #145 optional tier:
// an optional denial produces a note and the probe passes — the
// source runs with the dimension disabled instead of not at all.
func TestProbe_OptionalRequirementDegrades(t *testing.T) {
	t.Parallel()
	src := &fakeSource{
		name:  "saturation",
		scope: ScopeCluster,
		reqs: []Requirement{
			{Resource: "pods", Verb: "list"},
			{Resource: "nodes", Subresource: "proxy", Verb: "get", Optional: true},
		},
	}
	notes, err := Probe(context.Background(), scriptedReviewer{
		allowed: map[string]bool{"list pods cluster-wide": true},
		reasons: map[string]string{"get nodes/proxy cluster-wide": wardenReason},
	}, src)
	if err != nil {
		t.Fatalf("optional denial must not fail the probe: %v", err)
	}
	if len(notes) != 1 {
		t.Fatalf("notes = %v, want exactly one degradation note", notes)
	}
	for _, want := range []string{`source "saturation"`, "get nodes/proxy cluster-wide", wardenReason, "dimension disabled"} {
		if !strings.Contains(notes[0], want) {
			t.Errorf("note missing %q: %s", want, notes[0])
		}
	}
}

// TestDenialRemedy pins the two wordings: the classic §11 clause with
// no reason, the authorizer-led clause with one.
func TestDenialRemedy(t *testing.T) {
	t.Parallel()
	if got := DenialRemedy(Decision{}); !strings.Contains(got, "this ServiceAccount does not have it") {
		t.Errorf("no-reason remedy = %q", got)
	}
	got := DenialRemedy(Decision{Reason: wardenReason})
	if !strings.Contains(got, wardenReason) || !strings.Contains(got, "no RBAC grant can satisfy it") {
		t.Errorf("reasoned remedy = %q", got)
	}
	if strings.Contains(got, "this ServiceAccount does not have it") {
		t.Errorf("reasoned remedy still claims an RBAC gap: %q", got)
	}
}
