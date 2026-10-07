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
)

// recordingReviewer allows exactly the requirements asked in allowNS and
// records what it was asked.
type recordingReviewer struct {
	allowNS string
	asked   []Requirement
}

func (r *recordingReviewer) Allowed(_ context.Context, req Requirement) (Decision, error) {
	r.asked = append(r.asked, req)
	return Decision{Allowed: req.Namespace == r.allowNS}, nil
}

// testNamespaced is a two-entry discovery: pods are namespaced, nodes are
// not, anything else is unknown.
func testNamespaced(group, resource string) (bool, bool) {
	switch {
	case group == "" && resource == "pods":
		return true, true
	case group == "" && resource == "nodes":
		return false, true
	}
	return false, false
}

func TestNamespaceScopedReviewer_StampsNamespacedResourcesOnly(t *testing.T) {
	r := NewNamespaceScopedReviewer(&recordingReviewer{}, "team-a", testNamespaced)
	tests := []struct {
		name string
		in   Requirement
		want string
	}{
		{"namespaced resource is stamped", Requirement{Resource: "pods", Verb: "list"}, "team-a"},
		{"subresource follows its parent", Requirement{Resource: "pods", Subresource: "log", Verb: "get"}, "team-a"},
		{"cluster-scoped resource stays cluster-wide", Requirement{Resource: "nodes", Verb: "list"}, ""},
		{"unknown resource stays as declared", Requirement{Group: "x.example", Resource: "widgets", Verb: "list"}, ""},
		{"an explicit namespace is the source's own", Requirement{Resource: "pods", Verb: "list", Namespace: "kube-system"}, "kube-system"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Effective(r, tc.in).Namespace; got != tc.want {
				t.Errorf("Effective(%v).Namespace = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestProbe_NamespaceScope pins the degradation path end to end at the probe:
// a namespaced Role passes every namespaced requirement, the node requirement
// is still asked cluster-wide and denied, and the refusal names what was
// really asked rather than the unscoped declaration.
func TestProbe_NamespaceScope(t *testing.T) {
	inner := &recordingReviewer{allowNS: "team-a"}
	r := NewNamespaceScopedReviewer(inner, "team-a", testNamespaced)

	podsOnly := &fakeDeclarer{name: "pods-only", reqs: []Requirement{{Resource: "pods", Verb: "list"}, {Resource: "pods", Verb: "watch"}}}
	if _, err := Probe(context.Background(), r, podsOnly); err != nil {
		t.Fatalf("Probe(pods) under a namespaced Role = %v, want pass", err)
	}
	for _, req := range inner.asked {
		if req.Namespace != "team-a" {
			t.Errorf("asked %v, want every pods requirement asked in team-a", req)
		}
	}

	needsNodes := &fakeDeclarer{name: "needs-nodes", reqs: []Requirement{{Resource: "pods", Verb: "list"}, {Resource: "nodes", Verb: "list"}}}
	_, err := Probe(context.Background(), r, needsNodes)
	var denied *DeniedError
	if !errors.As(err, &denied) {
		t.Fatalf("Probe(nodes) = %v, want a DeniedError", err)
	}
	if denied.Requirement.Resource != "nodes" || denied.Requirement.Namespace != "" {
		t.Errorf("denied %v, want nodes cluster-wide", denied.Requirement)
	}
	if !strings.Contains(err.Error(), "list nodes cluster-wide") {
		t.Errorf("refusal does not name the cluster-wide node list: %v", err)
	}
}

// fakeDeclarer is a Source with declared requirements and nothing else.
type fakeDeclarer struct {
	name string
	reqs []Requirement
}

func (f *fakeDeclarer) Name() string                            { return f.name }
func (f *fakeDeclarer) Scope() Scope                            { return ScopeCluster }
func (f *fakeDeclarer) Run(context.Context, func(Signal)) error { return nil }
func (f *fakeDeclarer) RequiredAccess() []Requirement           { return f.reqs }
