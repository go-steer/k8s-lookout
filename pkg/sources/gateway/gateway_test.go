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

package gateway

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

var baseTime = time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)

// cond builds one status-condition map as unstructured decoding yields
// it (numbers as float64, timestamps as RFC3339 strings).
func cond(ctype, status, reason string, transitioned time.Time, observedGen int64) map[string]any {
	return map[string]any{
		"type":               ctype,
		"status":             status,
		"reason":             reason,
		"message":            ctype + " " + reason,
		"lastTransitionTime": transitioned.UTC().Format(time.RFC3339),
		"observedGeneration": float64(observedGen),
	}
}

// gw builds a Gateway unstructured with top-level conditions.
func gw(uid, name string, gen int64, conds ...map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("gateway.networking.k8s.io/v1")
	u.SetKind("Gateway")
	u.SetNamespace("infra")
	u.SetName(name)
	u.SetUID(types.UID(uid))
	u.SetGeneration(gen)
	_ = unstructured.SetNestedSlice(u.Object, condSlice(conds), "status", "conditions")
	return u
}

// withListener appends a listener with its own conditions to a Gateway.
func withListener(u *unstructured.Unstructured, name string, conds ...map[string]any) *unstructured.Unstructured {
	listeners := nestedSlice(u.Object, "status", "listeners")
	listeners = append(listeners, map[string]any{"name": name, "conditions": condSlice(conds)})
	_ = unstructured.SetNestedSlice(u.Object, listeners, "status", "listeners")
	return u
}

// httproute builds an HTTPRoute unstructured with one parent carrying
// the given conditions.
func httproute(uid, name string, gen int64, parentConds ...map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("gateway.networking.k8s.io/v1")
	u.SetKind("HTTPRoute")
	u.SetNamespace("apps")
	u.SetName(name)
	u.SetUID(types.UID(uid))
	u.SetGeneration(gen)
	parents := []any{map[string]any{"conditions": condSlice(parentConds)}}
	_ = unstructured.SetNestedSlice(u.Object, parents, "status", "parents")
	return u
}

func condSlice(conds []map[string]any) []any {
	cs := make([]any, len(conds))
	for i, c := range conds {
		cs[i] = c
	}
	return cs
}

// newTestSource returns an armed source with a fixed clock and an
// emission sink.
func newTestSource(t *testing.T, now time.Time) (*Source, *[]engine.Signal) {
	t.Helper()
	s := New(nil, nil, DefaultConfig())
	var out []engine.Signal
	s.now = func() time.Time { return now }
	s.emit = func(sig engine.Signal) { out = append(out, sig) }
	s.arm()
	return s, &out
}

func kindsOf(sigs []engine.Signal) []string {
	out := make([]string, len(sigs))
	for i, s := range sigs {
		out[i] = s.Kind
	}
	return out
}

func TestEvaluate_ConditionMapping(t *testing.T) {
	s := New(nil, nil, DefaultConfig())
	now := baseTime
	tests := []struct {
		name string
		u    *unstructured.Unstructured
		kind string
		want []string // kinds expected in the failing set
	}{
		{"gateway programmed false", gw("g1", "gw", 1, cond("Programmed", "False", "Invalid", now, 1)), "Gateway", []string{KindProgrammingFailed}},
		{"gateway accepted false", gw("g2", "gw", 1, cond("Accepted", "False", "UnsupportedValue", now, 1)), "Gateway", []string{KindRouteRejected}},
		{"gateway both", gw("g3", "gw", 1, cond("Programmed", "False", "Invalid", now, 1), cond("Accepted", "False", "Invalid", now, 1)), "Gateway", []string{KindProgrammingFailed, KindRouteRejected}},
		{"gateway healthy", gw("g4", "gw", 1, cond("Programmed", "True", "Programmed", now, 1), cond("Accepted", "True", "Accepted", now, 1)), "Gateway", nil},
		{"pending is not a failure", gw("g5", "gw", 1, cond("Programmed", "False", "Pending", now, 1)), "Gateway", nil},
		{"stale generation skipped", gw("g6", "gw", 3, cond("Programmed", "False", "Invalid", now, 1)), "Gateway", nil},
		{"listener resolvedrefs", withListener(gw("g7", "gw", 1), "https", cond("ResolvedRefs", "False", "InvalidCertificateRef", now, 1)), "Gateway", []string{KindRouteRejected}},
		{"httproute accepted false", httproute("h1", "route", 1, cond("Accepted", "False", "NoMatchingParent", now, 1)), "HTTPRoute", []string{KindRouteRejected}},
		{"httproute resolvedrefs false", httproute("h2", "route", 1, cond("ResolvedRefs", "False", "BackendNotFound", now, 1)), "HTTPRoute", []string{KindRouteRejected}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			failing := s.evaluate(tc.u, tc.kind, now)
			got := map[string]bool{}
			for k := range failing {
				got[k] = true
			}
			if len(got) != len(tc.want) {
				t.Fatalf("failing kinds = %v, want %v", got, tc.want)
			}
			for _, w := range tc.want {
				if !got[w] {
					t.Errorf("missing expected kind %q (got %v)", w, got)
				}
			}
		})
	}
}

func TestGraceWindow(t *testing.T) {
	s, out := newTestSource(t, baseTime)
	// Programmed=False only 1m ago: inside the 5m grace window, no fire.
	s.onObject(gw("g1", "store-gw", 1, cond("Programmed", "False", "AddressNotAssigned", baseTime.Add(-1*time.Minute), 1)), "Gateway")
	if len(*out) != 0 {
		t.Fatalf("fired inside grace window: %v", kindsOf(*out))
	}
	// The sweep at +5m (failure now 6m old) crosses the grace window.
	s.now = func() time.Time { return baseTime.Add(5 * time.Minute) }
	got := s.sweep(baseTime.Add(5 * time.Minute))
	if len(got) != 1 || got[0].Kind != KindProgrammingFailed {
		t.Fatalf("grace crossing = %v, want one programming_failed", kindsOf(got))
	}
	if got[0].Severity != engine.SeverityWarning {
		t.Errorf("severity = %v, want warning", got[0].Severity)
	}
}

func TestSustainedFailureFiresOnceThenClears(t *testing.T) {
	now := baseTime
	s, out := newTestSource(t, now)
	// A failure already 10m old fires immediately (level signal, not
	// edge — a long-broken Gateway present at the LIST is reported).
	broken := gw("g1", "prod-gw", 2, cond("Accepted", "False", "InvalidParameters", now.Add(-10*time.Minute), 2))
	s.onObject(broken, "Gateway")
	if k := kindsOf(*out); len(k) != 1 || k[0] != KindRouteRejected {
		t.Fatalf("first observation = %v, want one route_rejected", k)
	}
	// Re-observing the same ongoing failure does not re-fire (latch).
	*out = nil
	s.onObject(broken, "Gateway")
	if len(*out) != 0 {
		t.Fatalf("re-fired an already-fired failure: %v", kindsOf(*out))
	}
	// Clearance while still failing: judged, but not cleared.
	inc := engine.Incident{Key: engine.EventKey{UID: "g1", Reason: reasonOf(KindRouteRejected)}}
	if verdict, ok := (clearance{s}).Clearance(inc); !ok || verdict.Cleared {
		t.Fatalf("clearance while failing = (%+v, %v), want judged+not-cleared", verdict, ok)
	}
	// Condition recovers → clearance reports recovered at the recovery instant.
	healthy := gw("g1", "prod-gw", 2, cond("Accepted", "True", "Accepted", now, 2))
	s.onObject(healthy, "Gateway")
	verdict, ok := (clearance{s}).Clearance(inc)
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Fatalf("clearance after recovery = (%+v, %v), want cleared+recovered", verdict, ok)
	}
	if !verdict.StableSince.Equal(now) {
		t.Errorf("StableSince = %v, want %v (recovery instant)", verdict.StableSince, now)
	}
}

func TestClearanceObjectDeleted(t *testing.T) {
	s, _ := newTestSource(t, baseTime)
	s.onObject(gw("g1", "gw", 1, cond("Programmed", "False", "Invalid", baseTime.Add(-time.Hour), 1)), "Gateway")
	s.onDelete(gw("g1", "gw", 1))
	inc := engine.Incident{Key: engine.EventKey{UID: "g1", Reason: reasonOf(KindProgrammingFailed)}}
	verdict, ok := (clearance{s}).Clearance(inc)
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionObjectDeleted {
		t.Fatalf("clearance after delete = (%+v, %v), want cleared+object_deleted", verdict, ok)
	}
}

func TestClearanceDeclinesForeignReasonAndBeforeSync(t *testing.T) {
	// Foreign reason: never judged, even after sync.
	s := New(nil, nil, DefaultConfig())
	s.arm()
	if _, ok := (clearance{s}).Clearance(engine.Incident{Key: engine.EventKey{UID: "x", Reason: "some_other"}}); ok {
		t.Error("judged a foreign reason")
	}
	// Before sync: declines even for our own reasons.
	s2 := New(nil, nil, DefaultConfig())
	inc := engine.Incident{Key: engine.EventKey{UID: "g1", Reason: reasonOf(KindRouteRejected)}}
	if _, ok := (clearance{s2}).Clearance(inc); ok {
		t.Error("judged before cache sync")
	}
}

func TestNotArmedDoesNotFire(t *testing.T) {
	s := New(nil, nil, DefaultConfig())
	s.now = func() time.Time { return baseTime }
	var out []engine.Signal
	s.emit = func(sig engine.Signal) { out = append(out, sig) }
	// Not armed: the initial LIST records state without firing.
	s.onObject(gw("g1", "gw", 1, cond("Programmed", "False", "Invalid", baseTime.Add(-time.Hour), 1)), "Gateway")
	if len(out) != 0 {
		t.Fatalf("fired before arming: %v", kindsOf(out))
	}
	// Arming + sweep then reports the pre-existing sustained failure.
	s.arm()
	if got := s.sweep(baseTime); len(got) != 1 {
		t.Fatalf("post-arm sweep = %v, want one signal", kindsOf(got))
	}
}

func TestGatewayAPIServed(t *testing.T) {
	served := fake.NewSimpleClientset()
	served.Resources = []*metav1.APIResourceList{{
		GroupVersion: "gateway.networking.k8s.io/v1",
		APIResources: []metav1.APIResource{{Name: "gateways"}, {Name: "httproutes"}},
	}}
	if !GatewayAPIServed(served) {
		t.Error("GatewayAPIServed = false with the CRDs present")
	}
	absent := fake.NewSimpleClientset()
	if GatewayAPIServed(absent) {
		t.Error("GatewayAPIServed = true with no Gateway API group")
	}
}

func TestRequiredAccess(t *testing.T) {
	seen := map[string]bool{}
	for _, r := range New(nil, nil, DefaultConfig()).RequiredAccess() {
		seen[r.Group+"/"+r.Resource+"/"+r.Verb] = true
	}
	for _, want := range []string{
		"gateway.networking.k8s.io/gateways/list",
		"gateway.networking.k8s.io/gateways/watch",
		"gateway.networking.k8s.io/httproutes/list",
		"gateway.networking.k8s.io/httproutes/watch",
	} {
		if !seen[want] {
			t.Errorf("missing required-access declaration %q (have %v)", want, seen)
		}
	}
}
