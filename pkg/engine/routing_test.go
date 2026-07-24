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

package engine

import (
	"strings"
	"testing"
)

// TestRouteFor pins the §7.7 normative routing table plus the
// fail-toward-paging fallback for unclassifiable severities.
func TestRouteFor(t *testing.T) {
	t.Parallel()
	cases := []struct {
		sev  Severity
		want Route
	}{
		{SeverityCritical, RoutePerIncident},
		{SeverityWarning, RouteWatchboard},
		{SeverityInfo, RouteStore},
		{Severity(""), RoutePerIncident},        // zero value: fail toward paging
		{Severity("blocker"), RoutePerIncident}, // unknown class: fail toward paging
	}
	for _, c := range cases {
		if got := RouteFor(c.sev); got != c.want {
			t.Errorf("RouteFor(%q) = %v, want %v", c.sev, got, c.want)
		}
	}
}

func TestRouteString(t *testing.T) {
	t.Parallel()
	for r, want := range map[Route]string{
		RoutePerIncident: "per-incident",
		RouteWatchboard:  "watchboard",
		RouteStore:       "store",
	} {
		if got := r.String(); got != want {
			t.Errorf("Route(%d).String() = %q, want %q", r, got, want)
		}
	}
}

// TestRoutingPolicyClassify: config override wins over the
// source-stamped default; without an override the stamp stands; a
// missing/invalid stamp classifies critical.
func TestRoutingPolicyClassify(t *testing.T) {
	t.Parallel()
	p := NewRoutingPolicy(map[string]Severity{
		"k8s-event":                 SeverityWarning,
		"objectstate.restart_burst": SeverityInfo,
	})

	sig := Signal{Kind: "k8s-event", Severity: SeverityCritical}
	if got := p.Classify(sig); got != SeverityWarning {
		t.Errorf("override: Classify = %q, want warning", got)
	}
	sig = Signal{Kind: "objectstate.restart_burst", Severity: SeverityWarning}
	if got := p.Classify(sig); got != SeverityInfo {
		t.Errorf("override: Classify = %q, want info", got)
	}
	sig = Signal{Kind: "objectstate.node_notready", Severity: SeverityCritical}
	if got := p.Classify(sig); got != SeverityCritical {
		t.Errorf("no override: Classify = %q, want the source stamp (critical)", got)
	}
	sig = Signal{Kind: "custom.kind"} // source forgot to classify
	if got := p.Classify(sig); got != SeverityCritical {
		t.Errorf("unstamped: Classify = %q, want critical (fail toward paging)", got)
	}
	sig = Signal{Kind: "custom.kind", Severity: Severity("bogus")}
	if got := p.Classify(sig); got != SeverityCritical {
		t.Errorf("invalid stamp: Classify = %q, want critical (fail toward paging)", got)
	}

	// A nil policy and an empty policy both defer to the stamp.
	var nilPolicy *RoutingPolicy
	sig = Signal{Kind: "k8s-event", Severity: SeverityWarning}
	if got := nilPolicy.Classify(sig); got != SeverityWarning {
		t.Errorf("nil policy: Classify = %q, want the stamp", got)
	}
	if got := NewRoutingPolicy(nil).Classify(sig); got != SeverityWarning {
		t.Errorf("empty policy: Classify = %q, want the stamp", got)
	}
}

func TestParseSeverityOverrides(t *testing.T) {
	t.Parallel()

	// Additive across occurrences, comma-separated within one,
	// whitespace-tolerant.
	got, err := ParseSeverityOverrides([]string{
		"k8s-event=warning, objectstate.restart_burst=info",
		"objectstate.node_flapping=critical",
	})
	if err != nil {
		t.Fatalf("ParseSeverityOverrides: %v", err)
	}
	want := map[string]Severity{
		"k8s-event":                 SeverityWarning,
		"objectstate.restart_burst": SeverityInfo,
		"objectstate.node_flapping": SeverityCritical,
	}
	if len(got) != len(want) {
		t.Fatalf("parsed %d overrides, want %d: %v", len(got), len(want), got)
	}
	for kind, sev := range want {
		if got[kind] != sev {
			t.Errorf("override[%q] = %q, want %q", kind, got[kind], sev)
		}
	}

	// Empty input parses to no overrides.
	if m, err := ParseSeverityOverrides(nil); err != nil || m != nil {
		t.Errorf("ParseSeverityOverrides(nil) = (%v, %v), want (nil, nil)", m, err)
	}

	rejected := []struct {
		entries []string
		wantMsg string
	}{
		{[]string{"k8s-event"}, "kind=level"},                 // missing '='
		{[]string{"=critical"}, "kind=level"},                 // empty kind
		{[]string{"k8s-event=fatal"}, "level must be one of"}, // unknown level
		// A repeated kind is ambiguous config, even at the same level.
		{[]string{"k8s-event=warning", "k8s-event=info"}, "overridden twice"},
		{[]string{"k8s-event=warning,k8s-event=warning"}, "overridden twice"},
	}
	for _, c := range rejected {
		_, err := ParseSeverityOverrides(c.entries)
		if err == nil {
			t.Errorf("ParseSeverityOverrides(%v): want error", c.entries)
			continue
		}
		if !strings.Contains(err.Error(), c.wantMsg) {
			t.Errorf("ParseSeverityOverrides(%v) error %q, want it to mention %q", c.entries, err, c.wantMsg)
		}
	}
}
