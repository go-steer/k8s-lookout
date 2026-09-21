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

package leeway

import (
	"strings"
	"testing"
	"time"
)

func TestDomainAvailability_UnavailableReadsUsableNotReady(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a    DomainAvailability
		want bool
	}{{
		name: "a healthy domain",
		a:    DomainAvailability{Nodes: 3, Ready: 3, Usable: 3, Known: true},
		want: false,
	}, {
		name: "one usable node is not an outage",
		a:    DomainAvailability{Nodes: 20, Ready: 1, Usable: 1, Known: true},
		want: false,
	}, {
		name: "every node NotReady",
		a:    DomainAvailability{Nodes: 3, Known: true},
		want: true,
	}, {
		// The half of the exit criterion Ready alone cannot see. Every node
		// answers healthily and holds its running pods; nothing new can land.
		name: "every node Ready and cordoned",
		a:    DomainAvailability{Nodes: 3, Ready: 3, Known: true},
		want: true,
	}, {
		name: "every node gone",
		a:    DomainAvailability{Known: true},
		want: true,
	}, {
		// The reading that makes Known worth a field. Without it a domain the
		// cluster never had is indistinguishable from one that just died, and
		// a typo in --topology-domain-unavailable-keys would report every
		// nonexistent domain as out.
		name: "a domain the cluster does not have",
		a:    DomainAvailability{},
		want: false,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.a.Unavailable(); got != tc.want {
				t.Errorf("Unavailable() = %v, want %v for %+v", got, tc.want, tc.a)
			}
		})
	}
}

// The three causes are the whole reason §2.3 renamed the kind: it states what
// was measured, and the cause states what we think produced it.
func TestDomainAvailability_CauseIsToldApartByWhichCountSurvived(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a    DomainAvailability
		want SuspectedCause
		note string
	}{{
		name: "nodes removed",
		a:    DomainAvailability{Known: true},
		want: CauseConsolidation,
		note: "no nodes remain",
	}, {
		name: "nodes present, none Ready",
		a:    DomainAvailability{Nodes: 4, Known: true},
		want: CauseDomainOutage,
		note: "4 node(s) present, none Ready",
	}, {
		name: "nodes Ready, none schedulable",
		a:    DomainAvailability{Nodes: 4, Ready: 4, Known: true},
		want: CauseTaintExclusion,
		note: "4 node(s) Ready, none schedulable",
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.a.Cause(); got != tc.want {
				t.Errorf("Cause() = %q, want %q", got, tc.want)
			}
			// The note is the census rather than the conclusion, so a reader
			// who disagrees with the cause can see what it was drawn from.
			if got := tc.a.note(); !strings.Contains(got, tc.note) {
				t.Errorf("note() = %q, want it to contain %q", got, tc.note)
			}
		})
	}
}

func TestDomainVerdict_IsTierBAndBreached(t *testing.T) {
	t.Parallel()

	v := DomainVerdict()
	if !v.Breached || v.Tier != TierB {
		t.Errorf("DomainVerdict() = %+v, want a breached Tier B verdict (§2.3's table)", v)
	}
	if v.Severity != TierB.Severity() {
		t.Errorf("Severity = %q, want %q", v.Severity, TierB.Severity())
	}
	// Tier B routes to the wire whatever the Tier C policy says, which is what
	// makes a dead zone reach somebody on a default deployment.
	for _, tierCSignals := range []bool{false, true} {
		if d := v.Route(tierCSignals); !d.Signal {
			t.Errorf("Route(%v) = %+v, want a signal: a domain with no usable node is not a Tier C observation", tierCSignals, d)
		}
	}
}

func TestNewDomainFinding_NamesTheDomainAndCarriesOneRow(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	a := DomainAvailability{Domain: "us-central1-c", Nodes: 6, Known: true}
	f := NewDomainFinding(a, "topology.kubernetes.io/zone", AlertState{FirstSeenAt: first})

	if f.Kind != KindDomainUnavailable {
		t.Errorf("Kind = %q, want %q", f.Kind, KindDomainUnavailable)
	}
	// The subject is the domain, which is the whole point: one signal for the
	// cluster rather than one per workload that drifted because of it.
	want := SubjectRef{Kind: SubjectDomain, Name: "us-central1-c"}
	if f.Subject != want {
		t.Errorf("Subject = %+v, want %+v", f.Subject, want)
	}
	if f.SuspectedCause != CauseDomainOutage {
		t.Errorf("SuspectedCause = %q, want %q for six nodes present and none Ready", f.SuspectedCause, CauseDomainOutage)
	}
	if f.Tier != TierB.String() || f.Severity != TierB.Severity() {
		t.Errorf("tier/severity = %q/%q, want %q/%q", f.Tier, f.Severity, TierB.String(), TierB.Severity())
	}
	if len(f.Domains) != 1 || f.Domains[0].Domain != a.Domain {
		t.Fatalf("Domains = %+v, want exactly one row, for the domain itself", f.Domains)
	}
	if !f.FirstSeenAt.Equal(first) {
		t.Errorf("FirstSeenAt = %v, want the episode's %v", f.FirstSeenAt, first)
	}
	// Score is deliberately zero: every field in it describes a distribution,
	// and this finding has none. A zero drift here means "not applicable".
	if f.Score != (FindingScore{}) {
		t.Errorf("Score = %+v, want the zero value — nothing was placed and nothing was scored", f.Score)
	}
}
