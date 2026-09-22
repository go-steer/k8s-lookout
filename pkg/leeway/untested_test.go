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
	"slices"
	"testing"
	"time"
)

// A deployment with every sibling source running says nothing about untested
// causes, because there are none: the ladder's silence really does mean the
// hypothesis was tested and lost. This is the normal case and the reason
// Untested is omitempty on the wire.
func TestAttribute_AFullSourceSetLeavesNothingUntested(t *testing.T) {
	a := attribute(drifted(), Evidence{Domains: map[Domain]DomainFacts{
		zoneC: {ReadyNodes: 3, NotReadyNodes: 9},
	}})
	if a.Cause != CauseDomainOutage {
		t.Fatalf("Cause = %q, want %q", a.Cause, CauseDomainOutage)
	}
	if len(a.Untested) != 0 {
		t.Errorf("Untested = %v on a deployment that could ask every question", a.Untested)
	}
}

// The distinction #474 exists for: an unexplained finding on a deployment
// missing two sources must not read as "we looked and found nothing".
func TestAttribute_AMissingSourceIsNotARuledOutCause(t *testing.T) {
	gaps := EvidenceGaps{Capacity: true, Consolidation: true, RolloutCompletion: true}
	a := attribute(drifted(), Evidence{Unavailable: gaps})
	if a.Cause != CauseUnknown {
		t.Fatalf("Cause = %q, want %q", a.Cause, CauseUnknown)
	}
	want := []SuspectedCause{CauseConsolidation, CauseRolloutBias}
	if !slices.Equal(a.Untested, want) {
		t.Errorf("Untested = %v, want %v", a.Untested, want)
	}
}

// Capacity is not on the list even when it is missing, and that is deliberate:
// domain_capacity_shortfall is decided on the node census, which topology
// drift owns. Losing `capacity` costs the finding its corroborating pod
// evidence, not its ability to reach the cause.
func TestAttribute_AMissingCapacitySourceStillReachesTheShortfall(t *testing.T) {
	a := attribute(drifted(), Evidence{
		Domains:     map[Domain]DomainFacts{zoneC: {ReadyNodes: 3, PeakReadyNodes: 12, LostAt: cago(time.Hour)}},
		Unavailable: EvidenceGaps{Capacity: true},
	})
	if a.Cause != CauseCapacityShortfall {
		t.Fatalf("Cause = %q, want %q", a.Cause, CauseCapacityShortfall)
	}
	if len(a.Untested) != 0 {
		t.Errorf("Untested = %v; a missing capacity source does not make a cause untestable", a.Untested)
	}
}

// Only a MORE specific cause is worth reporting. A finding that landed on an
// outage has not tested rollout bias either, but rollout bias ranks below the
// answer it found — listing it would make every finding look uncertain while
// telling the reader nothing they would act on.
func TestAttribute_OnlyCausesMoreSpecificThanTheWinnerAreReported(t *testing.T) {
	gaps := EvidenceGaps{Consolidation: true, RolloutCompletion: true}

	outage := attribute(drifted(), Evidence{
		Domains:     map[Domain]DomainFacts{zoneC: {ReadyNodes: 3, NotReadyNodes: 9}},
		Unavailable: gaps,
	})
	if outage.Cause != CauseDomainOutage {
		t.Fatalf("Cause = %q, want %q", outage.Cause, CauseDomainOutage)
	}
	if len(outage.Untested) != 0 {
		t.Errorf("Untested = %v; nothing outranks an outage", outage.Untested)
	}

	// A taint ranks below consolidation and above rollout bias, so exactly one
	// of the two gaps survives the filter.
	tainted := attribute(drifted(), Evidence{
		Domains:     map[Domain]DomainFacts{zoneC: {ReadyNodes: 12, PeakReadyNodes: 12, TaintedAt: cago(5 * time.Minute)}},
		Unavailable: gaps,
	})
	if tainted.Cause != CauseTaintExclusion {
		t.Fatalf("Cause = %q, want %q", tainted.Cause, CauseTaintExclusion)
	}
	if want := []SuspectedCause{CauseConsolidation}; !slices.Equal(tainted.Untested, want) {
		t.Errorf("Untested = %v, want %v", tainted.Untested, want)
	}
}

// Nil scores is the one path that returns before the ladder runs. It still has
// to answer the question, because a subject that lost its scores between the
// sweep and the emit is exactly where an unexplained finding turns up.
func TestAttribute_ANilScoresStillReportsWhatWasUntested(t *testing.T) {
	var s *Scores
	a := attribute(s, Evidence{Unavailable: EvidenceGaps{Consolidation: true}})
	if a.Cause != CauseUnknown {
		t.Fatalf("Cause = %q, want %q", a.Cause, CauseUnknown)
	}
	if want := []SuspectedCause{CauseConsolidation}; !slices.Equal(a.Untested, want) {
		t.Errorf("Untested = %v, want %v", a.Untested, want)
	}
}

// causeRank orders the same ladder Attribute's switch walks, and the two are
// only correct together: a cause added to the switch without a rank would sort
// as CauseUnknown and start appearing as an open question under every finding.
func TestCauseRank_MatchesTheLadderOrder(t *testing.T) {
	ladder := []SuspectedCause{
		CauseDomainOutage,
		CauseConsolidation,
		CauseTaintExclusion,
		CauseCapacityShortfall,
		CauseVolumePinning,
		CauseRolloutBias,
		CauseConstraintIgnored,
		CauseUnknown,
	}
	for i := 1; i < len(ladder); i++ {
		if causeRank(ladder[i-1]) >= causeRank(ladder[i]) {
			t.Errorf("%q ranks %d and %q ranks %d, want strictly increasing down the ladder",
				ladder[i-1], causeRank(ladder[i-1]), ladder[i], causeRank(ladder[i]))
		}
	}
	if causeRank(SuspectedCause("a cause from the future")) != causeRank(CauseUnknown) {
		t.Error("an unrecognised cause must rank with CauseUnknown, not ahead of the ladder")
	}
}

// The finding is where any of this is visible, and the field is deliberately
// outside the fingerprint: turning a source on must not re-identify every open
// episode in the cluster.
func TestNewFinding_CarriesTheUntestedCauses(t *testing.T) {
	in := FindingInput{
		Subject:     SubjectRef{Kind: SubjectDeployment, Namespace: "shop", Name: "web"},
		TopologyKey: "topology.kubernetes.io/zone",
		Scores:      drifted(),
		Verdict:     Verdict{Tier: TierA, Severity: "warning"},
		Attribution: Attribution{Cause: CauseUnknown, Untested: []SuspectedCause{CauseConsolidation}},
	}
	f := NewFinding(in)
	if want := []SuspectedCause{CauseConsolidation}; !slices.Equal(f.UntestedCauses, want) {
		t.Errorf("UntestedCauses = %v, want %v", f.UntestedCauses, want)
	}

	in.Attribution.Untested = nil
	if got := NewFinding(in).UntestedCauses; len(got) != 0 {
		t.Errorf("UntestedCauses = %v, want none on a fully-sourced deployment", got)
	}
}
