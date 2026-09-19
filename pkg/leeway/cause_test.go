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
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
)

// A fixed clock. Attribution is pure, so every case below is exact.
var cnow = time.Date(2026, 9, 19, 14, 0, 0, 0, time.UTC)

func cago(d time.Duration) time.Time { return cnow.Add(-d) }

const (
	zoneA Domain = "us-east-1a"
	zoneB Domain = "us-east-1b"
	zoneC Domain = "us-east-1c"
)

// zones builds the scores attribution reads, computing the two derived fields
// it uses by hand rather than by calling Score. The point is that these cases
// exercise the rules engine and not the arithmetic upstream of it: a change to
// apportionment must not be able to silently re-attribute a cause.
func zones(actual, expected []int64) *Scores {
	s := &Scores{Domains: []Domain{zoneA, zoneB, zoneC}, Actual: actual, Expected: expected}
	minA, maxA := actual[0], actual[0]
	for _, a := range actual {
		s.Total += a
		minA = min(minA, a)
		maxA = max(maxA, a)
	}
	s.ObservedSkew = maxA - minA
	for i := range actual {
		if d := actual[i] - expected[i]; d > 0 {
			s.Relocation += d
		}
	}
	s.Drift = float64(s.Relocation) / float64(s.Total)
	return s
}

// drifted is the shape every case varies: zone a holds double its share, zone
// c almost none. Four objects must move.
func drifted() *Scores { return zones([]int64{8, 3, 1}, []int64{4, 4, 4}) }

func attribute(s *Scores, ev Evidence) Attribution {
	return s.Attribute(nil, ev, cnow, CauseConfig{})
}

func hasFactor(a Attribution, substr string) bool {
	return slices.ContainsFunc(a.Factors, func(f string) bool { return strings.Contains(f, substr) })
}

func TestCauseConfig_DefaultsAreTheDesignsNumbers(t *testing.T) {
	c := DefaultCauseConfig()
	if c.Window != 2*time.Hour || c.SettleWindow != 10*time.Minute || c.PinnedShare != 0.5 {
		t.Fatalf("DefaultCauseConfig() = %+v", c)
	}
	if got := (CauseConfig{}).normalize(); got != c {
		t.Errorf("a zero config normalizes to %+v, want the defaults %+v", got, c)
	}
}

func TestCauseConfig_NormalizeKeepsWhatWasSet(t *testing.T) {
	in := CauseConfig{Window: time.Hour, SettleWindow: time.Minute, PinnedShare: 0.9}
	if got := in.normalize(); got != in {
		t.Errorf("normalize(%+v) = %+v, want it unchanged", in, got)
	}
}

func TestAttribute_NoEvidenceIsUnknown(t *testing.T) {
	a := attribute(drifted(), Evidence{})
	if a.Cause != CauseUnknown {
		t.Errorf("Cause = %q, want %q", a.Cause, CauseUnknown)
	}
	// Even an unattributed finding reports what was ruled out.
	if !hasFactor(a, "0 pods pinned by zonal volumes") {
		t.Errorf("Factors = %q, want the ruled-out pinning line", a.Factors)
	}
}

func TestAttribute_ANilScoresIsUnknownAndDoesNotPanic(t *testing.T) {
	var s *Scores
	if a := attribute(s, Evidence{}); a.Cause != CauseUnknown || len(a.Factors) != 0 {
		t.Errorf("Attribute on nil scores = %+v", a)
	}
}

// --- domain_outage ----------------------------------------------------------

func TestAttribute_AMostlyNotReadyUnderFilledDomainIsAnOutage(t *testing.T) {
	a := attribute(drifted(), Evidence{Domains: map[Domain]DomainFacts{
		zoneC: {ReadyNodes: 3, NotReadyNodes: 9},
	}})
	if a.Cause != CauseDomainOutage {
		t.Fatalf("Cause = %q, want %q", a.Cause, CauseDomainOutage)
	}
	if !hasFactor(a, "us-east-1c has 9 NotReady node(s)") {
		t.Errorf("Factors = %q, want the NotReady census", a.Factors)
	}
}

func TestAttribute_ANotReadyNodeNeedsNoHistoryToBeAnOutage(t *testing.T) {
	// A process that started during the outage has no peak to compare
	// against; the census is the only evidence left and it is enough.
	a := attribute(drifted(), Evidence{Domains: map[Domain]DomainFacts{
		zoneC: {ReadyNodes: 0, NotReadyNodes: 12, PeakReadyNodes: 0},
	}})
	if a.Cause != CauseDomainOutage {
		t.Errorf("Cause = %q, want %q", a.Cause, CauseDomainOutage)
	}
}

func TestAttribute_AMinorityOfNotReadyNodesIsNotAnOutage(t *testing.T) {
	a := attribute(drifted(), Evidence{Domains: map[Domain]DomainFacts{
		zoneC: {ReadyNodes: 11, NotReadyNodes: 1},
	}})
	if a.Cause == CauseDomainOutage {
		t.Errorf("Cause = %q: one broken node out of twelve is not an outage", a.Cause)
	}
	if !hasFactor(a, "us-east-1c has 1 NotReady node(s)") {
		t.Errorf("Factors = %q: it is still worth reporting", a.Factors)
	}
}

func TestAttribute_ABrokenOverFilledDomainIsNotTheCauseOfDrift(t *testing.T) {
	// Zone a holds twice its share. Nodes failing there would push objects
	// away, not draw them in, so it does not explain this distribution.
	a := attribute(drifted(), Evidence{Domains: map[Domain]DomainFacts{
		zoneA: {ReadyNodes: 2, NotReadyNodes: 10},
	}})
	if a.Cause == CauseDomainOutage {
		t.Errorf("Cause = %q, want the over-filled side to be ignored", a.Cause)
	}
}

// --- domain_capacity_shortfall ---------------------------------------------

func TestAttribute_TheDesignsWorkedExample(t *testing.T) {
	// §8.5 verbatim: a fell 12 → 3 with nothing NotReady, two pods pending,
	// nothing pinned. Nine nodes left; they did not break.
	lost := cago(2*time.Hour - time.Minute)
	s := drifted()
	a := s.Attribute(nil, Evidence{
		Domains: map[Domain]DomainFacts{
			zoneA: {ReadyNodes: 12, PeakReadyNodes: 12},
			zoneB: {ReadyNodes: 11, PeakReadyNodes: 11},
			zoneC: {ReadyNodes: 3, PeakReadyNodes: 12, LostAt: lost},
		},
		InsufficientResource: 2,
		SchedulingMessage:    "0/26 nodes available: 9 Insufficient cpu",
	}, cnow, CauseConfig{})

	if a.Cause != CauseCapacityShortfall {
		t.Fatalf("Cause = %q, want %q", a.Cause, CauseCapacityShortfall)
	}
	want := []string{
		"us-east-1c ready node count fell 12 → 3 at " + lost.Format(time.RFC3339),
		"2 pods Pending with FailedScheduling: 0/26 nodes available: 9 Insufficient cpu",
		"0 pods pinned by zonal volumes",
	}
	if !slices.Equal(a.Factors, want) {
		t.Errorf("Factors =\n%q\nwant\n%q", a.Factors, want)
	}
	if got := a.Notes[2]; got != "9 nodes lost in last 2h" {
		t.Errorf("Notes[c] = %q, want the design's %q", got, "9 nodes lost in last 2h")
	}
	if a.Notes[0] != "" || a.Notes[1] != "" {
		t.Errorf("Notes = %q, want the unaffected domains silent", a.Notes)
	}
}

func TestAttribute_AClusterThatWasAlwaysTooSmallIsStillAShortfall(t *testing.T) {
	a := attribute(drifted(), Evidence{InsufficientResource: 2})
	if a.Cause != CauseCapacityShortfall {
		t.Errorf("Cause = %q, want %q with no ready-node fall to point at", a.Cause, CauseCapacityShortfall)
	}
}

func TestAttribute_AScaledDownZoneIsAShortfallWithNoPendingPods(t *testing.T) {
	a := attribute(drifted(), Evidence{Domains: map[Domain]DomainFacts{
		zoneC: {ReadyNodes: 3, PeakReadyNodes: 12, LostAt: cago(time.Hour)},
	}})
	if a.Cause != CauseCapacityShortfall {
		t.Errorf("Cause = %q, want %q", a.Cause, CauseCapacityShortfall)
	}
}

func TestAttribute_PendingPodsWithEveryDomainAtItsShareExplainNothing(t *testing.T) {
	even := zones([]int64{4, 4, 4}, []int64{4, 4, 4})
	if a := attribute(even, Evidence{InsufficientResource: 9}); a.Cause != CauseUnknown {
		t.Errorf("Cause = %q, want %q: there is no under-filled domain to blame", a.Cause, CauseUnknown)
	}
}

func TestAttribute_AStaleFallIsOutsideTheWindow(t *testing.T) {
	ev := Evidence{Domains: map[Domain]DomainFacts{
		zoneC: {ReadyNodes: 3, PeakReadyNodes: 12, LostAt: cago(3 * time.Hour)},
	}}
	a := attribute(drifted(), ev)
	if a.Cause != CauseUnknown {
		t.Errorf("Cause = %q, want %q: a fall three hours ago is not the cause of drift now", a.Cause, CauseUnknown)
	}
	if hasFactor(a, "fell") {
		t.Errorf("Factors = %q, want no stale fall reported", a.Factors)
	}
	if a.Notes[2] != "" {
		t.Errorf("Notes[c] = %q, want no note for a fall outside the window", a.Notes[2])
	}
}

// --- the specific mechanisms outrank the catch-all --------------------------

func TestAttribute_ConsolidationOutranksTheShortfallItLooksLike(t *testing.T) {
	// Identical ready-node fall to the scaled-down case above. The only
	// difference is that something told us who removed the nodes.
	at := cago(20 * time.Minute)
	a := attribute(drifted(), Evidence{Domains: map[Domain]DomainFacts{
		zoneC: {ReadyNodes: 3, PeakReadyNodes: 12, LostAt: cago(time.Hour), ConsolidatedAt: at},
	}})
	if a.Cause != CauseConsolidation {
		t.Fatalf("Cause = %q, want %q", a.Cause, CauseConsolidation)
	}
	if !hasFactor(a, "us-east-1c lost a node to autoscaler consolidation at "+at.Format(time.RFC3339)) {
		t.Errorf("Factors = %q, want the consolidation line", a.Factors)
	}
}

func TestAttribute_ANewTaintExcludesADomain(t *testing.T) {
	at := cago(5 * time.Minute)
	a := attribute(drifted(), Evidence{Domains: map[Domain]DomainFacts{
		zoneC: {ReadyNodes: 12, PeakReadyNodes: 12, TaintedAt: at},
	}})
	if a.Cause != CauseTaintExclusion {
		t.Fatalf("Cause = %q, want %q", a.Cause, CauseTaintExclusion)
	}
	if !hasFactor(a, "us-east-1c became ineligible via a taint at "+at.Format(time.RFC3339)) {
		t.Errorf("Factors = %q, want the taint line", a.Factors)
	}
}

func TestAttribute_AnOldTaintIsNotTheCause(t *testing.T) {
	a := attribute(drifted(), Evidence{Domains: map[Domain]DomainFacts{
		zoneC: {ReadyNodes: 12, PeakReadyNodes: 12, TaintedAt: cago(9 * time.Hour)},
	}})
	if a.Cause != CauseUnknown {
		t.Errorf("Cause = %q, want %q: a taint from this morning is the status quo", a.Cause, CauseUnknown)
	}
}

func TestAttribute_AnOutageOutranksEveryOtherMechanism(t *testing.T) {
	a := attribute(drifted(), Evidence{
		Domains: map[Domain]DomainFacts{
			zoneC: {
				ReadyNodes: 1, NotReadyNodes: 11, PeakReadyNodes: 12,
				LostAt:         cago(10 * time.Minute),
				TaintedAt:      cago(10 * time.Minute),
				ConsolidatedAt: cago(10 * time.Minute),
			},
		},
		InsufficientResource: 4,
	})
	if a.Cause != CauseDomainOutage {
		t.Errorf("Cause = %q, want %q", a.Cause, CauseDomainOutage)
	}
}

// --- volume_pinning ---------------------------------------------------------

func TestAttribute_MostOfTheRelocationBeingUnmovableIsPinning(t *testing.T) {
	// Four objects must move and two of them cannot.
	a := attribute(drifted(), Evidence{
		ZonalVolumes: true,
		Domains:      map[Domain]DomainFacts{zoneA: {ReadyNodes: 12, Pinned: 2}},
	})
	if a.Cause != CauseVolumePinning {
		t.Fatalf("Cause = %q, want %q", a.Cause, CauseVolumePinning)
	}
	if !hasFactor(a, "2 pods pinned by zonal volumes (subject claims zone-bound volumes)") {
		t.Errorf("Factors = %q, want the pinning line with its corroboration", a.Factors)
	}
}

func TestAttribute_AFewPinnedPodsAmongManyAreNotTheCause(t *testing.T) {
	a := attribute(drifted(), Evidence{
		Domains: map[Domain]DomainFacts{zoneA: {ReadyNodes: 12, Pinned: 1}},
	})
	if a.Cause == CauseVolumePinning {
		t.Errorf("Cause = %q: one of four immovable leaves three that can move", a.Cause)
	}
}

func TestAttribute_PinningIsReadOffTheOverFilledSide(t *testing.T) {
	// Eight pinned objects, all of them in the domain that is already short.
	// They are not what is keeping the surplus where it is.
	a := attribute(drifted(), Evidence{
		ZonalVolumes: true,
		Domains:      map[Domain]DomainFacts{zoneC: {ReadyNodes: 12, Pinned: 8}},
	})
	if a.Cause == CauseVolumePinning {
		t.Errorf("Cause = %q, want pinning in an under-filled domain to be ignored", a.Cause)
	}
	if !hasFactor(a, "0 pods pinned by zonal volumes") {
		t.Errorf("Factors = %q, want the surplus-side count of zero", a.Factors)
	}
}

func TestAttribute_NothingNeedsToMoveSoNothingIsPinned(t *testing.T) {
	even := zones([]int64{4, 4, 4}, []int64{4, 4, 4})
	a := attribute(even, Evidence{ZonalVolumes: true})
	if a.Cause == CauseVolumePinning {
		t.Errorf("Cause = %q: a relocation distance of zero cannot be obstructed", a.Cause)
	}
}

// --- rollout_bias -----------------------------------------------------------

func TestAttribute_ASettledRolloutThatLeftTheDriftBehind(t *testing.T) {
	a := attribute(drifted(), Evidence{RolloutEndedAt: cago(40 * time.Minute)})
	if a.Cause != CauseRolloutBias {
		t.Fatalf("Cause = %q, want %q", a.Cause, CauseRolloutBias)
	}
	if !hasFactor(a, "rollout completed 40m ago and the drift did not recover") {
		t.Errorf("Factors = %q, want the rollout line", a.Factors)
	}
}

func TestAttribute_ARolloutInsideItsSettleWindowIsNotYetBias(t *testing.T) {
	a := attribute(drifted(), Evidence{RolloutEndedAt: cago(3 * time.Minute)})
	if a.Cause != CauseUnknown {
		t.Errorf("Cause = %q, want %q: it may still be converging", a.Cause, CauseUnknown)
	}
	if !hasFactor(a, "rollout completed 3m ago") {
		t.Errorf("Factors = %q, want the rollout reported even where it is not the cause", a.Factors)
	}
}

func TestAttribute_ARolloutOutsideTheWindowIsNotBlamed(t *testing.T) {
	a := attribute(drifted(), Evidence{RolloutEndedAt: cago(5 * time.Hour)})
	if a.Cause != CauseUnknown {
		t.Errorf("Cause = %q, want %q", a.Cause, CauseUnknown)
	}
	if hasFactor(a, "rollout completed") {
		t.Errorf("Factors = %q, want no line for a rollout five hours back", a.Factors)
	}
}

// --- constraint_ignored -----------------------------------------------------

func scheduleAnyway(maxSkew int32) *Intent {
	in := spreadIntent()
	in.MaxSkew = ptr(maxSkew)
	in.WhenUnsatisfiable = v1.ScheduleAnyway
	return in
}

func TestAttribute_ScheduleAnywayAboveMaxSkewIsAnIgnoredConstraint(t *testing.T) {
	s := drifted() // observed skew 7
	a := s.Attribute(scheduleAnyway(1), Evidence{}, cnow, CauseConfig{})
	if a.Cause != CauseConstraintIgnored {
		t.Fatalf("Cause = %q, want %q", a.Cause, CauseConstraintIgnored)
	}
	if !hasFactor(a, "whenUnsatisfiable: ScheduleAnyway with maxSkew 1 and observed skew 7") {
		t.Errorf("Factors = %q, want the constraint line", a.Factors)
	}
}

func TestAttribute_ScheduleAnywayWithinItsBoundIsNotIgnored(t *testing.T) {
	s := drifted()
	if a := s.Attribute(scheduleAnyway(7), Evidence{}, cnow, CauseConfig{}); a.Cause != CauseUnknown {
		t.Errorf("Cause = %q, want %q: skew 7 does not exceed maxSkew 7", a.Cause, CauseUnknown)
	}
}

func TestAttribute_DoNotScheduleIsNotAnIgnoredConstraint(t *testing.T) {
	in := scheduleAnyway(1)
	in.WhenUnsatisfiable = v1.DoNotSchedule
	s := drifted()
	a := s.Attribute(in, Evidence{}, cnow, CauseConfig{})
	if a.Cause == CauseConstraintIgnored {
		t.Errorf("Cause = %q: the scheduler was not permitted to do this", a.Cause)
	}
	if hasFactor(a, "ScheduleAnyway") {
		t.Errorf("Factors = %q, want no ScheduleAnyway line", a.Factors)
	}
}

func TestAttribute_AMechanismOutranksThePermissiveConstraint(t *testing.T) {
	s := drifted()
	a := s.Attribute(scheduleAnyway(1), Evidence{Domains: map[Domain]DomainFacts{
		zoneC: {ReadyNodes: 1, NotReadyNodes: 11},
	}}, cnow, CauseConfig{})
	if a.Cause != CauseDomainOutage {
		t.Errorf("Cause = %q, want %q: ScheduleAnyway explains why nothing refused, not why the zone is down",
			a.Cause, CauseDomainOutage)
	}
	if !hasFactor(a, "ScheduleAnyway") {
		t.Errorf("Factors = %q, want the losing hypothesis still reported", a.Factors)
	}
}

// --- the ladder as a whole --------------------------------------------------

// TestAttribute_ThePrecedenceLadder loads every rule's evidence at once and
// removes them from the top down, so each step asserts both that the rule
// below was ready to fire and that the rule above was outranking it.
func TestAttribute_ThePrecedenceLadder(t *testing.T) {
	full := func() Evidence {
		return Evidence{
			Domains: map[Domain]DomainFacts{
				zoneA: {ReadyNodes: 12, PeakReadyNodes: 12, Pinned: 4},
				zoneC: {
					ReadyNodes: 1, NotReadyNodes: 11, PeakReadyNodes: 12,
					LostAt:         cago(10 * time.Minute),
					ConsolidatedAt: cago(10 * time.Minute),
					TaintedAt:      cago(10 * time.Minute),
				},
			},
			InsufficientResource: 4,
			ZonalVolumes:         true,
			RolloutEndedAt:       cago(40 * time.Minute),
		}
	}
	strip := []struct {
		want SuspectedCause
		next func(*Evidence) // removes the evidence the winner needed
	}{
		{CauseDomainOutage, func(e *Evidence) { f := e.Domains[zoneC]; f.NotReadyNodes = 0; e.Domains[zoneC] = f }},
		{CauseConsolidation, func(e *Evidence) { f := e.Domains[zoneC]; f.ConsolidatedAt = time.Time{}; e.Domains[zoneC] = f }},
		{CauseTaintExclusion, func(e *Evidence) { f := e.Domains[zoneC]; f.TaintedAt = time.Time{}; e.Domains[zoneC] = f }},
		{CauseCapacityShortfall, func(e *Evidence) {
			e.InsufficientResource = 0
			f := e.Domains[zoneC]
			f.PeakReadyNodes, f.LostAt = f.ReadyNodes, time.Time{}
			e.Domains[zoneC] = f
		}},
		{CauseVolumePinning, func(e *Evidence) { f := e.Domains[zoneA]; f.Pinned = 0; e.Domains[zoneA] = f }},
		{CauseRolloutBias, func(e *Evidence) { e.RolloutEndedAt = time.Time{} }},
		{CauseConstraintIgnored, nil},
	}

	ev := full()
	for _, step := range strip {
		s := drifted()
		got := s.Attribute(scheduleAnyway(1), ev, cnow, CauseConfig{})
		if got.Cause != step.want {
			t.Fatalf("Cause = %q, want %q", got.Cause, step.want)
		}
		if step.next == nil {
			break
		}
		step.next(&ev)
	}
	if a := attribute(drifted(), ev); a.Cause != CauseUnknown {
		t.Errorf("Cause = %q, want %q once every rule's evidence is gone", a.Cause, CauseUnknown)
	}
}

// TestAttribute_TheFactorOrderDoesNotDependOnTheWinner is the property that
// makes two findings on one subject diffable: the same evidence produces the
// same lines in the same places whichever cause came out on top.
func TestAttribute_TheFactorOrderDoesNotDependOnTheWinner(t *testing.T) {
	ev := Evidence{
		Domains: map[Domain]DomainFacts{
			zoneC: {ReadyNodes: 3, PeakReadyNodes: 12, LostAt: cago(time.Hour)},
		},
		RolloutEndedAt: cago(40 * time.Minute),
	}
	s := drifted()
	shortfall := s.Attribute(nil, ev, cnow, CauseConfig{})

	// The same evidence against a distribution drifting the other way. Zone c
	// now holds the surplus, so its lost nodes no longer explain anything and
	// the rollout wins instead — but the fall is still reported, because what
	// happened in the cluster did not change.
	inverted := zones([]int64{1, 3, 8}, []int64{4, 4, 4})
	rollout := inverted.Attribute(nil, ev, cnow, CauseConfig{})

	if shortfall.Cause != CauseCapacityShortfall || rollout.Cause != CauseRolloutBias {
		t.Fatalf("causes = %q / %q, want shortfall then rollout", shortfall.Cause, rollout.Cause)
	}
	if !slices.Equal(shortfall.Factors, rollout.Factors) {
		t.Errorf("Factors differ by winner:\n%q\n%q", shortfall.Factors, rollout.Factors)
	}
}

func TestAttribute_AtMostThreeDomainsGetAFallLine(t *testing.T) {
	many := &Scores{}
	facts := map[Domain]DomainFacts{}
	for i := range 8 {
		d := Domain("zone-" + string(rune('a'+i)))
		many.Domains = append(many.Domains, d)
		many.Actual = append(many.Actual, 1)
		many.Expected = append(many.Expected, 2)
		// Later zones lost more, so the last three should be the ones named.
		facts[d] = DomainFacts{ReadyNodes: 1, PeakReadyNodes: int64(2 + i), LostAt: cago(time.Minute)}
	}
	a := many.Attribute(nil, Evidence{Domains: facts}, cnow, CauseConfig{})

	var falls int
	for _, f := range a.Factors {
		if strings.Contains(f, "ready node count fell") {
			falls++
		}
	}
	if falls != maxFactorDomains {
		t.Errorf("%d fall lines, want %d", falls, maxFactorDomains)
	}
	if !hasFactor(a, "zone-h ready node count fell 9 → 1") {
		t.Errorf("Factors = %q, want the worst-hit domain first", a.Factors)
	}
	if hasFactor(a, "zone-a ") {
		t.Errorf("Factors = %q, want the least-hit domain dropped", a.Factors)
	}
	// Every domain still gets its note; only the factor list is capped.
	for i, n := range a.Notes {
		if n == "" {
			t.Errorf("Notes[%d] is empty, want a note on every domain that lost nodes", i)
		}
	}
}

// TestAttribute_EqualFallsBreakOnDomainName keeps the factor list stable when
// two domains lost the same number of nodes, which is the ordinary case for a
// cluster losing a machine type rather than a zone.
func TestAttribute_EqualFallsBreakOnDomainName(t *testing.T) {
	a := attribute(zones([]int64{1, 1, 10}, []int64{4, 4, 4}), Evidence{Domains: map[Domain]DomainFacts{
		zoneB: {ReadyNodes: 2, PeakReadyNodes: 6, LostAt: cago(time.Minute)},
		zoneA: {ReadyNodes: 4, PeakReadyNodes: 8, LostAt: cago(time.Minute)},
	}})
	want := []string{
		"us-east-1a ready node count fell 8 → 4 at " + cago(time.Minute).Format(time.RFC3339),
		"us-east-1b ready node count fell 6 → 2 at " + cago(time.Minute).Format(time.RFC3339),
	}
	if !slices.Equal(a.Factors[:2], want) {
		t.Errorf("Factors = %q, want %q first", a.Factors, want)
	}
}

func TestAttribute_AnAbsentExpectationFallsThroughToTheSubjectWideRules(t *testing.T) {
	// Score leaves Expected unset when the apportionment did not line up.
	// There are then no fill sides, so no domain rule can fire — but a
	// rollout is still a fact about the subject.
	s := &Scores{Domains: []Domain{zoneA, zoneB, zoneC}, Actual: []int64{8, 3, 1}, Total: 12, ObservedSkew: 7}
	a := s.Attribute(nil, Evidence{
		Domains:        map[Domain]DomainFacts{zoneC: {ReadyNodes: 1, NotReadyNodes: 11}},
		RolloutEndedAt: cago(40 * time.Minute),
	}, cnow, CauseConfig{})
	if a.Cause != CauseRolloutBias {
		t.Errorf("Cause = %q, want %q", a.Cause, CauseRolloutBias)
	}
}

func TestWithin(t *testing.T) {
	cases := []struct {
		name string
		at   time.Time
		want bool
	}{
		{"an event just now", cnow, true},
		{"an event inside the window", cago(time.Hour), true},
		{"an event on the boundary", cago(2 * time.Hour), false},
		{"an event outside the window", cago(3 * time.Hour), false},
		{"a zero time never happened", time.Time{}, false},
		{"modest skew into the future", cnow.Add(time.Second), true},
		{"a nonsense future stamp", cnow.Add(9 * time.Hour), false},
	}
	for _, tc := range cases {
		if got := within(tc.at, cnow, 2*time.Hour); got != tc.want {
			t.Errorf("%s: within = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestShortDur(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{2 * time.Hour, "2h"},
		{time.Hour, "1h"},
		{90 * time.Minute, "90m"},
		{10 * time.Minute, "10m"},
		{30 * time.Second, "30s"},
		{90 * time.Second, "1m30s"},
		{0, "0s"},
	}
	for _, tc := range cases {
		if got := shortDur(tc.d); got != tc.want {
			t.Errorf("shortDur(%s) = %q, want %q", tc.d, got, tc.want)
		}
	}
}
