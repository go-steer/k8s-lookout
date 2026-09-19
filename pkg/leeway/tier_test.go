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
	"testing"

	v1 "k8s.io/api/core/v1"
)

func ptr[T any](v T) *T { return &v }

func judgeEqual(actual []int64, maxSkew *int32, in *Intent, t Thresholds) Verdict {
	s := scoreEqual(actual, maxSkew)
	return s.Judge(in, t)
}

// spreadIntent is a plain inferred spread intent with no contract: the Tier B
// baseline every case below varies one thing away from.
func spreadIntent() *Intent {
	return &Intent{
		TopologyKey: "topology.kubernetes.io/zone",
		Mode:        ModeSpread,
		Source:      SourcePodAntiAffinityPreferred,
		Confidence:  ConfidenceInferred,
	}
}

func TestTier_StringsAndSeverities(t *testing.T) {
	cases := []struct {
		tier         Tier
		wantString   string
		wantSeverity string
	}{
		{TierNone, "", ""},
		{TierA, "A", "critical"},
		{TierB, "B", "warning"},
		{TierC, "C", "info"},
		{Tier(99), "", ""},
	}
	for _, tc := range cases {
		if got := tc.tier.String(); got != tc.wantString {
			t.Errorf("Tier(%d).String() = %q, want %q", tc.tier, got, tc.wantString)
		}
		if got := tc.tier.Severity(); got != tc.wantSeverity {
			t.Errorf("Tier(%d).Severity() = %q, want %q", tc.tier, got, tc.wantSeverity)
		}
	}
}

func TestBreachKind_StringsAndContract(t *testing.T) {
	cases := []struct {
		kind         BreachKind
		wantString   string
		wantContract bool
	}{
		{BreachNone, "", false},
		{BreachPerDomainCeiling, "per-domain-ceiling", true},
		{BreachMaxSkew, "max-skew", true},
		{BreachDrift, "drift", false},
		{BreachKind(99), "", false},
	}
	for _, tc := range cases {
		if got := tc.kind.String(); got != tc.wantString {
			t.Errorf("BreachKind(%d).String() = %q, want %q", tc.kind, got, tc.wantString)
		}
		if got := tc.kind.Contract(); got != tc.wantContract {
			t.Errorf("BreachKind(%d).Contract() = %v, want %v", tc.kind, got, tc.wantContract)
		}
	}
}

// TestJudge_TheTierComesFromTheRuleNotTheNumbers is §8.1's central claim. The
// two subjects here are deliberately the wrong way round on magnitude: the
// Tier A one is a single misplaced pod and the Tier B one is a rout.
func TestJudge_TheTierComesFromTheRuleNotTheNumbers(t *testing.T) {
	th := DefaultThresholds()

	contract := spreadIntent()
	contract.Source = SourceTopologySpreadConstraint
	contract.Confidence = ConfidenceDeclared
	contract.MaxSkew = ptr(int32(1))
	contract.WhenUnsatisfiable = v1.DoNotSchedule
	smallScores := scoreEqual([]int64{5, 3, 3}, contract.MaxSkew)
	bigScores := scoreEqual([]int64{20, 0, 0}, nil)
	if smallScores.Relocation >= bigScores.Relocation {
		t.Fatalf("premise broken: the Tier A case must be the smaller deviation (R=%d vs %d)",
			smallScores.Relocation, bigScores.Relocation)
	}

	small := smallScores.Judge(contract, th)
	big := bigScores.Judge(spreadIntent(), th)

	if small.Tier != TierA || small.Severity != "critical" {
		t.Errorf("declared DoNotSchedule breach = tier %v/%s, want A/critical", small.Tier, small.Severity)
	}
	if small.Kind != BreachMaxSkew {
		t.Errorf("rule = %v, want max-skew", small.Kind)
	}
	if big.Tier != TierB || big.Severity != "warning" {
		t.Errorf("inferred preference breach = tier %v/%s, want B/warning", big.Tier, big.Severity)
	}
}

func TestJudge_NoIntentIsTierC(t *testing.T) {
	v := judgeEqual([]int64{9, 0, 0}, nil, nil, DefaultThresholds())
	if !v.Breached {
		t.Fatalf("want a breach, got %+v", v)
	}
	if v.Tier != TierC {
		t.Errorf("tier = %v, want C — nothing was declared, so nothing was deviated from", v.Tier)
	}
}

func TestJudge_ALearnedBaselineIsTierC(t *testing.T) {
	in := spreadIntent()
	in.Source = SourceLearnedBaseline
	in.Confidence = ConfidenceLearned
	v := judgeEqual([]int64{9, 0, 0}, nil, in, DefaultThresholds())
	if v.Tier != TierC {
		t.Errorf("tier = %v, want C", v.Tier)
	}
}

// TestJudge_AnAssumedClusterDefaultNeverReachesTierA is §8.1's S4 rule, and
// the one case where the rule is not obvious from the table: this intent does
// say DoNotSchedule, and it is still capped, because we are the ones who
// guessed the constraint exists.
func TestJudge_AnAssumedClusterDefaultNeverReachesTierA(t *testing.T) {
	assumed := spreadIntent()
	assumed.Source = SourceClusterDefaultAssumed
	assumed.Confidence = ConfidenceAssumed
	assumed.MaxSkew = ptr(int32(1))
	assumed.WhenUnsatisfiable = v1.DoNotSchedule

	v := judgeEqual([]int64{9, 0, 0}, assumed.MaxSkew, assumed, DefaultThresholds())
	if !v.Breached {
		t.Fatalf("want a breach, got %+v", v)
	}
	if v.Tier != TierB {
		t.Errorf("tier = %v, want B — §8.1 caps an assumed default regardless of whenUnsatisfiable", v.Tier)
	}
	if v.Kind != BreachDrift {
		t.Errorf("rule = %v, want drift — the contract rules must not be reachable from an assumed source", v.Kind)
	}

	// The same constraint, declared by an operator rather than assumed by us,
	// is Tier A. This is the whole reason the two sources exist separately.
	declared := *assumed
	declared.Source = SourceClusterDefaultDeclared
	declared.Confidence = ConfidenceDeclared
	if got := judgeEqual([]int64{9, 0, 0}, declared.MaxSkew, &declared, DefaultThresholds()); got.Tier != TierA {
		t.Errorf("a declared cluster default scored tier %v, want A", got.Tier)
	}
}

// TestJudge_APerDomainCeilingIsReportedAgainstTheCeiling is the Phase 4 debt
// from FR-10's review: before this, a required podAntiAffinity's ceiling was
// checked by asking HardContract() and then testing ExcessSkew, so the finding
// said "observed skew exceeds the declared maxSkew" about a subject that
// declared no maxSkew, against a floor the ceiling had nothing to do with.
func TestJudge_APerDomainCeilingIsReportedAgainstTheCeiling(t *testing.T) {
	th := DefaultThresholds()
	in := spreadIntent()
	in.Source = SourcePodAntiAffinityRequired
	in.Confidence = ConfidenceDeclared
	in.MaxPerDomain = ptr(int64(1))

	// Two pods in one domain: the IgnoredDuringExecution case the ceiling
	// exists to catch. Skew is 1, which no skew rule would call a violation.
	v := judgeEqual([]int64{2, 1, 1}, nil, in, th)
	if !v.Breached || v.Kind != BreachPerDomainCeiling {
		t.Fatalf("judge = %+v, want a per-domain-ceiling breach", v)
	}
	if v.Tier != TierA {
		t.Errorf("tier = %v, want A", v.Tier)
	}
	if v.Reason == "observed skew exceeds the declared maxSkew" {
		t.Error("the per-domain ceiling is still being reported as a maxSkew violation")
	}

	// At the ceiling, not over it: no finding, even though the distribution is
	// as uneven as arithmetic allows.
	if got := judgeEqual([]int64{1, 1, 1}, nil, in, th); got.Breached {
		t.Errorf("at the ceiling = %+v, want no breach", got)
	}
}

// TestJudge_AnIntentWithOnlyACeilingCannotFireTheSkewRule is the other half of
// the split: the skew rule must not be reachable from an intent that declared
// no skew bound. [4,1,1] has excess skew against a [2,2,2] expectation, and
// the only honest contract statement about it is the ceiling.
func TestJudge_AnIntentWithOnlyACeilingCannotFireTheSkewRule(t *testing.T) {
	in := spreadIntent()
	in.Source = SourcePodAntiAffinityRequired
	in.MaxPerDomain = ptr(int64(9)) // deliberately not exceeded
	s := scoreEqual([]int64{4, 1, 1}, nil)
	if s.ExcessSkew == 0 {
		t.Fatal("premise broken: wanted a distribution with excess skew")
	}
	v := s.Judge(in, DefaultThresholds())
	if v.Kind == BreachMaxSkew {
		t.Error("an intent with no MaxSkew fired the maxSkew rule")
	}
	if v.Tier == TierA {
		t.Errorf("tier = A on %+v, want B: no declared bound was crossed", v)
	}
}

// TestJudge_AColocationIntentIsNotJudgedAsSpread is the false positive this
// rule exists to prevent. Without it every workload declaring a podAffinity
// fires immediately and permanently, because doing exactly what it was asked
// to do maximises ρ against a spread expectation.
func TestJudge_AColocationIntentIsNotJudgedAsSpread(t *testing.T) {
	th := DefaultThresholds()
	in := spreadIntent()
	in.Mode = ModeColocate
	in.Source = SourcePodAffinityPreferred

	perfect := scoreEqual([]int64{12, 0, 0}, nil)
	if perfect.Drift <= th.Drift {
		t.Fatal("premise broken: perfect colocation is supposed to look like maximal spread-drift")
	}
	if v := perfect.Judge(in, th); v.Breached {
		t.Errorf("perfectly colocated subject = %+v, want no breach", v)
	}
	if got := perfect.Dispersion(); got != 0 {
		t.Errorf("dispersion of a perfectly colocated subject = %v, want 0", got)
	}

	// Scattered against the same intent: four of twelve outside the fullest
	// domain is a dispersion of 0.33, over the 0.2 threshold.
	scattered := scoreEqual([]int64{8, 3, 1}, nil)
	v := scattered.Judge(in, th)
	if !v.Breached || v.Kind != BreachDrift {
		t.Fatalf("scattered colocation = %+v, want a drift breach", v)
	}
	if v.Tier != TierB {
		t.Errorf("tier = %v, want B", v.Tier)
	}
	if scattered.Scattered() != 4 {
		t.Errorf("Scattered() = %d, want 4", scattered.Scattered())
	}
}

// TestJudge_ColocationHonoursTheSmallNFloor mirrors §7.4 for the inverted
// frame: one pod away from its fellows out of four is not a finding.
func TestJudge_ColocationHonoursTheSmallNFloor(t *testing.T) {
	in := spreadIntent()
	in.Mode = ModeColocate
	s := scoreEqual([]int64{3, 1, 0}, nil)
	if s.Dispersion() <= DefaultThresholds().Drift {
		t.Fatal("premise broken: wanted a dispersion over threshold")
	}
	v := s.Judge(in, DefaultThresholds())
	if v.Breached {
		t.Errorf("judge = %+v, want the small-n floor to hold it", v)
	}
	if v.Reason != "small-n: scattered objects below floor" {
		t.Errorf("reason = %q, want the small-n explanation", v.Reason)
	}
}

// TestJudge_ConcentrationDoesNotEscalateAColocationFinding: max domain share
// is the risk signal for a subject that wanted to be spread. For one that
// asked to be together it is the goal, so escalating on it would raise
// severity on the subjects behaving best.
func TestJudge_ConcentrationDoesNotEscalateAColocationFinding(t *testing.T) {
	in := spreadIntent()
	in.Mode = ModeColocate
	s := scoreEqual([]int64{8, 3, 1}, nil)
	if !s.Escalate(DefaultThresholds()) {
		t.Fatal("premise broken: wanted a distribution over the max-domain-share threshold")
	}
	if v := s.Judge(in, DefaultThresholds()); v.Escalated {
		t.Errorf("judge = %+v, want no escalation on a colocation intent", v)
	}
}

// TestJudge_EscalationRaisesTierCAndOnlyTierC is §8.1's escalation rule. At A
// and B the severity is already at or above warning, so escalation is recorded
// and changes nothing — reporting it is what lets a consumer see the
// concentration risk without inventing a fourth severity.
func TestJudge_EscalationRaisesTierCAndOnlyTierC(t *testing.T) {
	th := DefaultThresholds()
	s := scoreEqual([]int64{9, 0, 0}, nil)
	if !s.Escalate(th) {
		t.Fatal("premise broken: wanted a concentrated distribution")
	}

	c := s.Judge(nil, th)
	if !c.Escalated || c.Severity != "warning" {
		t.Errorf("tier C escalated = %+v, want warning", c)
	}

	b := s.Judge(spreadIntent(), th)
	if !b.Escalated || b.Severity != "warning" {
		t.Errorf("tier B escalated = %+v, want warning (unchanged)", b)
	}

	a := spreadIntent()
	a.Source = SourceTopologySpreadConstraint
	a.MaxSkew = ptr(int32(1))
	a.WhenUnsatisfiable = v1.DoNotSchedule
	if got := s.Judge(a, th); !got.Escalated || got.Severity != "critical" {
		t.Errorf("tier A escalated = %+v, want critical (unchanged)", got)
	}
}

// TestJudge_UnbreachedVerdictsAreSafeToRead: the zero Tier is TierNone and the
// severity is empty, so a consumer that forgets to check Breached emits
// nothing rather than an info-severity finding about a healthy subject.
func TestJudge_UnbreachedVerdictsAreSafeToRead(t *testing.T) {
	noDomains := Score(nil, nil, Apportionment{}, nil, DefaultThresholds())
	cases := map[string]Verdict{
		"balanced":   judgeEqual([]int64{3, 3, 3}, nil, spreadIntent(), DefaultThresholds()),
		"below min":  judgeEqual([]int64{1, 1, 0}, nil, spreadIntent(), DefaultThresholds()),
		"no domains": noDomains.Judge(spreadIntent(), DefaultThresholds()),
	}
	for name, v := range cases {
		if v.Breached || v.Tier != TierNone || v.Severity != "" || v.Kind != BreachNone {
			t.Errorf("%s: %+v, want an empty verdict", name, v)
		}
	}
	if got := cases["below min"].Reason; got != GateBelowMinReplicas.String() {
		t.Errorf("below-min reason = %q, want the gate explanation", got)
	}
}

// TestJudge_ModeIgnoreSuppressesEverything, including a hard contract. An
// operator who declared a key uninteresting said so about the whole key.
func TestJudge_ModeIgnoreSuppressesEverything(t *testing.T) {
	in := spreadIntent()
	in.Mode = ModeIgnore
	in.MaxSkew = ptr(int32(1))
	in.WhenUnsatisfiable = v1.DoNotSchedule
	in.MaxPerDomain = ptr(int64(1))

	v := judgeEqual([]int64{9, 0, 0}, in.MaxSkew, in, DefaultThresholds())
	if v.Breached {
		t.Errorf("judge = %+v, want Ignore to suppress it", v)
	}
	if v.Reason != GateModeIgnore.String() {
		t.Errorf("reason = %q, want the Ignore gate", v.Reason)
	}
}

// TestDispersion_IsDefinedOnAnEmptySubject guards the division: Score returns
// early on a zero total, so MaxDomainShare is 0 and a naive 1−share would
// report a fully scattered subject that has no objects at all.
func TestDispersion_IsDefinedOnAnEmptySubject(t *testing.T) {
	s := Score(domains(3), []int64{0, 0, 0}, Apportionment{}, nil, DefaultThresholds())
	if got := s.Dispersion(); got != 0 {
		t.Errorf("Dispersion() on an empty subject = %v, want 0", got)
	}
	if got := s.Scattered(); got != 0 {
		t.Errorf("Scattered() on an empty subject = %d, want 0", got)
	}
}

func TestScore_MaxDomainObjectsIsTheShareNumerator(t *testing.T) {
	s := scoreEqual([]int64{7, 3, 2}, nil)
	if s.MaxDomainObjects != 7 {
		t.Errorf("MaxDomainObjects = %d, want 7", s.MaxDomainObjects)
	}
	if !almostEqual(s.MaxDomainShare, float64(s.MaxDomainObjects)/float64(s.Total), 1e-12) {
		t.Errorf("MaxDomainShare %v is not MaxDomainObjects/Total", s.MaxDomainShare)
	}
}

// TestHardContract_IsTheDisjunctionOfItsHalves keeps the three predicates from
// drifting apart, and pins which of MaxSkew/MaxPerDomain each half guarantees
// non-nil — breach dereferences MaxPerDomain on the strength of it.
func TestHardContract_IsTheDisjunctionOfItsHalves(t *testing.T) {
	skew := spreadIntent()
	skew.Source = SourceTopologySpreadConstraint
	skew.MaxSkew = ptr(int32(2))
	skew.WhenUnsatisfiable = v1.DoNotSchedule

	soft := *skew
	soft.WhenUnsatisfiable = v1.ScheduleAnyway

	ceiling := spreadIntent()
	ceiling.Source = SourcePodAntiAffinityRequired
	ceiling.MaxPerDomain = ptr(int64(1))

	assumed := *skew
	assumed.Source = SourceClusterDefaultAssumed

	cases := []struct {
		name                       string
		in                         *Intent
		wantSkew, wantPer, wantAny bool
	}{
		{"nil", nil, false, false, false},
		{"no contract", spreadIntent(), false, false, false},
		{"DoNotSchedule skew", skew, true, false, true},
		{"ScheduleAnyway skew", &soft, false, false, false},
		{"per-domain ceiling", ceiling, false, true, true},
		{"assumed DoNotSchedule", &assumed, false, false, false},
	}
	for _, tc := range cases {
		if got := tc.in.HardSkewContract(); got != tc.wantSkew {
			t.Errorf("%s: HardSkewContract() = %v, want %v", tc.name, got, tc.wantSkew)
		}
		if got := tc.in.HardPerDomainContract(); got != tc.wantPer {
			t.Errorf("%s: HardPerDomainContract() = %v, want %v", tc.name, got, tc.wantPer)
		}
		if got := tc.in.HardContract(); got != tc.wantAny {
			t.Errorf("%s: HardContract() = %v, want %v", tc.name, got, tc.wantAny)
		}
		if tc.in.HardSkewContract() && tc.in.MaxSkew == nil {
			t.Errorf("%s: HardSkewContract() true with a nil MaxSkew", tc.name)
		}
		if tc.in.HardPerDomainContract() && tc.in.MaxPerDomain == nil {
			t.Errorf("%s: HardPerDomainContract() true with a nil MaxPerDomain", tc.name)
		}
	}
}

func TestRoute_TheTierSetsTheSeverityAndTierCDecidesWhetherToSpeak(t *testing.T) {
	cases := []struct {
		name         string
		verdict      Verdict
		tierCSignals bool
		wantSignal   bool
		wantSeverity string
		wantReason   string
	}{
		{
			name:       "a subject that did not breach",
			verdict:    Verdict{},
			wantReason: "no breach",
		},
		{
			name:         "tier A takes the critical route and injects",
			verdict:      Verdict{Breached: true, Tier: TierA, Severity: severityCritical},
			wantSignal:   true,
			wantSeverity: severityCritical,
		},
		{
			name:         "tier B reaches the watchboard at warning",
			verdict:      Verdict{Breached: true, Tier: TierB, Severity: severityWarning},
			wantSignal:   true,
			wantSeverity: severityWarning,
		},
		{
			name:       "tier C is silent by default",
			verdict:    Verdict{Breached: true, Tier: TierC, Severity: severityInfo},
			wantReason: "tier C is metrics-only unless enabled by policy",
		},
		{
			name:         "tier C speaks when a policy opts in",
			verdict:      Verdict{Breached: true, Tier: TierC, Severity: severityInfo},
			tierCSignals: true,
			wantSignal:   true,
			wantSeverity: severityInfo,
		},
		{
			name: "an escalated tier C carries the severity Judge gave it",
			verdict: Verdict{
				Breached: true, Tier: TierC, Severity: severityWarning, Escalated: true,
			},
			tierCSignals: true,
			wantSignal:   true,
			wantSeverity: severityWarning,
		},
	}
	for _, tc := range cases {
		got := tc.verdict.Route(tc.tierCSignals)
		if got.Signal != tc.wantSignal || got.Severity != tc.wantSeverity || got.Reason != tc.wantReason {
			t.Errorf("%s: Route(%v) = %+v, want signal=%v severity=%q reason=%q",
				tc.name, tc.tierCSignals, got, tc.wantSignal, tc.wantSeverity, tc.wantReason)
		}
	}
}

// TestRoute_SuppressionSilencesEveryTier is §14's exit criterion in miniature.
// A zone going away puts four hundred workloads in breach of a contract they
// all genuinely declared, and Tier A is precisely the tier that would
// otherwise open four hundred agent sessions about one fact.
func TestRoute_SuppressionSilencesEveryTier(t *testing.T) {
	for _, tier := range []Tier{TierA, TierB, TierC} {
		v := Verdict{
			Breached: true, Tier: tier, Severity: tier.Severity(),
			Suppressed: true, Transient: TransientDomainOutage,
			Reason: "domain-outage: the counts describe a different cluster",
		}
		got := v.Route(true)
		if got.Signal {
			t.Errorf("tier %s: Route emitted a signal through a suppression", tier)
		}
		if got.Reason != v.Reason {
			t.Errorf("tier %s: Reason = %q, want the suppression's own %q", tier, got.Reason, v.Reason)
		}
		if got.Severity != "" {
			t.Errorf("tier %s: Severity = %q, want none where nothing is emitted", tier, got.Severity)
		}
	}
}

// TestRoute_ARelaxedBreachRoutesNormally: §7.6 relaxing the thresholds changes
// whether a subject breached, never what happens to it once it has.
func TestRoute_ARelaxedBreachRoutesNormally(t *testing.T) {
	v := Verdict{
		Breached: true, Tier: TierB, Severity: severityWarning,
		Relaxed: true, Transient: TransientRollout,
	}
	if got := v.Route(false); !got.Signal || got.Severity != severityWarning {
		t.Errorf("Route = %+v, want a plain warning signal", got)
	}
}
