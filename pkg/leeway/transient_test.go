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
	"math"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
)

// tnow is the fixed instant every transient case is judged against.
var tnow = time.Date(2026, 9, 19, 14, 0, 0, 0, time.UTC)

// ago returns an instant d before tnow.
func ago(d time.Duration) time.Time { return tnow.Add(-d) }

func TestTransientState_Strings(t *testing.T) {
	cases := map[TransientState]string{
		TransientNone:         "",
		TransientWarmup:       "cluster-warmup",
		TransientDomainOutage: "domain-outage",
		TransientRollout:      "rollout",
		TransientDrain:        "node-drain",
		TransientScale:        "recent-scale",
		TransientState(99):    "",
	}
	for state, want := range cases {
		if got := state.String(); got != want {
			t.Errorf("TransientState(%d).String() = %q, want %q", state, got, want)
		}
	}
}

func TestTransientConfig_DefaultsAreTheDesignsNumbers(t *testing.T) {
	c := DefaultTransientConfig()
	if c.ScaleSettleWindow != 5*time.Minute {
		t.Errorf("ScaleSettleWindow = %v, want 5m", c.ScaleSettleWindow)
	}
	if c.DrainSettleWindow != 10*time.Minute {
		t.Errorf("DrainSettleWindow = %v, want 10m", c.DrainSettleWindow)
	}
	if c.OutageWindow != 15*time.Minute {
		t.Errorf("OutageWindow = %v, want 15m", c.OutageWindow)
	}
	if c.Multiplier != 2.5 {
		t.Errorf("Multiplier = %v, want 2.5", c.Multiplier)
	}
}

// A multiplier below 1 tightens rather than relaxes, which no reading of
// "transientThresholdMultiplier" supports, so normalize refuses it. Everything
// else unset takes the default.
func TestTransientConfig_NormalizeRefusesATighteningMultiplier(t *testing.T) {
	got := TransientConfig{Multiplier: 0.5}.normalize()
	if got != DefaultTransientConfig() {
		t.Fatalf("normalize() = %+v, want the defaults", got)
	}
	// 1 is a legal setting and means "do not relax"; it is not refused.
	if got := (TransientConfig{Multiplier: 1}).normalize().Multiplier; got != 1 {
		t.Errorf("a multiplier of 1 normalised to %v, want it honoured", got)
	}
	// A partial config keeps what it set.
	part := TransientConfig{ScaleSettleWindow: time.Minute}.normalize()
	if part.ScaleSettleWindow != time.Minute {
		t.Errorf("ScaleSettleWindow = %v, want the 1m that was set", part.ScaleSettleWindow)
	}
	if part.Multiplier != 2.5 {
		t.Errorf("Multiplier = %v, want the default filled in", part.Multiplier)
	}
}

// Normalized is normalize with a caller outside the package. The source reads
// OutageWindow off it to size the history it keeps, and a zero there would
// mean keeping none.
func TestTransientConfig_NormalizedIsTheFilledInConfig(t *testing.T) {
	if got := (TransientConfig{}).Normalized(); got != DefaultTransientConfig() {
		t.Fatalf("Normalized() = %+v, want the defaults", got)
	}
	if got := (TransientConfig{OutageWindow: time.Hour}).Normalized(); got.OutageWindow != time.Hour {
		t.Errorf("OutageWindow = %v, want the hour that was set", got.OutageWindow)
	}
}

func TestClassify_AQuietSubjectIsNotInAnyTransientState(t *testing.T) {
	got := Transients{}.Classify(tnow, TransientConfig{})
	if got != (Suppression{}) {
		t.Fatalf("Classify() = %+v, want the zero Suppression", got)
	}
}

func TestClassify_TheTableRowByRow(t *testing.T) {
	cases := []struct {
		name         string
		tr           Transients
		wantState    TransientState
		wantSuppress bool
		wantRelax    bool
	}{
		{"warmup", Transients{Warming: true}, TransientWarmup, true, false},
		{"domain outage", Transients{DomainOutage: true}, TransientDomainOutage, true, false},
		{"rollout", Transients{RolloutInProgress: true}, TransientRollout, false, true},
		{"drain inside window", Transients{DrainedAt: ago(9 * time.Minute)}, TransientDrain, false, true},
		{"scale inside window", Transients{ScaledAt: ago(4 * time.Minute)}, TransientScale, false, true},
		{"drain outside window", Transients{DrainedAt: ago(11 * time.Minute)}, TransientNone, false, false},
		{"scale outside window", Transients{ScaledAt: ago(6 * time.Minute)}, TransientNone, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.tr.Classify(tnow, TransientConfig{})
			if got.State != tc.wantState {
				t.Errorf("State = %v, want %v", got.State, tc.wantState)
			}
			if got.Suppress != tc.wantSuppress || got.Relax != tc.wantRelax {
				t.Errorf("Suppress/Relax = %v/%v, want %v/%v",
					got.Suppress, got.Relax, tc.wantSuppress, tc.wantRelax)
			}
			if tc.wantState != TransientNone && got.Reason == "" {
				t.Error("a classified state came back with no reason")
			}
			if got.Relax && got.Multiplier != 2.5 {
				t.Errorf("Multiplier = %v, want the config's 2.5", got.Multiplier)
			}
			if got.Suppress && got.Relax {
				t.Error("a state was both suppressed and relaxed")
			}
		})
	}
}

// Suppression is for states that make the counts unreliable; relaxation is for
// states where the counts are correct and merely unflattering. That split is
// the whole of §7.6's "suppressed or evaluated against a relaxed multiplier",
// so it is asserted as a property rather than left implied by the table above.
func TestClassify_UnreliableCountsAreSuppressedAndSlowOnesAreRelaxed(t *testing.T) {
	unreliable := []Transients{{Warming: true}, {DomainOutage: true}}
	for _, tr := range unreliable {
		if got := tr.Classify(tnow, TransientConfig{}); !got.Suppress {
			t.Errorf("%+v was not suppressed", tr)
		}
	}
	slow := []Transients{
		{RolloutInProgress: true},
		{DrainedAt: ago(time.Minute)},
		{ScaledAt: ago(time.Minute)},
	}
	for _, tr := range slow {
		if got := tr.Classify(tnow, TransientConfig{}); !got.Relax {
			t.Errorf("%+v was not relaxed", tr)
		}
	}
}

// Warmup outranks everything because a half-synced informer can invent any of
// the others out of objects it has not seen yet.
func TestClassify_WarmupOutranksEveryOtherState(t *testing.T) {
	tr := Transients{
		Warming:           true,
		DomainOutage:      true,
		RolloutInProgress: true,
		DrainedAt:         ago(time.Minute),
		ScaledAt:          ago(time.Minute),
	}
	if got := tr.Classify(tnow, TransientConfig{}); got.State != TransientWarmup {
		t.Fatalf("State = %v, want cluster-warmup", got.State)
	}
}

func TestClassify_AnOutageOutranksTheRelaxingStates(t *testing.T) {
	tr := Transients{DomainOutage: true, RolloutInProgress: true}
	got := tr.Classify(tnow, TransientConfig{})
	if got.State != TransientDomainOutage || !got.Suppress {
		t.Fatalf("Classify() = %+v, want a suppressing domain-outage", got)
	}
}

// Windows are configurable, and the settle windows are the two §10.1 exposes.
func TestClassify_TheSettleWindowsAreHonoured(t *testing.T) {
	c := TransientConfig{ScaleSettleWindow: time.Hour, DrainSettleWindow: time.Hour}
	tr := Transients{ScaledAt: ago(30 * time.Minute)}
	if got := tr.Classify(tnow, c); got.State != TransientScale {
		t.Errorf("a 30m-old scale under a 1h window: State = %v, want recent-scale", got.State)
	}
}

// A few seconds of clock skew around an event that just happened is the moment
// suppression matters most, so a slightly-future timestamp counts. A wildly
// future one is bad data and must not suppress the subject forever.
func TestRecent_TreatsModestSkewAsRecentAndNonsenseAsStale(t *testing.T) {
	c := TransientConfig{}
	skewed := Transients{ScaledAt: tnow.Add(10 * time.Second)}
	if got := skewed.Classify(tnow, c); got.State != TransientScale {
		t.Errorf("a 10s-future scale: State = %v, want recent-scale", got.State)
	}
	nonsense := Transients{ScaledAt: tnow.Add(24 * time.Hour)}
	if got := nonsense.Classify(tnow, c); got.State != TransientNone {
		t.Errorf("a 24h-future scale: State = %v, want none", got.State)
	}
	// The zero time is "never observed", not "observed at the epoch".
	if got := (Transients{}).Classify(tnow, c); got.State != TransientNone {
		t.Errorf("a zero ScaledAt: State = %v, want none", got.State)
	}
}

func TestDomainOutage_MoreThanHalfIsTheLine(t *testing.T) {
	c := DefaultTransientConfig()
	hist := []ReadyCount{{At: ago(5 * time.Minute), Ready: 4}}
	if c.DomainOutage(hist, 2, tnow) {
		t.Error("4 → 2 is exactly half and must not be an outage: that is a rolling node-pool upgrade")
	}
	if !c.DomainOutage(hist, 1, tnow) {
		t.Error("4 → 1 is more than half and must be an outage")
	}
	if !c.DomainOutage([]ReadyCount{{At: ago(time.Minute), Ready: 9}}, 4, tnow) {
		t.Error("9 → 4 must be an outage")
	}
}

// The comparison is against the highest count in the window, not the oldest
// sample in it. With an oldest-sample rule this case flips to "no outage" the
// moment the 9 ages out, un-detecting an outage that is still in progress.
func TestDomainOutage_ComparesAgainstThePeakNotTheOldestSample(t *testing.T) {
	c := DefaultTransientConfig()
	hist := []ReadyCount{
		{At: ago(12 * time.Minute), Ready: 9},
		{At: ago(8 * time.Minute), Ready: 4},
		{At: ago(2 * time.Minute), Ready: 4},
	}
	if !c.DomainOutage(hist, 4, tnow) {
		t.Fatal("9 → 4 → 4 is an outage for as long as the 9 is in the window")
	}
	// Order must not matter; the caller's history is whatever its ring holds.
	shuffled := []ReadyCount{hist[2], hist[0], hist[1]}
	if !c.DomainOutage(shuffled, 4, tnow) {
		t.Error("the same history in a different order gave a different answer")
	}
}

func TestDomainOutage_IgnoresSamplesOutsideTheWindow(t *testing.T) {
	c := DefaultTransientConfig()
	hist := []ReadyCount{{At: ago(20 * time.Minute), Ready: 9}}
	if c.DomainOutage(hist, 4, tnow) {
		t.Error("a 20m-old peak is outside the 15m window and must not count")
	}
	if !(TransientConfig{OutageWindow: 30 * time.Minute}).DomainOutage(hist, 4, tnow) {
		t.Error("the same peak inside a 30m window must count")
	}
}

func TestDomainOutage_AnEmptyOrGrowingDomainIsNotAnOutage(t *testing.T) {
	c := DefaultTransientConfig()
	if c.DomainOutage(nil, 0, tnow) {
		t.Error("a domain that has never had a ready node is not an outage")
	}
	if c.DomainOutage(nil, 6, tnow) {
		t.Error("a domain with no history and six ready nodes is not an outage")
	}
	if c.DomainOutage([]ReadyCount{{At: ago(time.Minute), Ready: 2}}, 9, tnow) {
		t.Error("a domain that grew is not an outage")
	}
}

func TestRelaxed_MultipliesTheTolerances(t *testing.T) {
	base := DefaultThresholds()
	got := base.Relaxed(2.5)
	if math.Abs(got.Drift-0.5) > 1e-9 {
		t.Errorf("Drift = %v, want 0.5", got.Drift)
	}
	if got.SmallNThreshold != 25 {
		t.Errorf("SmallNThreshold = %d, want 25", got.SmallNThreshold)
	}
	if got.MinRelocationSmallN != 5 {
		t.Errorf("MinRelocationSmallN = %d, want 5", got.MinRelocationSmallN)
	}
}

// MinReplicasForScoring answers "does this subject have enough objects for a
// distribution to mean anything", which is a property of the subject. Scaling
// it would let a transient silently change which subjects are measured at all.
func TestRelaxed_LeavesTheEligibilityGateAlone(t *testing.T) {
	base := DefaultThresholds()
	if got := base.Relaxed(2.5).MinReplicasForScoring; got != base.MinReplicasForScoring {
		t.Errorf("MinReplicasForScoring = %d, want it untouched at %d", got, base.MinReplicasForScoring)
	}
}

// A share cannot exceed 1, so 1.25 and 1.0 are equally unreachable. Only one of
// them can be printed in a finding without looking like a bug.
func TestRelaxed_CapsMaxDomainShareAtOne(t *testing.T) {
	if got := DefaultThresholds().Relaxed(2.5).MaxDomainShare; got != 1 {
		t.Errorf("MaxDomainShare = %v, want it capped at 1", got)
	}
	if got := DefaultThresholds().Relaxed(1.5).MaxDomainShare; math.Abs(got-0.75) > 1e-9 {
		t.Errorf("MaxDomainShare = %v, want 0.75 — the cap must not bind early", got)
	}
}

func TestRelaxed_IsTheIdentityAtOrBelowOne(t *testing.T) {
	base := DefaultThresholds()
	for _, mult := range []float64{1, 0.5, 0, -3} {
		if got := base.Relaxed(mult); got != base {
			t.Errorf("Relaxed(%v) = %+v, want the thresholds unchanged", mult, got)
		}
	}
	// A zero threshold set stays zero rather than acquiring a ceil'd 1.
	if got := (Thresholds{}).Relaxed(2.5); got != (Thresholds{}) {
		t.Errorf("Relaxed on the zero Thresholds = %+v, want zero", got)
	}
}

// The corpus case: [6,1,1] over three zones is a breach on a quiet cluster and
// is not one mid-rollout, because that is what a rollout looks like halfway
// through.
func TestJudgeTransient_ARolloutRelaxesADriftBreachAway(t *testing.T) {
	s := scoreEqual([]int64{6, 1, 1}, nil)
	in, th := spreadIntent(), DefaultThresholds()

	quiet := s.JudgeTransient(in, th, Suppression{})
	if !quiet.Breached || quiet.Kind != BreachDrift {
		t.Fatalf("on a quiet cluster: %+v, want a drift breach", quiet)
	}
	sup := Transients{RolloutInProgress: true}.Classify(tnow, TransientConfig{})
	mid := s.JudgeTransient(in, th, sup)
	if mid.Breached {
		t.Errorf("mid-rollout: %+v, want no breach", mid)
	}
	if mid.Transient != TransientRollout || !mid.Relaxed {
		t.Errorf("mid-rollout verdict = %+v, want it marked relaxed and rollout", mid)
	}
}

// Relaxed does not mean silent. A subject far enough over to clear 2.5× the
// threshold still fires, and the reason says the rollout did not excuse it.
func TestJudgeTransient_ARelaxedBreachStillFiresAndSaysSo(t *testing.T) {
	s := scoreEqual([]int64{12, 0, 0}, nil)
	sup := Transients{RolloutInProgress: true}.Classify(tnow, TransientConfig{})
	v := s.JudgeTransient(spreadIntent(), DefaultThresholds(), sup)
	if !v.Breached {
		t.Fatalf("verdict = %+v, want a breach: ρ = %v is over 2.5× the threshold", v, s.Drift)
	}
	if !v.Relaxed || v.Transient != TransientRollout {
		t.Errorf("verdict = %+v, want it marked relaxed and rollout", v)
	}
	if !strings.Contains(v.Reason, "relaxed threshold was exceeded anyway") {
		t.Errorf("Reason = %q, want it to say the relaxed threshold was exceeded", v.Reason)
	}
	if v.Tier != TierB {
		t.Errorf("Tier = %v, want B — a transient changes the threshold, not the confidence", v.Tier)
	}
}

// Multiplying a number the *user* declared substitutes our judgement for
// theirs. A rollout does not license breaking a DoNotSchedule maxSkew.
func TestJudgeTransient_ARelaxationDoesNotReachTheContractRules(t *testing.T) {
	in := spreadIntent()
	in.Source = SourceTopologySpreadConstraint
	in.Confidence = ConfidenceDeclared
	in.MaxSkew = ptr(int32(1))
	in.WhenUnsatisfiable = v1.DoNotSchedule

	// Skew 2 against a declared maxSkew of 1: over the contract, but nowhere
	// near even the unrelaxed drift threshold.
	s := scoreEqual([]int64{4, 3, 2}, in.MaxSkew)
	sup := Transients{RolloutInProgress: true}.Classify(tnow, TransientConfig{})
	v := s.JudgeTransient(in, DefaultThresholds(), sup)
	if !v.Breached || v.Kind != BreachMaxSkew || v.Tier != TierA {
		t.Fatalf("verdict = %+v, want a Tier A max-skew breach through the rollout", v)
	}
}

// Suppression, unlike relaxation, covers Tier A. A zone dies and every
// DoNotSchedule constraint in the cluster is violated in the same second;
// §14's exit criterion is one finding, not four hundred.
func TestJudgeTransient_AnOutageSuppressesEvenAContractBreach(t *testing.T) {
	in := spreadIntent()
	in.Source = SourceTopologySpreadConstraint
	in.Confidence = ConfidenceDeclared
	in.MaxSkew = ptr(int32(1))
	in.WhenUnsatisfiable = v1.DoNotSchedule

	s := scoreEqual([]int64{9, 0, 0}, in.MaxSkew)
	sup := Transients{DomainOutage: true}.Classify(tnow, TransientConfig{})
	v := s.JudgeTransient(in, DefaultThresholds(), sup)
	if v.Breached || v.Tier != TierNone || v.Severity != "" {
		t.Fatalf("verdict = %+v, want nothing to fire", v)
	}
	if !v.Suppressed || v.Transient != TransientDomainOutage {
		t.Errorf("verdict = %+v, want it marked suppressed and domain-outage", v)
	}
	if v.Reason == "" {
		t.Error("a suppressed verdict must say why: that is the answer to 'why am I not being paged'")
	}
}

func TestJudgeTransient_WarmupSuppressesToo(t *testing.T) {
	s := scoreEqual([]int64{9, 0, 0}, nil)
	sup := Transients{Warming: true}.Classify(tnow, TransientConfig{})
	v := s.JudgeTransient(spreadIntent(), DefaultThresholds(), sup)
	if v.Breached || !v.Suppressed || v.Transient != TransientWarmup {
		t.Fatalf("verdict = %+v, want a suppressed cluster-warmup verdict", v)
	}
}

// Judge is JudgeTransient with nothing in force; a caller that has no transient
// facts to offer gets exactly the old behaviour.
func TestJudgeTransient_TheZeroSuppressionIsPlainJudge(t *testing.T) {
	for _, actual := range [][]int64{{6, 1, 1}, {3, 3, 3}, {12, 0, 0}, {1, 1}} {
		s := scoreEqual(actual, nil)
		want := s.Judge(spreadIntent(), DefaultThresholds())
		got := s.JudgeTransient(spreadIntent(), DefaultThresholds(), Suppression{})
		if got != want {
			t.Errorf("%v: JudgeTransient = %+v, want Judge's %+v", actual, got, want)
		}
	}
}
