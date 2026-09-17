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
	"testing"

	v1 "k8s.io/api/core/v1"
)

func domains(n int) []Domain {
	out := make([]Domain, n)
	for i := range out {
		out[i] = Domain(string(rune('a' + i)))
	}
	return out
}

func scoreEqual(actual []int64, maxSkew *int32) Scores {
	m := len(actual)
	var n int64
	for _, a := range actual {
		n += a
	}
	ap := Apportion(n, EqualWeights(m), nil)
	return Score(domains(m), actual, ap, maxSkew, DefaultThresholds())
}

// TestScore_RhoDistinguishesShapesThatSkewCannot is the §7.3 rationale, made
// executable: two distributions with identical observed skew and very
// different risk.
//
// The design states this example as "[10,0,0,0] and [10,3,3,4] both have skew
// 10 ... ρ is 0.75 vs 0.15". Those numbers do not survive contact with the
// definitions: [10,3,3,4] has skew 7, not 10, and against an integer
// expectation the two ρ values are 0.70 and 0.25. The *point* is exactly
// right, so the example is repaired here rather than dropped — [10,0,5,5] does
// have skew 10, and the gap in ρ is still the thing worth seeing. §7.3 has
// been corrected to match.
func TestScore_RhoDistinguishesShapesThatSkewCannot(t *testing.T) {
	concentrated := scoreEqual([]int64{10, 0, 0, 0}, nil)
	spread := scoreEqual([]int64{10, 0, 5, 5}, nil)

	if concentrated.ObservedSkew != spread.ObservedSkew {
		t.Fatalf("premise broken: skews differ (%d vs %d)", concentrated.ObservedSkew, spread.ObservedSkew)
	}
	if concentrated.ObservedSkew != 10 {
		t.Errorf("observed skew = %d, want 10", concentrated.ObservedSkew)
	}
	if !almostEqual(concentrated.Drift, 0.70, 1e-9) {
		t.Errorf("concentrated ρ = %v, want 0.70", concentrated.Drift)
	}
	if !almostEqual(spread.Drift, 0.25, 1e-9) {
		t.Errorf("spread ρ = %v, want 0.25", spread.Drift)
	}
	if concentrated.Drift <= spread.Drift {
		t.Errorf("ρ failed to rank the concentrated case worse: %v vs %v", concentrated.Drift, spread.Drift)
	}
}

func TestScore_Examples(t *testing.T) {
	tests := []struct {
		name       string
		actual     []int64
		wantSkew   int64
		wantMinS   int64
		wantR      int64
		wantRho    float64
		wantShare  float64
		wantConcen float64
	}{
		{"perfectly balanced", []int64{3, 3, 3}, 0, 0, 0, 0, 1.0 / 3, 1.0 / 3},
		{"unavoidable remainder", []int64{4, 3, 3}, 1, 1, 0, 0, 0.4, 0.34},
		{"one pod out of place", []int64{5, 3, 2}, 3, 1, 1, 0.1, 0.5, 0.38},
		{"everything in one domain", []int64{9, 0, 0}, 9, 0, 6, 2.0 / 3, 1, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := scoreEqual(tc.actual, nil)
			if got.ObservedSkew != tc.wantSkew {
				t.Errorf("ObservedSkew = %d, want %d", got.ObservedSkew, tc.wantSkew)
			}
			if got.MinAchievableSkew != tc.wantMinS {
				t.Errorf("MinAchievableSkew = %d, want %d", got.MinAchievableSkew, tc.wantMinS)
			}
			if got.Relocation != tc.wantR {
				t.Errorf("Relocation = %d, want %d", got.Relocation, tc.wantR)
			}
			if !almostEqual(got.Drift, tc.wantRho, 1e-9) {
				t.Errorf("Drift = %v, want %v", got.Drift, tc.wantRho)
			}
			if !almostEqual(got.MaxDomainShare, tc.wantShare, 1e-9) {
				t.Errorf("MaxDomainShare = %v, want %v", got.MaxDomainShare, tc.wantShare)
			}
			if !almostEqual(got.Concentration, tc.wantConcen, 0.01) {
				t.Errorf("Concentration = %v, want ~%v", got.Concentration, tc.wantConcen)
			}
		})
	}
}

// TestScore_UnavoidableRemainderIsNotDrift is the arithmetic-honesty case: 10
// pods over 3 zones cannot be even, and reporting the leftover as drift would
// make every workload whose replica count does not divide by its zone count
// permanently guilty.
func TestScore_UnavoidableRemainderIsNotDrift(t *testing.T) {
	got := scoreEqual([]int64{4, 3, 3}, nil)
	if got.Relocation != 0 {
		t.Errorf("Relocation = %d, want 0 — [4,3,3] is the best possible placement of 10 over 3", got.Relocation)
	}
	if got.ExcessSkew != 0 {
		t.Errorf("ExcessSkew = %d, want 0 — observed skew 1 equals the achievable floor", got.ExcessSkew)
	}
}

func TestScore_ExcessSkewUsesTheContractWhenItIsLooser(t *testing.T) {
	skew2 := int32(2)
	// Observed skew 3, achievable floor 1, declared maxSkew 2. The user said
	// 2 is acceptable, so only the excess over 2 counts.
	got := scoreEqual([]int64{5, 3, 2}, &skew2)
	if got.ExcessSkew != 1 {
		t.Errorf("ExcessSkew = %d, want 1 (observed 3 − max(S*=1, maxSkew=2))", got.ExcessSkew)
	}

	// A maxSkew tighter than arithmetic allows must not manufacture a
	// violation out of the remainder.
	skew0 := int32(0)
	tight := scoreEqual([]int64{4, 3, 3}, &skew0)
	if tight.ExcessSkew != 0 {
		t.Errorf("ExcessSkew = %d, want 0: maxSkew 0 is unachievable for 10 over 3, and S* protects against it", tight.ExcessSkew)
	}
}

func TestScore_Gating(t *testing.T) {
	tests := []struct {
		name     string
		actual   []int64
		wantEval bool
		wantGate GateReason
	}{
		{"no domains", nil, false, GateNoDomains},
		{"no objects", []int64{0, 0, 0}, false, GateNoObjects},
		{"below min replicas", []int64{2, 0, 0}, false, GateBelowMinReplicas},
		{"at min replicas", []int64{3, 0, 0}, true, GateNone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := scoreEqual(tc.actual, nil)
			if got.Evaluable != tc.wantEval || got.Gate != tc.wantGate {
				t.Errorf("Evaluable=%v Gate=%v, want %v and %v", got.Evaluable, got.Gate, tc.wantEval, tc.wantGate)
			}
		})
	}
}

// TestScore_RecordsShareEvenWhenNotEvaluable pins §7.4's "record distribution
// and max domain share, do not evaluate drift". The distinction matters: a
// two-replica workload entirely in one zone is still worth a dashboard, it is
// just not worth paging about.
func TestScore_RecordsShareEvenWhenNotEvaluable(t *testing.T) {
	got := scoreEqual([]int64{2, 0, 0}, nil)
	if got.Evaluable {
		t.Fatal("expected not evaluable at n=2")
	}
	if !almostEqual(got.MaxDomainShare, 1.0, 1e-9) {
		t.Errorf("MaxDomainShare = %v, want 1.0 — it must still be recorded", got.MaxDomainShare)
	}
	if got.Total != 2 {
		t.Errorf("Total = %d, want 2", got.Total)
	}
}

func TestScores_Breach(t *testing.T) {
	spread := &Intent{Mode: ModeSpread}
	ignore := &Intent{Mode: ModeIgnore}
	skew1 := int32(1)
	hard := &Intent{Mode: ModeSpread, MaxSkew: &skew1, WhenUnsatisfiable: v1.DoNotSchedule, Source: SourceTopologySpreadConstraint}
	assumed := &Intent{Mode: ModeSpread, MaxSkew: &skew1, WhenUnsatisfiable: v1.DoNotSchedule, Source: SourceClusterDefaultAssumed}

	t.Run("ignore mode never breaches", func(t *testing.T) {
		s := scoreEqual([]int64{20, 0, 0}, nil)
		if ok, _ := s.Breach(ignore, DefaultThresholds()); ok {
			t.Error("Ignore mode must not breach")
		}
	})

	t.Run("drift over threshold", func(t *testing.T) {
		s := scoreEqual([]int64{20, 0, 0}, nil)
		ok, why := s.Breach(spread, DefaultThresholds())
		if !ok {
			t.Errorf("expected breach, got %q", why)
		}
	})

	t.Run("drift under threshold", func(t *testing.T) {
		s := scoreEqual([]int64{11, 10, 9}, nil)
		if ok, _ := s.Breach(spread, DefaultThresholds()); ok {
			t.Errorf("ρ=%v should be under the 0.2 threshold", s.Drift)
		}
	})

	// §7.4: at n=4 a single misplaced pod is ρ=0.25, over the 0.2 threshold.
	// R=1 must keep it quiet.
	t.Run("small n needs two pods to move", func(t *testing.T) {
		s := scoreEqual([]int64{3, 1}, nil)
		if s.Drift <= DefaultThresholds().Drift {
			t.Fatalf("premise broken: ρ=%v is not over threshold", s.Drift)
		}
		ok, why := s.Breach(spread, DefaultThresholds())
		if ok {
			t.Errorf("one misplaced pod out of 4 must not breach")
		}
		if why != "small-n: relocation below floor" {
			t.Errorf("reason = %q, want the small-n explanation", why)
		}
	})

	t.Run("small n with two pods to move does breach", func(t *testing.T) {
		s := scoreEqual([]int64{5, 1}, nil)
		if ok, _ := s.Breach(spread, DefaultThresholds()); !ok {
			t.Errorf("R=%d at n=6 should breach", s.Relocation)
		}
	})

	// A hard contract supersedes ρ: skew 2 against maxSkew 1 is a violation
	// even though ρ is tiny at this scale.
	t.Run("hard contract supersedes rho", func(t *testing.T) {
		s := scoreEqual([]int64{34, 33, 32}, &skew1)
		if s.Drift > DefaultThresholds().Drift {
			t.Fatalf("premise broken: ρ=%v is over threshold, test proves nothing", s.Drift)
		}
		if s.ExcessSkew == 0 {
			t.Fatalf("premise broken: no excess skew")
		}
		ok, why := s.Breach(hard, DefaultThresholds())
		if !ok {
			t.Errorf("declared maxSkew violation must breach regardless of ρ, got %q", why)
		}
	})

	// The S4 rule: the same numbers, sourced from a default nobody declared,
	// must not reach the contract path.
	t.Run("assumed cluster default cannot use the contract path", func(t *testing.T) {
		s := scoreEqual([]int64{34, 33, 32}, &skew1)
		if ok, _ := s.Breach(assumed, DefaultThresholds()); ok {
			t.Error("an assumed cluster default must not raise a contract breach (§8.1, §13 S4)")
		}
	})
}

func TestGateReason_String(t *testing.T) {
	want := map[GateReason]string{
		GateNone:             "",
		GateNoDomains:        "no eligible domains",
		GateNoObjects:        "no objects counted",
		GateBelowMinReplicas: "below minReplicasForScoring",
		GateModeIgnore:       "intent mode is Ignore",
	}
	seen := map[string]bool{}
	for g, w := range want {
		got := g.String()
		if got != w {
			t.Errorf("GateReason(%d) = %q, want %q", g, got, w)
		}
		if g != GateNone && seen[got] {
			t.Errorf("duplicate gate reason %q", got)
		}
		seen[got] = true
	}
	// GateNone rendering as "" is load-bearing: the reason is emitted into a
	// finding body, and "none" would read as a gate that fired.
	if GateNone.String() != "" {
		t.Error("GateNone must render empty")
	}
	if got := GateReason(9).String(); got != "unknown" {
		t.Errorf("out-of-range gate = %q, want unknown", got)
	}
}

func TestScores_ExpectedShares(t *testing.T) {
	s := scoreEqual([]int64{5, 3, 2}, nil)
	got := s.ExpectedShares()

	var sum float64
	for _, v := range got {
		sum += v
	}
	if !almostEqual(sum, 1, 1e-9) {
		t.Errorf("shares sum to %v, want 1", sum)
	}
	// Expected is [4,3,3] for 10 over 3.
	if !almostEqual(got[0], 0.4, 1e-9) {
		t.Errorf("shares = %v, want the first at 0.4", got)
	}

	// An all-zero expectation must yield zeros, not NaN: a NaN reaching the
	// metric exporter poisons the series rather than reading as "nothing here".
	empty := scoreEqual([]int64{0, 0}, nil)
	for i, v := range empty.ExpectedShares() {
		if v != 0 {
			t.Errorf("share[%d] = %v for an empty subject, want 0", i, v)
		}
	}
}

func TestScores_Escalate(t *testing.T) {
	s := scoreEqual([]int64{8, 1, 1}, nil)
	if !s.Escalate(DefaultThresholds()) {
		t.Errorf("max domain share %v should escalate past 0.5", s.MaxDomainShare)
	}
	balanced := scoreEqual([]int64{4, 3, 3}, nil)
	if balanced.Escalate(DefaultThresholds()) {
		t.Errorf("max domain share %v should not escalate", balanced.MaxDomainShare)
	}
}

func TestScore_ChiSquareValidity(t *testing.T) {
	small := scoreEqual([]int64{2, 1, 1}, nil)
	if small.ChiSquareValid {
		t.Error("χ² must be marked invalid when expected cells are under 5")
	}
	big := scoreEqual([]int64{20, 20, 20}, nil)
	if !big.ChiSquareValid {
		t.Error("χ² should be valid when every expected cell is ≥ 5")
	}
	if big.DegreesOfFreedom != 2 {
		t.Errorf("df = %d, want m−1 = 2", big.DegreesOfFreedom)
	}
}

// TestScore_RelocationIsHalfTheL1Distance is the identity that makes R
// meaningful as "how many objects must move": because the surplus and deficit
// sides are equal, counting one side is the number of moves, not half of it.
func TestScore_RelocationIsHalfTheL1Distance(t *testing.T) {
	r := propRand()
	for i := 0; i < 3000; i++ {
		m := 1 + r.IntN(8)
		actual := make([]int64, m)
		for j := range actual {
			actual[j] = int64(r.IntN(200))
		}
		s := scoreEqual(actual, nil)
		if s.Total == 0 {
			continue
		}

		var l1 int64
		for j := range actual {
			d := actual[j] - s.Expected[j]
			if d < 0 {
				d = -d
			}
			l1 += d
		}
		if l1%2 != 0 {
			t.Fatalf("L1 distance %d is odd, which is impossible when both sides sum to n (actual=%v expected=%v)", l1, actual, s.Expected)
		}
		if s.Relocation != l1/2 {
			t.Fatalf("R=%d != L1/2=%d (actual=%v expected=%v)", s.Relocation, l1/2, actual, s.Expected)
		}
	}
}

// TestScore_MetricsStayInRange checks the bounds every downstream consumer
// assumes: a ρ over 1 or a negative Herfindahl would break dashboards and
// threshold comparisons silently rather than loudly.
func TestScore_MetricsStayInRange(t *testing.T) {
	r := propRand()
	for i := 0; i < 3000; i++ {
		m := 1 + r.IntN(10)
		actual := make([]int64, m)
		for j := range actual {
			actual[j] = int64(r.IntN(500))
		}
		s := scoreEqual(actual, nil)
		if s.Total == 0 {
			continue
		}

		if s.Drift < 0 || s.Drift > 1 {
			t.Fatalf("ρ=%v out of [0,1] for %v", s.Drift, actual)
		}
		lower := 1.0 / float64(m)
		if s.Concentration < lower-1e-9 || s.Concentration > 1+1e-9 {
			t.Fatalf("H=%v out of [1/m, 1] for %v", s.Concentration, actual)
		}
		if s.MaxDomainShare < lower-1e-9 || s.MaxDomainShare > 1+1e-9 {
			t.Fatalf("maxDomainShare=%v out of [1/m, 1] for %v", s.MaxDomainShare, actual)
		}
		if s.ObservedSkew < 0 || s.Relocation < 0 || s.ExcessSkew < 0 {
			t.Fatalf("negative metric for %v: %+v", actual, s)
		}
		if math.IsNaN(s.Drift) || math.IsNaN(s.Concentration) || math.IsNaN(s.ChiSquare) {
			t.Fatalf("NaN metric for %v: %+v", actual, s)
		}
	}
}

// TestScore_PerfectPlacementIsSilent: whatever the weights, an actual
// distribution equal to the expectation must score zero drift. If this can
// fail, leeway reports findings for clusters doing exactly what was asked.
func TestScore_PerfectPlacementIsSilent(t *testing.T) {
	r := propRand()
	for i := 0; i < 2000; i++ {
		n := int64(r.IntN(1000))
		m := 1 + r.IntN(8)
		w := randomPositiveWeights(r, m)
		ap := Apportion(n, w, nil)

		s := Score(domains(m), ap.Expected, ap, nil, DefaultThresholds())
		if s.Total == 0 {
			continue
		}
		if s.Relocation != 0 || s.Drift != 0 {
			t.Fatalf("perfect placement scored R=%d ρ=%v (n=%d w=%v e=%v)", s.Relocation, s.Drift, n, w, ap.Expected)
		}
		if s.ExcessSkew != 0 {
			t.Fatalf("perfect placement scored excess skew %d (e=%v)", s.ExcessSkew, ap.Expected)
		}
		if ok, _ := s.Breach(&Intent{Mode: ModeSpread}, DefaultThresholds()); ok {
			t.Fatalf("perfect placement breached (n=%d w=%v e=%v)", n, w, ap.Expected)
		}
	}
}

// TestScore_SaturatedCapsRaiseTheFloor: when a cap forces an uneven
// expectation, the resulting skew is achievable-by-definition and must not be
// charged as excess.
func TestScore_SaturatedCapsRaiseTheFloor(t *testing.T) {
	ap := Apportion(12, EqualWeights(3), []int64{1, Uncapped, Uncapped})
	if got := ap.Expected; got[0] != 1 {
		t.Fatalf("premise broken: expected [1,6,5], got %v", got)
	}
	s := Score(domains(3), []int64{1, 6, 5}, ap, nil, DefaultThresholds())
	if s.MinAchievableSkew != 5 {
		t.Errorf("S* = %d, want 5 — the cap makes a 5-wide spread the best available", s.MinAchievableSkew)
	}
	if s.ExcessSkew != 0 {
		t.Errorf("ExcessSkew = %d, want 0 — matching a capped expectation exactly is not drift", s.ExcessSkew)
	}
	if s.Relocation != 0 {
		t.Errorf("Relocation = %d, want 0", s.Relocation)
	}
}

func TestScore_UnplaceableIsCarried(t *testing.T) {
	ap := Apportion(10, EqualWeights(2), []int64{2, 3})
	s := Score(domains(2), []int64{2, 3}, ap, nil, DefaultThresholds())
	if s.Unplaceable != 5 {
		t.Errorf("Unplaceable = %d, want 5 — a capacity shortfall must reach the finding, not vanish", s.Unplaceable)
	}
}
