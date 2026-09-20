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
	"math/rand"
	"testing"
	"time"
)

var baseT0 = time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

func threeZones() []Domain { return []Domain{"zone-a", "zone-b", "zone-c"} }

// mature drives a set to maturity on an even split, returning the time of the
// last sample.
func mature(t *testing.T, b *BaselineSet, cfg BaselineConfig, counts []int64) time.Time {
	t.Helper()
	cfg = cfg.Normalized()
	now := baseT0
	step := cfg.MinAge / time.Duration(cfg.MinSamples)
	for i := uint64(0); i <= cfg.MinSamples; i++ {
		now = now.Add(step)
		b.Observe(threeZones(), counts, now, cfg)
	}
	if !b.Mature(now, cfg) {
		t.Fatalf("set did not mature after %d samples over %s", b.Samples, cfg.MinAge)
	}
	return now
}

func TestBaselineConfig_NormalizedFillsEveryFieldClosed(t *testing.T) {
	got := BaselineConfig{}.Normalized()
	want := DefaultBaselineConfig()
	if got != want {
		t.Errorf("zero config normalized to %+v, want %+v", got, want)
	}

	// A widen window at or past the stale cutoff is nonsense — it would mean
	// "widen forever" — and must not silently become the policy.
	got = BaselineConfig{WidenAfter: 48 * time.Hour, StaleAfter: time.Hour}.Normalized()
	if got.StaleAfter != want.StaleAfter {
		t.Errorf("StaleAfter %v <= WidenAfter was kept; want the default %v", got.StaleAfter, want.StaleAfter)
	}

	// A multiplier below 1 would *narrow* the bands after an outage, which is
	// the opposite of §9.3's intent.
	if got := (BaselineConfig{WidenFactor: 0.5}).Normalized().WidenFactor; got != want.WidenFactor {
		t.Errorf("WidenFactor 0.5 kept as %v, want the default %v", got, want.WidenFactor)
	}
}

func TestBaseline_FirstSampleSeedsRatherThanConverges(t *testing.T) {
	cfg := DefaultBaselineConfig()
	b := NewBaselineSet(threeZones(), baseT0)

	if got := b.Observe(threeZones(), []int64{8, 1, 1}, baseT0.Add(time.Minute), cfg); got != ObserveSeeded {
		t.Fatalf("first Observe = %v, want %v", got, ObserveSeeded)
	}
	if got := b.Baselines[0].Share; !almostEqual(got, 0.8, 1e-9) {
		t.Errorf("seeded share = %v, want 0.8 exactly — a converged first sample is the warm-up bias §7.5 has to avoid", got)
	}
	if got := b.Baselines[0].Deviation; got != 0 {
		t.Errorf("seeded deviation = %v, want 0; there is nothing yet to deviate from", got)
	}
	if b.Samples != 1 {
		t.Errorf("Samples = %d, want 1", b.Samples)
	}
}

func TestBaseline_HalfLifeMovesAStepChangeHalfway(t *testing.T) {
	cfg := DefaultBaselineConfig()
	b := NewBaselineSet(threeZones(), baseT0)
	b.Observe(threeZones(), []int64{10, 0, 0}, baseT0, cfg)

	// One half-life later, a single sample at the new value.
	b.Observe(threeZones(), []int64{0, 0, 10}, baseT0.Add(cfg.HalfLife), cfg)

	if got := b.Baselines[0].Share; !almostEqual(got, 0.5, 1e-9) {
		t.Errorf("share after one half-life = %v, want 0.5 — alpha is not 1-2^(-dt/halfLife)", got)
	}
}

func TestBaseline_DtComesFromTheClockNotTheSampleCount(t *testing.T) {
	cfg := DefaultBaselineConfig()
	fast := NewBaselineSet(threeZones(), baseT0)
	slow := NewBaselineSet(threeZones(), baseT0)
	fast.Observe(threeZones(), []int64{10, 0, 0}, baseT0, cfg)
	slow.Observe(threeZones(), []int64{10, 0, 0}, baseT0, cfg)

	// Twelve samples over an hour against one sample an hour later. The same
	// elapsed time must produce (very nearly) the same estimate, or a subject
	// whose pods churn learns faster than a quiet one that is equally wrong.
	for i := 1; i <= 12; i++ {
		fast.Observe(threeZones(), []int64{0, 0, 10}, baseT0.Add(time.Duration(i)*5*time.Minute), cfg)
	}
	slow.Observe(threeZones(), []int64{0, 0, 10}, baseT0.Add(time.Hour), cfg)

	if d := math.Abs(fast.Baselines[0].Share - slow.Baselines[0].Share); d > 0.005 {
		t.Errorf("12 samples/hour and 1 sample/hour disagree by %v (%v vs %v)", d, fast.Baselines[0].Share, slow.Baselines[0].Share)
	}
}

func TestBaseline_FrozenLearnsNothingAndDoesNotCatchUpOnThaw(t *testing.T) {
	cfg := DefaultBaselineConfig()
	b := NewBaselineSet(threeZones(), baseT0)
	b.Observe(threeZones(), []int64{4, 3, 3}, baseT0, cfg)
	before := b.Baselines[0].Share

	b.SetFrozen(true)
	for i := 1; i <= 6; i++ {
		if got := b.Observe(threeZones(), []int64{10, 0, 0}, baseT0.Add(time.Duration(i)*time.Hour), cfg); got != ObserveHeld {
			t.Fatalf("frozen Observe = %v, want %v", got, ObserveHeld)
		}
	}
	if b.Baselines[0].Share != before || b.Samples != 1 {
		t.Fatalf("frozen set moved: share %v -> %v, samples %d", before, b.Baselines[0].Share, b.Samples)
	}

	// The thaw is the part that matters. If freezing had not advanced
	// UpdatedAt, the first post-thaw sample would carry dt=6h and absorb ~30%
	// of the drift the freeze existed to keep out.
	b.SetFrozen(false)
	b.Observe(threeZones(), []int64{10, 0, 0}, baseT0.Add(6*time.Hour+30*time.Second), cfg)
	moved := b.Baselines[0].Share - before
	if moved > 0.01 {
		t.Errorf("first sample after a 6h freeze moved the share by %v; freezing must stop the clock, not just the arithmetic", moved)
	}
}

func TestBaseline_DomainSetChangeResetsButAReorderDoesNot(t *testing.T) {
	cfg := DefaultBaselineConfig()
	b := NewBaselineSet(threeZones(), baseT0)
	last := mature(t, b, cfg, []int64{4, 3, 3})

	// Same set, different order: no reset.
	if got := b.Observe([]Domain{"zone-c", "zone-a", "zone-b"}, []int64{3, 4, 3}, last.Add(time.Minute), cfg); got != ObserveApplied {
		t.Fatalf("reordered domains = %v, want %v — the fingerprint is the set, not the caller's slice order", got, ObserveApplied)
	}
	if !b.Mature(last.Add(time.Minute), cfg) {
		t.Error("a reorder invalidated the baseline")
	}

	// A fourth zone: a different cluster, so the estimate goes.
	now := last.Add(2 * time.Minute)
	if got := b.Observe([]Domain{"zone-a", "zone-b", "zone-c", "zone-d"}, []int64{3, 3, 2, 2}, now, cfg); got != ObserveReset {
		t.Fatalf("new domain = %v, want %v", got, ObserveReset)
	}
	if b.Samples != 1 || !b.FirstSeen.Equal(now) {
		t.Errorf("after reset Samples=%d FirstSeen=%v, want 1 and %v", b.Samples, b.FirstSeen, now)
	}
	if b.Mature(now.Add(24*time.Hour), cfg) {
		t.Error("a reset set is mature on age alone; the sample count must restart too")
	}
}

func TestBaseline_ASubstitutedDomainIsADifferentClusterToo(t *testing.T) {
	// Same count, same order, one name swapped: a zone drained and another
	// came up. Nothing about the sizes changed, and the old shares still
	// describe placement that no longer exists.
	cfg := DefaultBaselineConfig()
	b := NewBaselineSet(threeZones(), baseT0)
	b.Observe(threeZones(), []int64{4, 3, 3}, baseT0, cfg)

	got := b.Observe([]Domain{"zone-a", "zone-b", "zone-d"}, []int64{4, 3, 3}, baseT0.Add(time.Minute), cfg)
	if got != ObserveReset {
		t.Errorf("substituted domain = %v, want %v", got, ObserveReset)
	}
}

func TestBaseline_SetFrozenReportsOnlyTheEdges(t *testing.T) {
	b := NewBaselineSet(threeZones(), baseT0)
	if !b.SetFrozen(true) {
		t.Error("the first freeze reported no change")
	}
	if b.SetFrozen(true) {
		t.Error("freezing an already-frozen set reported a change; the caller would log every pass")
	}
	if !b.SetFrozen(false) {
		t.Error("the thaw reported no change")
	}
}

func TestEwmaAlpha_DegenerateInputs(t *testing.T) {
	// A clock that went backwards, or two samples in one instant. Weighting
	// either would move the estimate away from the sample.
	if got := ewmaAlpha(-time.Hour, time.Hour); got != 0 {
		t.Errorf("alpha for a negative dt = %v, want 0", got)
	}
	if got := ewmaAlpha(0, time.Hour); got != 0 {
		t.Errorf("alpha for a zero dt = %v, want 0", got)
	}
	// A zero half-life is "forget everything immediately", which is what a
	// caller that bypassed Normalized asked for.
	if got := ewmaAlpha(time.Second, 0); got != 1 {
		t.Errorf("alpha for a zero half-life = %v, want 1", got)
	}
}

func TestBaseline_ReorderedCountsFollowTheirDomains(t *testing.T) {
	cfg := DefaultBaselineConfig()
	b := NewBaselineSet(threeZones(), baseT0)
	b.Observe([]Domain{"zone-c", "zone-b", "zone-a"}, []int64{10, 0, 0}, baseT0, cfg)

	// zone-c held everything, and zone-c sorts last.
	if got := b.Baselines[2].Share; !almostEqual(got, 1, 1e-9) {
		t.Errorf("zone-c share = %v, want 1 — counts were not reordered with their domains", got)
	}
	if got := b.Baselines[0].Share; got != 0 {
		t.Errorf("zone-a share = %v, want 0", got)
	}
}

func TestBaseline_NothingToLearnFromIsNotASample(t *testing.T) {
	cfg := DefaultBaselineConfig()
	b := NewBaselineSet(threeZones(), baseT0)
	b.Observe(threeZones(), []int64{4, 3, 3}, baseT0, cfg)

	for name, call := range map[string]func() ObserveOutcome{
		"scaled to zero":  func() ObserveOutcome { return b.Observe(threeZones(), []int64{0, 0, 0}, baseT0.Add(time.Hour), cfg) },
		"no domains":      func() ObserveOutcome { return b.Observe(nil, nil, baseT0.Add(time.Hour), cfg) },
		"length mismatch": func() ObserveOutcome { return b.Observe(threeZones(), []int64{1, 1}, baseT0.Add(time.Hour), cfg) },
	} {
		if got := call(); got != ObserveEmpty {
			t.Errorf("%s: Observe = %v, want %v", name, got, ObserveEmpty)
		}
	}
	if b.Samples != 1 {
		t.Errorf("Samples = %d after three empty observations, want 1", b.Samples)
	}
}

func TestBaseline_MaturityNeedsBothGates(t *testing.T) {
	cfg := DefaultBaselineConfig().Normalized()
	b := NewBaselineSet(threeZones(), baseT0)

	// 200 samples in ten minutes: a very good estimate of ten minutes.
	now := baseT0
	for i := uint64(0); i < cfg.MinSamples+5; i++ {
		now = now.Add(3 * time.Second)
		b.Observe(threeZones(), []int64{4, 3, 3}, now, cfg)
	}
	if b.Mature(now, cfg) {
		t.Error("matured on sample count alone after ten minutes")
	}
	if !b.Mature(baseT0.Add(cfg.MinAge), cfg) {
		t.Error("did not mature once both gates were satisfied")
	}

	// Age alone is not enough either.
	slow := NewBaselineSet(threeZones(), baseT0)
	slow.Observe(threeZones(), []int64{4, 3, 3}, baseT0, cfg)
	if slow.Mature(baseT0.Add(72*time.Hour), cfg) {
		t.Error("matured on age alone with one sample")
	}

	var nilSet *BaselineSet
	if nilSet.Mature(baseT0, cfg) {
		t.Error("a nil set is mature")
	}
}

func TestBaseline_BandIsKTimesTheFlooredDeviation(t *testing.T) {
	cfg := DefaultBaselineConfig()
	b := NewBaselineSet(threeZones(), baseT0)
	b.Observe(threeZones(), []int64{4, 3, 3}, baseT0, cfg)

	// A perfectly steady subject learns a deviation of zero; the floor is what
	// stops its band collapsing to nothing.
	if got, want := b.Band(0, baseT0, cfg), cfg.K*cfg.FloorDeviation; !almostEqual(got, want, 1e-9) {
		t.Errorf("band on a zero deviation = %v, want the floored %v", got, want)
	}

	// Half the weight accumulated, half the raw deviation: the corrected
	// dispersion is 0.2, and that is what the band is built from.
	b.Baselines[0].Deviation = 0.1
	b.DevWeight = 0.5
	if got, want := b.Band(0, baseT0, cfg), cfg.K*0.2; !almostEqual(got, want, 1e-9) {
		t.Errorf("band = %v, want %v — the band must use the warm-up-corrected deviation", got, want)
	}

	// Out-of-range indexes answer rather than panic; the caller is iterating a
	// domain list that a concurrent invalidation may have shortened.
	if got := b.Band(99, baseT0, cfg); got != 0 {
		t.Errorf("Band(99) = %v, want 0", got)
	}
}

// TestBaseline_DeviationIsCorrectedForTheWarmUp pins the bias correction with
// a subject whose true mean absolute deviation is known by construction: it
// alternates between two placements, so every sample sits a fixed distance
// from the centre.
//
// Without the correction the estimate at maturity is short by ~3.5× at the
// default 12 h half-life and 6 h maturity gate, and every band built from it
// is 3.5× too tight.
func TestBaseline_DeviationIsCorrectedForTheWarmUp(t *testing.T) {
	cfg := DefaultBaselineConfig().Normalized()
	b := NewBaselineSet(threeZones(), baseT0)

	now := baseT0
	step := cfg.MinAge / time.Duration(cfg.MinSamples)
	for i := uint64(0); i <= 4*cfg.MinSamples; i++ {
		now = now.Add(step)
		counts := []int64{60, 20, 20}
		if i%2 == 1 {
			counts = []int64{40, 30, 30}
		}
		b.Observe(threeZones(), counts, now, cfg)
	}

	// zone-a alternates 0.6 and 0.4 about a centre of 0.5, so the mean
	// absolute deviation is 0.1.
	if got := b.DeviationOf(0); !almostEqual(got, 0.1, 0.01) {
		t.Errorf("corrected deviation = %v, want ~0.1", got)
	}
	if raw := b.Baselines[0].Deviation; raw >= b.DeviationOf(0) {
		t.Errorf("raw deviation %v is not below the corrected %v; the weight is not accumulating", raw, b.DeviationOf(0))
	}

	// An estimate with no weight yet answers zero rather than dividing by it.
	fresh := NewBaselineSet(threeZones(), baseT0)
	fresh.Observe(threeZones(), []int64{1, 1, 1}, baseT0, cfg)
	if got := fresh.DeviationOf(0); got != 0 {
		t.Errorf("DeviationOf on a seed = %v, want 0", got)
	}
	if got := fresh.DeviationOf(99); got != 0 {
		t.Errorf("DeviationOf(99) = %v, want 0", got)
	}
}

func TestBaseline_AssessDowntimeAppliesTheThreeStepSevenRows(t *testing.T) {
	cfg := DefaultBaselineConfig().Normalized()

	t.Run("short gap resumes untouched", func(t *testing.T) {
		b := NewBaselineSet(threeZones(), baseT0)
		last := mature(t, b, cfg, []int64{4, 3, 3})
		now := last.Add(cfg.WidenAfter - time.Minute)
		if got := b.AssessDowntime(now, cfg); got != DowntimeResumed {
			t.Fatalf("AssessDowntime = %v, want %v", got, DowntimeResumed)
		}
		if b.WidenBy != 0 || !b.Mature(now, cfg) {
			t.Errorf("a short gap changed the baseline: widenBy=%v mature=%v", b.WidenBy, b.Mature(now, cfg))
		}
	})

	t.Run("medium gap widens for one half-life", func(t *testing.T) {
		b := NewBaselineSet(threeZones(), baseT0)
		last := mature(t, b, cfg, []int64{4, 3, 3})
		b.Baselines[0].Deviation = 0.1
		narrow := b.Band(0, last, cfg)

		now := last.Add(cfg.WidenAfter + time.Minute)
		if got := b.AssessDowntime(now, cfg); got != DowntimeWidened {
			t.Fatalf("AssessDowntime = %v, want %v", got, DowntimeWidened)
		}
		if got, want := b.Band(0, now, cfg), narrow*cfg.WidenFactor; !almostEqual(got, want, 1e-9) {
			t.Errorf("widened band = %v, want %v", got, want)
		}
		// And it expires, rather than being a permanent loosening.
		if got := b.Band(0, now.Add(cfg.HalfLife+time.Second), cfg); !almostEqual(got, narrow, 1e-9) {
			t.Errorf("band one half-life later = %v, want back to %v", got, narrow)
		}
		if !b.Mature(now, cfg) {
			t.Error("a widened baseline stopped being mature; §9.3 says resume, not suppress")
		}
	})

	t.Run("long gap restarts the maturity clock but keeps the shares", func(t *testing.T) {
		b := NewBaselineSet(threeZones(), baseT0)
		last := mature(t, b, cfg, []int64{8, 1, 1})
		shares := b.Baselines[0].Share

		now := last.Add(cfg.StaleAfter + time.Hour)
		if got := b.AssessDowntime(now, cfg); got != DowntimeStale {
			t.Fatalf("AssessDowntime = %v, want %v", got, DowntimeStale)
		}
		if b.Mature(now.Add(72*time.Hour), cfg) {
			t.Error("a stale baseline is still mature; Tier C must be suppressed until it re-matures")
		}
		if b.Baselines[0].Share != shares {
			t.Errorf("stale reset discarded the shares (%v -> %v); they are the best available seed", shares, b.Baselines[0].Share)
		}
	})
}

func TestBaseline_IntentIsNilUntilMatureAndThenIsTierCShaped(t *testing.T) {
	cfg := DefaultBaselineConfig()
	b := NewBaselineSet(threeZones(), baseT0)
	b.Observe(threeZones(), []int64{8, 1, 1}, baseT0, cfg)

	if got := b.Intent("topology.kubernetes.io/zone", baseT0, cfg); got != nil {
		t.Fatalf("immature baseline produced an intent: %+v", got)
	}

	last := mature(t, b, cfg, []int64{8, 1, 1})
	in := b.Intent("topology.kubernetes.io/zone", last, cfg)
	if in == nil {
		t.Fatal("mature baseline produced no intent")
	}
	if in.Source != SourceLearnedBaseline || in.Confidence != ConfidenceLearned {
		t.Errorf("source/confidence = %v/%v, want learned/learned", in.Source, in.Confidence)
	}
	if in.Mode != ModeSpread {
		t.Errorf("mode = %v, want spread", in.Mode)
	}
	if got := in.ExplicitShares["zone-a"]; !almostEqual(got, 0.8, 1e-6) {
		t.Errorf("learned share for zone-a = %v, want ~0.8", got)
	}
	if len(in.Bands) != 3 {
		t.Errorf("bands = %d, want one per domain", len(in.Bands))
	}
	if in.HardContract() || in.HardSkewContract() || in.HardPerDomainContract() {
		t.Error("a learned baseline carries a hard contract; it is a measurement, not a promise")
	}
	if got := classify(in, BreachBaseline); got != TierC {
		t.Errorf("classify = %v, want TierC", got)
	}
	if got := FindingKind(TierC, in); got != KindBaselineBreach {
		t.Errorf("FindingKind = %q, want %q", got, KindBaselineBreach)
	}
	if len(in.Evidence) != 1 || in.Evidence[0].Source != SourceLearnedBaseline {
		t.Errorf("evidence = %+v, want one learned-baseline line", in.Evidence)
	}
}

func TestCauseConfig_NormalizedIsTheExportedNormalize(t *testing.T) {
	// The source sizes its ready-count retention off Window, so the exported
	// accessor has to fill the defaults the internal one does.
	if got, want := (CauseConfig{}).Normalized(), DefaultCauseConfig(); got != want {
		t.Errorf("zero CauseConfig normalized to %+v, want %+v", got, want)
	}
}

func TestBaseline_CloneIsIndependent(t *testing.T) {
	cfg := DefaultBaselineConfig()
	b := NewBaselineSet(threeZones(), baseT0)
	b.Observe(threeZones(), []int64{4, 3, 3}, baseT0, cfg)

	c := b.Clone()
	c.Baselines[0].Share = 99
	c.Domains[0] = "mutated"
	if b.Baselines[0].Share == 99 || b.Domains[0] == "mutated" {
		t.Error("Clone shares backing arrays with the original")
	}

	var nilSet *BaselineSet
	if nilSet.Clone() != nil {
		t.Error("nil.Clone() is not nil")
	}
}

func TestObserveOutcome_And_DowntimeVerdict_Strings(t *testing.T) {
	for v, want := range map[ObserveOutcome]string{
		ObserveApplied: "applied", ObserveSeeded: "seeded", ObserveReset: "reset",
		ObserveHeld: "held", ObserveEmpty: "empty", ObserveOutcome(99): "unknown",
	} {
		if got := v.String(); got != want {
			t.Errorf("ObserveOutcome(%d) = %q, want %q", v, got, want)
		}
	}
	for v, want := range map[DowntimeVerdict]string{
		DowntimeResumed: "resumed", DowntimeWidened: "widened",
		DowntimeStale: "stale", DowntimeVerdict(99): "unknown",
	} {
		if got := v.String(); got != want {
			t.Errorf("DowntimeVerdict(%d) = %q, want %q", v, got, want)
		}
	}
	if got := BreachBaseline.String(); got != "baseline" {
		t.Errorf("BreachBaseline.String() = %q", got)
	}
	if BreachBaseline.Contract() {
		t.Error("a learned baseline is a contract; it is nobody's promise")
	}
}

// TestBaseline_SharesStaySummingToOne is the property that keeps the learned
// expectation usable as apportionment weights: whatever the sample sequence,
// the EWMAs of shares over a fixed domain set must themselves sum to one,
// because each individual EWMA is a convex combination of values that did.
func TestBaseline_SharesStaySummingToOne(t *testing.T) {
	cfg := DefaultBaselineConfig()
	rng := rand.New(rand.NewSource(20260920))
	b := NewBaselineSet(threeZones(), baseT0)

	now := baseT0
	for i := 0; i < 2000; i++ {
		now = now.Add(time.Duration(1+rng.Intn(600)) * time.Second)
		counts := []int64{int64(rng.Intn(40)), int64(rng.Intn(40)), int64(rng.Intn(40))}
		if counts[0]+counts[1]+counts[2] == 0 {
			counts[0] = 1
		}
		b.Observe(threeZones(), counts, now, cfg)

		var sum float64
		for _, e := range b.Baselines {
			if e.Share < 0 || e.Share > 1 {
				t.Fatalf("iteration %d: share out of range: %v", i, e.Share)
			}
			sum += e.Share
		}
		if !almostEqual(sum, 1, 1e-9) {
			t.Fatalf("iteration %d: shares sum to %v", i, sum)
		}
	}
}
