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
)

// quickBaseline matures after three samples and no wait, so a test can reach
// a judgeable baseline without two hundred Observe calls.
func quickBaseline() BaselineConfig {
	c := DefaultBaselineConfig()
	c.MinSamples = 3
	c.MinAge = time.Nanosecond
	return c
}

// windowAt is an hour-long window with the given pod-seconds per rank.
func windowAt(secs map[Rank]float64) RankWindow {
	return RankWindow{Elapsed: time.Hour, PodSeconds: secs}
}

// learn feeds windows to a fresh set one minute apart and returns it with the
// instant of the last sample.
func learn(t *testing.T, axis *PreferenceAxis, cfg BaselineConfig, ws ...RankWindow) (*BaselineSet, time.Time) {
	t.Helper()
	at := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	domains, _ := RankBaselineSample(axis, ws[0])
	b := NewBaselineSet(domains, at)
	for i, w := range ws {
		at = at.Add(time.Minute)
		d, a := RankBaselineSample(axis, w)
		if out := b.Observe(d, a, at, cfg); out == ObserveEmpty || out == ObserveReset {
			t.Fatalf("sample %d: Observe = %s", i, out)
		}
	}
	return b, at
}

// Every tier is in the sample, occupied or not. A recovery that empties the
// fallback for a window is a change in the shares, and must not read as a
// different axis — which would reset six hours of learning every time the
// class got better.
func TestRankBaselineSample_EveryTierEvenWhenEmpty(t *testing.T) {
	axis := judgeClass(t, n4PreferredSpec).Axis
	domains, actual := RankBaselineSample(axis, windowAt(map[Rank]float64{0: 3599.6, RankUnknown: 50}))
	if len(domains) != 3 || len(actual) != 3 {
		t.Fatalf("domains=%v actual=%v, want all three tiers", domains, actual)
	}
	if actual[0] != 3600 || actual[1] != 0 || actual[2] != 0 {
		t.Errorf("actual = %v, want [3600 0 0] — rounded, and a sentinel rank counted nowhere", actual)
	}
	for i, d := range domains {
		if !strings.HasPrefix(string(d), axis.SpecHash+"/") {
			t.Errorf("domain %d = %q does not carry the spec hash", i, d)
		}
	}
}

// The mean is the EWMA of the window means, exactly, because the EWMA is
// linear — which is the whole case for learning it through the share
// estimator rather than a scalar of its own.
func TestRankBaselineOf_MeanIsTheEWMAOfTheMean(t *testing.T) {
	axis := judgeClass(t, n4PreferredSpec).Axis
	cfg := quickBaseline()
	ws := []RankWindow{
		windowAt(map[Rank]float64{0: 3000, 1: 600}),          // mean 1/6
		windowAt(map[Rank]float64{0: 1800, 1: 1200, 2: 600}), // mean 2/3
		windowAt(map[Rank]float64{0: 3600}),                  // mean 0
		windowAt(map[Rank]float64{1: 1800, 2: 1800}),         // mean 1.5
		windowAt(map[Rank]float64{0: 2400, 2: 1200}),         // mean 2/3
	}
	b, at := learn(t, axis, cfg, ws...)

	alpha := ewmaAlpha(time.Minute, cfg.HalfLife)
	want := ws[0].MeanRank()
	for _, w := range ws[1:] {
		want += alpha * (w.MeanRank() - want)
	}
	got := RankBaselineOf(b, axis, at, cfg)
	if got == nil {
		t.Fatal("a mature set rendered no baseline")
	}
	if math.Abs(got.Mean-want) > 1e-9 {
		t.Errorf("Mean = %v, want %v", got.Mean, want)
	}
	if got.Samples != uint64(len(ws)) {
		t.Errorf("Samples = %d, want %d", got.Samples, len(ws))
	}
}

// On a two-tier axis the mean IS the rank-1 share, so the band is exactly the
// rank-1 band. Longer axes get the triangle-inequality upper bound, which is
// wider — the direction §7.5 fails.
func TestRankBaselineOf_BandIsExactOnTwoTiersAndWiderOnMore(t *testing.T) {
	cfg := quickBaseline()
	two := judgeClass(t, s2ScoredSpec).Axis
	if two.Tiers != 2 {
		t.Fatalf("fixture has %d tiers, want 2", two.Tiers)
	}
	b, at := learn(t, two, cfg,
		windowAt(map[Rank]float64{0: 3000, 1: 600}),
		windowAt(map[Rank]float64{0: 1800, 1: 1800}),
		windowAt(map[Rank]float64{0: 3600}),
		windowAt(map[Rank]float64{0: 2000, 1: 1600}),
	)
	got := RankBaselineOf(b, two, at, cfg)
	var rank1 int
	for i, d := range b.Domains {
		if r, _ := rankOfDomain(two, d); r == 1 {
			rank1 = i
		}
	}
	if want := b.Band(rank1, at, cfg); math.Abs(got.Band-want) > 1e-12 {
		t.Errorf("two-tier Band = %v, want the rank-1 band %v", got.Band, want)
	}
	if math.Abs(got.Mean-b.Baselines[rank1].Share) > 1e-12 {
		t.Errorf("two-tier Mean = %v, want the rank-1 share %v", got.Mean, b.Baselines[rank1].Share)
	}

	// Three tiers, a set that has never moved: every band is the floor, so the
	// mean's band is k·floor·(1+2).
	three := judgeClass(t, n4PreferredSpec).Axis
	steady := windowAt(map[Rank]float64{0: 3600})
	b3, at3 := learn(t, three, cfg, steady, steady, steady)
	got3 := RankBaselineOf(b3, three, at3, cfg)
	if want := cfg.K * cfg.FloorDeviation * 3; math.Abs(got3.Band-want) > 1e-12 {
		t.Errorf("three-tier floor Band = %v, want %v", got3.Band, want)
	}
	if got3.Mean != 0 || got3.Ceiling() != got3.Band {
		t.Errorf("steady at rank 0: Mean = %v, Ceiling = %v", got3.Mean, got3.Ceiling())
	}
}

func TestRankBaselineOf_ImmatureIsNil(t *testing.T) {
	axis := judgeClass(t, n4PreferredSpec).Axis
	cfg := DefaultBaselineConfig()
	b, at := learn(t, axis, cfg, windowAt(map[Rank]float64{0: 3600}))
	if got := RankBaselineOf(b, axis, at, cfg); got != nil {
		t.Errorf("one sample rendered a baseline: %+v", got)
	}
	if got := RankBaselineOf(nil, axis, at, cfg); got != nil {
		t.Errorf("a nil set rendered a baseline: %+v", got)
	}
}

// A re-tiering changes the spec hash, so the set learned against the old list
// stops being a baseline for the new one at once — before the estimator has
// seen a sample of the new tiers and reset itself — and the first such sample
// does reset it.
func TestRankBaselineOf_ARetieringInvalidatesTheLearnedNormal(t *testing.T) {
	cfg := quickBaseline()
	old := judgeClass(t, n4PreferredSpec).Axis
	steady := windowAt(map[Rank]float64{0: 3600})
	b, at := learn(t, old, cfg, steady, steady, steady)
	if RankBaselineOf(b, old, at, cfg) == nil {
		t.Fatal("fixture did not mature")
	}

	retiered := judgeClass(t, strings.Replace(n4PreferredSpec, `"c3"`, `"c4"`, 1)).Axis
	if retiered.SpecHash == old.SpecHash || retiered.Tiers != old.Tiers {
		t.Fatalf("fixture must re-tier without changing the tier count: %s/%d vs %s/%d",
			old.SpecHash, old.Tiers, retiered.SpecHash, retiered.Tiers)
	}
	if got := RankBaselineOf(b, retiered, at, cfg); got != nil {
		t.Errorf("the old list's normal was served for the new one: %+v", got)
	}
	d, a := RankBaselineSample(retiered, steady)
	if out := b.Observe(d, a, at.Add(time.Minute), cfg); out != ObserveReset {
		t.Errorf("first sample of the new tiers: Observe = %s, want reset", out)
	}

	// A set over a different number of tiers is refused on the count alone.
	two := judgeClass(t, s2ScoredSpec).Axis
	if got := RankBaselineOf(b, two, at, cfg); got != nil {
		t.Errorf("a three-tier set was served for a two-tier axis: %+v", got)
	}
}

func TestRankOfDomain_RefusesWhatItDidNotWrite(t *testing.T) {
	axis := judgeClass(t, n4PreferredSpec).Axis
	for _, d := range []Domain{
		"no-separator",
		Domain("someotherhash/1"),
		Domain(axis.SpecHash + "/x"),
		Domain(axis.SpecHash + "/-1"),
	} {
		if r, ok := rankOfDomain(axis, d); ok {
			t.Errorf("rankOfDomain(%q) = %s, want refused", d, r)
		}
	}
	if r, ok := rankOfDomain(axis, RankDomain(axis, 2)); !ok || r != 2 {
		t.Errorf("round trip = %s, %v", r, ok)
	}

	// A set carrying a domain of this hash that is not one of the axis's
	// tiers is not this axis's baseline.
	cfg := quickBaseline()
	b := &BaselineSet{
		Domains:   []Domain{RankDomain(axis, 0), RankDomain(axis, 1), RankDomain(axis, 7)},
		Baselines: make([]Baseline, 3),
		Samples:   10,
		FirstSeen: time.Unix(0, 0),
	}
	if got := RankBaselineOf(b, axis, time.Now(), cfg); got != nil {
		t.Errorf("a set with a tier the axis does not have was served: %+v", got)
	}
}

// The issue's motivating shape: a class whose normal is a mean rank of 0.3
// drifts to 1.4. No absolute rule sees it — the last-rank share is well under
// its 0.9 ceiling because most of the estate is on rank 1, not the last rung —
// and the baseline rule does.
func TestJudgeRank_BaselineCatchesWhatTheAbsoluteRulesCannot(t *testing.T) {
	class := judgeClass(t, n4PreferredSpec)
	base := &RankBaseline{Mean: 0.3, Band: 0.4, Samples: 400, FirstSeen: time.Unix(0, 0)}
	drifted := windowAt(map[Rank]float64{0: 360, 1: 1440, 2: 1800}) // mean 1.4, last-rank 0.5
	if m := drifted.MeanRank(); math.Abs(m-1.4) > 1e-9 {
		t.Fatalf("fixture mean = %v", m)
	}
	vs := JudgeRank(RankInput{Class: class, Window: drifted, Baseline: base}, DefaultRankThresholds())

	if v := ruleOf(t, vs, RankRuleLastRank); v.Breached {
		t.Errorf("last-rank breached on a 0.5 share — the fixture no longer isolates the baseline rule")
	}
	v := ruleOf(t, vs, RankRuleBaseline)
	if !v.Breached || v.Kind != KindRankDegraded || v.Tier != TierC || v.Severity != TierC.Severity() {
		t.Fatalf("baseline verdict = %+v", v)
	}
	if !strings.Contains(v.Reason, "1.40") || !strings.Contains(v.Reason, "0.30") {
		t.Errorf("reason should quote both means: %q", v.Reason)
	}
	for _, other := range vs {
		if other.Observed.Baseline != base {
			t.Errorf("%s verdict does not carry the baseline", other.EpisodeKey())
		}
	}
	if d := RouteRank(v, false); d.Signal {
		t.Error("a Tier C baseline breach reached a sink with tier-C signals off")
	}
	if d := RouteRank(v, true); !d.Signal {
		t.Error("a Tier C baseline breach was withheld with tier-C signals on")
	}
}

func TestJudgeRank_BaselineAbstainsAndHolds(t *testing.T) {
	class := judgeClass(t, n4PreferredSpec)
	base := &RankBaseline{Mean: 0.3, Band: 0.4}
	for _, tc := range []struct {
		name     string
		baseline *RankBaseline
		window   RankWindow
		reason   string
	}{
		{"no-baseline", nil, windowAt(map[Rank]float64{2: 3600}), "no mature baseline"},
		{"short-window", base, RankWindow{Elapsed: time.Minute, PodSeconds: map[Rank]float64{2: 60}}, "shorter than"},
		{"idle-window", base, windowAt(nil), "no pod-seconds"},
		{"at-the-ceiling", base, windowAt(map[Rank]float64{0: 1080, 1: 2520}), "within"}, // mean 0.7
		{"better-than-normal", base, windowAt(map[Rank]float64{0: 3600}), "within"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := ruleOf(t, JudgeRank(RankInput{Class: class, Window: tc.window, Baseline: tc.baseline}, DefaultRankThresholds()), RankRuleBaseline)
			if v.Breached {
				t.Errorf("breached: %s", v.Reason)
			}
			if !strings.Contains(v.Reason, tc.reason) {
				t.Errorf("reason = %q, want it to mention %q", v.Reason, tc.reason)
			}
		})
	}
}
