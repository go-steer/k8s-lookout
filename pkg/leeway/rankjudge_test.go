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

// judgeClass decodes one of the shared spec fixtures.
func judgeClass(t *testing.T, doc string) *ComputeClass {
	t.Helper()
	class, err := DecodeComputeClass("test", specFromJSON(t, doc))
	if err != nil {
		t.Fatalf("fixture does not decode: %v", err)
	}
	return class
}

// steadyWindow is an hour in which everything sat at rank 0: the shape that
// must produce no breach on any rule.
func steadyWindow() RankWindow {
	return RankWindow{
		Elapsed:    time.Hour,
		PodSeconds: map[Rank]float64{0: 36000},
		Pods:       map[Rank]int{0: 10},
	}
}

// ruleOf finds one rule's verdict, failing if the judge did not return it.
func ruleOf(t *testing.T, vs []RankVerdict, rule RankRule) RankVerdict {
	t.Helper()
	for _, v := range vs {
		if v.Rule == rule && v.Focus == RankUnknown {
			return v
		}
	}
	t.Fatalf("no axis-wide verdict for rule %s in %d verdicts", rule, len(vs))
	return RankVerdict{}
}

// tierOf finds one per-tier verdict.
func tierOf(t *testing.T, vs []RankVerdict, rule RankRule, focus Rank) RankVerdict {
	t.Helper()
	for _, v := range vs {
		if v.Rule == rule && v.Focus == focus {
			return v
		}
	}
	t.Fatalf("no verdict for rule %s at rank %s", rule, focus)
	return RankVerdict{}
}

// breached lists the rules that fired, for the tests whose assertion is about
// the whole set rather than about one rule.
func breached(vs []RankVerdict) []string {
	var out []string
	for _, v := range vs {
		if v.Breached {
			out = append(out, v.EpisodeKey())
		}
	}
	return out
}

func TestJudgeRank_ASteadyAxisBreachesNothing(t *testing.T) {
	in := RankInput{
		Class:      judgeClass(t, n4PreferredSpec),
		Window:     steadyWindow(),
		Conditions: RankConditions{Lifetime: time.Hour},
	}
	vs := JudgeRank(in, DefaultRankThresholds())
	if got := breached(vs); len(got) != 0 {
		t.Errorf("a class sitting entirely at rank 0 breached %v", got)
	}
}

// TestJudgeRank_EveryApplicableRuleIsReturnedEvenWhenQuiet is the property the
// §8.2 machine depends on: an episode resolves by being handed a non-breaching
// verdict, so a judge that returned only breaches would leave every episode
// open forever.
func TestJudgeRank_EveryApplicableRuleIsReturnedEvenWhenQuiet(t *testing.T) {
	in := RankInput{
		Class:      judgeClass(t, n4PreferredSpec),
		Window:     steadyWindow(),
		Conditions: RankConditions{Lifetime: time.Hour},
	}
	vs := JudgeRank(in, DefaultRankThresholds())

	want := map[string]bool{
		"wedged": false, "rank0-share": false, "last-rank": false, "no-migration": false,
		"tier-unused/0": false, "tier-unused/1": false, "tier-unused/2": false,
	}
	for _, v := range vs {
		key := v.EpisodeKey()
		if _, ok := want[key]; !ok {
			t.Errorf("unexpected verdict %q", key)
			continue
		}
		want[key] = true
	}
	for key, seen := range want {
		if !seen {
			t.Errorf("no verdict for %q — an open episode on that rule could never resolve", key)
		}
	}
}

// TestJudgeRank_AnUnscorableAxisIsJudgedByNothing covers the four GKE-managed
// Autopilot classes, each of which declares exactly one priority. Without this
// every GKE cluster starts with four axes of guaranteed-silent noise.
func TestJudgeRank_AnUnscorableAxisIsJudgedByNothing(t *testing.T) {
	for _, tc := range []struct{ name, spec string }{
		{"one priority", `{"priorities":[{"machineFamily":"n2"}],"whenUnsatisfiable":"DoNotScaleUp"}`},
		{"two rules, one tier", `{"priorities":[{"machineFamily":"n2","priorityScore":5},{"machineFamily":"n4","priorityScore":5}],"whenUnsatisfiable":"DoNotScaleUp"}`},
		{"partially scored", `{"priorities":[{"machineFamily":"n2"},{"machineFamily":"n4","priorityScore":50}],"whenUnsatisfiable":"DoNotScaleUp"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := RankInput{
				Class:      judgeClass(t, tc.spec),
				Conditions: RankConditions{PendingPods: 7, Lifetime: 90 * 24 * time.Hour},
			}
			if vs := JudgeRank(in, DefaultRankThresholds()); len(vs) != 0 {
				t.Errorf("an unscorable axis produced %d verdicts, want none: %v", len(vs), breached(vs))
			}
		})
	}
}

func TestJudgeRank_ANilClassIsJudgedByNothing(t *testing.T) {
	if vs := JudgeRank(RankInput{}, DefaultRankThresholds()); vs != nil {
		t.Errorf("a class that did not decode produced %d verdicts", len(vs))
	}
}

// TestJudgeWedged_NeedsDoNotScaleUpDeclaredNotDefaulted is the gate §7.7.4
// names. GKE's documented default is ScaleUpAnyway, so an absent field means a
// Pending pod is waiting for a node rather than wedged against a policy.
func TestJudgeWedged_NeedsDoNotScaleUpDeclaredNotDefaulted(t *testing.T) {
	tests := []struct {
		name    string
		spec    string
		pending int
		want    bool
	}{
		{"declared, with pending pods", s2ScoredSpec, 4, true},
		{"declared, nothing pending", s2ScoredSpec, 0, false},
		{"ScaleUpAnyway", n4PreferredSpec, 4, false},
		{"unset", `{"priorities":[{"machineFamily":"n4"},{"machineFamily":"n2"}]}`, 4, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := RankInput{
				Class:      judgeClass(t, tc.spec),
				Window:     steadyWindow(),
				Conditions: RankConditions{PendingPods: tc.pending, Lifetime: time.Hour},
			}
			v := ruleOf(t, JudgeRank(in, DefaultRankThresholds()), RankRuleWedged)
			if v.Breached != tc.want {
				t.Errorf("breached=%v, want %v (%s)", v.Breached, tc.want, v.Reason)
			}
			if v.Breached {
				if v.Tier != TierA {
					t.Errorf("tier %s, want A — a wedged class is §7.7.4's only Tier A row", v.Tier)
				}
				if v.Kind != KindRankWedged {
					t.Errorf("kind %q, want %q", v.Kind, KindRankWedged)
				}
				if v.Severity != severityCritical {
					t.Errorf("severity %q, want critical", v.Severity)
				}
			}
		})
	}
}

// TestJudgeWedged_IsIndependentOfOccupancy: the pods that matter here have no
// rank at all, so a class whose running pods are all at rank 0 can still be
// wedged for the ones that are not running.
func TestJudgeWedged_IsIndependentOfOccupancy(t *testing.T) {
	in := RankInput{
		Class:      judgeClass(t, s2ScoredSpec),
		Window:     steadyWindow(),
		Conditions: RankConditions{PendingPods: 1, Lifetime: time.Hour},
	}
	v := ruleOf(t, JudgeRank(in, DefaultRankThresholds()), RankRuleWedged)
	if !v.Breached {
		t.Fatalf("a class with a Pending pod did not fire: %s", v.Reason)
	}
	if v.Observed.Rank0Share != 1 {
		t.Errorf("rank-0 share %.2f, want 1 — the running pods are unaffected", v.Observed.Rank0Share)
	}
}

func TestJudgeLastRank_FiresOnlyAboveTheCeiling(t *testing.T) {
	// n4-preferred has three tiers, so rank 2 is the last one.
	tests := []struct {
		name      string
		lastShare float64
		want      bool
	}{
		{"all of it", 1.0, true},
		{"just above", 0.95, true},
		{"exactly at the ceiling", 0.9, false},
		{"most of it but under", 0.8, false},
		{"none of it", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			total := 36000.0
			in := RankInput{
				Class: judgeClass(t, n4PreferredSpec),
				Window: RankWindow{
					Elapsed: time.Hour,
					PodSeconds: map[Rank]float64{
						0: total * (1 - tc.lastShare),
						2: total * tc.lastShare,
					},
					Pods: map[Rank]int{0: 1, 2: 9},
				},
				Conditions: RankConditions{Lifetime: time.Hour},
			}
			v := ruleOf(t, JudgeRank(in, DefaultRankThresholds()), RankRuleLastRank)
			if v.Breached != tc.want {
				t.Errorf("breached=%v, want %v (%s)", v.Breached, tc.want, v.Reason)
			}
			if v.Observed.LastRank != 2 {
				t.Errorf("last rank %s, want 2", v.Observed.LastRank)
			}
			if v.Breached && v.Kind != KindRankDegraded {
				t.Errorf("kind %q, want %q", v.Kind, KindRankDegraded)
			}
		})
	}
}

// TestJudgeLastRank_OnAScoredClassTheLastTierIsNotTheLastListPosition is
// §7.7.1's trap applied to a finding. On s2ScoredSpec the least preferred rule
// is at list position 0, so a rule reading the annotation would call the
// *most* degraded state healthy.
func TestJudgeLastRank_OnAScoredClassTheLastTierIsNotTheLastListPosition(t *testing.T) {
	class := judgeClass(t, s2ScoredSpec)
	if got := class.Axis.LastRank(); got != 1 {
		t.Fatalf("fixture drifted: last rank %s, want 1 (two tiers, n2 alone at the bottom)", got)
	}
	if rule, ok := class.Axis.RuleAt(0); !ok || rule.Rank != 1 {
		t.Fatalf("fixture drifted: list position 0 should be the least preferred tier, got rank %s", rule.Rank)
	}

	in := RankInput{
		Class: class,
		Window: RankWindow{
			Elapsed:    time.Hour,
			PodSeconds: map[Rank]float64{1: 36000},
			Pods:       map[Rank]int{1: 10},
		},
		Conditions: RankConditions{Lifetime: time.Hour},
	}
	v := ruleOf(t, JudgeRank(in, DefaultRankThresholds()), RankRuleLastRank)
	if !v.Breached {
		t.Errorf("an axis spending all its time on rank 1 of 2 did not fire: %s", v.Reason)
	}
	if !strings.Contains(v.Reason, "rank 1") {
		t.Errorf("reason names the wrong rank: %s", v.Reason)
	}
}

// TestJudgeRank0Share_IsOffUntilConfigured is §7.7.5's "not all fallback is
// bad" made operative: an absolute depth threshold is a statement about one
// estate's intent, and we cannot infer it.
func TestJudgeRank0Share_IsOffUntilConfigured(t *testing.T) {
	window := RankWindow{
		Elapsed:    time.Hour,
		PodSeconds: map[Rank]float64{0: 3600, 1: 32400},
		Pods:       map[Rank]int{0: 1, 1: 9},
	}
	in := RankInput{
		Class:      judgeClass(t, n4PreferredSpec),
		Window:     window,
		Conditions: RankConditions{Lifetime: time.Hour},
	}

	off := ruleOf(t, JudgeRank(in, DefaultRankThresholds()), RankRuleRank0Share)
	if off.Breached {
		t.Errorf("the floor fired with no floor configured: %s", off.Reason)
	}
	if !strings.Contains(off.Reason, "no rank-0 share floor") {
		t.Errorf("unhelpful reason for the default-off case: %s", off.Reason)
	}

	th := DefaultRankThresholds()
	th.Rank0ShareFloor = 0.5
	on := ruleOf(t, JudgeRank(in, th), RankRuleRank0Share)
	if !on.Breached {
		t.Errorf("a 0.10 rank-0 share did not breach a 0.50 floor: %s", on.Reason)
	}
	if on.Tier != TierB {
		t.Errorf("tier %s, want B", on.Tier)
	}
}

// TestJudgeShares_AbstainOnAWindowTooShortToMeanAnything: a two-second window
// in which one pod happened to be at the last rank reads as a 100 % share, and
// the dwell would not save us because the machine would go on being handed
// that same reading.
func TestJudgeShares_AbstainOnAWindowTooShortToMeanAnything(t *testing.T) {
	th := DefaultRankThresholds()
	th.Rank0ShareFloor = 0.9

	in := RankInput{
		Class: judgeClass(t, n4PreferredSpec),
		Window: RankWindow{
			Elapsed:    2 * time.Second,
			PodSeconds: map[Rank]float64{2: 2},
			Pods:       map[Rank]int{2: 1},
		},
		Conditions: RankConditions{Lifetime: time.Hour},
	}
	vs := JudgeRank(in, th)
	for _, rule := range []RankRule{RankRuleRank0Share, RankRuleLastRank} {
		v := ruleOf(t, vs, rule)
		if v.Breached {
			t.Errorf("%s fired on a 2s window: %s", rule, v.Reason)
		}
		if !strings.Contains(v.Reason, "shorter than") {
			t.Errorf("%s: reason should name the short window, got %q", rule, v.Reason)
		}
	}
}

// TestJudgeShares_AbstainOnAnEmptyAxis: zero pod-seconds over zero pod-seconds
// is "we have nothing to say", and a share rule that read it as 0.00 would
// report every idle class as having lost its first-choice capacity.
func TestJudgeShares_AbstainOnAnEmptyAxis(t *testing.T) {
	th := DefaultRankThresholds()
	th.Rank0ShareFloor = 0.9

	in := RankInput{
		Class:      judgeClass(t, n4PreferredSpec),
		Window:     RankWindow{Elapsed: time.Hour},
		Conditions: RankConditions{Lifetime: time.Hour},
	}
	v := ruleOf(t, JudgeRank(in, th), RankRuleRank0Share)
	if v.Breached {
		t.Errorf("an axis with no pods fired the floor: %s", v.Reason)
	}
	if !strings.Contains(v.Reason, "no pod-seconds") {
		t.Errorf("reason should name the empty axis, got %q", v.Reason)
	}
}

// TestJudgeShares_SentinelSecondsAreNotInTheDenominator: a mean achieved rank
// computed over a bucket whose rank is -2 is not a mean of anything, and an
// axis parked off-axis must not thereby look like it is doing well at rank 0.
func TestJudgeShares_SentinelSecondsAreNotInTheDenominator(t *testing.T) {
	in := RankInput{
		Class: judgeClass(t, n4PreferredSpec),
		Window: RankWindow{
			Elapsed: time.Hour,
			PodSeconds: map[Rank]float64{
				2:                 36000,
				RankOffAxis:       100000,
				RankUnsatisfiable: 100000,
				RankUnknown:       100000,
			},
			Pods: map[Rank]int{2: 10, RankOffAxis: 30},
		},
		Conditions: RankConditions{Lifetime: time.Hour},
	}
	v := ruleOf(t, JudgeRank(in, DefaultRankThresholds()), RankRuleLastRank)
	if !v.Breached {
		t.Fatalf("sentinel pod-seconds diluted the last-rank share to %.2f: %s", v.Observed.LastRankShare, v.Reason)
	}
	if v.Observed.MeanRank != 2 {
		t.Errorf("mean achieved rank %.2f, want 2 — sentinels must be out of both halves", v.Observed.MeanRank)
	}
	if v.Observed.TierSeconds != 36000 {
		t.Errorf("tier seconds %.0f, want 36000", v.Observed.TierSeconds)
	}
}

func TestJudgeNoMigration_Gates(t *testing.T) {
	// Two tiers of pods, both occupied: the shape a stuck migration leaves.
	window := RankWindow{
		Elapsed:    time.Hour,
		PodSeconds: map[Rank]float64{0: 3600, 1: 32400},
		Pods:       map[Rank]int{0: 1, 1: 9},
	}
	tests := []struct {
		name string
		spec string
		cond RankConditions
		want bool
		says string
	}{
		{
			name: "everything lines up",
			spec: n4PreferredSpec,
			cond: RankConditions{Rank0Restored: true, SinceRestore: time.Hour, Lifetime: time.Hour},
			want: true,
		},
		{
			name: "the class never promised to migrate",
			spec: `{"priorities":[{"machineFamily":"n4"},{"machineFamily":"n2"}]}`,
			cond: RankConditions{Rank0Restored: true, SinceRestore: time.Hour, Lifetime: time.Hour},
			want: false,
			says: "optimizeRulePriority",
		},
		{
			name: "the top tier never went away",
			spec: n4PreferredSpec,
			cond: RankConditions{Rank0Restored: false, SinceRestore: time.Hour, Lifetime: time.Hour},
			want: false,
			says: "has not come back into use",
		},
		{
			name: "inside the grace period",
			spec: n4PreferredSpec,
			cond: RankConditions{Rank0Restored: true, SinceRestore: time.Minute, Lifetime: time.Hour},
			want: false,
			says: "grace period",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := RankInput{Class: judgeClass(t, tc.spec), Window: window, Conditions: tc.cond}
			v := ruleOf(t, JudgeRank(in, DefaultRankThresholds()), RankRuleNoMigration)
			if v.Breached != tc.want {
				t.Errorf("breached=%v, want %v (%s)", v.Breached, tc.want, v.Reason)
			}
			if tc.says != "" && !strings.Contains(v.Reason, tc.says) {
				t.Errorf("reason %q does not mention %q", v.Reason, tc.says)
			}
			if v.Breached && v.Tier != TierB {
				t.Errorf("tier %s, want B", v.Tier)
			}
		})
	}
}

// TestJudgeNoMigration_AnObservedMigrationClearsIt — S1 measured GKE acting
// 4 m 17 s after the class edit, and the rule must recognise the thing it was
// waiting for when it happens.
func TestJudgeNoMigration_AnObservedMigrationClearsIt(t *testing.T) {
	in := RankInput{
		Class: judgeClass(t, n4PreferredSpec),
		Window: RankWindow{
			Elapsed:    time.Hour,
			PodSeconds: map[Rank]float64{0: 18000, 1: 18000},
			Pods:       map[Rank]int{0: 8, 1: 2},
			Improving:  3,
		},
		Conditions: RankConditions{Rank0Restored: true, SinceRestore: time.Hour, Lifetime: time.Hour},
	}
	v := ruleOf(t, JudgeRank(in, DefaultRankThresholds()), RankRuleNoMigration)
	if v.Breached {
		t.Errorf("fired despite three observed migrations: %s", v.Reason)
	}
}

// TestJudgeNoMigration_LateralMovesAreNotMigrations: an equal-score alternative
// is capacity churn inside a preference level, not a fallback and not a
// recovery, so it must neither cause nor clear this finding (§7.7.3).
func TestJudgeNoMigration_LateralMovesAreNotMigrations(t *testing.T) {
	in := RankInput{
		Class: judgeClass(t, n4PreferredSpec),
		Window: RankWindow{
			Elapsed:    time.Hour,
			PodSeconds: map[Rank]float64{0: 3600, 1: 32400},
			Pods:       map[Rank]int{0: 1, 1: 9},
			Lateral:    12,
		},
		Conditions: RankConditions{Rank0Restored: true, SinceRestore: time.Hour, Lifetime: time.Hour},
	}
	v := ruleOf(t, JudgeRank(in, DefaultRankThresholds()), RankRuleNoMigration)
	if !v.Breached {
		t.Errorf("twelve lateral moves cleared a stuck migration: %s", v.Reason)
	}
	if v.Observed.Lateral != 12 {
		t.Errorf("lateral count %d, want 12 — counted and reported, just not scored", v.Observed.Lateral)
	}
}

func TestJudgeTierUnused_NeedsTheWholeWindow(t *testing.T) {
	th := DefaultRankThresholds()
	long := th.UnusedTierFor + time.Hour

	t.Run("too young to say", func(t *testing.T) {
		in := RankInput{
			Class:      judgeClass(t, n4PreferredSpec),
			Window:     steadyWindow(),
			Conditions: RankConditions{Lifetime: time.Hour, LifetimeSeconds: map[Rank]float64{0: 36000}},
		}
		v := tierOf(t, JudgeRank(in, th), RankRuleTierUnused, 2)
		if v.Breached {
			t.Errorf("an hour-old axis reported a 30-day dead tier: %s", v.Reason)
		}
	})

	t.Run("old enough, and dead", func(t *testing.T) {
		in := RankInput{
			Class:      judgeClass(t, n4PreferredSpec),
			Window:     steadyWindow(),
			Conditions: RankConditions{Lifetime: long, LifetimeSeconds: map[Rank]float64{0: 1e7}},
		}
		vs := JudgeRank(in, th)
		if v := tierOf(t, vs, RankRuleTierUnused, 0); v.Breached {
			t.Errorf("the busy tier fired: %s", v.Reason)
		}
		for _, tier := range []Rank{1, 2} {
			v := tierOf(t, vs, RankRuleTierUnused, tier)
			if !v.Breached {
				t.Errorf("rank %s idle for %s did not fire: %s", tier, long, v.Reason)
			}
			if v.Tier != TierC {
				t.Errorf("rank %s: tier %s, want C — §7.7.4 calls this info (cost)", tier, v.Tier)
			}
			if v.Severity != severityInfo {
				t.Errorf("rank %s: severity %q, want info", tier, v.Severity)
			}
			if v.Kind != KindRankTierUnused {
				t.Errorf("rank %s: kind %q", tier, v.Kind)
			}
		}
	})

	t.Run("a tier used once is not dead", func(t *testing.T) {
		in := RankInput{
			Class:  judgeClass(t, n4PreferredSpec),
			Window: steadyWindow(),
			Conditions: RankConditions{
				Lifetime:        long,
				LifetimeSeconds: map[Rank]float64{0: 1e7, 2: 1},
			},
		}
		v := tierOf(t, JudgeRank(in, th), RankRuleTierUnused, 2)
		if v.Breached {
			t.Errorf("one pod-second of use still read as dead: %s", v.Reason)
		}
	})
}

// TestJudgeTierUnused_IsPerTierSoOneFixDoesNotCloseTheOther: two dead rungs may
// be a reservation nobody draws on and a machine family that no longer exists
// in the region, and resolving the first must not close the second.
func TestJudgeTierUnused_IsPerTierSoOneFixDoesNotCloseTheOther(t *testing.T) {
	th := DefaultRankThresholds()
	in := RankInput{
		Class:      judgeClass(t, n4PreferredSpec),
		Window:     steadyWindow(),
		Conditions: RankConditions{Lifetime: th.UnusedTierFor + time.Hour, LifetimeSeconds: map[Rank]float64{0: 1e7}},
	}
	keys := breached(JudgeRank(in, th))
	want := map[string]bool{"tier-unused/1": false, "tier-unused/2": false}
	for _, k := range keys {
		if _, ok := want[k]; ok {
			want[k] = true
		}
	}
	for k, seen := range want {
		if !seen {
			t.Errorf("missing episode %q — two dead tiers are two findings", k)
		}
	}
}

// TestJudgeTierUnused_PeersShareOneTierAndOneVerdict: on a scored class two
// rules at the same score are one preference level, so an unused level is one
// finding and not two.
func TestJudgeTierUnused_PeersShareOneTierAndOneVerdict(t *testing.T) {
	th := DefaultRankThresholds()
	in := RankInput{
		Class:      judgeClass(t, s2ScoredSpec),
		Window:     steadyWindow(),
		Conditions: RankConditions{Lifetime: th.UnusedTierFor + time.Hour, LifetimeSeconds: map[Rank]float64{0: 1e7}},
	}
	vs := JudgeRank(in, th)
	var n int
	for _, v := range vs {
		if v.Rule == RankRuleTierUnused {
			n++
		}
	}
	if n != 2 {
		t.Errorf("%d unused-tier verdicts for a 3-rule 2-tier class, want 2", n)
	}
	// The dead tier is 1 — the n2 rule at score 10, alone at the bottom.
	v := tierOf(t, vs, RankRuleTierUnused, 1)
	if !v.Breached {
		t.Fatalf("rank 1 did not fire: %s", v.Reason)
	}
	if !strings.Contains(v.Reason, "machineFamily=n2") {
		t.Errorf("reason should name the hardware nobody is getting: %s", v.Reason)
	}
}

func TestRankThresholds_NormalizedRefusesAnUnusableShare(t *testing.T) {
	tests := []struct {
		name            string
		in              RankThresholds
		wantFloor       float64
		wantCeil        float64
		wantMinWindow   time.Duration
		wantUnusedTiers time.Duration
	}{
		{
			name:            "zero value takes every default",
			in:              RankThresholds{},
			wantFloor:       0,
			wantCeil:        0,
			wantMinWindow:   DefaultRankThresholds().MinWindow,
			wantUnusedTiers: DefaultRankThresholds().UnusedTierFor,
		},
		{
			name:      "a share above one can never stop firing, so it is off",
			in:        RankThresholds{Rank0ShareFloor: 1.5, LastRankShareCeiling: 42},
			wantFloor: 0, wantCeil: 0,
			wantMinWindow:   DefaultRankThresholds().MinWindow,
			wantUnusedTiers: DefaultRankThresholds().UnusedTierFor,
		},
		{
			name:      "a negative share is off, not inverted",
			in:        RankThresholds{Rank0ShareFloor: -0.5, LastRankShareCeiling: -1},
			wantFloor: 0, wantCeil: 0,
			wantMinWindow:   DefaultRankThresholds().MinWindow,
			wantUnusedTiers: DefaultRankThresholds().UnusedTierFor,
		},
		{
			name:      "a legal pair survives",
			in:        RankThresholds{Rank0ShareFloor: 0.25, LastRankShareCeiling: 1, MinWindow: time.Minute, UnusedTierFor: time.Hour},
			wantFloor: 0.25, wantCeil: 1,
			wantMinWindow:   time.Minute,
			wantUnusedTiers: time.Hour,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.in.Normalized()
			if got.Rank0ShareFloor != tc.wantFloor {
				t.Errorf("floor %v, want %v", got.Rank0ShareFloor, tc.wantFloor)
			}
			if got.LastRankShareCeiling != tc.wantCeil {
				t.Errorf("ceiling %v, want %v", got.LastRankShareCeiling, tc.wantCeil)
			}
			if got.MinWindow != tc.wantMinWindow {
				t.Errorf("min window %v, want %v", got.MinWindow, tc.wantMinWindow)
			}
			if got.UnusedTierFor != tc.wantUnusedTiers {
				t.Errorf("unused-tier window %v, want %v", got.UnusedTierFor, tc.wantUnusedTiers)
			}
			if got.MigrationGrace <= 0 {
				t.Errorf("migration grace %v — a zero grace concludes during every successful migration", got.MigrationGrace)
			}
		})
	}
}

func TestRankRule_KindAndTierAgreeWithSection234(t *testing.T) {
	tests := []struct {
		rule RankRule
		kind string
		tier Tier
		str  string
	}{
		{RankRuleWedged, KindRankWedged, TierA, "wedged"},
		{RankRuleRank0Share, KindRankDegraded, TierB, "rank0-share"},
		{RankRuleLastRank, KindRankDegraded, TierB, "last-rank"},
		{RankRuleNoMigration, KindRankNoMigration, TierB, "no-migration"},
		{RankRuleTierUnused, KindRankTierUnused, TierC, "tier-unused"},
		{RankRuleNone, "", TierNone, ""},
	}
	for _, tc := range tests {
		if got := tc.rule.Kind(); got != tc.kind {
			t.Errorf("%s.Kind() = %q, want %q", tc.str, got, tc.kind)
		}
		if got := tc.rule.Tier(); got != tc.tier {
			t.Errorf("%s.Tier() = %s, want %s", tc.str, got, tc.tier)
		}
		if got := tc.rule.String(); got != tc.str {
			t.Errorf("String() = %q, want %q", got, tc.str)
		}
	}
	if got := RankRule(200).String(); got != "" {
		t.Errorf("an unknown rule rendered %q", got)
	}
}

func TestRouteRank(t *testing.T) {
	tests := []struct {
		name       string
		verdict    RankVerdict
		tierC      bool
		wantSignal bool
		wantSev    string
	}{
		{"quiet", RankVerdict{Breached: false, Tier: TierA, Severity: severityCritical}, false, false, ""},
		{"tier A", RankVerdict{Breached: true, Tier: TierA, Severity: severityCritical}, false, true, severityCritical},
		{"tier B", RankVerdict{Breached: true, Tier: TierB, Severity: severityWarning}, false, true, severityWarning},
		{"tier C, opt-in off", RankVerdict{Breached: true, Tier: TierC, Severity: severityInfo}, false, false, ""},
		{"tier C, opt-in on", RankVerdict{Breached: true, Tier: TierC, Severity: severityInfo}, true, true, severityInfo},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := RouteRank(tc.verdict, tc.tierC)
			if got.Signal != tc.wantSignal {
				t.Errorf("signal=%v, want %v (%s)", got.Signal, tc.wantSignal, got.Reason)
			}
			if got.Severity != tc.wantSev {
				t.Errorf("severity %q, want %q", got.Severity, tc.wantSev)
			}
			if !got.Signal && got.Reason == "" {
				t.Error("a metrics-only outcome with no reason: the question asked of a quiet finding is why")
			}
		})
	}
}

func TestRankWindow_Arithmetic(t *testing.T) {
	w := RankWindow{
		Elapsed:    time.Hour,
		PodSeconds: map[Rank]float64{0: 100, 1: 300, RankOffAxis: 999},
		Pods:       map[Rank]int{0: 1, 2: 4, RankUnknown: 7},
	}
	if got := w.TierSeconds(); got != 400 {
		t.Errorf("tier seconds %v, want 400", got)
	}
	if got := w.Share(1); got != 0.75 {
		t.Errorf("share(1) = %v, want 0.75", got)
	}
	if got := w.MeanRank(); got != 0.75 {
		t.Errorf("mean rank %v, want 0.75", got)
	}
	if got := w.TierPods(); got != 5 {
		t.Errorf("tier pods %d, want 5 — the unknown bucket is not a tier", got)
	}
	if got := w.DegradedPods(); got != 4 {
		t.Errorf("degraded pods %d, want 4", got)
	}

	empty := RankWindow{Elapsed: time.Hour}
	if got := empty.Share(0); got != 0 {
		t.Errorf("share of an empty axis %v, want 0", got)
	}
	if got := empty.MeanRank(); got != 0 {
		t.Errorf("mean rank of an empty axis %v, want 0", got)
	}
}

func TestRankVerdict_EpisodeKeySeparatesTheTwoDegradedRules(t *testing.T) {
	floor := RankVerdict{Rule: RankRuleRank0Share, Focus: RankUnknown}
	ceiling := RankVerdict{Rule: RankRuleLastRank, Focus: RankUnknown}
	if floor.EpisodeKey() == ceiling.EpisodeKey() {
		t.Fatalf("both rank_degraded rules share the episode key %q — a recovery on one would extend the other's episode", floor.EpisodeKey())
	}
	if floor.Kind != ceiling.Kind {
		t.Log("(they still share a kind, which is §2.3's decision and not this one's)")
	}
	dead1 := RankVerdict{Rule: RankRuleTierUnused, Focus: 1}
	dead2 := RankVerdict{Rule: RankRuleTierUnused, Focus: 2}
	if dead1.EpisodeKey() == dead2.EpisodeKey() {
		t.Errorf("two dead tiers share the episode key %q", dead1.EpisodeKey())
	}
	if got := dead1.EpisodeKey(); got != "tier-unused/1" {
		t.Errorf("episode key %q", got)
	}
}

// TestJudgeShares_TheQuietReadingOfEachRule covers the two share rules when
// they are configured and satisfied. That is the reading an operator sees every
// day, and — because §8.2 resolves an episode by being handed a non-breaching
// verdict — it is also the one that ends an alert. A regression here would not
// make anything fire wrongly; it would make something stop being able to stop.
func TestJudgeShares_TheQuietReadingOfEachRule(t *testing.T) {
	th := DefaultRankThresholds()
	th.Rank0ShareFloor = 0.5

	vs := JudgeRank(RankInput{
		Class: judgeClass(t, n4PreferredSpec),
		Window: RankWindow{
			Elapsed:    time.Hour,
			PodSeconds: map[Rank]float64{0: 30000, 2: 6000},
			Pods:       map[Rank]int{0: 8, 2: 2},
		},
		Conditions: RankConditions{Lifetime: time.Hour},
	}, th)

	floor := ruleOf(t, vs, RankRuleRank0Share)
	if floor.Breached || !strings.Contains(floor.Reason, "at or above the floor") {
		t.Errorf("rank0-share: breached=%v %q", floor.Breached, floor.Reason)
	}
	ceiling := ruleOf(t, vs, RankRuleLastRank)
	if ceiling.Breached || !strings.Contains(ceiling.Reason, "at or below the ceiling") {
		t.Errorf("last-rank: breached=%v %q", ceiling.Breached, ceiling.Reason)
	}
}

// TestJudgeShares_AreOffWhenTheirShareIsNotConfigured, both of them, and by
// the same mechanism. §7.7.5's "not all fallback is bad" is why the rank-0
// floor ships at zero, and an operator who decides the same about their
// last-resort tier gets to turn the ceiling off the same way — rather than
// having to set it to 1 and reason about whether that is inclusive.
func TestJudgeShares_AreOffWhenTheirShareIsNotConfigured(t *testing.T) {
	th := DefaultRankThresholds()
	th.LastRankShareCeiling = 0

	vs := JudgeRank(RankInput{
		Class: judgeClass(t, n4PreferredSpec),
		Window: RankWindow{
			Elapsed:    time.Hour,
			PodSeconds: map[Rank]float64{2: 3600},
			Pods:       map[Rank]int{2: 1},
		},
		Conditions: RankConditions{Lifetime: time.Hour},
	}, th)

	ceiling := ruleOf(t, vs, RankRuleLastRank)
	if ceiling.Breached {
		t.Errorf("an unconfigured ceiling breached on a window entirely at the last rank: %s", ceiling.Reason)
	}
	if !strings.Contains(ceiling.Reason, "no last-rank share ceiling is configured") {
		t.Errorf("reason %q", ceiling.Reason)
	}
}

// TestJudgeShares_AbstainWhenNothingRanAtAnyTier is the second of the two
// abstention gates, and the one that keeps a class with every node off-axis
// from reading as a total rank-0 collapse. A long window full of sentinel
// seconds is not a low share; it is no share at all.
func TestJudgeShares_AbstainWhenNothingRanAtAnyTier(t *testing.T) {
	th := DefaultRankThresholds()
	th.Rank0ShareFloor = 0.5

	vs := JudgeRank(RankInput{
		Class: judgeClass(t, n4PreferredSpec),
		Window: RankWindow{
			Elapsed:    time.Hour,
			PodSeconds: map[Rank]float64{RankUnknown: 1800, RankOffAxis: 1800},
			Pods:       map[Rank]int{RankUnknown: 2},
		},
		Conditions: RankConditions{Lifetime: time.Hour},
	}, th)

	for _, rule := range []RankRule{RankRuleRank0Share, RankRuleLastRank} {
		v := ruleOf(t, vs, rule)
		if v.Breached {
			t.Errorf("%s breached on a window with no tier seconds: %s", rule, v.Reason)
		}
		if !strings.Contains(v.Reason, "no pod-seconds accrued") {
			t.Errorf("%s abstained for the wrong reason: %q", rule, v.Reason)
		}
	}
}

// TestJudgeNoMigration_ClearsWhenTheLastDegradedPodGoes is the ordinary way
// this episode ends. The window still remembers the seconds spent at rank 1 —
// it is a time integral and always will — so the rule has to look at who is
// there *now*, not at what the window accrued.
func TestJudgeNoMigration_ClearsWhenTheLastDegradedPodGoes(t *testing.T) {
	v := ruleOf(t, JudgeRank(RankInput{
		Class: judgeClass(t, n4PreferredSpec),
		Window: RankWindow{
			Elapsed:    time.Hour,
			PodSeconds: map[Rank]float64{0: 18000, 1: 18000},
			Pods:       map[Rank]int{0: 10},
		},
		Conditions: RankConditions{Rank0Restored: true, SinceRestore: time.Hour, Lifetime: time.Hour},
	}, DefaultRankThresholds()), RankRuleNoMigration)

	if v.Breached || !strings.Contains(v.Reason, "no pods remain") {
		t.Errorf("breached=%v %q", v.Breached, v.Reason)
	}
}

// TestTierHelpers_OnAnAxisThatCouldNotBeOrdered reaches the two guards
// JudgeRank keeps unreachable by checking Scorable first.
//
// Tested directly rather than left uncovered, because what makes them
// unreachable is one caller's gate and not a property of the helpers: rankRows
// calls both from a second site that does not repeat the check, and a
// partially-scored class is a shape we decode precisely because we did not
// admit it.
func TestTierHelpers_OnAnAxisThatCouldNotBeOrdered(t *testing.T) {
	axis := NewPreferenceAxis(AxisKey{Provider: ProviderGKEComputeClass, Name: "partial"}, []PreferenceRule{
		{Raw: map[string]any{"machineFamily": "n2"}},
		{Score: ptr(50), Raw: map[string]any{"machineFamily": "n4"}},
	})
	if axis.Ordering != OrderingInvalid {
		t.Fatalf("fixture drifted: ordering %s, want invalid", axis.Ordering)
	}
	if got := tiersOf(axis); len(got) != 0 {
		t.Errorf("tiersOf on an unorderable axis returned %v, want none — no rule has a tier rank", got)
	}
	if got := renderTier(axis, 0); got != "no rules" {
		t.Errorf("renderTier on a tier no rule sits at = %q", got)
	}
}
