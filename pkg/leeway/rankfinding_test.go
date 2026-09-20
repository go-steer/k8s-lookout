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
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var rankEpisodeStart = time.Date(2026, 9, 20, 9, 31, 0, 0, time.UTC)

// degradedInput is a three-tier class that has spent nine-tenths of an hour on
// its spot fallback — the shape the shipped default is set to catch.
func degradedInput(t *testing.T) RankFindingInput {
	t.Helper()
	class := judgeClass(t, n4PreferredSpec)
	window := RankWindow{
		Elapsed:    time.Hour,
		PodSeconds: map[Rank]float64{0: 1800, 1: 0, 2: 34200},
		Pods:       map[Rank]int{0: 1, 2: 9},
		Improving:  0,
		Lateral:    2,
	}
	vs := JudgeRank(RankInput{Class: class, Window: window, Conditions: RankConditions{Lifetime: time.Hour}}, DefaultRankThresholds())
	v := ruleOf(t, vs, RankRuleLastRank)
	if !v.Breached {
		t.Fatalf("fixture does not breach: %s", v.Reason)
	}
	return RankFindingInput{
		Class:      class,
		Verdict:    v,
		Window:     window,
		State:      AlertState{Phase: PhaseFiring, FirstSeenAt: rankEpisodeStart},
		ObjectKind: "ComputeClass",
	}
}

func TestNewRankFinding_Envelope(t *testing.T) {
	f := NewRankFinding(degradedInput(t))

	if f.Kind != KindRankDegraded {
		t.Errorf("kind %q", f.Kind)
	}
	if f.Subject.Kind != SubjectPreferenceAxis || f.Subject.Name != "test" {
		t.Errorf("subject %+v, want a PreferenceAxis named after the class", f.Subject)
	}
	if f.Subject.Namespace != "" {
		t.Errorf("namespace %q — a preference axis is cluster-scoped", f.Subject.Namespace)
	}
	if f.Provider != string(ProviderGKEComputeClass) {
		t.Errorf("provider %q", f.Provider)
	}
	if f.ObjectKind != "ComputeClass" {
		t.Errorf("objectKind %q — the neutral subject vocabulary drops it, so the payload has to carry it", f.ObjectKind)
	}
	if f.SpecHash == "" || f.SpecHash != judgeClass(t, n4PreferredSpec).Axis.SpecHash {
		t.Errorf("specHash %q does not pin the rule list the numbers are about", f.SpecHash)
	}
	if f.Tier != "B" || f.Severity != severityWarning {
		t.Errorf("tier %q severity %q", f.Tier, f.Severity)
	}
	if f.Rule != "last-rank" {
		t.Errorf("rule %q", f.Rule)
	}
	if !f.FirstSeenAt.Equal(rankEpisodeStart) {
		t.Errorf("firstSeenAt %s, want the episode start and not this evaluation", f.FirstSeenAt)
	}
	if f.Focus != nil {
		t.Errorf("focus %v on an axis-wide rule", *f.Focus)
	}
}

// TestNewRankFinding_RestatesHowTheLadderWasBuilt: the single most common way
// to misread a rank finding is to assume rank is list position, so the payload
// says which it was.
func TestNewRankFinding_RestatesHowTheLadderWasBuilt(t *testing.T) {
	f := NewRankFinding(degradedInput(t))
	if f.Ordering != "list-position" || f.Tiers != 3 {
		t.Errorf("ordering %q tiers %d, want list-position/3", f.Ordering, f.Tiers)
	}

	scored := judgeClass(t, s2ScoredSpec)
	in := degradedInput(t)
	in.Class = scored
	if got := NewRankFinding(in); got.Ordering != "priority-score" || got.Tiers != 2 {
		t.Errorf("scored class: ordering %q tiers %d, want priority-score/2", got.Ordering, got.Tiers)
	}
}

// TestNewRankFinding_CarriesBothGatesOnEveryFinding — they are the first thing
// a reader changes in response, and the first thing they need to know has not
// already been changed.
func TestNewRankFinding_CarriesBothGatesOnEveryFinding(t *testing.T) {
	f := NewRankFinding(degradedInput(t))
	if f.ScaleUp != "scale-up-anyway" {
		t.Errorf("scaleUp %q", f.ScaleUp)
	}
	if !f.OptimizeRulePriority {
		t.Error("optimizeRulePriority is false on a fixture that declares it true")
	}

	in := degradedInput(t)
	in.Class = judgeClass(t, s2ScoredSpec)
	if got := NewRankFinding(in); got.ScaleUp != "do-not-scale-up" || got.OptimizeRulePriority {
		t.Errorf("scored fixture: scaleUp %q optimize %v", got.ScaleUp, got.OptimizeRulePriority)
	}
}

// TestNewRankFinding_APerTierFindingCarriesItsTier, because two dead rungs
// produce two documents and nothing else in them differs.
func TestNewRankFinding_APerTierFindingCarriesItsTier(t *testing.T) {
	th := DefaultRankThresholds()
	class := judgeClass(t, n4PreferredSpec)
	window := steadyWindow()
	vs := JudgeRank(RankInput{
		Class:      class,
		Window:     window,
		Conditions: RankConditions{Lifetime: th.UnusedTierFor + time.Hour, LifetimeSeconds: map[Rank]float64{0: 1e7}},
	}, th)

	for _, tier := range []Rank{1, 2} {
		v := tierOf(t, vs, RankRuleTierUnused, tier)
		f := NewRankFinding(RankFindingInput{Class: class, Verdict: v, Window: window, ObjectKind: "ComputeClass"})
		if f.Focus == nil {
			t.Fatalf("rank %s: no focus on a per-tier finding", tier)
		}
		if *f.Focus != tier {
			t.Errorf("focus %s, want %s", *f.Focus, tier)
		}
	}
}

// TestRankRows_KeepEmptyTiers: a zero row is the most informative row in a
// dead-tier finding, and dropping empty rows would make the one finding that
// is about an empty row unable to show it.
func TestRankRows_KeepEmptyTiers(t *testing.T) {
	class := judgeClass(t, n4PreferredSpec)
	rows := rankRows(class.Axis, steadyWindow())
	if len(rows) != 3 {
		t.Fatalf("%d rows for a three-tier axis with one occupied: %+v", len(rows), rows)
	}
	for i, want := range []Rank{0, 1, 2} {
		if rows[i].Rank != want {
			t.Fatalf("row %d is rank %s, want %s", i, rows[i].Rank, want)
		}
	}
	if rows[0].Share != 1 || rows[1].Share != 0 || rows[2].Share != 0 {
		t.Errorf("shares %v/%v/%v", rows[0].Share, rows[1].Share, rows[2].Share)
	}
	if rows[2].Rules != "machineFamily=n2,spot=true" {
		t.Errorf("row 2 rules %q — an empty tier still has to say which hardware nobody is getting", rows[2].Rules)
	}
}

// TestRankRows_SentinelsSortAfterTheTiers so a reader scanning the table sees
// the preference ladder before the three states that are not on it.
func TestRankRows_SentinelsSortAfterTheTiers(t *testing.T) {
	class := judgeClass(t, n4PreferredSpec)
	w := RankWindow{
		Elapsed: time.Hour,
		PodSeconds: map[Rank]float64{
			RankOffAxis: 10, 2: 20, RankUnknown: 30, 0: 40, RankUnsatisfiable: 50,
		},
	}
	rows := rankRows(class.Axis, w)
	var got []string
	for _, r := range rows {
		got = append(got, r.Rank.String())
	}
	want := []string{"0", "1", "2", "unknown", "unsatisfiable", "off-axis"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("row order %v, want %v", got, want)
	}
	for _, r := range rows {
		if !r.Rank.IsTier() && r.Rules != "" {
			t.Errorf("sentinel %s carries rules %q", r.Rank, r.Rules)
		}
	}
}

func TestRankMessage(t *testing.T) {
	msg := RankMessage(NewRankFinding(degradedInput(t)))
	for _, want := range []string{
		"preference rank degraded",
		"ComputeClass test",
		"tier B",
		"mean achieved rank 1.90",
		"rank 2 95%",
		"machineFamily=n2,spot=true",
		"ordered by list-position, 3 tier(s)",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("message is missing %q:\n  %s", want, msg)
		}
	}
	if strings.Contains(msg, KindRankDegraded) {
		t.Errorf("the message repeats the kind, which is already its own field:\n  %s", msg)
	}
}

func TestRankMessage_Headlines(t *testing.T) {
	for kind, want := range map[string]string{
		KindRankWedged:      "compute class wedged",
		KindRankDegraded:    "preference rank degraded",
		KindRankNoMigration: "no migration back to preferred capacity",
		KindRankTierUnused:  "preference tier unused",
		"leeway.something":  "preference rank degraded",
	} {
		if got := rankHeadline(kind); got != want {
			t.Errorf("%s -> %q, want %q", kind, got, want)
		}
	}
}

// TestRankMessage_CapsTheTableAndSaysItDid, the same rule §8.5 applies to
// domain lines: a ten-tier class is one story, and the whole table is in the
// payload for anyone who wants it.
func TestRankMessage_CapsTheTableAndSaysItDid(t *testing.T) {
	in := degradedInput(t)
	in.Window.PodSeconds[RankOffAxis] = 5
	in.Window.PodSeconds[RankUnknown] = 5
	msg := RankMessage(NewRankFinding(in))
	if !strings.Contains(msg, "(+2 more)") {
		t.Errorf("a five-row table did not report the two rows it dropped:\n  %s", msg)
	}
}

// TestBusiestRanks_TiesAreReproducible: two tiers with the same share must not
// swap places between two findings about the same axis.
func TestBusiestRanks_TiesAreReproducible(t *testing.T) {
	rows := []RankShare{
		{Rank: 0, Share: 0.25}, {Rank: 1, Share: 0.25},
		{Rank: 2, Share: 0.25}, {Rank: 3, Share: 0.25},
	}
	first := busiestRanks(rows, 2)
	for i := 0; i < 20; i++ {
		got := busiestRanks(rows, 2)
		if got[0].Rank != first[0].Rank || got[1].Rank != first[1].Rank {
			t.Fatalf("tie-break is unstable: %v then %v", first, got)
		}
	}
	if first[0].Rank != 0 || first[1].Rank != 1 {
		t.Errorf("ties broke to %v, want the two most preferred", first)
	}
}

// TestRankFinding_MarshalsToTheDocumentedShape pins the JSON, which is what
// `cmd/leeway` prints and what the store keeps.
func TestRankFinding_MarshalsToTheDocumentedShape(t *testing.T) {
	f := NewRankFinding(degradedInput(t))
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back map[string]any
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{
		"kind", "subject", "provider", "objectKind", "specHash", "tier", "severity",
		"rule", "observed", "ranks", "ordering", "tiers", "scaleUp",
		"optimizeRulePriority", "reason", "firstSeenAt",
	} {
		if _, ok := back[key]; !ok {
			t.Errorf("payload is missing %q", key)
		}
	}
	if _, ok := back["focus"]; ok {
		t.Error("focus is present on an axis-wide finding; it is omitempty for a reason")
	}
	obs, _ := back["observed"].(map[string]any)
	if obs["lastRankShare"] != 0.95 {
		t.Errorf("observed.lastRankShare = %v, want 0.95", obs["lastRankShare"])
	}
}
