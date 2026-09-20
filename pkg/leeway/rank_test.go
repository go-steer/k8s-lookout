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
	"reflect"
	"sort"
	"strings"
	"testing"
)

func score(n int) *int { return &n }

// family builds the commonest rule shape there is: a bare machine family.
func family(name string) PreferenceRule {
	return PreferenceRule{Raw: map[string]any{"machineFamily": name}}
}

// scored is `family` with an explicit priorityScore.
func scored(name string, s int) PreferenceRule {
	r := family(name)
	r.Score = score(s)
	return r
}

// axisOf builds an axis under the GKE provider, which is the only one there
// is today and saves every case below repeating the key.
func axisOf(name string, rules ...PreferenceRule) *PreferenceAxis {
	return NewPreferenceAxis(AxisKey{Provider: ProviderGKEComputeClass, Name: name}, rules)
}

// ranksOf is the assertion most of these tests make.
func ranksOf(a *PreferenceAxis) []Rank {
	out := make([]Rank, len(a.Rules))
	for i, r := range a.Rules {
		out[i] = r.Rank
	}
	return out
}

func TestNewPreferenceAxis_UnscoredRulesRankByListPosition(t *testing.T) {
	a := axisOf("n4-preferred", family("n4"), family("n2"))

	if a.Ordering != ByListPosition {
		t.Errorf("Ordering = %v, want %v for a class with no scores", a.Ordering, ByListPosition)
	}
	if got, want := ranksOf(a), []Rank{0, 1}; !reflect.DeepEqual(got, want) {
		t.Errorf("ranks = %v, want %v — without scores, position is preference", got, want)
	}
	if a.Tiers != 2 {
		t.Errorf("Tiers = %d, want 2", a.Tiers)
	}
	if !a.Scorable() {
		t.Error("a two-priority class is not scorable")
	}
}

// TestNewPreferenceAxis_ScoresOutrankListPosition is spike S2's measured class,
// and the reason Rank is derived rather than read. The *least* preferred rule
// sits at list position 0, and two tied rules follow it.
//
// GKE skipped index 0 and provisioned the rule at index 2, stamping
// ccc_priority_index: "2" on a node the class ranks first. An implementation
// that read the annotation as a rank would have inverted this class.
func TestNewPreferenceAxis_ScoresOutrankListPosition(t *testing.T) {
	a := axisOf("mixed", scored("n2", 10), scored("n4", 50), scored("c3", 50))

	if a.Ordering != ByPriorityScore {
		t.Fatalf("Ordering = %v, want %v", a.Ordering, ByPriorityScore)
	}
	// Descending score: the two 50s are the top tier, the 10 is below them.
	if got, want := ranksOf(a), []Rank{1, 0, 0}; !reflect.DeepEqual(got, want) {
		t.Errorf("ranks = %v, want %v — higher score is more preferred", got, want)
	}
	if a.Tiers != 2 {
		t.Errorf("Tiers = %d, want 2 — three rules, two distinct scores", a.Tiers)
	}
	// The index is still the raw list position, which is what the node
	// annotation will hold. The whole point is that the two disagree.
	if a.Rules[2].Index != 2 || a.Rules[2].Rank != 0 {
		t.Errorf("rule c3 = index %d rank %d, want index 2 rank 0 — identity is not preference",
			a.Rules[2].Index, a.Rules[2].Rank)
	}
}

// TestPreferenceRule_TiedRulesArePeers is the other half of S2, and the false
// positive it would have caused. The first scale-up attempt was n4 (index 1),
// it hit a real stockout, and GKE fell through to c3 (index 2). The index rose
// by one and the rank did not move, because the class author declared those
// two families interchangeable.
func TestPreferenceRule_TiedRulesArePeers(t *testing.T) {
	a := axisOf("mixed", scored("n2", 10), scored("n4", 50), scored("c3", 50))

	n4, c3, n2 := a.Rules[1], a.Rules[2], a.Rules[0]
	if !n4.Peer(c3) {
		t.Error("two rules with the same score are not peers; a stockout between them would report as a fallback")
	}
	if n4.Peer(n2) {
		t.Error("rules with different scores are peers")
	}
}

func TestPreferenceRule_PeerIsFalseForNonTierRanks(t *testing.T) {
	// Two rules off a class that could not be ordered. They are both
	// RankUnknown, and equality on a sentinel must not read as peerage.
	a := axisOf("partial", scored("n4", 50), family("n2"))
	if a.Rules[0].Peer(a.Rules[1]) {
		t.Error("two unrankable rules reported as peers")
	}
}

func TestNewPreferenceAxis_APartiallyScoredClassRefusesToOrder(t *testing.T) {
	// GKE rejects this at admission — S2 saw "PriorityScore must be set for
	// all priorities or for none of them" — so it can only arrive from an
	// older control plane, a restored backup, or another provider. Inventing
	// an ordering for it would silently mis-rank every node on the axis.
	a := axisOf("partial", scored("n4", 50), family("n2"))

	if a.Ordering != OrderingInvalid {
		t.Errorf("Ordering = %v, want %v", a.Ordering, OrderingInvalid)
	}
	if a.Tiers != 0 {
		t.Errorf("Tiers = %d, want 0 — an unordered axis has no tiers", a.Tiers)
	}
	if a.Scorable() {
		t.Error("an unorderable axis is scorable")
	}
	for i, r := range a.Rules {
		if r.Rank != RankUnknown {
			t.Errorf("rule %d rank = %v, want %v — no rule may default to the top tier", i, r.Rank, RankUnknown)
		}
	}
}

// TestNewPreferenceAxis_SingleTierIsNotScorable covers the four GKE-managed
// Autopilot classes, each of which declares exactly one priority.
func TestNewPreferenceAxis_SingleTierIsNotScorable(t *testing.T) {
	for _, a := range []*PreferenceAxis{
		axisOf("autopilot", family("n2")),
		// Two rules, one tier: same conclusion by a different route.
		axisOf("tied", scored("n4", 50), scored("c3", 50)),
	} {
		if a.Tiers != 1 {
			t.Errorf("%s: Tiers = %d, want 1", a.Key.Name, a.Tiers)
		}
		if a.Scorable() {
			t.Errorf("%s: scorable with one tier — there is no fallback to detect", a.Key.Name)
		}
	}
}

func TestNewPreferenceAxis_NoRulesAtAll(t *testing.T) {
	a := axisOf("empty")
	if a.Ordering != ByListPosition {
		t.Errorf("Ordering = %v, want %v — an empty list is well-formed, just empty", a.Ordering, ByListPosition)
	}
	if a.Tiers != 0 || a.Scorable() {
		t.Errorf("Tiers = %d scorable = %v, want 0 and false", a.Tiers, a.Scorable())
	}
	if got := a.LastRank(); got != RankUnknown {
		t.Errorf("LastRank = %v, want %v", got, RankUnknown)
	}
}

func TestNewPreferenceAxis_IndexIsPositionNotWhateverTheCallerSaid(t *testing.T) {
	// A caller filling Index by hand is a caller who can make it disagree
	// with the annotation it will be compared against.
	lying := []PreferenceRule{{Index: 7, Raw: map[string]any{"machineFamily": "n4"}}, {Index: 7}}
	a := axisOf("lying", lying...)

	if a.Rules[0].Index != 0 || a.Rules[1].Index != 1 {
		t.Errorf("indexes = %d,%d, want 0,1", a.Rules[0].Index, a.Rules[1].Index)
	}
	// And the caller's slice is untouched.
	if lying[0].Index != 7 {
		t.Error("NewPreferenceAxis mutated its caller's rules")
	}
}

func TestPreferenceAxis_LastRank(t *testing.T) {
	if got := axisOf("three", family("a"), family("b"), family("c")).LastRank(); got != 2 {
		t.Errorf("LastRank = %v, want 2", got)
	}
	// Tiers, not rules: the two peers collapse into one tier.
	if got := axisOf("scored", scored("a", 9), scored("b", 5), scored("c", 5)).LastRank(); got != 1 {
		t.Errorf("LastRank = %v, want 1 — three rules but two tiers", got)
	}
}

func TestPreferenceAxis_RuleAtRefusesAStaleIndex(t *testing.T) {
	// A class edited under running nodes leaves annotations naming rules
	// that no longer exist. This is the expected path, not an error one.
	a := axisOf("shrunk", family("n4"))
	for _, idx := range []int{-1, 1, 99} {
		if _, ok := a.RuleAt(idx); ok {
			t.Errorf("RuleAt(%d) resolved against a %d-rule axis", idx, len(a.Rules))
		}
	}
	if r, ok := a.RuleAt(0); !ok || r.Index != 0 {
		t.Errorf("RuleAt(0) = %+v, %v; want the only rule", r, ok)
	}
}

func TestPreferenceAxis_NilReceiverIsSafe(t *testing.T) {
	// An axis whose CRD has not synced is nil at every call site, and the
	// source reaching for its rank before then must not take the process out.
	var a *PreferenceAxis
	if a.Scorable() {
		t.Error("a nil axis is scorable")
	}
	if got := a.LastRank(); got != RankUnknown {
		t.Errorf("LastRank on nil = %v, want %v", got, RankUnknown)
	}
	if _, ok := a.RuleAt(0); ok {
		t.Error("RuleAt on nil resolved")
	}
}

func TestSpecHash_ChangesWhenOnlyAScoreChanges(t *testing.T) {
	// The §7.7.5 trap: the rule bodies are byte-identical and the tiers move.
	// A hash over bodies alone would let this slip past the baseline reset.
	before := axisOf("x", scored("n4", 50), scored("c3", 50))
	after := axisOf("x", scored("n4", 50), scored("c3", 10))

	if before.SpecHash == after.SpecHash {
		t.Fatal("re-scoring a class left its spec hash unchanged; every historical rank sample would silently change meaning")
	}
	if before.Tiers == after.Tiers {
		t.Errorf("Tiers = %d both sides, want the re-score to split the tier", before.Tiers)
	}
}

func TestSpecHash_DistinguishesNoScoreFromScoreZero(t *testing.T) {
	// One is ordered by list position, the other by score. Different classes,
	// and the separator placement in specHash is what keeps them apart.
	unscored := axisOf("x", family("n4"), family("c3"))
	zeroed := axisOf("x", scored("n4", 0), scored("c3", 0))
	if unscored.SpecHash == zeroed.SpecHash {
		t.Error("an unscored class hashes the same as an all-zero-score one")
	}
}

func TestSpecHash_IsStableAcrossEquivalentDecodes(t *testing.T) {
	// Two decodes of the same YAML differ only in map iteration order. If
	// that reached the hash, every informer resync would look like an edit
	// and reset every baseline on the axis.
	build := func() *PreferenceAxis {
		return axisOf("x", PreferenceRule{Raw: map[string]any{
			"machineFamily": "n4",
			"spot":          true,
			"minCores":      int64(8),
			"reservations":  map[string]any{"affinity": "specific", "specific": []any{map[string]any{"name": "r1", "project": "p1"}}},
		}})
	}
	first := build().SpecHash
	for i := 0; i < 50; i++ {
		if got := build().SpecHash; got != first {
			t.Fatalf("spec hash is not stable across decodes: %q then %q", first, got)
		}
	}
}

func TestSpecHash_OrderMatters(t *testing.T) {
	// Reordering the priority list is exactly the edit that changes what a
	// rank means, so it must not hash the same.
	ab := axisOf("x", family("n4"), family("n2"))
	ba := axisOf("x", family("n2"), family("n4"))
	if ab.SpecHash == ba.SpecHash {
		t.Error("swapping two priorities left the spec hash unchanged")
	}
}

func TestRank_StringSpellsTheSentinels(t *testing.T) {
	// These land in a metric label, where "-2" would be unreadable and
	// would also sort next to real tiers in a dashboard legend.
	cases := map[Rank]string{
		0: "0", 3: "3",
		RankUnknown: "unknown", RankUnsatisfiable: "unsatisfiable", RankOffAxis: "off-axis",
	}
	for r, want := range cases {
		if got := r.String(); got != want {
			t.Errorf("Rank(%d).String() = %q, want %q", int(r), got, want)
		}
	}
}

func TestRank_IsTier(t *testing.T) {
	for _, r := range []Rank{0, 1, 99} {
		if !r.IsTier() {
			t.Errorf("Rank(%d) is not a tier", int(r))
		}
	}
	for _, r := range []Rank{RankUnknown, RankUnsatisfiable, RankOffAxis} {
		if r.IsTier() {
			t.Errorf("sentinel %v reported as a tier; it would be bucketed as one", r)
		}
	}
}

func TestOrderingMode_String(t *testing.T) {
	cases := map[OrderingMode]string{
		OrderingInvalid: "invalid", ByListPosition: "list-position", ByPriorityScore: "priority-score",
		OrderingMode(7): "unknown(7)",
	}
	for m, want := range cases {
		if got := m.String(); got != want {
			t.Errorf("OrderingMode(%d).String() = %q, want %q", int(m), got, want)
		}
	}
}

func TestAxisKey_String(t *testing.T) {
	k := AxisKey{Provider: ProviderGKEComputeClass, Name: "n4-preferred"}
	if got, want := k.String(), "gke-computeclass/n4-preferred"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestPreferenceRule_Render(t *testing.T) {
	tests := []struct {
		name string
		raw  map[string]any
		want string
	}{
		{"a bare family", map[string]any{"machineFamily": "n4"}, "machineFamily=n4"},
		{"sorted, not map order", map[string]any{"spot": true, "machineFamily": "n2"}, "machineFamily=n2,spot=true"},
		{"numbers", map[string]any{"minCores": int64(8), "minMemoryGb": 3.5}, "minCores=8,minMemoryGb=3.5"},
		{"nested collapses", map[string]any{"reservations": map[string]any{"affinity": "specific"}}, "reservations={…}"},
		{"lists collapse", map[string]any{"gpu": []any{"t4"}}, "gpu=[…]"},
		{"a null field", map[string]any{"spot": nil}, "spot="},
		{"an unmodelled type", map[string]any{"weird": struct{ A int }{2}}, "weird={2}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := PreferenceRule{Raw: tc.raw}
			if got := r.Render(); got != tc.want {
				t.Errorf("Render() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPreferenceRule_RenderWithNoBody(t *testing.T) {
	// A rule that constrains nothing matches everything, so the index is the
	// only thing that distinguishes it from its siblings.
	r := PreferenceRule{Index: 2}
	if got, want := r.Render(), "index=2"; got != want {
		t.Errorf("Render() = %q, want %q", got, want)
	}
}

// TestAssignRanks_ScoredRanksAreAContiguousDescendingRun is the property the
// arithmetic has to hold for any score list at all: tiers are 0..Tiers-1 with
// no gaps, a higher score never gets a worse rank, and equal scores always
// land on the same rank.
func TestAssignRanks_ScoredRanksAreAContiguousDescendingRun(t *testing.T) {
	rng := rand.New(rand.NewSource(20260920))
	for trial := 0; trial < 500; trial++ {
		n := 1 + rng.Intn(8)
		rules := make([]PreferenceRule, n)
		for i := range rules {
			// A small range so ties are common, and negatives because
			// nothing in the CRD forbids them.
			rules[i] = scored("m", rng.Intn(5)-2)
		}
		a := axisOf("prop", rules...)

		if a.Ordering != ByPriorityScore {
			t.Fatalf("trial %d: Ordering = %v", trial, a.Ordering)
		}
		seen := map[Rank]bool{}
		for _, r := range a.Rules {
			if !r.Rank.IsTier() {
				t.Fatalf("trial %d: rank %v is not a tier", trial, r.Rank)
			}
			seen[r.Rank] = true
		}
		for want := Rank(0); want < Rank(a.Tiers); want++ {
			if !seen[want] {
				t.Fatalf("trial %d: tier %v is missing; ranks must be contiguous", trial, want)
			}
		}
		if len(seen) != a.Tiers {
			t.Fatalf("trial %d: %d distinct ranks but Tiers = %d", trial, len(seen), a.Tiers)
		}
		for _, x := range a.Rules {
			for _, y := range a.Rules {
				switch {
				case *x.Score > *y.Score && x.Rank >= y.Rank:
					t.Fatalf("trial %d: score %d ranked %v, score %d ranked %v — higher score must rank better",
						trial, *x.Score, x.Rank, *y.Score, y.Rank)
				case *x.Score == *y.Score && x.Rank != y.Rank:
					t.Fatalf("trial %d: equal scores %d ranked %v and %v", trial, *x.Score, x.Rank, y.Rank)
				}
			}
		}
	}
}

// TestAssignRanks_ExtremeScores guards the sort comparison against the values
// a CRD will happily accept even though nobody sensible would write them.
func TestAssignRanks_ExtremeScores(t *testing.T) {
	a := axisOf("extreme", scored("a", math.MinInt), scored("b", 0), scored("c", math.MaxInt))
	if got, want := ranksOf(a), []Rank{2, 1, 0}; !reflect.DeepEqual(got, want) {
		t.Errorf("ranks = %v, want %v", got, want)
	}
}

func TestDescendingDistinctScores(t *testing.T) {
	rules := []PreferenceRule{scored("a", 5), scored("b", 9), scored("c", 5), scored("d", 1)}
	got := descendingDistinctScores(rules)
	if want := []int{9, 5, 1}; !reflect.DeepEqual(got, want) {
		t.Errorf("= %v, want %v", got, want)
	}
	if !sort.SliceIsSorted(got, func(i, j int) bool { return got[i] > got[j] }) {
		t.Error("not descending")
	}
}

func TestCanonicalJSON_FallsBackOnAnUnmarshallableBody(t *testing.T) {
	// Unreachable from a decoded unstructured object, which is why it is
	// tested here rather than through NewPreferenceAxis. It exists so that a
	// body we cannot marshal still hashes deterministically instead of
	// hashing as the empty string and colliding with every other such body.
	bad := map[string]any{"ch": make(chan int)}
	first := canonicalJSON(bad)
	if !strings.Contains(first, "chan") {
		t.Errorf("fallback rendering = %q, want something naming the value", first)
	}
	if second := canonicalJSON(bad); second != first {
		t.Errorf("fallback is not deterministic: %q then %q", first, second)
	}
}
