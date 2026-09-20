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
)

// unscoredAxis is the §7.7.2 worked example's class: three rules, no scores,
// so rank and index coincide and a mistake in one is invisible.
func unscoredAxis() *PreferenceAxis {
	return axisOf("n4-preferred", family("n4"), family("c3"), family("n2"))
}

// scoredAxis is S2's class, the one that proves rank and index are different
// things. List order is n2, n4, c3; scores are 10, 50, 50. So the rule at
// index 0 is the LEAST preferred, and indices 1 and 2 are peers at rank 0.
func scoredAxis() *PreferenceAxis {
	return axisOf("s2-scored",
		scored("n2", 10),
		scored("n4", 50),
		scored("c3", 50),
	)
}

func n4Profile() NodeProfile {
	return gkeExtractorOrDie().Extract(map[string]string{
		"node.kubernetes.io/instance-type":  "n4-highmem-2",
		"cloud.google.com/machine-family":   "n4",
		"cloud.google.com/gke-provisioning": "standard",
	}, 2, 16)
}

func gkeExtractorOrDie() *ProfileExtractor {
	e, err := NewProfileExtractor(DefaultNodeProfileConfig())
	if err != nil {
		panic(err)
	}
	return e
}

func TestParseDeclared_IsTotalOverStrings(t *testing.T) {
	// strconv.Atoi fails on three of the four shapes this field takes, and two
	// of those three are findings rather than errors.
	tests := []struct {
		in    string
		kind  declaredKind
		index int
	}{
		{"", declaredAbsent, 0},
		{"0", declaredIndex, 0},
		{"2", declaredIndex, 2},
		{"17", declaredIndex, 17},
		{"007", declaredIndex, 7},
		{"ccc_no_rule_matching", declaredNoRuleMatching, 0},
		{"ccc_scale_up_anyway", declaredScaleUpAnyway, 0},
		{"ccc_something_gke_added_later", declaredUnrecognised, 0},
		{"-1", declaredUnrecognised, 0},
		{"+1", declaredUnrecognised, 0},
		{" 1", declaredUnrecognised, 0},
		{"1 ", declaredUnrecognised, 0},
		{"1.0", declaredUnrecognised, 0},
		{"0x2", declaredUnrecognised, 0},
		{"CCC_NO_RULE_MATCHING", declaredUnrecognised, 0},
		// Long enough to overflow an int if the loop kept multiplying.
		{strings.Repeat("9", 40), declaredUnrecognised, 0},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got := parseDeclared(tc.in)
			if got.kind != tc.kind || got.index != tc.index {
				t.Errorf("parseDeclared(%q) = {%v %d}, want {%v %d}", tc.in, got.kind, got.index, tc.kind, tc.index)
			}
		})
	}
}

func TestResolve_TheHappyPathAgreesWithItself(t *testing.T) {
	// §7.7.2's verified node: machineFamily n4, ccc_priority_index "0". Both
	// paths answer, they agree, and no counter moves.
	got := NewRankResolver().Resolve("0", n4Profile(), unscoredAxis())

	if got.Rank != 0 || got.RuleIndex != 0 {
		t.Errorf("rank/index = %v/%d, want 0/0", got.Rank, got.RuleIndex)
	}
	if got.Source != SourceNodeAnnotation {
		t.Errorf("Source = %v, want the annotation — it is the primary path", got.Source)
	}
	if got.Outcome != OutcomeResolved || got.Disagreed || got.Ambiguous {
		t.Errorf("outcome %v disagreed %v ambiguous %v, want a clean resolve", got.Outcome, got.Disagreed, got.Ambiguous)
	}
}

// TestResolve_S2TheIndexIsNotTheRank is the measurement this whole subsystem's
// data model exists for. GKE stamped ccc_priority_index "2" on a node whose
// class ranks that rule FIRST. An implementation reading the annotation as a
// rank would have reported a two-tier fallback on a node at its best tier.
func TestResolve_S2TheIndexIsNotTheRank(t *testing.T) {
	c3 := gkeExtractorOrDie().Extract(map[string]string{
		"node.kubernetes.io/instance-type": "c3-standard-4",
		"cloud.google.com/machine-family":  "c3",
	}, 4, 16)

	got := NewRankResolver().Resolve("2", c3, scoredAxis())

	if got.RuleIndex != 2 {
		t.Errorf("RuleIndex = %d, want 2 — identity is what the annotation carries", got.RuleIndex)
	}
	if got.Rank != 0 {
		t.Errorf("Rank = %v, want 0 — score 50 is the top tier on this class", got.Rank)
	}
	if got.Disagreed {
		t.Error("inference disagreed with the annotation on the case they were both measured against")
	}
}

// TestResolve_S2ThePeerFallbackIsNotADemotion is the accidental half of S2: a
// real GCE stockout on n4 (index 1) fell through to c3 (index 2). The index
// rose; the rank did not, because both rules score 50.
func TestResolve_S2ThePeerFallbackIsNotADemotion(t *testing.T) {
	axis := scoredAxis()
	before := NewRankResolver().Resolve("1", n4Profile(), axis)
	after := NewRankResolver().Resolve("2", NodeProfile{MachineFamily: "c3"}, axis)

	if before.RuleIndex == after.RuleIndex {
		t.Fatal("the fixture is wrong: the two observations must name different rules")
	}
	if before.Rank != after.Rank {
		t.Errorf("rank moved %v -> %v across a peer fallback; it must not", before.Rank, after.Rank)
	}
}

func TestResolve_TheSentinelsAreFindingsNotErrors(t *testing.T) {
	tests := []struct {
		value   string
		rank    Rank
		outcome RankOutcome
	}{
		{"ccc_no_rule_matching", RankUnsatisfiable, OutcomeNoRuleMatching},
		{"ccc_scale_up_anyway", RankOffAxis, OutcomeOffAxis},
	}
	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			got := NewRankResolver().Resolve(tc.value, n4Profile(), unscoredAxis())
			if got.Rank != tc.rank || got.Outcome != tc.outcome {
				t.Errorf("rank/outcome = %v/%v, want %v/%v", got.Rank, got.Outcome, tc.rank, tc.outcome)
			}
			if got.RuleIndex != -1 {
				t.Errorf("RuleIndex = %d, want -1 — neither sentinel names a rule", got.RuleIndex)
			}
			if got.Source != SourceNodeAnnotation {
				t.Errorf("Source = %v, want the annotation — the provider said this", got.Source)
			}
		})
	}
}

// TestResolve_TheSentinelsSurviveAnUnorderableAxis: neither sentinel needs a
// tier to mean something, and they are the two most interesting things the
// provider ever says. An axis that cannot order its rules must not swallow them.
func TestResolve_TheSentinelsSurviveAnUnorderableAxis(t *testing.T) {
	broken := axisOf("partly-scored", family("n4"), scored("c3", 50))
	if broken.Ordering != OrderingInvalid {
		t.Fatal("the fixture is wrong: this axis should refuse to order")
	}
	got := NewRankResolver().Resolve("ccc_scale_up_anyway", n4Profile(), broken)
	if got.Outcome != OutcomeOffAxis {
		t.Errorf("Outcome = %v, want off-axis even though the axis is invalid", got.Outcome)
	}
}

// TestResolve_AnUnknownSentinelFailsClosed. GKE could add a third value to an
// undocumented field. Falling through to inference would answer confidently
// about a node the provider just said something unusual about; the loud answer
// is how that change gets noticed at upgrade time.
func TestResolve_AnUnknownSentinelFailsClosed(t *testing.T) {
	got := NewRankResolver().Resolve("ccc_reserved_for_something", n4Profile(), unscoredAxis())
	if got.Rank != RankUnknown || got.Outcome != OutcomeUnrecognised {
		t.Errorf("rank/outcome = %v/%v, want unknown/unrecognised", got.Rank, got.Outcome)
	}
	if got.Source != SourceNone {
		t.Errorf("Source = %v, want none — inference must not paper over it", got.Source)
	}
}

// TestResolve_AbsentIsNotZero is the §7.7.2 hazard. Every node carries the
// class label and taint from creation but no rank annotation for 33-44 s, and
// pods are Running on it before the rank appears. Defaulting to 0 manufactures
// a rank-0 -> rank-1 fallback forty seconds into the life of every node that
// lands anywhere else.
func TestResolve_AbsentIsNotZero(t *testing.T) {
	// A node inference cannot place either: no family label, no instance type.
	got := NewRankResolver().Resolve("", NodeProfile{}, unscoredAxis())

	if got.Rank == 0 {
		t.Fatal("an unstamped node resolved to rank 0 — this is the spurious-fallback bug")
	}
	if got.Rank != RankUnknown || got.Outcome != OutcomePending {
		t.Errorf("rank/outcome = %v/%v, want unknown/pending", got.Rank, got.Outcome)
	}
	if got.RuleIndex != -1 || got.Source != SourceNone {
		t.Errorf("index/source = %d/%v, want -1/none", got.RuleIndex, got.Source)
	}
}

func TestResolve_InferenceCoversTheStampingLag(t *testing.T) {
	// The other half of the same window: the annotation has not arrived, but
	// the node's labels are enough to place it. This is what demotes the
	// pending counter from a permanent blind spot to a short one.
	got := NewRankResolver().Resolve("", n4Profile(), unscoredAxis())

	if got.Rank != 0 || got.RuleIndex != 0 {
		t.Errorf("rank/index = %v/%d, want 0/0 from inference", got.Rank, got.RuleIndex)
	}
	if got.Source != SourceInferred || got.Outcome != OutcomeResolved {
		t.Errorf("source/outcome = %v/%v, want inferred/resolved", got.Source, got.Outcome)
	}
}

// TestResolve_UnmatchedIsAFlagNotAState. The node's rank is pending either
// way — that is what it IS. Whether inference could have covered for the
// missing annotation is a separate fact about our matcher, and §7.7's exit
// criterion asks for zero of it.
func TestResolve_UnmatchedIsAFlagNotAState(t *testing.T) {
	odd := NodeProfile{MachineFamily: "t2a", InstanceType: "t2a-standard-4"}
	got := NewRankResolver().Resolve("", odd, unscoredAxis())

	if got.Outcome != OutcomePending || got.Rank != RankUnknown {
		t.Errorf("rank/outcome = %v/%v, want unknown/pending", got.Rank, got.Outcome)
	}
	if !got.Unmatched {
		t.Error("inference placed the node nowhere and did not say so")
	}
}

// TestResolve_UnmatchedIsCountedBehindTheAnnotationToo. Counting it only when
// inference is the sole path would measure it exactly where it does least
// good: the coverage gap that matters is the one the primary path is hiding.
func TestResolve_UnmatchedIsCountedBehindTheAnnotationToo(t *testing.T) {
	got := NewRankResolver().Resolve("1", NodeProfile{}, unscoredAxis())

	if got.Outcome != OutcomeResolved || got.Rank != 1 {
		t.Fatalf("rank/outcome = %v/%v, want 1/resolved — the annotation answered", got.Rank, got.Outcome)
	}
	if !got.Unmatched {
		t.Error("inference failed silently behind a working annotation")
	}
	if got.Disagreed {
		t.Error("an unmatched node was counted as a disagreement")
	}
}

func TestResolve_UnmatchedIsNotSetWhenInferenceNeverRan(t *testing.T) {
	// With Infer off the matcher has no opinion to have failed to form, and a
	// counter that ticked here would report a coverage gap in a path that is
	// switched off.
	got := (&RankResolver{Infer: false}).Resolve("1", NodeProfile{}, unscoredAxis())
	if got.Unmatched {
		t.Error("inference was off and still reported itself unmatched")
	}
}

// TestResolve_DisagreementIsRecordedNotActedOn. We are checking ourselves
// against the provider, not overruling it, so the annotation still wins.
func TestResolve_DisagreementIsRecordedNotActedOn(t *testing.T) {
	// The node is n4 — rule 0 on this axis — but the annotation says rule 2.
	got := NewRankResolver().Resolve("2", n4Profile(), unscoredAxis())

	if !got.Disagreed {
		t.Fatal("the annotation and inference named different rules and nothing was recorded")
	}
	if got.RuleIndex != 2 || got.Source != SourceNodeAnnotation {
		t.Errorf("index/source = %d/%v, want 2/annotation — the provider wins", got.RuleIndex, got.Source)
	}
	if got.Rank != 2 {
		t.Errorf("Rank = %v, want 2 — the rank follows the winning index", got.Rank)
	}
	if got.Outcome != OutcomeResolved {
		t.Errorf("Outcome = %v, want resolved — a disagreement is still an answer", got.Outcome)
	}
}

func TestResolve_AStaleIndexIsExpectedNotAPanic(t *testing.T) {
	// A class edited under running nodes leaves annotations naming rules that
	// no longer exist. Indexing the slice here is a panic waiting for a rule
	// deletion.
	got := NewRankResolver().Resolve("9", n4Profile(), unscoredAxis())

	if got.Outcome != OutcomeOutOfRange {
		t.Errorf("Outcome = %v, want out-of-range", got.Outcome)
	}
	if got.Rank != RankUnknown {
		t.Errorf("Rank = %v, want unknown", got.Rank)
	}
	if got.RuleIndex != 9 {
		t.Errorf("RuleIndex = %d, want the offending 9 kept for the operator", got.RuleIndex)
	}
	if got.Source != SourceNodeAnnotation {
		t.Errorf("Source = %v, want annotation — we know who said it", got.Source)
	}
}

func TestResolve_AnUnsyncedOrUnorderableAxisRanksNothing(t *testing.T) {
	tests := []struct {
		name string
		axis *PreferenceAxis
	}{
		{"class not yet synced", nil},
		{"partially scored", axisOf("partly", family("n4"), scored("c3", 50))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NewRankResolver().Resolve("0", n4Profile(), tc.axis)
			if got.Rank != RankUnknown || got.Outcome != OutcomeAxisInvalid {
				t.Errorf("rank/outcome = %v/%v, want unknown/axis-invalid", got.Rank, got.Outcome)
			}
			if got.Rank == 0 {
				t.Error("a rank resolved against an axis that cannot rank must never be 0")
			}
		})
	}
}

func TestResolve_InferenceCanBeSwitchedOff(t *testing.T) {
	r := &RankResolver{Infer: false}

	// The annotation still answers.
	if got := r.Resolve("1", n4Profile(), unscoredAxis()); got.Rank != 1 || got.Disagreed {
		t.Errorf("rank %v disagreed %v, want 1 and no cross-check", got.Rank, got.Disagreed)
	}
	// And the fallback is gone with it — which is the cost, and is why the
	// shipped resolver has it on.
	if got := r.Resolve("", n4Profile(), unscoredAxis()); got.Outcome != OutcomePending {
		t.Errorf("Outcome = %v, want pending with inference off", got.Outcome)
	}
}

func TestResolve_UnsupportedFieldsAreReportedEvenWhenTheAnnotationAnswered(t *testing.T) {
	// The whole hazard is the matcher rotting while the primary path keeps
	// working and hides it.
	axis := axisOf("future", rule(map[string]any{"somethingGKEAddedLater": "x"}), family("n4"))
	got := NewRankResolver().Resolve("1", n4Profile(), axis)

	if got.Outcome != OutcomeResolved {
		t.Fatalf("Outcome = %v, want resolved — the annotation answered", got.Outcome)
	}
	if len(got.Unsupported) != 1 || got.Unsupported[0].Field != "somethingGKEAddedLater" {
		t.Errorf("Unsupported = %+v, want the one unmodelled field", got.Unsupported)
	}
}

func TestResolve_AmbiguityIsCarried(t *testing.T) {
	// Two rules both accept an n4 node. First-match-wins picks one; the
	// arbitrariness is reported rather than hidden.
	axis := axisOf("overlapping", family("n4"), rule(map[string]any{}))
	got := NewRankResolver().Resolve("", n4Profile(), axis)

	if !got.Ambiguous {
		t.Error("two rules accepted the node and nothing was reported")
	}
	if got.RuleIndex != 0 {
		t.Errorf("RuleIndex = %d, want the first match", got.RuleIndex)
	}
}

func TestRankSource_String(t *testing.T) {
	tests := map[RankSource]string{
		SourceNone: "none", SourceNodeAnnotation: "annotation",
		SourceInferred: "inferred", RankSource(9): "unknown",
	}
	for in, want := range tests {
		if got := in.String(); got != want {
			t.Errorf("RankSource(%d).String() = %q, want %q", in, got, want)
		}
	}
}

func TestRankOutcome_String(t *testing.T) {
	// These spellings become metric label values, so they are API.
	tests := map[RankOutcome]string{
		OutcomeResolved: "resolved", OutcomeNoRuleMatching: "no-rule-matching",
		OutcomeOffAxis: "off-axis", OutcomePending: "pending",
		OutcomeOutOfRange:  "out-of-range",
		OutcomeAxisInvalid: "axis-invalid", OutcomeUnrecognised: "unrecognised",
		RankOutcome(99): "unknown",
	}
	seen := map[string]bool{}
	for in, want := range tests {
		got := in.String()
		if got != want {
			t.Errorf("RankOutcome(%d).String() = %q, want %q", in, got, want)
		}
		if seen[got] {
			t.Errorf("%q is used by two outcomes; the counters would merge", got)
		}
		seen[got] = true
	}
}
