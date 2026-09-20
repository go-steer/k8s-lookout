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
)

// specFromJSON decodes a spec the way a real object arrives.
//
// Via encoding/json deliberately, because that is the harsher of the two
// decoders this code sees: it gives float64 for every number, where the
// unstructured decoder gives int64 for whole ones. Code that survives this
// survives both, and TestDecodeComputeClass_AcceptsBothNumericDecodes pins the
// other direction.
func specFromJSON(t *testing.T, doc string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(doc), &out); err != nil {
		t.Fatalf("fixture does not parse: %v", err)
	}
	return out
}

// n4PreferredSpec is the class from §7.7.2's verified observation, as GKE
// stores it: three priorities, no scores, so rank and index coincide.
const n4PreferredSpec = `{
  "nodePoolAutoCreation": {"enabled": true},
  "whenUnsatisfiable": "ScaleUpAnyway",
  "activeMigration": {"optimizeRulePriority": true},
  "priorities": [
    {"machineFamily": "n4"},
    {"machineFamily": "c3"},
    {"machineFamily": "n2", "spot": true}
  ]
}`

// s2ScoredSpec is spike S2's class: the LEAST preferred rule sits at list
// position 0, and positions 1 and 2 are peers. It is the fixture that catches
// a decoder that quietly sorts the list.
const s2ScoredSpec = `{
  "whenUnsatisfiable": "DoNotScaleUp",
  "priorities": [
    {"machineFamily": "n2", "priorityScore": 10},
    {"machineFamily": "n4", "priorityScore": 50},
    {"machineFamily": "c3", "priorityScore": 50}
  ]
}`

func TestDecodeComputeClass_TheVerifiedClass(t *testing.T) {
	got, err := DecodeComputeClass("n4-preferred", specFromJSON(t, n4PreferredSpec))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Axis.Key.String() != "gke-computeclass/n4-preferred" {
		t.Errorf("axis key = %q", got.Axis.Key)
	}
	if got.Axis.Ordering != ByListPosition {
		t.Errorf("Ordering = %v, want list-position — no rule carries a score", got.Axis.Ordering)
	}
	if got.Axis.Tiers != 3 {
		t.Errorf("Tiers = %d, want 3", got.Axis.Tiers)
	}
	if got.ScaleUp != ScaleUpPolicyAnyway {
		t.Errorf("ScaleUp = %v, want scale-up-anyway", got.ScaleUp)
	}
	if !got.OptimizeRulePriority {
		t.Error("OptimizeRulePriority = false; the fixture declares it true")
	}
	if !got.Scorable() {
		t.Error("a three-tier class is not scorable")
	}
	// The whole rule body survives, including fields the ordering ignores, or
	// the matcher has nothing to match on.
	if got.Axis.Rules[2].Raw["spot"] != true {
		t.Errorf("rule 2 Raw = %v, want the spot field carried through", got.Axis.Rules[2].Raw)
	}
}

// TestDecodeComputeClass_ListOrderIsNotPreferenceOrder is S2 in one assertion.
// A decoder that sorted by score, or that assumed position implied preference,
// would invert this class.
func TestDecodeComputeClass_ListOrderIsNotPreferenceOrder(t *testing.T) {
	got, err := DecodeComputeClass("s2-scored", specFromJSON(t, s2ScoredSpec))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Axis.Ordering != ByPriorityScore {
		t.Fatalf("Ordering = %v, want priority-score", got.Axis.Ordering)
	}
	// Index is list position, untouched — it is what ccc_priority_index holds.
	for i, r := range got.Axis.Rules {
		if r.Index != i {
			t.Errorf("rule %d has Index %d; list order must survive the decode", i, r.Index)
		}
	}
	// Rank is the other thing entirely: the score-10 rule at position 0 is the
	// WORST tier, and positions 1 and 2 are peers at the best.
	wantRanks := []Rank{1, 0, 0}
	for i, want := range wantRanks {
		if got.Axis.Rules[i].Rank != want {
			t.Errorf("rule %d rank = %v, want %v", i, got.Axis.Rules[i].Rank, want)
		}
	}
	if got.Axis.Tiers != 2 {
		t.Errorf("Tiers = %d, want 2 — three rules, two tiers", got.Axis.Tiers)
	}
	if got.ScaleUp != ScaleUpPolicyDoNotScaleUp {
		t.Errorf("ScaleUp = %v, want do-not-scale-up", got.ScaleUp)
	}
	if got.OptimizeRulePriority {
		t.Error("OptimizeRulePriority = true with no activeMigration block")
	}
}

// TestDecodeComputeClass_ASingleTierClassIsNotScorable. Not a corner case: the
// four GKE-managed Autopilot classes each declare exactly one priority, so
// without this every GKE cluster starts with four axes of guaranteed-silent
// noise.
func TestDecodeComputeClass_ASingleTierClassIsNotScorable(t *testing.T) {
	tests := []struct {
		name string
		spec string
	}{
		{"one priority", `{"priorities":[{"machineFamily":"n2"}]}`},
		{"two rules, one tier", `{"priorities":[{"machineFamily":"n2","priorityScore":5},{"machineFamily":"n4","priorityScore":5}]}`},
		{"no priorities at all", `{"whenUnsatisfiable":"ScaleUpAnyway"}`},
		{"an empty list", `{"priorities":[]}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeComputeClass("managed", specFromJSON(t, tc.spec))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.Scorable() {
				t.Error("class is scorable; every node on it is rank 0 by construction")
			}
			if got.Axis == nil {
				t.Error("Axis is nil — the class exists and should still be exportable")
			}
		})
	}
}

func TestDecodeComputeClass_APartiallyScoredClassRefusesToOrder(t *testing.T) {
	// GKE rejects these at admission, but we decode objects we did not admit:
	// an older control plane, a restored backup, another provider reusing the
	// shape.
	spec := `{"priorities":[{"machineFamily":"n2"},{"machineFamily":"n4","priorityScore":50}]}`
	got, err := DecodeComputeClass("half-scored", specFromJSON(t, spec))
	if err != nil {
		t.Fatalf("decode: %v — an unadmittable class is not an unreadable one", err)
	}
	if got.Axis.Ordering != OrderingInvalid {
		t.Errorf("Ordering = %v, want invalid", got.Axis.Ordering)
	}
	if got.Scorable() {
		t.Error("an unorderable class is scorable")
	}
	for _, r := range got.Axis.Rules {
		if r.Rank != RankUnknown {
			t.Errorf("rule %d has rank %v; an unorderable class ranks nothing", r.Index, r.Rank)
		}
	}
}

// TestDecodeComputeClass_AcceptsBothNumericDecodes. encoding/json gives
// float64 for every number; the unstructured decoder gives int64 for whole
// ones. Both reach this code depending on how the object arrived.
func TestDecodeComputeClass_AcceptsBothNumericDecodes(t *testing.T) {
	fromUnstructured := map[string]any{
		"priorities": []any{
			map[string]any{"machineFamily": "n2", "priorityScore": int64(10)},
			map[string]any{"machineFamily": "n4", "priorityScore": int64(50)},
		},
	}
	viaInt64, err := DecodeComputeClass("x", fromUnstructured)
	if err != nil {
		t.Fatalf("int64 scores: %v", err)
	}
	viaFloat64, err := DecodeComputeClass("x", specFromJSON(t,
		`{"priorities":[{"machineFamily":"n2","priorityScore":10},{"machineFamily":"n4","priorityScore":50}]}`))
	if err != nil {
		t.Fatalf("float64 scores: %v", err)
	}

	if viaInt64.Axis.SpecHash != viaFloat64.Axis.SpecHash {
		t.Errorf("the same class hashed differently by decoder: %s vs %s",
			viaInt64.Axis.SpecHash, viaFloat64.Axis.SpecHash)
	}
	if *viaInt64.Axis.Rules[1].Score != 50 {
		t.Errorf("score = %d, want 50", *viaInt64.Axis.Rules[1].Score)
	}
}

func TestDecodeComputeClass_RejectsAShapeItCannotRead(t *testing.T) {
	// Strict about shape: a half-read class produces a plausible-looking axis
	// that is wrong, which is worse than a loud refusal.
	tests := []struct {
		name string
		spec string
		want string
	}{
		{"priorities is not a list", `{"priorities":{"machineFamily":"n2"}}`, "want a list"},
		{"priorities is a string", `{"priorities":"n2"}`, "want a list"},
		{"a rule is not an object", `{"priorities":["n2"]}`, "want an object"},
		{"a rule is a list", `{"priorities":[[]]}`, "want an object"},
		{"a score is a string", `{"priorities":[{"priorityScore":"50"}]}`, "want a number"},
		{"a score is a bool", `{"priorities":[{"priorityScore":true}]}`, "want a number"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeComputeClass("broken", specFromJSON(t, tc.spec))
			if err == nil {
				t.Fatal("a spec this decoder cannot read was accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
			if !strings.Contains(err.Error(), "broken") {
				t.Errorf("error = %v, want it to name the class", err)
			}
		})
	}
}

func TestDecodeComputeClass_AnUnknownRuleFieldIsTheMatchersProblem(t *testing.T) {
	// Strict about shape, permissive about content: GKE adds fields over time,
	// and refusing to decode the class would blind us to the rules we DO
	// understand. rankmatch.go already fails closed on the rule itself.
	spec := `{"priorities":[{"machineFamily":"n4"},{"somethingNew":{"nested":1}}]}`
	got, err := DecodeComputeClass("forward", specFromJSON(t, spec))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Axis.Rules) != 2 {
		t.Fatalf("Rules = %d, want both kept", len(got.Axis.Rules))
	}
	m := got.Axis.Match(NodeProfile{MachineFamily: "n4"})
	if !m.Matched || m.Index != 0 {
		t.Errorf("match = %+v, want rule 0 — the readable rule still works", m)
	}
	if len(m.Unsupported) != 1 || m.Unsupported[0].Index != 1 {
		t.Errorf("Unsupported = %+v, want rule 1 declined", m.Unsupported)
	}
}

func TestDecodeComputeClass_ScaleUpPolicy(t *testing.T) {
	tests := []struct {
		spec string
		want ScaleUpPolicy
	}{
		{`{}`, ScaleUpPolicyUnset},
		{`{"whenUnsatisfiable":""}`, ScaleUpPolicyUnset},
		{`{"whenUnsatisfiable":"ScaleUpAnyway"}`, ScaleUpPolicyAnyway},
		{`{"whenUnsatisfiable":"DoNotScaleUp"}`, ScaleUpPolicyDoNotScaleUp},
		// Case-sensitive on purpose: the API server enforces the enum, so a
		// differing case is a value from somewhere else.
		{`{"whenUnsatisfiable":"donotscaleup"}`, ScaleUpPolicyUnrecognised},
		{`{"whenUnsatisfiable":"SomethingNew"}`, ScaleUpPolicyUnrecognised},
		// The spread-constraint spelling, which shares the field name and
		// nothing else. It must not resolve to anything here.
		{`{"whenUnsatisfiable":"DoNotSchedule"}`, ScaleUpPolicyUnrecognised},
		{`{"whenUnsatisfiable":true}`, ScaleUpPolicyUnset},
	}
	for _, tc := range tests {
		t.Run(tc.spec, func(t *testing.T) {
			got, err := DecodeComputeClass("c", specFromJSON(t, tc.spec))
			if err != nil {
				t.Fatal(err)
			}
			if got.ScaleUp != tc.want {
				t.Errorf("ScaleUp = %v, want %v", got.ScaleUp, tc.want)
			}
		})
	}
}

func TestDecodeComputeClass_OptimizeRulePriority(t *testing.T) {
	tests := []struct {
		spec string
		want bool
	}{
		{`{}`, false},
		{`{"activeMigration":{}}`, false},
		{`{"activeMigration":{"optimizeRulePriority":true}}`, true},
		{`{"activeMigration":{"optimizeRulePriority":false}}`, false},
		{`{"activeMigration":{"optimizeRulePriority":"true"}}`, false},
		{`{"activeMigration":"true"}`, false},
	}
	for _, tc := range tests {
		t.Run(tc.spec, func(t *testing.T) {
			got, err := DecodeComputeClass("c", specFromJSON(t, tc.spec))
			if err != nil {
				t.Fatal(err)
			}
			if got.OptimizeRulePriority != tc.want {
				t.Errorf("OptimizeRulePriority = %v, want %v", got.OptimizeRulePriority, tc.want)
			}
		})
	}
}

// TestDecodeComputeClass_AFractionalScoreIsTruncatedNotRejected. The CRD types
// the field as an integer, so a fraction means something upstream re-encoded
// it — and the ordering it implies is still the ordering the author wrote.
func TestDecodeComputeClass_AFractionalScoreIsTruncatedNotRejected(t *testing.T) {
	spec := `{"priorities":[{"machineFamily":"n2","priorityScore":10.9},{"machineFamily":"n4","priorityScore":50.2}]}`
	got, err := DecodeComputeClass("fractional", specFromJSON(t, spec))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if *got.Axis.Rules[0].Score != 10 || *got.Axis.Rules[1].Score != 50 {
		t.Errorf("scores = %d/%d, want 10/50", *got.Axis.Rules[0].Score, *got.Axis.Rules[1].Score)
	}
	if got.Axis.Rules[0].Rank != 1 || got.Axis.Rules[1].Rank != 0 {
		t.Error("truncation changed the ordering")
	}
}

// TestDecodeComputeClass_TheSpecHashCoversTheScores. Re-scoring re-tiers a
// class without touching a single rule body — §7.7.5's "edit most likely to be
// made casually" — and a hash over bodies alone would let it past the
// baseline-reset guard.
func TestDecodeComputeClass_TheSpecHashCoversTheScores(t *testing.T) {
	before, err := DecodeComputeClass("c", specFromJSON(t, s2ScoredSpec))
	if err != nil {
		t.Fatal(err)
	}
	rescored := strings.Replace(s2ScoredSpec, `"priorityScore": 10`, `"priorityScore": 90`, 1)
	after, err := DecodeComputeClass("c", specFromJSON(t, rescored))
	if err != nil {
		t.Fatal(err)
	}

	if before.Axis.SpecHash == after.Axis.SpecHash {
		t.Fatal("re-scoring left the spec hash unchanged")
	}
	// And it really did re-tier: the worst rule became the best.
	if before.Axis.Rules[0].Rank != 1 || after.Axis.Rules[0].Rank != 0 {
		t.Errorf("rule 0 rank %v -> %v, want 1 -> 0", before.Axis.Rules[0].Rank, after.Axis.Rules[0].Rank)
	}
}

func TestDecodeComputeClass_NilSpec(t *testing.T) {
	// An object with no spec at all, which is what a partially-written or
	// finalizer-only object looks like on the way past.
	got, err := DecodeComputeClass("empty", nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Axis == nil || len(got.Axis.Rules) != 0 || got.Scorable() {
		t.Errorf("got %+v, want an empty unscorable axis", got.Axis)
	}
}

func TestScaleUpPolicy_String(t *testing.T) {
	tests := map[ScaleUpPolicy]string{
		ScaleUpPolicyUnset: "unset", ScaleUpPolicyAnyway: "scale-up-anyway",
		ScaleUpPolicyDoNotScaleUp: "do-not-scale-up",
		ScaleUpPolicyUnrecognised: "unrecognised", ScaleUpPolicy(9): "unknown",
	}
	for in, want := range tests {
		if got := in.String(); got != want {
			t.Errorf("ScaleUpPolicy(%d).String() = %q, want %q", in, got, want)
		}
	}
}

func TestComputeClass_ScorableOnANilReceiver(t *testing.T) {
	// The source holds a map of these and will ask before a decode lands.
	var c *ComputeClass
	if c.Scorable() {
		t.Error("a nil class is scorable")
	}
}
