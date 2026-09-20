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
	"reflect"
	"testing"
)

// n4Node is an ordinary on-demand n4, which is what most rules in these tests
// are trying to accept or reject.
func n4Node() NodeProfile {
	return NodeProfile{
		InstanceType:  "n4-highmem-2",
		MachineFamily: "n4",
		Spot:          boolPtr(false),
		Cores:         2,
		MemoryGB:      16,
	}
}

// rule is a one-liner for a rule body.
func rule(body map[string]any) PreferenceRule { return PreferenceRule{Raw: body} }

func TestMatch_ASparseRuleConstrainsOnlyWhatItNames(t *testing.T) {
	// {machineFamily: n4} wildcards spot, cores, everything. Treating an
	// unspecified field as required-empty would reject every real node.
	a := axisOf("x", rule(map[string]any{"machineFamily": "n4"}), rule(map[string]any{"machineFamily": "n2"}))
	got := a.Match(n4Node())
	if !got.Matched || got.Index != 0 {
		t.Errorf("Match = %+v, want the n4 rule at index 0", got)
	}
	if got.Ambiguous || len(got.Unsupported) != 0 {
		t.Errorf("Match = %+v, want a clean single match", got)
	}
}

func TestMatch_ARuleWithNoConstraintsMatchesEverything(t *testing.T) {
	// A bare {} — and a rule carrying only a priorityScore, which is the
	// ordering and not a constraint — are both wildcards.
	for name, body := range map[string]map[string]any{
		"empty":          {},
		"score only":     {ruleFieldPriorityScore: int64(50)},
		"score and kind": {ruleFieldPriorityScore: int64(50), "machineFamily": "n4"},
	} {
		t.Run(name, func(t *testing.T) {
			got := axisOf("x", rule(body)).Match(n4Node())
			if !got.Matched {
				t.Errorf("Match = %+v, want a match", got)
			}
		})
	}
}

func TestMatch_NoRuleAccepts(t *testing.T) {
	a := axisOf("x", rule(map[string]any{"machineFamily": "c3"}), rule(map[string]any{"machineFamily": "n2"}))
	got := a.Match(n4Node())
	if got.Matched || got.Index != -1 {
		t.Errorf("Match = %+v, want no match and index -1", got)
	}
}

func TestMatch_FirstMatchWinsAndTheOverlapIsCounted(t *testing.T) {
	// Where rules overlap, attribution is arbitrary. §7.7.5's position is to
	// count it rather than hide it — and to note that it is less harmful
	// than it looks, since two overlapping rules at the same tier give the
	// same rank either way.
	a := axisOf("x",
		rule(map[string]any{"machineFamily": "n4"}),
		rule(map[string]any{"spot": false}),
		rule(map[string]any{"minCores": int64(1)}),
	)
	got := a.Match(n4Node())
	if got.Index != 0 || !got.Matched {
		t.Errorf("Match = %+v, want the first accepting rule", got)
	}
	if !got.Ambiguous {
		t.Error("three rules accepted the node and the overlap was not reported")
	}
}

// TestMatch_AnUnmodelledFieldIsDeclinedNotGuessed is §7.7.2's fail-closed
// rule. Priority rules are open-ended and GKE adds fields over time; a matcher
// that ignored what it did not understand would match rules it should not,
// attributing nodes to the wrong rule and corrupting the cross-check into
// agreement with nothing.
func TestMatch_AnUnmodelledFieldIsDeclinedNotGuessed(t *testing.T) {
	a := axisOf("x",
		// Would match on machineFamily alone, and must not.
		rule(map[string]any{"machineFamily": "n4", "nodepools": []any{"pool-1"}}),
		rule(map[string]any{"machineFamily": "n4"}),
	)
	got := a.Match(n4Node())

	if got.Index != 1 {
		t.Errorf("Index = %d, want 1 — the rule with the unmodelled field must be skipped, not matched", got.Index)
	}
	want := []UnsupportedRule{{Index: 0, Field: "nodepools"}}
	if !reflect.DeepEqual(got.Unsupported, want) {
		t.Errorf("Unsupported = %+v, want %+v", got.Unsupported, want)
	}
}

func TestMatch_TheReportedUnsupportedFieldIsStable(t *testing.T) {
	// Two unknown fields in one rule. Map order would make the metric label
	// flap between them on every resync, so the first is the sorted first.
	a := axisOf("x", rule(map[string]any{"zzz": 1, "aaa": 2, "machineFamily": "n4"}))
	for i := 0; i < 20; i++ {
		got := a.Match(n4Node())
		if len(got.Unsupported) != 1 || got.Unsupported[0].Field != "aaa" {
			t.Fatalf("Unsupported = %+v, want the alphabetically first unknown field every time", got.Unsupported)
		}
	}
}

// TestMatch_AFieldWeCannotEvaluateIsAlsoDeclined covers the second half of
// fail-closed: a field this matcher understands, on a node carrying nothing
// to evaluate it against.
func TestMatch_AFieldWeCannotEvaluateIsAlsoDeclined(t *testing.T) {
	tests := []struct {
		name  string
		body  map[string]any
		node  NodeProfile
		field string
	}{
		{
			"an unlabelled node is not a node whose family is empty",
			map[string]any{"machineFamily": "n4"}, NodeProfile{}, "machineFamily",
		},
		{
			"spot is unknown, not false",
			map[string]any{"spot": false}, NodeProfile{MachineFamily: "n4"}, "spot",
		},
		{
			"no capacity to compare a core floor against",
			map[string]any{"minCores": int64(4)}, NodeProfile{MachineFamily: "n4"}, "minCores",
		},
		{
			"no capacity to compare a memory floor against",
			map[string]any{"minMemoryGb": int64(8)}, NodeProfile{MachineFamily: "n4"}, "minMemoryGb",
		},
		{
			"a podFamily rule with no configured source",
			map[string]any{"podFamily": "general-purpose"}, n4Node(), "podFamily",
		},
		{
			"a rule value of the wrong type",
			map[string]any{"machineFamily": int64(4)}, n4Node(), "machineFamily",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := axisOf("x", rule(tc.body)).Match(tc.node)
			if got.Matched {
				t.Fatalf("Match = %+v, want the rule declined", got)
			}
			want := []UnsupportedRule{{Index: 0, Field: tc.field}}
			if !reflect.DeepEqual(got.Unsupported, want) {
				t.Errorf("Unsupported = %+v, want %+v", got.Unsupported, want)
			}
		})
	}
}

func TestMatch_Scalars(t *testing.T) {
	tests := []struct {
		name string
		body map[string]any
		node NodeProfile
		want bool
	}{
		{"family hit", map[string]any{"machineFamily": "n4"}, n4Node(), true},
		{"family miss", map[string]any{"machineFamily": "c3"}, n4Node(), false},
		{"type hit", map[string]any{"machineType": "n4-highmem-2"}, n4Node(), true},
		{"type miss", map[string]any{"machineType": "n4-standard-8"}, n4Node(), false},
		{"spot false hit", map[string]any{"spot": false}, n4Node(), true},
		{"spot true miss", map[string]any{"spot": true}, n4Node(), false},
		{"cores met exactly", map[string]any{"minCores": int64(2)}, n4Node(), true},
		{"cores exceeded", map[string]any{"minCores": int64(1)}, n4Node(), true},
		{"cores short", map[string]any{"minCores": int64(8)}, n4Node(), false},
		{"memory as a float", map[string]any{"minMemoryGb": 15.5}, n4Node(), true},
		{"memory short", map[string]any{"minMemoryGb": float64(64)}, n4Node(), false},
		{"a plain int decodes too", map[string]any{"minCores": 2}, n4Node(), true},
		{"every field must hold", map[string]any{"machineFamily": "n4", "minCores": int64(8)}, n4Node(), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := axisOf("x", rule(tc.body)).Match(tc.node)
			if got.Matched != tc.want {
				t.Errorf("Matched = %v, want %v (unsupported: %+v)", got.Matched, tc.want, got.Unsupported)
			}
		})
	}
}

func TestMatch_GPUComparesTheTypeVerbatim(t *testing.T) {
	gpuNode := n4Node()
	gpuNode.Accelerator = "nvidia-tesla-t4"

	hit := axisOf("x", rule(map[string]any{"gpu": map[string]any{"type": "nvidia-tesla-t4"}})).Match(gpuNode)
	if !hit.Matched {
		t.Errorf("a matching GPU type did not match: %+v", hit)
	}
	miss := axisOf("x", rule(map[string]any{"gpu": map[string]any{"type": "nvidia-l4"}})).Match(gpuNode)
	if miss.Matched {
		t.Error("a different GPU type matched")
	}
	// An absent accelerator label means no GPU, which is a clean miss and
	// not something we failed to evaluate.
	none := axisOf("x", rule(map[string]any{"gpu": map[string]any{"type": "nvidia-l4"}})).Match(n4Node())
	if none.Matched || len(none.Unsupported) != 0 {
		t.Errorf("a GPU rule against a GPU-less node = %+v, want a plain miss", none)
	}
}

// TestMatch_AGPUCountIsDeclined keeps the matcher from attributing a one-GPU
// node to an eight-GPU rule. Nothing observed on a node carries the count, so
// a rule constraining it cannot be judged — and on GKE the annotation is the
// primary path anyway, so declining costs nothing.
func TestMatch_AGPUCountIsDeclined(t *testing.T) {
	gpuNode := n4Node()
	gpuNode.Accelerator = "nvidia-tesla-t4"
	got := axisOf("x", rule(map[string]any{"gpu": map[string]any{"type": "nvidia-tesla-t4", "count": int64(8)}})).Match(gpuNode)

	if got.Matched {
		t.Fatalf("a rule constraining GPU count was matched on the type alone: %+v", got)
	}
	if want := []UnsupportedRule{{Index: 0, Field: "gpu.count"}}; !reflect.DeepEqual(got.Unsupported, want) {
		t.Errorf("Unsupported = %+v, want %+v — the dotted subfield, so the metric names it", got.Unsupported, want)
	}
}

func TestMatch_GPUMalformed(t *testing.T) {
	for name, body := range map[string]map[string]any{
		"not a map":     {"gpu": "nvidia-tesla-t4"},
		"no type field": {"gpu": map[string]any{}},
		"empty type":    {"gpu": map[string]any{"type": ""}},
	} {
		t.Run(name, func(t *testing.T) {
			got := axisOf("x", rule(body)).Match(n4Node())
			if got.Matched || len(got.Unsupported) != 1 {
				t.Errorf("Match = %+v, want a declined rule", got)
			}
		})
	}
}

// reservedNode consumed a specific reservation in another project — the shape
// S3 established is possible, and the reason identity is a pair.
func reservedNode() NodeProfile {
	p := n4Node()
	p.Reservation = ReservationRef{Name: "block-a", Project: "other-proj", Affinity: "specific"}
	return p
}

func TestMatch_ReservationsComparesThePairNeverTheName(t *testing.T) {
	specific := func(entries ...map[string]any) map[string]any {
		list := make([]any, len(entries))
		for i, e := range entries {
			list[i] = e
		}
		return map[string]any{"reservations": map[string]any{"affinity": "specific", "specific": list}}
	}

	tests := []struct {
		name string
		body map[string]any
		node NodeProfile
		want bool
	}{
		{
			"the pair matches",
			specific(map[string]any{"name": "block-a", "project": "other-proj"}), reservedNode(), true,
		},
		{
			"same name, different project",
			specific(map[string]any{"name": "block-a", "project": "ours"}), reservedNode(), false,
		},
		{
			"one of several entries matches",
			specific(
				map[string]any{"name": "block-z", "project": "other-proj"},
				map[string]any{"name": "block-a", "project": "other-proj"},
			), reservedNode(), true,
		},
		{
			"a node that consumed nothing",
			specific(map[string]any{"name": "block-a", "project": "other-proj"}), n4Node(), false,
		},
		{
			"affinity any, and the node took one",
			map[string]any{"reservations": map[string]any{"affinity": "any"}}, reservedNode(), true,
		},
		{
			"affinity any, and the node took none",
			map[string]any{"reservations": map[string]any{"affinity": "any"}}, n4Node(), false,
		},
		{
			"affinity is case-insensitive on the rule side too",
			map[string]any{"reservations": map[string]any{"affinity": "ANY"}}, reservedNode(), true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := axisOf("x", rule(tc.body)).Match(tc.node)
			if got.Matched != tc.want {
				t.Errorf("Matched = %v, want %v (%+v)", got.Matched, tc.want, got)
			}
		})
	}
}

func TestMatch_ReservationsMalformed(t *testing.T) {
	tests := map[string]struct {
		body  map[string]any
		field string
	}{
		"not a map":            {map[string]any{"reservations": "block-a"}, "reservations.<malformed>"},
		"an unknown subfield":  {map[string]any{"reservations": map[string]any{"affinity": "any", "zone": "us-central1-a"}}, "reservations.zone"},
		"an unknown affinity":  {map[string]any{"reservations": map[string]any{"affinity": "sometimes"}}, "reservations.affinity"},
		"no affinity at all":   {map[string]any{"reservations": map[string]any{}}, "reservations.affinity"},
		"specific is not list": {map[string]any{"reservations": map[string]any{"affinity": "specific", "specific": "block-a"}}, "reservations.specific"},
		"an entry is not a map": {map[string]any{"reservations": map[string]any{
			"affinity": "specific", "specific": []any{"block-a"},
		}}, "reservations.specific"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := axisOf("x", rule(tc.body)).Match(reservedNode())
			if got.Matched {
				t.Fatalf("Match = %+v, want declined", got)
			}
			if want := []UnsupportedRule{{Index: 0, Field: tc.field}}; !reflect.DeepEqual(got.Unsupported, want) {
				t.Errorf("Unsupported = %+v, want %+v", got.Unsupported, want)
			}
		})
	}
}

func TestMatch_ASpecificRuleWithAnEmptyNameDoesNotMatchAnUnreservedNode(t *testing.T) {
	// Otherwise "" == "" would attribute every reservation-less node to it.
	body := map[string]any{"reservations": map[string]any{
		"affinity": "specific", "specific": []any{map[string]any{"project": "p"}},
	}}
	if got := axisOf("x", rule(body)).Match(n4Node()); got.Matched {
		t.Errorf("Match = %+v, want no match", got)
	}
}

func TestMatch_NilAxis(t *testing.T) {
	var a *PreferenceAxis
	got := a.Match(n4Node())
	if got.Matched || got.Index != -1 {
		t.Errorf("Match on a nil axis = %+v, want no match", got)
	}
}

func TestMatch_EveryRuleUnsupportedIsReportedInListOrder(t *testing.T) {
	a := axisOf("x",
		rule(map[string]any{"storage": "ssd"}),
		rule(map[string]any{"machineFamily": "n4"}),
		rule(map[string]any{"nodepools": []any{"p"}}),
	)
	got := a.Match(n4Node())
	want := []UnsupportedRule{{Index: 0, Field: "storage"}, {Index: 2, Field: "nodepools"}}
	if !reflect.DeepEqual(got.Unsupported, want) {
		t.Errorf("Unsupported = %+v, want %+v", got.Unsupported, want)
	}
	if got.Index != 1 {
		t.Errorf("Index = %d, want the one supported rule", got.Index)
	}
}

func TestLowerASCII(t *testing.T) {
	// Deliberately not strings.ToLower: Unicode case folding has no business
	// deciding whether a reservation affinity says "specific".
	cases := map[string]string{"SPECIFIC": "specific", "Any": "any", "": "", "a1-B2": "a1-b2"}
	for in, want := range cases {
		if got := lowerASCII(in); got != want {
			t.Errorf("lowerASCII(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAsFloat(t *testing.T) {
	for _, v := range []any{int64(3), float64(3), int(3)} {
		if got, ok := asFloat(v); !ok || got != 3 {
			t.Errorf("asFloat(%T) = %v, %v", v, got, ok)
		}
	}
	if _, ok := asFloat("3"); ok {
		t.Error("asFloat accepted a string")
	}
}
