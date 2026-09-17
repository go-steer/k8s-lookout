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

	v1 "k8s.io/api/core/v1"
)

// allSources is the precedence list from §5.1, written out in the order the
// design states it. The point of repeating it here rather than deriving it from
// the constants is that a test derived from the thing it tests proves nothing:
// this slice is the design document, and the constants have to match it.
var allSources = []IntentSource{
	SourcePolicyCRD,
	SourceWorkloadAnnotation,
	SourceTopologySpreadConstraint,
	SourcePodAntiAffinityRequired,
	SourceClusterDefaultDeclared,
	SourceClusterDefaultAssumed,
	SourcePodAntiAffinityPreferred,
	SourceLearnedBaseline,
}

func TestIntentSource_PrecedenceOrder(t *testing.T) {
	for i := 0; i < len(allSources); i++ {
		for j := 0; j < len(allSources); j++ {
			hi, lo := allSources[i], allSources[j]
			want := i < j
			if got := hi.Outranks(lo); got != want {
				t.Errorf("%s.Outranks(%s) = %v, want %v (positions %d and %d)", hi, lo, got, want, i, j)
			}
		}
	}

	// Precedence must be a strict order: nothing outranks itself, or resolution
	// would depend on candidate ordering for equal sources.
	for _, s := range allSources {
		if s.Outranks(s) {
			t.Errorf("%s outranks itself", s)
		}
	}
}

// TestIntentSource_StringsAreStableAndDistinct guards the metric labels. Two
// sources sharing a spelling would silently merge two series on the
// intent_info gauge, and a typo'd label is a dashboard that reads "unknown"
// forever.
func TestIntentSource_StringsAreStableAndDistinct(t *testing.T) {
	want := map[IntentSource]string{
		SourcePolicyCRD:                "policy-crd",
		SourceWorkloadAnnotation:       "workload-annotation",
		SourceTopologySpreadConstraint: "topology-spread-constraint",
		SourcePodAntiAffinityRequired:  "pod-anti-affinity-required",
		SourceClusterDefaultDeclared:   "cluster-default-declared",
		SourceClusterDefaultAssumed:    "cluster-default-assumed",
		SourcePodAntiAffinityPreferred: "pod-anti-affinity-preferred",
		SourceLearnedBaseline:          "learned-baseline",
	}
	if len(want) != len(allSources) {
		t.Fatalf("a source was added without a pinned label: %d labels for %d sources", len(want), len(allSources))
	}
	seen := make(map[string]IntentSource, len(want))
	for _, s := range allSources {
		got := s.String()
		if got != want[s] {
			t.Errorf("source %d String() = %q, want %q", s, got, want[s])
		}
		if prev, dup := seen[got]; dup {
			t.Errorf("sources %d and %d share the label %q", prev, s, got)
		}
		seen[got] = s
	}

	if got := IntentSource(200).String(); got != "unknown" {
		t.Errorf("out-of-range source String() = %q, want %q", got, "unknown")
	}
}

func TestConfidence_Strings(t *testing.T) {
	want := map[Confidence]string{
		ConfidenceDeclared: "declared",
		ConfidenceInferred: "inferred",
		ConfidenceAssumed:  "assumed",
		ConfidenceLearned:  "learned",
	}
	for c, s := range want {
		if got := c.String(); got != s {
			t.Errorf("Confidence(%d).String() = %q, want %q", c, got, s)
		}
	}
	if got := Confidence(9).String(); got != "unknown" {
		t.Errorf("out-of-range confidence = %q, want unknown", got)
	}
}

func TestIntentMode_And_Weighting_Strings(t *testing.T) {
	if got := ModeSpread.String(); got != "Spread" {
		t.Errorf("ModeSpread = %q", got)
	}
	if got := ModeColocate.String(); got != "Colocate" {
		t.Errorf("ModeColocate = %q", got)
	}
	if got := ModeIgnore.String(); got != "Ignore" {
		t.Errorf("ModeIgnore = %q", got)
	}
	if got := IntentMode(7).String(); got != "Unknown" {
		t.Errorf("out-of-range mode = %q", got)
	}

	weights := map[Weighting]string{
		WeightEqual:             "Equal",
		WeightNodeCount:         "NodeCount",
		WeightAllocatableCPU:    "AllocatableCPU",
		WeightAllocatableMemory: "AllocatableMemory",
	}
	for w, s := range weights {
		if got := w.String(); got != s {
			t.Errorf("Weighting(%d) = %q, want %q", w, got, s)
		}
	}
	if got := Weighting(9).String(); got != "Unknown" {
		t.Errorf("out-of-range weighting = %q", got)
	}
}

func skewPtr(v int32) *int32 { return &v }

func TestIntent_HardContract(t *testing.T) {
	tests := []struct {
		name string
		in   *Intent
		want bool
	}{
		{"nil intent", nil, false},
		{
			"no maxSkew",
			&Intent{Source: SourceTopologySpreadConstraint, WhenUnsatisfiable: v1.DoNotSchedule},
			false,
		},
		{
			"schedule anyway is a preference",
			&Intent{Source: SourceTopologySpreadConstraint, MaxSkew: skewPtr(1), WhenUnsatisfiable: v1.ScheduleAnyway},
			false,
		},
		{
			"declared DoNotSchedule",
			&Intent{Source: SourceTopologySpreadConstraint, MaxSkew: skewPtr(1), WhenUnsatisfiable: v1.DoNotSchedule},
			true,
		},
		{
			// An operator may legitimately declare a DoNotSchedule cluster
			// default, and having told us, they get the contract.
			"declared cluster default still counts",
			&Intent{Source: SourceClusterDefaultDeclared, MaxSkew: skewPtr(1), WhenUnsatisfiable: v1.DoNotSchedule},
			true,
		},
		{
			// S4: the same shape, sourced from a default we guessed at because
			// managed GKE does not expose the scheduler config. Inventing a
			// contract is the one thing this must not do.
			"assumed cluster default never counts",
			&Intent{Source: SourceClusterDefaultAssumed, MaxSkew: skewPtr(1), WhenUnsatisfiable: v1.DoNotSchedule},
			false,
		},
		{
			// maxSkew 0 is a real, very tight contract — not the absence of one.
			// Conflating it with nil is exactly the three-state trap from S4.
			"zero maxSkew is a contract",
			&Intent{Source: SourcePolicyCRD, MaxSkew: skewPtr(0), WhenUnsatisfiable: v1.DoNotSchedule},
			true,
		},
		{
			// An empty action is not DoNotSchedule. Kubernetes defaults the
			// field at admission, but an intent synthesised in-process may not
			// have been through admission, and defaulting to the enforcing
			// value here would be the unsafe direction to guess.
			"empty action is not a contract",
			&Intent{Source: SourceTopologySpreadConstraint, MaxSkew: skewPtr(1)},
			false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.in.HardContract(); got != tc.want {
				t.Errorf("HardContract() = %v, want %v", got, tc.want)
			}
		})
	}
}

const (
	zoneKey TopologyKey = "topology.kubernetes.io/zone"
	hostKey TopologyKey = "kubernetes.io/hostname"
)

func TestResolveIntents_Empty(t *testing.T) {
	got := ResolveIntents(nil)
	if got == nil {
		t.Fatal("ResolveIntents(nil) returned a nil map; callers range over this")
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
}

func TestResolveIntents_HighestPrecedenceWinsPerKey(t *testing.T) {
	candidates := []Intent{
		{TopologyKey: zoneKey, Source: SourcePodAntiAffinityPreferred, Mode: ModeSpread},
		{TopologyKey: zoneKey, Source: SourceTopologySpreadConstraint, Mode: ModeSpread, MaxSkew: skewPtr(1)},
		{TopologyKey: zoneKey, Source: SourceLearnedBaseline, Mode: ModeSpread},
	}
	got := ResolveIntents(candidates)

	if len(got) != 1 {
		t.Fatalf("resolved %d keys, want 1", len(got))
	}
	win := got[zoneKey]
	if win.Source != SourceTopologySpreadConstraint {
		t.Errorf("winner = %s, want topology-spread-constraint", win.Source)
	}
	if win.MaxSkew == nil || *win.MaxSkew != 1 {
		t.Errorf("winner lost its maxSkew: %v", win.MaxSkew)
	}
}

// TestResolveIntents_DifferentKeysCoexist: spreading across zones and
// colocating within a rack are not in conflict, and collapsing them to one
// intent would make one of the two unexpressible.
func TestResolveIntents_DifferentKeysCoexist(t *testing.T) {
	got := ResolveIntents([]Intent{
		{TopologyKey: zoneKey, Source: SourceTopologySpreadConstraint, Mode: ModeSpread},
		{TopologyKey: hostKey, Source: SourcePodAntiAffinityRequired, Mode: ModeColocate},
	})
	if len(got) != 2 {
		t.Fatalf("resolved %d keys, want 2", len(got))
	}
	if got[zoneKey].Mode != ModeSpread {
		t.Errorf("zone mode = %s, want Spread", got[zoneKey].Mode)
	}
	if got[hostKey].Mode != ModeColocate {
		t.Errorf("host mode = %s, want Colocate", got[hostKey].Mode)
	}
}

// TestResolveIntents_LosersBecomeEvidence is the explainability requirement. A
// finding that says "you are skewed on zone" when the user is looking at a
// preferred anti-affinity rule they wrote reads as a bug unless the evidence
// says the TSC overrode it.
func TestResolveIntents_LosersBecomeEvidence(t *testing.T) {
	got := ResolveIntents([]Intent{
		{
			TopologyKey: zoneKey,
			Source:      SourcePodAntiAffinityPreferred,
			Evidence:    []EvidenceItem{{Source: SourcePodAntiAffinityPreferred, Detail: "weight 100"}},
		},
		{TopologyKey: zoneKey, Source: SourceTopologySpreadConstraint},
	})

	win := got[zoneKey]
	if len(win.Evidence) != 2 {
		t.Fatalf("evidence = %+v, want the loser and its own trail", win.Evidence)
	}
	for _, e := range win.Evidence {
		if !e.Ignored {
			t.Errorf("carried-forward evidence %+v must be flagged Ignored", e)
		}
		if e.Source != SourcePodAntiAffinityPreferred {
			t.Errorf("evidence source = %s, want the demoted source", e.Source)
		}
	}
	if win.Evidence[1].Detail != "weight 100" {
		t.Errorf("the loser's own evidence trail was dropped: %+v", win.Evidence)
	}
}

// The reverse arrival order must produce the same winner and the same
// evidence, because informer event order is not something we control.
func TestResolveIntents_IsOrderIndependentAcrossSources(t *testing.T) {
	forward := ResolveIntents([]Intent{
		{TopologyKey: zoneKey, Source: SourceLearnedBaseline},
		{TopologyKey: zoneKey, Source: SourceWorkloadAnnotation},
		{TopologyKey: zoneKey, Source: SourcePodAntiAffinityRequired},
	})
	backward := ResolveIntents([]Intent{
		{TopologyKey: zoneKey, Source: SourcePodAntiAffinityRequired},
		{TopologyKey: zoneKey, Source: SourceWorkloadAnnotation},
		{TopologyKey: zoneKey, Source: SourceLearnedBaseline},
	})

	if forward[zoneKey].Source != SourceWorkloadAnnotation {
		t.Errorf("forward winner = %s, want workload-annotation", forward[zoneKey].Source)
	}
	if backward[zoneKey].Source != SourceWorkloadAnnotation {
		t.Errorf("backward winner = %s, want workload-annotation", backward[zoneKey].Source)
	}

	sources := func(m map[TopologyKey]*Intent) []IntentSource {
		out := []IntentSource{}
		for _, e := range m[zoneKey].Evidence {
			out = append(out, e.Source)
		}
		return out
	}
	// Both losers must be present either way; only their order may differ.
	countOf := func(ss []IntentSource) map[IntentSource]int {
		m := map[IntentSource]int{}
		for _, s := range ss {
			m[s]++
		}
		return m
	}
	if !reflect.DeepEqual(countOf(sources(forward)), countOf(sources(backward))) {
		t.Errorf("evidence differs by arrival order: %v vs %v", sources(forward), sources(backward))
	}
}

// TestResolveIntents_EqualSourcesKeepTheFirst pins the tie-break, which exists
// so that two TSCs on one key resolve the same way every evaluation rather than
// flapping.
func TestResolveIntents_EqualSourcesKeepTheFirst(t *testing.T) {
	got := ResolveIntents([]Intent{
		{TopologyKey: zoneKey, Source: SourceTopologySpreadConstraint, MaxSkew: skewPtr(1)},
		{TopologyKey: zoneKey, Source: SourceTopologySpreadConstraint, MaxSkew: skewPtr(5)},
	})
	win := got[zoneKey]
	if win.MaxSkew == nil || *win.MaxSkew != 1 {
		t.Errorf("tie kept maxSkew %v, want the first candidate's 1", win.MaxSkew)
	}
	if len(win.Evidence) != 1 || !win.Evidence[0].Ignored {
		t.Errorf("the tied loser should be retained as ignored evidence, got %+v", win.Evidence)
	}
}

// TestResolveIntents_DoesNotAliasTheInput: the resolved map must not hand out
// pointers into the caller's slice, or appending evidence would mutate the
// candidate list and a second Resolve over the same slice would see the first
// call's output.
func TestResolveIntents_DoesNotAliasTheInput(t *testing.T) {
	candidates := []Intent{
		{TopologyKey: zoneKey, Source: SourcePodAntiAffinityPreferred},
		{TopologyKey: zoneKey, Source: SourceTopologySpreadConstraint},
	}
	first := ResolveIntents(candidates)
	if n := len(first[zoneKey].Evidence); n != 1 {
		t.Fatalf("first resolve produced %d evidence items, want 1", n)
	}

	for i, c := range candidates {
		if len(c.Evidence) != 0 {
			t.Errorf("candidate %d was mutated: %+v", i, c.Evidence)
		}
	}

	second := ResolveIntents(candidates)
	if n := len(second[zoneKey].Evidence); n != 1 {
		t.Errorf("second resolve produced %d evidence items, want 1 — state leaked between calls", n)
	}
}

func TestSortedKeys(t *testing.T) {
	in := map[TopologyKey]*Intent{
		"topology.kubernetes.io/zone":   {},
		"kubernetes.io/hostname":        {},
		"topology.kubernetes.io/region": {},
	}
	want := []TopologyKey{
		"kubernetes.io/hostname",
		"topology.kubernetes.io/region",
		"topology.kubernetes.io/zone",
	}
	for i := 0; i < 20; i++ {
		if got := SortedKeys(in); !reflect.DeepEqual(got, want) {
			t.Fatalf("SortedKeys = %v, want %v", got, want)
		}
	}
	if got := SortedKeys(map[TopologyKey]*Intent{}); len(got) != 0 {
		t.Errorf("SortedKeys(empty) = %v, want empty", got)
	}
}
