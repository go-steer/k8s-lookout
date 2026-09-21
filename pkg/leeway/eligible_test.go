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
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
)

// node returns a healthy, matching, tolerated node in the given domain. Tests
// state only the thing they are varying, so the interesting field of each case
// is visible rather than buried in six booleans.
func node(name string, domain Domain, opts ...func(*NodeView)) NodeView {
	n := NodeView{
		Name:            name,
		Domain:          domain,
		Ready:           true,
		Schedulable:     true,
		MatchesSelector: true,
		Tolerated:       true,
		AllocatableCPU:  1,
	}
	for _, o := range opts {
		o(&n)
	}
	return n
}

func notReady(n *NodeView)     { n.Ready = false }
func cordoned(n *NodeView)     { n.Schedulable = false }
func noMatch(n *NodeView)      { n.MatchesSelector = false }
func notTolerated(n *NodeView) { n.Tolerated = false }

// capacity sets both allocatable dimensions to the same number, which is what
// most of these tests want: they are about eligibility and apportionment, not
// about which resource was read. memory overrides the second one, so a test
// that asserts memory weighting reads the memory field can make the two
// disagree and would fail if it silently read CPU.
func capacity(c float64) func(*NodeView) {
	return func(n *NodeView) { n.AllocatableCPU, n.AllocatableMemory = c, c }
}

func memory(m float64) func(*NodeView) {
	return func(n *NodeView) { n.AllocatableMemory = m }
}

func TestEligibleDomains_OneUsableNodeMakesADomainEligible(t *testing.T) {
	// The existential quantifier, stated as a test: domain "b" has three nodes
	// and only one is usable, and that is enough. Requiring more would make
	// eligibility depend on how healthy a zone happens to be right now, and a
	// zone would drop out of the expectation the moment it got busy.
	got := EligibleDomains([]NodeView{
		node("a1", "a"),
		node("b1", "b", notReady),
		node("b2", "b", cordoned),
		node("b3", "b"),
	}, DefaultEligibilityOptions())

	if want := []Domain{"a", "b"}; !reflect.DeepEqual(got.Domains, want) {
		t.Errorf("Domains = %v, want %v", got.Domains, want)
	}
	if want := []int64{1, 1}; !reflect.DeepEqual(got.NodeCount, want) {
		t.Errorf("NodeCount = %v, want %v — only usable nodes count", got.NodeCount, want)
	}
	if len(got.Excluded) != 0 {
		t.Errorf("Excluded = %v, want empty", got.Excluded)
	}
}

func TestEligibleDomains_ExclusionsAreExplained(t *testing.T) {
	got := EligibleDomains([]NodeView{
		node("a1", "a"),
		node("b1", "b", noMatch),
		node("c1", "c", notReady),
		node("d1", "d", cordoned),
	}, DefaultEligibilityOptions())

	if want := []Domain{"a"}; !reflect.DeepEqual(got.Domains, want) {
		t.Fatalf("Domains = %v, want %v", got.Domains, want)
	}
	want := map[Domain]string{
		"b": "no usable nodes: selector or required affinity does not match",
		"c": "no usable nodes: NotReady",
		"d": "no usable nodes: cordoned",
	}
	if !reflect.DeepEqual(got.Excluded, want) {
		t.Errorf("Excluded = %v, want %v", got.Excluded, want)
	}
}

// TestEligibleDomains_MixedExclusionReasonsAreAllReported: a domain excluded
// for two different reasons must say both, or the reader fixes the cordon and
// wonders why the zone is still missing.
func TestEligibleDomains_MixedExclusionReasonsAreAllReported(t *testing.T) {
	got := EligibleDomains([]NodeView{
		node("a1", "a"),
		node("b1", "b", cordoned),
		node("b2", "b", notReady),
		node("b3", "b", cordoned),
	}, DefaultEligibilityOptions())

	reason := got.Excluded["b"]
	if !strings.Contains(reason, "cordoned") || !strings.Contains(reason, "NotReady") {
		t.Errorf("Excluded[b] = %q, want both reasons", reason)
	}
	// Duplicates are folded: three nodes, two distinct reasons.
	if n := strings.Count(reason, "cordoned"); n != 1 {
		t.Errorf("reason repeats 'cordoned' %d times: %q", n, reason)
	}
}

// TestEligibleDomains_TaintsAreIgnoredByDefault pins the asymmetry with
// affinity. Diverging from kube-scheduler's defaults here would compute a
// different eligible set than the scheduler used, which is the one thing §7.1
// exists to avoid.
func TestEligibleDomains_TaintsAreIgnoredByDefault(t *testing.T) {
	nodes := []NodeView{node("a1", "a"), node("b1", "b", notTolerated)}

	byDefault := EligibleDomains(nodes, DefaultEligibilityOptions())
	if want := []Domain{"a", "b"}; !reflect.DeepEqual(byDefault.Domains, want) {
		t.Errorf("default taints policy: Domains = %v, want %v", byDefault.Domains, want)
	}

	opts := DefaultEligibilityOptions()
	opts.Policies.NodeTaintsPolicy = v1.NodeInclusionPolicyHonor
	honoured := EligibleDomains(nodes, opts)
	if want := []Domain{"a"}; !reflect.DeepEqual(honoured.Domains, want) {
		t.Errorf("Honor taints policy: Domains = %v, want %v", honoured.Domains, want)
	}
}

func TestEligibleDomains_AffinityPolicyIgnoreAdmitsNonMatchingNodes(t *testing.T) {
	nodes := []NodeView{node("a1", "a"), node("b1", "b", noMatch)}
	opts := DefaultEligibilityOptions()
	opts.Policies.NodeAffinityPolicy = v1.NodeInclusionPolicyIgnore

	got := EligibleDomains(nodes, opts)
	if want := []Domain{"a", "b"}; !reflect.DeepEqual(got.Domains, want) {
		t.Errorf("Domains = %v, want %v", got.Domains, want)
	}
}

// TestEligibleDomains_UnlabelledNodesBecomeAVisibleDomain: a node pool created
// without the topology label is the most common cause of real skew, and
// dropping those nodes would both hide the cause and produce a distribution
// that does not sum to the replica count.
func TestEligibleDomains_UnlabelledNodesBecomeAVisibleDomain(t *testing.T) {
	got := EligibleDomains([]NodeView{
		node("a1", "a"),
		node("x1", ""),
	}, DefaultEligibilityOptions())

	want := []Domain{"a", DomainUnknown}
	if !reflect.DeepEqual(got.Domains, want) {
		t.Errorf("Domains = %v, want %v (unknown sorts last)", got.Domains, want)
	}
}

func TestEligibleDomains_NoNodes(t *testing.T) {
	got := EligibleDomains(nil, DefaultEligibilityOptions())
	if len(got.Domains) != 0 {
		t.Errorf("Domains = %v, want none", got.Domains)
	}
	if got.Excluded == nil {
		t.Error("Excluded must be non-nil; callers index it")
	}
}

func TestEligibleDomains_MinDomainsPadsWithSyntheticDomains(t *testing.T) {
	three := int32(3)
	opts := DefaultEligibilityOptions()
	opts.MinDomains = &three

	got := EligibleDomains([]NodeView{node("a1", "a", capacity(8))}, opts)

	if len(got.Domains) != 3 {
		t.Fatalf("Domains = %v, want 3 entries", got.Domains)
	}
	if got.SyntheticDomains != 2 {
		t.Errorf("SyntheticDomains = %d, want 2", got.SyntheticDomains)
	}
	if got.Domains[0] != "a" {
		t.Errorf("real domain should come first, got %v", got.Domains)
	}
	for _, d := range got.Domains[1:] {
		if !IsSynthetic(d) {
			t.Errorf("%q should be synthetic", d)
		}
	}
	// Index alignment is what makes Capacity and NodeCount usable as weights.
	if len(got.CapacityCPU) != 3 || len(got.NodeCount) != 3 {
		t.Fatalf("Capacity=%v NodeCount=%v must stay index-aligned with Domains", got.CapacityCPU, got.NodeCount)
	}
	if got.CapacityCPU[1] != 0 || got.NodeCount[1] != 0 {
		t.Errorf("synthetic domain has capacity %v / %d nodes, want zero", got.CapacityCPU[1], got.NodeCount[1])
	}

	// §7.1: the whole point of the padding is that the skew becomes visible.
	// Without it, one domain holding everything scores a perfect zero.
	ap := Apportion(6, EqualWeights(len(got.Domains)), nil)
	s := Score(got.Domains, []int64{6, 0, 0}, ap, nil, DefaultThresholds())
	if s.ObservedSkew == 0 {
		t.Error("padding failed to expose the shortfall: skew is 0")
	}
}

func TestEligibleDomains_MinDomainsIsInertWhenSatisfied(t *testing.T) {
	two := int32(2)
	opts := DefaultEligibilityOptions()
	opts.MinDomains = &two

	got := EligibleDomains([]NodeView{node("a1", "a"), node("b1", "b"), node("c1", "c")}, opts)
	if got.SyntheticDomains != 0 {
		t.Errorf("SyntheticDomains = %d, want 0 — 3 domains already satisfy minDomains 2", got.SyntheticDomains)
	}
	if len(got.Domains) != 3 {
		t.Errorf("Domains = %v, want 3", got.Domains)
	}
}

func TestIsSynthetic(t *testing.T) {
	cases := map[Domain]bool{
		syntheticDomain(0): true,
		syntheticDomain(7): true,
		"__absent__/":      false, // the bare prefix names no domain
		"__absent__":       false,
		"us-central1-a":    false,
		DomainUnknown:      false,
		"":                 false,
	}
	for d, want := range cases {
		if got := IsSynthetic(d); got != want {
			t.Errorf("IsSynthetic(%q) = %v, want %v", d, got, want)
		}
	}
}

func TestSyntheticDomain_IsStable(t *testing.T) {
	// Stability is what keeps apportionment deterministic across evaluations:
	// a name derived from a counter or a timestamp would reorder the domain
	// list and make findings flap.
	for i := 0; i < 12; i++ {
		if a, b := syntheticDomain(i), syntheticDomain(i); a != b {
			t.Fatalf("syntheticDomain(%d) is not stable: %q vs %q", i, a, b)
		}
		if i > 0 && syntheticDomain(i) == syntheticDomain(i-1) {
			t.Fatalf("syntheticDomain(%d) collides with its predecessor", i)
		}
	}
}

func TestEligibility_Weights(t *testing.T) {
	// The memory numbers are deliberately not the CPU ones: a Weights that
	// read the wrong field would otherwise pass both capacity rows.
	e := EligibleDomains([]NodeView{
		node("a1", "a", capacity(4), memory(40)),
		node("a2", "a", capacity(4), memory(40)),
		node("b1", "b", capacity(16), memory(160)),
	}, DefaultEligibilityOptions())

	tests := []struct {
		mode Weighting
		want []float64
	}{
		{WeightEqual, []float64{1, 1}},
		{WeightNodeCount, []float64{2, 1}},
		{WeightAllocatableCPU, []float64{8, 16}},
		{WeightAllocatableMemory, []float64{80, 160}},
		// Auto is not a weighting and must not resolve itself here; see
		// Weights. EffectiveWeighting is what turns it into one.
		{WeightAuto, []float64{1, 1}},
		{Weighting(99), []float64{1, 1}},
	}
	for _, tc := range tests {
		t.Run(tc.mode.String(), func(t *testing.T) {
			if got := e.Weights(tc.mode); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Weights(%v) = %v, want %v", tc.mode, got, tc.want)
			}
		})
	}
}

// TestEligibility_WeightsDoNotAliasTheEligibility: Apportion sanitises weights
// in place in a copy, but a caller that scaled the returned slice would
// otherwise corrupt the capacity record the next weighting reads.
func TestEligibility_WeightsDoNotAliasTheEligibility(t *testing.T) {
	e := EligibleDomains([]NodeView{node("a1", "a", capacity(4)), node("b1", "b", capacity(8))}, DefaultEligibilityOptions())

	w := e.Weights(WeightAllocatableCPU)
	w[0] = 999
	if e.CapacityCPU[0] != 4 {
		t.Errorf("mutating the weights changed Eligibility.CapacityCPU: %v", e.CapacityCPU)
	}

	nc := e.Weights(WeightNodeCount)
	nc[0] = 999
	if e.NodeCount[0] != 1 {
		t.Errorf("mutating the weights changed Eligibility.NodeCount: %v", e.NodeCount)
	}
}

func TestEligibility_EffectiveWeighting(t *testing.T) {
	// 4 and 16 cores: a 4× spread, well over the 1.25 trigger.
	uneven := EligibleDomains([]NodeView{
		node("a1", "a", capacity(4)),
		node("b1", "b", capacity(16)),
	}, DefaultEligibilityOptions())
	// 100 and 104: real clusters are never exactly equal, and 1.04× must not
	// move a homogeneous cluster off the even expectation.
	even := EligibleDomains([]NodeView{
		node("a1", "a", capacity(100)),
		node("b1", "b", capacity(104)),
	}, DefaultEligibilityOptions())

	maxSkew := int32(1)
	ceiling := int64(2)

	tests := []struct {
		name string
		e    Eligibility
		in   *Intent
		want Weighting
	}{
		{"nil intent on an uneven cluster takes the trigger", uneven, nil, WeightAllocatableCPU},
		{"nil intent on an even cluster does not", even, nil, WeightEqual},
		{"undeclared on an uneven cluster takes the trigger", uneven, &Intent{}, WeightAllocatableCPU},
		{"undeclared on an even cluster does not", even, &Intent{}, WeightEqual},

		// A declaration is a declaration. §7.2 makes capacity weighting a
		// default, and a default that overrode what the operator wrote would
		// be a rule, so an explicit Equal survives the trigger.
		{"declared Equal beats the trigger", uneven, &Intent{Weighting: WeightEqual}, WeightEqual},
		{"declared NodeCount beats the trigger", uneven, &Intent{Weighting: WeightNodeCount}, WeightNodeCount},
		{"declared memory beats the trigger", even, &Intent{Weighting: WeightAllocatableMemory}, WeightAllocatableMemory},

		// The pod-count carve-out. Both of these are contracts over objects,
		// and a capacity-weighted expectation raises MinAchievableSkew until
		// they cannot be violated; see EffectiveWeighting.
		{"a maxSkew keeps the even expectation", uneven, &Intent{MaxSkew: &maxSkew}, WeightEqual},
		{"a per-domain ceiling keeps the even expectation", uneven, &Intent{MaxPerDomain: &ceiling}, WeightEqual},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.e.EffectiveWeighting(tc.in); got != tc.want {
				t.Errorf("EffectiveWeighting = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestEligibility_EffectiveWeightingIgnoresSyntheticPadding is the subtlest of
// the trigger's rules, and the one with the worst failure mode: synthetic
// MinDomains padding carries zero capacity, a zero makes the ratio infinite,
// and capacity weighting then apportions zero objects to the padding — so the
// missing domain the padding exists to expose scores as perfectly balanced.
func TestEligibility_EffectiveWeightingIgnoresSyntheticPadding(t *testing.T) {
	min := int32(3)
	opts := DefaultEligibilityOptions()
	opts.MinDomains = &min

	e := EligibleDomains([]NodeView{node("a1", "a", capacity(8))}, opts)
	if e.SyntheticDomains != 2 {
		t.Fatalf("SyntheticDomains = %d, want 2 — the fixture is not testing what it claims", e.SyntheticDomains)
	}
	if got := e.EffectiveWeighting(nil); got != WeightEqual {
		t.Errorf("EffectiveWeighting = %v, want Equal: the zero-capacity padding drove the ratio", got)
	}

	// And the even expectation still asks the padding to hold objects, which
	// is what makes the gap visible: a subject sitting [3 0 0] is measured
	// against [1 1 1] and scores the two missing domains, instead of being
	// apportioned [3 0 0] by capacity and coming out perfect.
	ap := Apportion(3, e.WeightsFor(nil), nil)
	if want := []int64{1, 1, 1}; !reflect.DeepEqual(ap.Expected, want) {
		t.Errorf("expected = %v, want %v — the padding must be expected to hold objects it does not have",
			ap.Expected, want)
	}
}

// TestEligibility_CapacityRatioTriggerIsConfigurable: the operator knob reaches
// the decision rather than only the free function it delegates to.
func TestEligibility_CapacityRatioTriggerIsConfigurable(t *testing.T) {
	nodes := []NodeView{node("a1", "a", capacity(4)), node("b1", "b", capacity(16))}

	opts := DefaultEligibilityOptions()
	if opts.CapacityRatioTrigger != CapacityWeightingRatioTrigger {
		t.Errorf("default trigger = %v, want %v", opts.CapacityRatioTrigger, CapacityWeightingRatioTrigger)
	}

	opts.CapacityRatioTrigger = 8 // above the fixture's 4× spread
	lenient := EligibleDomains(nodes, opts)
	if got := lenient.EffectiveWeighting(nil); got != WeightEqual {
		t.Errorf("EffectiveWeighting = %v, want Equal — a 4× spread should not cross an 8× trigger", got)
	}

	opts.CapacityRatioTrigger = 2
	strict := EligibleDomains(nodes, opts)
	if got := strict.EffectiveWeighting(nil); got != WeightAllocatableCPU {
		t.Errorf("EffectiveWeighting = %v, want AllocatableCPU at a 2× trigger", got)
	}
}

// TestEligibleDomains_IsDeterministic guards against map-iteration order
// reaching the result. Domains feed Apportion, whose tie-break is positional,
// so an unstable order here would make findings flap with nothing changed.
func TestEligibleDomains_IsDeterministic(t *testing.T) {
	nodes := []NodeView{
		node("z1", "z"), node("a1", "a"), node("m1", "m"),
		node("x1", ""), node("b1", "b", cordoned), node("c1", "c", noMatch),
	}
	first := EligibleDomains(nodes, DefaultEligibilityOptions())
	for i := 0; i < 50; i++ {
		again := EligibleDomains(nodes, DefaultEligibilityOptions())
		if !reflect.DeepEqual(first, again) {
			t.Fatalf("non-deterministic:\n %+v\n %+v", first, again)
		}
	}
	if want := []Domain{"a", "m", "z", DomainUnknown}; !reflect.DeepEqual(first.Domains, want) {
		t.Errorf("Domains = %v, want %v", first.Domains, want)
	}
}

func TestNormaliseDomain(t *testing.T) {
	if got := NormaliseDomain(""); got != DomainUnknown {
		t.Errorf("NormaliseDomain(\"\") = %q, want %q", got, DomainUnknown)
	}
	if got := NormaliseDomain("us-central1-a"); got != "us-central1-a" {
		t.Errorf("NormaliseDomain mangled a real value: %q", got)
	}
}

func TestDefaultEligibilityOptions(t *testing.T) {
	opts := DefaultEligibilityOptions()
	if opts.Policies.NodeAffinityPolicy != v1.NodeInclusionPolicyHonor {
		t.Errorf("NodeAffinityPolicy = %v, want Honor", opts.Policies.NodeAffinityPolicy)
	}
	if opts.Policies.NodeTaintsPolicy != v1.NodeInclusionPolicyIgnore {
		t.Errorf("NodeTaintsPolicy = %v, want Ignore", opts.Policies.NodeTaintsPolicy)
	}
	if !opts.RequireReady || !opts.RequireSchedulable {
		t.Errorf("health gates should default on: %+v", opts)
	}
	if opts.MinDomains != nil {
		t.Errorf("MinDomains should default to unset, got %v", *opts.MinDomains)
	}
}

func TestEligibility_WeightsFor(t *testing.T) {
	e := &Eligibility{
		Domains:     []Domain{"a", "b", "c"},
		NodeCount:   []int64{10, 5, 1},
		CapacityCPU: []float64{40, 20, 4},
	}

	t.Run("nil intent weights equally", func(t *testing.T) {
		if got := e.WeightsFor(nil); !reflect.DeepEqual(got, []float64{1, 1, 1}) {
			t.Errorf("WeightsFor(nil) = %v", got)
		}
	})

	t.Run("no explicit shares falls through to the weighting mode", func(t *testing.T) {
		got := e.WeightsFor(&Intent{Weighting: WeightNodeCount})
		if !reflect.DeepEqual(got, []float64{10, 5, 1}) {
			t.Errorf("WeightsFor = %v, want the node counts", got)
		}
	})

	t.Run("explicit shares beat the weighting mode", func(t *testing.T) {
		// The declaration is what the operator asked for; the weighting mode
		// is a rule for deriving one when they did not. Reading Weighting
		// here would apportion a declared 40/40/20 as node counts and report
		// drift against a distribution nobody asked for.
		got := e.WeightsFor(&Intent{
			Weighting:      WeightNodeCount,
			ExplicitShares: map[Domain]float64{"a": 40, "b": 40, "c": 20},
		})
		if !reflect.DeepEqual(got, []float64{40, 40, 20}) {
			t.Errorf("WeightsFor = %v, want the declared shares", got)
		}
	})

	t.Run("an unnamed eligible domain weights zero", func(t *testing.T) {
		got := e.WeightsFor(&Intent{ExplicitShares: map[Domain]float64{"a": 1, "b": 1}})
		if !reflect.DeepEqual(got, []float64{1, 1, 0}) {
			t.Errorf("WeightsFor = %v, want c to expect nothing", got)
		}
	})

	t.Run("a declaration naming only drained domains degrades to equal", func(t *testing.T) {
		// Every weight zero. Apportion reads that as "nothing distinguishes
		// these domains" and falls back to equal shares, which beats placing
		// the whole workload nowhere and calling all of it drift.
		w := e.WeightsFor(&Intent{ExplicitShares: map[Domain]float64{"gone": 100}})
		if !reflect.DeepEqual(w, []float64{0, 0, 0}) {
			t.Fatalf("WeightsFor = %v, want all zero", w)
		}
		ap := Apportion(3, w, nil)
		if !reflect.DeepEqual(ap.Expected, []int64{1, 1, 1}) {
			t.Errorf("Apportion(3, allZero) = %v, want an equal fallback", ap.Expected)
		}
	})
}
