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

package topologydrift

import (
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// unevenZones is 64 cores in zone-a and 8 in each of its neighbours: a 8×
// spread, far over the 1.25× trigger. The node *counts* are equal on purpose,
// so a weighting that came out capacity-shaped cannot have come from
// NodeCount.
func unevenZones(t *testing.T) *Inventory {
	t.Helper()
	return zonedInventory(t,
		zoneSpec{zone: "zone-a", nodes: 2, opts: []nodeOpt{withAllocatable("32", "128Gi")}},
		zoneSpec{zone: "zone-b", nodes: 2, opts: []nodeOpt{withAllocatable("4", "16Gi")}},
		zoneSpec{zone: "zone-c", nodes: 2, opts: []nodeOpt{withAllocatable("4", "16Gi")}},
	)
}

// distOver counts a placement into a Distribution the way ScoreAxis reads one.
func distOver(domains []leeway.Domain, counts ...int64) *leeway.Distribution {
	dist := leeway.NewDistribution()
	for i, d := range domains {
		for range counts[i] {
			dist.Add(d, leeway.StateRunning, false)
		}
	}
	return dist
}

// TestCapacityWeighting_ResolveWritesBackTheEffectiveWeightingAndSaysWhy:
// §7.2's trigger is resolved once, during Resolve, and the answer is written
// onto the intent. Leaving Auto there and resolving it again at apportionment
// time would put the string "Auto" on the intent_info label and in the §8.5
// payload next to an expectation apportioned by CPU.
func TestCapacityWeighting_ResolveWritesBackTheEffectiveWeightingAndSaysWhy(t *testing.T) {
	// A *preferred* anti-affinity: the one inference source that asks for a
	// spread without also stating a pod-count contract.
	pod := affPod(antiPreferred(100, selfTerm(corev1.LabelTopologyZone)))
	res := Resolve(pod, unevenZones(t), ResolveConfig{ClusterDefaults: noClusterDefaults})

	in := res.Intents[zoneKey]
	if in == nil {
		t.Fatal("no zone intent — the fixture is not testing what it claims")
	}
	if in.Weighting != leeway.WeightAllocatableCPU {
		t.Fatalf("Weighting = %v, want AllocatableCPU", in.Weighting)
	}

	var said string
	for _, ev := range in.Evidence {
		if strings.Contains(ev.Detail, "allocatable CPU") {
			said = ev.Detail
		}
	}
	if said == "" {
		t.Fatalf("no evidence explains the weighting: %+v", in.Evidence)
	}
	// The ratio quoted has to be the one the decision was made on, which is
	// why capacityRatio reads the same slice the trigger did rather than
	// recomputing from the inventory.
	if !strings.Contains(said, "8.00×") {
		t.Errorf("evidence quotes the wrong ratio (want 8.00×): %q", said)
	}
	if !strings.Contains(said, "1.25×") {
		t.Errorf("evidence does not name the trigger it crossed: %q", said)
	}

	// And the expectation that follows is the capacities, not thirds.
	el := res.Eligible[zoneKey]
	ap := leeway.Apportion(10, el.WeightsFor(in), nil)
	if want := []int64{8, 1, 1}; !slices.Equal(ap.Expected, want) {
		t.Errorf("expected = %v, want %v", ap.Expected, want)
	}
}

// TestCapacityWeighting_SaysNothingOnAHomogeneousCluster: the trigger is silent
// where it does not fire. An evidence line on every even cluster in the world
// would say only that nothing happened, and Equal is the value the label
// already carried.
func TestCapacityWeighting_SaysNothingOnAHomogeneousCluster(t *testing.T) {
	pod := affPod(antiPreferred(100, selfTerm(corev1.LabelTopologyZone)))
	res := Resolve(pod, threeZones(t), ResolveConfig{ClusterDefaults: noClusterDefaults})

	in := res.Intents[zoneKey]
	if in == nil {
		t.Fatal("no zone intent")
	}
	if in.Weighting != leeway.WeightEqual {
		t.Errorf("Weighting = %v, want Equal on a cluster of identical nodes", in.Weighting)
	}
	for _, ev := range in.Evidence {
		if strings.Contains(ev.Detail, "allocatable CPU") {
			t.Errorf("evidence explains a weighting that did not change: %q", ev.Detail)
		}
	}
}

// TestCapacityWeighting_APodCountContractKeepsTheEvenExpectation is the rule
// that was found by breaking it: two false-positive fixtures went quiet for the
// wrong reason when the first draft let a declared maxSkew take the trigger.
//
// §7.3 reads MinAchievableSkew off the expectation and measures ExcessSkew from
// max(S*, maxSkew). Apportioning by capacity therefore *raises the floor the
// contract is checked against* — an expectation of [8 1 1] has a floor of 7, so
// a `maxSkew: 1` that the placement violates by six comes out at zero excess.
// kube-scheduler bounds pods, not millicores; a workload whose zones are
// unequal is precisely the one whose DoNotSchedule constraint is going to be
// violated, and quietly re-deriving the operator's contract from the shape of
// their cluster is the one thing a contract check must not do.
func TestCapacityWeighting_APodCountContractKeepsTheEvenExpectation(t *testing.T) {
	inv := unevenZones(t)
	pod := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	res := Resolve(pod, inv, ResolveConfig{ClusterDefaults: noClusterDefaults})

	in, el := res.Intents[zoneKey], res.Eligible[zoneKey]
	if in == nil || in.MaxSkew == nil {
		t.Fatalf("intent = %+v, want one carrying a maxSkew", in)
	}
	if in.Weighting != leeway.WeightEqual {
		t.Fatalf("Weighting = %v, want Equal — a pod-count contract does not take the trigger", in.Weighting)
	}

	// [8 1 1] on zones that are 8:1:1 by capacity, which is exactly the
	// placement capacity weighting would call perfect.
	dist := distOver(el.Domains, 8, 1, 1)
	ev := ScoreAxis(zoneKey, in, el, dist, leeway.DefaultThresholds(), leeway.Suppression{})
	if want := []int64{4, 3, 3}; !slices.Equal(ev.Scores.Expected, want) {
		t.Fatalf("expected = %v, want %v", ev.Scores.Expected, want)
	}
	if ev.Scores.ExcessSkew != 6 {
		t.Errorf("E = %d, want 6 — a maxSkew of 1 is violated by six pods here: %+v", ev.Scores.ExcessSkew, ev.Scores)
	}
	if !ev.Verdict.Breached {
		t.Errorf("a violated DoNotSchedule contract did not breach: %q", ev.Verdict.Reason)
	}

	// The counterfactual, which is what makes the carve-out load-bearing: the
	// same placement with the trigger allowed through. The contract becomes
	// unviolable — not satisfied, unviolable — and the finding disappears.
	weighted := *in
	weighted.Weighting = leeway.WeightAllocatableCPU
	got := ScoreAxis(zoneKey, &weighted, el, dist, leeway.DefaultThresholds(), leeway.Suppression{})
	if got.Scores.ExcessSkew != 0 || got.Verdict.Breached {
		t.Errorf("capacity weighting no longer hides this violation (E = %d, breached = %v); "+
			"the carve-out may no longer be what is reporting it",
			got.Scores.ExcessSkew, got.Verdict.Breached)
	}
}

// TestCapacityWeighting_TheTriggerIsConfigurableThroughResolve: the
// --topology-capacity-ratio knob reaches the decision, not just the free
// function underneath it.
func TestCapacityWeighting_TheTriggerIsConfigurableThroughResolve(t *testing.T) {
	pod := affPod(antiPreferred(100, selfTerm(corev1.LabelTopologyZone)))

	// Above the fixture's 8× spread: the operator has said this cluster's
	// unevenness is not worth apportioning around.
	lenient := Resolve(pod, unevenZones(t), ResolveConfig{
		ClusterDefaults:      noClusterDefaults,
		CapacityRatioTrigger: 16,
	})
	if got := lenient.Intents[zoneKey].Weighting; got != leeway.WeightEqual {
		t.Errorf("Weighting = %v, want Equal — an 8× spread should not cross a 16× trigger", got)
	}

	// And a trigger under any real inequality fires on a cluster the default
	// would leave alone.
	strict := Resolve(pod, threeZones(t), ResolveConfig{
		ClusterDefaults:      noClusterDefaults,
		CapacityRatioTrigger: 0.5,
	})
	if got := strict.Intents[zoneKey].Weighting; got != leeway.WeightAllocatableCPU {
		t.Errorf("Weighting = %v, want AllocatableCPU — a trigger under 1 fires on any inequality", got)
	}
}
