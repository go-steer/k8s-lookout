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
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// threeZones is an inventory of six ready nodes over three zones, half of them
// in a pool the test workloads can select on.
func threeZones(t *testing.T) *Inventory {
	t.Helper()
	inv := NewInventory([]leeway.TopologyKey{zoneKey, poolKey})
	for _, n := range []struct{ name, zone, pool string }{
		{"n1", "zone-a", "general"},
		{"n2", "zone-a", "gpu"},
		{"n3", "zone-b", "general"},
		{"n4", "zone-b", "gpu"},
		{"n5", "zone-c", "general"},
		{"n6", "zone-c", "general"},
	} {
		inv.Upsert(node(n.name, n.zone, withLabel(string(poolKey), n.pool)))
	}
	return inv
}

func domains(e leeway.Eligibility) []string {
	out := make([]string, 0, len(e.Domains))
	for _, d := range e.Domains {
		out = append(out, string(d))
	}
	slices.Sort(out)
	return out
}

// TestResolve_IntentAndEligibilityTogether is the assembly this file exists
// for: FR-4's constraints and FR-5/FR-6's affinity terms reach one map through
// §5.1 precedence, and every axis the inventory tracks comes back with an
// eligible set whether it carries an intent or not.
func TestResolve_IntentAndEligibilityTogether(t *testing.T) {
	pod := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	pod.Spec.Affinity = antiRequired(selfTerm(string(poolKey)))

	res := Resolve(pod, threeZones(t), ResolveConfig{ClusterDefaults: noClusterDefaults})

	if len(res.Intents) != 2 {
		t.Fatalf("Intents = %+v, want one per axis", res.Intents)
	}
	if got := res.Intents[zoneKey]; got == nil || got.Source != leeway.SourceTopologySpreadConstraint {
		t.Errorf("zone intent = %+v, want the spread constraint", got)
	}
	if got := res.Intents[poolKey]; got == nil || got.MaxPerDomain == nil || *got.MaxPerDomain != 1 {
		t.Errorf("pool intent = %+v, want a required anti-affinity's one-per-domain ceiling", got)
	}

	if len(res.Eligible) != 2 {
		t.Fatalf("Eligible = %+v, want an entry for every tracked axis", res.Eligible)
	}
	if got, want := domains(res.Eligible[zoneKey]), []string{"zone-a", "zone-b", "zone-c"}; !slices.Equal(got, want) {
		t.Errorf("eligible zones = %v, want %v", got, want)
	}

	// The eligible set is written back onto the intent, which is what scoring
	// apportions over — an intent carrying the cluster's domains instead of the
	// subject's would score a pinned workload against zones it cannot reach.
	if got := res.Intents[zoneKey].EligibleDomains; got.Len() != 3 {
		t.Errorf("intent.EligibleDomains = %v, want the three eligible zones", got)
	}
}

// TestResolve_NodeSelectorNarrowsEligibility is §7.1's largest class of false
// positive: a workload confined to two of three zones is not drifting because
// the third is empty.
func TestResolve_NodeSelectorNarrowsEligibility(t *testing.T) {
	pod := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	pod.Spec.NodeSelector = map[string]string{string(poolKey): "gpu"}

	res := Resolve(pod, threeZones(t), ResolveConfig{ClusterDefaults: noClusterDefaults})

	if got, want := domains(res.Eligible[zoneKey]), []string{"zone-a", "zone-b"}; !slices.Equal(got, want) {
		t.Errorf("eligible zones = %v, want %v — zone-c has no gpu node", got, want)
	}
	if reason := res.Eligible[zoneKey].Excluded[leeway.Domain("zone-c")]; reason == "" {
		t.Error("zone-c was dropped without a reason; a narrowed eligible set has to say why")
	}
}

// TestResolve_TaintsAreIgnoredUnlessTheIntentSaysOtherwise pins the accessor
// this call has to go through. NodeInclusionPolicy's zero value is the empty
// string and §7.1 reads "not Honor" as "do not apply", so reading Policies off
// the struct would stop honouring nodeSelector on every axis whose intent did
// not set it — widening every pinned workload's eligible set to the cluster.
func TestResolve_TaintsAreIgnoredUnlessTheIntentSaysOtherwise(t *testing.T) {
	inv := threeZones(t)
	inv.Upsert(node("n5", "zone-c", withLabel(string(poolKey), "general"), withTaint("dedicated", "batch", corev1.TaintEffectNoSchedule)))
	inv.Upsert(node("n6", "zone-c", withLabel(string(poolKey), "general"), withTaint("dedicated", "batch", corev1.TaintEffectNoSchedule)))

	pod := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	pod.Spec.NodeSelector = map[string]string{string(poolKey): "general"}

	res := Resolve(pod, inv, ResolveConfig{ClusterDefaults: noClusterDefaults})

	// Taints ignored by default, mirroring kube-scheduler: a workload with no
	// toleration is usually unaware of the taint, so zone-c stays eligible.
	if got, want := domains(res.Eligible[zoneKey]), []string{"zone-a", "zone-b", "zone-c"}; !slices.Equal(got, want) {
		t.Errorf("eligible zones = %v, want %v", got, want)
	}
	// The nodeSelector is honoured in the same call, which is the asymmetry the
	// accessor exists to preserve. n2 and n4 are gpu, so both zones survive on
	// their general node alone — flip the selector to gpu and zone-c goes.
	pod.Spec.NodeSelector = map[string]string{string(poolKey): "gpu"}
	if got, want := domains(Resolve(pod, inv, ResolveConfig{ClusterDefaults: noClusterDefaults}).Eligible[zoneKey]), []string{"zone-a", "zone-b"}; !slices.Equal(got, want) {
		t.Errorf("eligible zones under a gpu selector = %v, want %v", got, want)
	}
}

// TestResolve_NoIntentStillHasAnEligibleSet is the nil-receiver path through
// EligibilityPolicies, and the reason Eligible is computed for every axis.
func TestResolve_NoIntentStillHasAnEligibleSet(t *testing.T) {
	pod := tscPod()
	pod.Spec.NodeSelector = map[string]string{string(poolKey): "gpu"}

	res := Resolve(pod, threeZones(t), ResolveConfig{ClusterDefaults: noClusterDefaults})

	if len(res.Intents) != 0 {
		t.Errorf("Intents = %+v, want none — a pod that declared nothing has no intent", res.Intents)
	}
	if got, want := domains(res.Eligible[zoneKey]), []string{"zone-a", "zone-b"}; !slices.Equal(got, want) {
		t.Errorf("eligible zones = %v, want %v", got, want)
	}
}

// TestResolve_NothingToReadFrom keeps the callers honest: evaluate hands over
// whatever the representative lookup found, which can be nothing.
func TestResolve_NothingToReadFrom(t *testing.T) {
	for name, res := range map[string]Resolution{
		"nil pod":       Resolve(nil, threeZones(t), ResolveConfig{ClusterDefaults: noClusterDefaults}),
		"nil inventory": Resolve(tscPod(zoneSpread(1, corev1.DoNotSchedule)), nil, ResolveConfig{ClusterDefaults: noClusterDefaults}),
	} {
		t.Run(name, func(t *testing.T) {
			if res.Intents == nil || len(res.Intents) != 0 {
				t.Errorf("Intents = %+v, want an empty non-nil map", res.Intents)
			}
			if res.Eligible == nil || len(res.Eligible) != 0 {
				t.Errorf("Eligible = %+v, want an empty non-nil map", res.Eligible)
			}
		})
	}
}

// TestResolve_MinDomainsPadsTheEligibleSet checks the one eligibility option an
// intent contributes beyond its policies. minDomains is only carried by an
// enforced constraint (§7.1), so this is also the path that proves the carry.
func TestResolve_MinDomainsPadsTheEligibleSet(t *testing.T) {
	tsc := zoneSpread(1, corev1.DoNotSchedule)
	tsc.MinDomains = ptr(int32(5))

	res := Resolve(tscPod(tsc), threeZones(t), ResolveConfig{ClusterDefaults: noClusterDefaults})

	el := res.Eligible[zoneKey]
	if el.SyntheticDomains != 2 {
		t.Errorf("SyntheticDomains = %d, want 2 — three real zones against a minDomains of five",
			el.SyntheticDomains)
	}
	if len(el.Domains) != 5 {
		t.Errorf("Domains = %v, want five", el.Domains)
	}
}
