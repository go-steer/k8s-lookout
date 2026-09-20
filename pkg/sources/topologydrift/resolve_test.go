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

// antiPod is a pod whose only placement statement is a required zone
// anti-affinity.
func antiPod() *corev1.Pod {
	p := tscPod()
	p.Spec.Affinity = antiRequired(selfTerm(string(zoneKey)))
	return p
}

// learnedZones is a mature baseline rendered as §7.5's intent, built by hand so
// that the precedence tests below do not also depend on the estimator's
// maturity arithmetic — that is BaselineSet's own test's job.
func learnedZones() map[leeway.TopologyKey]*leeway.Intent {
	return map[leeway.TopologyKey]*leeway.Intent{
		zoneKey: {
			TopologyKey:    zoneKey,
			Mode:           leeway.ModeSpread,
			Source:         leeway.SourceLearnedBaseline,
			ExplicitShares: map[leeway.Domain]float64{"zone-a": 0.5, "zone-b": 0.5},
			Bands:          map[leeway.Domain]float64{"zone-a": 0.2, "zone-b": 0.2},
			Confidence:     leeway.ConfidenceLearned,
		},
	}
}

// TestResolve_ALearnedBaselineAppliesWhereNothingSpoke is the whole point of
// §7.5: the majority of an estate declares nothing, and without a baseline the
// only expectation available for it is an even split over its eligible domains.
func TestResolve_ALearnedBaselineAppliesWhereNothingSpoke(t *testing.T) {
	res := Resolve(tscPod(), threeZones(t), ResolveConfig{
		ClusterDefaults: noClusterDefaults,
		Baselines:       learnedZones(),
	})

	got := res.Intents[zoneKey]
	if got == nil || got.Source != leeway.SourceLearnedBaseline {
		t.Fatalf("zone intent = %+v, want the learned baseline", got)
	}
	if len(got.Bands) != 2 {
		t.Errorf("Bands = %v, want the learned band per domain — Bands is what switches breach from the drift rule to the per-domain one", got.Bands)
	}
	// The eligible set is still the pod's, not the baseline's domain list: a
	// baseline learned while a third zone existed must not keep scoring against
	// a zone the subject can no longer reach.
	if got, want := domains(res.Eligible[zoneKey]), []string{"zone-a", "zone-b", "zone-c"}; !slices.Equal(got, want) {
		t.Errorf("eligible zones = %v, want %v", got, want)
	}
	// And the axis nobody learned anything about is untouched, rather than
	// picking up the zone baseline by accident.
	if got := res.Intents[poolKey]; got != nil {
		t.Errorf("pool intent = %+v, want none — nothing was learned on that axis", got)
	}
}

// TestResolve_ALearnedBaselineLosesToEveryDeclaredSource pins §5.1's ordering
// at the bottom of the list, which is the property that makes learning safe to
// leave on: it can only ever fill a gap, never overrule an answer.
func TestResolve_ALearnedBaselineLosesToEveryDeclaredSource(t *testing.T) {
	cases := map[string]struct {
		pod  *corev1.Pod
		cfg  ResolveConfig
		want leeway.IntentSource
	}{
		"a spread constraint": {
			pod:  tscPod(zoneSpread(1, corev1.DoNotSchedule)),
			cfg:  ResolveConfig{ClusterDefaults: noClusterDefaults},
			want: leeway.SourceTopologySpreadConstraint,
		},
		"an anti-affinity term": {
			pod:  antiPod(),
			cfg:  ResolveConfig{ClusterDefaults: noClusterDefaults},
			want: leeway.SourcePodAntiAffinityRequired,
		},
		"a declared cluster default": {
			pod:  tscPod(),
			cfg:  ResolveConfig{ClusterDefaults: &[]corev1.TopologySpreadConstraint{zoneSpread(2, corev1.ScheduleAnyway)}},
			want: leeway.SourceClusterDefaultDeclared,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tc.cfg.Baselines = learnedZones()
			res := Resolve(tc.pod, threeZones(t), tc.cfg)
			got := res.Intents[zoneKey]
			if got == nil || got.Source != tc.want {
				t.Fatalf("zone intent source = %+v, want %v — the baseline outranked %s", got, tc.want, name)
			}
			if len(got.Bands) != 0 {
				t.Errorf("Bands = %v on a declared intent; only a learned baseline sets them, and their presence changes the breach rule", got.Bands)
			}
		})
	}
}

// TestResolve_ALearnedBaselineBeatsAnAssumedClusterDefault is the one pair in
// §5.1 the baseline wins, and the reason Phase 5 moved the assumed default to
// the bottom of the list. The assumed defaults apply to exactly the pods a
// baseline is learned for — those declaring no topologySpreadConstraints — so
// while the guess ranked higher, no cluster that had left
// --topology-cluster-defaults unset ever scored anything against §7.5 at all.
func TestResolve_ALearnedBaselineBeatsAnAssumedClusterDefault(t *testing.T) {
	// ClusterDefaults nil is the assumed state, and the shipped default.
	res := Resolve(tscPod(), threeZones(t), ResolveConfig{Baselines: learnedZones()})

	got := res.Intents[zoneKey]
	if got == nil || got.Source != leeway.SourceLearnedBaseline {
		t.Fatalf("zone intent = %+v, want the learned baseline to beat our own guess", got)
	}
	// The guess is kept as evidence rather than discarded, so a reader of the
	// finding can still see what it would have been scored against.
	var sawDefault bool
	for _, e := range got.Evidence {
		sawDefault = sawDefault || (e.Source == leeway.SourceClusterDefaultAssumed && e.Ignored)
	}
	if !sawDefault {
		t.Errorf("evidence = %+v, want the superseded assumed default retained and flagged ignored", got.Evidence)
	}
}

// TestResolve_EveryLearnedAxisLandsAndAHoleIsSkipped covers the two remaining
// shapes of the map: more than one axis at once, and a nil entry. IntentsFor
// omits an immature axis rather than storing nil for it, but the field is
// exported and a caller assembling the map by hand can leave a hole, which must
// not be dereferenced as an intent claiming the workload asked for nothing.
func TestResolve_EveryLearnedAxisLandsAndAHoleIsSkipped(t *testing.T) {
	baselines := learnedZones()
	baselines[poolKey] = &leeway.Intent{
		TopologyKey:    poolKey,
		Mode:           leeway.ModeSpread,
		Source:         leeway.SourceLearnedBaseline,
		ExplicitShares: map[leeway.Domain]float64{"general": 1},
		Bands:          map[leeway.Domain]float64{"general": 0.1},
	}
	baselines[leeway.TopologyKey("unwatched")] = nil

	res := Resolve(tscPod(), threeZones(t), ResolveConfig{
		ClusterDefaults: noClusterDefaults,
		Baselines:       baselines,
	})

	if len(res.Intents) != 2 {
		t.Fatalf("Intents = %+v, want one per learned axis and nothing for the hole", res.Intents)
	}
	for _, key := range []leeway.TopologyKey{zoneKey, poolKey} {
		if got := res.Intents[key]; got == nil || got.Source != leeway.SourceLearnedBaseline {
			t.Errorf("%s intent = %+v, want the learned baseline", key, got)
		}
	}
}
