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

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// threeEligibleZones is an eligible set of three equal zones, the shape most of these
// tests want and none of them are about.
func threeEligibleZones() leeway.Eligibility {
	return leeway.Eligibility{
		Domains:   []leeway.Domain{"zone-a", "zone-b", "zone-c"},
		NodeCount: []int64{4, 4, 4},
		Capacity:  []float64{4, 4, 4},
	}
}

// running builds a distribution holding n Running objects in each named
// domain, index-aligned with the eligible set.
func running(e leeway.Eligibility, counts ...int64) *leeway.Distribution {
	d := leeway.NewDistribution()
	for i, n := range counts {
		for range n {
			d.Add(e.Domains[i], leeway.StateRunning, false)
		}
	}
	return d
}

func TestScoreAxis_ScoresTheDistributionAgainstTheApportionment(t *testing.T) {
	e := threeEligibleZones()
	got := ScoreAxis(zoneKey, nil, e, running(e, 8, 3, 1), leeway.DefaultThresholds(), leeway.Suppression{})

	if !slices.Equal(got.Scores.Actual, []int64{8, 3, 1}) {
		t.Errorf("actual = %v, want [8 3 1]", got.Scores.Actual)
	}
	if !slices.Equal(got.Scores.Expected, []int64{4, 4, 4}) {
		t.Errorf("expected = %v, want [4 4 4] — twelve objects over three equal zones", got.Scores.Expected)
	}
	if got.Scores.Relocation != 4 {
		t.Errorf("R = %d, want 4", got.Scores.Relocation)
	}
	if !got.Verdict.Breached {
		t.Errorf("a 8/3/1 split over three equal zones did not breach: %+v", got.Verdict)
	}
	// Tier C: nobody declared anything, so the expectation is ours.
	if got.Verdict.Tier != leeway.TierC {
		t.Errorf("tier = %s, want C for a subject with no intent", got.Verdict.Tier)
	}
}

func TestScoreAxis_OnlyRunningObjectsAreScored(t *testing.T) {
	// §7.6's reason: a Terminating pod is where the subject *was* and a
	// Pending one is where the scheduler has so far declined to put it.
	// Counting either would report a mid-rollout cluster as drifting away from
	// an expectation computed over a replica count that includes both halves
	// of the move.
	e := threeEligibleZones()
	d := running(e, 4, 4, 4)
	for range 6 {
		d.Add("zone-a", leeway.StateTerminating, false)
		d.Add("zone-a", leeway.StatePending, false)
	}

	got := ScoreAxis(zoneKey, nil, e, d, leeway.DefaultThresholds(), leeway.Suppression{})
	if got.Scores.Total != 12 {
		t.Fatalf("n = %d, want 12: twelve Running objects and twelve that are not", got.Scores.Total)
	}
	if got.Verdict.Breached {
		t.Errorf("an evenly placed subject breached on its Terminating pods: %+v", got.Scores)
	}
}

func TestScoreAxis_ASubjectWithNoObjectsIsGatedNotBreached(t *testing.T) {
	e := threeEligibleZones()
	got := ScoreAxis(zoneKey, nil, e, nil, leeway.DefaultThresholds(), leeway.Suppression{})

	if got.Scores.Evaluable {
		t.Error("a subject with no counted objects was evaluable")
	}
	if got.Scores.Gate != leeway.GateNoObjects {
		t.Errorf("gate = %s, want %s", got.Scores.Gate, leeway.GateNoObjects)
	}
	if got.Verdict.Breached {
		t.Error("a gated subject breached")
	}
	// A nil distribution is not a missing axis: the eligible domains are still
	// reported, holding nothing, which is the statement a domain outage makes.
	if !slices.Equal(got.Scores.Domains, e.Domains) {
		t.Errorf("domains = %v, want the eligible set %v", got.Scores.Domains, e.Domains)
	}
}

func TestScoreAxis_ADeclaredBoundMakesItATierAFinding(t *testing.T) {
	e := threeEligibleZones()
	skew := int32(1)
	intent := &leeway.Intent{
		TopologyKey:       zoneKey,
		Mode:              leeway.ModeSpread,
		Source:            leeway.SourceTopologySpreadConstraint,
		Confidence:        leeway.ConfidenceDeclared,
		MaxSkew:           &skew,
		WhenUnsatisfiable: "DoNotSchedule",
	}

	got := ScoreAxis(zoneKey, intent, e, running(e, 8, 3, 1), leeway.DefaultThresholds(), leeway.Suppression{})
	if got.Verdict.Tier != leeway.TierA {
		t.Errorf("tier = %s, want A: maxSkew 1 and DoNotSchedule is a contract", got.Verdict.Tier)
	}
	if got.Scores.ExcessSkew == 0 {
		t.Error("E = 0 with an observed skew of 7 against a declared bound of 1")
	}
}

func TestScoreSubject_ScoresEveryEligibleAxisAndNotOnlyTheDeclaredOnes(t *testing.T) {
	// The loop runs over Eligible, not Intents. Driving it off the intents
	// would silently stop measuring the majority of an estate, since most
	// workloads declare nothing at all.
	e := threeEligibleZones()
	res := Resolution{
		Intents:  map[leeway.TopologyKey]*leeway.Intent{zoneKey: {TopologyKey: zoneKey, Mode: leeway.ModeSpread}},
		Eligible: map[leeway.TopologyKey]leeway.Eligibility{zoneKey: e, poolKey: e},
	}
	dists := map[leeway.TopologyKey]*leeway.Distribution{
		zoneKey: running(e, 8, 3, 1),
		poolKey: running(e, 4, 4, 4),
	}

	got := ScoreSubject(res, dists, leeway.DefaultThresholds(), nil)
	if len(got) != 2 {
		t.Fatalf("scored %d axes, want 2 — the undeclared one is still measured", len(got))
	}
	byKey := map[leeway.TopologyKey]Evaluation{}
	for _, ev := range got {
		byKey[ev.Key] = ev
	}
	if !byKey[zoneKey].Verdict.Breached {
		t.Error("the zone axis did not breach on 8/3/1")
	}
	if byKey[poolKey].Verdict.Breached {
		t.Error("the pool axis breached on an even split")
	}
	if byKey[poolKey].Intent != nil {
		t.Error("an axis with no resolved intent was given one")
	}
}

func TestScoreSubject_IsInCanonicalKeyOrder(t *testing.T) {
	// Two evaluations of the same subject must produce the same slice order,
	// or a caller indexing the result reads a different axis each pass and a
	// finding flaps with nothing in the cluster having changed.
	e := threeEligibleZones()
	res := Resolution{Eligible: map[leeway.TopologyKey]leeway.Eligibility{}}
	for _, k := range []leeway.TopologyKey{"z", "a", "m", "b"} {
		res.Eligible[k] = e
	}

	for range 8 {
		var keys []leeway.TopologyKey
		for _, ev := range ScoreSubject(res, nil, leeway.DefaultThresholds(), nil) {
			keys = append(keys, ev.Key)
		}
		if want := []leeway.TopologyKey{"a", "b", "m", "z"}; !slices.Equal(keys, want) {
			t.Fatalf("keys = %v, want %v", keys, want)
		}
	}
}

func TestScoreSubject_AnAxisWithNoDistributionIsStillScored(t *testing.T) {
	e := threeEligibleZones()
	res := Resolution{Eligible: map[leeway.TopologyKey]leeway.Eligibility{zoneKey: e}}

	got := ScoreSubject(res, map[leeway.TopologyKey]*leeway.Distribution{}, leeway.DefaultThresholds(), nil)
	if len(got) != 1 {
		t.Fatalf("scored %d axes, want 1: a missing distribution is an empty one, not a skipped axis", len(got))
	}
	if !slices.Equal(got[0].Scores.Domains, e.Domains) {
		t.Errorf("domains = %v, want %v", got[0].Scores.Domains, e.Domains)
	}
}

func TestCapsFor_AnIntentWithNoCeilingCapsNothing(t *testing.T) {
	e := threeEligibleZones()
	if got := capsFor(nil, e); got != nil {
		t.Errorf("capsFor(nil) = %v, want nil", got)
	}
	// Nil rather than a slice of Uncapped: Apportion reads nil as "no domain
	// is capped", and this is the overwhelmingly common case — allocating a
	// slice per evaluation to say nothing is the one cost worth avoiding on
	// this path.
	if got := capsFor(&leeway.Intent{TopologyKey: zoneKey}, e); got != nil {
		t.Errorf("capsFor(no ceiling) = %v, want nil", got)
	}
}

func TestCapsFor_AUniformCeilingReachesEveryDomain(t *testing.T) {
	e := threeEligibleZones()
	ceiling := int64(1)
	got := capsFor(&leeway.Intent{MaxPerDomain: &ceiling}, e)
	if want := []int64{1, 1, 1}; !slices.Equal(got, want) {
		t.Errorf("caps = %v, want %v", got, want)
	}
}

func TestCapsFor_ANamedDomainsCeilingWinsAndTheRestAreUncapped(t *testing.T) {
	e := threeEligibleZones()

	// Named domains only: everything else has no ceiling at all, which is
	// Uncapped and emphatically not zero — a zero would apportion nothing to
	// the domains the declaration did not mention and then report every object
	// in them as needing to move.
	got := capsFor(&leeway.Intent{DomainCaps: map[leeway.Domain]int64{"zone-b": 2}}, e)
	if want := []int64{leeway.Uncapped, 2, leeway.Uncapped}; !slices.Equal(got, want) {
		t.Errorf("caps = %v, want %v", got, want)
	}

	// With a uniform ceiling as well, the named domain still wins: naming it
	// is the more specific statement and the only way to say one domain is
	// different.
	ceiling := int64(5)
	got = capsFor(&leeway.Intent{MaxPerDomain: &ceiling, DomainCaps: map[leeway.Domain]int64{"zone-b": 2}}, e)
	if want := []int64{5, 2, 5}; !slices.Equal(got, want) {
		t.Errorf("caps = %v, want %v", got, want)
	}
}

func TestCapsFor_IsAlignedWithTheEligibleSetAndNotTheDeclaration(t *testing.T) {
	// A ceiling named on a domain this subject cannot reach contributes
	// nothing, and one the declaration skipped is still present as Uncapped.
	// The slice Apportion takes is index-aligned with the eligible domains, so
	// a caps slice built from the declaration's own key order would cap the
	// wrong zones.
	e := threeEligibleZones()
	got := capsFor(&leeway.Intent{DomainCaps: map[leeway.Domain]int64{
		"zone-c":       3,
		"zone-nowhere": 1,
	}}, e)
	if want := []int64{leeway.Uncapped, leeway.Uncapped, 3}; !slices.Equal(got, want) {
		t.Errorf("caps = %v, want %v", got, want)
	}
}
