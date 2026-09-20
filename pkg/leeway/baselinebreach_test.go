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
	"testing"
	"time"
)

// scoreAgainst builds the Scores a caller would get for these counts under an
// intent, apportioning exactly the way the source does.
func scoreAgainst(t *testing.T, in *Intent, domains []Domain, actual []int64) Scores {
	t.Helper()
	weights := make([]float64, len(domains))
	for i, d := range domains {
		weights[i] = in.ExplicitShares[d]
	}
	var n int64
	for _, a := range actual {
		n += a
	}
	return Score(domains, actual, Apportion(n, weights, nil), nil, DefaultThresholds())
}

func learnedIntent(t *testing.T, counts []int64) *Intent {
	t.Helper()
	cfg := DefaultBaselineConfig()
	b := NewBaselineSet(threeZones(), baseT0)
	last := mature(t, b, cfg, counts)
	in := b.Intent("topology.kubernetes.io/zone", last, cfg)
	if in == nil {
		t.Fatal("no intent from a matured baseline")
	}
	return in
}

func TestBreachBaseline_FiresOnlyOutsideTheLearnedBand(t *testing.T) {
	// A subject that has always sat 80/10/10. Its learned deviation is ~0, so
	// the band is the floor: k=4 × 0.05 = 0.2.
	in := learnedIntent(t, []int64{80, 10, 10})
	th := DefaultThresholds()

	t.Run("normal is not a breach", func(t *testing.T) {
		s := scoreAgainst(t, in, threeZones(), []int64{78, 11, 11})
		if k, why := s.breach(in, th); k != BreachNone {
			t.Errorf("breach = %v (%s), want none", k, why)
		}
	})

	t.Run("a shift inside the band is not a breach", func(t *testing.T) {
		// zone-a at 0.65 against a learned 0.8 — a deviation of 0.15, inside
		// the 0.2 band.
		s := scoreAgainst(t, in, threeZones(), []int64{65, 20, 15})
		if k, why := s.breach(in, th); k != BreachNone {
			t.Errorf("breach = %v (%s), want none", k, why)
		}
	})

	t.Run("a shift outside the band is", func(t *testing.T) {
		s := scoreAgainst(t, in, threeZones(), []int64{50, 30, 20})
		k, why := s.breach(in, th)
		if k != BreachBaseline {
			t.Fatalf("breach = %v (%s), want %v", k, why, BreachBaseline)
		}
		if want := "domain zone-a is outside its learned band"; why != want {
			t.Errorf("reason = %q, want %q — the worst domain is what a human looks at", why, want)
		}
	})
}

func TestBreachBaseline_ALooseBaselineToleratesWhatATightOneDoesNot(t *testing.T) {
	// This is the whole reason §7.5 learns a dispersion rather than reusing ρ:
	// the same observed placement is normal for one subject and not for
	// another, and one global threshold cannot say so.
	steady := learnedIntent(t, []int64{50, 25, 25})

	cfg := DefaultBaselineConfig().Normalized()
	wobbly := NewBaselineSet(threeZones(), baseT0)
	last := baseT0
	step := cfg.MinAge / time.Duration(cfg.MinSamples)
	for i := uint64(0); i <= cfg.MinSamples; i++ {
		last = last.Add(step)
		// Alternating 50/25/25 and 20/40/40: a similar centre, a much wider
		// spread.
		counts := []int64{50, 25, 25}
		if i%2 == 1 {
			counts = []int64{20, 40, 40}
		}
		wobbly.Observe(threeZones(), counts, last, cfg)
	}
	loose := wobbly.Intent("topology.kubernetes.io/zone", last, cfg)
	if loose == nil {
		t.Fatal("the wobbly baseline never matured")
	}

	observed := []int64{15, 45, 40}
	th := DefaultThresholds()
	tight := scoreAgainst(t, steady, threeZones(), observed)
	if k, _ := tight.breach(steady, th); k != BreachBaseline {
		t.Error("the steady subject did not breach on a placement it has never held")
	}
	wide := scoreAgainst(t, loose, threeZones(), observed)
	if k, why := wide.breach(loose, th); k != BreachNone {
		t.Errorf("the wobbly subject breached on a placement it holds every other sample: %v (%s)", k, why)
	}
}

func TestBreachBaseline_KeepsTheSmallNFloor(t *testing.T) {
	in := learnedIntent(t, []int64{80, 10, 10})
	// n=4: one pod out of zone-a is a deviation far outside the band, and
	// still only one pod anybody could move.
	s := scoreAgainst(t, in, threeZones(), []int64{2, 1, 1})
	k, why := s.breach(in, DefaultThresholds())
	if k != BreachNone || why != "small-n: relocation below floor" {
		t.Errorf("breach = %v (%s), want none with the small-n reason", k, why)
	}
}

func TestBreachBaseline_AnUnknownDomainIsNotEvidence(t *testing.T) {
	in := learnedIntent(t, []int64{80, 10, 10})
	domains := []Domain{"zone-a", "zone-b", "zone-c", "zone-d"}
	// zone-d is not in the baseline at all — a sample racing an invalidation.
	// It must not be read as a large deviation from an implicit zero.
	s := scoreAgainst(t, in, domains, []int64{80, 10, 10, 30})
	if k, why := s.breach(in, DefaultThresholds()); k != BreachNone {
		t.Errorf("breach = %v (%s), want none: an unseen domain means a stale baseline, not drift", k, why)
	}
}

func TestBreachBaseline_RidesTheOrdinaryVerdictPath(t *testing.T) {
	in := learnedIntent(t, []int64{80, 10, 10})
	s := scoreAgainst(t, in, threeZones(), []int64{55, 30, 15})
	v := s.Judge(in, DefaultThresholds())

	if !v.Breached || v.Kind != BreachBaseline {
		t.Fatalf("verdict = %+v, want a baseline breach", v)
	}
	if v.Tier != TierC {
		t.Errorf("tier = %v, want C — a learned bound is nobody's promise", v.Tier)
	}
	if v.Severity != severityWarning {
		// zone-a still holds half the objects, which is §8.1's escalation.
		t.Errorf("severity = %q, want %q (escalated by max domain share)", v.Severity, severityWarning)
	}
	if got := FindingKind(v.Tier, in); got != KindBaselineBreach {
		t.Errorf("kind = %q, want %q", got, KindBaselineBreach)
	}
	if d := v.Route(false); d.Signal {
		t.Error("a Tier C baseline breach signalled without the policy opt-in")
	}
	if d := v.Route(true); !d.Signal || d.Severity != severityWarning {
		t.Errorf("with the opt-in, delivery = %+v, want a warning signal", d)
	}
}

func TestBreachBaseline_ADeclaredIntentIsNeverJudgedAgainstABand(t *testing.T) {
	// Bands are what switch the rule, and only a learned baseline sets them.
	// A declared expectedDistribution with the same shares is still judged by
	// ρ, or an operator's declaration would silently acquire a tolerance they
	// never wrote.
	declared := &Intent{
		TopologyKey:    "topology.kubernetes.io/zone",
		Mode:           ModeSpread,
		Source:         SourcePolicyCRD,
		ExplicitShares: map[Domain]float64{"zone-a": 0.8, "zone-b": 0.1, "zone-c": 0.1},
		Confidence:     ConfidenceDeclared,
	}
	s := scoreAgainst(t, declared, threeZones(), []int64{50, 30, 20})
	k, _ := s.breach(declared, DefaultThresholds())
	if k != BreachDrift {
		t.Errorf("breach = %v, want %v — a declaration has no learned band", k, BreachDrift)
	}
	if got := s.Judge(declared, DefaultThresholds()).Tier; got != TierB {
		t.Errorf("tier = %v, want B", got)
	}
}
