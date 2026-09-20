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

const zoneKeyForTest = "topology.kubernetes.io/zone"

// The thing that has to survive a restart is not the numbers, it is the
// verdict: a set rebuilt from its record must be mature when the original
// was, and must answer the same bands. Everything else is bookkeeping.
func TestBaselineRecord_RoundTripPreservesTheVerdict(t *testing.T) {
	t.Parallel()
	cfg := DefaultBaselineConfig()
	b := NewBaselineSet(threeZones(), baseT0)
	now := mature(t, b, cfg, []int64{6, 3, 1})

	rec := b.Record("prod-east", "prod/Deployment/payments", zoneKeyForTest)
	if rec.Cluster != "prod-east" || rec.SubjectKey != "prod/Deployment/payments" || rec.TopologyKey != zoneKeyForTest {
		t.Errorf("identity = %q/%q/%q", rec.Cluster, rec.SubjectKey, rec.TopologyKey)
	}
	if len(rec.Domains) != 3 {
		t.Fatalf("recorded %d domains, want 3", len(rec.Domains))
	}

	got := rec.Set()
	if got == nil {
		t.Fatal("a mature set did not rebuild")
	}
	if !got.Mature(now, cfg) {
		t.Error("a set that was mature when recorded was immature when loaded")
	}
	for i := range b.Domains {
		if got.Domains[i] != b.Domains[i] {
			t.Errorf("domain %d = %q, want %q", i, got.Domains[i], b.Domains[i])
		}
		if got.Band(i, now, cfg) != b.Band(i, now, cfg) {
			t.Errorf("band %d = %v, want %v", i, got.Band(i, now, cfg), b.Band(i, now, cfg))
		}
	}

	// And the rendered intent is the same one, which is what actually
	// reaches scoring.
	want := b.Intent(TopologyKey(zoneKeyForTest), now, cfg)
	after := got.Intent(TopologyKey(zoneKeyForTest), now, cfg)
	if want == nil || after == nil {
		t.Fatalf("intents = %v / %v, want both non-nil", want, after)
	}
	for d, w := range want.Bands {
		if after.Bands[d] != w {
			t.Errorf("band for %q = %v, want %v", d, after.Bands[d], w)
		}
	}
}

// Frozen, WidenBy and WidenUntil deliberately do not cross. A persisted
// freeze could outlive the finding that justified it and stop a subject
// learning for good; a persisted widening would be applied against the
// wrong clock.
func TestBaselineRecord_RuntimeStateDoesNotCross(t *testing.T) {
	t.Parallel()
	cfg := DefaultBaselineConfig()
	b := NewBaselineSet(threeZones(), baseT0)
	mature(t, b, cfg, []int64{6, 3, 1})
	b.SetFrozen(true)
	b.WidenBy, b.WidenUntil = 1.5, baseT0.Add(48*time.Hour)

	got := b.Record("c", "s", zoneKeyForTest).Set()
	if got == nil {
		t.Fatal("did not rebuild")
	}
	if got.Frozen {
		t.Error("a freeze survived a restart")
	}
	if got.WidenBy != 0 || !got.WidenUntil.IsZero() {
		t.Errorf("widening survived a restart: %v until %v", got.WidenBy, got.WidenUntil)
	}
}

// A nil or empty set records cleanly and reads back as "nothing learned",
// which is the same answer an absent row gives.
func TestBaselineRecord_NothingLearned(t *testing.T) {
	t.Parallel()
	var nilSet *BaselineSet
	rec := nilSet.Record("c", "s", zoneKeyForTest)
	if len(rec.Domains) != 0 || rec.Samples != 0 {
		t.Errorf("nil set recorded %+v", rec)
	}
	if rec.Set() != nil {
		t.Error("a record of nothing rebuilt into an estimator")
	}
	if NewBaselineSet(threeZones(), baseT0).Record("c", "s", zoneKeyForTest).Set() != nil {
		t.Error("an unobserved set rebuilt into an estimator")
	}
}

// Every refusal branch, one row each. The rule is §9.3's: prefer the reading
// that loses information over the one that acts on a lie — a baseline that
// relearns costs six hours, one built from a corrupt band fires on a cluster
// that is fine.
func TestBaselineRecord_DamagedRecordsAreRefused(t *testing.T) {
	t.Parallel()
	good := func() BaselineRecord {
		return BaselineRecord{
			Domains: []BaselineDomain{
				{Domain: "zone-a", Share: 0.6, Deviation: 0.05},
				{Domain: "zone-b", Share: 0.4, Deviation: 0.05},
			},
			DevWeight: 0.5,
			Samples:   300,
			FirstSeen: baseT0,
			UpdatedAt: baseT0.Add(12 * time.Hour),
		}
	}
	if good().Set() == nil {
		t.Fatal("the control record was refused")
	}

	for name, damage := range map[string]func(*BaselineRecord){
		"no samples":         func(r *BaselineRecord) { r.Samples = 0 },
		"no domains":         func(r *BaselineRecord) { r.Domains = nil },
		"unnamed domain":     func(r *BaselineRecord) { r.Domains[0].Domain = "" },
		"negative share":     func(r *BaselineRecord) { r.Domains[0].Share = -0.1 },
		"share over one":     func(r *BaselineRecord) { r.Domains[0].Share = 1.5 },
		"negative deviation": func(r *BaselineRecord) { r.Domains[0].Deviation = -0.01 },
		"duplicate domain":   func(r *BaselineRecord) { r.Domains[1].Domain = "zone-a" },
		"negative weight":    func(r *BaselineRecord) { r.DevWeight = -0.5 },
		"weight over one":    func(r *BaselineRecord) { r.DevWeight = 1.5 },
		"no updated at":      func(r *BaselineRecord) { r.UpdatedAt = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := good()
			damage(&rec)
			if got := rec.Set(); got != nil {
				t.Errorf("a record with %s rebuilt into %+v", name, got)
			}
		})
	}
}

// Two repairs rather than refusals, because both lose information in the
// safe direction.
func TestBaselineRecord_RepairsRatherThanRefuses(t *testing.T) {
	t.Parallel()
	cfg := DefaultBaselineConfig()

	// Scrambled domain order: the order is the invalidation fingerprint, so
	// loading it as written would reset a perfectly good baseline on the
	// next sample. Re-canonicalising keeps the pairs with their domains.
	scrambled := BaselineRecord{
		Domains: []BaselineDomain{
			{Domain: "zone-c", Share: 0.1, Deviation: 0.01},
			{Domain: "zone-a", Share: 0.6, Deviation: 0.06},
			{Domain: "zone-b", Share: 0.3, Deviation: 0.03},
		},
		DevWeight: 0.5,
		Samples:   300,
		FirstSeen: baseT0,
		UpdatedAt: baseT0.Add(12 * time.Hour),
	}
	got := scrambled.Set()
	if got == nil {
		t.Fatal("a scrambled record was refused")
	}
	for i, want := range threeZones() {
		if got.Domains[i] != want {
			t.Fatalf("domain %d = %q, want %q", i, got.Domains[i], want)
		}
	}
	if got.Baselines[0].Share != 0.6 || got.Baselines[2].Share != 0.1 {
		t.Errorf("shares did not follow their domains: %+v", got.Baselines)
	}
	if got.Observe(threeZones(), []int64{6, 3, 1}, scrambled.UpdatedAt.Add(time.Minute), cfg) != ObserveApplied {
		t.Error("a re-canonicalised record was reset by its next sample")
	}

	// A missing FirstSeen would make the set mature the instant it loaded.
	// Treating it as written now costs six hours and cannot fire on a
	// cluster that is fine.
	noFirst := scrambled
	noFirst.FirstSeen = time.Time{}
	loaded := noFirst.Set()
	if loaded == nil {
		t.Fatal("a record with no FirstSeen was refused")
	}
	if !loaded.FirstSeen.Equal(noFirst.UpdatedAt) {
		t.Errorf("FirstSeen = %v, want it repaired to UpdatedAt %v", loaded.FirstSeen, noFirst.UpdatedAt)
	}
	if loaded.Mature(noFirst.UpdatedAt, cfg) {
		t.Error("a record with no FirstSeen was mature the instant it loaded")
	}

	// A FirstSeen in the future is left alone: the only effect is to
	// postpone maturity, and postponing a Tier C finding is never the error
	// worth repairing.
	future := scrambled
	future.FirstSeen = scrambled.UpdatedAt.Add(72 * time.Hour)
	ahead := future.Set()
	if ahead == nil || !ahead.FirstSeen.Equal(future.FirstSeen) {
		t.Errorf("a future FirstSeen was rewritten: %v", ahead)
	}
	if ahead.Mature(future.UpdatedAt, cfg) {
		t.Error("a set whose history starts in the future was mature")
	}
}
