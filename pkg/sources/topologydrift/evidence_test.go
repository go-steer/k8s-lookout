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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

func TestInventory_DrainTimesAnswersEveryDomainInOnePass(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)
	aDrain := t0.Add(-10 * time.Minute)
	bDrain := t0.Add(-2 * time.Minute)
	inv.Upsert(node("n-a1", "us-central1-a", cordonedAtTaint(aDrain)))
	inv.Upsert(node("n-a2", "us-central1-a", cordonedAtTaint(t0.Add(-time.Hour))))
	inv.Upsert(node("n-b1", "us-central1-b", cordonedAtTaint(bDrain)))
	inv.Upsert(node("n-c1", "us-central1-c"))

	domains := []leeway.Domain{"us-central1-a", "us-central1-b", "us-central1-c"}
	got := inv.DrainTimes(zoneKey, domains)

	// Per domain, the most recent drain — and c, which was never drained, is
	// absent rather than present at the zero time, so a caller reading the map
	// cannot mistake "nothing happened" for "drained at the epoch".
	if len(got) != 2 {
		t.Fatalf("DrainTimes = %v, want only the two drained zones", got)
	}
	if !got["us-central1-a"].Equal(aDrain) {
		t.Errorf("zone a = %v, want the most recent of its two cordons %v", got["us-central1-a"], aDrain)
	}
	if !got["us-central1-b"].Equal(bDrain) {
		t.Errorf("zone b = %v, want %v", got["us-central1-b"], bDrain)
	}

	// It must agree with LastDrainIn, which is the single-domain form of the
	// same question.
	for _, d := range domains {
		if want := inv.LastDrainIn(zoneKey, []leeway.Domain{d}); !got[d].Equal(want) {
			t.Errorf("DrainTimes[%s] = %v, LastDrainIn = %v — the two disagree", d, got[d], want)
		}
	}
}

func TestInventory_DrainTimesRefusesAnUnknownAxisOrAnEmptySet(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)
	inv.Upsert(node("n-1", "us-central1-a", cordonedAtTaint(t0)))

	if got := inv.DrainTimes("nope/key", []leeway.Domain{"us-central1-a"}); got != nil {
		t.Errorf("DrainTimes on an untracked axis = %v, want nil", got)
	}
	if got := inv.DrainTimes(zoneKey, nil); got != nil {
		t.Errorf("DrainTimes over no domains = %v, want nil", got)
	}
}

func TestPeakAndFall(t *testing.T) {
	cutoff := t0.Add(-time.Hour)
	at := func(d time.Duration, ready int64) leeway.ReadyCount {
		return leeway.ReadyCount{At: t0.Add(d), Ready: ready}
	}

	for _, tc := range []struct {
		name     string
		hist     []leeway.ReadyCount
		ready    int64
		wantPeak int64
		wantLost time.Duration // relative to t0; 0 means "no fall"
	}{
		{
			name:     "no history at all reports the live count and no fall",
			ready:    3,
			wantPeak: 3,
		},
		{
			name:     "the peak floors at the live count",
			hist:     []leeway.ReadyCount{at(-30*time.Minute, 2)},
			ready:    5,
			wantPeak: 5,
		},
		{
			name:     "a steady series has a peak and no fall",
			hist:     []leeway.ReadyCount{at(-30*time.Minute, 6), at(-20*time.Minute, 6), at(-10*time.Minute, 6)},
			ready:    6,
			wantPeak: 6,
		},
		{
			name:     "the fall is the last decrease, the peak is the whole window",
			hist:     []leeway.ReadyCount{at(-50*time.Minute, 12), at(-40*time.Minute, 3), at(-30*time.Minute, 3), at(-time.Minute, 1)},
			ready:    1,
			wantPeak: 12,
			wantLost: -time.Minute,
		},
		{
			name:     "a recovery after a fall still dates the fall",
			hist:     []leeway.ReadyCount{at(-50*time.Minute, 9), at(-40*time.Minute, 4), at(-5*time.Minute, 9)},
			ready:    9,
			wantPeak: 9,
			wantLost: -40 * time.Minute,
		},
		{
			name: "samples outside the window are skipped, not pruned",
			// The 20-node sample is older than the cutoff: it must not become
			// the peak, and the step down out of it must not become a fall.
			hist:     []leeway.ReadyCount{at(-2*time.Hour, 20), at(-30*time.Minute, 4), at(-10*time.Minute, 4)},
			ready:    4,
			wantPeak: 4,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peak, lostAt := peakAndFall(tc.hist, cutoff, tc.ready)
			if peak != tc.wantPeak {
				t.Errorf("peak = %d, want %d", peak, tc.wantPeak)
			}
			switch {
			case tc.wantLost == 0 && !lostAt.IsZero():
				t.Errorf("lostAt = %v, want no fall", lostAt)
			case tc.wantLost != 0 && !lostAt.Equal(t0.Add(tc.wantLost)):
				t.Errorf("lostAt = %v, want %v", lostAt, t0.Add(tc.wantLost))
			}
		})
	}
}

func TestSource_EvidenceFor(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	now := t0
	s.inv.now = func() time.Time { return now }

	drainedAt := t0.Add(-4 * time.Minute)
	s.inv.Upsert(node("n-a1", "us-central1-a"))
	s.inv.Upsert(node("n-a2", "us-central1-a"))
	s.inv.Upsert(node("n-b1", "us-central1-b"))
	s.inv.Upsert(node("n-b2", "us-central1-b"))
	s.inv.Upsert(node("n-c1", "us-central1-c", cordonedAtTaint(drainedAt)))

	// Two samples inside the window, with zone b losing a node between them.
	s.inv.SampleReady(t0.Add(-20*time.Minute), time.Hour)
	s.inv.Upsert(node("n-b2", "us-central1-b", notReady()))
	s.inv.SampleReady(t0.Add(-time.Minute), time.Hour)

	eligible := evenlyEligible("us-central1-a", "us-central1-b", "us-central1-c")
	dist := &leeway.Distribution{ByDomain: map[leeway.Domain]*leeway.DomainCount{
		"us-central1-a": {Running: 3},
		"us-central1-b": {Running: 1, Pinned: 1},
	}}

	ev := s.evidenceFor(webSubject, zoneKey, eligible, dist, now)

	if len(ev.Domains) != 3 {
		t.Fatalf("evidence covers %d domains, want one per eligible domain", len(ev.Domains))
	}
	if got := ev.Domains["us-central1-a"]; got.ReadyNodes != 2 || got.NotReadyNodes != 0 {
		t.Errorf("zone a = %+v, want 2 ready and none not-ready", got)
	}
	b := ev.Domains["us-central1-b"]
	if b.ReadyNodes != 1 || b.NotReadyNodes != 1 {
		t.Errorf("zone b = %+v, want 1 ready and 1 present-but-not-ready", b)
	}
	if b.PeakReadyNodes != 2 {
		t.Errorf("zone b peak = %d, want the 2 it held before the node went NotReady", b.PeakReadyNodes)
	}
	if b.LostAt.IsZero() {
		t.Error("zone b lost a ready node inside the window and the fall was not dated")
	}
	if b.Pinned != 1 {
		t.Errorf("zone b pinned = %d, want the distribution's count", b.Pinned)
	}
	if !ev.ZonalVolumes {
		t.Error("a pinned object in an eligible domain did not set ZonalVolumes — §8.5's pinning rule reads it")
	}
	if got := ev.Domains["us-central1-c"].TaintedAt; !got.Equal(drainedAt) {
		t.Errorf("zone c TaintedAt = %v, want the cordon's stamp %v", got, drainedAt)
	}

	// No capacity oracle wired: the pending-pod fields stay zero, and the
	// evidence says they are unanswered rather than negative. That difference
	// is the whole of #474 — a rule with no evidence still loses, but the
	// finding no longer claims the hypothesis was tested.
	if ev.InsufficientResource != 0 || ev.SchedulingMessage != "" {
		t.Errorf("pending-pod evidence with no oracle wired: %+v", ev)
	}
	if !ev.Unavailable.Capacity {
		t.Error("no capacity oracle is wired and the evidence does not report it unavailable")
	}
	if !ev.RolloutEndedAt.IsZero() {
		t.Errorf("RolloutEndedAt = %v, want the zero value with no oracle wired", ev.RolloutEndedAt)
	}
	if !ev.Unavailable.RolloutCompletion {
		t.Error("no rollout-end oracle is wired and the evidence does not report it unavailable")
	}
	// Consolidation has no seam to be missing: it is read off the node objects
	// this source already watches, so a domain with no consolidation carries
	// the zero stamp and means it.
	for d, facts := range ev.Domains {
		if !facts.ConsolidatedAt.IsZero() {
			t.Errorf("%s carries a consolidation stamp and no node here was ever claimed", d)
		}
	}
}

// The other half of the same call: a domain the autoscaler packed up carries
// the stamp §8.5's consolidation rung reads, and its neighbours do not.
func TestSource_EvidenceForCarriesTheConsolidationStamp(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	now := t0
	s.inv.now = func() time.Time { return now }

	packedAt := t0.Add(-9 * time.Minute)
	s.inv.Upsert(node("n-a1", "us-central1-a"))
	s.inv.Upsert(node("n-b1", "us-central1-b"))
	s.inv.Upsert(node("n-b1", "us-central1-b", disruptedAtTaint(packedAt)))
	s.inv.Remove("n-b1")

	eligible := evenlyEligible("us-central1-a", "us-central1-b")
	ev := s.evidenceFor(webSubject, zoneKey, eligible, nil, now)

	if got := ev.Domains["us-central1-b"].ConsolidatedAt; !got.Equal(packedAt) {
		t.Errorf("zone b ConsolidatedAt = %v, want the autoscaler's stamp %v", got, packedAt)
	}
	if got := ev.Domains["us-central1-a"].ConsolidatedAt; !got.IsZero() {
		t.Errorf("zone a ConsolidatedAt = %v; the consolidation was in another zone", got)
	}

	// And the rung is genuinely reachable, which is the whole claim: run the
	// same Attribute call emit makes. Consolidation has to outrank the
	// shortfall it is indistinguishable from — both are a domain short of
	// nodes, and "the autoscaler removed it" is a different remedy.
	scores := &leeway.Scores{
		Domains:    []leeway.Domain{"us-central1-a", "us-central1-b"},
		Actual:     []int64{4, 0},
		Expected:   []int64{2, 2},
		Total:      4,
		Relocation: 2,
		Drift:      0.5,
		Evaluable:  true,
	}
	a := scores.Attribute(nil, ev, now, leeway.DefaultCauseConfig())
	if a.Cause != leeway.CauseConsolidation {
		t.Errorf("Cause = %q, want %q", a.Cause, leeway.CauseConsolidation)
	}
}

func TestSource_EvidenceForReadsTheCapacityOracle(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	s.inv.Upsert(node("n-a1", "us-central1-a"))

	other := leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "default", Name: "api"}
	s.state.subjects = map[types.UID]leeway.SubjectRef{
		"mine-late":  webSubject,
		"mine-early": webSubject,
		"mine-taint": webSubject,
		"theirs":     other,
	}
	s.WithCapacityOracle(func() map[types.UID]PendingFact {
		return map[types.UID]PendingFact{
			"mine-late":  {Since: t0.Add(-time.Minute), Message: "late", InsufficientResource: true},
			"mine-early": {Since: t0.Add(-time.Hour), Message: "early", InsufficientResource: true},
			// Refused, but not for room: a taint is a different rung.
			"mine-taint": {Since: t0.Add(-2 * time.Hour), Message: "taint", InsufficientResource: false},
			// Another workload's problem entirely.
			"theirs": {Since: t0.Add(-3 * time.Hour), Message: "theirs", InsufficientResource: true},
			// Refused, counted, but this source has never seen the pod.
			"unknown": {Since: t0.Add(-4 * time.Hour), Message: "unknown", InsufficientResource: true},
		}
	})

	ev := s.evidenceFor(webSubject, zoneKey, evenlyEligible("us-central1-a"), nil, t0)

	if ev.InsufficientResource != 2 {
		t.Errorf("InsufficientResource = %d, want the 2 of this subject's pods refused for room", ev.InsufficientResource)
	}
	if ev.SchedulingMessage != "early" {
		t.Errorf("SchedulingMessage = %q, want the earliest refusal's so the payload is stable across passes", ev.SchedulingMessage)
	}
	if ev.Unavailable.Capacity {
		t.Error("a capacity oracle is wired and the evidence still reports it unavailable")
	}
}

func TestSource_EvidenceForReadsTheRolloutEndOracle(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	s.inv.Upsert(node("n-a1", "us-central1-a"))

	ended := t0.Add(-30 * time.Minute)
	other := leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "default", Name: "api"}
	s.WithRolloutEndOracle(func() map[leeway.SubjectRef]time.Time {
		return map[leeway.SubjectRef]time.Time{webSubject: ended, other: t0.Add(-time.Minute)}
	})

	// Read from the last cluster sample, not from the oracle directly: the
	// answer is cluster-wide and a pass covers every subject.
	if got := s.evidenceFor(webSubject, zoneKey, evenlyEligible("us-central1-a"), nil, t0); !got.RolloutEndedAt.IsZero() {
		t.Errorf("RolloutEndedAt = %v before the first sample, want the zero value", got.RolloutEndedAt)
	}
	if got := s.evidenceFor(webSubject, zoneKey, evenlyEligible("us-central1-a"), nil, t0); got.Unavailable.RolloutCompletion {
		t.Error("an oracle is wired and the evidence reports rollout completion unavailable")
	}

	s.sampleCluster(t0)

	ev := s.evidenceFor(webSubject, zoneKey, evenlyEligible("us-central1-a"), nil, t0)
	if !ev.RolloutEndedAt.Equal(ended) {
		t.Errorf("RolloutEndedAt = %v, want this subject's stamp %v", ev.RolloutEndedAt, ended)
	}

	// Another subject's rollout is not this subject's.
	untouched := leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "default", Name: "cache"}
	if got := s.evidenceFor(untouched, zoneKey, evenlyEligible("us-central1-a"), nil, t0); !got.RolloutEndedAt.IsZero() {
		t.Errorf("RolloutEndedAt = %v for a subject with no stamp, want the zero value", got.RolloutEndedAt)
	}
}

func TestSource_EvidenceForToleratesAnAbsentDistribution(t *testing.T) {
	// The eligibility sweep and the distribution snapshot are taken under
	// different locks, so a subject whose last pod went away between them
	// arrives here with nil. It must still get its node facts.
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	s.inv.Upsert(node("n-a1", "us-central1-a"))

	ev := s.evidenceFor(webSubject, zoneKey, evenlyEligible("us-central1-a", "us-central1-b"), nil, t0)
	if got := ev.Domains["us-central1-a"].ReadyNodes; got != 1 {
		t.Errorf("zone a ready = %d, want 1", got)
	}
	if ev.ZonalVolumes {
		t.Error("ZonalVolumes is set with no distribution to read it from")
	}
	// A domain the inventory has never seen is still a row: the finding's
	// argument is "nothing is here", and an absent row would read as "not
	// eligible".
	if _, ok := ev.Domains["us-central1-b"]; !ok {
		t.Error("an eligible domain with no nodes got no row")
	}
}
