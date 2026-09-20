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

package computeclass

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

func ptr[T any](v T) *T { return &v }

func sample(d time.Duration, secs map[leeway.Rank]float64) rankSample {
	return rankSample{at: at(d), podSeconds: secs}
}

// TestSampleAxis_KeepsTheNewestSampleAtOrBeforeTheEdge is the trim rule, and it
// is the rule that decides how long the window actually is.
//
// Dropping the oldest sample INSIDE the window instead would shorten the window
// by up to one tick on every tick — a share computed over 58 minutes and called
// an hour — and the error would be invisible because the number still looks
// like a share.
func TestSampleAxis_KeepsTheNewestSampleAtOrBeforeTheEdge(t *testing.T) {
	h := &axisHistory{}
	for i := 0; i <= 10; i++ {
		h.sampleAxis(sample(time.Duration(i)*time.Minute, nil), 5*time.Minute)
	}

	// Edge is minute 5, so the base must be the minute-5 sample: at the edge,
	// not inside it.
	if got, want := h.samples[0].at, at(5*time.Minute); !got.Equal(want) {
		t.Errorf("base sample at %s, want %s", got, want)
	}
	if got := h.samples[len(h.samples)-1].at; !got.Equal(at(10 * time.Minute)) {
		t.Errorf("newest sample at %s, want the one just appended", got)
	}
	if got := h.samples[len(h.samples)-1].at.Sub(h.samples[0].at); got != 5*time.Minute {
		t.Errorf("window spans %s, want the configured 5m", got)
	}
}

// TestSampleAxis_NeverDropsItsOnlySample: a ring trimmed to nothing would make
// every window zero-length, which the MinWindow gate reads as "not judgeable"
// forever rather than as a bug.
func TestSampleAxis_NeverDropsItsOnlySample(t *testing.T) {
	h := &axisHistory{}
	h.sampleAxis(sample(0, nil), time.Nanosecond)
	if len(h.samples) != 1 {
		t.Fatalf("samples = %d, want the one just appended", len(h.samples))
	}
}

// TestObserveRank0_NeedsAWorseTierOccupiedToCallItALoss is the qualifier the
// whole no-migration rule rests on.
//
// A class is empty at rank 0 for the first seconds of its life and whenever
// nothing is running on it at all. Reading either as "first-choice capacity was
// lost" would make every fresh class look like a recovery in progress.
func TestObserveRank0_NeedsAWorseTierOccupiedToCallItALoss(t *testing.T) {
	h := &axisHistory{}
	h.observeRank0(0, 0, at(time.Minute))
	if h.lost {
		t.Error("an idle class with nothing anywhere was recorded as a loss")
	}
	h.observeRank0(3, 0, at(2*time.Minute))
	if h.lost || h.restored {
		t.Error("a healthy class moved the loss machine")
	}
}

func TestObserveRank0_LossThenRestoreThenLossAgain(t *testing.T) {
	h := &axisHistory{}

	h.observeRank0(0, 4, at(time.Minute))
	if !h.lost || h.restored {
		t.Fatalf("after the loss: lost=%v restored=%v", h.lost, h.restored)
	}

	h.observeRank0(2, 4, at(5*time.Minute))
	if !h.restored || !h.restoredAt.Equal(at(5*time.Minute)) {
		t.Fatalf("after the restore: restored=%v at %s", h.restored, h.restoredAt)
	}

	// A second restore reading must not move the clock: the grace period runs
	// from when rank 0 came back, not from the last time we noticed it was up.
	h.observeRank0(3, 4, at(9*time.Minute))
	if !h.restoredAt.Equal(at(5 * time.Minute)) {
		t.Errorf("restoredAt moved to %s on a second healthy reading", h.restoredAt)
	}

	// Falling over again clears the restore, so a flapping class cannot bank a
	// grace period it never sat through.
	h.observeRank0(0, 4, at(11*time.Minute))
	if !h.lost || h.restored {
		t.Errorf("after the second loss: lost=%v restored=%v", h.lost, h.restored)
	}
}

func TestWindow_DiffsTheEndsAndClampsWhatWentBackwards(t *testing.T) {
	h := &axisHistory{}
	h.sampleAxis(rankSample{
		at:         at(0),
		podSeconds: map[leeway.Rank]float64{0: 100, 1: 40},
		improving:  2, worsening: 5, lateral: 1,
	}, time.Hour)
	h.sampleAxis(rankSample{
		at: at(30 * time.Minute),
		// Rank 1 went backwards — a bucket discarded and recreated between two
		// readings — and rank 2 is new.
		podSeconds: map[leeway.Rank]float64{0: 160, 1: 10, 2: 30},
		improving:  2, worsening: 9, lateral: 1,
	}, time.Hour)

	w := h.window(map[leeway.Rank]int{0: 3})
	if w.Elapsed != 30*time.Minute {
		t.Errorf("Elapsed = %s, want 30m", w.Elapsed)
	}
	if got := w.PodSeconds[0]; got != 60 {
		t.Errorf("rank 0 = %v, want the 60s difference", got)
	}
	if got, ok := w.PodSeconds[1]; ok {
		t.Errorf("rank 1 = %v, want the backwards bucket dropped rather than negative", got)
	}
	if got := w.PodSeconds[2]; got != 30 {
		t.Errorf("rank 2 = %v, want 30", got)
	}
	if w.Improving != 0 || w.Worsening != 4 || w.Lateral != 0 {
		t.Errorf("moves = %d/%d/%d, want 0 improving, 4 worsening, 0 lateral",
			w.Improving, w.Worsening, w.Lateral)
	}
	if w.Pods[0] != 3 {
		t.Errorf("Pods was not carried through: %v", w.Pods)
	}
}

// TestWindow_OnAnEmptyRing returns a zero window rather than panicking on
// samples[0]. judgeAll always samples before it asks, so this is the guard
// rather than a path — and a guard that is not exercised is not a guard.
func TestWindow_OnAnEmptyRing(t *testing.T) {
	h := &axisHistory{}
	w := h.window(nil)
	if w.Elapsed != 0 || len(w.PodSeconds) != 0 {
		t.Errorf("window = %+v, want the zero value", w)
	}
}

// TestJudgeAll_WarmsUpBeforeItWillScore: the first pass has a zero-length
// window, which MinWindow declines rather than scoring a single instant as
// though it were an hour.
func TestJudgeAll_WarmsUpBeforeItWillScore(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("worst", "n4-preferred", "n2", "2"), t0)
	s.UpsertPod(pod("p", "worst", corev1.PodRunning), t0)
	s.cfg.LastRankShareCeiling = ptr(0.5)

	js := s.judgeAll(at(time.Second))
	if len(js) != 1 {
		t.Fatalf("judgements = %d, want one per class", len(js))
	}
	for _, v := range js[0].verdicts {
		if v.Breached {
			t.Errorf("%s breached on the warm-up pass: %s", v.Rule, v.Reason)
		}
	}

	// An hour later the same class is entirely on its last rank, and the same
	// rule has something to say.
	js = s.judgeAll(at(time.Hour))
	var found bool
	for _, v := range js[0].verdicts {
		if v.Rule == leeway.RankRuleLastRank {
			found = true
			if !v.Breached {
				t.Errorf("last-rank did not breach on a class 100%% at rank 2: %s", v.Reason)
			}
		}
	}
	if !found {
		t.Error("no last-rank verdict was returned at all")
	}
}

// TestJudgeAll_SamplesAClassNothingIsRunningOn. An axis with no buckets is
// exactly the subject of the unused-tier rule, so skipping it would make that
// rule unable to see the thing it is about.
func TestJudgeAll_SamplesAClassNothingIsRunningOn(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)

	js := s.judgeAll(at(time.Minute))
	if len(js) != 1 {
		t.Fatalf("judgements = %d, want the empty class judged", len(js))
	}
	key := leeway.AxisKey{Provider: leeway.ProviderGKEComputeClass, Name: "n4-preferred"}
	if _, ok := s.history[key]; !ok {
		t.Error("no history ring was opened for the empty class")
	}
}

// TestJudgeAll_ForgetsTheRingOfADeletedClass. AxisKey is provider and name
// only, so a ring left behind is both a leak and a stale window waiting for a
// class of the same name to come back.
func TestJudgeAll_ForgetsTheRingOfADeletedClass(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	s.judgeAll(at(time.Minute))
	if len(s.history) != 1 {
		t.Fatalf("history = %d rings, want 1", len(s.history))
	}

	s.DeleteClass("n4-preferred", at(2*time.Minute))
	s.judgeAll(at(3 * time.Minute))
	if len(s.history) != 0 {
		t.Errorf("history = %d rings after the class was deleted, want 0", len(s.history))
	}
}

// TestTransitionTotals_CountAMoveInOneDirectionOnly, and count three of them as
// neither direction: a lateral move is churn within a preference level, and a
// move in or out of a sentinel rank is most often a node's annotation arriving
// for the first time.
func TestTransitionTotals_CountAMoveInOneDirectionOnly(t *testing.T) {
	s := newTestSource(t)
	axis := leeway.AxisKey{Provider: leeway.ProviderGKEComputeClass, Name: "x"}
	s.transitions = map[transitionKey]int64{
		{Axis: axis, From: 0, To: 2}:                                   3, // worsening
		{Axis: axis, From: 2, To: 1}:                                   2, // improving
		{Axis: axis, From: 1, To: 1, Lateral: true}:                    7, // neither
		{Axis: axis, From: leeway.RankUnknown, To: 0}:                  9, // neither
		{Axis: axis, From: 1, To: leeway.RankOffAxis}:                  4, // neither
		{Axis: axis, From: leeway.RankUnknown, To: leeway.RankOffAxis}: 5, // neither
	}

	got := s.transitionTotals()[axis]
	if got.worsening != 3 || got.improving != 2 || got.lateral != 7 {
		t.Errorf("totals = %+v, want 3 worsening, 2 improving, 7 lateral", got)
	}
}

// TestConditionsFor_SinceRestoreIsZeroUntilRankZeroIsBack. A grace period
// measured from the zero time would be decades long and would clear the rule on
// its first pass, which is the opposite of what it is for.
func TestConditionsFor_SinceRestoreIsZeroUntilRankZeroIsBack(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	class := s.classes["n4-preferred"]

	h := &axisHistory{epoch: t0}
	c := s.conditionsFor(class, h, nil, at(time.Hour))
	if c.SinceRestore != 0 {
		t.Errorf("SinceRestore = %s with no restore recorded, want 0", c.SinceRestore)
	}
	if c.Lifetime != time.Hour {
		t.Errorf("Lifetime = %s, want an hour since the epoch", c.Lifetime)
	}

	h.restored, h.restoredAt = true, at(30*time.Minute)
	if c := s.conditionsFor(class, h, nil, at(time.Hour)); c.SinceRestore != 30*time.Minute {
		t.Errorf("SinceRestore = %s, want 30m", c.SinceRestore)
	}
}

func TestConditionsFor_CountsThePodsWedgedOnThisClass(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	s.UpsertPod(wedgedPod("a", "n4-preferred"), t0)
	s.UpsertPod(wedgedPod("b", "n4-preferred"), t0)
	s.UpsertPod(wedgedPod("c", "somewhere-else"), t0)

	c := s.conditionsFor(s.classes["n4-preferred"], &axisHistory{epoch: t0}, nil, at(time.Minute))
	if c.PendingPods != 2 {
		t.Errorf("PendingPods = %d, want the two on this class", c.PendingPods)
	}
}

// TestThresholds_ZeroIsASettingAndNotAnAbsence is §7.7.5 made operative. An
// estate whose last rank is the cheap family it was always meant to run on
// wants the ceiling OFF, and a plain float could not tell that apart from an
// operator who said nothing.
func TestThresholds_ZeroIsASettingAndNotAnAbsence(t *testing.T) {
	s := newTestSource(t)

	def := s.thresholds()
	if def.LastRankShareCeiling != leeway.DefaultRankThresholds().LastRankShareCeiling {
		t.Errorf("unset ceiling = %v, want the default", def.LastRankShareCeiling)
	}
	if def.Rank0ShareFloor != 0 {
		t.Errorf("unset floor = %v, want the rule off by default", def.Rank0ShareFloor)
	}

	s.cfg.LastRankShareCeiling = ptr(0.0)
	s.cfg.Rank0ShareFloor = ptr(0.8)
	s.cfg.UnusedTierFor = 48 * time.Hour
	s.cfg.MigrationGrace = 90 * time.Minute

	got := s.thresholds()
	if got.LastRankShareCeiling != 0 {
		t.Errorf("ceiling = %v, want the operator's explicit zero", got.LastRankShareCeiling)
	}
	if got.Rank0ShareFloor != 0.8 {
		t.Errorf("floor = %v, want 0.8", got.Rank0ShareFloor)
	}
	if got.UnusedTierFor != 48*time.Hour || got.MigrationGrace != 90*time.Minute {
		t.Errorf("durations = %s/%s, want the configured ones", got.UnusedTierFor, got.MigrationGrace)
	}
}
