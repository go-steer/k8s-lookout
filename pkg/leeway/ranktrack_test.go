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
	"math"
	"sync"
	"testing"
	"time"
)

var trackAxis = AxisKey{Provider: ProviderGKEComputeClass, Name: "high-perf"}

// rankT0 is an arbitrary fixed instant. Everything in here is relative to it, so
// no test depends on a wall clock.
var rankT0 = time.Date(2026, 9, 15, 18, 0, 0, 0, time.UTC)

func rankAt(d time.Duration) time.Time { return rankT0.Add(d) }

// podSecondsAt is the one assertion most of these tests make.
func podSecondsAt(t *testing.T, s RankSnapshot, rank Rank) float64 {
	t.Helper()
	for _, r := range s.Ranks {
		if r.Rank == rank {
			return r.PodSeconds
		}
	}
	return math.NaN()
}

func TestRankTracker_TimeWeighted(t *testing.T) {
	// Two pods at rank 0 for a minute is 120 pod-seconds, not 2 and not 60.
	tr := NewRankTracker()
	tr.Enter(trackAxis, 0, rankAt(0))
	tr.Enter(trackAxis, 0, rankAt(0))
	tr.Flush(rankAt(time.Minute))

	if got := podSecondsAt(t, tr.Snapshot(), 0); got != 120 {
		t.Errorf("pod-seconds = %v, want 120", got)
	}
}

// TestRankTracker_ABurstAndAParkAreDistinguishable is the reason the primary
// signal is not a gauge. A 90-second burst of rank-3 pods during a scale-up
// and three weeks parked on the fallback look identical to a scrape.
func TestRankTracker_ABurstAndAParkAreDistinguishable(t *testing.T) {
	tr := NewRankTracker()
	// Ten pods at rank 3 for ninety seconds, then gone.
	for i := 0; i < 10; i++ {
		tr.Enter(trackAxis, 3, rankAt(0))
	}
	for i := 0; i < 10; i++ {
		tr.Leave(trackAxis, 3, rankAt(90*time.Second))
	}
	// One pod at rank 1 for a day.
	tr.Enter(trackAxis, 1, rankAt(0))
	tr.Flush(rankAt(24 * time.Hour))

	snap := tr.Snapshot()
	burst, park := podSecondsAt(t, snap, 3), podSecondsAt(t, snap, 1)
	if burst != 900 {
		t.Errorf("burst = %v pod-seconds, want 900", burst)
	}
	if park != 86400 {
		t.Errorf("park = %v pod-seconds, want 86400", park)
	}
	if park <= burst {
		t.Error("the park did not outweigh the burst — a gauge would have called them equal")
	}
	// And the burst's pods are gone, which the gauge alone would have said.
	for _, r := range snap.Ranks {
		if r.Rank == 3 && r.Pods != 0 {
			t.Errorf("rank 3 still holds %d pods", r.Pods)
		}
	}
}

// TestRankTracker_EnterAndLeaveAreIndependent. S1: a fallback provisions a new
// node and the ReplicaSet creates a NEW pod; the old one is deleted. The two
// calls land minutes apart, on different pods, and the ranks overlap for the
// whole migration — two minutes, measured.
func TestRankTracker_EnterAndLeaveAreIndependent(t *testing.T) {
	tr := NewRankTracker()
	tr.Enter(trackAxis, 1, rankAt(0))              // pod vd5cj on the fallback node
	tr.Enter(trackAxis, 0, rankAt(10*time.Minute)) // pod vhp6p, a DIFFERENT pod
	tr.Leave(trackAxis, 1, rankAt(12*time.Minute)) // vd5cj drained, 2 min later
	tr.Flush(rankAt(12 * time.Minute))

	snap := tr.Snapshot()
	if got := podSecondsAt(t, snap, 1); got != 720 {
		t.Errorf("rank 1 = %v pod-seconds, want 720 — twelve minutes of one pod", got)
	}
	if got := podSecondsAt(t, snap, 0); got != 120 {
		t.Errorf("rank 0 = %v pod-seconds, want 120 — the overlapping two minutes", got)
	}
}

func TestRankTracker_FlushIsWhatMakesAQuiescentRankAdvance(t *testing.T) {
	// Without it, a workload that reaches its fallback and stays there accrues
	// pod-seconds only when something changes, which is to say never.
	tr := NewRankTracker()
	tr.Enter(trackAxis, 2, rankAt(0))

	if got := podSecondsAt(t, tr.Snapshot(), 2); got != 0 {
		t.Errorf("pod-seconds = %v before any flush, want 0", got)
	}
	tr.Flush(rankAt(time.Hour))
	if got := podSecondsAt(t, tr.Snapshot(), 2); got != 3600 {
		t.Errorf("pod-seconds = %v after an hour, want 3600", got)
	}
}

func TestRankTracker_SnapshotDoesNotAdvanceTheClock(t *testing.T) {
	// If a read advanced time, the totals would depend on how often they were
	// looked at, and two dashboards scraping at different rates would disagree.
	tr := NewRankTracker()
	tr.Enter(trackAxis, 0, rankAt(0))
	tr.Flush(rankAt(time.Minute))

	first := podSecondsAt(t, tr.Snapshot(), 0)
	for i := 0; i < 5; i++ {
		tr.Snapshot()
	}
	if got := podSecondsAt(t, tr.Snapshot(), 0); got != first {
		t.Errorf("pod-seconds drifted from %v to %v across reads", first, got)
	}
}

// TestRankTracker_AnUnbalancedLeaveDoesNotRunTheCounterBackwards. The design
// snippet decrements unconditionally, and a negative count makes accumulate
// SUBTRACT time from a counter — every rate() over the window then reads as a
// reset. A pod whose node was unknown at Enter and resolved by the time it
// died produces exactly this.
func TestRankTracker_AnUnbalancedLeaveDoesNotRunTheCounterBackwards(t *testing.T) {
	tr := NewRankTracker()
	tr.Enter(trackAxis, 0, rankAt(0))
	tr.Leave(trackAxis, 0, rankAt(time.Minute))
	tr.Leave(trackAxis, 0, rankAt(time.Minute)) // one too many
	tr.Flush(rankAt(10 * time.Minute))

	snap := tr.Snapshot()
	if got := podSecondsAt(t, snap, 0); got != 60 {
		t.Errorf("pod-seconds = %v, want 60 and no further movement", got)
	}
	if snap.Underflows != 1 {
		t.Errorf("Underflows = %d, want 1 — the imbalance must be visible", snap.Underflows)
	}
	for _, r := range snap.Ranks {
		if r.Pods < 0 {
			t.Errorf("rank %v holds %d pods", r.Rank, r.Pods)
		}
	}
}

// TestRankTracker_TimeNeverRunsBackwards. An informer replaying a stale event
// and a clock step both produce an `at` earlier than the last flush.
func TestRankTracker_TimeNeverRunsBackwards(t *testing.T) {
	tr := NewRankTracker()
	tr.Enter(trackAxis, 0, rankAt(0))
	tr.Flush(rankAt(time.Hour))
	before := podSecondsAt(t, tr.Snapshot(), 0)

	tr.Flush(rankAt(time.Minute)) // fifty-nine minutes in the past
	tr.Enter(trackAxis, 0, rankAt(-time.Hour))

	if got := podSecondsAt(t, tr.Snapshot(), 0); got != before {
		t.Errorf("pod-seconds = %v after a backwards clock, want it held at %v", got, before)
	}
	// And the bucket must still be anchored at the later instant, or the next
	// real flush would double-count the hour it already credited.
	tr.Flush(rankAt(time.Hour))
	if got := podSecondsAt(t, tr.Snapshot(), 0); got != before {
		t.Errorf("pod-seconds = %v, want no double count", got)
	}
}

// TestRankTracker_ABucketStartsWhenItIsFirstOccupied. A zero-valued lastFlush
// would credit the first flush with the time since year one.
func TestRankTracker_ABucketStartsWhenItIsFirstOccupied(t *testing.T) {
	tr := NewRankTracker()
	tr.Enter(trackAxis, 0, rankAt(0))
	tr.Flush(rankAt(time.Minute))

	if got := podSecondsAt(t, tr.Snapshot(), 0); got != 60 {
		t.Errorf("pod-seconds = %v, want 60 — not two millennia", got)
	}
}

// TestRankTracker_TheMeanExcludesTheNonTierBuckets. A mean achieved rank
// computed over a bucket whose rank is -2 is not a mean of anything.
func TestRankTracker_TheMeanExcludesTheNonTierBuckets(t *testing.T) {
	tr := NewRankTracker()
	tr.Enter(trackAxis, 0, rankAt(0))
	tr.Enter(trackAxis, 2, rankAt(0))
	tr.Enter(trackAxis, RankOffAxis, rankAt(0))
	tr.Enter(trackAxis, RankUnknown, rankAt(0))
	tr.Enter(trackAxis, RankUnsatisfiable, rankAt(0))
	tr.Flush(rankAt(time.Minute))

	snap := tr.Snapshot()
	if len(snap.Axes) != 1 {
		t.Fatalf("Axes = %+v, want one", snap.Axes)
	}
	a := snap.Axes[0]
	if a.TierPodSeconds != 120 {
		t.Errorf("TierPodSeconds = %v, want 120 — ranks 0 and 2 only", a.TierPodSeconds)
	}
	if a.RankWeightedSeconds != 120 {
		t.Errorf("RankWeightedSeconds = %v, want 120 — 0*60 + 2*60", a.RankWeightedSeconds)
	}
	if mean := a.RankWeightedSeconds / a.TierPodSeconds; mean != 1 {
		t.Errorf("mean achieved rank = %v, want 1", mean)
	}
	// The bad states still report their time — "we spent a week off-axis" is
	// the finding, not something to drop.
	if got := podSecondsAt(t, snap, RankOffAxis); got != 60 {
		t.Errorf("off-axis pod-seconds = %v, want 60", got)
	}
}

func TestRankTracker_SnapshotIsOrdered(t *testing.T) {
	// A deterministic emit order keeps the metric export from reshuffling on
	// every scrape, which is Go map iteration's default gift.
	other := AxisKey{Provider: ProviderGKEComputeClass, Name: "aaa-first"}
	tr := NewRankTracker()
	for _, r := range []Rank{2, RankOffAxis, 0, 1} {
		tr.Enter(trackAxis, r, rankAt(0))
	}
	tr.Enter(other, 0, rankAt(0))

	snap := tr.Snapshot()
	var last RankOccupancy
	for i, r := range snap.Ranks {
		if i > 0 {
			sameAxis := r.Axis == last.Axis
			if (!sameAxis && r.Axis.String() < last.Axis.String()) || (sameAxis && r.Rank < last.Rank) {
				t.Errorf("out of order at %d: %v/%v after %v/%v", i, r.Axis, r.Rank, last.Axis, last.Rank)
			}
		}
		last = r
	}
	if len(snap.Axes) != 2 || snap.Axes[0].Axis != other {
		t.Errorf("Axes = %+v, want aaa-first sorted ahead", snap.Axes)
	}
}

func TestRankTracker_AxesAreIndependent(t *testing.T) {
	other := AxisKey{Provider: ProviderGKEComputeClass, Name: "batch"}
	tr := NewRankTracker()
	tr.Enter(trackAxis, 0, rankAt(0))
	tr.Enter(other, 0, rankAt(0))
	tr.Leave(other, 0, rankAt(30*time.Second))
	tr.Flush(rankAt(time.Minute))

	snap := tr.Snapshot()
	for _, r := range snap.Ranks {
		want := 60.0
		if r.Axis == other {
			want = 30
		}
		if r.PodSeconds != want {
			t.Errorf("%v rank %v = %v pod-seconds, want %v", r.Axis, r.Rank, r.PodSeconds, want)
		}
	}
}

// TestRankTracker_ForgetDropsOneAxisAndOnlyThatOne is the re-tiering path: a
// class whose spec hash changed has to start a new series, because rank 1 no
// longer means what the accrued seconds were accrued at. The buckets go rather
// than zero, since a zeroed bucket would go on reporting a rank the new spec
// may not have.
func TestRankTracker_ForgetDropsOneAxisAndOnlyThatOne(t *testing.T) {
	other := AxisKey{Provider: ProviderGKEComputeClass, Name: "batch"}
	tr := NewRankTracker()
	tr.Enter(trackAxis, 0, rankAt(0))
	tr.Enter(trackAxis, 2, rankAt(0))
	tr.Enter(other, 1, rankAt(0))
	tr.Flush(rankAt(time.Minute))

	tr.Forget(trackAxis)

	snap := tr.Snapshot()
	if len(snap.Ranks) != 1 || snap.Ranks[0].Axis != other {
		t.Fatalf("Snapshot after Forget = %+v, want only the untouched axis", snap.Ranks)
	}
	if snap.Ranks[0].PodSeconds != 60 || snap.Ranks[0].Pods != 1 {
		t.Errorf("the surviving axis lost state: %+v", snap.Ranks[0])
	}

	// Re-entering is the caller's job — this type does not hold occupancy —
	// and the new series starts from zero rather than from the old total.
	tr.Enter(trackAxis, 0, rankAt(time.Minute))
	tr.Flush(rankAt(2 * time.Minute))
	for _, r := range tr.Snapshot().Ranks {
		if r.Axis == trackAxis && r.PodSeconds != 60 {
			t.Errorf("re-entered axis = %v pod-seconds, want a series starting at zero", r.PodSeconds)
		}
	}
}

func TestRankTracker_EmptyTrackerSnapshots(t *testing.T) {
	snap := NewRankTracker().Snapshot()
	if len(snap.Ranks) != 0 || len(snap.Axes) != 0 || snap.Underflows != 0 {
		t.Errorf("Snapshot = %+v, want empty", snap)
	}
}

// TestRankTracker_IsConcurrencySafe: node and pod events arrive from separate
// informer handlers while the flush ticker and the scrape both read.
func TestRankTracker_IsConcurrencySafe(t *testing.T) {
	tr := NewRankTracker()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				d := time.Duration(j) * time.Second
				tr.Enter(trackAxis, Rank(i%3), rankAt(d))
				tr.Flush(rankAt(d))
				tr.Snapshot()
				tr.Leave(trackAxis, Rank(i%3), rankAt(d))
			}
		}(i)
	}
	wg.Wait()

	for _, r := range tr.Snapshot().Ranks {
		if r.Pods != 0 {
			t.Errorf("rank %v holds %d pods after balanced traffic", r.Rank, r.Pods)
		}
	}
}
