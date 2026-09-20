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
	"sort"
	"sync"
	"time"
)

// RankTracker accumulates time-weighted occupancy per rank.
//
// The question §7.7 answers is not "how many pods are at rank 2 right now" but
// "what fraction of the time do we run at rank 2", so the primary signal is
// pod-seconds. A gauge answers the wrong question: a ninety-second burst of
// rank-3 pods during a scale-up and three weeks parked on the spot fallback
// look identical to a scrape every thirty seconds.
//
// Accumulation is O(1) per transition — a count and a timestamp per bucket, no
// per-pod timers — plus a flush so a quiescent rank still advances.
//
// Enter and Leave, not Move. Spike S1 established that a fallback provisions a
// new node and the ReplicaSet creates a new pod there; the old pod is deleted.
// No pod ever changes rank, so the two calls are independent, usually minutes
// apart, and on different pods. Pod-seconds are right either way: the old pod
// stops accruing and the new one starts.
type RankTracker struct {
	mu      sync.Mutex
	buckets map[rankBucketKey]*rankBucket
	// underflows counts Leave calls against an empty bucket. See Leave.
	underflows int64
}

type rankBucketKey struct {
	Axis AxisKey
	Rank Rank
}

type rankBucket struct {
	podSeconds float64
	count      int
	lastFlush  time.Time
}

// NewRankTracker returns an empty tracker.
func NewRankTracker() *RankTracker {
	return &RankTracker{buckets: map[rankBucketKey]*rankBucket{}}
}

// accumulate credits the time since the last flush to this bucket's total.
//
// The clamp is not defensive noise. podSeconds feeds a counter, and a counter
// that goes backwards is worse than one that stops: every rate() over the
// window reads as a spike or a reset. Two ways it could: a count driven
// negative by an unbalanced Leave, and an `at` earlier than the last flush,
// which an informer replaying a stale event or a clock step will both produce.
func (b *rankBucket) accumulate(at time.Time) {
	if elapsed := at.Sub(b.lastFlush).Seconds(); elapsed > 0 && b.count > 0 {
		b.podSeconds += float64(b.count) * elapsed
	}
	if at.After(b.lastFlush) {
		b.lastFlush = at
	}
}

// bucket returns the bucket for one (axis, rank), creating it at `at` so that
// a bucket's first interval starts when it was first occupied and not at the
// zero time — which would credit it with two thousand years of pod-seconds.
func (t *RankTracker) bucket(key rankBucketKey, at time.Time) *rankBucket {
	b, ok := t.buckets[key]
	if !ok {
		b = &rankBucket{lastFlush: at}
		t.buckets[key] = b
	}
	return b
}

// Enter records a pod beginning to occupy a rank.
func (t *RankTracker) Enter(axis AxisKey, rank Rank, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	b := t.bucket(rankBucketKey{axis, rank}, at)
	b.accumulate(at)
	b.count++
}

// Leave records a pod ceasing to occupy a rank.
//
// An unbalanced Leave is possible in normal operation — a pod whose node was
// unknown at Enter and resolved by the time it died, a delete replayed after a
// resync — so the count is clamped at zero rather than allowed to go negative
// and start subtracting time from a counter. The imbalance is counted instead,
// because a steady stream of it means the source's bookkeeping is wrong and
// the shares are quietly skewed.
func (t *RankTracker) Leave(axis AxisKey, rank Rank, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	b := t.bucket(rankBucketKey{axis, rank}, at)
	b.accumulate(at)
	if b.count == 0 {
		t.underflows++
		return
	}
	b.count--
}

// Flush advances every bucket to now, so that a rank nothing has entered or
// left for an hour still reports the hour.
//
// This is what makes the metric time-weighted rather than transition-weighted.
// Without it a workload that reaches its fallback and stays there forever
// accrues its pod-seconds only when something changes, which is to say never.
func (t *RankTracker) Flush(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, b := range t.buckets {
		b.accumulate(now)
	}
}

// RankOccupancy is one axis/rank bucket at one instant.
type RankOccupancy struct {
	Axis       AxisKey
	Rank       Rank
	PodSeconds float64
	Pods       int
}

// AxisOccupancy is the per-axis roll-up the mean-achieved-rank query needs.
type AxisOccupancy struct {
	Axis AxisKey
	// RankWeightedSeconds is sum(rank * pod-seconds) over tier ranks only.
	RankWeightedSeconds float64
	// TierPodSeconds is the denominator that goes with it — pod-seconds at
	// tier ranks only. The unknown, unsatisfiable and off-axis buckets are
	// excluded from both: a mean achieved rank computed over a bucket whose
	// rank is -2 is not a mean of anything. Their time is still reported
	// per-rank, because "we spent a week off-axis" is the finding.
	TierPodSeconds float64
}

// RankSnapshot is the tracker's state, ordered for a deterministic emit.
type RankSnapshot struct {
	Ranks []RankOccupancy
	Axes  []AxisOccupancy
	// Underflows is the running total of unbalanced Leave calls — an SLI on
	// the source's own bookkeeping, not on the cluster.
	Underflows int64
}

// Snapshot reads the accumulated totals without flushing.
//
// Read-only on purpose: the caller decides when time advances (Flush), and a
// scrape that silently advanced the clock would make the totals depend on how
// often they were looked at.
func (t *RankTracker) Snapshot() RankSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := RankSnapshot{
		Ranks:      make([]RankOccupancy, 0, len(t.buckets)),
		Underflows: t.underflows,
	}
	perAxis := map[AxisKey]*AxisOccupancy{}
	for key, b := range t.buckets {
		out.Ranks = append(out.Ranks, RankOccupancy{
			Axis:       key.Axis,
			Rank:       key.Rank,
			PodSeconds: b.podSeconds,
			Pods:       b.count,
		})
		if !key.Rank.IsTier() {
			continue
		}
		a, ok := perAxis[key.Axis]
		if !ok {
			a = &AxisOccupancy{Axis: key.Axis}
			perAxis[key.Axis] = a
		}
		a.RankWeightedSeconds += float64(key.Rank) * b.podSeconds
		a.TierPodSeconds += b.podSeconds
	}

	out.Axes = make([]AxisOccupancy, 0, len(perAxis))
	for _, a := range perAxis {
		out.Axes = append(out.Axes, *a)
	}
	sort.Slice(out.Ranks, func(i, j int) bool {
		if out.Ranks[i].Axis != out.Ranks[j].Axis {
			return out.Ranks[i].Axis.String() < out.Ranks[j].Axis.String()
		}
		return out.Ranks[i].Rank < out.Ranks[j].Rank
	})
	sort.Slice(out.Axes, func(i, j int) bool {
		return out.Axes[i].Axis.String() < out.Axes[j].Axis.String()
	})
	return out
}
