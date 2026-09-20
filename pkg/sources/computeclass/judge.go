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
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// The judging half of §7.7.4: turning a monotonic pod-second counter into the
// bounded window a share can be computed over.
//
// # Why a window and not the counter
//
// pkg/leeway's tracker accumulates since the axis was first seen, and a share
// taken off those totals answers "what fraction of this class's whole recorded
// history ran at rank 2". That number is almost useless for alerting: a class
// that spent a bad week on its fallback in March goes on reporting a degraded
// share in June, and one that has been on its fallback for the last two hours
// after a clean quarter reports nothing at all. Both are backwards. So the
// judge diffs two readings of the counter and scores the difference.
//
// # Why a ring of samples and not a single previous reading
//
// The window has to be a fixed length or its meaning moves with the tick rate,
// and it has to survive a tick that was late. Keeping a short history and
// picking the sample at the far edge gives a window that is the configured
// length once warm and honestly shorter before that — which the MinWindow gate
// in pkg/leeway then declines to score, rather than scoring a ninety-second
// sample as though it were an hour.

// rankSample is one axis's cumulative counters at one instant.
//
// Cumulative, not per-window: the subtraction happens at judging time. Storing
// deltas instead would make every window length a decision taken at sampling
// time and unchangeable afterwards.
type rankSample struct {
	at         time.Time
	podSeconds map[leeway.Rank]float64
	improving  int64
	worsening  int64
	lateral    int64
}

// axisHistory is one axis's sample ring plus the two facts about it that no
// single sample can carry.
type axisHistory struct {
	// epoch is when this axis — this spec hash — was first sampled. It is what
	// §7.7.4's thirty-day unused-tier rule measures against, and it is reset by
	// a re-tiering for the reason the pod-seconds are: rank 1 stopped meaning
	// what it meant, so how long rank 1 has been idle is a new question.
	epoch time.Time

	samples []rankSample

	// lost records that rank 0 has been observed empty WHILE some worse tier
	// was occupied. That qualifier is the whole of it. A class is empty at rank
	// 0 for the first few seconds of its life and whenever it has no pods at
	// all, and treating either as "capacity was lost" would make every fresh
	// class look like a recovery in progress and fire the no-migration rule on
	// an estate that has simply always been mixed — which §7.7.4 names as the
	// thing that rule must not do.
	lost bool
	// restored is rank 0 coming back into use after a loss, and restoredAt is
	// when. Cleared by a subsequent loss, so a class that flaps does not
	// accumulate a grace period it never sat through.
	restored   bool
	restoredAt time.Time
}

// sampleAxis appends one reading and trims the ring to the window.
//
// The trim keeps the newest sample that is at or before the far edge, not the
// oldest inside it: dropping that one would shorten the window by up to one
// tick every tick, and the length is what the share is a share of.
func (h *axisHistory) sampleAxis(s rankSample, window time.Duration) {
	h.samples = append(h.samples, s)
	edge := s.at.Add(-window)
	for len(h.samples) >= 2 && !h.samples[1].at.After(edge) {
		h.samples = h.samples[1:]
	}
}

// observeRank0 advances the loss/restore machine from one reading.
func (h *axisHistory) observeRank0(rank0Pods, worsePods int, at time.Time) {
	switch {
	case rank0Pods == 0 && worsePods > 0:
		h.lost = true
		h.restored = false
	case rank0Pods > 0 && h.lost && !h.restored:
		h.restored = true
		h.restoredAt = at
	}
}

// window diffs the ring's two ends.
func (h *axisHistory) window(pods map[leeway.Rank]int) leeway.RankWindow {
	w := leeway.RankWindow{
		PodSeconds: map[leeway.Rank]float64{},
		Pods:       pods,
	}
	if len(h.samples) == 0 {
		return w
	}
	base, cur := h.samples[0], h.samples[len(h.samples)-1]
	w.Elapsed = cur.at.Sub(base.at)
	for rank, secs := range cur.podSeconds {
		// Clamped at zero. A rank whose bucket was discarded and recreated
		// between the two readings — a re-tiering that resetAxis did not catch
		// because the spec hash was equal — would otherwise contribute a
		// negative share, and a negative numerator in a ratio is worse than a
		// lost sample.
		if d := secs - base.podSeconds[rank]; d > 0 {
			w.PodSeconds[rank] = d
		}
	}
	w.Improving = diffCount(cur.improving, base.improving)
	w.Worsening = diffCount(cur.worsening, base.worsening)
	w.Lateral = diffCount(cur.lateral, base.lateral)
	return w
}

func diffCount(cur, base int64) int64 {
	if d := cur - base; d > 0 {
		return d
	}
	return 0
}

// rankJudgement is one axis's whole evaluation, held so the emit path can
// rebuild a payload without re-deriving the window under the pass's lock.
type rankJudgement struct {
	axis     leeway.AxisKey
	class    *leeway.ComputeClass
	window   leeway.RankWindow
	verdicts []leeway.RankVerdict
}

// judgeAll samples every axis and judges it. Caller must NOT hold s.mu.
//
// Flush first, then Snapshot. The tracker is deliberately read-only on
// Snapshot — the caller decides when time advances — so a judge pass that only
// snapshotted would diff two readings taken at the same accounting instant and
// find every window empty however long the wall clock said it was.
func (s *Source) judgeAll(now time.Time) []rankJudgement {
	s.tracker.Flush(now)
	snap := s.tracker.Snapshot()

	type axisTotals struct {
		podSeconds map[leeway.Rank]float64
		pods       map[leeway.Rank]int
		rank0Pods  int
		worsePods  int
	}
	totals := map[leeway.AxisKey]*axisTotals{}
	for _, r := range snap.Ranks {
		t, ok := totals[r.Axis]
		if !ok {
			t = &axisTotals{podSeconds: map[leeway.Rank]float64{}, pods: map[leeway.Rank]int{}}
			totals[r.Axis] = t
		}
		t.podSeconds[r.Rank] = r.PodSeconds
		t.pods[r.Rank] = r.Pods
		if !r.Rank.IsTier() {
			continue
		}
		if r.Rank == 0 {
			t.rank0Pods += r.Pods
		} else {
			t.worsePods += r.Pods
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	moves := s.transitionTotals()
	out := make([]rankJudgement, 0, len(s.classes))
	live := map[leeway.AxisKey]struct{}{}

	for _, class := range s.classes {
		key := class.Axis.Key
		live[key] = struct{}{}

		t := totals[key]
		if t == nil {
			// An axis with no buckets at all: a class nothing is running on.
			// Sampled anyway, because "rank 1 has been idle for thirty days" is
			// exactly a statement about an axis with no occupancy, and skipping
			// it would make the unused-tier rule unable to see its own subject.
			t = &axisTotals{podSeconds: map[leeway.Rank]float64{}, pods: map[leeway.Rank]int{}}
		}

		h, ok := s.history[key]
		if !ok {
			h = &axisHistory{epoch: now}
			s.history[key] = h
		}
		m := moves[key]
		h.sampleAxis(rankSample{
			at:         now,
			podSeconds: t.podSeconds,
			improving:  m.improving,
			worsening:  m.worsening,
			lateral:    m.lateral,
		}, s.cfg.Window)
		h.observeRank0(t.rank0Pods, t.worsePods, now)

		w := h.window(t.pods)
		out = append(out, rankJudgement{
			axis:   key,
			class:  class,
			window: w,
			verdicts: leeway.JudgeRank(leeway.RankInput{
				Class:      class,
				Window:     w,
				Conditions: s.conditionsFor(class, h, t.podSeconds, now),
			}, s.thresholds()),
		})
	}

	// An axis whose class was deleted keeps no history. Its episodes end by
	// being absent from the pass, which is the same eviction rule topologydrift
	// draws, and holding the ring would leak one map per class ever seen.
	for key := range s.history {
		if _, ok := live[key]; !ok {
			delete(s.history, key)
		}
	}
	return out
}

// conditionsFor assembles the non-window inputs. Caller holds s.mu.
func (s *Source) conditionsFor(class *leeway.ComputeClass, h *axisHistory, lifetime map[leeway.Rank]float64, now time.Time) leeway.RankConditions {
	c := leeway.RankConditions{
		PendingPods:     len(s.pendingByClass[class.Axis.Key.Name]),
		Rank0Restored:   h.restored,
		Lifetime:        now.Sub(h.epoch),
		LifetimeSeconds: lifetime,
	}
	if h.restored {
		c.SinceRestore = now.Sub(h.restoredAt)
	}
	return c
}

// transitionTotals rolls the per-key transition counters up per axis. Caller
// holds s.mu.
//
// Lateral moves are their own total and are in neither of the other two, which
// is §7.7.3's rule stated once more where it matters: an equal-score
// alternative is capacity churn, not a migration in either direction, and
// counting one as an improvement would clear a stuck-migration finding that
// nothing actually migrated out of.
func (s *Source) transitionTotals() map[leeway.AxisKey]struct{ improving, worsening, lateral int64 } {
	out := map[leeway.AxisKey]struct{ improving, worsening, lateral int64 }{}
	for key, n := range s.transitions {
		t := out[key.Axis]
		switch {
		case key.Lateral:
			t.lateral += n
		case !key.From.IsTier() || !key.To.IsTier():
			// A move in or out of unknown, unsatisfiable or off-axis. Neither
			// direction: the node's annotation arriving for the first time is
			// the commonest event on this counter and it is not a fallback.
		case key.To < key.From:
			t.improving += n
		case key.To > key.From:
			t.worsening += n
		}
		out[key.Axis] = t
	}
	return out
}

// thresholds returns the configured rule bounds, normalized.
//
// The two shares are pointers in Config rather than floats, because zero is a
// meaningful setting for both — it is how a rule is turned off — and a plain
// float could not distinguish "the operator asked for no ceiling" from "the
// operator said nothing". §7.7.5's "not all fallback is bad" is why that is
// worth a pointer: an estate whose rank 2 is the cheap family it was always
// meant to run on wants the ceiling off, not raised to something arbitrary.
func (s *Source) thresholds() leeway.RankThresholds {
	t := leeway.DefaultRankThresholds()
	if s.cfg.Rank0ShareFloor != nil {
		t.Rank0ShareFloor = *s.cfg.Rank0ShareFloor
	}
	if s.cfg.LastRankShareCeiling != nil {
		t.LastRankShareCeiling = *s.cfg.LastRankShareCeiling
	}
	if s.cfg.UnusedTierFor > 0 {
		t.UnusedTierFor = s.cfg.UnusedTierFor
	}
	if s.cfg.MigrationGrace > 0 {
		t.MigrationGrace = s.cfg.MigrationGrace
	}
	return t.Normalized()
}
