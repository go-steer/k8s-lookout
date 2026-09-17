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
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// Coalescing defaults (§6.4).
const (
	// DefaultCoalesceWindow is how long a quiet subject waits before it is
	// evaluated. Two seconds is long enough to absorb the burst a single
	// controller action produces and short enough that a human watching a
	// dashboard does not notice the lag.
	DefaultCoalesceWindow = 2 * time.Second

	// DefaultRolloutCoalesceWindow is the widened window used once a subject
	// turns out to be churning rather than settling.
	DefaultRolloutCoalesceWindow = 15 * time.Second

	// DefaultMaxCoalesceDelay bounds the widening. Without it a subject whose
	// pods never stop moving — a Deployment stuck in a crash-restart loop, a
	// Job queue with constant turnover — would have its evaluation pushed out
	// forever, which is precisely the subject most worth evaluating.
	DefaultMaxCoalesceDelay = 60 * time.Second
)

// coalesceWindows is the timing policy of §6.4, separated from the goroutine
// that applies it so the decisions can be tested without a clock.
type coalesceWindows struct {
	window   time.Duration
	rollout  time.Duration
	maxDelay time.Duration
}

// normalise fills zero values with the shipped defaults and repairs a
// configuration that cannot be satisfied as written.
func (w coalesceWindows) normalise() coalesceWindows {
	if w.window <= 0 {
		w.window = DefaultCoalesceWindow
	}
	if w.rollout <= 0 {
		w.rollout = DefaultRolloutCoalesceWindow
	}
	if w.maxDelay <= 0 {
		w.maxDelay = DefaultMaxCoalesceDelay
	}
	// A cap below the windows it caps would make every re-enqueue fire
	// immediately, turning the queue into a pass-through. Raise it rather than
	// reject the config: the cap is a safety bound, not an intent.
	if w.rollout < w.window {
		w.rollout = w.window
	}
	if w.maxDelay < w.rollout {
		w.maxDelay = w.rollout
	}
	return w
}

// pendingEntry is one subject waiting to be evaluated.
type pendingEntry struct {
	// first is when this burst started, and is what maxDelay is measured from.
	first time.Time
	// readyAt is when the subject becomes eligible for evaluation.
	readyAt time.Time
	// hits counts enqueues in this burst. Carried for the metric and because
	// hits == 0 is how a fresh entry is recognised.
	hits int
}

// timerHeap orders subjects by readyAt.
//
// It exists because the alternative — scanning the pending map for the ripest
// subject — is O(pending) per evaluation, and pending is the whole subject set
// after a relist. At the 20k subjects of §6.6's baseline row that scan turns
// one relist into 400M map iterations; the heap turns it into 20k pops.
//
// Entries are stale-tolerant rather than updated in place: rescheduling pushes
// a second item for the same subject and the pop discards any item whose
// readyAt no longer matches the live entry. Deleting from the middle of a heap
// would need an index per subject kept correct through every swap, which is
// more moving parts than the discard costs.
type timerHeap []heapItem

type heapItem struct {
	sub     leeway.SubjectRef
	readyAt time.Time
	seq     int64
}

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if h[i].readyAt.Equal(h[j].readyAt) {
		return h[i].seq < h[j].seq // FIFO among equals, so nothing starves
	}
	return h[i].readyAt.Before(h[j].readyAt)
}
func (h timerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *timerHeap) Push(x any)   { *h = append(*h, x.(heapItem)) }
func (h *timerHeap) Pop() (x any) { old := *h; x, *h = old[len(old)-1], old[:len(old)-1]; return x }

// reschedule folds an enqueue at now into e.
//
// The first enqueue of a burst waits the ordinary window. A second one says
// the subject is still moving, which is what a rollout looks like from in here
// — there is no need to ask the rollout source, because a Deployment replacing
// 200 pods *is* 200 enqueues — so the window widens. The widening is measured
// from now and clamped to maxDelay after the first enqueue, and it can only
// push readyAt later, never pull it earlier: a burst must not be able to
// starve itself by arriving in a tight loop, nor to shorten a wait it already
// earned.
func (w coalesceWindows) reschedule(e *pendingEntry, now time.Time) {
	if e.hits == 0 {
		e.first = now
		e.readyAt = now.Add(w.window)
		e.hits = 1
		return
	}
	e.hits++

	next := now.Add(w.rollout)
	if limit := e.first.Add(w.maxDelay); next.After(limit) {
		next = limit
	}
	if next.After(e.readyAt) {
		e.readyAt = next
	}
}

// CoalesceOptions configures the §6.4 evaluation queue.
type CoalesceOptions struct {
	// Window is the quiet-subject delay. Zero takes DefaultCoalesceWindow.
	Window time.Duration

	// RolloutWindow is the widened delay applied once a subject re-enqueues
	// while already pending. Zero takes DefaultRolloutCoalesceWindow.
	RolloutWindow time.Duration

	// MaxDelay bounds the widening, measured from the first enqueue of a
	// burst. Zero takes DefaultMaxCoalesceDelay.
	MaxDelay time.Duration

	// Evaluate is called once per coalesced burst. Nil means the queue still
	// does its bookkeeping and drops the result, which is Phase 2's behaviour:
	// there is nothing to evaluate until intent inference lands in Phase 3, but
	// running the queue now means the scale properties are exercised by the
	// same events that exercise the counters.
	Evaluate func(context.Context, leeway.SubjectRef)
}

// coalescer collapses a burst of subject enqueues into one evaluation (§6.4).
//
// Evaluation is serial by construction: one goroutine owns both the timing and
// the callback. That is not a placeholder for a worker pool. A subject's
// counters are read by the evaluator and rewritten by §6.5's verifier, and
// keeping evaluation single-threaded means those two never need to agree about
// a lock they do not share. Raising concurrency is a Phase 3 decision to be
// made against a measurement, not assumed here.
//
// Not built on k8s.io/client-go/util/workqueue, for one concrete reason: its
// delaying queue takes the *earliest* readyAt when a waiting key is re-added,
// so AddAfter can shorten a pending delay but never widen one. §6.4 needs
// exactly the widening.
type coalescer struct {
	windows  coalesceWindows
	evaluate func(context.Context, leeway.SubjectRef)

	mu      sync.Mutex
	pending map[leeway.SubjectRef]*pendingEntry
	timers  timerHeap
	seq     int64
	// stats accumulate for the source's metrics; see stats.
	enqueued  int64
	evaluated int64

	// wake carries "the pending set changed" to the run loop. Capacity 1 with
	// a non-blocking send: a missed signal is impossible because the loop
	// recomputes the whole pending set every pass, and a queued one costs a
	// single extra pass.
	wake chan struct{}
}

// newCoalescer returns a coalescer. Call Run to start it.
func newCoalescer(opts CoalesceOptions) *coalescer {
	return &coalescer{
		windows: coalesceWindows{
			window:   opts.Window,
			rollout:  opts.RolloutWindow,
			maxDelay: opts.MaxDelay,
		}.normalise(),
		evaluate: opts.Evaluate,
		pending:  make(map[leeway.SubjectRef]*pendingEntry),
		wake:     make(chan struct{}, 1),
	}
}

// Enqueue schedules sub for evaluation. Safe to call from any goroutine,
// including before Run and after it returns; it never blocks.
func (c *coalescer) Enqueue(sub leeway.SubjectRef) {
	c.mu.Lock()
	e := c.pending[sub]
	if e == nil {
		e = &pendingEntry{}
		c.pending[sub] = e
	}
	before := e.readyAt
	c.windows.reschedule(e, time.Now())
	c.enqueued++
	if !e.readyAt.Equal(before) {
		c.seq++
		heap.Push(&c.timers, heapItem{sub: sub, readyAt: e.readyAt, seq: c.seq})
	}
	c.mu.Unlock()

	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Pending reports how many subjects are waiting.
func (c *coalescer) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// stats reports the running enqueue and evaluation totals.
func (c *coalescer) stats() (enqueued, evaluated int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enqueued, c.evaluated
}

// takeReady removes and returns the ripest ready subject, or reports how long
// to wait for the earliest one. ok is false when nothing is ready; wait is
// zero when nothing is pending at all, meaning "sleep until woken".
func (c *coalescer) takeReady(now time.Time) (sub leeway.SubjectRef, ok bool, wait time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for c.timers.Len() > 0 {
		item := c.timers[0]
		e := c.pending[item.sub]
		if e == nil || !e.readyAt.Equal(item.readyAt) {
			// Superseded by a later reschedule, or already evaluated.
			heap.Pop(&c.timers)
			continue
		}
		if e.readyAt.After(now) {
			return leeway.SubjectRef{}, false, e.readyAt.Sub(now)
		}
		heap.Pop(&c.timers)
		delete(c.pending, item.sub)
		c.evaluated++
		return item.sub, true, 0
	}
	return leeway.SubjectRef{}, false, 0
}

// Run drives the queue until ctx is cancelled.
//
// Subjects still pending at cancellation are dropped rather than flushed. They
// are pointers into indexes that are about to stop being updated, and the next
// process rebuilds the whole set from its initial LIST.
func (c *coalescer) Run(ctx context.Context) {
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		sub, ok, wait := c.takeReady(time.Now())
		if ok {
			if c.evaluate != nil {
				c.evaluate(ctx, sub)
			}
			// Check for cancellation between evaluations so a backlog cannot
			// outlive the context.
			if ctx.Err() != nil {
				return
			}
			continue
		}

		var fire <-chan time.Time
		if wait > 0 {
			timer.Reset(wait)
			fire = timer.C
		}
		select {
		case <-ctx.Done():
			if fire != nil && !timer.Stop() {
				<-timer.C
			}
			return
		case <-c.wake:
			// A new or rescheduled subject; recompute the wait. Draining the
			// timer keeps the next Reset honest.
			if fire != nil && !timer.Stop() {
				<-timer.C
			}
		case <-fire:
		}
	}
}
