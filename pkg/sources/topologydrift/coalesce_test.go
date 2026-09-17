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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

var (
	subA = leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "a"}
	subB = leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "b"}
)

// The timing tests below run the real clock with millisecond windows. The
// assertions are all one-sided — "fired at least once", "had not fired yet
// after a fraction of the window" — with an order of magnitude between the
// windows they compare, so a slow CI box makes them slower, not red.
const (
	tickWindow   = 30 * time.Millisecond
	tickRollout  = 60 * time.Millisecond
	tickMaxDelay = 150 * time.Millisecond
)

func TestCoalesceWindows_Normalise(t *testing.T) {
	tests := []struct {
		name string
		in   coalesceWindows
		want coalesceWindows
	}{
		{
			"zero takes the shipped defaults",
			coalesceWindows{},
			coalesceWindows{DefaultCoalesceWindow, DefaultRolloutCoalesceWindow, DefaultMaxCoalesceDelay},
		},
		{
			"negatives are treated as unset",
			coalesceWindows{window: -1, rollout: -1, maxDelay: -1},
			coalesceWindows{DefaultCoalesceWindow, DefaultRolloutCoalesceWindow, DefaultMaxCoalesceDelay},
		},
		{
			// Configured backwards: the rollout window is meant to be the wider
			// of the two, so a narrower one is raised rather than honoured.
			"a rollout window below the base window is raised to it",
			coalesceWindows{window: 10 * time.Second, rollout: time.Second, maxDelay: time.Minute},
			coalesceWindows{window: 10 * time.Second, rollout: 10 * time.Second, maxDelay: time.Minute},
		},
		{
			"a cap below the windows it caps is raised",
			coalesceWindows{window: time.Second, rollout: 5 * time.Second, maxDelay: time.Second},
			coalesceWindows{window: time.Second, rollout: 5 * time.Second, maxDelay: 5 * time.Second},
		},
		{
			"a valid configuration is left alone",
			coalesceWindows{window: time.Second, rollout: 5 * time.Second, maxDelay: time.Minute},
			coalesceWindows{window: time.Second, rollout: 5 * time.Second, maxDelay: time.Minute},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.in.normalise(); got != tc.want {
				t.Errorf("normalise() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestCoalesceWindows_Reschedule(t *testing.T) {
	w := coalesceWindows{window: 2 * time.Second, rollout: 15 * time.Second, maxDelay: 60 * time.Second}
	base := time.Unix(1_700_000_000, 0)

	t.Run("the first enqueue waits the base window", func(t *testing.T) {
		var e pendingEntry
		w.reschedule(&e, base)
		if want := base.Add(2 * time.Second); !e.readyAt.Equal(want) {
			t.Errorf("readyAt = %v, want %v", e.readyAt, want)
		}
		if !e.first.Equal(base) || e.hits != 1 {
			t.Errorf("first = %v hits = %d, want %v and 1", e.first, e.hits, base)
		}
	})

	t.Run("a second enqueue widens to the rollout window", func(t *testing.T) {
		var e pendingEntry
		w.reschedule(&e, base)
		w.reschedule(&e, base.Add(time.Second))
		if want := base.Add(16 * time.Second); !e.readyAt.Equal(want) {
			t.Errorf("readyAt = %v, want %v", e.readyAt, want)
		}
		if e.hits != 2 {
			t.Errorf("hits = %d, want 2", e.hits)
		}
	})

	t.Run("the widening is clamped to maxDelay from the first enqueue", func(t *testing.T) {
		var e pendingEntry
		w.reschedule(&e, base)
		// Churn all the way to the cap: without the clamp, each enqueue would
		// push readyAt another 15s out and the subject would never evaluate.
		for i := 1; i <= 100; i++ {
			w.reschedule(&e, base.Add(time.Duration(i)*time.Second))
		}
		if want := base.Add(60 * time.Second); !e.readyAt.Equal(want) {
			t.Errorf("readyAt = %v, want the maxDelay cap %v", e.readyAt, want)
		}
	})

	t.Run("a re-enqueue can never pull readyAt earlier", func(t *testing.T) {
		var e pendingEntry
		w.reschedule(&e, base)
		w.reschedule(&e, base.Add(time.Second)) // widens to base+16s
		before := e.readyAt
		// An enqueue so late that now+rollout still lands before the pending
		// readyAt cannot happen with these windows, so force the shape by
		// rescheduling at the same instant: next = base+16s, not earlier.
		w.reschedule(&e, base.Add(time.Second))
		if e.readyAt.Before(before) {
			t.Errorf("readyAt moved earlier: %v -> %v", before, e.readyAt)
		}
	})

	t.Run("a burst past the cap keeps readyAt pinned, not receding", func(t *testing.T) {
		// Once clamped, every further enqueue computes the same cap. The
		// max-of guard is what stops a stale computation from moving it.
		var e pendingEntry
		w.reschedule(&e, base)
		w.reschedule(&e, base.Add(59*time.Second)) // clamped to base+60s
		w.reschedule(&e, base.Add(59*time.Second))
		if want := base.Add(60 * time.Second); !e.readyAt.Equal(want) {
			t.Errorf("readyAt = %v, want %v", e.readyAt, want)
		}
	})
}

// recorder collects evaluated subjects.
type recorder struct {
	mu   sync.Mutex
	seen []leeway.SubjectRef
	// block, when set, is held for this long inside the callback.
	block time.Duration
}

func (r *recorder) evaluate(_ context.Context, sub leeway.SubjectRef) {
	if r.block > 0 {
		time.Sleep(r.block)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, sub)
}

func (r *recorder) count(sub leeway.SubjectRef) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, s := range r.seen {
		if s == sub {
			n++
		}
	}
	return n
}

// runQueue starts a coalescer and stops it when the test ends.
func runQueue(t *testing.T, opts CoalesceOptions) *coalescer {
	t.Helper()
	c := newQueue(t, opts)
	startQueue(t, c)
	return c
}

// newQueue builds a coalescer with the test windows filled in but does not
// start it, so a test can stage a burst before the evaluator can see any of it.
func newQueue(t *testing.T, opts CoalesceOptions) *coalescer {
	t.Helper()
	if opts.Window == 0 {
		opts.Window = tickWindow
	}
	if opts.RolloutWindow == 0 {
		opts.RolloutWindow = tickRollout
	}
	if opts.MaxDelay == 0 {
		opts.MaxDelay = tickMaxDelay
	}
	return newCoalescer(opts)
}

// startQueue runs c until the test finishes.
func startQueue(t *testing.T, c *coalescer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("coalescer.Run did not return after cancellation")
		}
	})
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestCoalescer_CollapsesABurstIntoOneEvaluation(t *testing.T) {
	// The §6.4 headline: one Deployment churning 200 pods must not become 200
	// evaluations.
	rec := &recorder{}
	c := runQueue(t, CoalesceOptions{Evaluate: rec.evaluate})

	for i := 0; i < 200; i++ {
		c.Enqueue(subA)
	}
	waitFor(t, "the burst to evaluate", func() bool { return rec.count(subA) > 0 })
	time.Sleep(2 * tickRollout) // let any straggler through before asserting

	if got := rec.count(subA); got != 1 {
		t.Errorf("200 enqueues produced %d evaluations, want 1", got)
	}
	enq, eval := c.stats()
	if enq != 200 || eval != 1 {
		t.Errorf("stats = (%d enqueued, %d evaluated), want (200, 1)", enq, eval)
	}
}

func TestCoalescer_IndependentSubjectsDoNotCollapseTogether(t *testing.T) {
	rec := &recorder{}
	c := runQueue(t, CoalesceOptions{Evaluate: rec.evaluate})

	c.Enqueue(subA)
	c.Enqueue(subB)
	waitFor(t, "both subjects to evaluate", func() bool {
		return rec.count(subA) == 1 && rec.count(subB) == 1
	})
}

func TestCoalescer_ChurnIsCappedByMaxDelay(t *testing.T) {
	// A subject re-enqueued faster than the rollout window would, without the
	// cap, never be evaluated at all. This is the test that would fail if the
	// clamp in reschedule were dropped.
	rec := &recorder{}
	c := runQueue(t, CoalesceOptions{Evaluate: rec.evaluate})

	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				c.Enqueue(subA)
				time.Sleep(tickWindow / 4)
			}
		}
	}()
	defer close(stop)

	waitFor(t, "the capped evaluation", func() bool { return rec.count(subA) > 0 })
}

func TestCoalescer_PendingReflectsTheQueue(t *testing.T) {
	c := newCoalescer(CoalesceOptions{Window: time.Hour})
	if got := c.Pending(); got != 0 {
		t.Fatalf("Pending() = %d on a fresh queue, want 0", got)
	}
	c.Enqueue(subA)
	c.Enqueue(subA)
	c.Enqueue(subB)
	if got := c.Pending(); got != 2 {
		t.Errorf("Pending() = %d after two subjects (three enqueues), want 2", got)
	}
}

func TestCoalescer_TakeReadyDiscardsSupersededTimers(t *testing.T) {
	// Rescheduling leaves the old heap item behind rather than deleting it.
	// The pop must skip it and honour the live readyAt, or a widened subject
	// would fire on its original schedule and the widening would be a no-op.
	c := newCoalescer(CoalesceOptions{
		Window: 10 * time.Millisecond, RolloutWindow: time.Hour, MaxDelay: 2 * time.Hour,
	})
	c.Enqueue(subA)
	c.Enqueue(subA) // widens to ~now+1h, superseding the first heap item
	if c.timers.Len() != 2 {
		t.Fatalf("timers.Len() = %d, want the superseded item still present", c.timers.Len())
	}

	_, ok, wait := c.takeReady(time.Now().Add(time.Second))
	if ok {
		t.Fatal("takeReady honoured a superseded timer")
	}
	if wait < 50*time.Minute {
		t.Errorf("wait = %v, want roughly the widened hour", wait)
	}
	if c.timers.Len() != 1 {
		t.Errorf("timers.Len() = %d, want the stale item dropped", c.timers.Len())
	}
}

func TestCoalescer_TakeReadyOnAnEmptyQueue(t *testing.T) {
	c := newCoalescer(CoalesceOptions{})
	sub, ok, wait := c.takeReady(time.Now())
	if ok || wait != 0 || sub != (leeway.SubjectRef{}) {
		t.Errorf("takeReady on an empty queue = (%v, %v, %v), want the sleep-until-woken answer", sub, ok, wait)
	}

	// An entry whose heap items are all stale — the subject was evaluated
	// while items remained — drains to the same answer.
	c.Enqueue(subA)
	c.mu.Lock()
	delete(c.pending, subA)
	c.mu.Unlock()
	if _, ok, wait := c.takeReady(time.Now()); ok || wait != 0 {
		t.Errorf("takeReady over stale-only timers = (%v, %v), want (false, 0)", ok, wait)
	}
	if c.timers.Len() != 0 {
		t.Errorf("timers.Len() = %d, want the stale items drained", c.timers.Len())
	}
}

func TestCoalescer_NilEvaluateStillDrains(t *testing.T) {
	// Phase 2's shape: the queue does its bookkeeping and drops the result.
	c := runQueue(t, CoalesceOptions{})
	c.Enqueue(subA)
	waitFor(t, "the queue to drain with no evaluator", func() bool { return c.Pending() == 0 })
	if _, eval := c.stats(); eval != 1 {
		t.Errorf("evaluated = %d, want 1", eval)
	}
}

func TestCoalescer_RunReturnsOnCancelWithABacklog(t *testing.T) {
	// Cancellation must win over a backlog: the subjects left pending point
	// into indexes that are about to stop being updated.
	rec := &recorder{block: 5 * time.Millisecond}
	c := newCoalescer(CoalesceOptions{Window: time.Millisecond, Evaluate: rec.evaluate})
	for i := 0; i < 500; i++ {
		c.Enqueue(leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: string(rune('a' + i%26))})
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.Run(ctx) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return with a backlog pending")
	}
}

func TestCoalescer_ScaleOfARelist(t *testing.T) {
	// The shape a relist produces: every subject enqueued at once, several
	// times over. This is the case the timer heap exists for — the map scan it
	// replaced is O(pending) per evaluation, so this test would take quadratic
	// time rather than fail. Kept modest because what is being checked is the
	// bookkeeping, not the wall clock.
	//
	// The whole burst is staged before the queue starts. Enqueuing 10,000
	// subjects takes longer than any window short enough to keep the test
	// quick, so with a running evaluator the collapse would be a race against
	// the enqueue loop rather than a property of the queue — and on a loaded
	// box the loop loses. Staged, the collapse is arithmetic: 10,000 enqueues
	// of 2,000 subjects owe exactly 2,000 evaluations. The re-enqueue-while-
	// pending path that this deliberately excludes is covered above by
	// TestCoalescer_CollapsesABurstIntoOneEvaluation.
	const subjects, rounds = 2000, 5

	rec := &recorder{}
	c := newQueue(t, CoalesceOptions{
		Window: time.Millisecond, RolloutWindow: 2 * time.Millisecond, MaxDelay: 20 * time.Millisecond,
		Evaluate: rec.evaluate,
	})

	refs := make([]leeway.SubjectRef, subjects)
	for i := range refs {
		refs[i] = leeway.SubjectRef{
			Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "app-" + strconv.Itoa(i),
		}
	}
	for r := 0; r < rounds; r++ {
		for _, sub := range refs {
			c.Enqueue(sub)
		}
	}
	if got := c.Pending(); got != subjects {
		t.Fatalf("pending = %d after %d enqueues, want one entry per subject (%d)",
			got, subjects*rounds, subjects)
	}

	startQueue(t, c)
	waitFor(t, "the relist to drain", func() bool { return c.Pending() == 0 })
	enq, eval := c.stats()
	if enq != subjects*rounds {
		t.Errorf("enqueued = %d, want %d", enq, subjects*rounds)
	}
	if eval != subjects {
		t.Errorf("evaluated = %d for %d subjects over %d rounds; the burst did not coalesce",
			eval, subjects, rounds)
	}
	for _, sub := range refs {
		if rec.count(sub) == 0 {
			t.Fatalf("%s was never evaluated", sub)
		}
	}
}

func TestTimerHeap_OrdersByReadyAtThenSequence(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	h := &timerHeap{}
	heap.Push(h, heapItem{sub: subB, readyAt: base.Add(time.Second), seq: 1})
	heap.Push(h, heapItem{sub: subA, readyAt: base, seq: 3})
	heap.Push(h, heapItem{sub: subB, readyAt: base, seq: 2})

	want := []heapItem{
		{sub: subB, readyAt: base, seq: 2},
		{sub: subA, readyAt: base, seq: 3},
		{sub: subB, readyAt: base.Add(time.Second), seq: 1},
	}
	for i, w := range want {
		got := heap.Pop(h).(heapItem)
		if got != w {
			t.Errorf("pop %d = %+v, want %+v", i, got, w)
		}
	}
}
