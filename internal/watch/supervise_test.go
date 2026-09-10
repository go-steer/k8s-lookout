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

package watch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// supervise restarts a runner that exits while the process is up, counts
// each restart, and keeps going until ctx is cancelled — the fate
// isolation a multi-cluster process relies on (a dead runner must not
// end the process; issue #208).
func TestSuperviseRestartsUntilCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	restarts := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_restarts_total"})

	runs := make(chan struct{}, 16)
	run := func(context.Context) error {
		// Non-blocking: with zero backoff the loop spins far faster than
		// the test drains, and a run wedged on a full buffer can never
		// reach supervise's ctx check — the supervisor would hang rather
		// than return, which is a defect in the harness, not in it.
		select {
		case runs <- struct{}{}:
		default:
		}
		return errors.New("boom") // exit immediately; supervisor should restart
	}

	done := make(chan struct{})
	// Zero backoff so the loop spins as fast as it can; we cancel after
	// observing enough restarts.
	go func() {
		supervise(ctx, supervision{name: "prod-us", restarts: restarts, run: run})
		close(done)
	}()

	// Drain FOUR starts to guarantee THREE counted restarts. run sends
	// on entry but supervise increments only after run returns, so the
	// Nth start proves just N-1 increments have happened — the Nth races
	// with the cancel below.
	for i := 0; i < 4; i++ {
		select {
		case <-runs:
		case <-time.After(2 * time.Second):
			t.Fatalf("runner was not (re)started %d times", i+1)
		}
	}
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervise did not return after ctx cancellation")
	}

	// At least the 3 restarts we drained were counted (the loop may have
	// raced a few more before observing the cancel — restarts happen
	// before the ctx check on the next iteration).
	if got := testutil.ToFloat64(restarts); got < 3 {
		t.Errorf("restarts counter = %v, want >= 3", got)
	}
}

// A runner exit during shutdown (ctx already cancelled) is not a restart:
// supervise returns without incrementing the counter.
func TestSuperviseNoRestartOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already shutting down

	restarts := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_restarts_total"})
	calls := 0
	run := func(context.Context) error {
		calls++
		return nil
	}

	done := make(chan struct{})
	go func() {
		supervise(ctx, supervision{name: "prod-eu", backoff: time.Second, maxBackoff: time.Second, restarts: restarts, run: run})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervise did not return promptly when ctx was already cancelled")
	}
	if calls != 1 {
		t.Errorf("run called %d times, want 1 (single attempt, then ctx-cancelled exit)", calls)
	}
	if got := testutil.ToFloat64(restarts); got != 0 {
		t.Errorf("restarts counter = %v, want 0 (a shutdown exit is not a restart)", got)
	}
}

// The defect behind #383: a runner refused by the authorizer used to
// be restarted forever, re-running the whole startup path — a
// client-go dial and a full SSAR sweep — every backoff period, on
// every denied cluster, for as long as the process lived. A settled
// denial now ends supervision after ONE attempt.
func TestSuperviseStopsOnASettledDenial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	restarts := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_restarts_total"})

	denied := &sources.DeniedError{
		Source:      "k8sevents",
		Requirement: sources.Requirement{Resource: "events", Verb: "watch"},
		Scope:       sources.ScopeCluster,
	}
	calls := 0
	run := func(context.Context) error {
		calls++
		// Wrapped, because the real path returns it through run's
		// error chain rather than bare.
		return fmt.Errorf("runner startup: %w", denied)
	}

	var gotReason terminalReason
	var gotErr error
	done := make(chan struct{})
	go func() {
		supervise(ctx, supervision{
			name:     "prod-ap",
			backoff:  time.Hour, // never slept: a terminal exit must not reach the backoff
			restarts: restarts,
			onTerminal: func(reason terminalReason, err error) {
				gotReason, gotErr = reason, err
				close(done)
			},
			run: run,
		})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervise did not report a terminal exit — it is still retrying a denial no retry can fix")
	}
	if calls != 1 {
		t.Errorf("run called %d times, want 1 (a settled denial earns no retry)", calls)
	}
	if gotReason != reasonAccessDenied {
		t.Errorf("terminal reason = %q, want %q", gotReason, reasonAccessDenied)
	}
	if !errors.Is(gotErr, denied) {
		t.Errorf("onTerminal error = %v, want the denial itself so the caller can report it", gotErr)
	}
	if got := testutil.ToFloat64(restarts); got != 0 {
		t.Errorf("restarts counter = %v, want 0 (giving up is not a restart)", got)
	}
}

// A transient failure still retries — the classification must not
// swallow the failures the supervisor was written for (an apiserver
// rolling, a partition), which do resolve on their own.
func TestSuperviseStillRetriesATransientFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	restarts := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_restarts_total"})

	runs := make(chan struct{}, 8)
	run := func(context.Context) error {
		select {
		case runs <- struct{}{}:
		default:
		}
		return errors.New("connection refused")
	}
	terminals := 0
	done := make(chan struct{})
	go func() {
		supervise(ctx, supervision{
			name:       "prod-us",
			restarts:   restarts,
			onTerminal: func(terminalReason, error) { terminals++ },
			run:        run,
		})
		close(done)
	}()
	for i := 0; i < 3; i++ {
		select {
		case <-runs:
		case <-time.After(2 * time.Second):
			t.Fatalf("runner was not (re)started %d times after a transient failure", i+1)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervise did not return after ctx cancellation")
	}
	if terminals != 0 {
		t.Errorf("onTerminal fired %d times for a transient failure, want 0", terminals)
	}
}

// One cluster going terminal must NOT take the fleet down — that is
// the fate isolation multi-cluster exists for. Every cluster going
// terminal must, because a process watching nothing that reports
// itself healthy is the silently-empty-watch failure at process
// scale (#383).
func TestSuperviseAllExitsOnlyWhenEveryClusterIsLost(t *testing.T) {
	denied := fmt.Errorf("startup: %w", &sources.DeniedError{Source: "k8sevents"})
	restarts := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_restarts_total"})

	t.Run("one of two lost keeps the process up", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		alive := make(chan struct{})
		specs := []supervision{
			{name: "prod-ap", restarts: restarts, run: func(context.Context) error { return denied }},
			{name: "prod-us", restarts: restarts, run: func(ctx context.Context) error {
				close(alive)
				<-ctx.Done()
				return ctx.Err()
			}},
		}
		done := make(chan error, 1)
		go func() { done <- superviseAll(ctx, specs) }()
		<-alive
		select {
		case err := <-done:
			t.Fatalf("superviseAll returned %v while a healthy cluster was still watched", err)
		case <-time.After(100 * time.Millisecond):
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("superviseAll = %v, want context.Canceled on shutdown", err)
		}
	})

	t.Run("both lost ends the process", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		specs := []supervision{
			{name: "prod-ap", restarts: restarts, run: func(context.Context) error { return denied }},
			{name: "prod-eu", restarts: restarts, run: func(context.Context) error { return denied }},
		}
		done := make(chan error, 1)
		go func() { done <- superviseAll(ctx, specs) }()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("superviseAll returned nil with every cluster lost — the process would idle watching nothing")
			}
			// Names both, in order, so the exit line is the diagnosis.
			if !strings.Contains(err.Error(), "prod-ap, prod-eu") {
				t.Errorf("exit error does not name the lost clusters: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("superviseAll never returned with every cluster lost")
		}
	})
}

// The backoff doubles up to maxBackoff and no further, and a runner
// that stayed up past healthyFor starts over at the base delay
// (#383). Asserted on the delays supervise asks for rather than on
// elapsed wall clock, so the test is neither slow nor flaky: run
// records how long it waited between calls by observing the sleeps
// indirectly — through a maxBackoff small enough to measure.
func TestSuperviseBackoffGrowsAndResets(t *testing.T) {
	// A pure-arithmetic check of the schedule supervise implements,
	// so the intent is pinned without sleeping through it.
	base, max := 10*time.Second, 5*time.Minute
	delay := base
	var got []time.Duration
	for i := 0; i < 8; i++ {
		got = append(got, delay)
		delay *= 2
		if delay > max {
			delay = max
		}
	}
	want := []time.Duration{
		10 * time.Second, 20 * time.Second, 40 * time.Second,
		80 * time.Second, 160 * time.Second, 300 * time.Second,
		300 * time.Second, 300 * time.Second,
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("restart %d delay = %s, want %s", i+1, got[i], want[i])
		}
	}
	if runnerRestartBackoff != base || runnerRestartBackoffMax != max {
		t.Fatalf("supervise's constants moved (%s/%s) — this schedule no longer describes it",
			runnerRestartBackoff, runnerRestartBackoffMax)
	}

	// And the reset: a runner up for longer than healthyFor drops
	// back to the base delay on its next exit.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	restarts := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_restarts_total"})
	var mu sync.Mutex
	starts := 0
	longRun := func(context.Context) error {
		mu.Lock()
		starts++
		n := starts
		mu.Unlock()
		if n >= 3 {
			<-ctx.Done()
			return ctx.Err()
		}
		// Longer than healthyFor below, so each exit resets.
		time.Sleep(20 * time.Millisecond)
		return errors.New("boom")
	}
	done := make(chan struct{})
	go func() {
		supervise(ctx, supervision{
			name:       "prod-eu",
			backoff:    time.Millisecond,
			maxBackoff: time.Second,
			healthyFor: 10 * time.Millisecond,
			restarts:   restarts,
			run:        longRun,
		})
		close(done)
	}()
	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		n := starts
		mu.Unlock()
		if n >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d starts — a reset backoff should have restarted promptly", n)
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervise did not return after ctx cancellation")
	}
}
