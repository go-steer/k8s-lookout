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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
		supervise(ctx, "prod-us", 0, restarts, run)
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
		supervise(ctx, "prod-eu", time.Second, restarts, run)
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
