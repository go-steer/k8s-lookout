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

// Issue #108 — watchboard shutdown flush runs in an unjoined goroutine,
// so buffered warnings are lost on SIGTERM.
//
// wiring.go launches `go disp.board.run(ctx)` fire-and-forget. On
// ctx.Done(), watchboard.run does a FINAL best-effort FlushNow before
// returning — but nothing waits for that goroutine. The wiring Run
// function unblocks from sources.RunAll(ctx) the moment ctx cancels and
// returns, so the process can exit before the shutdown FlushNow (which
// makes a real network inject) completes → buffered warnings dropped.
//
// ---------------------------------------------------------------------
// startBoard contract (the production seam this test drives).
//
// The coder is expected to replace the bare `go disp.board.run(ctx)`
// with a joinable lifecycle helper:
//
//	// startBoard runs the watchboard flush loop in a goroutine and
//	// returns a wait func that blocks until run has returned — i.e.
//	// until the final shutdown FlushNow has completed.
//	func startBoard(ctx context.Context, board *watchboard) (wait func())
//
// wiring.go then holds the returned wait func and, after RunAll returns
// (after the dedup snapshot, before `return err`), calls it so shutdown
// blocks on the board's final flush.
//
// Contract:
//   - Runs board.run(ctx) in its own goroutine.
//   - Returns a wait func that BLOCKS until board.run has returned —
//     which, per run's shutdown branch, is after the final FlushNow has
//     finished delivering. When wait() returns, the shutdown flush is
//     done (or the 3s FlushNow timeout elapsed).
//   - wait is safe to call exactly once from the shutdown path; it is
//     never nil (the caller in wiring.go guards nil today only because
//     the board is optional — an absent board yields a no-op wait).
//
// A fire-and-forget `go board.run(ctx)` with a wait func that returns
// immediately (or a nil/no-op wait) satisfies "runs in a goroutine" but
// NOT the join guarantee — so this test fails against it and passes
// only when wait() truly blocks on run's return.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// slowSink is an inject.Sink whose delivery takes measurable wall-clock
// time and records, atomically, that it COMPLETED. It deliberately does
// NOT implement inject.SessionOpener, so a first flush (sid=="") takes
// the stateless OpenIncident path — the call the shutdown flush makes.
type slowSink struct {
	delay time.Duration
	calls atomic.Int32 // deliveries started
	done  atomic.Bool  // set true AFTER a delivery's delay elapses
}

func (s *slowSink) OpenIncident(ctx context.Context, payload any) (string, error) {
	s.calls.Add(1)
	time.Sleep(s.delay)
	s.done.Store(true)
	return "sess-slow", nil
}

func (s *slowSink) Append(ctx context.Context, id string, payload any) error {
	s.calls.Add(1)
	time.Sleep(s.delay)
	s.done.Store(true)
	return nil
}

var _ inject.Sink = (*slowSink)(nil)

// TestStartBoard_ShutdownFlushIsJoined reproduces issue #108: the
// watchboard's shutdown FlushNow must complete before the lifecycle
// wait func returns, so a terminating sentinel does not drop buffered
// warnings.
//
// Deterministic discriminator: the sink's delivery sleeps 50ms and only
// then records completion. A correctly joined wait() blocks on run's
// return (which happens after FlushNow → the 50ms delivery) so done is
// set; a fire-and-forget stub returns wait() immediately, well before
// the 50ms delivery finishes, so done is still false.
//
// Fails to COMPILE until the coder adds startBoard; then fails the
// assertion against any wait that does not truly join; passes only when
// wait() blocks until board.run returns.
func TestStartBoard_ShutdownFlushIsJoined(t *testing.T) {
	t.Parallel()

	sink := &slowSink{delay: 50 * time.Millisecond}
	board := newWatchboard(watchboardConfig{
		injector:      sink,
		metrics:       newMetrics(),
		cluster:       "prod-us-central1",
		batch:         100,       // high: Add buffers without an eager flush
		flushInterval: time.Hour, // long: the ticker never age-flushes
		rotateAfter:   200,
	})

	// Buffer one warning so the shutdown FlushNow has something to
	// deliver (warningSignal is defined in watchboard_dispatch_test.go).
	board.Add(context.Background(), warningSignal(1), 1)

	ctx, cancel := context.WithCancel(context.Background())
	wait := startBoard(ctx, board)

	cancel() // SIGTERM analogue: trigger run's shutdown branch.
	wait()   // must block until the final FlushNow has completed.

	if !sink.done.Load() {
		t.Fatalf("wait() returned before the shutdown flush completed "+
			"(delivery started=%d, completed=%v) — board.run was not "+
			"joined, buffered warnings would be lost on SIGTERM (issue #108)",
			sink.calls.Load(), sink.done.Load())
	}
	if got := sink.calls.Load(); got == 0 {
		t.Fatalf("shutdown flush never delivered the buffered warning "+
			"(deliveries=%d) — nothing flushed on shutdown", got)
	}
}
