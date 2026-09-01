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

// Issue #364 — a fatal source error wedged the process instead of
// exiting.
//
// runner.run reaches the watchboard join on two paths: ctx cancelled
// (SIGTERM), and sources.RunAll returning an error. RunAll cancels only
// its own CHILD context, so on the error path the runner's ctx is still
// live — and a join that waited for board.run to return waited for a
// cancellation that was never coming. The sentinel kept /healthz at 200
// with its diagnosis unprinted, which inverts §7.2: a source that
// cannot run must stop the process, not silence it.
//
// The seam is startBoard's returned wait func. It must STOP the board,
// not merely observe it, so the caller cannot get the ordering wrong on
// the path where nothing else has cancelled anything.

import (
	"context"
	"testing"
	"time"
)

// TestStartBoard_WaitReturnsOnALiveContext is the #364 regression: the
// parent ctx is never cancelled — the fatal-source path — and wait()
// must still return.
//
// Discriminator: with a wait that only observes board.run, this blocks
// until the test's timeout, exactly as the sentinel blocked until
// someone sent it a SIGTERM.
func TestStartBoard_WaitReturnsOnALiveContext(t *testing.T) {
	t.Parallel()

	sink := &slowSink{delay: 10 * time.Millisecond}
	board := newWatchboard(watchboardConfig{
		injector:      sink,
		metrics:       newMetrics(),
		cluster:       "prod-us-central1",
		batch:         100,
		flushInterval: time.Hour,
		rotateAfter:   200,
	})
	board.Add(context.Background(), warningSignal(1), 1)

	// No cancel, no deadline: the ctx a runner still holds when RunAll
	// hands back a fatal source error.
	wait := startBoard(context.Background(), board)

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		wait()
	}()

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("wait() did not return with the parent context still live — " +
			"the fatal-source path blocks on the watchboard join and the " +
			"process wedges instead of exiting (issue #364)")
	}

	// And #108's guarantee has to survive the change: stopping the board
	// still flushes what it was holding.
	if !sink.done.Load() {
		t.Fatalf("wait() returned before the final flush completed "+
			"(deliveries started=%d) — buffered warnings would be dropped "+
			"on the error path (issue #108)", sink.calls.Load())
	}
}
