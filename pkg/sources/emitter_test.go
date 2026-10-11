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

package sources

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

func TestAsyncEmitter_SlowSinkNeverBlocksProducersAndFlushesOnClose(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	unblockSink := make(chan struct{})
	firstEntered := make(chan struct{})
	var firstOnce sync.Once

	var mu sync.Mutex
	var got []string

	ae := NewAsyncEmitter(ctx, func(sig Signal) {
		firstOnce.Do(func() { close(firstEntered) })
		<-unblockSink
		mu.Lock()
		got = append(got, sig.Name)
		mu.Unlock()
	}, 8)

	// Emit 12 signals while the sink callback is completely stalled on the very
	// first signal: 1 is held in-flight by the worker goroutine, 8 fill the channel
	// buffer, and the remaining 3 overflow without blocking any caller.
	doneProducing := make(chan struct{})
	go func() {
		defer close(doneProducing)
		// Send the first item and wait for the worker to pick it up so buffer accounting is deterministic.
		ae.Emit(Signal{TriageEvent: engine.TriageEvent{Name: "sig-0"}})
		<-firstEntered
		for i := 1; i < 12; i++ {
			ae.Emit(Signal{TriageEvent: engine.TriageEvent{Name: fmt.Sprintf("sig-%d", i)}})
		}
	}()

	select {
	case <-doneProducing:
	case <-time.After(time.Second):
		t.Fatal("Emit blocked while downstream sink callback was stalled")
	}

	if dropped := ae.Dropped(); dropped != 3 {
		t.Fatalf("Dropped() = %d, want 3 (1 in-flight + 8 queued + 3 dropped = 12)", dropped)
	}

	// Cancel context and release sink; Close must flush all 9 accepted signals in order.
	cancel()
	close(unblockSink)
	ae.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 9 {
		t.Fatalf("flushed %d signals on shutdown, want 9: %v", len(got), got)
	}
	for i := 0; i < 9; i++ {
		want := fmt.Sprintf("sig-%d", i)
		if got[i] != want {
			t.Errorf("got[%d] = %q, want %q", i, got[i], want)
		}
	}
}
