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
	"log"
	"sync"
	"sync/atomic"
)

// DefaultQueueSize is the bounded FIFO buffer capacity used by NewAsyncEmitter
// when queueSize <= 0. Sizing it at 1024 absorbs informer watch bursts without
// ever stalling client-go's shared processor goroutine on downstream HTTP/store
// backpressure.
const DefaultQueueSize = 1024

// AsyncEmitter decouples client-go informer callbacks (OnAdd/OnUpdate/OnDelete)
// from synchronous downstream dispatch by queuing signals into a bounded FIFO
// ring buffer and draining them on a single worker goroutine tied to ctx.
//
// When the buffer is full, Emit increments the atomic drop counter and returns
// immediately rather than blocking the informer reflector/processor loop.
// Calling Close waits for the worker goroutine to flush all buffered signals so
// callers uphold the Source contract that emit is never invoked after Run exits.
type AsyncEmitter struct {
	ch        chan Signal
	emit      func(Signal)
	stopCh    chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
	dropped   atomic.Int64
	warnOnce  sync.Once
}

// NewAsyncEmitter constructs and starts a bounded non-blocking emitter that
// forwards queued signals to emit in FIFO order. If queueSize <= 0,
// DefaultQueueSize is used.
func NewAsyncEmitter(ctx context.Context, emit func(Signal), queueSize int) *AsyncEmitter {
	if queueSize <= 0 {
		queueSize = DefaultQueueSize
	}
	if emit == nil {
		emit = func(Signal) {}
	}
	ae := &AsyncEmitter{
		ch:     make(chan Signal, queueSize),
		emit:   emit,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
	go ae.run(ctx)
	return ae
}

func (ae *AsyncEmitter) run(ctx context.Context) {
	defer close(ae.done)
	for {
		select {
		case sig := <-ae.ch:
			ae.emit(sig)
		case <-ae.stopCh:
			ae.drain()
			return
		case <-ctx.Done():
			ae.drain()
			return
		}
	}
}

func (ae *AsyncEmitter) drain() {
	for {
		select {
		case sig := <-ae.ch:
			ae.emit(sig)
		default:
			return
		}
	}
}

// Emit enqueues sig without blocking. If Close has already been called or the
// bounded queue is full, sig is dropped and the dropped counter is incremented.
func (ae *AsyncEmitter) Emit(sig Signal) {
	if ae.closed.Load() {
		return
	}
	select {
	case ae.ch <- sig:
	default:
		n := ae.dropped.Add(1)
		ae.warnOnce.Do(func() {
			log.Printf("sources: async emitter queue full (capacity=%d); dropping signals under downstream backpressure (dropped=%d)",
				cap(ae.ch), n)
		})
	}
}

// Dropped returns the total number of signals dropped due to queue overflow.
func (ae *AsyncEmitter) Dropped() int64 {
	return ae.dropped.Load()
}

// Close stops accepting new signals, signals the background worker to finish
// draining buffered items, and blocks until all queued signals have been
// delivered. Safe to call multiple times.
func (ae *AsyncEmitter) Close() {
	ae.closeOnce.Do(func() {
		ae.closed.Store(true)
		close(ae.stopCh)
	})
	<-ae.done
	// Drain any straggler signal that won the race between closed.Load() and
	// ctx cancellation so no signal buffered before Close is ever left behind.
	ae.drain()
}
