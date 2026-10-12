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
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

type watchboardGoneSink struct {
	mu          sync.Mutex
	nextSession int
	openErr     error
	appendErrs  map[string]error
	appended    map[string][]any
}

func (s *watchboardGoneSink) CreateSession(_ context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.openErr != nil {
		return "", s.openErr
	}
	s.nextSession++
	return fmt.Sprintf("wb-%d", s.nextSession), nil
}

func (s *watchboardGoneSink) OpenIncident(_ context.Context, _ any) (string, error) {
	return "", errors.New("unexpected OpenIncident call on SessionOpener sink")
}

func (s *watchboardGoneSink) Append(_ context.Context, sid string, payload any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err, ok := s.appendErrs[sid]; ok && err != nil {
		return err
	}
	if s.appended == nil {
		s.appended = make(map[string][]any)
	}
	s.appended[sid] = append(s.appended[sid], payload)
	return nil
}

func TestWatchboard_SessionGoneReopensAndRetriesDigest(t *testing.T) {
	ctx := context.Background()
	sink := &watchboardGoneSink{
		appendErrs: make(map[string]error),
		appended:   make(map[string][]any),
	}
	m := newMetrics()
	now := time.Date(2026, 10, 12, 13, 0, 0, 0, time.UTC)
	bound := make(map[string]string)

	b := newWatchboard(watchboardConfig{
		injector:      sink,
		metrics:       m,
		cluster:       "prod-east",
		batch:         1,
		flushInterval: time.Minute,
		rotateAfter:   200,
	})
	b.clock = func() time.Time { return now }
	b.bind = func(sig engine.Signal, sid string) {
		bound[sig.Name] = sid
	}

	// Flush 1 opens wb-1 (generation 1) and delivers warning cart-1.
	b.Add(ctx, warningSignal(1), 1)

	if b.sid != "wb-1" || b.generation != 1 || b.injects != 1 {
		t.Fatalf("after initial flush got (sid=%q, gen=%d, injects=%d), want (wb-1, 1, 1)",
			b.sid, b.generation, b.injects)
	}

	// Simulate wb-1 being deleted or unrestorable on the sink (HTTP 404).
	sink.mu.Lock()
	sink.appendErrs["wb-1"] = &inject.StatusError{Code: 404}
	sink.mu.Unlock()

	// Flush 2 hits 404 on wb-1: watchboard must forget wb-1, advance
	// generation to 2, open wb-2, and retry cart-2 once so nothing is dropped.
	b.Add(ctx, warningSignal(2), 1)

	if b.sid != "wb-2" || b.generation != 2 || b.injects != 1 {
		t.Fatalf("after 404 recovery got (sid=%q, gen=%d, injects=%d), want (wb-2, 2, 1)",
			b.sid, b.generation, b.injects)
	}
	if got := testutil.ToFloat64(m.watchboardSessionsLost); got != 1 {
		t.Fatalf("lookout_watchboard_sessions_lost_total = %v, want 1", got)
	}
	if got := bound["cart-2"]; got != "wb-2" {
		t.Fatalf("entry cart-2 bound to %q, want wb-2", got)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.appended["wb-1"]) != 1 {
		t.Fatalf("expected only initial digest in dead wb-1 (no watchboard.rotated), got %d injects", len(sink.appended["wb-1"]))
	}
	if len(sink.appended["wb-2"]) != 1 {
		t.Fatalf("expected retried digest delivered to wb-2, got %d injects", len(sink.appended["wb-2"]))
	}
	digest, ok := sink.appended["wb-2"][0].(inject.WatchboardDigestPayload)
	if !ok {
		t.Fatalf("unexpected payload type on wb-2: %T", sink.appended["wb-2"][0])
	}
	if digest.BoardGeneration != 2 || digest.Sequence != 1 || len(digest.Entries) != 1 || digest.Entries[0].Name != "cart-2" {
		t.Fatalf("unexpected digest payload on wb-2: %+v", digest)
	}
}

func TestWatchboard_SessionGoneAndReopenFailsClearsDeadSession(t *testing.T) {
	ctx := context.Background()
	sink := &watchboardGoneSink{
		appendErrs: make(map[string]error),
		appended:   make(map[string][]any),
	}
	m := newMetrics()
	now := time.Date(2026, 10, 12, 13, 0, 0, 0, time.UTC)

	b := newWatchboard(watchboardConfig{
		injector:      sink,
		metrics:       m,
		cluster:       "prod-east",
		batch:         1,
		flushInterval: time.Minute,
		rotateAfter:   200,
	})
	b.clock = func() time.Time { return now }

	// Flush 1 opens wb-1.
	b.Add(ctx, warningSignal(1), 1)
	if b.sid != "wb-1" {
		t.Fatalf("got sid=%q, want wb-1", b.sid)
	}

	// wb-1 is gone (HTTP 410) AND CreateSession transiently fails.
	sink.mu.Lock()
	sink.appendErrs["wb-1"] = &inject.StatusError{Code: 410}
	sink.openErr = errors.New("temporary control-plane outage")
	sink.mu.Unlock()

	b.Add(ctx, warningSignal(2), 1)
	if b.sid != "" || b.injects != 0 {
		t.Fatalf("expected cleared session (sid=\"\", injects=0) after failed reopen, got (sid=%q, injects=%d)", b.sid, b.injects)
	}

	// Once CreateSession recovers, the next flush opens wb-2 cleanly.
	sink.mu.Lock()
	sink.openErr = nil
	sink.mu.Unlock()

	b.Add(ctx, warningSignal(3), 1)
	if b.sid != "wb-2" || b.generation != 2 || b.injects != 1 {
		t.Fatalf("after control-plane recovery got (sid=%q, gen=%d, injects=%d), want (wb-2, 2, 1)",
			b.sid, b.generation, b.injects)
	}
}
