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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// concurrentBarrierSink blocks OpenIncident calls until concurrencyTarget
// distinct incident opens are simultaneously in-flight inside OpenIncident.
// If dispatcher held a global injectLock across OpenIncident, the second open
// could never enter OpenIncident and the test would time out (#642).
type concurrentBarrierSink struct {
	concurrencyTarget int
	inFlight          atomic.Int32
	maxInFlight       atomic.Int32
	barrier           chan struct{}
	barrierOnce       sync.Once

	mu       sync.Mutex
	sessions int
	appends  map[string][]any
}

func newConcurrentBarrierSink(target int) *concurrentBarrierSink {
	return &concurrentBarrierSink{
		concurrencyTarget: target,
		barrier:           make(chan struct{}),
		appends:           make(map[string][]any),
	}
}

func (s *concurrentBarrierSink) OpenIncident(ctx context.Context, payload any) (string, error) {
	cur := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)

	for {
		prev := s.maxInFlight.Load()
		if cur <= prev || s.maxInFlight.CompareAndSwap(prev, cur) {
			break
		}
	}
	if int(cur) >= s.concurrencyTarget {
		s.barrierOnce.Do(func() { close(s.barrier) })
	}

	if s.concurrencyTarget > 1 {
		select {
		case <-s.barrier:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	s.mu.Lock()
	s.sessions++
	sid := fmt.Sprintf("sess-%d", s.sessions)
	s.mu.Unlock()
	return sid, nil
}

func (s *concurrentBarrierSink) Append(ctx context.Context, sessionID string, payload any) error {
	s.mu.Lock()
	s.appends[sessionID] = append(s.appends[sessionID], payload)
	s.mu.Unlock()
	return nil
}

func TestDispatcherConcurrentSlowSinkParallelism(t *testing.T) {
	const workers = 6
	sink := newConcurrentBarrierSink(workers)
	metrics := newMetrics()
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	routing := engine.NewRoutingPolicy(nil)
	board := newWatchboard(watchboardConfig{
		injector:      sink,
		metrics:       metrics,
		cluster:       "race-cluster",
		batch:         5,
		flushInterval: 10 * time.Minute,
		rotateAfter:   100,
	})
	storm, _ := engine.NewStormCorrelator(2*time.Minute, 3, nil)
	d := &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1, 0)),
		dedup:    dedup,
		injector: sink,
		metrics:  metrics,
		cluster:  "race-cluster",
		mode:     "per-incident",
		routing:  routing,
		board:    board,
		storm:    storm,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	now := time.Now()
	var wg sync.WaitGroup

	// 1. Launch workers distinct critical incidents concurrently — all must
	// enter sink.OpenIncident in parallel without global lock serialization.
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			sig := engine.Signal{
				Kind:     engine.KindK8sEvent,
				Severity: engine.SeverityCritical,
				TriageEvent: engine.TriageEvent{
					Key: engine.EventKey{
						UID:    fmt.Sprintf("uid-crit-%d", i),
						Reason: "BackOff",
					},
					Namespace:    fmt.Sprintf("ns-%d", i),
					KindOfObject: "Pod",
					Name:         fmt.Sprintf("pod-%d", i),
					Message:      "Back-off restarting failed container",
					Count:        5,
					FirstSeen:    now,
					LastSeen:     now,
				},
			}
			d.DispatchSignal(ctx, sig)
			// Immediately fire a duplicate on the same key while other keys are active.
			d.DispatchSignal(ctx, sig)
		}()
	}

	// 2. Simultaneously dispatch warning signals onto watchboard + concurrent flushes.
	for w := 0; w < 12; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.DispatchSignal(ctx, engine.Signal{
				Kind:     engine.KindK8sEvent,
				Severity: engine.SeverityWarning,
				TriageEvent: engine.TriageEvent{
					Key: engine.EventKey{
						UID:    fmt.Sprintf("uid-warn-%d", w),
						Reason: "Unhealthy",
					},
					Namespace:    "default",
					KindOfObject: "Pod",
					Name:         fmt.Sprintf("warn-pod-%d", w),
					Message:      "Readiness probe failed",
					Count:        5,
					FirstSeen:    now,
					LastSeen:     now,
				},
			})
			board.Tick(ctx)
		}()
	}

	// 3. Simultaneously burst 6 pods sharing a node ancestor to exercise storm formation + attach.
	for m := 0; m < 6; m++ {
		m := m
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.DispatchSignal(ctx, engine.Signal{
				Kind:     engine.KindK8sEvent,
				Severity: engine.SeverityCritical,
				TriageEvent: engine.TriageEvent{
					Key: engine.EventKey{
						UID:    fmt.Sprintf("uid-storm-%d", m),
						Reason: "BackOff",
					},
					Namespace:    fmt.Sprintf("tenant-%d", m),
					KindOfObject: "Pod",
					Name:         fmt.Sprintf("storm-pod-%d", m),
					Node:         "node-pool-a-1",
					Message:      "Back-off restarting failed container",
					Count:        5,
					FirstSeen:    now,
					LastSeen:     now,
				},
			})
		}()
	}

	wg.Wait()

	if got := sink.maxInFlight.Load(); got < int32(workers) {
		t.Fatalf("max concurrent OpenIncident calls = %d, want >= %d (global lock regression)", got, workers)
	}
}

type testClearanceObserver struct {
	mu      sync.Mutex
	cleared map[engine.EventKey]bool
}

func (o *testClearanceObserver) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.cleared[inc.Key] {
		return engine.Clearance{
			Cleared:    true,
			Resolution: engine.ResolutionRecovered,
		}, true
	}
	return engine.Clearance{Cleared: false}, true
}

func TestSiblingResolvedCoalescingAndActiveGauge(t *testing.T) {
	sink := newConcurrentBarrierSink(1)
	metrics := newMetrics()
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	curNow := now
	clock := func() time.Time { return curNow }

	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	dedup.SetClock(clock)

	var d *dispatcher
	ctx := context.Background()
	tracker := engine.NewRecoveryTrackerWithClock(time.Minute, func(sig engine.Signal) {
		d.DispatchSignal(ctx, sig)
	}, clock)
	obs := &testClearanceObserver{cleared: make(map[engine.EventKey]bool)}
	tracker.AddObserver(obs)
	dedup.SetOpenCheck(tracker.Open)

	d = &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1, 0)),
		dedup:    dedup,
		injector: sink,
		metrics:  metrics,
		cluster:  "test-cluster",
		mode:     "per-incident",
		tracker:  tracker,
	}

	ctrl := "Deployment/checkout"
	keyA := engine.EventKey{UID: "pod-a-uid", Reason: "BackOff"}
	keyB := engine.EventKey{UID: "pod-b-uid", Reason: "BackOff"}

	sigA := engine.Signal{
		Kind:     engine.KindK8sEvent,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:           keyA,
			Namespace:     "prod",
			KindOfObject:  "Pod",
			Name:          "checkout-a",
			ControllerRef: ctrl,
			Message:       "Back-off restarting failed container",
			Count:         5,
			FirstSeen:     now,
			LastSeen:      now,
		},
	}
	sigB := sigA
	sigB.Key = keyB
	sigB.Name = "checkout-b"

	d.DispatchSignal(ctx, sigA)
	d.DispatchSignal(ctx, sigB)

	sidA, ok := dedup.LookupSession(keyA)
	if !ok || sidA == "" {
		t.Fatalf("expected session bound for keyA")
	}
	sidB, ok := dedup.LookupSession(keyB)
	if !ok || sidB != sidA {
		t.Fatalf("sibling pod B bound to %q, want same session %q", sidB, sidA)
	}

	// Pod A clears first while sibling Pod B is still symptomatic:
	// first tick marks clearing, second tick past stableFor emits resolved for A,
	// which deferSiblingResolved suppresses from premature wire injection (#641).
	obs.cleared[sigA.CanonicalKey()] = true
	tracker.Tick()
	curNow = now.Add(2 * time.Minute)
	tracker.Tick()

	// Only the sibling fold append (from sigB folding into sidA) should exist so far.
	sink.mu.Lock()
	countAfterFirstClear := len(sink.appends[sidA])
	sink.mu.Unlock()
	if countAfterFirstClear != 1 {
		t.Fatalf("appends to %s after first sibling cleared = %d, want 1 (premature resolved emitted while sibling B still open)", sidA, countAfterFirstClear)
	}

	// Now sibling Pod B clears: the single coalesced kind=resolved payload goes out!
	obs.cleared[sigB.CanonicalKey()] = true
	tracker.Tick()
	curNow = now.Add(4 * time.Minute)
	tracker.Tick()

	sink.mu.Lock()
	countAfterAllClear := len(sink.appends[sidA])
	sink.mu.Unlock()
	if countAfterAllClear != 2 {
		t.Fatalf("appends to %s after last sibling cleared = %d, want 2 (1 sibling-fold + 1 coalesced resolved)", sidA, countAfterAllClear)
	}

	if got := dedup.ActiveBoundCount(); got != 0 {
		t.Fatalf("ActiveBoundCount after all siblings resolved = %d, want 0", got)
	}
}

func TestNamespaceMetricLabelCardinalityBounded(t *testing.T) {
	sink := newConcurrentBarrierSink(1)
	metrics := newMetrics()
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	d := &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1, 0)),
		dedup:    dedup,
		injector: sink,
		metrics:  metrics,
		cluster:  "test-cluster",
		mode:     "per-incident",
	}
	ctx := context.Background()
	now := time.Now()
	for i := 0; i < namespaceLabelCap+50; i++ {
		d.DispatchSignal(ctx, engine.Signal{
			Kind:     engine.KindK8sEvent,
			Severity: engine.SeverityCritical,
			TriageEvent: engine.TriageEvent{
				Key:          engine.EventKey{UID: fmt.Sprintf("uid-%d", i), Reason: "BackOff"},
				Namespace:    fmt.Sprintf("ephemeral-pr-%d", i),
				KindOfObject: "Pod",
				Name:         "app",
				Count:        5,
				FirstSeen:    now,
				LastSeen:     now,
			},
		})
	}

	mfs, err := metrics.registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "lookout_events_seen_total" {
			if len(mf.GetMetric()) > namespaceLabelCap+1 {
				t.Fatalf("lookout_events_seen_total series = %d, want <= %d", len(mf.GetMetric()), namespaceLabelCap+1)
			}
			return
		}
	}
	t.Fatalf("lookout_events_seen_total metric family not found")
}
