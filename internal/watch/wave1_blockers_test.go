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
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

type stubStormSink struct {
	mu          sync.Mutex
	nextID      int
	deadIDs     map[string]bool
	appendErr   error
	appendsByID map[string][]any
}

func newStubStormSink() *stubStormSink {
	return &stubStormSink{
		deadIDs:     map[string]bool{},
		appendsByID: map[string][]any{},
	}
}

func (s *stubStormSink) OpenIncident(_ context.Context, _ any) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	return fmt.Sprintf("storm-sess-%d", s.nextID), nil
}

func (s *stubStormSink) Append(_ context.Context, id string, payload any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deadIDs[id] {
		return &inject.StatusError{Op: "POST", Code: http.StatusNotFound, Body: "session gone"}
	}
	if s.appendErr != nil {
		return s.appendErr
	}
	s.appendsByID[id] = append(s.appendsByID[id], payload)
	return nil
}

type staticNodeResolver string

func (r staticNodeResolver) Ancestors(engine.ObjectRef) []engine.Ancestor {
	return []engine.Ancestor{{Kind: "Node", Name: string(r)}}
}

type fixedClearanceObserver struct {
	cleared bool
}

func (f *fixedClearanceObserver) Clearance(_ engine.Incident) (engine.Clearance, bool) {
	return engine.Clearance{Cleared: f.cleared, Resolution: engine.ResolutionRecovered}, true
}

// TestStormDeadSessionReopensOnAttach verifies H2 (#608): when the bound storm
// session returns HTTP 404/410 on attach, the dead session ID is cleared from
// the correlator and members, a replacement storm session is opened, and future
// members route to the new session instead of black-holing forever.
func TestStormDeadSessionReopensOnAttach(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	sink := newStubStormSink()

	correlator, err := engine.NewStormCorrelator(10*time.Minute, 3, staticNodeResolver("gke-node-1"))
	if err != nil {
		t.Fatalf("NewStormCorrelator: %v", err)
	}
	dedup, err := engine.NewDedupCache(5*time.Minute, "")
	if err != nil {
		t.Fatalf("NewDedupCache: %v", err)
	}
	d := &dispatcher{
		mode:     "per-incident",
		cluster:  "prod-us",
		injector: sink,
		dedup:    dedup,
		storm:    correlator,
		metrics:  newMetrics(),
	}

	makeSig := func(i int) engine.Signal {
		return engine.Signal{
			Kind:     engine.KindK8sEvent,
			Severity: engine.SeverityCritical,
			TriageEvent: engine.TriageEvent{
				Namespace:    fmt.Sprintf("ns-%d", i),
				KindOfObject: "Pod",
				Name:         fmt.Sprintf("pod-%d", i),
				Node:         "gke-node-1",
				Key: engine.EventKey{
					UID:    fmt.Sprintf("uid-%d", i),
					Reason: "FailedMount",
				},
				FirstSeen: now,
				LastSeen:  now,
			},
		}
	}

	// Form initial storm -> opens storm-sess-1.
	for i := 1; i <= 3; i++ {
		d.DispatchSignal(context.Background(), makeSig(i))
	}
	if got := correlator.ActiveStorms(); got != 1 {
		t.Fatalf("ActiveStorms = %d, want 1", got)
	}

	// Expire storm-sess-3 (the formed storm session after per-incident opens
	// storm-sess-1 and storm-sess-2) on the sink (404 NotFound).
	sink.mu.Lock()
	sink.deadIDs["storm-sess-3"] = true
	sink.mu.Unlock()

	// Next arriving member triggers 404 on storm-sess-3 -> clears dead session
	// and reopens storm-sess-4 immediately (#608).
	d.DispatchSignal(context.Background(), makeSig(4))

	boundSid, ok := dedup.LookupSession(makeSig(4).Key)
	if !ok || boundSid != "storm-sess-4" {
		t.Fatalf("member 4 bound session = %q (ok=%v), want \"storm-sess-4\"", boundSid, ok)
	}
}

// TestRecoverySkipsResolvedBindingsAndRetriesTransientAppendErrors verifies H3
// and H4 (#640):
//  1. Resolved dedup entries are excluded from Bindings() so restarts do not
//     re-track or re-emit resolved incidents.
//  2. A transient 503 error on kind=resolved Append requeues the incident on
//     RecoveryTracker instead of prematurely closing local state.
func TestRecoverySkipsResolvedBindingsAndRetriesTransientAppendErrors(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	sink := newStubStormSink()
	dedup, err := engine.NewDedupCache(5*time.Minute, "")
	if err != nil {
		t.Fatalf("NewDedupCache: %v", err)
	}
	dedup.SetClock(func() time.Time { return now })
	obs := &fixedClearanceObserver{cleared: true}

	var d *dispatcher
	tracker := engine.NewRecoveryTrackerWithClock(2*time.Minute, func(sig engine.Signal) {
		d.DispatchSignal(context.Background(), sig)
	}, func() time.Time { return now })
	tracker.AddObserver(obs)

	d = &dispatcher{
		mode:     "per-incident",
		cluster:  "prod-us",
		injector: sink,
		dedup:    dedup,
		tracker:  tracker,
		metrics:  newMetrics(),
	}

	key := engine.EventKey{UID: "uid-api-0", Reason: "CrashLoopBackOff"}
	dedup.Observe(key, now)
	dedup.BindIncident(key, "sess-1", engine.IncidentRef{Namespace: "prod", KindOfObject: "Pod", Name: "api-0", Fingerprint: "fp-api-0", Cluster: "prod-us"})
	tracker.Track(engine.Incident{Key: key, SessionID: "sess-1", FirstSeen: now})

	// Move into stateClearing at t=1m.
	now = now.Add(1 * time.Minute)
	tracker.Tick()

	// At t=4m (>= 2m stable), first Tick tries Append while sink returns 503.
	sink.mu.Lock()
	sink.appendErr = &inject.StatusError{Op: "POST", Code: http.StatusServiceUnavailable, Body: "temporary outage"}
	sink.mu.Unlock()

	now = now.Add(3 * time.Minute)
	tracker.Tick()

	// Incident must NOT be marked Resolved in Bindings() yet because Append failed transiently!
	if got := len(dedup.Bindings()); got != 1 {
		t.Fatalf("after failed 503 Append, Bindings() len = %d, want 1 (still open for retry)", got)
	}

	// Sink recovers at t=5m -> next Tick delivers kind=resolved cleanly!
	sink.mu.Lock()
	sink.appendErr = nil
	sink.mu.Unlock()

	now = now.Add(1 * time.Minute)
	tracker.Tick()

	sink.mu.Lock()
	gotAppends := len(sink.appendsByID["sess-1"])
	sink.mu.Unlock()
	if gotAppends != 1 {
		t.Fatalf("sess-1 received %d appends after recovery retry, want 1", gotAppends)
	}

	// Now that kind=resolved succeeded, Bindings() must exclude the resolved entry (H3)!
	if got := len(dedup.Bindings()); got != 0 {
		t.Fatalf("after successful resolved delivery, Bindings() len = %d, want 0 (resolved excluded)", got)
	}
}
