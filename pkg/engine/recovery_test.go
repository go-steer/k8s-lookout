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

package engine

import (
	"testing"
	"time"
)

// scriptedObserver drives the tracker with a swappable verdict.
type scriptedObserver struct {
	verdict Clearance
	ok      bool
}

func (s *scriptedObserver) Clearance(Incident) (Clearance, bool) {
	return s.verdict, s.ok
}

// recoveryHarness bundles a tracker, its fake clock, the scripted
// observer, and the captured emissions.
type recoveryHarness struct {
	tracker  *RecoveryTracker
	obs      *scriptedObserver
	now      time.Time
	emitted  []Signal
	incident Incident
}

func newRecoveryHarness(t *testing.T, stableFor time.Duration) *recoveryHarness {
	t.Helper()
	h := &recoveryHarness{
		now: time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC),
		obs: &scriptedObserver{},
	}
	h.tracker = NewRecoveryTracker(stableFor, func(sig Signal) {
		h.emitted = append(h.emitted, sig)
	})
	h.tracker.now = func() time.Time { return h.now }
	h.tracker.AddObserver(h.obs)
	h.incident = Incident{
		Key:       EventKey{UID: "u1", Reason: "CrashLoopBackOff"},
		SessionID: "sess-1",
		FirstSeen: h.now,
		Ref: IncidentRef{
			Namespace:    "checkout",
			KindOfObject: "Pod",
			Name:         "checkout-7b9d-x4kzq",
			Fingerprint:  "sha256:orig",
			Cluster:      "prod",
		},
	}
	return h
}

func (h *recoveryHarness) advance(d time.Duration) { h.now = h.now.Add(d) }

func (h *recoveryHarness) tickAt(offset time.Duration, verdict Clearance, ok bool) {
	h.advance(offset)
	h.obs.verdict = verdict
	h.obs.ok = ok
	h.tracker.Tick()
}

func TestRecoveryTracker_ClearStableResolved(t *testing.T) {
	t.Parallel()
	h := newRecoveryHarness(t, 5*time.Minute)
	h.tracker.Track(h.incident)

	// Symptomatic: predicate false → nothing.
	h.tickAt(time.Minute, Clearance{Cleared: false}, true)
	if len(h.emitted) != 0 {
		t.Fatalf("emitted while symptomatic: %+v", h.emitted)
	}

	// Symptom clears at t+2m (observer vouches StableSince = now).
	clearedAt := h.now.Add(time.Minute)
	h.tickAt(time.Minute, Clearance{Cleared: true, StableSince: clearedAt, Resolution: ResolutionRecovered}, true)
	// Window running: 3m in, nothing yet.
	h.tickAt(3*time.Minute, Clearance{Cleared: true, StableSince: clearedAt, Resolution: ResolutionRecovered}, true)
	if len(h.emitted) != 0 {
		t.Fatalf("emitted before stability window elapsed: %+v", h.emitted)
	}
	// Past the window → resolved.
	h.tickAt(2*time.Minute+time.Second, Clearance{Cleared: true, StableSince: clearedAt, Resolution: ResolutionRecovered}, true)
	if len(h.emitted) != 1 {
		t.Fatalf("want 1 resolved emission, got %d", len(h.emitted))
	}
	sig := h.emitted[0]
	if sig.Kind != KindResolved {
		t.Errorf("Kind = %q, want %q", sig.Kind, KindResolved)
	}
	if sig.Recovery == nil {
		t.Fatal("resolved signal missing Recovery attachment")
	}
	if sig.Recovery.Resolution != ResolutionRecovered {
		t.Errorf("Resolution = %q, want recovered", sig.Recovery.Resolution)
	}
	if got, want := sig.Recovery.ClearedAfter, 2*time.Minute; got != want {
		t.Errorf("ClearedAfter = %v, want %v (first_seen → clear)", got, want)
	}
	if sig.Recovery.ObservedStableFor < 5*time.Minute {
		t.Errorf("ObservedStableFor = %v, want >= stability window", sig.Recovery.ObservedStableFor)
	}
	// Identity + fingerprint of the ORIGINAL incident carried over.
	if sig.Fingerprint != "sha256:orig" {
		t.Errorf("Fingerprint = %q, want the original incident's", sig.Fingerprint)
	}
	if sig.Namespace != "checkout" || sig.Name != "checkout-7b9d-x4kzq" || sig.Key.UID != "u1" {
		t.Errorf("identity fields not carried: %+v", sig.TriageEvent)
	}

	// Still tracked through the revert window…
	if h.tracker.Len() != 1 {
		t.Errorf("tracker should keep the incident through the revert window")
	}
	// …and untracked (no further emission) once it passes clean.
	h.tickAt(5*time.Minute+time.Second, Clearance{Cleared: true, StableSince: clearedAt, Resolution: ResolutionRecovered}, true)
	if h.tracker.Len() != 0 {
		t.Errorf("tracker should drop the incident after a clean revert window, Len=%d", h.tracker.Len())
	}
	if len(h.emitted) != 1 {
		t.Errorf("no extra emissions expected, got %d", len(h.emitted))
	}
}

func TestRecoveryTracker_FlapWithinWindowResets(t *testing.T) {
	t.Parallel()
	h := newRecoveryHarness(t, 5*time.Minute)
	h.tracker.Track(h.incident)

	// Clears at t+1m…
	clearedAt := h.now.Add(time.Minute)
	h.tickAt(time.Minute, Clearance{Cleared: true, StableSince: clearedAt, Resolution: ResolutionRecovered}, true)
	// …flaps back at t+3m (inside the window) → reset.
	h.tickAt(2*time.Minute, Clearance{Cleared: false}, true)
	// Clears again at t+4m; at t+8m the ORIGINAL window would have
	// elapsed, but the reset one has not.
	reclearedAt := h.now.Add(time.Minute)
	h.tickAt(time.Minute, Clearance{Cleared: true, StableSince: reclearedAt, Resolution: ResolutionRecovered}, true)
	h.tickAt(4*time.Minute, Clearance{Cleared: true, StableSince: reclearedAt, Resolution: ResolutionRecovered}, true)
	if len(h.emitted) != 0 {
		t.Fatalf("flap did not reset the stability window: %+v", h.emitted)
	}
	// Full window from the re-clear → resolved.
	h.tickAt(time.Minute+time.Second, Clearance{Cleared: true, StableSince: reclearedAt, Resolution: ResolutionRecovered}, true)
	if len(h.emitted) != 1 || h.emitted[0].Kind != KindResolved {
		t.Fatalf("want 1 resolved after the reset window, got %+v", h.emitted)
	}
	if got, want := h.emitted[0].Recovery.ClearedAfter, 4*time.Minute; got != want {
		t.Errorf("ClearedAfter = %v, want %v (measured from the LAST clear)", got, want)
	}
}

// A restart between ticks is visible only as StableSince jumping
// forward (the pod reads Ready at every tick). The window must
// restart from the jump.
func TestRecoveryTracker_StableSinceJumpRestartsWindow(t *testing.T) {
	t.Parallel()
	h := newRecoveryHarness(t, 5*time.Minute)
	h.tracker.Track(h.incident)

	clearedAt := h.now.Add(time.Minute)
	h.tickAt(time.Minute, Clearance{Cleared: true, StableSince: clearedAt, Resolution: ResolutionRecovered}, true)
	// At t+4m the container has restarted and come back Ready:
	// StableSince jumps to t+3m30s.
	jumped := h.now.Add(2*time.Minute + 30*time.Second)
	h.tickAt(3*time.Minute, Clearance{Cleared: true, StableSince: jumped, Resolution: ResolutionRecovered}, true)
	// t+6m30s: 5m from the ORIGINAL clear, but only 3m from the jump.
	h.tickAt(2*time.Minute+30*time.Second, Clearance{Cleared: true, StableSince: jumped, Resolution: ResolutionRecovered}, true)
	if len(h.emitted) != 0 {
		t.Fatalf("StableSince jump did not restart the window: %+v", h.emitted)
	}
	// t+8m31s: 5m+1s from the jump → resolved.
	h.tickAt(2*time.Minute+time.Second, Clearance{Cleared: true, StableSince: jumped, Resolution: ResolutionRecovered}, true)
	if len(h.emitted) != 1 {
		t.Fatalf("want resolved after window from jump, got %d emissions", len(h.emitted))
	}
}

func TestRecoveryTracker_RevertAfterResolve(t *testing.T) {
	t.Parallel()
	h := newRecoveryHarness(t, 5*time.Minute)
	h.tracker.Track(h.incident)

	clearedAt := h.now.Add(time.Minute)
	verdict := Clearance{Cleared: true, StableSince: clearedAt, Resolution: ResolutionRecovered}
	h.tickAt(time.Minute, verdict, true)
	h.tickAt(5*time.Minute+time.Second, verdict, true)
	if len(h.emitted) != 1 || h.emitted[0].Kind != KindResolved {
		t.Fatalf("setup: want resolved, got %+v", h.emitted)
	}

	// Symptom recurs 2m after the resolve — inside the revert window.
	h.tickAt(2*time.Minute, Clearance{Cleared: false}, true)
	if len(h.emitted) != 2 {
		t.Fatalf("want resolved.reverted emission, got %d", len(h.emitted))
	}
	rev := h.emitted[1]
	if rev.Kind != KindResolvedReverted {
		t.Errorf("Kind = %q, want %q", rev.Kind, KindResolvedReverted)
	}
	if got, want := rev.Recovery.RevertedAfter, 2*time.Minute; got != want {
		t.Errorf("RevertedAfter = %v, want %v", got, want)
	}
	if rev.Recovery.Resolution != ResolutionRecovered {
		t.Errorf("reverted record keeps the original resolution, got %q", rev.Recovery.Resolution)
	}
	if rev.Fingerprint != "sha256:orig" {
		t.Errorf("reverted Fingerprint = %q, want the original incident's", rev.Fingerprint)
	}

	// Re-armed: a second full clear → stable cycle resolves again.
	if h.tracker.Len() != 1 {
		t.Fatalf("tracker must keep the incident after a revert (re-arm)")
	}
	recleared := h.now.Add(time.Minute)
	verdict2 := Clearance{Cleared: true, StableSince: recleared, Resolution: ResolutionRecovered}
	h.tickAt(time.Minute, verdict2, true)
	h.tickAt(5*time.Minute+time.Second, verdict2, true)
	if len(h.emitted) != 3 || h.emitted[2].Kind != KindResolved {
		t.Fatalf("want a second resolved after re-arm, got %+v", h.emitted)
	}
}

func TestRecoveryTracker_ObjectDeletedResolution(t *testing.T) {
	t.Parallel()
	h := newRecoveryHarness(t, 5*time.Minute)
	h.tracker.Track(h.incident)

	deletedAt := h.now.Add(time.Minute)
	verdict := Clearance{Cleared: true, StableSince: deletedAt, Resolution: ResolutionObjectDeleted}
	h.tickAt(time.Minute, verdict, true)
	h.tickAt(5*time.Minute+time.Second, verdict, true)
	if len(h.emitted) != 1 {
		t.Fatalf("want 1 resolved, got %d", len(h.emitted))
	}
	if h.emitted[0].Recovery.Resolution != ResolutionObjectDeleted {
		t.Errorf("Resolution = %q, want object_deleted — the agent must be able to tell deleted from fixed", h.emitted[0].Recovery.Resolution)
	}
}

// An incident no observer can judge (this PR ships only the
// pod-scoped observer) must not be tracked forever.
func TestRecoveryTracker_UncoveredIncidentExpires(t *testing.T) {
	t.Parallel()
	h := newRecoveryHarness(t, 5*time.Minute)
	h.tracker.uncoveredTTL = 30 * time.Minute
	h.tracker.Track(h.incident)

	h.tickAt(10*time.Minute, Clearance{}, false) // observer: not mine
	if h.tracker.Len() != 1 {
		t.Fatalf("still within uncoveredTTL; must stay tracked")
	}
	h.tickAt(21*time.Minute, Clearance{}, false)
	if h.tracker.Len() != 0 {
		t.Errorf("never-judged incident must expire after uncoveredTTL")
	}
	if len(h.emitted) != 0 {
		t.Errorf("expiry must not emit, got %+v", h.emitted)
	}
}

// A long-symptomatic incident that HAS been judged stays tracked —
// the TTL is only for incidents nothing can judge.
func TestRecoveryTracker_JudgedSymptomaticNeverExpires(t *testing.T) {
	t.Parallel()
	h := newRecoveryHarness(t, 5*time.Minute)
	h.tracker.uncoveredTTL = 30 * time.Minute
	h.tracker.Track(h.incident)

	h.tickAt(time.Minute, Clearance{Cleared: false}, true)
	h.tickAt(2*time.Hour, Clearance{Cleared: false}, true)
	if h.tracker.Len() != 1 {
		t.Errorf("judged symptomatic incident must stay tracked indefinitely")
	}
}

// Track canonicalizes the key's reason so tracked incidents line up
// with dedup bindings, and re-tracking (dedup retry safety net: new
// session for the same key) re-arms cleanly.
func TestRecoveryTracker_TrackCanonicalizesAndUpserts(t *testing.T) {
	t.Parallel()
	h := newRecoveryHarness(t, 5*time.Minute)
	inc := h.incident
	inc.Key.Reason = "BackOff" // canonicalizes to CrashLoopBackOff
	h.tracker.Track(inc)
	if h.tracker.Len() != 1 {
		t.Fatalf("Len = %d", h.tracker.Len())
	}
	// Same canonical key under the other reason variant: upsert.
	h.tracker.Track(h.incident)
	if h.tracker.Len() != 1 {
		t.Errorf("re-Track of the same canonical key must upsert, Len = %d", h.tracker.Len())
	}
	// The emitted key carries the canonical reason.
	verdict := Clearance{Cleared: true, StableSince: h.now, Resolution: ResolutionRecovered}
	h.tickAt(time.Minute, verdict, true)
	h.tickAt(5*time.Minute+time.Second, verdict, true)
	if len(h.emitted) != 1 {
		t.Fatalf("want resolved, got %d", len(h.emitted))
	}
	if h.emitted[0].Key.Reason != "CrashLoopBackOff" {
		t.Errorf("resolved Key.Reason = %q, want canonical CrashLoopBackOff", h.emitted[0].Key.Reason)
	}
}
