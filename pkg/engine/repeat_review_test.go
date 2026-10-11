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
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// Edge cases of the #599 repeat schedule: a lower-class angle on an
// open incident, changes inside a window, rotating shapes, undelivered
// injects, a session the sink lost, and Snapshot racing repeats.

// TestRepeat_LowerClassAngleIsAbsorbed: another source's warning-class
// signal on the same key (capacity.pending on a FailedScheduling
// incident) after the lapse must not unbind the open incident; the
// next critical occurrence still lands in sess-1.
func TestRepeat_LowerClassAngleIsAbsorbed(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	h.c.NoteIncidentKind(h.key, KindK8sEvent)
	h.emit(5*time.Minute + 5*time.Second) // reminder 1
	h.now = h.now.Add(5*time.Minute + 2*time.Second)
	got := h.c.ObserveSignal(EventKey{UID: h.key.UID, Reason: "pending"}, Observation{
		LastTS: h.now, Kind: "capacity.pending", Severity: SeverityWarning, Message: "pod pending", Repeatable: false,
	})
	if got.Kind != DedupDuplicate || !got.Absorbed || got.SessionID != "sess-1" {
		t.Fatalf("warning angle after the lapse = %+v, want absorbed into sess-1", got)
	}
	got = h.emit(3 * time.Second)
	if got.Kind == DedupNewIncident || got.SessionID != "sess-1" {
		t.Fatalf("critical occurrence after the warning = %+v, want it still bound to sess-1", got)
	}
}

// TestRepeat_OpeningWindowShapesAreNotChanges: shapes first seen
// within a window of the open (kubelet's pull cycle emits four within
// seconds) are part of what the open described, so the first lapse is
// reminder 1, not a change.
func TestRepeat_OpeningWindowShapesAreNotChanges(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	h.msg = "a completely different failure clause"
	if got := h.emit(time.Minute); got.Kind != DedupDuplicate || got.Absorbed {
		t.Fatalf("opening-window shape = %+v, want a plain duplicate", got)
	}
	got := h.emit(5*time.Minute + time.Second)
	if got.Kind != DedupRepeat || got.RepeatCount != 1 {
		t.Fatalf("first lapse = %+v, want reminder 1", got)
	}
}

// TestRepeat_RotationAtSoakCadence: 17 rotating shapes at the stuck
// pod's own 5m05s cadence. Change followups must never come more often
// than max(2 windows, the next reminder interval), so rotation cannot
// out-inject the reminder schedule.
func TestRepeat_RotationAtSoakCadence(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	const cadence = 5*time.Minute + 5*time.Second
	var gaps []time.Duration
	last := h.now
	for i := range 100 { // ~8.5h
		h.msg = fmt.Sprintf("shape %c failed", 'a'+i%17)
		got := h.emit(cadence)
		if got.Kind != DedupRepeat {
			continue
		}
		if got.RepeatCount != 0 {
			t.Fatalf("rotation produced a pure reminder %+v; every inject should be a change", got)
		}
		gaps = append(gaps, h.now.Sub(last))
		last = h.now
	}
	// Floors: step interval → 5m, 10m, 20m, 40m, 1h, 1h, …
	floors := []time.Duration{5 * time.Minute, 10 * time.Minute, 20 * time.Minute, 40 * time.Minute, time.Hour}
	if len(gaps) < len(floors)+2 {
		t.Fatalf("gaps = %v, want at least %d injects", gaps, len(floors)+2)
	}
	for i, g := range gaps {
		floor := time.Hour
		if i < len(floors) {
			floor = floors[i]
		}
		if g <= floor || g > floor+cadence {
			t.Errorf("inject %d came %s after the previous one, want just over %s", i+1, g, floor)
		}
	}
	if len(gaps) > 12 {
		t.Errorf("injects = %d over ~8.5h of rotation, want the reminder schedule's pace (≤12)", len(gaps))
	}
}

// TestRepeat_LoneChangeLateInBackoff: deep in the backoff (1h step) a
// single real change goes out no later than the next reminder would
// have — it takes that reminder's slot.
func TestRepeat_LoneChangeLateInBackoff(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	const cadence = 5*time.Minute + 5*time.Second
	var lastInject time.Time
	for {
		got := h.emit(cadence)
		if got.Kind == DedupRepeat {
			lastInject = h.now
			if got.RepeatCount == 5 { // next interval: the 1h cap
				break
			}
		}
	}
	h.msg = "0/6 nodes are available: 6 Insufficient memory."
	for {
		got := h.emit(cadence)
		if got.Kind != DedupRepeat {
			continue
		}
		if got.Change != RepeatChangeClass {
			t.Fatalf("first inject after the change = %+v, want the class change", got)
		}
		if gap := h.now.Sub(lastInject); gap > time.Hour+cadence {
			t.Fatalf("change went out %s after the last reminder, want within the 1h reminder interval", gap)
		}
		return
	}
}

// TestRepeat_UndeliveredReminderCarriesPendingChange: reminder 1 fails;
// the next occurrence brings a new shape. The re-send must be the
// change, not a stale reminder that swallows it.
func TestRepeat_UndeliveredReminderCarriesPendingChange(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	got := h.emit(6 * time.Minute)
	if got.RepeatCount != 1 {
		t.Fatalf("lapse = %+v, want reminder 1", got)
	}
	h.c.MarkUndelivered(h.key, got.Change)
	h.msg = "0/6 nodes are available: 6 Insufficient memory."
	got = h.emit(time.Minute)
	if got.Kind != DedupRepeat || got.RepeatCount != 0 || got.Change != RepeatChangeClass {
		t.Fatalf("re-send with a new shape = %+v, want a class change followup", got)
	}
}

// TestRepeat_FastStreamChangeIsAnnounced: a one-minute cadence never
// lets the window lapse; a message change an hour in must still reach
// the session, at the first occurrence a full window after the last
// inject, and reminders follow from there.
func TestRepeat_FastStreamChangeIsAnnounced(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	var injects []DedupResult
	for i := 1; i <= 120; i++ {
		if i == 60 {
			h.msg = "0/6 nodes are available: 6 Insufficient memory."
		}
		switch got := h.emit(time.Minute); got.Kind {
		case DedupRepeat:
			injects = append(injects, got)
		case DedupNewIncident:
			t.Fatalf("minute %d: new incident", i)
		}
	}
	if len(injects) == 0 || injects[0].Change != RepeatChangeClass || injects[0].RepeatCount != 0 {
		t.Fatalf("injects = %+v, want the first to be the class change", injects)
	}
	if len(injects) < 2 || injects[1].RepeatCount != 1 {
		t.Fatalf("injects = %+v, want reminders to follow the change", injects)
	}
}

// TestRepeat_RotatingShapesAreRateLimited: an incident cycling through
// 17 shapes keeps evicting its own classes (cap 16), so every
// occurrence is "new"; change followups still come at most one per
// window.
func TestRepeat_RotatingShapesAreRateLimited(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	h.emit(6 * time.Minute) // reminder 1
	var changes int
	var last time.Time
	for i := range 17 * 6 {
		h.msg = fmt.Sprintf("shape %c failed", 'a'+i%17)
		got := h.emit(30 * time.Second)
		if got.Kind == DedupRepeat && got.RepeatCount == 0 {
			if !last.IsZero() && h.now.Sub(last) <= 5*time.Minute {
				t.Fatalf("change followups %s apart, want more than one window", h.now.Sub(last))
			}
			last = h.now
			changes++
		}
	}
	// 51 minutes of rotation: one change per window at most.
	if changes == 0 || changes > 10 {
		t.Fatalf("changes = %d over 102 rotating occurrences, want 1..10", changes)
	}
}

// TestRepeat_UndeliveredIsResent: an inject that did not reach the
// session is re-sent at the next occurrence — a failed open as the open
// itself, a failed reminder as the same reminder — not skipped.
func TestRepeat_UndeliveredIsResent(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	h.c.MarkUndelivered(h.key, RepeatChangeOpen)
	got := h.emit(time.Minute)
	if got.Kind != DedupRepeat || got.Change != RepeatChangeOpen || got.RepeatCount != 0 {
		t.Fatalf("occurrence after an undelivered open = %+v, want the open re-sent", got)
	}
	if got := h.emit(time.Minute); got.Kind != DedupDuplicate {
		t.Fatalf("after the re-send = %+v, want a duplicate", got)
	}
	got = h.emit(6 * time.Minute)
	if got.RepeatCount != 1 {
		t.Fatalf("lapse = %+v, want reminder 1", got)
	}
	h.c.MarkUndelivered(h.key, got.Change)
	got = h.emit(time.Minute)
	if got.Kind != DedupRepeat || got.RepeatCount != 1 || got.Change != "" {
		t.Fatalf("occurrence after an undelivered reminder = %+v, want reminder 1 again", got)
	}
}

// TestRepeat_UnbindRestartsAsOpen: a session the sink reports gone is
// unbound and the schedule restarts as a fresh open, so the dispatcher
// can reopen it.
func TestRepeat_UnbindRestartsAsOpen(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	h.emit(6 * time.Minute) // reminder 1
	h.c.Unbind(h.key)
	if sid, ok := h.c.LookupSession(h.key); ok {
		t.Fatalf("still bound to %q after Unbind", sid)
	}
	h.c.BindIncident(h.key, "sess-2", IncidentRef{Name: "p"})
	if got := h.emit(time.Minute); got.Kind != DedupDuplicate || got.Absorbed {
		t.Fatalf("after rebind = %+v, want a first-window duplicate", got)
	}
	if got := h.emit(5*time.Minute + time.Second); got.RepeatCount != 1 || got.SessionID != "sess-2" {
		t.Fatalf("first lapse after rebind = %+v, want reminder 1 into sess-2", got)
	}
}

// TestDedup_SnapshotConcurrentWithRepeats: Snapshot encodes deep
// copies, so it is race-free against repeat appending to Classes.
// Meaningful under -race (dev/tools/test-unit runs it so).
func TestDedup_SnapshotConcurrentWithRepeats(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dedup.json")
	c := newTestDedup(t, time.Millisecond, path)
	c.SetOpenCheck(func(EventKey) bool { return true })
	key := EventKey{UID: "u", Reason: "FailedScheduling"}
	base := time.Now()
	c.ObserveSignal(key, Observation{LastTS: base, Kind: KindK8sEvent, Repeatable: true})
	c.BindIncident(key, "sess-1", IncidentRef{Name: "p"})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 300 {
			c.ObserveSignal(key, Observation{
				LastTS: base.Add(time.Duration(i+1) * time.Second), Kind: KindK8sEvent, Repeatable: true,
				Message: fmt.Sprintf("shape %c", 'a'+i%26),
			})
		}
	}()
	for range 50 {
		if err := c.Snapshot(); err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
	}
	<-done
}
