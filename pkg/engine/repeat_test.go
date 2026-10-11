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
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The repeat schedule (issue #599): a lapse on an incident that is
// still open is a followup into its session on a doubling schedule,
// never a second open.

const soakMessage = "0/6 nodes are available: 3 node(s) didn't match pod anti-affinity rules, 3 node(s) didn't match Pod's node affinity/selector. preemption: 0/6 nodes are available: 6 Preemption is not helpful for scheduling."

// repeatHarness drives one key through a DedupCache on a fake clock
// with an open check the test controls.
type repeatHarness struct {
	t     *testing.T
	c     *DedupCache
	key   EventKey
	now   time.Time
	ts    time.Time
	open  bool
	sev   Severity
	msg   string
	kind  string
	start time.Time
}

func newRepeatHarness(t *testing.T, persistPath string) *repeatHarness {
	t.Helper()
	h := &repeatHarness{
		t:    t,
		key:  EventKey{UID: "pod-cf29j", Reason: "FailedScheduling"},
		now:  time.Date(2026, 10, 9, 16, 56, 19, 0, time.UTC),
		open: true,
		sev:  SeverityCritical,
		msg:  soakMessage,
		kind: KindK8sEvent,
	}
	h.start = h.now
	h.c = newTestDedup(t, 5*time.Minute, persistPath)
	h.wire()
	return h
}

// wire points a (possibly fresh) cache at the harness clock and open
// flag.
func (h *repeatHarness) wire() {
	h.c.SetClock(func() time.Time { return h.now })
	h.c.SetOpenCheck(func(EventKey) bool { return h.open })
}

// emit advances the clock by gap and observes one new occurrence.
func (h *repeatHarness) emit(gap time.Duration) DedupResult {
	h.now = h.now.Add(gap)
	h.ts = h.now
	return h.c.ObserveSignal(h.key, Observation{
		LastTS: h.ts, Kind: h.kind, Severity: h.sev, Message: h.msg, Repeatable: true,
	})
}

// openIncident fires the first event and binds it the way the
// dispatcher does after a successful open.
func (h *repeatHarness) openIncident() {
	h.t.Helper()
	if got := h.emit(0); got.Kind != DedupNewIncident {
		h.t.Fatalf("first event: kind = %v, want DedupNewIncident", got.Kind)
	}
	h.c.BindIncident(h.key, "sess-1", IncidentRef{Namespace: "triage-lab", KindOfObject: "Pod", Name: "batch-worker-664c6c98fb-cf29j"})
}

func TestRepeatInterval(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		window time.Duration
		n      int
		want   time.Duration
	}{
		{5 * time.Minute, 1, 5 * time.Minute},
		{5 * time.Minute, 2, 10 * time.Minute},
		{5 * time.Minute, 3, 20 * time.Minute},
		{5 * time.Minute, 4, 40 * time.Minute},
		{5 * time.Minute, 5, time.Hour},
		{5 * time.Minute, 6, time.Hour},
		{5 * time.Minute, 60, time.Hour},
		{time.Minute, 7, time.Hour},
		{2 * time.Hour, 1, 2 * time.Hour}, // a window over the cap is its own cap
		{2 * time.Hour, 4, 2 * time.Hour},
	} {
		if got := repeatInterval(tc.window, tc.n); got != tc.want {
			t.Errorf("repeatInterval(%s, %d) = %s, want %s", tc.window, tc.n, got, tc.want)
		}
	}
}

// TestRepeat_SoakScenario is the #585 soak at cache scale: one pod the
// scheduler re-queues every ~5m (just past the 5m window) for six
// hours. Before #599 that was a new incident per event (~70 here, 225
// in the 24h soak). Now: ONE open, then reminders whose gaps follow
// 5m, 10m, 20m, 40m, 1h, 1h… and whose repeat_count counts up, with
// every other occurrence absorbed.
func TestRepeat_SoakScenario(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	const cadence = 5*time.Minute + 5*time.Second

	var opens, absorbed int
	var reminders []DedupResult
	var at []time.Time
	for h.now.Sub(h.start) < 6*time.Hour {
		got := h.emit(cadence)
		switch got.Kind {
		case DedupNewIncident:
			opens++
		case DedupRepeat:
			if got.RepeatCount == 0 {
				t.Fatalf("unchanged signal produced a material-change followup at %s (change=%q)", h.now.Sub(h.start), got.Change)
			}
			if got.SessionID != "sess-1" {
				t.Errorf("reminder routed to %q, want the bound sess-1", got.SessionID)
			}
			reminders = append(reminders, got)
			at = append(at, h.now)
		case DedupDuplicate:
			if !got.Absorbed {
				t.Errorf("occurrence at %s past the first window is a duplicate but not marked absorbed", h.now.Sub(h.start))
			}
			absorbed++
		}
	}
	if opens != 0 {
		t.Fatalf("still-open incident opened %d more times in 6h, want 0 (the #599 bug)", opens)
	}
	want := []time.Duration{5 * time.Minute, 10 * time.Minute, 20 * time.Minute, 40 * time.Minute, time.Hour, time.Hour, time.Hour}
	if len(reminders) < len(want) {
		t.Fatalf("got %d reminders in 6h, want at least %d", len(reminders), len(want))
	}
	prev := h.start
	for i, w := range want {
		if reminders[i].RepeatCount != i+1 {
			t.Errorf("reminder %d: repeat_count = %d, want %d", i, reminders[i].RepeatCount, i+1)
		}
		gap := at[i].Sub(prev)
		if gap <= w || gap > w+cadence {
			t.Errorf("reminder %d came %s after the previous inject, want just over %s (within one %s cadence)", i+1, gap, w, cadence)
		}
		prev = at[i]
	}
	if absorbed == 0 {
		t.Error("no occurrence was absorbed between reminders")
	}
	// The count keeps every occurrence since the open: 1 + every emit.
	if lastR := reminders[len(reminders)-1]; lastR.Count < 60 {
		t.Errorf("last reminder count = %d, want every occurrence since the open (≥60)", lastR.Count)
	}
}

// TestRepeat_FirstWindowIsPlainDuplicate: inside the incident's first
// window nothing changes — duplicates are suppressed, not absorbed —
// and the first lapse is reminder 1.
func TestRepeat_FirstWindowIsPlainDuplicate(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	got := h.emit(time.Minute)
	if got.Kind != DedupDuplicate || got.Absorbed {
		t.Fatalf("first-window event = %+v, want a plain (non-absorbed) duplicate", got)
	}
	got = h.emit(5*time.Minute + time.Second)
	if got.Kind != DedupRepeat || got.RepeatCount != 1 {
		t.Fatalf("first lapse = %+v, want reminder 1", got)
	}
}

// TestRepeat_MaterialChangeRestartsNumbering: a new message shape in
// the backoff is a RepeatCount-0 followup once past its floor. The next
// reminder is numbered 1 again, but the change took a backoff step, so
// it comes at the following step's interval (40m), not one window.
func TestRepeat_MaterialChangeRestartsNumbering(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	const cadence = 5*time.Minute + 5*time.Second
	for range 4 { // reminders 1 and 2, with absorbed events between
		h.emit(cadence)
	}
	h.msg = "0/6 nodes are available: 6 Insufficient cpu."
	// Held until max(2 windows, reminder 3's 20m interval) after
	// reminder 2 (at 15m15s).
	if got := h.emit(time.Minute); got.Kind != DedupDuplicate || !got.Absorbed {
		t.Fatalf("change inside the floor = %+v, want it held", got)
	}
	got := h.emit(14 * time.Minute)
	if got.Kind != DedupRepeat || got.RepeatCount != 0 || got.Change != RepeatChangeClass {
		t.Fatalf("new message shape = %+v, want a material-change followup (repeat 0, change=class)", got)
	}
	// Absorbed through step 4's 40m interval, then reminder 1.
	for range 7 { // 7 × 5m05s = 35m35s
		if got := h.emit(cadence); got.Kind != DedupDuplicate || !got.Absorbed {
			t.Fatalf("event inside step 4's interval = %+v, want absorbed", got)
		}
	}
	if got := h.emit(cadence); got.Kind != DedupRepeat || got.RepeatCount != 1 {
		t.Fatalf("first reminder after the change = %+v, want reminder 1 at ~40m", got)
	}
}

// TestRepeat_SeverityChangeIsMaterial: the same message at a different
// severity is a new class.
func TestRepeat_SeverityChangeIsMaterial(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	h.emit(6 * time.Minute) // reminder 1
	h.sev = SeverityWarning
	if got := h.emit(time.Minute); got.Kind != DedupDuplicate || !got.Absorbed {
		t.Fatalf("severity change within a window of the last inject = %+v, want it held", got)
	}
	// Reminder 1 was the last inject, so the floor is max(2 windows,
	// reminder 2's 10m interval) = 10m — and it supersedes reminder 2.
	if got := h.emit(5 * time.Minute); got.Kind != DedupDuplicate || !got.Absorbed {
		t.Fatalf("held severity change at 6m = %+v, want it still held", got)
	}
	if got := h.emit(5 * time.Minute); got.Kind != DedupRepeat || got.RepeatCount != 0 || got.Change != RepeatChangeClass {
		t.Fatalf("held severity change = %+v, want a class change followup past the floor", got)
	}
}

// TestRepeat_DigitsAndAlternationAreNotChanges: counts and durations
// inside a message, and a family that alternates between known message
// shapes (kubelet's pull cycle), are the same incident.
func TestRepeat_DigitsAndAlternationAreNotChanges(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.key = EventKey{UID: "pod-pull", Reason: "ImagePullBackOff"}
	h.msg = `Failed to pull image "r.example/app:v3": rpc error: code = NotFound`
	h.openIncident()
	h.msg = `Back-off pulling image "r.example/app:v3"`
	h.emit(30 * time.Second) // learned in the first window
	h.emit(6 * time.Minute)  // reminder 1
	for i := range 12 {
		if i%2 == 0 {
			h.msg = `Failed to pull image "r.example/app:v3": rpc error: code = NotFound`
		} else {
			h.msg = `Back-off pulling image "r.example/app:v3"`
		}
		if got := h.emit(time.Minute); got.Kind == DedupRepeat && got.RepeatCount == 0 {
			t.Fatalf("alternating known shapes produced a change at step %d", i)
		}
	}
	if messageShape("0/6 nodes are available: 3 node(s)") != messageShape("0/7 nodes are available: 12 node(s)") {
		t.Error("digit runs must not change a message's shape")
	}
}

// TestRepeat_CountJump: a failure whose rate jumps 4x or more over the
// reference, by at least repeatJumpMin occurrences, is material; the
// new rate then becomes the reference, so it does not fire again.
func TestRepeat_CountJump(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	const cadence = 5*time.Minute + 5*time.Second
	h.emit(cadence) // reminder 1: reference ≈ 12/h
	// A burst: one occurrence every 20s.
	var change *DedupResult
	for i := range 20 {
		got := h.emit(20 * time.Second)
		if got.Kind == DedupRepeat {
			if got.RepeatCount != 0 || got.Change != RepeatChangeRate {
				t.Fatalf("burst step %d: %+v, want a rate change", i, got)
			}
			if change != nil {
				t.Fatalf("burst fired a second rate change at step %d; the jumped rate must become the reference", i)
			}
			change = &got
			if i+1 < repeatJumpMin {
				t.Fatalf("rate change after %d occurrences, below the %d floor", i+1, repeatJumpMin)
			}
		}
	}
	if change == nil {
		t.Fatal("a 15x rate jump over 20 occurrences produced no change followup")
	}
	// A steady stream at the old cadence never trips it.
	h2 := newRepeatHarness(t, "")
	h2.openIncident()
	for range 80 {
		if got := h2.emit(cadence); got.Kind == DedupRepeat && got.RepeatCount == 0 {
			t.Fatalf("steady cadence produced a change followup: %+v", got)
		}
	}
}

// TestRepeat_ResolvedRecurrenceOpensFresh: after §7.4 resolves the
// incident, its recurrence is a new incident — whether the cache was
// told (MarkResolved) or the tracker no longer holds it open.
func TestRepeat_ResolvedRecurrenceOpensFresh(t *testing.T) {
	t.Parallel()
	t.Run("marked resolved", func(t *testing.T) {
		t.Parallel()
		h := newRepeatHarness(t, "")
		h.openIncident()
		h.emit(6 * time.Minute) // reminder 1
		h.c.MarkResolved(h.key)
		if got := h.emit(20 * time.Minute); got.Kind != DedupNewIncident {
			t.Fatalf("recurrence after resolve = %+v, want DedupNewIncident", got)
		}
		// The fresh incident schedules from scratch.
		h.c.BindIncident(h.key, "sess-2", IncidentRef{Name: "p"})
		if got := h.emit(6 * time.Minute); got.Kind != DedupRepeat || got.RepeatCount != 1 || got.SessionID != "sess-2" {
			t.Fatalf("lapse on the fresh incident = %+v, want reminder 1 into sess-2", got)
		}
	})
	t.Run("tracker says closed", func(t *testing.T) {
		t.Parallel()
		h := newRepeatHarness(t, "")
		h.openIncident()
		h.emit(6 * time.Minute) // reminder 1
		h.open = false
		if got := h.emit(6 * time.Minute); got.Kind != DedupNewIncident {
			t.Fatalf("lapse with the incident closed = %+v, want DedupNewIncident", got)
		}
	})
}

// TestRepeat_PreconditionsKeepLegacyLapse: every way the incident is
// not known to be open keeps the pre-#599 lapse-and-reopen.
func TestRepeat_PreconditionsKeepLegacyLapse(t *testing.T) {
	t.Parallel()
	ref := IncidentRef{Name: "p"}
	for _, tc := range []struct {
		name       string
		openCheck  bool // install the open check at all
		bind       bool
		openedAs   bool // Repeatable on the opening event
		repeatable bool // Repeatable on the lapsing event
	}{
		{"recovery disabled (no open check)", false, true, true, true},
		{"never bound", true, false, true, true},
		{"opened by a non-session class (watchboard/info)", true, true, false, true},
		{"lapsing signal is not session class (downgraded)", true, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newTestDedup(t, 5*time.Minute, "")
			now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
			c.SetClock(func() time.Time { return now })
			if tc.openCheck {
				c.SetOpenCheck(func(EventKey) bool { return true })
			}
			key := EventKey{UID: "u", Reason: "FailedScheduling"}
			c.ObserveSignal(key, Observation{LastTS: now, Kind: KindK8sEvent, Repeatable: tc.openedAs})
			if tc.bind {
				c.BindIncident(key, "sess-1", ref)
			}
			now = now.Add(6 * time.Minute)
			got := c.ObserveSignal(key, Observation{LastTS: now, Kind: KindK8sEvent, Repeatable: tc.repeatable, Downgraded: !tc.repeatable})
			if got.Kind != DedupNewIncident {
				t.Fatalf("lapse = %+v, want the legacy DedupNewIncident", got)
			}
		})
	}
	// And the legacy Observe entry point never repeats.
	c := newTestDedup(t, 5*time.Minute, "")
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	c.SetClock(func() time.Time { return now })
	c.SetOpenCheck(func(EventKey) bool { return true })
	key := EventKey{UID: "u", Reason: "FailedScheduling"}
	c.Observe(key, now)
	c.BindIncident(key, "sess-1", ref)
	now = now.Add(6 * time.Minute)
	if got := c.Observe(key, now); got.Kind != DedupNewIncident {
		t.Fatalf("Observe lapse = %+v, want DedupNewIncident", got)
	}
}

// TestRepeat_ReplaySuppressionUnchanged: an informer re-list replaying
// an already-processed Event (same LastTimestamp) past the window is
// still a replay — no reminder, no new incident, LastSeen untouched.
func TestRepeat_ReplaySuppressionUnchanged(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	h.emit(6 * time.Minute) // reminder 1
	replayTS := h.ts
	for range 5 {
		h.now = h.now.Add(20 * time.Minute)
		got := h.c.ObserveSignal(h.key, Observation{LastTS: replayTS, Kind: h.kind, Severity: h.sev, Message: h.msg, Repeatable: true})
		if got.Kind != DedupDuplicate || got.Absorbed {
			t.Fatalf("replay = %+v, want a plain duplicate", got)
		}
	}
	h.c.mu.Lock()
	e := h.c.entries[h.key]
	lastSeen, lastInject := e.LastSeen, e.LastInjectAt
	h.c.mu.Unlock()
	if !lastSeen.Equal(replayTS) || !lastInject.Equal(replayTS) {
		t.Errorf("replays advanced the schedule: LastSeen=%s LastInjectAt=%s, want both %s", lastSeen, lastInject, replayTS)
	}
	// Genuinely new activity afterwards is reminder 2 (the 10m step
	// long passed), not a new incident.
	if got := h.emit(time.Minute); got.Kind != DedupRepeat || got.RepeatCount != 2 {
		t.Fatalf("new activity after replays = %+v, want reminder 2", got)
	}
}

// TestRepeat_RestartMidBackoffResumes: the schedule rides the dedup
// snapshot, so a restart between reminders 3 and 4 resumes at reminder
// 4, due 40m after reminder 3 — not a new incident and not reminder 1.
func TestRepeat_RestartMidBackoffResumes(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dedup.json")
	h := newRepeatHarness(t, path)
	h.openIncident()
	const cadence = 5*time.Minute + 5*time.Second
	var third time.Time
	for third.IsZero() {
		if got := h.emit(cadence); got.Kind == DedupRepeat && got.RepeatCount == 3 {
			third = h.now
		}
	}
	h.emit(cadence) // absorbed, after reminder 3
	if err := h.c.Snapshot(); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	h.c = newTestDedup(t, 5*time.Minute, path) // the restart
	h.wire()
	for {
		got := h.emit(cadence)
		if got.Kind == DedupNewIncident {
			t.Fatal("restart mid-backoff opened a new incident")
		}
		if got.Kind == DedupRepeat {
			if got.RepeatCount != 4 {
				t.Fatalf("first inject after restart = %+v, want reminder 4", got)
			}
			if gap := h.now.Sub(third); gap <= 40*time.Minute || gap > 40*time.Minute+cadence {
				t.Fatalf("reminder 4 came %s after reminder 3, want just over 40m", gap)
			}
			if got.SessionID != "sess-1" {
				t.Fatalf("reminder 4 routed to %q, want sess-1", got.SessionID)
			}
			return
		}
	}
}

// TestRepeat_LegacySnapshotTolerance: a snapshot written before #599
// carries none of the schedule fields. Its bound entry reopens once on
// the next lapse (it cannot prove it was opened by a session-class
// signal), and the entry that replaces it repeats normally. A legacy
// entry that is session-class (hand-set here) adopts its first class
// silently rather than reading it as a change.
func TestRepeat_LegacySnapshotTolerance(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		sessionClass string
		want         DedupResultKind
	}{
		{"pre-599 entry", "", DedupNewIncident},
		{"session-class entry without schedule fields", `"session_class": true,`, DedupRepeat},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "dedup.json")
			old := `{
  "pod-cf29j|FailedScheduling": {
    "session_id": "sess-old",
    "first_seen": "2026-10-09T16:56:19Z",
    "last_seen": "2026-10-09T16:56:19Z",
    "event_last_ts": "2026-10-09T16:56:19Z",
    "count": 1,` + tc.sessionClass + `
    "incident": {"namespace": "triage-lab", "kind_of_object": "Pod", "name": "batch-worker"}
  }
}`
			if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
				t.Fatalf("write legacy snapshot: %v", err)
			}
			h := newRepeatHarness(t, path)
			got := h.emit(6 * time.Minute)
			if got.Kind != tc.want {
				t.Fatalf("first lapse after restore = %+v, want kind %v", got, tc.want)
			}
			if got.Kind == DedupRepeat && got.RepeatCount != 1 {
				t.Fatalf("legacy session-class entry: %+v, want reminder 1 (no spurious change)", got)
			}
		})
	}
}

// TestStorm_NoteMemberActivityKeepsStormAlive: a still-failing member
// that now repeats instead of re-firing must keep its storm open past
// the idle TTL (the pre-#599 re-fire did this as a side effect).
func TestStorm_NoteMemberActivityKeepsStormAlive(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	c, now := newTestCorrelator(t, DefaultStormWindow, 3, res)
	var key EventKey
	for i := range 3 {
		sig := podSignal(i, "shop")
		res.byObject[sig.ref()] = []Ancestor{ancNode}
		key = sig.CanonicalKey()
		c.Observe(sig)
	}
	if c.ActiveStorms() != 1 {
		t.Fatalf("setup: %d storms, want 1", c.ActiveStorms())
	}
	for range 4 { // 4 × 20m = 80m, well past the 30m idle TTL
		*now = now.Add(20 * time.Minute)
		c.NoteMemberActivity(key, *now)
		c.mu.Lock()
		c.prune(*now)
		c.mu.Unlock()
	}
	if c.ActiveStorms() != 1 {
		t.Fatal("storm closed while a member kept reporting activity")
	}
	*now = now.Add(31 * time.Minute)
	c.mu.Lock()
	c.prune(*now)
	c.mu.Unlock()
	if c.ActiveStorms() != 0 {
		t.Fatal("storm stayed open past the idle TTL with no member activity")
	}
}

// TestRepeat_FirstLapseClassChangeAtSingleWindow verifies #641: when a
// new signal class appears in window 2, it is announced at the first
// reminder lapse (1*window = 5m after open), not delayed until 2*window.
func TestRepeat_FirstLapseClassChangeAtSingleWindow(t *testing.T) {
	t.Parallel()
	h := newRepeatHarness(t, "")
	h.openIncident()
	h.msg = "new failure shape after opening window"
	got := h.emit(5*time.Minute + time.Second)
	if got.Kind != DedupRepeat || got.Change != RepeatChangeClass || got.RepeatCount != 0 {
		t.Fatalf("first lapse class change = %+v, want DedupRepeat with Change=%q RepeatCount=0 at 5m01s", got, RepeatChangeClass)
	}
}

// TestRepeat_SiblingFoldCoalescesSchedule verifies #641: multiple sibling
// pods folded onto one per-incident session share a single doubling
// repeat schedule instead of fanning out N parallel reminder schedules.
func TestRepeat_SiblingFoldCoalescesSchedule(t *testing.T) {
	t.Parallel()
	const window = 5 * time.Minute
	now := time.Unix(1_700_000_000, 0).UTC()
	c, err := NewDedupCache(window, "")
	if err != nil {
		t.Fatalf("NewDedupCache: %v", err)
	}
	c.SetClock(func() time.Time { return now })
	openPods := map[EventKey]bool{}
	c.SetOpenCheck(func(k EventKey) bool { return openPods[k] })

	deploy := "Deployment/shop/api"
	root := EventKey{UID: "pod-0", Reason: "CrashLoopBackOff"}
	openPods[root] = true
	obs := func() Observation {
		return Observation{
			LastTS:     now,
			Kind:       KindK8sEvent,
			Severity:   SeverityCritical,
			Message:    "Back-off restarting failed container",
			Repeatable: true,
		}
	}
	res := c.ObserveSignal(root, obs())
	if res.Kind != DedupNewIncident {
		t.Fatalf("root open = %+v, want DedupNewIncident", res)
	}
	c.NoteIncidentKind(root, KindK8sEvent)
	c.BindIncident(root, "sess-1", IncidentRef{Fingerprint: "fp-api"})
	c.NoteAncestors(root, []string{deploy})

	siblings := make([]EventKey, 0, 4)
	for i := 1; i <= 4; i++ {
		now = now.Add(time.Second)
		k := EventKey{UID: fmt.Sprintf("pod-%d", i), Reason: "CrashLoopBackOff"}
		openPods[k] = true
		c.ObserveSignal(k, obs())
		sid, _, ok := c.SessionForSibling(k, KindK8sEvent, []string{deploy})
		if !ok || sid != "sess-1" {
			t.Fatalf("sibling %d did not fold: sid=%q ok=%v", i, sid, ok)
		}
		c.NoteIncidentKind(k, KindK8sEvent)
		c.BindIncident(k, sid, IncidentRef{Fingerprint: "fp-api"})
		c.NoteAncestors(k, []string{deploy})
		siblings = append(siblings, k)
	}

	// Across 7 × (5m05s) = 35m35s of all 5 sibling pods lapsing,
	// reminders must follow ONE doubling schedule (5m, +10m, +20m => 3
	// reminders total), never 5 × 3 = 15 reminders.
	var reminders int
	allKeys := append([]EventKey{root}, siblings...)
	for round := 0; round < 7; round++ {
		now = now.Add(5*time.Minute + 5*time.Second)
		for _, k := range allKeys {
			now = now.Add(time.Second)
			r := c.ObserveSignal(k, obs())
			if r.Kind == DedupRepeat {
				reminders++
			}
		}
	}
	if reminders != 3 {
		t.Fatalf("coalesced reminders across 5 sibling pods over ~36m = %d, want 3 (5m, 10m, 20m)", reminders)
	}
}

