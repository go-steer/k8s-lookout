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

// tombRig is a dedup cache on a settable clock with one resolved
// incident whose Event was last seen at eventTS.
type tombRig struct {
	c       *DedupCache
	now     time.Time
	key     EventKey
	eventTS time.Time
}

func newTombRig(t *testing.T) *tombRig {
	t.Helper()
	r := &tombRig{
		now:     time.Date(2026, 11, 1, 3, 19, 25, 0, time.UTC),
		key:     EventKey{UID: "1329dd83", Reason: "CrashLoopBackOff"},
		eventTS: time.Date(2026, 11, 1, 3, 19, 22, 0, time.UTC),
	}
	r.c = newTestDedup(t, 5*time.Minute, "")
	r.c.SetClock(func() time.Time { return r.now })
	if got := r.c.ObserveSignal(r.key, r.obs(r.eventTS)); got.Kind != DedupNewIncident {
		t.Fatalf("first sighting = %v, want new incident", got.Kind)
	}
	r.c.BindIncident(r.key, "sess-1", IncidentRef{Fingerprint: "fp"})
	r.c.MarkResolved(r.key)
	return r
}

func (r *tombRig) obs(ts time.Time) Observation {
	return Observation{LastTS: ts, Kind: KindK8sEvent, Severity: SeverityCritical, Repeatable: true}
}

// Issue #816: a resolved entry retires one window after its last
// activity, but the Event it was opened from is replayed for up to an
// hour. The replay stays a suppressed duplicate, marked Replay so the
// dispatcher does not retry an open; it is never a new incident.
func TestDedup_RetiredResolvedEntryStillAnswersReplays(t *testing.T) {
	t.Parallel()
	r := newTombRig(t)
	for _, gap := range []time.Duration{6 * time.Minute, 10 * time.Minute, 20 * time.Minute, 20 * time.Minute} {
		r.now = r.now.Add(gap)
		r.c.Sweep()
		got := r.c.ObserveSignal(r.key, r.obs(r.eventTS))
		if got.Kind != DedupDuplicate || !got.Replay || got.SessionID != "" {
			t.Fatalf("relist at +%s = %+v, want a session-less Replay duplicate", r.now.Sub(r.eventTS), got)
		}
	}
	if _, bound := r.c.LookupSession(r.key); bound {
		t.Error("a retired entry still routes to its session")
	}
}

// Bindings (restart seeding of the recovery tracker) retires a resolved
// entry the same way the sweep does.
func TestDedup_BindingsRetiresToATombstone(t *testing.T) {
	t.Parallel()
	r := newTombRig(t)
	r.now = r.now.Add(6 * time.Minute)
	if b := r.c.Bindings(); len(b) != 0 {
		t.Fatalf("Bindings = %v, want none (resolved)", b)
	}
	if got := r.c.ObserveSignal(r.key, r.obs(r.eventTS)); !got.Replay {
		t.Errorf("replay after Bindings retired the entry = %+v, want Replay", got)
	}
}

// The tombstone holds only the Event as it was: an advanced LastTS is
// new activity and opens a new incident.
func TestDedup_TombstoneLetsNewActivityThrough(t *testing.T) {
	t.Parallel()
	r := newTombRig(t)
	r.now = r.now.Add(10 * time.Minute)
	r.c.Sweep()
	if got := r.c.ObserveSignal(r.key, r.obs(r.now)); got.Kind != DedupNewIncident || got.Replay {
		t.Fatalf("advanced LastTS after retirement = %+v, want a new incident", got)
	}
}

// Tombstones expire with the replay TTL (the API server's Event TTL is
// an hour), so the cache does not grow with every incident ever resolved.
func TestDedup_TombstoneExpires(t *testing.T) {
	t.Parallel()
	r := newTombRig(t)
	r.now = r.now.Add(6 * time.Minute)
	r.c.Sweep()
	r.now = r.now.Add(r.c.replayTTL())
	r.c.Sweep()
	if n := len(r.c.tombstones); n != 0 {
		t.Errorf("tombstones after the replay TTL = %d, want 0", n)
	}
}

// Evicting a resolved entry under capacity pressure leaves a tombstone
// too, and the tombstones are bounded by the same cap as the entries.
func TestDedup_EvictionTombstonesAndBound(t *testing.T) {
	t.Parallel()
	r := newTombRig(t)
	r.c.max = 2
	for i := range 6 {
		r.now = r.now.Add(time.Second)
		k := EventKey{UID: fmt.Sprintf("other-%d", i), Reason: "CrashLoopBackOff"}
		r.c.ObserveSignal(k, r.obs(r.now))
		r.c.MarkResolved(k)
	}
	if n := len(r.c.tombstones); n > r.c.max {
		t.Errorf("tombstones = %d, want at most %d", n, r.c.max)
	}
	k := EventKey{UID: "other-5", Reason: "CrashLoopBackOff"}
	if _, live := r.c.entries[k]; !live {
		if _, dead := r.c.tombstones[k]; !dead {
			t.Error("the newest resolved entry is neither live nor tombstoned")
		}
	}
}

// Issue #833: once Series.LastObservedTime populates LastTS, a repeating
// series-only Event advances EventLastTS and reaches the #599 repeat schedule
// on window lapse rather than staying stuck in Case 1 replay.
func TestDedup_SeriesRepeatReachesRepeatSchedule(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	c := newTestDedup(t, 5*time.Minute, "")
	c.SetClock(func() time.Time { return now })
	key := EventKey{UID: "hog-series", Reason: "FailedScheduling"}
	c.SetOpenCheck(func(k EventKey) bool { return k == key })

	obs := func(ts time.Time) Observation {
		return Observation{LastTS: ts, Kind: KindK8sEvent, Severity: SeverityCritical, Repeatable: true}
	}
	if got := c.ObserveSignal(key, obs(now)); got.Kind != DedupNewIncident {
		t.Fatalf("occurrence #1 = %+v, want DedupNewIncident", got)
	}
	c.BindIncident(key, "sess-1", IncidentRef{Fingerprint: "fp"})

	// Occurrence #2 inside the window (LastObservedTime advanced) updates EventLastTS.
	now = now.Add(2 * time.Minute)
	if got := c.ObserveSignal(key, obs(now)); got.Kind != DedupDuplicate || got.Replay {
		t.Fatalf("occurrence #2 = %+v, want active-window DedupDuplicate", got)
	}

	// Series 30m refresh past the dedup window produces a #599 DedupRepeat reminder.
	now = now.Add(30 * time.Minute)
	got := c.ObserveSignal(key, obs(now))
	if got.Kind != DedupRepeat || got.RepeatCount != 1 || got.SessionID != "sess-1" {
		t.Fatalf("series refresh past window = %+v, want DedupRepeat{RepeatCount:1, SessionID:sess-1}", got)
	}
}

// Issue #833: a delayed events.k8s.io/v1 finishSeries flush whose
// lastObservedTime predates resolution (EventLastTS < LastTS <= ResolvedAt)
// arriving BEFORE the resolved entry retires to a tombstone must be suppressed
// as Replay, never reopening or extending the resolved entry's session routing.
func TestDedup_FinishSeriesSuppressedBeforeRetirement(t *testing.T) {
	t.Parallel()

	t.Run("within window", func(t *testing.T) {
		t.Parallel()
		now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
		c := newTestDedup(t, 5*time.Minute, "")
		c.SetClock(func() time.Time { return now })
		key := EventKey{UID: "pod-pre-retire", Reason: "FailedScheduling"}
		obs := func(ts time.Time) Observation {
			return Observation{LastTS: ts, Kind: KindK8sEvent, Severity: SeverityCritical, Repeatable: true}
		}

		// Open at T+0, occurrence #2 at T+2s.
		t1 := now
		c.ObserveSignal(key, obs(t1))
		c.BindIncident(key, "sess-1", IncidentRef{Fingerprint: "fp"})
		t2 := t1.Add(2 * time.Second)
		now = t2
		c.ObserveSignal(key, obs(t2))

		// Pod schedules and resolves at T+2m (ResolvedAt = T+2m).
		t3 := t1.Add(10 * time.Second) // occ #3 batched in scheduler memory before pod scheduled
		resolvedAt := t1.Add(2 * time.Minute)
		now = resolvedAt
		c.MarkResolved(key)

		// Unchanged Case 1 replay on a resolved live entry also returns Replay: true.
		now = t1.Add(2*time.Minute + 30*time.Second)
		if got := c.ObserveSignal(key, obs(t2)); got.Kind != DedupDuplicate || !got.Replay || got.SessionID != "" {
			t.Fatalf("Case 1 replay on resolved entry = %+v, want session-less Replay", got)
		}

		// finishSeries arrives at T+4m50s (< 5m window) carrying LastObservedTime=t3 <= resolvedAt.
		flushArrival := t1.Add(4*time.Minute + 50*time.Second)
		now = flushArrival
		got := c.ObserveSignal(key, obs(t3))
		if got.Kind != DedupDuplicate || !got.Replay || got.SessionID != "" {
			t.Fatalf("pre-retirement finishSeries (< window) = %+v, want session-less Replay", got)
		}
		// Entry stayed on its original LastSeen (t2) so retirement is not delayed,
		// while replayAnchor records flushArrival for the tombstone's renewed 1h lease.
		if e := c.entries[key]; e == nil || !e.EventLastTS.Equal(t3) || !e.LastSeen.Equal(t2) || !e.replayAnchor.Equal(flushArrival) {
			t.Fatalf("entry after <window finishSeries = %+v, want EventLastTS=%s LastSeen=%s replayAnchor=%s", e, t3, t2, flushArrival)
		}

		// Entry retires on schedule at T+5m03s (> 5m after t2), carrying flushArrival as tomb.LastSeen.
		now = t2.Add(5*time.Minute + time.Second)
		c.Sweep()
		if _, live := c.entries[key]; live {
			t.Fatal("resolved entry should retire on its original LastSeen schedule")
		}
		tomb, dead := c.tombstones[key]
		if !dead || !tomb.LastSeen.Equal(flushArrival) || !tomb.EventLastTS.Equal(t3) {
			t.Fatalf("retired tombstone = %+v (dead=%v), want EventLastTS=%s LastSeen=%s", tomb, dead, t3, flushArrival)
		}

		// Relist at flush+59m (> 1h after t2, < 1h after flushArrival): still suppressed as Replay
		// and does not extend tomb.LastSeen.
		now = flushArrival.Add(59 * time.Minute)
		c.Sweep()
		if got := c.ObserveSignal(key, obs(t3)); got.Kind != DedupDuplicate || !got.Replay || got.SessionID != "" {
			t.Fatalf("relist at flush+59m = %+v, want session-less Replay", got)
		}
		if tomb := c.tombstones[key]; !tomb.LastSeen.Equal(flushArrival) {
			t.Errorf("unchanged relist extended tomb.LastSeen to %s, want %s", tomb.LastSeen, flushArrival)
		}

		// Past 1h after flushArrival: tombstone expires cleanly.
		now = flushArrival.Add(c.replayTTL() + time.Second)
		c.Sweep()
		if len(c.tombstones) != 0 {
			t.Errorf("tombstones after renewed replay TTL = %d, want 0", len(c.tombstones))
		}
	})

	t.Run("past window before sweep", func(t *testing.T) {
		t.Parallel()
		now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
		c := newTestDedup(t, 5*time.Minute, "")
		c.SetClock(func() time.Time { return now })
		key := EventKey{UID: "pod-pre-sweep", Reason: "FailedScheduling"}
		obs := func(ts time.Time) Observation {
			return Observation{LastTS: ts, Kind: KindK8sEvent, Severity: SeverityCritical, Repeatable: true}
		}

		t1 := now
		c.ObserveSignal(key, obs(t1))
		c.BindIncident(key, "sess-1", IncidentRef{Fingerprint: "fp"})
		t3 := t1.Add(10 * time.Second)
		now = t1.Add(2 * time.Minute)
		c.MarkResolved(key)

		// Advance past 5m window (T+7m), but simulate maybeSweepLocked having run
		// recently (at T+4m) so the resolved entry is still in c.entries when
		// finishSeries arrives at T+7m.
		now = t1.Add(7 * time.Minute)
		c.lastSweep = t1.Add(4 * time.Minute)
		got := c.ObserveSignal(key, obs(t3))
		if got.Kind != DedupDuplicate || !got.Replay || got.SessionID != "" {
			t.Fatalf("pre-sweep finishSeries (> window) = %+v, want session-less Replay", got)
		}
		if _, live := c.entries[key]; live {
			t.Error("lapsed resolved entry must retire immediately on finishSeries")
		}
		tomb, dead := c.tombstones[key]
		if !dead {
			t.Fatal("retired tombstone missing after pre-sweep finishSeries")
		}
		if !tomb.EventLastTS.Equal(t3) || !tomb.LastSeen.Equal(now) || !tomb.ResolvedAt.Equal(t1.Add(2*time.Minute)) {
			t.Errorf("tombstone = %+v, want EventLastTS=%s LastSeen=%s ResolvedAt=%s", tomb, t3, now, t1.Add(2*time.Minute))
		}
	})
}

// Issue #833: a finishSeries flush arriving AFTER the resolved entry retired
// to a tombstone is suppressed as Replay and refreshes the tombstone's TTL
// anchor once to cover the apiserver's renewed 1-hour Event lease. Subsequent
// unchanged relists do NOT extend the TTL, so the tombstone still expires
// after 1 hour of silence past the finishSeries write.
func TestDedup_FinishSeriesSuppressedAfterRetirementAndRenewsTTL(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	c := newTestDedup(t, 5*time.Minute, "")
	c.SetClock(func() time.Time { return now })
	key := EventKey{UID: "pod-post-retire", Reason: "FailedScheduling"}
	obs := func(ts time.Time) Observation {
		return Observation{LastTS: ts, Kind: KindK8sEvent, Severity: SeverityCritical, Repeatable: true}
	}

	t0 := now
	c.ObserveSignal(key, obs(t0))
	c.BindIncident(key, "sess-1", IncidentRef{Fingerprint: "fp"})
	t3 := t0.Add(15 * time.Second)

	// Resolves at T+2m; sweeps into tombstone at T+6m.
	now = t0.Add(2 * time.Minute)
	c.MarkResolved(key)
	now = t0.Add(6 * time.Minute)
	c.Sweep()

	// finishSeries arrives at T+8m (past retirement) with LastObservedTime=t3 <= ResolvedAt.
	finishArrival := t0.Add(8 * time.Minute)
	now = finishArrival
	got := c.ObserveSignal(key, obs(t3))
	if got.Kind != DedupDuplicate || !got.Replay || got.SessionID != "" {
		t.Fatalf("post-retirement finishSeries = %+v, want session-less Replay", got)
	}
	if tomb := c.tombstones[key]; !tomb.EventLastTS.Equal(t3) || !tomb.LastSeen.Equal(finishArrival) {
		t.Fatalf("tombstone after finishSeries = %+v, want EventLastTS=%s LastSeen=%s", tomb, t3, finishArrival)
	}

	// Relist at T+64m: >1h past original open (T+0), but <1h past finishSeries (T+8m).
	// Must still be suppressed AND must NOT bump tomb.LastSeen.
	now = t0.Add(64 * time.Minute)
	c.Sweep()
	got = c.ObserveSignal(key, obs(t3))
	if got.Kind != DedupDuplicate || !got.Replay || got.SessionID != "" {
		t.Fatalf("relist at +64m = %+v, want Replay (renewed Event TTL)", got)
	}
	if tomb := c.tombstones[key]; !tomb.LastSeen.Equal(finishArrival) {
		t.Errorf("unchanged relist bumped tomb.LastSeen to %s, want unchanged %s", tomb.LastSeen, finishArrival)
	}

	// Past 1h after finishSeries (T+69m): tombstone expires cleanly.
	now = finishArrival.Add(c.replayTTL() + time.Second)
	c.Sweep()
	if n := len(c.tombstones); n != 0 {
		t.Errorf("tombstones after renewed replay TTL = %d, want 0", n)
	}
}

// Issue #833: --dedup-persist Snapshot/restore round-trip preserves ResolvedAt
// on a resolved entry so a finishSeries flush after restart stays suppressed.
func TestDedup_ResolvedAtPersistsAcrossSnapshotRestore(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dedup.json")
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	c1 := newTestDedup(t, 5*time.Minute, path)
	c1.SetClock(func() time.Time { return now })
	key := EventKey{UID: "pod-persist", Reason: "FailedScheduling"}
	obs := func(ts time.Time) Observation {
		return Observation{LastTS: ts, Kind: KindK8sEvent, Severity: SeverityCritical, Repeatable: true}
	}

	t0 := now
	c1.ObserveSignal(key, obs(t0))
	c1.BindIncident(key, "sess-1", IncidentRef{Fingerprint: "fp"})
	now = t0.Add(2 * time.Minute)
	c1.MarkResolved(key)
	if err := c1.Snapshot(); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Restart into a fresh cache loaded from the snapshot.
	c2, err := NewDedupCache(5*time.Minute, path)
	if err != nil {
		t.Fatalf("restore NewDedupCache: %v", err)
	}
	now = t0.Add(8 * time.Minute)
	c2.SetClock(func() time.Time { return now })

	// finishSeries with LastObservedTime before ResolvedAt stays suppressed after restore.
	got := c2.ObserveSignal(key, obs(t0.Add(30*time.Second)))
	if got.Kind != DedupDuplicate || !got.Replay || got.SessionID != "" {
		t.Fatalf("finishSeries after snapshot restore = %+v, want session-less Replay", got)
	}
}

// Issue #833: genuinely new activity strictly after ResolvedAt opens a fresh
// incident whether the resolved entry is still in c.entries (lapsed) or retired
// to a tombstone.
func TestDedup_PostResolveActivityAfterResolvedAtOpensFresh(t *testing.T) {
	t.Parallel()

	t.Run("from tombstone", func(t *testing.T) {
		t.Parallel()
		r := newTombRig(t)
		r.now = r.now.Add(10 * time.Minute)
		r.c.Sweep()
		if got := r.c.ObserveSignal(r.key, r.obs(r.now)); got.Kind != DedupNewIncident || got.Replay {
			t.Fatalf("activity strictly after ResolvedAt (tombstone) = %+v, want DedupNewIncident", got)
		}
	})

	t.Run("from lapsed live resolved entry", func(t *testing.T) {
		t.Parallel()
		r := newTombRig(t)
		r.now = r.now.Add(7 * time.Minute)
		r.c.lastSweep = r.now.Add(-time.Minute) // keep entry live in c.entries past window
		if got := r.c.ObserveSignal(r.key, r.obs(r.now)); got.Kind != DedupNewIncident || got.Replay {
			t.Fatalf("activity strictly after ResolvedAt (live lapsed entry) = %+v, want DedupNewIncident", got)
		}
	})

	t.Run("within window on live resolved entry stays Case 4", func(t *testing.T) {
		t.Parallel()
		r := newTombRig(t)
		r.now = r.now.Add(2 * time.Minute)
		got := r.c.ObserveSignal(r.key, r.obs(r.now))
		if got.Kind != DedupDuplicate || got.Replay || got.SessionID != "sess-1" {
			t.Fatalf("activity strictly after ResolvedAt within window = %+v, want Case 4 DedupDuplicate{SessionID:sess-1, Replay:false}", got)
		}
	})
}

// Issue #833: MarkResolved updates ResolvedAt on every KindResolved transition
// (covering resolved → resolved.reverted → second resolved), whereas
// MarkResolvedIfUnresolved preserves an earlier ResolvedAt (sibling-fold rootKey).
func TestDedup_MarkResolvedUpdatesAndIfUnresolvedPreserves(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	c := newTestDedup(t, 5*time.Minute, "")
	c.SetClock(func() time.Time { return now })
	obs := func(ts time.Time) Observation {
		return Observation{LastTS: ts, Kind: KindK8sEvent, Severity: SeverityCritical, Repeatable: true}
	}

	// 1. resolved at T+2m → reverted → second resolved at T+4m: ResolvedAt advances to T+4m.
	kRe := EventKey{UID: "pod-revert", Reason: "FailedScheduling"}
	c.ObserveSignal(kRe, obs(now))
	now = now.Add(2 * time.Minute)
	c.MarkResolved(kRe)
	tFirstResolve := now
	now = now.Add(2 * time.Minute)
	c.MarkResolved(kRe)
	tSecondResolve := now
	if got := c.entries[kRe].ResolvedAt; !got.Equal(tSecondResolve) {
		t.Fatalf("second MarkResolved ResolvedAt = %s, want %s", got, tSecondResolve)
	}
	// finishSeries timestamp between T+2m and T+4m is suppressed.
	now = now.Add(4 * time.Minute)
	if got := c.ObserveSignal(kRe, obs(tFirstResolve.Add(30*time.Second))); got.Kind != DedupDuplicate || !got.Replay {
		t.Fatalf("finishSeries between first and second resolve = %+v, want Replay", got)
	}

	// 2. MarkResolvedIfUnresolved preserves rootKey's original ResolvedAt.
	tStart := time.Date(2026, 11, 18, 13, 0, 0, 0, time.UTC)
	now = tStart
	kRoot := EventKey{UID: "pod-root", Reason: "FailedScheduling"}
	c.ObserveSignal(kRoot, obs(tStart))
	now = tStart.Add(1 * time.Minute)
	c.MarkResolved(kRoot)
	rootResolvedAt := now
	now = tStart.Add(4 * time.Minute)
	c.MarkResolvedIfUnresolved(kRoot)
	if got := c.entries[kRoot].ResolvedAt; !got.Equal(rootResolvedAt) {
		t.Fatalf("MarkResolvedIfUnresolved overwrote ResolvedAt to %s, want %s", got, rootResolvedAt)
	}
}
