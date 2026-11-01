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
