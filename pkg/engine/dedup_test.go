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
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTestDedup(t *testing.T, window time.Duration, persistPath string) *DedupCache {
	t.Helper()
	c, err := NewDedupCache(window, persistPath)
	if err != nil {
		t.Fatalf("NewDedupCache: %v", err)
	}
	return c
}

// tsGen returns a helper that yields monotonically increasing
// timestamps starting at base. Used by tests that need distinct
// k8s Event.LastTimestamp values across Observe calls without
// caring about their absolute values.
func tsGen(base time.Time, step time.Duration) func() time.Time {
	t := base
	return func() time.Time {
		t = t.Add(step)
		return t
	}
}

func TestDedup_FirstEvent_IsNewIncident(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	got := c.Observe(EventKey{UID: "u1", Reason: "CrashLoopBackOff"}, time.Now())
	if got.Kind != DedupNewIncident {
		t.Errorf("first sighting: kind = %v, want DedupNewIncident", got.Kind)
	}
	if got.Count != 1 {
		t.Errorf("first sighting: count = %d, want 1", got.Count)
	}
}

func TestDedup_SecondEventWithinWindow_IsDuplicate(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	key := EventKey{UID: "u1", Reason: "CrashLoopBackOff"}
	next := tsGen(time.Now(), 1*time.Second)
	c.Observe(key, next())
	got := c.Observe(key, next())
	if got.Kind != DedupDuplicate {
		t.Errorf("second sighting: kind = %v, want DedupDuplicate", got.Kind)
	}
	if got.Count != 2 {
		t.Errorf("second sighting: count = %d, want 2", got.Count)
	}
}

func TestDedup_EventAfterWindow_IsNewIncident(t *testing.T) {
	t.Parallel()
	// Use the injectable clock so we can simulate window rollover
	// without sleeping.
	c := newTestDedup(t, 5*time.Minute, "")
	now := time.Now()
	c.now = func() time.Time { return now }
	key := EventKey{UID: "u1", Reason: "CrashLoopBackOff"}
	c.Observe(key, now)

	// Advance past the window AND advance eventLastTS so this is
	// classified as real new activity (not a replay), which is
	// the shape that triggers the retry-safety-net path.
	now = now.Add(10 * time.Minute)
	got := c.Observe(key, now)
	if got.Kind != DedupNewIncident {
		t.Errorf("post-window sighting: kind = %v, want DedupNewIncident (window should have expired)", got.Kind)
	}
	if got.Count != 1 {
		t.Errorf("post-window sighting: count = %d, want 1 (fresh window)", got.Count)
	}
}

func TestDedup_BindSession_AttachesToEntry(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	key := EventKey{UID: "u1", Reason: "CrashLoopBackOff"}
	next := tsGen(time.Now(), 1*time.Second)
	c.Observe(key, next())
	c.BindSession(key, "sess-abc")
	// Second sighting is a duplicate — should carry the bound
	// SessionID so the caller can route the inject to it.
	got := c.Observe(key, next())
	if got.SessionID != "sess-abc" {
		t.Errorf("duplicate should carry bound SessionID; got %q", got.SessionID)
	}
}

func TestDedup_BindSession_NoOp_OnMissingEntry(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	// Race case: BindSession called on a key whose entry has since
	// been evicted. Must not panic.
	c.BindSession(EventKey{UID: "u-gone", Reason: "X"}, "sess-orphan")
}

// TestCanonicalizeReason pins the reason-family mapping (#219).
// Reasons in the map collapse to their canonical primary; every
// other reason maps to itself.
func TestCanonicalizeReason(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in, want string
	}{
		{"ErrImagePull", "ImagePullBackOff"},
		{"BackOff", "CrashLoopBackOff"},
		{"ImagePullBackOff", "ImagePullBackOff"}, // canonical stays itself
		{"CrashLoopBackOff", "CrashLoopBackOff"}, // canonical stays itself
		{"OOMKilled", "OOMKilled"},               // not in map → identity
		{"FailedMount", "FailedMount"},
		{"", ""}, // edge: empty stays empty
		{"Unknown", "Unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got := CanonicalReason(tc.in)
			if got != tc.want {
				t.Errorf("CanonicalReason(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestDedup_ReasonFamilyCollapsesIntoOneSlot is the #219 regression
// test. Before the fix, ImagePullBackOff and ErrImagePull for the
// same pod produced two independent dedup entries → two parallel
// sessions (observed live: 4 sessions per incident, 4× cost).
// After canonicalization, both hit the same slot; second sighting
// is a duplicate.
func TestDedup_ReasonFamilyCollapsesIntoOneSlot(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	next := tsGen(time.Now(), 1*time.Second)

	// First: ErrImagePull (the earlier event kubelet emits when a
	// pull attempt fails). Canonicalizes to ImagePullBackOff.
	first := c.Observe(EventKey{UID: "u-payment", Reason: "ErrImagePull"}, next())
	if first.Kind != DedupNewIncident {
		t.Fatalf("ErrImagePull first: want DedupNewIncident, got %v", first.Kind)
	}

	// Second: ImagePullBackOff (the settled kubelet backoff state,
	// same underlying failure, arrives seconds later). Must be
	// treated as a duplicate of the first event, not a new incident.
	second := c.Observe(EventKey{UID: "u-payment", Reason: "ImagePullBackOff"}, next())
	if second.Kind != DedupDuplicate {
		t.Errorf("ImagePullBackOff after ErrImagePull for same UID: want DedupDuplicate (family collision), got %v", second.Kind)
	}
	if second.Count != 2 {
		t.Errorf("family-collision count: want 2 (first + second), got %d", second.Count)
	}
}

// TestDedup_BackOff_CanonicalizesTo_CrashLoopBackOff — the second
// documented reason-family mapping. Locks in behavior operators
// depend on for the crash-loop cycle.
func TestDedup_BackOff_CanonicalizesTo_CrashLoopBackOff(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	next := tsGen(time.Now(), 1*time.Second)

	c.Observe(EventKey{UID: "u-flappy", Reason: "CrashLoopBackOff"}, next())
	second := c.Observe(EventKey{UID: "u-flappy", Reason: "BackOff"}, next())
	if second.Kind != DedupDuplicate {
		t.Errorf("BackOff after CrashLoopBackOff same UID: want DedupDuplicate, got %v", second.Kind)
	}
}

// TestDedup_DifferentPodsDontCollide — sanity check that
// canonicalization only collapses SAME-UID events. Different pods
// with related reasons stay independent.
func TestDedup_DifferentPodsDontCollide(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	next := tsGen(time.Now(), 1*time.Second)

	a := c.Observe(EventKey{UID: "u-pod-a", Reason: "ImagePullBackOff"}, next())
	b := c.Observe(EventKey{UID: "u-pod-b", Reason: "ImagePullBackOff"}, next())

	if a.Kind != DedupNewIncident {
		t.Errorf("pod-a: want DedupNewIncident, got %v", a.Kind)
	}
	if b.Kind != DedupNewIncident {
		t.Errorf("pod-b (different UID): want DedupNewIncident, got %v", b.Kind)
	}
}

// TestDedup_BindSession_CanonicalizesLookup verifies the caller
// can pass the wire-level reason when binding a session and have
// the lookup find the entry via canonicalization. Otherwise the
// duplicate-routing SessionID would be lost.
func TestDedup_BindSession_CanonicalizesLookup(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	next := tsGen(time.Now(), 1*time.Second)

	// First event with a NON-canonical reason. Observe canonicalizes
	// the stored key; BindSession must apply the same mapping so
	// the same entry is found.
	c.Observe(EventKey{UID: "u-payment", Reason: "ErrImagePull"}, next())
	c.BindSession(EventKey{UID: "u-payment", Reason: "ErrImagePull"}, "sess-payment")

	// Follow-up ImagePullBackOff for same UID must resolve to the
	// same session (via canonicalization).
	got := c.Observe(EventKey{UID: "u-payment", Reason: "ImagePullBackOff"}, next())
	if got.SessionID != "sess-payment" {
		t.Errorf("family follow-up should carry bound SessionID; got %q", got.SessionID)
	}
}

func TestDedup_DifferentKeys_AreIndependent(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	next := tsGen(time.Now(), 1*time.Second)
	a := c.Observe(EventKey{UID: "u1", Reason: "CrashLoopBackOff"}, next())
	b := c.Observe(EventKey{UID: "u2", Reason: "CrashLoopBackOff"}, next())
	if a.Kind != DedupNewIncident || b.Kind != DedupNewIncident {
		t.Errorf("distinct UIDs should both be new incidents (a=%v, b=%v)", a.Kind, b.Kind)
	}
}

func TestDedup_LRUEvictionAtCapacity(t *testing.T) {
	t.Parallel()
	// Override the cache cap so we don't need 10k entries in the
	// test. Reach into the internals — this file lives in the
	// same package.
	c := newTestDedup(t, 5*time.Minute, "")
	c.max = 3
	now := time.Now()
	c.now = func() time.Time { return now }

	c.Observe(EventKey{UID: "u1", Reason: "R"}, now)
	now = now.Add(1 * time.Second)
	c.Observe(EventKey{UID: "u2", Reason: "R"}, now)
	now = now.Add(1 * time.Second)
	c.Observe(EventKey{UID: "u3", Reason: "R"}, now)
	if c.Len() != 3 {
		t.Errorf("Len = %d, want 3 after three distinct observations", c.Len())
	}
	// Adding a fourth should evict u1 (oldest LastSeen).
	now = now.Add(1 * time.Second)
	c.Observe(EventKey{UID: "u4", Reason: "R"}, now)
	if c.Len() != 3 {
		t.Errorf("Len = %d, want 3 after LRU eviction", c.Len())
	}
	// u1 should now be evicted; observing it again is a fresh incident.
	got := c.Observe(EventKey{UID: "u1", Reason: "R"}, now)
	if got.Kind != DedupNewIncident {
		t.Errorf("evicted key re-observed: kind = %v, want DedupNewIncident", got.Kind)
	}
}

func TestDedup_Snapshot_RoundTrip(t *testing.T) {
	t.Parallel()
	// Snapshot then restore into a fresh cache; state should
	// survive intact.
	path := filepath.Join(t.TempDir(), "dedup.json")
	c1 := newTestDedup(t, 5*time.Minute, path)
	key := EventKey{UID: "u-persist", Reason: "CrashLoopBackOff"}
	next := tsGen(time.Now(), 1*time.Second)
	c1.Observe(key, next())
	c1.BindSession(key, "sess-persist")
	if err := c1.Snapshot(); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	c2 := newTestDedup(t, 5*time.Minute, path)
	got := c2.Observe(key, next())
	if got.Kind != DedupDuplicate {
		t.Errorf("restored key: kind = %v, want DedupDuplicate (should be within window from restored state)", got.Kind)
	}
	if got.SessionID != "sess-persist" {
		t.Errorf("restored key: SessionID = %q, want sess-persist", got.SessionID)
	}
}

// TestDedup_Snapshot_RoundTripWithBindings verifies the §7.4
// persistence contract: BindIncident's identity ref rides the
// existing snapshot path, so a rebooted sentinel can resume recovery
// tracking (Bindings) AND still route outcomes (LookupSession).
func TestDedup_Snapshot_RoundTripWithBindings(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dedup.json")
	c1 := newTestDedup(t, 5*time.Minute, path)
	key := EventKey{UID: "u-rec", Reason: "ErrImagePull"} // canonicalizes
	c1.Observe(key, time.Now())
	ref := IncidentRef{
		Namespace:     "checkout",
		KindOfObject:  "Pod",
		Name:          "checkout-7b9d-x4kzq",
		ControllerRef: "ReplicaSet/checkout-7b9d",
		Fingerprint:   "sha256:orig",
		Cluster:       "prod",
	}
	c1.BindIncident(key, "sess-rec", ref)
	if err := c1.Snapshot(); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	c2 := newTestDedup(t, 5*time.Minute, path)
	sid, ok := c2.LookupSession(key)
	if !ok || sid != "sess-rec" {
		t.Errorf("LookupSession after restore = (%q, %v), want (sess-rec, true)", sid, ok)
	}
	bindings := c2.Bindings()
	if len(bindings) != 1 {
		t.Fatalf("Bindings after restore: got %d, want 1", len(bindings))
	}
	b := bindings[0]
	if b.SessionID != "sess-rec" {
		t.Errorf("binding SessionID = %q", b.SessionID)
	}
	if b.Key.Reason != "ImagePullBackOff" {
		t.Errorf("binding key reason = %q, want canonical ImagePullBackOff", b.Key.Reason)
	}
	if b.Ref != ref {
		t.Errorf("binding ref drifted through persistence:\n got %+v\nwant %+v", b.Ref, ref)
	}
	if b.FirstSeen.IsZero() {
		t.Errorf("binding FirstSeen must survive persistence")
	}
}

// TestDedup_Restore_VersionTolerant pins the loader against the
// PRE-recovery snapshot format: entries without the "incident" key
// (written by older sentinels) must load cleanly — the binding still
// routes followups; only recovery tracking is unavailable for them.
func TestDedup_Restore_VersionTolerant(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dedup.json")
	old := `{
  "u-old|CrashLoopBackOff": {
    "session_id": "sess-old",
    "first_seen": "2026-07-24T10:00:00Z",
    "last_seen": "2026-07-24T10:05:00Z",
    "event_last_ts": "2026-07-24T10:05:00Z",
    "count": 3
  }
}`
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatalf("write old-format snapshot: %v", err)
	}
	c := newTestDedup(t, 5*time.Minute, path)
	key := EventKey{UID: "u-old", Reason: "CrashLoopBackOff"}
	sid, ok := c.LookupSession(key)
	if !ok || sid != "sess-old" {
		t.Errorf("LookupSession on old-format entry = (%q, %v), want (sess-old, true)", sid, ok)
	}
	if got := c.Bindings(); len(got) != 0 {
		t.Errorf("old-format entry has no incident ref; Bindings must skip it, got %+v", got)
	}
	// And the new format must not choke an entry that has BOTH.
	c.Observe(EventKey{UID: "u-new", Reason: "FailedMount"}, time.Now())
	c.BindIncident(EventKey{UID: "u-new", Reason: "FailedMount"}, "sess-new", IncidentRef{Namespace: "ns", KindOfObject: "Pod", Name: "p"})
	if err := c.Snapshot(); err != nil {
		t.Fatalf("Snapshot mixed formats: %v", err)
	}
	c2 := newTestDedup(t, 5*time.Minute, path)
	if got := c2.Bindings(); len(got) != 1 {
		t.Errorf("mixed snapshot: want 1 resumable binding, got %d", len(got))
	}
}

func TestDedup_LookupSession_UnknownKey(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	if sid, ok := c.LookupSession(EventKey{UID: "nope", Reason: "R"}); ok || sid != "" {
		t.Errorf("LookupSession on unknown key = (%q, %v), want empty/false", sid, ok)
	}
	// Entry without a bound session is also "unknown".
	c.Observe(EventKey{UID: "u1", Reason: "R"}, time.Now())
	if _, ok := c.LookupSession(EventKey{UID: "u1", Reason: "R"}); ok {
		t.Error("LookupSession must report false for an unbound entry")
	}
}

func TestDedup_Snapshot_NoPersistPathIsNoOp(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	c.Observe(EventKey{UID: "u1", Reason: "R"}, time.Now())
	if err := c.Snapshot(); err != nil {
		t.Errorf("Snapshot on non-persisted cache should succeed as no-op; got %v", err)
	}
}

func TestDedup_NegativeWindow_Rejected(t *testing.T) {
	t.Parallel()
	if _, err := NewDedupCache(0, ""); err == nil {
		t.Error("zero window should be rejected")
	}
	if _, err := NewDedupCache(-1*time.Second, ""); err == nil {
		t.Error("negative window should be rejected")
	}
}

func TestSerializeDeserializeKey_RoundTrip(t *testing.T) {
	t.Parallel()
	orig := EventKey{UID: "abc-123-def-456", Reason: "CrashLoopBackOff"}
	got, ok := deserializeKey(serializeKey(orig))
	if !ok {
		t.Fatal("deserialize failed")
	}
	if got != orig {
		t.Errorf("round-trip: got %+v, want %+v", got, orig)
	}
}

// TestDedup_ReplayOfSameEventTimestampDedups is the informer
// re-List regression. Live demo drive 2026-07-14 produced 4
// sessions for one ImagePullBackOff because the client-go
// informer re-Lists Events every ~15-25min on watch-connection
// rotation, and the re-Listed event's arrival was outside the
// wall-clock dedup window even though the Event object itself
// hadn't advanced. Comparing incoming eventLastTS against the
// recorded value now catches this — same LastTimestamp = same
// activity, dedup regardless of arrival time.
func TestDedup_ReplayOfSameEventTimestampDedups(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	now := time.Now()
	c.now = func() time.Time { return now }
	key := EventKey{UID: "u-payment", Reason: "ImagePullBackOff"}
	eventTS := now // k8s Event.LastTimestamp for this incident

	first := c.Observe(key, eventTS)
	if first.Kind != DedupNewIncident {
		t.Fatalf("first sighting: want DedupNewIncident, got %v", first.Kind)
	}

	// Simulate informer re-List after wall clock advances past the
	// window (kube-apiserver rotates watch connections every
	// ~15-25min). The same Event object is re-delivered with the
	// SAME LastTimestamp — this must NOT fire a new session.
	now = now.Add(20 * time.Minute) // past the 5m dedup window
	replay := c.Observe(key, eventTS)
	if replay.Kind != DedupDuplicate {
		t.Errorf("replay past wall-clock window: kind = %v, want DedupDuplicate (same eventLastTS = replay, not new activity)", replay.Kind)
	}
	if replay.Count != 2 {
		t.Errorf("replay count = %d, want 2 (initial + replay)", replay.Count)
	}
}

// TestDedup_ReplayDoesNotAdvanceLastSeen — subtle but load-bearing:
// replay dedup bumps Count but must NOT bump LastSeen. Otherwise
// the retry-safety-net cooldown would keep resetting every time
// the informer re-Lists and a stalled session would never get a
// second attempt at the incident.
func TestDedup_ReplayDoesNotAdvanceLastSeen(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	now := time.Now()
	c.now = func() time.Time { return now }
	key := EventKey{UID: "u", Reason: "R"}
	firstEventTS := now

	c.Observe(key, firstEventTS)
	firstLastSeen := c.entries[key].LastSeen

	// A dozen informer replays over 30 minutes with the SAME
	// eventLastTS. Cooldown timer must remain anchored to
	// firstLastSeen.
	for i := 1; i <= 12; i++ {
		now = now.Add(3 * time.Minute)
		c.Observe(key, firstEventTS) // replay: same eventLastTS
	}

	if !c.entries[key].LastSeen.Equal(firstLastSeen) {
		t.Errorf("LastSeen advanced by replays; want %v, got %v", firstLastSeen, c.entries[key].LastSeen)
	}
	if c.entries[key].Count != 13 {
		t.Errorf("Count = %d, want 13 (initial + 12 replays)", c.entries[key].Count)
	}
}

// TestDedup_NewActivityPastCooldownFiresRetrySafetyNet — with the
// new replay-aware logic, the retry safety net still triggers when
// k8s reports REAL new activity (advancing eventLastTS) past the
// cooldown. This is the "agent failed to process, incident still
// ongoing" case the operator flagged as a concern: if the pod is
// still emitting BackOff events and enough wall-clock has passed,
// we spin up a fresh session.
func TestDedup_NewActivityPastCooldownFiresRetrySafetyNet(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	now := time.Now()
	c.now = func() time.Time { return now }
	key := EventKey{UID: "u", Reason: "ImagePullBackOff"}

	c.Observe(key, now)
	c.BindSession(key, "sess-first")

	// Advance both the wall clock (past the cooldown) AND the
	// event's LastTimestamp (k8s aggregated a fresh backoff cycle
	// into the same Event object — this is real new activity, not
	// a replay). Retry safety net should fire.
	now = now.Add(10 * time.Minute)
	newEventTS := now // k8s bumped LastTimestamp for this Event
	got := c.Observe(key, newEventTS)
	if got.Kind != DedupNewIncident {
		t.Errorf("real new activity past cooldown: kind = %v, want DedupNewIncident (retry safety net)", got.Kind)
	}
	if got.Count != 1 {
		t.Errorf("retry fresh window count = %d, want 1", got.Count)
	}
}

// TestDedup_NewActivityWithinCooldownDedups — real new activity
// (advancing eventLastTS) but within the cooldown window routes to
// the existing session. Matches the "same incident, still active"
// case: k8s keeps emitting BackOff events, we've already spun a
// session, no need for another.
func TestDedup_NewActivityWithinCooldownDedups(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	now := time.Now()
	c.now = func() time.Time { return now }
	key := EventKey{UID: "u", Reason: "ImagePullBackOff"}

	c.Observe(key, now)
	c.BindSession(key, "sess-first")

	// 2 minutes later, k8s advances the Event's LastTimestamp
	// (real new activity). Well within cooldown → dedup to same session.
	now = now.Add(2 * time.Minute)
	got := c.Observe(key, now)
	if got.Kind != DedupDuplicate {
		t.Errorf("real new activity within cooldown: kind = %v, want DedupDuplicate", got.Kind)
	}
	if got.SessionID != "sess-first" {
		t.Errorf("SessionID = %q, want sess-first (routed to existing session)", got.SessionID)
	}
	if got.Count != 2 {
		t.Errorf("Count = %d, want 2", got.Count)
	}
}

// TestDedup_BackwardsEventTimestampTreatedAsReplay — defensive: if
// somehow (misconfigured k8s Event source, wall-clock drift) the
// incoming eventLastTS is EARLIER than what we recorded, treat as
// replay. Better to under-fire than to spuriously spin new sessions.
func TestDedup_BackwardsEventTimestampTreatedAsReplay(t *testing.T) {
	t.Parallel()
	c := newTestDedup(t, 5*time.Minute, "")
	key := EventKey{UID: "u", Reason: "R"}
	base := time.Now()

	c.Observe(key, base)
	got := c.Observe(key, base.Add(-1*time.Minute)) // earlier ts

	if got.Kind != DedupDuplicate {
		t.Errorf("backwards eventLastTS: kind = %v, want DedupDuplicate (treat as replay)", got.Kind)
	}
}

// TestCanonicalReason_ObjectStateFamilies pins the APPEND-ONLY M2
// additions: object-state's leading reasons collapse into the same
// dedup family as their reactive k8s-event counterparts (same object
// UID), so whichever fires first opens the session and the other
// attaches as a followup — the claim-and-attach flow.
func TestCanonicalReason_ObjectStateFamilies(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"node_notready": "NodeNotReady",     // joins the node controller's events on the Node
		"restart_burst": "CrashLoopBackOff", // joins kubelet's BackOff family on the Pod
		// No k8s-event counterpart on the same UID → map to themselves.
		"node_flapping":     "node_flapping",
		"progress_deadline": "progress_deadline",
		"endpoints_empty":   "endpoints_empty",
		"pdb_gridlocked":    "pdb_gridlocked",
	}
	for in, want := range cases {
		if got := CanonicalReason(in); got != want {
			t.Errorf("CanonicalReason(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestDedup_LeadingSignalClaimsThenEventAttaches proves the collapse
// end to end at the cache: a restart_burst observation opens the
// incident; the later BackOff event for the same pod is a duplicate
// routed to the bound session.
func TestDedup_LeadingSignalClaimsThenEventAttaches(t *testing.T) {
	t.Parallel()
	c, err := NewDedupCache(5*time.Minute, "")
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	if r := c.Observe(EventKey{UID: "pod-1", Reason: "restart_burst"}, t0); r.Kind != DedupNewIncident {
		t.Fatalf("leading signal: got %v, want new incident", r.Kind)
	}
	c.BindSession(EventKey{UID: "pod-1", Reason: "restart_burst"}, "sess-lead")
	r := c.Observe(EventKey{UID: "pod-1", Reason: "BackOff"}, t0.Add(time.Minute))
	if r.Kind != DedupDuplicate {
		t.Fatalf("later BackOff event: got %v, want duplicate (claim-and-attach)", r.Kind)
	}
	if r.SessionID != "sess-lead" {
		t.Errorf("followup routes to %q, want the leading signal's session sess-lead", r.SessionID)
	}
}

// TestSourceFamily pins the kind→family bucketing the cross-source
// join followups key on (M4 drill observation 4).
func TestSourceFamily(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"k8s-event":                 "k8s-event",
		"k8s-event-followup":        "k8s-event-followup",
		"capacity.quota_blocked":    "capacity",
		"capacity.pending":          "capacity",
		"quota.forecast":            "quota",
		"objectstate.restart_burst": "objectstate",
	}
	for in, want := range cases {
		if got := SourceFamily(in); got != want {
			t.Errorf("SourceFamily(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestCrossSourceJoin_OncePerFamilyPerWindow covers the M4
// observation-4 mechanics at the cache: the opening kind is stamped,
// a different-family duplicate answers positive exactly once, the
// same family never does, and a rolled window resets everything.
func TestCrossSourceJoin_OncePerFamilyPerWindow(t *testing.T) {
	t.Parallel()
	c, err := NewDedupCache(50*time.Millisecond, "")
	if err != nil {
		t.Fatal(err)
	}
	key := EventKey{UID: "quota:CPUS/us-east1", Reason: "quota_forecast"}
	t0 := time.Now()
	if r := c.Observe(key, t0); r.Kind != DedupNewIncident {
		t.Fatalf("opener: got %v, want new incident", r.Kind)
	}
	c.NoteIncidentKind(key, "quota.forecast")

	// Same family: plain suppression.
	if _, join := c.CrossSourceJoin(key, "quota.forecast"); join {
		t.Error("same-family duplicate reported as cross-source join")
	}
	// Different family (the drill's exact join): once.
	blocked := EventKey{UID: key.UID, Reason: "quota_blocked"} // same canonical family → same entry
	openedBy, join := c.CrossSourceJoin(blocked, "capacity.quota_blocked")
	if !join || openedBy != "quota" {
		t.Fatalf("cross-source join = (%q, %v), want (quota, true)", openedBy, join)
	}
	if _, join := c.CrossSourceJoin(blocked, "capacity.quota_blocked"); join {
		t.Error("second join from the same family answered positive (want max 1 per family per window)")
	}
	// A third family gets its own single announcement.
	if _, join := c.CrossSourceJoin(key, "objectstate.thing"); !join {
		t.Error("a different source family should get its own followup slot")
	}

	// Window rolls → fresh entry, unstamped until NoteIncidentKind:
	// joins stay suppressed (pre-M4 posture for unstamped entries).
	time.Sleep(60 * time.Millisecond)
	if r := c.Observe(key, t0.Add(time.Minute)); r.Kind != DedupNewIncident {
		t.Fatalf("post-window observe: got %v, want new incident", r.Kind)
	}
	if _, join := c.CrossSourceJoin(key, "capacity.quota_blocked"); join {
		t.Error("unstamped fresh entry reported a cross-source join")
	}
}
