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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

// dedupEntry holds the per-incident dedup state cached against an
// EventKey. Bounded: the sidecar's cache is capped at maxDedupEntries
// (LRU eviction) so a runaway cluster with tens of thousands of
// distinct incidents doesn't OOM the sidecar.
type dedupEntry struct {
	// SessionID is the daemon-side session created by the first
	// event in this window. Follow-up events for the same key
	// route to this session via POST /sessions/<sid>/inject.
	SessionID string `json:"session_id"`
	// FirstSeen is when the sidecar first observed this key.
	// Rolls forward when the window expires + a new event arrives.
	FirstSeen time.Time `json:"first_seen"`
	// LastSeen is the wall-clock time we last recorded REAL new
	// activity for this key (replays do NOT advance it — see
	// Observe). Used to compute retry-cooldown: the entry ages
	// out when LastSeen + window < now.
	LastSeen time.Time `json:"last_seen"`
	// EventLastTS is the k8s Event's own LastTimestamp we last
	// processed for this key. Enables idempotent replay handling:
	// client-go informers periodically re-List all Events on
	// watch-connection rotation (~15-25min in practice), which
	// used to trigger spurious "new incident" fires. Comparing
	// incoming event.LastTimestamp against this field lets us
	// distinguish replay of already-seen activity (dedup) from
	// genuinely new activity (real new events k8s aggregated
	// into the same Event object since we last saw it).
	EventLastTS time.Time `json:"event_last_ts"`
	// Count is how many events the sidecar has seen for this key
	// in the current window (the first event that created the
	// session counts as 1). Reset when the window rolls.
	Count int `json:"count"`
	// Incident, when non-nil, is the identity of the incident bound
	// to SessionID — enough for the RecoveryTracker (§7.4) to resume
	// clearance watching and compose the resolved payload after a
	// sentinel restart. Set by BindIncident. Version tolerance both
	// ways: snapshots written before this field existed simply load
	// with a nil Incident (the binding still routes followups; only
	// recovery tracking is lost), and older binaries reading a newer
	// snapshot ignore the unknown key per encoding/json.
	Incident *IncidentRef `json:"incident,omitempty"`
	// Storm, when non-empty, records that this incident was folded
	// into a §7.5 storm: the value is the storm's fingerprint and
	// SessionID is the storm's session, so followups and recovery
	// outcomes route there instead of a per-incident session. Same
	// version tolerance as Incident (older snapshots simply lack it).
	Storm string `json:"storm,omitempty"`
	// SourceKind is the signal kind that opened the current window
	// entry (stamped via NoteIncidentKind), the reference point for
	// cross-source join visibility (M4 drill observation 4): a
	// duplicate whose kind comes from a DIFFERENT source family is a
	// leading↔reactive join the bound session should hear about.
	// Version tolerance both ways like Incident/Storm: older
	// snapshots load with it empty (joins simply stay suppressed for
	// that window — the pre-M4 behavior).
	SourceKind string `json:"source_kind,omitempty"`
	// FollowupSources are the source families that already emitted a
	// cross-source join followup in this window — the "max 1 per
	// source per incident per window" bound. Reset when the window
	// rolls (a fresh entry is created).
	FollowupSources []string `json:"followup_sources,omitempty"`
}

// DedupResult tells the caller what to do with the event that just
// came in: kind==firstInWindow means create a session + inject;
// kind==duplicate means suppress; kind==newIncident means the prior
// window expired and this is a fresh incident (create new session).
type DedupResult struct {
	Kind      DedupResultKind
	SessionID string // only set when Kind==duplicate; the existing session
	Count     int    // window count (1 for first, N for duplicates)
}

type DedupResultKind int

const (
	// DedupNewIncident: no prior entry (or the prior window
	// expired). Caller must create a new session and inject.
	DedupNewIncident DedupResultKind = iota
	// DedupDuplicate: an entry exists within the window. Caller
	// suppresses this event; the count is bumped and available
	// via DedupResult.Count.
	DedupDuplicate
)

// DedupCache is the rolling-window dedup store. Backed by a map +
// a mutex; bounded by LRU eviction at maxDedupEntries.
type DedupCache struct {
	mu      sync.Mutex
	entries map[EventKey]*dedupEntry
	window  time.Duration
	max     int
	// persistPath, when non-empty, causes Snapshot+Restore calls
	// to read/write JSON at that path so the cache survives
	// sidecar restart. Optional (nil disables persistence).
	persistPath string
	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
}

// maxDedupEntries caps the sidecar's cache size. 10k is plenty for
// any realistic single-cluster deployment (unique events per 5-min
// window). LRU eviction beyond this bound.
const maxDedupEntries = 10_000

// NewDedupCache constructs a cache with the supplied rolling window
// duration. window must be > 0. persistPath is optional; empty
// disables the on-disk cache.
func NewDedupCache(window time.Duration, persistPath string) (*DedupCache, error) {
	if window <= 0 {
		return nil, fmt.Errorf("dedup: window must be > 0 (got %s)", window)
	}
	c := &DedupCache{
		entries:     make(map[EventKey]*dedupEntry),
		window:      window,
		max:         maxDedupEntries,
		persistPath: persistPath,
	}
	if persistPath != "" {
		if err := c.restore(); err != nil {
			return nil, fmt.Errorf("dedup: restore from %s: %w", persistPath, err)
		}
	}
	return c, nil
}

// clock returns the current time. Overridable for tests.
func (c *DedupCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// reasonCanonical maps well-known secondary Event.Reason values to
// their canonical primary reason for dedup-key computation. Two
// events for the same target whose reasons are in the same family
// collapse into one dedup entry — e.g. an ImagePullBackOff event
// and an ErrImagePull event for the same pod count as ONE incident,
// not two parallel sessions.
//
// The mapping is deliberately narrow — only well-known equivalences
// where a single underlying failure emits multiple reason variants
// from kubelet's retry cycle. Reasons not in this map are their
// own canonical value.
//
// Rationale for each entry:
//
//   - ErrImagePull → ImagePullBackOff: kubelet emits ErrImagePull
//     on the first failed pull attempt, then ImagePullBackOff once
//     the exponential backoff kicks in. Same failure, two reason
//     values within seconds.
//   - BackOff → CrashLoopBackOff: BackOff accompanies both crash-
//     loop and image-pull cycles. In practice, when it collides
//     with an ImagePullBackOff for the same pod, that pod's
//     ImagePullBackOff entry already exists so BackOff routes there
//     via the CrashLoopBackOff canonical (which will be reset when
//     the crash-loop entry expires). Yes, this is subtle. If a
//     future variant wants to disambiguate, a `--reason-canonical`
//     config override can drop or remap entries.
//
// Observed live during v2.6 GKE-troubleshoot demo drive: one
// paymentservice ImagePullBackOff spawned 4 parallel sessions
// (one per reason variant) at $0.28/session, 4× baseline spend
// per incident. See go-steer/core-agent#219.
var reasonCanonical = map[string]string{
	"ErrImagePull": "ImagePullBackOff",
	"BackOff":      "CrashLoopBackOff",

	// APPEND-ONLY additions for the object-state source (M2): a
	// leading transition signal and its reactive k8s-event
	// counterpart describe the same incident on the same object UID,
	// so they share a dedup family — whichever fires first (usually
	// the leading one; that's the point) opens the session, and the
	// other attaches as a followup. The dedup collapse IS the
	// claim-and-attach flow; no extra machinery.
	//
	//   - node_notready (objectstate.node_notready, Node UID) joins
	//     the node controller's NodeNotReady events on the Node.
	//   - restart_burst (objectstate.restart_burst, Pod UID) joins
	//     the kubelet's BackOff → CrashLoopBackOff family on the Pod.
	//
	// The remaining object-state reasons (node_flapping,
	// progress_deadline, endpoints_empty, pdb_gridlocked) have no
	// k8s-event counterpart on the same UID and map to themselves.
	"node_notready": "NodeNotReady",
	"restart_burst": "CrashLoopBackOff",

	// APPEND-ONLY additions for the capacity source (M4), same M2
	// leading/reactive pattern: a pod the autoscaler cannot place is
	// ONE incident however many angles observe it. capacity.pending
	// (the NotTriggerScaleUp event, Pod UID) and
	// capacity.pending-aged (the resident aging sweep, Pod UID) join
	// the scheduler's FailedScheduling events on the same pod —
	// whichever fires first opens the session, the rest attach as
	// followups. The nodegroup-keyed capacity reasons (scaleup_gap,
	// stockout, ip_exhausted) have no k8s-event counterpart on their
	// synthetic UID and map to themselves; quota_blocked joined the
	// QuotaExhausted family below when the quota source landed.
	"pending":      "FailedScheduling",
	"pending-aged": "FailedScheduling",

	// APPEND-ONLY additions for the quota source (M4, §10.3), same
	// leading/reactive pattern one tier up: "scaleup failed
	// (GCE_QUOTA_EXCEEDED)" and "CPUS at 98%, exhausted in ~6 days"
	// are ONE diagnosed incident, not two alerts a human joins. The
	// quota source's quota_forecast (leading) and the capacity
	// source's quota_blocked (reactive) collapse into the
	// QuotaExhausted family; the join lands when the two signals
	// also share a UID, which is the canonical quota identity
	// "quota:<NAME>/<SCOPE>" (pkg/sources/quota.UID — the quota, not
	// the nodegroup, is the incident at project scope). The capacity
	// source re-keys quota_blocked decisions to that UID when the
	// provider's decision message names the quota; when it does not,
	// the signal stays nodegroup-keyed and only the reason family is
	// shared (documented limitation — no false joins).
	"quota_blocked":  "QuotaExhausted",
	"quota_forecast": "QuotaExhausted",
}

// CanonicalReason returns the canonical reason for a given
// Event.Reason value — the dedup-key reason, and the "reason-class"
// input to Fingerprint (§8): fingerprints must collapse the same
// reason families dedup does, or the same underlying failure would
// carry different fingerprints across clusters depending on which
// kubelet retry-cycle variant each sentinel saw first. Reasons not in
// reasonCanonical map to themselves (no change).
func CanonicalReason(reason string) string {
	if canonical, ok := reasonCanonical[reason]; ok {
		return canonical
	}
	return reason
}

// Observe records that key was just seen with eventLastTS (the
// k8s Event's own LastTimestamp). Returns a dedupResult telling the
// caller whether this is a fresh incident (start a new session) or
// a duplicate within the current window (suppress).
//
// The key's Reason is canonicalized (see reasonCanonical) so events
// from the same underlying failure with different reason variants
// (e.g. ImagePullBackOff vs ErrImagePull) collapse into one dedup
// slot. The wire event's original Reason is preserved on the
// inject payload — canonicalization is a dedup-only mechanism.
//
// Three cases, checked in order:
//
//  1. **Replay** — eventLastTS is not later than the recorded
//     EventLastTS. This is the k8s Event object being re-delivered
//     (informer watch-rotation triggers a re-List; kube-apiserver
//     rotates watch connections every ~15-25min in practice). The
//     activity is one we've already processed — dedup, do NOT
//     advance LastSeen (retry cooldown continues to age from real
//     activity time). Bump count so metrics stay accurate.
//
//  2. **New activity past cooldown** — eventLastTS is later AND
//     wall-clock now is more than `window` past LastSeen. The prior
//     session's cooldown has expired and k8s is still actively
//     reporting the issue, so create a new session (retry safety
//     net: if the previous agent-run failed to resolve the
//     incident, the fresh reporting gives it another chance).
//
//  3. **New activity within cooldown** — eventLastTS is later,
//     within the window. Real new k8s aggregation on top of an
//     ongoing incident — dedup to the existing session, advance
//     both LastSeen and EventLastTS.
//
// Contract: the caller MUST call BindSession after a successful
// CreateSession call to attach the SessionID to the newly-created
// entry, so subsequent duplicates can route to the same session.
func (c *DedupCache) Observe(key EventKey, eventLastTS time.Time) DedupResult {
	key.Reason = CanonicalReason(key.Reason)
	now := c.clock()
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		// First sighting for this key.
		c.evictIfFull()
		c.entries[key] = &dedupEntry{
			FirstSeen:   now,
			LastSeen:    now,
			EventLastTS: eventLastTS,
			Count:       1,
		}
		return DedupResult{Kind: DedupNewIncident, Count: 1}
	}
	if !eventLastTS.After(entry.EventLastTS) {
		// Case 1: replay. Same activity we already processed;
		// don't advance LastSeen (retry cooldown untouched).
		entry.Count++
		return DedupResult{Kind: DedupDuplicate, SessionID: entry.SessionID, Count: entry.Count}
	}
	if now.Sub(entry.LastSeen) > c.window {
		// Case 2: retry safety net. Prior session's cooldown
		// elapsed AND k8s is still reporting new activity —
		// spin up a fresh session so an agent that failed to
		// resolve the last one gets another attempt.
		c.evictIfFull()
		c.entries[key] = &dedupEntry{
			FirstSeen:   now,
			LastSeen:    now,
			EventLastTS: eventLastTS,
			Count:       1,
		}
		return DedupResult{Kind: DedupNewIncident, Count: 1}
	}
	// Case 3: new activity within cooldown → dedup + advance.
	entry.Count++
	entry.LastSeen = now
	entry.EventLastTS = eventLastTS
	return DedupResult{Kind: DedupDuplicate, SessionID: entry.SessionID, Count: entry.Count}
}

// BindSession attaches the SessionID from a successful CreateSession
// call to the entry created by the preceding Observe. No-op if the
// entry has since been evicted (window elapsed AND the LRU sweep
// dropped it), which is a possible but harmless race.
//
// Applies the same reason canonicalization Observe does so a caller
// that saw a `dedupNewIncident` result on one reason variant can
// bind the session using the wire-level reason without having to
// know about the family mapping.
func (c *DedupCache) BindSession(key EventKey, sessionID string) {
	key.Reason = CanonicalReason(key.Reason)
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[key]; ok {
		entry.SessionID = sessionID
	}
}

// BindIncident is BindSession plus the incident identity the §7.4
// recovery tracker needs to survive a restart: the ref rides on the
// entry through Snapshot/restore, so a rebooted sentinel can resume
// watching for clearance and still route the outcome to the original
// session. Same no-op-on-evicted-entry semantics as BindSession.
func (c *DedupCache) BindIncident(key EventKey, sessionID string, ref IncidentRef) {
	key.Reason = CanonicalReason(key.Reason)
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[key]; ok {
		entry.SessionID = sessionID
		r := ref
		entry.Incident = &r
	}
}

// AttachToStorm rebinds key to a §7.5 storm: SessionID becomes the
// storm's session (followups and §7.4 outcomes now route there) and
// the storm's fingerprint is recorded on the entry. ref supplies the
// incident identity when the entry has none yet (the storm's trigger
// incident never went through BindIncident — its per-incident session
// was suppressed); an existing richer ref is kept. Same
// no-op-on-evicted-entry semantics as BindSession.
func (c *DedupCache) AttachToStorm(key EventKey, sessionID, stormFingerprint string, ref IncidentRef) {
	key.Reason = CanonicalReason(key.Reason)
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return
	}
	entry.SessionID = sessionID
	entry.Storm = stormFingerprint
	if entry.Incident == nil {
		r := ref
		entry.Incident = &r
	}
}

// LookupBinding returns the full bound incident for key (session +
// identity ref), if the entry is bound and carries a ref. The storm
// path uses it to supersede a member's per-incident session with the
// member's complete identity.
func (c *DedupCache) LookupBinding(key EventKey) (BoundIncident, bool) {
	key.Reason = CanonicalReason(key.Reason)
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok || entry.SessionID == "" || entry.Incident == nil {
		return BoundIncident{}, false
	}
	return BoundIncident{
		Key:       key,
		SessionID: entry.SessionID,
		FirstSeen: entry.FirstSeen,
		Ref:       *entry.Incident,
	}, true
}

// LookupSession returns the session currently bound to key, if any.
// The §7.4 routing primitive: a resolved signal is a followup into
// the incident's own session, and this lookup — not tracker-local
// state — is authoritative, so a re-fired incident (dedup retry
// safety net) routes its outcome to the NEWEST session for the key.
func (c *DedupCache) LookupSession(key EventKey) (string, bool) {
	key.Reason = CanonicalReason(key.Reason)
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok || entry.SessionID == "" {
		return "", false
	}
	return entry.SessionID, true
}

// SourceFamily buckets a signal kind by the source implementation
// that emits it — the prefix before the first "." for the
// source-namespaced kinds ("capacity.quota_blocked" → "capacity",
// "quota.forecast" → "quota", "objectstate.restart_burst" →
// "objectstate"), the whole kind for the frozen un-namespaced ones
// ("k8s-event"). This is the granularity of the §7.2 source table,
// which is what "a different observation angle" means for the
// cross-source join followups (M4 observation 4).
func SourceFamily(kind string) string {
	if i := strings.IndexByte(kind, '.'); i > 0 {
		return kind[:i]
	}
	return kind
}

// NoteIncidentKind stamps the signal kind that opened the current
// window entry for key. Called by the dispatcher on every
// DedupNewIncident result, before any routing branch, so the entry
// knows which source family the incident's ORIGINAL observation came
// from. No-op when the entry is missing (evicted — harmless race,
// like BindSession).
func (c *DedupCache) NoteIncidentKind(key EventKey, kind string) {
	key.Reason = CanonicalReason(key.Reason)
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[key]; ok {
		entry.SourceKind = kind
		entry.FollowupSources = nil
	}
}

// CrossSourceJoin reports whether a duplicate signal of the given
// kind is a cross-SOURCE join of the current window entry — a signal
// from a different source family than the one that opened the
// incident (leading↔reactive: quota_blocked joining a quota.forecast
// session, capacity.pending joining a FailedScheduling session) —
// and, when it is, marks the family as announced so the answer is
// positive AT MOST ONCE per source family per incident per window
// (M4 observation 4's bound). openedBy is the source family that
// opened the incident. Same-family duplicates, unstamped entries
// (pre-M4 snapshots), and repeat joins all answer false: plain
// dedup suppression.
func (c *DedupCache) CrossSourceJoin(key EventKey, kind string) (openedBy string, ok bool) {
	key.Reason = CanonicalReason(key.Reason)
	family := SourceFamily(kind)
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, found := c.entries[key]
	if !found || entry.SourceKind == "" || SourceFamily(entry.SourceKind) == family {
		return "", false
	}
	if slices.Contains(entry.FollowupSources, family) {
		return "", false
	}
	entry.FollowupSources = append(entry.FollowupSources, family)
	return SourceFamily(entry.SourceKind), true
}

// BoundIncident is one session binding with full incident identity,
// as returned by Bindings for restart seeding of the recovery
// tracker.
type BoundIncident struct {
	Key       EventKey
	SessionID string
	FirstSeen time.Time
	Ref       IncidentRef
}

// Bindings returns every entry that is bound to a session AND carries
// an incident ref — i.e. everything the recovery tracker can resume
// after a restart. Entries bound by the pre-recovery BindSession (or
// restored from an older snapshot) lack the ref and are skipped.
func (c *DedupCache) Bindings() []BoundIncident {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]BoundIncident, 0, len(c.entries))
	for key, entry := range c.entries {
		if entry.SessionID == "" || entry.Incident == nil {
			continue
		}
		out = append(out, BoundIncident{
			Key:       key,
			SessionID: entry.SessionID,
			FirstSeen: entry.FirstSeen,
			Ref:       *entry.Incident,
		})
	}
	return out
}

// evictIfFull is called under lock. If the cache is at capacity,
// evicts the LRU entry (lowest LastSeen). Bounded O(N) scan; called
// only on new-incident cache-miss paths so amortized cost is fine.
func (c *DedupCache) evictIfFull() {
	if len(c.entries) < c.max {
		return
	}
	var oldestKey EventKey
	var oldestTs time.Time
	first := true
	for k, e := range c.entries {
		if first || e.LastSeen.Before(oldestTs) {
			oldestKey = k
			oldestTs = e.LastSeen
			first = false
		}
	}
	delete(c.entries, oldestKey)
}

// Len returns the current cache size. Test / metrics helper.
func (c *DedupCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Snapshot writes the current cache state to persistPath. Idempotent;
// no-op when persistPath is empty. Callers should call this on
// graceful shutdown (SIGTERM handler in main.go) and periodically
// while running (e.g., every 30s ticker) so a crash doesn't lose
// more than 30s of dedup state.
//
// Format: pretty-printed JSON — small enough that a human can
// inspect it during incident debugging, and simple enough that the
// on-disk shape doesn't need its own migration story.
func (c *DedupCache) Snapshot() error {
	if c.persistPath == "" {
		return nil
	}
	c.mu.Lock()
	// Copy under lock; encode outside so we don't hold the mutex
	// during I/O.
	snapshot := make(map[string]*dedupEntry, len(c.entries))
	for k, v := range c.entries {
		snapshot[serializeKey(k)] = v
	}
	c.mu.Unlock()
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("dedup: marshal snapshot: %w", err)
	}
	// Atomic write: temp file + rename so an interrupted write
	// doesn't corrupt the persisted state.
	tmp := c.persistPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("dedup: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, c.persistPath); err != nil {
		return fmt.Errorf("dedup: rename %s → %s: %w", tmp, c.persistPath, err)
	}
	return nil
}

// restore reads persistPath (if it exists) and hydrates the cache.
// Missing file is not an error — first-time startup has nothing to
// restore. Called by NewDedupCache during construction.
func (c *DedupCache) restore() error {
	data, err := os.ReadFile(c.persistPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil // first startup; nothing to restore
		}
		return fmt.Errorf("dedup: read %s: %w", c.persistPath, err)
	}
	var snapshot map[string]*dedupEntry
	if err := json.Unmarshal(data, &snapshot); err != nil {
		// Corrupt persist file: log and start fresh. Better than
		// refusing to boot the sidecar. Caller can inspect the
		// file if they care.
		return fmt.Errorf("dedup: unmarshal snapshot (starting fresh): %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for keyStr, entry := range snapshot {
		key, ok := deserializeKey(keyStr)
		if !ok {
			continue // silently skip malformed keys
		}
		c.entries[key] = entry
	}
	return nil
}

// serializeKey / deserializeKey encode an EventKey for use as a
// JSON map key (which must be a string). Using a delimiter that
// can't appear in a k8s UID (which is hex + hyphens) or an Event
// reason (which is CamelCase alphanumeric).
func serializeKey(k EventKey) string {
	return k.UID + "|" + k.Reason
}

func deserializeKey(s string) (EventKey, bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == '|' {
			return EventKey{UID: s[:i], Reason: s[i+1:]}, true
		}
	}
	return EventKey{}, false
}
