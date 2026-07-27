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
	"sync"
	"time"
)

// This file implements storm correlation (DESIGN.md §7.5): the
// second-level correlation window that sits after dedup and before
// severity routing in the §7.1 pipeline. Dedup collapses repeats of
// ONE incident; storm correlation collapses MANY distinct incidents
// that share a blast-radius key — the nearest common ancestor of the
// affected objects in the topology index (node, owner chain, shared
// ConfigMap/PVC, namespace) — into one kind=storm incident, so a node
// failure opens one session instead of thirty.
//
// The correlator itself is topology-agnostic: it consumes
// priority-ordered ancestor candidates from an AncestorResolver
// (implemented over pkg/graph by the sentinel's graph feed) and keeps
// no k8s types, so scripted-signal-stream tests (§13) drive it with a
// fake resolver.

// ObjectRef identifies an incident's affected object for topology
// resolution: the same (kind, namespace, name) identity pkg/graph
// interns. Namespace is empty for cluster-scoped kinds.
type ObjectRef struct {
	Kind      string
	Namespace string
	Name      string
}

// Ancestor is one blast-radius key candidate: a shared-ancestor
// identity in the topology. Two incidents correlate when they carry
// an equal Ancestor among their candidates.
type Ancestor struct {
	Kind      string
	Namespace string
	Name      string
}

// Key returns the stable string identity of the ancestor, used as the
// storm's correlation key and ID. The three-segment form matches
// pkg/graph's interner keys ("Kind/namespace/name"; cluster-scoped
// kinds have an empty middle segment).
func (a Ancestor) Key() string { return a.Kind + "/" + a.Namespace + "/" + a.Name }

// Display renders the ancestor for humans: "Node gke-a" or
// "Deployment checkout/payment".
func (a Ancestor) Display() string {
	if a.Namespace == "" {
		return a.Kind + " " + a.Name
	}
	return a.Kind + " " + a.Namespace + "/" + a.Name
}

// AncestorResolver answers "what does this object's blast radius hang
// off of?": the ancestor candidates of one object, best key first.
// The §7.5 priority is node > owner chain > shared ConfigMap/PVC >
// namespace, nearest first within a class (as pkg/graph's
// CommonAncestors returns them). An object that is itself a groupable
// ancestor (a Node incident, a Deployment incident) includes itself as
// the first candidate — that is how the root incident of a storm joins
// the storm keyed on it. Nil/empty means "cannot resolve" (object not
// in the topology, index not ready): the incident proceeds
// per-incident, never blocks.
type AncestorResolver interface {
	Ancestors(ref ObjectRef) []Ancestor
}

// StormMember is one member incident of a (forming or formed) storm.
type StormMember struct {
	// Key is the member's dedup key (reason canonicalized).
	Key EventKey
	// Fingerprint is the member's own §8 fingerprint.
	Fingerprint string
	// Object identity + wire reason, for representative listings.
	Namespace    string
	KindOfObject string
	Name         string
	Reason       string
	// SessionID is the member's per-incident session when it fired
	// before the storm formed (§7.5: the first incidents of a burst
	// inherently may). Empty for members suppressed by the storm.
	SessionID string
	// FirstSeen is the member incident's first observation.
	FirstSeen time.Time
}

// StormInfo is an immutable snapshot of one storm, handed to the
// dispatcher for payload composition. Representatives are the first
// stormRepresentatives members in arrival order; MemberFingerprints
// records every member (arrival order, one entry per member).
type StormInfo struct {
	// ID is the storm's stable handle: the ancestor Key().
	ID          string
	Ancestor    Ancestor
	Fingerprint string
	// Reason is the storm's reason-class: the canonical reason of the
	// first member (the burst's leading symptom class); also the
	// reason-class input of the storm fingerprint.
	Reason string
	// Severity is the max member severity plus the §7.5 size
	// escalator: at or beyond stormEscalateSize members, warning is
	// promoted to critical (info and critical are unchanged).
	Severity  Severity
	SessionID string
	// AffectedCount is the member count; NamespaceCount the distinct
	// (non-empty) member namespaces.
	AffectedCount      int
	NamespaceCount     int
	FirstSeen          time.Time
	LastSeen           time.Time
	Representatives    []StormMember
	MemberFingerprints []string
}

// StormVerdictKind classifies a StormCorrelator.Observe result.
type StormVerdictKind int

const (
	// StormNone: not correlated. The incident proceeds per-incident;
	// it stays in the rolling window as a future storm's member.
	StormNone StormVerdictKind = iota
	// StormFormed: this incident is the one that pushed a
	// blast-radius key over the threshold. The caller opens ONE storm
	// session, injects the kind=storm payload, supersedes members
	// that already fired per-incident, and suppresses this incident's
	// own session.
	StormFormed
	// StormAttached: an open, unresolved storm already owns this
	// incident's best blast-radius key. No per-incident session; the
	// incident attaches to the storm session as a followup.
	StormAttached
)

// StormVerdict is Observe's answer.
type StormVerdict struct {
	Kind StormVerdictKind
	// Storm is set for StormFormed / StormAttached.
	Storm StormInfo
	// Members is set for StormFormed: every member in arrival order,
	// the observed incident last. Members with a non-empty SessionID
	// fired per-incident before the storm formed and must be
	// superseded into the storm session.
	Members []StormMember
	// Member is the observed incident's own record (Formed/Attached).
	Member StormMember
	// SizeUpdate, set only on StormAttached, tells the caller a
	// kind=storm.update size refresh is due: membership grew past a
	// reporting threshold (doubling or +stormUpdateGrowth since the
	// last report, at most one per stormUpdateMinInterval). Nil when
	// no update is due.
	SizeUpdate *StormSizeUpdate
}

// StormSizeUpdate carries the freshness counters for one due
// kind=storm.update followup (M2 drill observation 4).
type StormSizeUpdate struct {
	// AffectedCount / NamespaceCount are the storm's CURRENT totals
	// (the formation payload's counts are frozen at formation time).
	AffectedCount  int
	NamespaceCount int
	// NewSinceLast is the membership growth since the previous size
	// report (the formation payload counts as the first report).
	NewSinceLast int
}

const (
	// DefaultStormWindow is the §7.5 second-level correlation window.
	DefaultStormWindow = 60 * time.Second
	// DefaultStormMin is the formation threshold: how many incidents
	// must share a blast-radius key within the window.
	DefaultStormMin = 3
	// stormRepresentatives caps the representative incident list on
	// the storm payload (§7.5: "3 representative incidents attached").
	stormRepresentatives = 3
	// stormEscalateSize is the §7.5 size escalator: a storm with at
	// least this many members has its severity bumped
	// warning→critical. Documented rule: only that one promotion —
	// info stays info (it never opened sessions anyway) and critical
	// cannot go higher.
	stormEscalateSize = 10
	// stormUpdateGrowth and stormUpdateMinInterval gate the §7.5 size
	// refresh (M2 drill observation 4: the formation payload's
	// affected_count is frozen at formation time — 3 — while reality
	// grows to 33). A kind=storm.update followup is due when
	// membership has DOUBLED since the last size report or grown by
	// stormUpdateGrowth members, whichever comes first, rate-limited
	// to one update per stormUpdateMinInterval. The initial payload
	// stays byte-identical (schema stability); the update is a NEW
	// kind with its own pin.
	stormUpdateGrowth      = 10
	stormUpdateMinInterval = time.Minute
	// stormIdleTTL bounds an unresolved storm's lifetime: a storm
	// that saw no member activity (attach or re-fire) for this long
	// is closed so it cannot absorb unrelated future incidents
	// forever (e.g. recovery tracking disabled, so "all members
	// cleared" can never be observed). Deliberately generous — an
	// ongoing storm keeps refreshing itself through dedup's retry
	// safety net.
	stormIdleTTL = 30 * time.Minute
)

// pendingIncident is one windowed incident that is not (yet) part of
// a storm.
type pendingIncident struct {
	at       time.Time
	member   StormMember
	severity Severity
	zone     string
	// seen is the incident's own LastSeen (signal data, deterministic
	// — wall clock only drives the window), feeding the storm's
	// last_seen field.
	seen time.Time
	// keys indexes the candidate set; order preserves the resolver's
	// priority order.
	keys  map[string]Ancestor
	order []string
}

// stormState is one open storm.
type stormState struct {
	id          string
	ancestor    Ancestor
	fingerprint string
	reason      string
	sessionID   string
	maxSeverity Severity
	firstSeen   time.Time
	lastSeen    time.Time
	lastActive  time.Time
	members     []StormMember
	resolved    map[EventKey]bool
	unresolved  int
	// reportedCount / reportedAt track the last size report on the
	// wire (formation, then each kind=storm.update) for the
	// SizeUpdate thresholds.
	reportedCount int
	reportedAt    time.Time
}

// StormCorrelator is the §7.5 pipeline stage. Single instance per
// sentinel; safe for concurrent use, though the dispatcher serializes
// Observe with its inject path anyway.
//
// State is in-memory only: after a sentinel restart the window and
// open storms are gone, but the members' dedup bindings (SessionID =
// storm session, persisted via --dedup-persist) still route followups
// and recovery outcomes to the storm session — only the aggregate
// "all members cleared → storm resolved" record is lost for storms
// that straddle a restart.
type StormCorrelator struct {
	mu       sync.Mutex
	window   time.Duration
	min      int
	resolver AncestorResolver

	pending []*pendingIncident
	storms  map[string]*stormState
	byKey   map[EventKey]string // member key → storm ID

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
}

// NewStormCorrelator constructs the correlator. window must be > 0,
// min >= 2 (a "storm" of one is an incident), resolver non-nil.
func NewStormCorrelator(window time.Duration, min int, resolver AncestorResolver) (*StormCorrelator, error) {
	if window <= 0 {
		return nil, fmt.Errorf("storm: window must be > 0 (got %s)", window)
	}
	if min < 2 {
		return nil, fmt.Errorf("storm: min must be >= 2 (got %d)", min)
	}
	if resolver == nil {
		return nil, fmt.Errorf("storm: resolver is required")
	}
	return &StormCorrelator{
		window:   window,
		min:      min,
		resolver: resolver,
		storms:   make(map[string]*stormState),
		byKey:    make(map[EventKey]string),
	}, nil
}

func (c *StormCorrelator) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// severityRank orders severities for the max-member computation.
func severityRank(s Severity) int {
	switch s {
	case SeverityCritical:
		return 2
	case SeverityWarning:
		return 1
	default:
		return 0
	}
}

// Observe records a NEW incident (dedup verdict: new — duplicates
// never reach this stage) and answers whether it forms a storm,
// attaches to one, or proceeds per-incident. Attach is checked before
// formation: while a storm holding one of the incident's candidate
// keys is open and unresolved, the incident is a late arrival of that
// storm, never the seed of a second one.
func (c *StormCorrelator) Observe(sig Signal) StormVerdict {
	now := c.clock()
	// Message-aware canonical: the correlator has the signal (and so
	// its message) in hand, and its member keys must agree with the
	// dedup cache's — both sides derive them via
	// CanonicalReasonForEvent. The member ref's wire Reason below
	// stays the original.
	key := sig.CanonicalKey()
	member := StormMember{
		Key:          key,
		Fingerprint:  sig.Fingerprint,
		Namespace:    sig.Namespace,
		KindOfObject: sig.KindOfObject,
		Name:         sig.Name,
		Reason:       sig.Key.Reason,
		FirstSeen:    sig.FirstSeen,
	}
	if member.FirstSeen.IsZero() {
		member.FirstSeen = now
	}
	seen := sig.LastSeen
	if seen.IsZero() {
		seen = now
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.prune(now)

	cands := c.resolver.Ancestors(ObjectRef{Kind: sig.KindOfObject, Namespace: sig.Namespace, Name: sig.Name})

	// 1. Late arrival: best-priority candidate owned by an open storm
	// wins (dedup's retry safety net can re-fire an existing member —
	// addMember is idempotent by key and re-arms its clearance).
	for _, a := range cands {
		st, ok := c.storms[a.Key()]
		if !ok {
			continue
		}
		st.addMember(member, sig.Severity, seen, now)
		c.byKey[key] = st.id
		v := StormVerdict{Kind: StormAttached, Storm: st.info(), Member: member}
		v.SizeUpdate = st.maybeSizeUpdate(now)
		return v
	}

	if len(cands) == 0 {
		// Unresolvable (topology index not ready, or the object is
		// not in it): per-incident, and not windowed — an incident
		// with no key can never correlate.
		return StormVerdict{Kind: StormNone}
	}

	// 2. Window the incident (replacing any prior entry for the same
	// key — a re-fire is still one incident) and check formation on
	// ITS candidates, best key first.
	p := &pendingIncident{
		at:       now,
		member:   member,
		severity: sig.Severity,
		zone:     sig.Zone,
		seen:     seen,
		keys:     make(map[string]Ancestor, len(cands)),
	}
	for _, a := range cands {
		k := a.Key()
		if _, dup := p.keys[k]; dup {
			continue
		}
		p.keys[k] = a
		p.order = append(p.order, k)
	}
	replaced := false
	for i, q := range c.pending {
		if q.member.Key == key {
			c.pending[i] = p
			replaced = true
			break
		}
	}
	if !replaced {
		c.pending = append(c.pending, p)
	}

	for _, k := range p.order {
		var group []*pendingIncident
		for _, q := range c.pending {
			if _, shares := q.keys[k]; shares {
				group = append(group, q)
			}
		}
		if len(group) < c.min {
			continue
		}
		st := c.form(p.keys[k], group, now)
		return StormVerdict{
			Kind:    StormFormed,
			Storm:   st.info(),
			Members: append([]StormMember(nil), st.members...),
			Member:  member,
		}
	}
	return StormVerdict{Kind: StormNone}
}

// form opens a storm over key with the grouped window entries as
// members (arrival order) and removes them from the window. Caller
// holds mu.
func (c *StormCorrelator) form(ancestor Ancestor, group []*pendingIncident, now time.Time) *stormState {
	first := group[0]
	st := &stormState{
		id:          ancestor.Key(),
		ancestor:    ancestor,
		reason:      first.member.Key.Reason,
		fingerprint: Fingerprint(KindStorm, first.member.Key.Reason, ancestor.Kind, first.zone),
		maxSeverity: SeverityInfo,
		firstSeen:   first.member.FirstSeen,
		lastSeen:    first.seen,
		lastActive:  now,
		resolved:    make(map[EventKey]bool),
	}
	inGroup := make(map[EventKey]bool, len(group))
	for _, q := range group {
		inGroup[q.member.Key] = true
		st.members = append(st.members, q.member)
		st.resolved[q.member.Key] = false
		st.unresolved++
		if severityRank(q.severity) > severityRank(st.maxSeverity) {
			st.maxSeverity = q.severity
		}
		if q.member.FirstSeen.Before(st.firstSeen) {
			st.firstSeen = q.member.FirstSeen
		}
		if q.seen.After(st.lastSeen) {
			st.lastSeen = q.seen
		}
		c.byKey[q.member.Key] = st.id
	}
	kept := c.pending[:0]
	for _, q := range c.pending {
		if !inGroup[q.member.Key] {
			kept = append(kept, q)
		}
	}
	c.pending = kept
	// The formation payload is the first size report.
	st.reportedCount = len(st.members)
	st.reportedAt = now
	c.storms[st.id] = st
	return st
}

// maybeSizeUpdate decides whether a kind=storm.update size refresh is
// due after a member attach (see stormUpdateGrowth /
// stormUpdateMinInterval), advancing the report cursor when it is.
// Deliberately event-driven: a storm that stops growing gets no
// trailing update — the member followups and the eventual resolved
// record already carry the final count. Caller holds mu.
func (st *stormState) maybeSizeUpdate(now time.Time) *StormSizeUpdate {
	count := len(st.members)
	grew := count - st.reportedCount
	if count < 2*st.reportedCount && count < st.reportedCount+stormUpdateGrowth {
		return nil
	}
	if now.Sub(st.reportedAt) < stormUpdateMinInterval {
		// Rate limit: hold the cursor so a later attach fires the
		// (larger) update once the interval has passed.
		return nil
	}
	nss := make(map[string]bool)
	for _, m := range st.members {
		if m.Namespace != "" {
			nss[m.Namespace] = true
		}
	}
	st.reportedCount = count
	st.reportedAt = now
	return &StormSizeUpdate{
		AffectedCount:  count,
		NamespaceCount: len(nss),
		NewSinceLast:   grew,
	}
}

// addMember attaches (or re-fires) a member on an open storm.
func (st *stormState) addMember(m StormMember, sev Severity, seen, now time.Time) {
	if seen.After(st.lastSeen) {
		st.lastSeen = seen
	}
	st.lastActive = now
	if severityRank(sev) > severityRank(st.maxSeverity) {
		st.maxSeverity = sev
	}
	if done, known := st.resolved[m.Key]; known {
		if done {
			// Re-fire of a member that had cleared: re-arm it.
			st.resolved[m.Key] = false
			st.unresolved++
		}
		return
	}
	st.members = append(st.members, m)
	st.resolved[m.Key] = false
	st.unresolved++
}

// info snapshots the storm.
func (st *stormState) info() StormInfo {
	sev := st.maxSeverity
	if sev == SeverityWarning && len(st.members) >= stormEscalateSize {
		sev = SeverityCritical
	}
	nss := make(map[string]bool)
	fps := make([]string, 0, len(st.members))
	for _, m := range st.members {
		if m.Namespace != "" {
			nss[m.Namespace] = true
		}
		fps = append(fps, m.Fingerprint)
	}
	reps := st.members
	if len(reps) > stormRepresentatives {
		reps = reps[:stormRepresentatives]
	}
	return StormInfo{
		ID:                 st.id,
		Ancestor:           st.ancestor,
		Fingerprint:        st.fingerprint,
		Reason:             st.reason,
		Severity:           sev,
		SessionID:          st.sessionID,
		AffectedCount:      len(st.members),
		NamespaceCount:     len(nss),
		FirstSeen:          st.firstSeen,
		LastSeen:           st.lastSeen,
		Representatives:    append([]StormMember(nil), reps...),
		MemberFingerprints: fps,
	}
}

// prune expires window entries beyond the correlation window and
// closes idle storms (see stormIdleTTL). Caller holds mu.
func (c *StormCorrelator) prune(now time.Time) {
	cutoff := now.Add(-c.window)
	kept := c.pending[:0]
	for _, q := range c.pending {
		if q.at.After(cutoff) {
			kept = append(kept, q)
		}
	}
	c.pending = kept
	for id, st := range c.storms {
		if now.Sub(st.lastActive) > stormIdleTTL {
			c.close(id, st)
		}
	}
}

// close removes a storm and its member index. Caller holds mu.
func (c *StormCorrelator) close(id string, st *stormState) {
	for _, m := range st.members {
		if c.byKey[m.Key] == id {
			delete(c.byKey, m.Key)
		}
	}
	delete(c.storms, id)
}

// NoteMemberSession records the per-incident session a windowed
// incident opened, so that if it later becomes a storm member the
// dispatcher knows which session to supersede. Called by the
// dispatcher right after a per-incident CreateSession succeeds.
func (c *StormCorrelator) NoteMemberSession(key EventKey, sessionID string) {
	key.Reason = CanonicalReason(key.Reason)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, q := range c.pending {
		if q.member.Key == key {
			q.member.SessionID = sessionID
			return
		}
	}
}

// BindStormSession attaches the created storm session to the storm.
// The dispatcher calls it right after CreateSession for a
// StormFormed verdict; late arrivals read it back via
// StormVerdict.Storm.SessionID.
func (c *StormCorrelator) BindStormSession(id, sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st, ok := c.storms[id]; ok {
		st.sessionID = sessionID
	}
}

// MemberResolved records that a member incident's symptom cleared
// (its kind=resolved outcome fired, §7.4). When that was the LAST
// unresolved member, the storm is resolved: it is closed (so future
// incidents form fresh storms) and its final snapshot is returned
// with done=true for the caller to inject the storm's own outcome
// record. ok=false when the key is not a storm member.
func (c *StormCorrelator) MemberResolved(key EventKey) (info StormInfo, done, ok bool) {
	key.Reason = CanonicalReason(key.Reason)
	c.mu.Lock()
	defer c.mu.Unlock()
	id, isMember := c.byKey[key]
	if !isMember {
		return StormInfo{}, false, false
	}
	st := c.storms[id]
	if st == nil {
		delete(c.byKey, key)
		return StormInfo{}, false, false
	}
	if resolved, known := st.resolved[key]; known && !resolved {
		st.resolved[key] = true
		st.unresolved--
	}
	if st.unresolved > 0 {
		return st.info(), false, true
	}
	final := st.info()
	c.close(id, st)
	return final, true, true
}

// MemberReverted re-arms a member whose symptom recurred after its
// resolve (kind=resolved.reverted): the storm's "all members clear"
// bar rises back accordingly.
func (c *StormCorrelator) MemberReverted(key EventKey) {
	key.Reason = CanonicalReason(key.Reason)
	c.mu.Lock()
	defer c.mu.Unlock()
	id, isMember := c.byKey[key]
	if !isMember {
		return
	}
	st := c.storms[id]
	if st == nil {
		return
	}
	if resolved, known := st.resolved[key]; known && resolved {
		st.resolved[key] = false
		st.unresolved++
		st.lastActive = c.clock()
	}
}

// ActiveStorms reports the number of open storms (metrics).
func (c *StormCorrelator) ActiveStorms() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.storms)
}
