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
	"sync"
	"time"
)

// Resolution is the §7.4/§9.3 outcome class on a resolved incident.
// It is schema-stable — the corpus harvester and the agent both
// branch on it, so the values are part of the wire contract: the
// agent must be able to distinguish "the workload recovered" from
// "the workload was deleted out from under the incident".
type Resolution string

const (
	// ResolutionRecovered: the symptom cleared and the object (or its
	// controller's replacement) was observed healthy for the full
	// stability window. The fix — whatever it was — stuck.
	ResolutionRecovered Resolution = "recovered"
	// ResolutionObjectDeleted: the affected object is gone and no
	// owner-managed replacement exists. The incident is over, but
	// nothing was "fixed" — the agent must not count this as a
	// verified fix.
	ResolutionObjectDeleted Resolution = "object_deleted"
)

// Recovery is the §7.4 outcome attachment carried by kind=resolved /
// kind=resolved.reverted Signals. Like Forecast/Enrichment it is nil
// on every other kind.
type Recovery struct {
	// ClearedAfter is how long after the incident's FirstSeen the
	// symptom was (last) observed to clear.
	ClearedAfter time.Duration
	// ObservedStableFor is how long the symptom stayed continuously
	// clear before the sentinel called it resolved (>= the configured
	// stability window).
	ObservedStableFor time.Duration
	// Resolution distinguishes recovered from object_deleted.
	Resolution Resolution
	// ResolvedAt is when the tracker declared the incident resolved.
	ResolvedAt time.Time
	// RevertedAfter is set only on kind=resolved.reverted: how long
	// after ResolvedAt the symptom recurred.
	RevertedAfter time.Duration
}

// IncidentRef is the identity of a bound incident that the recovery
// tracker needs beyond the dedup EventKey: enough to (a) let a
// clearance observer find the object, and (b) compose the resolved
// payload after a sentinel restart, when the original Signal is gone.
// It rides on the dedup cache's persisted entries (omitempty — older
// snapshots simply lack it).
type IncidentRef struct {
	Namespace     string `json:"namespace,omitempty"`
	KindOfObject  string `json:"kind_of_object,omitempty"`
	Name          string `json:"name,omitempty"`
	Container     string `json:"container,omitempty"`
	ControllerRef string `json:"controller_ref,omitempty"`
	// Fingerprint is the ORIGINAL incident's fingerprint. Resolved
	// signals carry it unchanged so AX / health can join outcome to
	// incident on one key.
	Fingerprint string `json:"fingerprint,omitempty"`
	Cluster     string `json:"cluster,omitempty"`
}

// IncidentRef extracts the recovery-relevant identity from a Signal.
// Used by the dispatcher both to persist the binding (BindIncident)
// and to start tracking (RecoveryTracker.Track) from one source of
// truth.
func (s Signal) IncidentRef() IncidentRef {
	return IncidentRef{
		Namespace:     s.Namespace,
		KindOfObject:  s.KindOfObject,
		Name:          s.Name,
		Container:     s.Container,
		ControllerRef: s.ControllerRef,
		Fingerprint:   s.Fingerprint,
		Cluster:       s.Cluster,
	}
}

// Incident is one bound incident the recovery tracker watches: the
// dedup key, the session the outcome routes to, when the symptom was
// first seen, and the identity reference.
type Incident struct {
	Key       EventKey
	SessionID string
	FirstSeen time.Time
	Ref       IncidentRef
}

// Clearance is a ClearanceObserver's verdict on one tracked incident.
type Clearance struct {
	// Cleared reports whether the symptom is absent right now.
	Cleared bool
	// StableSince is the earliest instant since which the observer
	// can vouch the symptom has been continuously absent (e.g. the
	// pod's Ready transition or its newest container start). The
	// tracker counts the stability window from here, so a container
	// restart between ticks — visible as a forward jump of
	// StableSince — restarts the window even if the pod looks Ready
	// at every tick. Zero means "absent as of this observation only".
	StableSince time.Time
	// Resolution says HOW the symptom is absent: recovered (object or
	// replacement healthy) or object_deleted (object gone, owner
	// gone). Meaningful only when Cleared.
	Resolution Resolution
}

// ClearanceObserver answers "is this incident's symptom currently
// absent?" for the incidents it knows how to judge. Observers are
// symptom/source-specific (§7.4: each source that can observe a
// symptom can observe its absence) — the tracker itself hardcodes no
// predicates. The second return is false when the observer cannot
// judge this incident at all (wrong object kind, informer not yet
// synced); the tracker then keeps waiting or asks other observers.
type ClearanceObserver interface {
	Clearance(inc Incident) (Clearance, bool)
}

// recoveryState is the per-incident state machine position (§7.4):
//
//	symptomatic --predicate true--> clearing
//	clearing --predicate false--> symptomatic        (flap: window resets)
//	clearing --window elapsed--> resolved            (emit kind=resolved)
//	resolved --recurs within revert window--> symptomatic
//	                                                 (emit kind=resolved.reverted, re-arm)
//	resolved --revert window elapses--> untracked    (outcome final)
type recoveryState int

const (
	stateSymptomatic recoveryState = iota
	stateClearing
	stateResolved
)

// trackedIncident is the tracker's per-incident record.
type trackedIncident struct {
	inc   Incident
	state recoveryState
	// stabilityStart is the start of the current clear streak: the
	// later of when the tracker first saw the predicate true and the
	// observer's StableSince. Valid in stateClearing.
	stabilityStart time.Time
	// resolvedAt / recovery capture the resolution, for composing a
	// resolved.reverted payload if the symptom recurs.
	resolvedAt time.Time
	recovery   Recovery
	// trackedAt + judged implement the uncovered-incident TTL: an
	// incident no observer ever claims (e.g. a Node-scoped event in
	// this PR, where the only observer is pod-scoped) is dropped
	// after uncoveredTTL instead of being tracked forever.
	trackedAt time.Time
	judged    bool
}

// RecoveryTracker watches bound incidents for symptom clearance and
// drives the §7.4 state machine, emitting kind=resolved /
// kind=resolved.reverted Signals through the emit callback (wired to
// the dispatcher, which routes them to the incident's bound session).
//
// Concurrency: Track/Tick/Len are safe from any goroutine. emit is
// invoked WITHOUT the tracker lock held, from the goroutine that
// called Tick.
type RecoveryTracker struct {
	mu        sync.Mutex
	incidents map[EventKey]*trackedIncident
	observers []ClearanceObserver
	// stableFor is the §7.4 stability window: how long the symptom
	// must stay clear before resolved is emitted.
	stableFor time.Duration
	// revertWindow is how long after a resolve a recurrence counts as
	// resolved.reverted (§7.4: "recurs within the stability window" —
	// defaults to stableFor).
	revertWindow time.Duration
	// uncoveredTTL bounds how long a never-judged incident stays
	// tracked; see trackedIncident.judged.
	uncoveredTTL time.Duration
	emit         func(Signal)
	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
}

// defaultUncoveredTTL is how long the tracker holds an incident no
// observer can judge before giving up on it. Generous: observer
// coverage grows source by source, and a dropped incident costs only
// the missing outcome record, never a missed page.
const defaultUncoveredTTL = time.Hour

// NewRecoveryTracker constructs a tracker. stableFor must be > 0
// (callers disable recovery by not constructing a tracker). The
// revert window is set equal to stableFor per §7.4. emit receives
// the resolved / resolved.reverted Signals.
func NewRecoveryTracker(stableFor time.Duration, emit func(Signal)) *RecoveryTracker {
	return &RecoveryTracker{
		incidents:    make(map[EventKey]*trackedIncident),
		stableFor:    stableFor,
		revertWindow: stableFor,
		uncoveredTTL: defaultUncoveredTTL,
		emit:         emit,
	}
}

// AddObserver registers a clearance observer. Not safe to call
// concurrently with Tick — wire observers at startup.
func (t *RecoveryTracker) AddObserver(o ClearanceObserver) {
	t.observers = append(t.observers, o)
}

func (t *RecoveryTracker) clock() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

// Track starts (or restarts) watching a bound incident. Called by the
// dispatcher right after it binds a new incident to a session, and by
// startup seeding for bindings restored from the dedup snapshot. The
// key's reason is canonicalized to match the dedup cache's binding
// key. Re-tracking an already-tracked key resets it to symptomatic —
// correct for the dedup retry-safety-net case, where a new session
// replaces the old binding.
func (t *RecoveryTracker) Track(inc Incident) {
	inc.Key.Reason = CanonicalReason(inc.Key.Reason)
	now := t.clock()
	if inc.FirstSeen.IsZero() {
		inc.FirstSeen = now
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.incidents[inc.Key] = &trackedIncident{
		inc:       inc,
		state:     stateSymptomatic,
		trackedAt: now,
	}
}

// Len returns the number of incidents currently tracked (the
// recovery_tracking gauge).
func (t *RecoveryTracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.incidents)
}

// Tick evaluates every tracked incident against the observers and
// advances the state machine, emitting resolved / resolved.reverted
// Signals for any transitions. Called on an interval by the watch
// loop; exported so tests can drive it with a fake clock.
func (t *RecoveryTracker) Tick() {
	now := t.clock()
	var emits []Signal
	t.mu.Lock()
	for key, ti := range t.incidents {
		verdict, ok := t.judge(ti.inc)
		if !ok {
			if !ti.judged && now.Sub(ti.trackedAt) > t.uncoveredTTL {
				// No observer has ever claimed this incident;
				// stop carrying it (no outcome record — this
				// PR's observer coverage is pod-scoped only).
				delete(t.incidents, key)
			}
			continue
		}
		ti.judged = true
		switch ti.state {
		case stateSymptomatic:
			if verdict.Cleared {
				ti.state = stateClearing
				ti.stabilityStart = t.stabilityStart(ti.inc, verdict, now)
			}
		case stateClearing:
			if !verdict.Cleared {
				// Flap: symptom recurred inside the window.
				// Back to symptomatic; window resets.
				ti.state = stateSymptomatic
				continue
			}
			// A forward jump of StableSince (container restarted
			// between ticks, then went Ready again) restarts the
			// window even though the predicate never read false.
			if s := t.stabilityStart(ti.inc, verdict, now); s.After(ti.stabilityStart) {
				ti.stabilityStart = s
			}
			if now.Sub(ti.stabilityStart) >= t.stableFor {
				rec := Recovery{
					ClearedAfter:      ti.stabilityStart.Sub(ti.inc.FirstSeen),
					ObservedStableFor: now.Sub(ti.stabilityStart),
					Resolution:        verdict.Resolution,
					ResolvedAt:        now,
				}
				ti.state = stateResolved
				ti.resolvedAt = now
				ti.recovery = rec
				emits = append(emits, resolvedSignal(KindResolved, ti.inc, rec))
			}
		case stateResolved:
			if !verdict.Cleared && now.Sub(ti.resolvedAt) <= t.revertWindow {
				// The fix did not stick: recurrence within the
				// revert window. Emit the reversion into the same
				// session and re-arm the state machine.
				rec := ti.recovery
				rec.RevertedAfter = now.Sub(ti.resolvedAt)
				emits = append(emits, resolvedSignal(KindResolvedReverted, ti.inc, rec))
				ti.state = stateSymptomatic
				continue
			}
			if now.Sub(ti.resolvedAt) > t.revertWindow {
				// Outcome final; stop tracking. The dedup entry
				// (and its session binding) age out on their own.
				delete(t.incidents, key)
			}
		}
	}
	t.mu.Unlock()
	for _, sig := range emits {
		t.emit(sig)
	}
}

// judge asks the observers in registration order; the first one that
// can judge the incident wins.
func (t *RecoveryTracker) judge(inc Incident) (Clearance, bool) {
	for _, o := range t.observers {
		if verdict, ok := o.Clearance(inc); ok {
			return verdict, true
		}
	}
	return Clearance{}, false
}

// stabilityStart computes when the current clear streak began: the
// observer's StableSince, clamped into (FirstSeen, now]. The lower
// clamp stops a stale "ready since before the incident" reading from
// resolving instantly; the upper clamp defends against clock skew.
// A zero StableSince means the observer vouches for this instant only.
func (t *RecoveryTracker) stabilityStart(inc Incident, verdict Clearance, now time.Time) time.Time {
	s := verdict.StableSince
	if s.IsZero() || s.After(now) {
		s = now
	}
	if s.Before(inc.FirstSeen) {
		s = inc.FirstSeen
	}
	return s
}

// resolvedSignal composes the outcome Signal for a transition: the
// original incident's identity and fingerprint (same fingerprint →
// AX/health join outcome to incident), with the Recovery attachment.
// Severity is info — the outcome routes to the incident's bound
// session regardless of severity policy.
func resolvedSignal(kind string, inc Incident, rec Recovery) Signal {
	r := rec
	return Signal{
		Kind:        kind,
		Source:      SourceSentinel,
		Severity:    SeverityInfo,
		Fingerprint: inc.Ref.Fingerprint,
		Cluster:     inc.Ref.Cluster,
		TriageEvent: TriageEvent{
			Key:           inc.Key,
			Namespace:     inc.Ref.Namespace,
			KindOfObject:  inc.Ref.KindOfObject,
			Name:          inc.Ref.Name,
			Container:     inc.Ref.Container,
			ControllerRef: inc.Ref.ControllerRef,
			FirstSeen:     inc.FirstSeen,
		},
		Recovery: &r,
	}
}
