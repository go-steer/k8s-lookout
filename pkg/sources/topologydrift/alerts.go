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

package topologydrift

import (
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// alertKey identifies one episode: a subject on one axis.
//
// §9.1's reason for the pair rather than the subject alone: a Deployment with
// both a zone constraint and a hostname constraint can be perfectly spread
// across zones and stacked three deep on one node, and collapsing those into
// one machine would lose whichever breached second.
type alertKey struct {
	Subject leeway.SubjectRef
	Key     leeway.TopologyKey
}

// alertEntry is one machine plus the tier its last verdict carried.
//
// The tier is not part of AlertState and deliberately is not persisted: it is
// recomputed from the cluster at every evaluation, and a restart that restored
// a stale tier would report yesterday's confidence about today's distribution.
// It is kept here only so the alert_state series can be split by it without
// joining against the evaluation map at scrape time.
type alertEntry struct {
	state leeway.AlertState
	tier  leeway.Tier
}

// Alerts is the §8.2 state machine for every episode this source is tracking.
//
// # Only breaching subjects have an entry
//
// A subject in PhaseOK that is not breaching is absent, not stored as a zero
// value. That is the same bound the store draws — "deleting rather than
// persisting a row in PhaseOK is what keeps the table bounded by how many
// subjects are drifting right now" — and it has to hold in memory for the same
// reason: a fleet-scale cluster has twenty thousand subjects across two axes
// and, on a good day, none of them drifting.
//
// # The machine is not driven by the evaluation path
//
// Advance is called from one periodic pass over every stored verdict, not from
// Source.evaluate. Two reasons. Dwell is a measure of how long a condition has
// held, and driving it off events would make it advance faster for a subject
// whose pods churn than for a quiet one that is equally wrong. And §8.2 wants
// a whole pass judged against one instant, or a slow sweep over twenty
// thousand subjects gives the ones at the end a longer dwell than the ones at
// the start.
type Alerts struct {
	mu    sync.Mutex
	dwell leeway.Dwell

	byKey map[alertKey]*alertEntry

	// pending holds records read from the store that have not yet met a scored
	// evaluation. See Reconcile.
	pending   map[alertKey]leeway.AlertRecord
	pendingAt time.Time
}

// NewAlerts returns an empty machine set running on d.
func NewAlerts(d leeway.Dwell) *Alerts {
	return &Alerts{dwell: d, byKey: make(map[alertKey]*alertEntry)}
}

// Load seats the persisted records §9.3 step 6 reconciles against.
//
// They are held aside rather than installed directly, because a record can
// only be reconciled against a verdict and there are no verdicts at startup:
// the informers have synced but the work queue has not yet scored anything. So
// each record waits until its subject's axis turns up in a pass, and Pass
// reconciles it there. at is the instant the load happened, and bounds the
// wait — see Pass.
//
// A record whose subject key this build cannot parse is dropped here. It was
// written by a different build, and §9.2's rule is that an unreadable history
// costs one dwell rather than the monitoring.
func (a *Alerts) Load(records []leeway.AlertRecord, at time.Time) int {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.pending = make(map[alertKey]leeway.AlertRecord, len(records))
	a.pendingAt = at
	for _, rec := range records {
		sub, ok := leeway.ParseSubjectRef(rec.SubjectKey)
		if !ok {
			continue
		}
		a.pending[alertKey{Subject: sub, Key: leeway.TopologyKey(rec.TopologyKey)}] = rec
	}
	return len(a.pending)
}

// Outcome is what one pass did to one episode.
type Outcome struct {
	Subject    leeway.SubjectRef
	Key        leeway.TopologyKey
	Transition leeway.Transition
	State      leeway.AlertState
	Tier       leeway.Tier

	// Gone reports that the episode left the machine — either it ended, or
	// its subject stopped being tracked. The caller owes the store a delete
	// for it and nothing else.
	Gone bool
}

func outcome(k alertKey, tr leeway.Transition, st leeway.AlertState, tier leeway.Tier, gone bool) Outcome {
	return Outcome{Subject: k.Subject, Key: k.Key, Transition: tr, State: st, Tier: tier, Gone: gone}
}

// Judgement is one subject-axis's verdict, as Pass consumes them.
type Judgement struct {
	Subject  leeway.SubjectRef
	Key      leeway.TopologyKey
	Breached bool
	Tier     leeway.Tier
}

// Pass runs the machine over one complete set of verdicts and reports every
// episode that moved.
//
// The set must be complete — every subject-axis this source currently has a
// score for. Pass uses its absence as information: an episode with no
// judgement in the pass belongs to a subject that is no longer tracked, and it
// is dropped rather than left to dwell forever against a cluster that has
// forgotten it. That is the only eviction rule this map has.
//
// Reconciliation of persisted state happens here too, rather than at load, for
// the reason Load describes: a record can only be reconciled against a
// verdict. Records still unclaimed once reconcileGrace has elapsed since the
// load are discarded — their subjects did not come back, and holding a dwell
// timer open for a Deployment that was deleted while we were down is exactly
// the phantom episode §9.3's repair rules are written to avoid.
func (a *Alerts) Pass(js []Judgement, now time.Time, reconcileGrace time.Duration) []Outcome {
	a.mu.Lock()
	defer a.mu.Unlock()

	var out []Outcome
	seen := make(map[alertKey]struct{}, len(js))
	for _, j := range js {
		k := alertKey{Subject: j.Subject, Key: j.Key}
		seen[k] = struct{}{}

		if entry, live := a.byKey[k]; live {
			tr := entry.state.Advance(j.Breached, now, a.dwell)
			entry.tier = j.Tier
			if entry.state.Phase == leeway.PhaseOK {
				delete(a.byKey, k)
				out = append(out, outcome(k, tr, entry.state, j.Tier, true))
				continue
			}
			if tr != leeway.TransitionNone {
				out = append(out, outcome(k, tr, entry.state, j.Tier, false))
			}
			continue
		}

		if rec, restored := a.pending[k]; restored {
			delete(a.pending, k)
			rec, tr := leeway.Reconcile(rec, j.Breached, now, a.dwell)
			if rec.Phase == leeway.PhaseOK {
				// Either the drift went away while we were down, or the
				// record was damaged past repair. Both owe the store a
				// delete; whether they owe a resolution is the caller's call,
				// and Transition already says which it was.
				out = append(out, outcome(k, tr, rec.AlertState, j.Tier, true))
				continue
			}
			a.byKey[k] = &alertEntry{state: rec.AlertState, tier: j.Tier}
			out = append(out, outcome(k, tr, rec.AlertState, j.Tier, false))
			continue
		}

		if !j.Breached {
			// The overwhelming majority: not drifting, no history, no entry.
			// Allocating one would make the map bounded by the cluster's size
			// rather than by its trouble.
			continue
		}
		entry := &alertEntry{tier: j.Tier}
		tr := entry.state.Advance(true, now, a.dwell)
		a.byKey[k] = entry
		out = append(out, outcome(k, tr, entry.state, j.Tier, false))
	}

	// Episodes whose subject stopped being tracked.
	for k, entry := range a.byKey {
		if _, ok := seen[k]; ok {
			continue
		}
		delete(a.byKey, k)
		out = append(out, outcome(k, leeway.TransitionNone, entry.state, entry.tier, true))
	}

	// Persisted records nobody claimed.
	if len(a.pending) > 0 && now.Sub(a.pendingAt) >= reconcileGrace {
		for k := range a.pending {
			out = append(out, outcome(k, leeway.TransitionNone, leeway.AlertState{}, leeway.TierNone, true))
		}
		a.pending = nil
	}
	return out
}

// Len is how many episodes are open.
func (a *Alerts) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.byKey)
}

// PendingLen is how many persisted records are still waiting for a verdict.
func (a *Alerts) PendingLen() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.pending)
}

// StateOf returns one episode's machine, and false when there is none.
func (a *Alerts) StateOf(sub leeway.SubjectRef, key leeway.TopologyKey) (leeway.AlertState, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry, ok := a.byKey[alertKey{Subject: sub, Key: key}]
	if !ok {
		return leeway.AlertState{}, false
	}
	return entry.state, true
}

// Each walks every open episode. It holds the lock for the duration, so the
// callback must not call back into Alerts.
func (a *Alerts) Each(yield alertObserver) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for k, entry := range a.byKey {
		yield(k.Subject, k.Key, entry.state, entry.tier)
	}
}
