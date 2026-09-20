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

package computeclass

import (
	"strings"
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// §8.2's dwell machine, at the grain §7.7.4 needs.
//
// The machine itself is pkg/leeway's and is reused unchanged — "the §8.2 state
// machine, dwell, hysteresis and §7.5 baselines all apply unchanged; only the
// score differs". What is local is the key it runs at.
//
// # One episode per rule, not per axis
//
// A class can be running almost entirely on its last rank AND have a dead tier
// nobody has touched in a month. Those are two facts, they carry two tiers, and
// resolving the first must not close the second. So the key is the axis plus
// the verdict's own episode key, which already separates the two rank_degraded
// rules from each other and the per-tier unused findings from one another.

// alertKey identifies one episode: one rule on one axis.
type alertKey struct {
	Axis leeway.AxisKey
	Rule string
}

// subjectKey renders the axis as the §9.1 subject it is persisted under.
func (k alertKey) subjectKey() string {
	return leeway.SubjectRef{Kind: leeway.SubjectPreferenceAxis, Name: k.Axis.Name}.String()
}

// stateKey is the discriminator the episode is persisted under alongside the
// subject — the column §9.1 calls topology_key, which at this grain is not a
// topology key at all.
//
// Reusing the column rather than adding a table is deliberate: the grain is
// identical (one subject, one independent episode per axis of judgement) and
// the column is free text. The provider is folded in because an axis is
// identified by provider AND name, and two providers could name a class the
// same thing — a collision that would silently merge two clusters' worth of
// dwell into one row.
func (k alertKey) stateKey() string {
	return string(k.Axis.Provider) + "/" + k.Rule
}

// parseStateKey inverts stateKey.
func parseStateKey(sub, state string) (alertKey, bool) {
	ref, ok := leeway.ParseSubjectRef(sub)
	if !ok || ref.Kind != leeway.SubjectPreferenceAxis {
		return alertKey{}, false
	}
	provider, rule, ok := strings.Cut(state, "/")
	if !ok || provider == "" || rule == "" {
		return alertKey{}, false
	}
	return alertKey{
		Axis: leeway.AxisKey{Provider: leeway.Provider(provider), Name: ref.Name},
		Rule: rule,
	}, true
}

// alertEntry is one machine plus the tier its last verdict carried. The tier is
// recomputed every pass and never persisted, for the reason topologydrift gives
// for the same field: a restored tier would report yesterday's confidence.
type alertEntry struct {
	state leeway.AlertState
	tier  leeway.Tier
}

// rankAlerts holds every open episode.
//
// Only a breaching rule has an entry. A class in a perfectly healthy state
// carries no rows at all, which keeps the map bounded by how much trouble the
// estate is in rather than by how many classes it declares — the same bound
// §8.2 draws for topology subjects, and it matters less here only because a
// cluster has seven compute classes and twenty thousand Deployments.
type rankAlerts struct {
	mu    sync.Mutex
	dwell leeway.Dwell

	byKey map[alertKey]*alertEntry

	pending   map[alertKey]leeway.AlertRecord
	pendingAt time.Time
}

func newRankAlerts(d leeway.Dwell) *rankAlerts {
	return &rankAlerts{dwell: d, byKey: map[alertKey]*alertEntry{}}
}

// Load seats persisted records for §9.3's reconciliation.
//
// Rows whose subject is not a preference axis are skipped rather than
// reported: the leeway_alert_state table is shared with topologydrift, and a
// source that claimed another's rows would reconcile them against verdicts it
// never made and then delete them. The filter is on the subject KIND, so it
// stays correct when a third kind of episode arrives.
func (a *rankAlerts) Load(records []leeway.AlertRecord, at time.Time) int {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.pending = make(map[alertKey]leeway.AlertRecord, len(records))
	a.pendingAt = at
	for _, rec := range records {
		key, ok := parseStateKey(rec.SubjectKey, rec.TopologyKey)
		if !ok {
			continue
		}
		a.pending[key] = rec
	}
	return len(a.pending)
}

// rankOutcome is what one pass did to one episode.
type rankOutcome struct {
	Key        alertKey
	Transition leeway.Transition
	State      leeway.AlertState
	Tier       leeway.Tier

	// Verdict is the reading that produced the move, absent for an episode
	// that left the machine because its axis stopped being judged.
	Verdict *leeway.RankVerdict

	// Gone reports that the episode is over and the store owes it a delete.
	Gone bool
}

// pass advances every machine against one complete set of judgements.
//
// Complete is load-bearing in the same way it is for topologydrift: absence is
// read as "this rule is no longer being judged" — a class deleted, or re-tiered
// so that the rule no longer applies — and the episode is dropped rather than
// left to dwell against something nobody is measuring.
func (a *rankAlerts) pass(js []rankJudgement, now time.Time, reconcileGrace time.Duration) []rankOutcome {
	a.mu.Lock()
	defer a.mu.Unlock()

	var out []rankOutcome
	seen := map[alertKey]struct{}{}

	for i := range js {
		j := &js[i]
		for vi := range j.verdicts {
			v := &j.verdicts[vi]
			k := alertKey{Axis: j.axis, Rule: v.EpisodeKey()}
			seen[k] = struct{}{}

			if entry, live := a.byKey[k]; live {
				tr := entry.state.Advance(v.Breached, now, a.dwell)
				entry.tier = v.Tier
				if entry.state.Phase == leeway.PhaseOK {
					delete(a.byKey, k)
					out = append(out, rankOutcome{Key: k, Transition: tr, State: entry.state, Tier: v.Tier, Verdict: v, Gone: true})
					continue
				}
				if tr != leeway.TransitionNone {
					out = append(out, rankOutcome{Key: k, Transition: tr, State: entry.state, Tier: v.Tier, Verdict: v})
				}
				continue
			}

			if rec, restored := a.pending[k]; restored {
				delete(a.pending, k)
				rec, tr := leeway.Reconcile(rec, v.Breached, now, a.dwell)
				if rec.Phase == leeway.PhaseOK {
					out = append(out, rankOutcome{Key: k, Transition: tr, State: rec.AlertState, Tier: v.Tier, Verdict: v, Gone: true})
					continue
				}
				a.byKey[k] = &alertEntry{state: rec.AlertState, tier: v.Tier}
				out = append(out, rankOutcome{Key: k, Transition: tr, State: rec.AlertState, Tier: v.Tier, Verdict: v})
				continue
			}

			if !v.Breached {
				continue
			}
			entry := &alertEntry{tier: v.Tier}
			tr := entry.state.Advance(true, now, a.dwell)
			a.byKey[k] = entry
			out = append(out, rankOutcome{Key: k, Transition: tr, State: entry.state, Tier: v.Tier, Verdict: v})
		}
	}

	for k, entry := range a.byKey {
		if _, ok := seen[k]; ok {
			continue
		}
		delete(a.byKey, k)
		out = append(out, rankOutcome{Key: k, Transition: leeway.TransitionNone, State: entry.state, Tier: entry.tier, Gone: true})
	}

	if len(a.pending) > 0 && now.Sub(a.pendingAt) >= reconcileGrace {
		for k := range a.pending {
			out = append(out, rankOutcome{Key: k, Transition: leeway.TransitionNone, Tier: leeway.TierNone, Gone: true})
		}
		a.pending = nil
	}
	return out
}

// Len is how many episodes are open.
func (a *rankAlerts) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.byKey)
}

// PendingLen is how many persisted records are still waiting for a verdict.
func (a *rankAlerts) PendingLen() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.pending)
}

// open reports whether an episode is still being tracked.
//
// The phase is deliberately not returned with it: the clearance observer's
// question is only "is this incident still ours to answer for", and a
// Resolving episode is as much ours as a Firing one — reporting it cleared
// because the machine has started its resolve dwell would pre-empt the dwell.
func (a *rankAlerts) open(k alertKey) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.byKey[k]
	return ok
}

// each walks every open episode. It holds the lock for the duration, so the
// callback must not call back into rankAlerts.
func (a *rankAlerts) each(yield func(alertKey, leeway.AlertState, leeway.Tier)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for k, entry := range a.byKey {
		yield(k, entry.state, entry.tier)
	}
}
