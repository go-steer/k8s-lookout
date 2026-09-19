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

package leeway

import "time"

// AlertRecord is one AlertState with the identity it belongs to and the
// instant it was last written — §9.1's "alert state, firstSeenAt, since, flap
// counters", which is the first of the three things that table says we must
// persist because we are their only source of truth.
//
// The unit is (cluster, subject, topology key), the same grain §8.2's machine
// runs at. A Deployment with both a zone constraint and a hostname constraint
// is two independent episodes: it can be perfectly spread across zones and
// stacked three-deep on one node, and collapsing those into one alert would
// lose whichever fired second.
type AlertRecord struct {
	Cluster     string
	SubjectKey  string
	TopologyKey string
	AlertState

	// UpdatedAt is when the record was last written. It is the store's
	// stamp, not the machine's, and it exists so a restart can measure how
	// long it was blind — §9.3 step 7's downtime assessment.
	UpdatedAt time.Time
}

// ParseAlertPhase is AlertPhase.String() inverted, for decoding a persisted
// record. ok is false for anything this binary does not recognise, which is
// how a store written by a newer build reads: not an error, just a phase we
// cannot act on. See Reconcile.
func ParseAlertPhase(s string) (AlertPhase, bool) {
	switch s {
	case "ok":
		return PhaseOK, true
	case "pending":
		return PhasePending, true
	case "firing":
		return PhaseFiring, true
	case "resolving":
		return PhaseResolving, true
	default:
		return PhaseOK, false
	}
}

// Reconcile is §9.3 step 6: one persisted alert state met with one fresh
// verdict, at the moment the sentinel arms.
//
// Three of §9.3's four rows are already what Advance does — a persisted Firing
// that is still drifting stays Firing with its `since` intact, a persisted
// Pending whose dwell elapsed during the downtime fires on the spot, and a
// subject with no persisted state starts a new Pending. The machine does not
// need to be told that a restart happened for those, because none of them
// depends on having watched the interval.
//
// The fourth row does, and it is why this function exists: **the clear-side
// clock restarts.** §9.3 states it for a persisted Firing that now looks OK —
// enter Resolving with the full resolveDuration, "do not resolve instantly;
// the cluster may look fine only because we just started" — and the same
// hazard applies, more sharply, to a persisted *Resolving*, which §9.3's list
// omits. A process that was down for two hours holding a ClearSince from
// before the outage would, on its very first observation, find the resolve
// window long elapsed and close the finding on the strength of one sample
// taken seconds after start-up. So any subject arriving in Resolving has its
// window restarted from now.
//
// The asymmetry with the fire side is deliberate and is the same one §8.2
// already encodes in resolveDuration = 3× forDuration: firing on a dwell that
// elapsed while we were blind at worst reports drift we did watch for the full
// dwell and which is still true now, whereas resolving on a stale clear
// silently drops a finding nobody was told was going away. Being wrong about
// "it is fixed" is the more expensive mistake.
func Reconcile(persisted AlertRecord, breached bool, now time.Time, d Dwell) (AlertRecord, Transition) {
	rec := persisted
	rec.AlertState = persisted.repair(now)
	if rec.Phase == PhaseResolving {
		rec.ClearSince = now
	}
	tr := rec.Advance(breached, now, d)
	return rec, tr
}

// repair makes a decoded record safe to run the machine on.
//
// Everything here is a state that cannot have been written by Advance, so
// reaching any of it means the record is damaged, was written by a different
// build, or was read across a clock that moved. The policy throughout is to
// prefer the reading that loses information over the one that acts on a lie: a
// forgotten episode costs one dwell, a phantom one pages somebody.
func (s AlertState) repair(now time.Time) AlertState {
	// A phase this binary does not have a rule for. Dropping the record
	// entirely is the only honest option — we would otherwise be guessing at
	// the semantics of a state a newer build invented.
	if s.Phase > PhaseResolving {
		return AlertState{}
	}

	// A timestamp in the future means the clock moved backwards under us (a
	// VM restore, an NTP step). Left alone, a future FirstSeenAt makes
	// now.Sub negative and the subject never leaves Pending — it would be
	// stuck, silently, for as long as the skew lasts. Clamping to now costs
	// at most one extra dwell.
	s.FirstSeenAt = notAfter(s.FirstSeenAt, now)
	s.Since = notAfter(s.Since, now)
	s.ClearSince = notAfter(s.ClearSince, now)
	for i := range s.Fires {
		s.Fires[i] = notAfter(s.Fires[i], now)
	}

	// A phase that needs a timestamp and has not got one is a truncated
	// write. Zero is not a neutral default here: time.Time{} is the year 1,
	// so a Pending with no FirstSeenAt would fire immediately.
	//
	// The matching Resolving-with-no-ClearSince case — which would resolve
	// immediately — is not repaired here, because Reconcile restarts that
	// window from `now` for every arriving Resolving record regardless. A
	// branch here would be unreachable; the behaviour is pinned by
	// TestReconcile_APersistedResolvingRestartsItsWindow instead.
	if s.Phase != PhaseOK && s.FirstSeenAt.IsZero() {
		s.FirstSeenAt = now
	}
	if s.Phase.Firing() && s.Since.IsZero() {
		s.Since = now
	}
	return s
}

func notAfter(t, now time.Time) time.Time {
	if t.After(now) {
		return now
	}
	return t
}
