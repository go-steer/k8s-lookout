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

// AlertPhase is where one (subject, topology key) sits in the §8.2 state
// machine.
//
// The machine exists because drift is slow and noisy and a verdict is
// instantaneous. Judge answers "is this subject over threshold right now",
// which on its own would page on a thirty-second rollout wobble and stop
// paging the moment a single pod moved back. The phases are what turn a
// sequence of verdicts into a condition worth telling somebody about.
type AlertPhase uint8

// The phases. PhaseOK is the zero value, so a freshly constructed or
// freshly decoded AlertState is a subject nobody has anything to say about.
const (
	PhaseOK AlertPhase = iota
	PhasePending
	PhaseFiring
	PhaseResolving
)

// String implements fmt.Stringer. These reach the `phase` metric label and the
// persisted record, so a spelling change here is a schema change.
func (p AlertPhase) String() string {
	switch p {
	case PhaseOK:
		return "ok"
	case PhasePending:
		return "pending"
	case PhaseFiring:
		return "firing"
	case PhaseResolving:
		return "resolving"
	default:
		return "unknown"
	}
}

// Firing reports whether a finding for this subject is currently outstanding —
// true in both Firing and Resolving, because a subject on its way back to OK
// has not got there yet and its finding is still the truth as last told.
func (p AlertPhase) Firing() bool { return p == PhaseFiring || p == PhaseResolving }

// Transition is what one Advance did, and is the caller's instruction about
// what to emit.
//
// It is returned rather than inferred from a before/after phase comparison
// because two of the five are invisible that way: a Pending subject falling
// back to OK and a Resolving one doing the same are the same pair of phases
// and opposite obligations — the first was never announced and must stay
// silent, the second was and owes a resolution.
type Transition uint8

// The transitions.
const (
	// TransitionNone is any evaluation that did not move the machine,
	// including the great majority that stay in PhaseOK.
	TransitionNone Transition = iota
	// TransitionPending is the first breach of an episode. Nothing is
	// emitted; the dwell clock starts.
	TransitionPending
	// TransitionFiring is a finding becoming real, from Pending after dwell
	// or from Resolving on a recurrence.
	TransitionFiring
	// TransitionClearing is a firing subject dropping under threshold. The
	// finding stays outstanding — this is the start of the resolve dwell,
	// not the end of the episode.
	TransitionClearing
	// TransitionResolved is the end of an episode that fired. It owes a
	// resolution to whoever received the finding.
	TransitionResolved
	// TransitionAbandoned is the end of an episode that never fired: a
	// breach that went away inside the dwell window. It owes nothing,
	// because nothing was ever said.
	TransitionAbandoned
)

// String implements fmt.Stringer.
func (t Transition) String() string {
	switch t {
	case TransitionPending:
		return "pending"
	case TransitionFiring:
		return "firing"
	case TransitionClearing:
		return "clearing"
	case TransitionResolved:
		return "resolved"
	case TransitionAbandoned:
		return "abandoned"
	default:
		return ""
	}
}

// Emits reports whether the transition is one a consumer has to act on.
// TransitionPending and TransitionClearing are internal bookkeeping: they move
// the machine without changing what anyone outside has been told.
func (t Transition) Emits() bool {
	return t == TransitionFiring || t == TransitionResolved
}

// Dwell is §8.2's timing, and §10.1's per-policy `thresholds.for` /
// `resolveAfter`.
type Dwell struct {
	// For is how long a breach must hold before it becomes a finding.
	For time.Duration
	// Resolve is how long it must stay clear before the finding resolves.
	// §8.2 sets it to 3×For by default to damp flapping: the asymmetry is
	// deliberate, because being wrong about "it is fixed" wastes somebody's
	// attention twice.
	Resolve time.Duration
	// FlapCount is how many times an episode may recur inside FlapWindow
	// before the subject is marked flapping.
	FlapCount int
	// FlapWindow is the span those recurrences are counted over.
	FlapWindow time.Duration
}

// DefaultDwell returns §8.2's and §10.1's defaults.
func DefaultDwell() Dwell {
	return Dwell{
		For:        10 * time.Minute,
		Resolve:    30 * time.Minute,
		FlapCount:  3,
		FlapWindow: time.Hour,
	}
}

// normalize fills in what a partial Dwell leaves out. A policy setting `for`
// and not `resolveAfter` means §8.2's 3× rule, not an instant resolve — the
// zero value of a duration is the most dangerous possible reading of "unset"
// here, because it turns the machine's whole hysteresis off silently.
func (d Dwell) normalize() Dwell {
	if d.For <= 0 {
		d.For = DefaultDwell().For
	}
	if d.Resolve <= 0 {
		d.Resolve = 3 * d.For
	}
	if d.FlapCount <= 0 {
		d.FlapCount = DefaultDwell().FlapCount
	}
	if d.FlapWindow <= 0 {
		d.FlapWindow = DefaultDwell().FlapWindow
	}
	return d
}

// AlertState is one subject-key's position in the machine, and is exactly what
// §9.1 says to persist: the phase, the two timestamps, and the flap history.
//
// Everything else a finding needs — the scores, the intent, the tier — is
// recomputed from the cluster at every evaluation, so it is deliberately not
// here. §9.1's durability requirement is one sentence: a thirty-minute dwell
// timer must not restart its clock because the process did.
type AlertState struct {
	Phase AlertPhase

	// FirstSeenAt is when the breach that produced the current episode was
	// first observed — the Pending entry, not the Firing one. It is what the
	// §8.5 payload reports, because "this has been wrong since 09:31" is the
	// fact an operator acts on, and the ten minutes we spent making sure are
	// ours, not theirs.
	FirstSeenAt time.Time

	// Since is when the subject entered PhaseFiring. It survives a recurrence
	// out of PhaseResolving; see Advance.
	Since time.Time

	// ClearSince is when the subject last dropped under threshold. Valid in
	// PhaseResolving.
	ClearSince time.Time

	// Fires are the instants at which this subject entered PhaseFiring,
	// newest last, pruned to FlapWindow. §8.2 counts Firing→OK→Firing
	// transitions, which is this list less its first entry — recording the
	// firings rather than the transitions between them means one rule covers
	// both ways a subject can come back, through a completed resolve and
	// through a recurrence inside the resolve window.
	//
	// A slice rather than a counter is what lets the window slide. A counter
	// would need something to tick it down, and a subject that has settled
	// generates no events to tick it with — so it would stay marked flapping
	// until the next time it misbehaved.
	Fires []time.Time
}

// Advance feeds one verdict to the machine and reports what moved.
//
// now is passed in rather than read, and not only for testability: every
// subject in a reconcile pass must be judged against one instant, or a slow
// sweep over twenty thousand subjects would give the ones at the end a
// slightly longer dwell than the ones at the start.
func (s *AlertState) Advance(breached bool, now time.Time, d Dwell) Transition {
	d = d.normalize()

	switch s.Phase {
	case PhaseOK:
		if !breached {
			return TransitionNone
		}
		s.Phase = PhasePending
		s.FirstSeenAt = now
		return TransitionPending

	case PhasePending:
		switch {
		case !breached:
			// Abandoned, not resolved. Nothing was emitted, so the episode
			// leaves no trace and — importantly — does not count as a flap:
			// a subject that twitches over the threshold for a minute every
			// ten is not flapping, it is below the dwell, which is the case
			// the dwell exists for.
			s.reset()
			return TransitionAbandoned
		case now.Sub(s.FirstSeenAt) >= d.For:
			s.fire(now, d)
			s.Since = now
			return TransitionFiring
		default:
			return TransitionNone
		}

	case PhaseFiring:
		if breached {
			return TransitionNone
		}
		s.Phase = PhaseResolving
		s.ClearSince = now
		return TransitionClearing

	case PhaseResolving:
		switch {
		case breached:
			// Straight back to Firing with no second dwell. The finding never
			// stopped being outstanding, so there is nothing to re-confirm —
			// and making a recurrence serve another For would leave a subject
			// oscillating on either side of the threshold permanently
			// unreported.
			// Since is deliberately not touched: a subject that dipped under
			// threshold for four minutes of a thirty-minute resolve window
			// did not start a new episode, and restarting the clock would let
			// one oscillating just inside the resolve dwell report itself as
			// perpetually new.
			s.fire(now, d)
			s.ClearSince = time.Time{}
			return TransitionFiring
		case now.Sub(s.ClearSince) >= d.Resolve:
			s.reset()
			return TransitionResolved
		default:
			return TransitionNone
		}

	default:
		return TransitionNone
	}
}

// reset returns the state to OK. The fire history survives, and has to: it is
// the record of how badly this subject has been behaving, and flapping is by
// definition a sequence of episodes that ended. Clearing it at the end of each
// one would mean no subject could ever be seen to flap.
func (s *AlertState) reset() {
	*s = AlertState{Fires: s.Fires}
}

// fire records an entry into PhaseFiring and prunes the history to the window.
func (s *AlertState) fire(now time.Time, d Dwell) {
	s.Phase = PhaseFiring
	cutoff := now.Add(-d.FlapWindow)
	kept := 0
	for _, t := range s.Fires {
		if t.After(cutoff) {
			s.Fires[kept] = t
			kept++
		}
	}
	s.Fires = append(s.Fires[:kept], now)
}

// Recurrences counts §8.2's Firing→OK→Firing transitions inside FlapWindow:
// the firings in the window, less the one that started the sequence.
func (s *AlertState) Recurrences(now time.Time, d Dwell) int {
	cutoff := now.Add(-d.normalize().FlapWindow)
	var n int
	for _, t := range s.Fires {
		if t.After(cutoff) {
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return n - 1
}

// Flapping reports §8.2's flap condition: more than FlapCount recurrences
// inside FlapWindow.
//
// It is computed from the history at read time rather than latched, because a
// subject that has settled should stop being described as flapping without
// needing an event to tell it so, and a settled subject is by construction not
// generating events.
func (s *AlertState) Flapping(now time.Time, d Dwell) bool {
	return s.Recurrences(now, d) > d.normalize().FlapCount
}

// DeliveryBackoff is how long §8.2's "delivery backs off exponentially" asks a
// consumer to wait before re-delivering this subject's finding.
//
// Zero until the subject is flapping, then For doubled once per recurrence
// beyond the allowance, capped at FlapWindow. Findings continue — the
// annotation and the metrics are unaffected, and a flapping subject is still
// drifting — it is the notification that slows down, which is the half a human
// experiences as noise.
func (s *AlertState) DeliveryBackoff(now time.Time, d Dwell) time.Duration {
	d = d.normalize()
	excess := s.Recurrences(now, d) - d.FlapCount
	if excess <= 0 {
		return 0
	}
	backoff := d.For
	for i := 1; i < excess; i++ {
		backoff *= 2
		if backoff >= d.FlapWindow {
			return d.FlapWindow
		}
	}
	return backoff
}
