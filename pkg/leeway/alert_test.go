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

import (
	"testing"
	"time"
)

// t0 is an arbitrary fixed instant. Every test below states its own clock in
// offsets from it, because a state machine whose tests read "now.Add(...)"
// hides exactly the arithmetic that goes wrong.
var t0 = time.Date(2026, 9, 19, 9, 0, 0, 0, time.UTC)

func at(m int) time.Time { return t0.Add(time.Duration(m) * time.Minute) }

// drive feeds a sequence of (minute, breached) observations and returns the
// transitions, one per observation.
func drive(s *AlertState, d Dwell, obs ...struct {
	min     int
	breach  bool
	wantTrs Transition
}) []Transition {
	out := make([]Transition, 0, len(obs))
	for _, o := range obs {
		out = append(out, s.Advance(o.breach, at(o.min), d))
	}
	return out
}

type step struct {
	min     int
	breach  bool
	wantTrs Transition
}

// run drives the machine and checks each transition in place, so a failure
// names the observation that diverged rather than a slice mismatch.
func run(t *testing.T, s *AlertState, d Dwell, steps ...step) {
	t.Helper()
	for i, st := range steps {
		got := s.Advance(st.breach, at(st.min), d)
		if got != st.wantTrs {
			t.Fatalf("step %d (minute %d, breach=%v): transition = %q, want %q (phase now %q)",
				i, st.min, st.breach, got, st.wantTrs, s.Phase)
		}
	}
}

func TestAlertPhase_Strings(t *testing.T) {
	cases := map[AlertPhase]string{
		PhaseOK: "ok", PhasePending: "pending", PhaseFiring: "firing",
		PhaseResolving: "resolving", AlertPhase(9): "unknown",
	}
	for p, want := range cases {
		if got := p.String(); got != want {
			t.Errorf("AlertPhase(%d).String() = %q, want %q", p, got, want)
		}
	}
	for p, wantFiring := range map[AlertPhase]bool{
		PhaseOK: false, PhasePending: false, PhaseFiring: true, PhaseResolving: true,
	} {
		if got := p.Firing(); got != wantFiring {
			t.Errorf("AlertPhase(%v).Firing() = %v, want %v", p, got, wantFiring)
		}
	}
}

func TestTransition_StringsAndEmits(t *testing.T) {
	cases := []struct {
		tr        Transition
		want      string
		wantEmits bool
	}{
		{TransitionNone, "", false},
		{TransitionPending, "pending", false},
		{TransitionFiring, "firing", true},
		{TransitionClearing, "clearing", false},
		{TransitionResolved, "resolved", true},
		{TransitionAbandoned, "abandoned", false},
		{Transition(9), "", false},
	}
	for _, tc := range cases {
		if got := tc.tr.String(); got != tc.want {
			t.Errorf("Transition(%d).String() = %q, want %q", tc.tr, got, tc.want)
		}
		if got := tc.tr.Emits(); got != tc.wantEmits {
			t.Errorf("Transition(%d).Emits() = %v, want %v", tc.tr, got, tc.wantEmits)
		}
	}
}

// TestAlert_TheHappyPath walks §8.2's diagram once, end to end, with the
// default 10m/30m dwells.
func TestAlert_TheHappyPath(t *testing.T) {
	d := DefaultDwell()
	var s AlertState

	run(t, &s, d,
		step{0, true, TransitionPending},
		step{5, true, TransitionNone}, // inside the dwell
		step{9, true, TransitionNone},
		step{10, true, TransitionFiring}, // exactly at For: the boundary fires
		step{20, true, TransitionNone},
		step{25, false, TransitionClearing},
		step{40, false, TransitionNone}, // inside the resolve dwell
		step{54, false, TransitionNone},
		step{55, false, TransitionResolved}, // 25 + 30
	)

	if s.Phase != PhaseOK {
		t.Errorf("phase = %v, want OK", s.Phase)
	}
	if !s.FirstSeenAt.IsZero() || !s.Since.IsZero() {
		t.Errorf("timestamps survived the resolve: %+v", s)
	}
}

// TestAlert_FirstSeenAtIsThePendingEntryNotTheFiringOne: §8.5's payload reports
// firstSeenAt, and an operator reads it as "this has been wrong since". The ten
// minutes we spent making sure are ours, not theirs.
func TestAlert_FirstSeenAtIsThePendingEntryNotTheFiringOne(t *testing.T) {
	d := DefaultDwell()
	var s AlertState
	run(t, &s, d,
		step{0, true, TransitionPending},
		step{12, true, TransitionFiring},
	)
	if !s.FirstSeenAt.Equal(at(0)) {
		t.Errorf("FirstSeenAt = %v, want the Pending entry at %v", s.FirstSeenAt, at(0))
	}
	if !s.Since.Equal(at(12)) {
		t.Errorf("Since = %v, want the Firing entry at %v", s.Since, at(12))
	}
}

// TestAlert_ABreachInsideTheDwellIsAbandonedNotResolved is why Transition has
// five values rather than being inferred from a phase pair. Pending→OK and
// Resolving→OK are the same two phases and opposite obligations.
func TestAlert_ABreachInsideTheDwellIsAbandonedNotResolved(t *testing.T) {
	d := DefaultDwell()
	var s AlertState
	run(t, &s, d,
		step{0, true, TransitionPending},
		step{4, false, TransitionAbandoned},
	)
	if s.Phase != PhaseOK {
		t.Errorf("phase = %v, want OK", s.Phase)
	}
	if len(s.Fires) != 0 {
		t.Errorf("an abandoned episode recorded a firing: %v", s.Fires)
	}
	if TransitionAbandoned.Emits() {
		t.Error("abandoned must not emit: nothing was ever announced, so nothing is owed a resolution")
	}
}

// TestAlert_ARecurrenceInsideTheResolveWindowDoesNotServeASecondDwell. A
// subject oscillating either side of the threshold would otherwise spend every
// breach in Pending and never be reported at all.
func TestAlert_ARecurrenceInsideTheResolveWindowDoesNotServeASecondDwell(t *testing.T) {
	d := DefaultDwell()
	var s AlertState
	run(t, &s, d,
		step{0, true, TransitionPending},
		step{10, true, TransitionFiring},
		step{20, false, TransitionClearing},
		step{24, true, TransitionFiring}, // straight back, no dwell
	)
	if !s.Since.Equal(at(10)) {
		t.Errorf("Since = %v, want the original firing at %v — a dip is not a new episode",
			s.Since, at(10))
	}
	if !s.ClearSince.IsZero() {
		t.Errorf("ClearSince = %v, want it cleared on the recurrence", s.ClearSince)
	}
}

// TestAlert_TheResolveWindowRestartsAfterARecurrence: a subject that dips,
// recurs, and dips again owes the full resolve dwell from the second dip, not
// from the first.
func TestAlert_TheResolveWindowRestartsAfterARecurrence(t *testing.T) {
	d := DefaultDwell()
	var s AlertState
	run(t, &s, d,
		step{0, true, TransitionPending},
		step{10, true, TransitionFiring},
		step{20, false, TransitionClearing},
		step{24, true, TransitionFiring},
		step{30, false, TransitionClearing},
		step{55, false, TransitionNone},     // 30 + 30 is minute 60, not yet
		step{60, false, TransitionResolved}, // now
	)
}

// TestAlert_FlappingCountsEpisodesThroughEitherRoute. §8.2 counts
// Firing→OK→Firing, and a subject can come back two ways — through a completed
// resolve and through a recurrence inside the resolve window. Counting firings
// rather than the transitions between them is what makes one rule cover both.
func TestAlert_FlappingCountsEpisodesThroughEitherRoute(t *testing.T) {
	d := Dwell{For: time.Minute, Resolve: 2 * time.Minute, FlapCount: 3, FlapWindow: time.Hour}
	var s AlertState

	// Four firings inside the hour: two through a full resolve, two as
	// recurrences inside the resolve window.
	run(t, &s, d,
		step{0, true, TransitionPending},
		step{1, true, TransitionFiring}, // fire 1
		step{2, false, TransitionClearing},
		step{4, false, TransitionResolved},
		step{5, true, TransitionPending},
		step{6, true, TransitionFiring}, // fire 2, via OK
		step{7, false, TransitionClearing},
		step{8, true, TransitionFiring}, // fire 3, via the resolve window
		step{9, false, TransitionClearing},
		step{10, true, TransitionFiring}, // fire 4
	)

	if got := s.Recurrences(at(10), d); got != 3 {
		t.Errorf("recurrences = %d, want 3 (four firings, less the one that started it)", got)
	}
	if s.Flapping(at(10), d) {
		t.Error("3 recurrences is not MORE than flapCount 3 — the boundary must not be flapping")
	}

	run(t, &s, d,
		step{11, false, TransitionClearing},
		step{12, true, TransitionFiring}, // fire 5
	)
	if !s.Flapping(at(12), d) {
		t.Errorf("4 recurrences inside the window is flapping: %+v", s)
	}
}

// TestAlert_FlappingLapsesWithTheWindow, without anything happening to make it
// lapse. A settled subject generates no events, so a latched flag would stay
// set until the next time it misbehaved.
func TestAlert_FlappingLapsesWithTheWindow(t *testing.T) {
	d := Dwell{For: time.Minute, Resolve: time.Minute, FlapCount: 1, FlapWindow: 30 * time.Minute}
	var s AlertState
	run(t, &s, d,
		step{0, true, TransitionPending},
		step{1, true, TransitionFiring},
		step{2, false, TransitionClearing},
		step{3, true, TransitionFiring},
		step{4, false, TransitionClearing},
		step{5, true, TransitionFiring},
	)
	if !s.Flapping(at(5), d) {
		t.Fatalf("premise broken: wanted a flapping subject, got %+v", s)
	}
	// Nothing happens for an hour.
	if s.Flapping(at(65), d) {
		t.Errorf("still flapping an hour later with no events: %+v", s)
	}
	if got := s.Recurrences(at(65), d); got != 0 {
		t.Errorf("recurrences an hour later = %d, want 0", got)
	}
}

// TestAlert_TheFireHistorySurvivesAnEpisode. Flapping is by definition a
// sequence of episodes that ended, so clearing the history at the end of each
// one would mean no subject could ever be seen to flap.
func TestAlert_TheFireHistorySurvivesAnEpisode(t *testing.T) {
	d := Dwell{For: time.Minute, Resolve: time.Minute, FlapCount: 3, FlapWindow: time.Hour}
	var s AlertState
	run(t, &s, d,
		step{0, true, TransitionPending},
		step{1, true, TransitionFiring},
		step{2, false, TransitionClearing},
		step{3, false, TransitionResolved},
	)
	if len(s.Fires) != 1 {
		t.Errorf("Fires = %v, want the resolved episode's firing to survive", s.Fires)
	}
	if s.Phase != PhaseOK || !s.FirstSeenAt.IsZero() || !s.Since.IsZero() || !s.ClearSince.IsZero() {
		t.Errorf("the rest of the state did not reset: %+v", s)
	}

	// And an abandoned Pending afterwards must not erase it either.
	run(t, &s, d,
		step{4, true, TransitionPending},
		step{4, false, TransitionAbandoned},
	)
	if len(s.Fires) != 1 {
		t.Errorf("Fires = %v after an abandoned episode, want the earlier firing kept", s.Fires)
	}
}

// TestAlert_FiresArePrunedToTheWindow keeps the slice bounded: the state is
// persisted per subject-key, and an unbounded history on a permanently
// misbehaving subject is a slow leak in the store as well as in memory.
func TestAlert_FiresArePrunedToTheWindow(t *testing.T) {
	d := Dwell{For: time.Minute, Resolve: time.Minute, FlapCount: 3, FlapWindow: 10 * time.Minute}
	var s AlertState
	for i := 0; i < 20; i++ {
		base := i * 3
		run(t, &s, d,
			step{base, true, TransitionPending},
			step{base + 1, true, TransitionFiring},
			step{base + 2, false, TransitionClearing},
			step{base + 3, false, TransitionResolved},
		)
	}
	if len(s.Fires) > 5 {
		t.Errorf("Fires grew to %d entries over 20 episodes in a 10-minute window: %v", len(s.Fires), s.Fires)
	}
	// Pruning happens at fire time, so the window is measured from the last
	// firing — minute 58, the twentieth episode's.
	for _, f := range s.Fires {
		if !f.After(at(58 - 10)) {
			t.Errorf("a firing at %v survived outside the window ending at %v", f, at(58))
		}
	}
}

func TestDwell_NormalizeFillsInTheDangerousZeroes(t *testing.T) {
	got := Dwell{For: 5 * time.Minute}.normalize()
	if got.Resolve != 15*time.Minute {
		t.Errorf("Resolve = %v, want 3×For — an unset resolve dwell must not mean an instant one", got.Resolve)
	}
	if got.FlapCount != DefaultDwell().FlapCount || got.FlapWindow != DefaultDwell().FlapWindow {
		t.Errorf("flap settings = %d/%v, want the defaults", got.FlapCount, got.FlapWindow)
	}

	// A wholly zero Dwell is the defaults, not a machine with no hysteresis at
	// all: Advance normalises, so a caller who forgot to configure one gets
	// ten minutes of dwell rather than firing on the first observation.
	var s AlertState
	if tr := s.Advance(true, at(0), Dwell{}); tr != TransitionPending {
		t.Errorf("first breach under a zero Dwell = %q, want pending", tr)
	}
	if tr := s.Advance(true, at(1), Dwell{}); tr != TransitionNone {
		t.Errorf("one minute later under a zero Dwell = %q, want the dwell to hold it", tr)
	}
	if tr := s.Advance(true, at(10), Dwell{}); tr != TransitionFiring {
		t.Errorf("ten minutes later = %q, want firing", tr)
	}
}

func TestDwell_DefaultsAreTheDesignsNumbers(t *testing.T) {
	d := DefaultDwell()
	if d.For != 10*time.Minute || d.Resolve != 30*time.Minute {
		t.Errorf("dwells = %v/%v, want §10.1's 10m/30m", d.For, d.Resolve)
	}
	if d.Resolve != 3*d.For {
		t.Errorf("the defaults break §8.2's 3× rule: %v vs %v", d.Resolve, d.For)
	}
	if d.FlapCount != 3 || d.FlapWindow != time.Hour {
		t.Errorf("flap settings = %d/%v, want §8.2's 3 per hour", d.FlapCount, d.FlapWindow)
	}
}

func TestAlert_DeliveryBackoffIsZeroUntilFlapping(t *testing.T) {
	d := Dwell{For: time.Minute, Resolve: time.Minute, FlapCount: 2, FlapWindow: time.Hour}
	var s AlertState

	if got := s.DeliveryBackoff(at(0), d); got != 0 {
		t.Errorf("backoff on a fresh subject = %v, want 0", got)
	}

	fireOnce := func(base int) {
		run(t, &s, d,
			step{base, true, TransitionPending},
			step{base + 1, true, TransitionFiring},
			step{base + 2, false, TransitionClearing},
			step{base + 3, false, TransitionResolved},
		)
	}
	for i := 0; i < 3; i++ {
		fireOnce(i * 4)
	}
	// Three firings = two recurrences = exactly the allowance.
	if got := s.DeliveryBackoff(at(12), d); got != 0 {
		t.Errorf("backoff at the allowance = %v, want 0", got)
	}

	fireOnce(12)
	if got := s.DeliveryBackoff(at(16), d); got != time.Minute {
		t.Errorf("backoff one past the allowance = %v, want For", got)
	}
	fireOnce(16)
	if got := s.DeliveryBackoff(at(20), d); got != 2*time.Minute {
		t.Errorf("backoff two past = %v, want 2×For", got)
	}
	fireOnce(20)
	if got := s.DeliveryBackoff(at(24), d); got != 4*time.Minute {
		t.Errorf("backoff three past = %v, want 4×For", got)
	}
}

// TestAlert_DeliveryBackoffIsCappedAtTheFlapWindow. Exponential means
// exponential, and a subject that has been flapping all day would otherwise be
// asked to wait longer than the window over which we are still calling it a
// flapper.
func TestAlert_DeliveryBackoffIsCappedAtTheFlapWindow(t *testing.T) {
	d := Dwell{For: time.Minute, Resolve: time.Minute, FlapCount: 1, FlapWindow: 2 * time.Hour}
	s := AlertState{}
	for i := 0; i < 40; i++ {
		s.Fires = append(s.Fires, at(i))
	}
	if got := s.DeliveryBackoff(at(41), d); got != d.FlapWindow {
		t.Errorf("backoff after 40 firings = %v, want the %v cap", got, d.FlapWindow)
	}
}

// TestAlert_AnUnknownPhaseDoesNothing. The phase is persisted, so a record
// written by a future version — or a corrupted one — can arrive here. Doing
// nothing leaves the subject inert until something overwrites it, which is the
// only safe answer: the alternative readings are "fire" and "resolve", and
// both would be an announcement made on the strength of a byte we do not
// understand.
func TestAlert_AnUnknownPhaseDoesNothing(t *testing.T) {
	s := AlertState{Phase: AlertPhase(200)}
	if got := s.Advance(true, at(0), DefaultDwell()); got != TransitionNone {
		t.Errorf("transition = %q, want none", got)
	}
	if s.Phase != AlertPhase(200) {
		t.Errorf("phase = %v, want it left alone", s.Phase)
	}
}

// TestAlert_TheMachineIsDeterministicUnderRepeatedObservations. The source
// re-evaluates a subject on every pod event, so the same verdict arrives many
// times between transitions; none of those may move anything.
func TestAlert_TheMachineIsDeterministicUnderRepeatedObservations(t *testing.T) {
	d := DefaultDwell()
	var s AlertState
	_ = drive(&s, d) // no observations at all is also a no-op

	// The overwhelmingly common case: a healthy subject, evaluated forever.
	for i := 0; i < 5; i++ {
		if got := s.Advance(false, at(i), d); got != TransitionNone {
			t.Fatalf("healthy subject at minute %d = %q, want none", i, got)
		}
		if s.Phase != PhaseOK {
			t.Fatalf("healthy subject left OK: %+v", s)
		}
	}

	if got := s.Advance(true, at(0), d); got != TransitionPending {
		t.Fatalf("first breach = %q", got)
	}
	for i := 1; i < 10; i++ {
		if got := s.Advance(true, at(i), d); got != TransitionNone {
			t.Fatalf("repeat breach at minute %d = %q, want none", i, got)
		}
	}
	if got := s.Advance(true, at(10), d); got != TransitionFiring {
		t.Fatalf("at the dwell boundary = %q", got)
	}
	for i := 11; i < 30; i++ {
		if got := s.Advance(true, at(i), d); got != TransitionNone {
			t.Fatalf("repeat breach while firing at minute %d = %q, want none", i, got)
		}
	}
	if !s.Since.Equal(at(10)) {
		t.Errorf("Since moved under repeated observation: %v", s.Since)
	}
}
