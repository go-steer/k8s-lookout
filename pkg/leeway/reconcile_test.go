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

// restart is the instant the sentinel arms after a two-hour outage: long
// enough that every dwell in DefaultDwell would have elapsed unobserved,
// which is the whole hazard §9.3 step 6 is about.
var restart = at(120)

func persisted(s AlertState) AlertRecord {
	return AlertRecord{
		Cluster:     "prod-east",
		SubjectKey:  "prod/Deployment/payments",
		TopologyKey: "topology.kubernetes.io/zone",
		AlertState:  s,
		UpdatedAt:   t0,
	}
}

func TestParseAlertPhase_RoundTripsEveryPhase(t *testing.T) {
	for _, p := range []AlertPhase{PhaseOK, PhasePending, PhaseFiring, PhaseResolving} {
		got, ok := ParseAlertPhase(p.String())
		if !ok || got != p {
			t.Errorf("ParseAlertPhase(%q) = %v, %v, want %v, true", p.String(), got, ok, p)
		}
	}
	for _, s := range []string{"", "unknown", "quiescing", "OK"} {
		if got, ok := ParseAlertPhase(s); ok || got != PhaseOK {
			t.Errorf("ParseAlertPhase(%q) = %v, %v, want PhaseOK, false", s, got, ok)
		}
	}
}

// §9.3 row 1: a persisted Firing that is still drifting stays Firing, and
// `since` survives — "drifting since 09:00" must not become "drifting since
// the restart", which is the one number an operator reads off the finding.
func TestReconcile_APersistedFiringThatIsStillDriftingKeepsItsSince(t *testing.T) {
	rec := persisted(AlertState{
		Phase:       PhaseFiring,
		FirstSeenAt: at(-30),
		Since:       at(-20),
		Fires:       []time.Time{at(-20)},
	})
	got, tr := Reconcile(rec, true, restart, DefaultDwell())
	if tr != TransitionNone {
		t.Errorf("transition = %v, want none", tr)
	}
	if got.Phase != PhaseFiring {
		t.Errorf("phase = %v, want firing", got.Phase)
	}
	if !got.Since.Equal(at(-20)) || !got.FirstSeenAt.Equal(at(-30)) {
		t.Errorf("timestamps = %v/%v, want them preserved", got.FirstSeenAt, got.Since)
	}
}

// §9.3 row 2: a persisted Firing that now looks OK enters Resolving with the
// FULL resolve window. The cluster may look fine only because we just started.
func TestReconcile_APersistedFiringThatLooksOKServesTheFullResolveWindow(t *testing.T) {
	rec := persisted(AlertState{Phase: PhaseFiring, FirstSeenAt: at(-30), Since: at(-20)})
	d := DefaultDwell()

	got, tr := Reconcile(rec, false, restart, d)
	if tr != TransitionClearing || got.Phase != PhaseResolving {
		t.Fatalf("got %v/%v, want clearing into resolving", tr, got.Phase)
	}
	if !got.ClearSince.Equal(restart) {
		t.Errorf("ClearSince = %v, want the restart instant %v", got.ClearSince, restart)
	}
	// One minute short of the window: still outstanding.
	early := got.AlertState
	if tr := early.Advance(false, restart.Add(d.Resolve-time.Minute), d); tr != TransitionNone {
		t.Errorf("transition one minute early = %v, want none", tr)
	}
	onTime := got.AlertState
	if tr := onTime.Advance(false, restart.Add(d.Resolve), d); tr != TransitionResolved {
		t.Errorf("transition at the full window = %v, want resolved", tr)
	}
}

// §9.3 row 3: a persisted Pending whose dwell elapsed while we were down fires
// on the spot. The fire side honours downtime; the resolve side does not, and
// that asymmetry is the same one resolveDuration = 3× forDuration encodes —
// being wrong about "it is fixed" is the more expensive mistake.
func TestReconcile_APersistedPendingCanFireImmediately(t *testing.T) {
	rec := persisted(AlertState{Phase: PhasePending, FirstSeenAt: at(-5)})
	got, tr := Reconcile(rec, true, restart, DefaultDwell())
	if tr != TransitionFiring || got.Phase != PhaseFiring {
		t.Fatalf("got %v/%v, want a firing transition", tr, got.Phase)
	}
	if !got.FirstSeenAt.Equal(at(-5)) {
		t.Errorf("FirstSeenAt = %v, want the pre-restart %v", got.FirstSeenAt, at(-5))
	}
	if !got.Since.Equal(restart) {
		t.Errorf("Since = %v, want the restart instant", got.Since)
	}
}

// The other half of row 3: a Pending whose dwell has NOT elapsed stays Pending
// with its clock intact, so the restart neither accelerates nor restarts it.
func TestReconcile_APersistedPendingInsideItsDwellStaysPending(t *testing.T) {
	rec := persisted(AlertState{Phase: PhasePending, FirstSeenAt: restart.Add(-3 * time.Minute)})
	got, tr := Reconcile(rec, true, restart, DefaultDwell())
	if tr != TransitionNone || got.Phase != PhasePending {
		t.Fatalf("got %v/%v, want it still pending", tr, got.Phase)
	}
	if !got.FirstSeenAt.Equal(restart.Add(-3 * time.Minute)) {
		t.Errorf("FirstSeenAt = %v, want it preserved", got.FirstSeenAt)
	}
}

// §9.3 row 4: no persisted state and drifting is a new Pending dated now.
func TestReconcile_NoPersistedStateStartsAFreshEpisode(t *testing.T) {
	got, tr := Reconcile(persisted(AlertState{}), true, restart, DefaultDwell())
	if tr != TransitionPending || got.Phase != PhasePending {
		t.Fatalf("got %v/%v, want a new pending", tr, got.Phase)
	}
	if !got.FirstSeenAt.Equal(restart) {
		t.Errorf("FirstSeenAt = %v, want now", got.FirstSeenAt)
	}
}

func TestReconcile_NoPersistedStateAndNoDriftIsSilent(t *testing.T) {
	got, tr := Reconcile(persisted(AlertState{}), false, restart, DefaultDwell())
	if tr != TransitionNone || got.Phase != PhaseOK {
		t.Fatalf("got %v/%v, want nothing at all", tr, got.Phase)
	}
}

// The row §9.3's list omits, and the reason Reconcile exists rather than
// callers just calling Advance. A process down for two hours holding a
// ClearSince from before the outage would find the resolve window long elapsed
// and close the finding on one sample taken seconds after start-up.
func TestReconcile_APersistedResolvingRestartsItsWindow(t *testing.T) {
	d := DefaultDwell()
	rec := persisted(AlertState{
		Phase:       PhaseResolving,
		FirstSeenAt: at(-60),
		Since:       at(-50),
		ClearSince:  at(-5), // well over resolveDuration ago by the time we arm
	})
	if restart.Sub(at(-5)) <= d.Resolve {
		t.Fatalf("fixture is not testing anything: the persisted clear is only %v old", restart.Sub(at(-5)))
	}

	got, tr := Reconcile(rec, false, restart, d)
	if tr != TransitionNone {
		t.Fatalf("transition = %v, want none — resolving on one post-restart sample is the bug", tr)
	}
	if got.Phase != PhaseResolving {
		t.Fatalf("phase = %v, want resolving", got.Phase)
	}
	if !got.ClearSince.Equal(restart) {
		t.Errorf("ClearSince = %v, want it restarted at %v", got.ClearSince, restart)
	}
	if !got.Since.Equal(at(-50)) {
		t.Errorf("Since = %v, want the original firing instant preserved", got.Since)
	}
}

// A persisted Resolving that is drifting again goes straight back to Firing,
// which is Advance's recurrence rule; restarting the clear window first must
// not disturb it.
func TestReconcile_APersistedResolvingThatIsDriftingAgainRefires(t *testing.T) {
	rec := persisted(AlertState{
		Phase: PhaseResolving, FirstSeenAt: at(-60), Since: at(-50), ClearSince: at(-5),
	})
	got, tr := Reconcile(rec, true, restart, DefaultDwell())
	if tr != TransitionFiring || got.Phase != PhaseFiring {
		t.Fatalf("got %v/%v, want a firing transition", tr, got.Phase)
	}
	if !got.Since.Equal(at(-50)) {
		t.Errorf("Since = %v, want the original firing instant — this is not a new episode", got.Since)
	}
}

// The flap history is part of what we persist, so an episode that ends after a
// restart still counts towards flapping.
func TestReconcile_TheFlapHistorySurvivesTheRestart(t *testing.T) {
	d := Dwell{For: time.Minute, Resolve: time.Minute, FlapCount: 1, FlapWindow: 24 * time.Hour}
	rec := persisted(AlertState{
		Phase:       PhasePending,
		FirstSeenAt: at(-5),
		Fires:       []time.Time{at(-90), at(-60)},
	})
	got, tr := Reconcile(rec, true, restart, d)
	if tr != TransitionFiring {
		t.Fatalf("transition = %v, want firing", tr)
	}
	if n := got.Recurrences(restart, d); n != 2 {
		t.Errorf("Recurrences = %d, want 2 — three firings in the window less the first", n)
	}
	if !got.Flapping(restart, d) {
		t.Error("the subject is not flapping, but two recurrences beat a FlapCount of 1")
	}
}

// The record's identity comes back untouched: Reconcile judges one subject,
// it does not re-key it.
func TestReconcile_TheIdentityIsCarriedThrough(t *testing.T) {
	rec := persisted(AlertState{Phase: PhaseFiring, FirstSeenAt: at(-30), Since: at(-20)})
	got, _ := Reconcile(rec, true, restart, DefaultDwell())
	if got.Cluster != rec.Cluster || got.SubjectKey != rec.SubjectKey || got.TopologyKey != rec.TopologyKey {
		t.Errorf("identity = %+v, want %+v", got, rec)
	}
	if !got.UpdatedAt.Equal(rec.UpdatedAt) {
		t.Errorf("UpdatedAt = %v, want the persisted %v — the store stamps it, not this", got.UpdatedAt, rec.UpdatedAt)
	}
}

// A phase this binary has no transitions for — a store written by a newer
// build. Forgetting the episode costs one dwell; guessing at the semantics of
// a state we do not implement could page somebody.
func TestReconcile_AnUnreadablePhaseIsForgotten(t *testing.T) {
	rec := persisted(AlertState{Phase: AlertPhase(9), FirstSeenAt: at(-90), Since: at(-80)})
	got, tr := Reconcile(rec, true, restart, DefaultDwell())
	if tr != TransitionPending || got.Phase != PhasePending {
		t.Fatalf("got %v/%v, want a fresh pending", tr, got.Phase)
	}
	if !got.FirstSeenAt.Equal(restart) {
		t.Errorf("FirstSeenAt = %v, want now — the old episode is gone", got.FirstSeenAt)
	}
}

// time.Time{} is the year 1, not a neutral default. A truncated write that
// left a Pending with no FirstSeenAt would fire on the spot.
func TestReconcile_ATruncatedRecordDoesNotFireOnTheYearOne(t *testing.T) {
	got, tr := Reconcile(persisted(AlertState{Phase: PhasePending}), true, restart, DefaultDwell())
	if tr != TransitionNone || got.Phase != PhasePending {
		t.Fatalf("got %v/%v, want it to stay pending and serve a real dwell", tr, got.Phase)
	}
	if !got.FirstSeenAt.Equal(restart) {
		t.Errorf("FirstSeenAt = %v, want now", got.FirstSeenAt)
	}
}

// The mirror: a Resolving with no ClearSince would resolve on the spot. The
// clear-window restart covers this one too, so it is asserted on a Firing
// record with no Since instead, which repair fills and the machine then keeps.
func TestReconcile_ATruncatedFiringRecordGetsATimestamp(t *testing.T) {
	got, tr := Reconcile(persisted(AlertState{Phase: PhaseFiring}), true, restart, DefaultDwell())
	if tr != TransitionNone || got.Phase != PhaseFiring {
		t.Fatalf("got %v/%v, want it to stay firing", tr, got.Phase)
	}
	if got.Since.IsZero() || got.FirstSeenAt.IsZero() {
		t.Errorf("timestamps = %v/%v, want both filled in", got.FirstSeenAt, got.Since)
	}
}

// A clock that moved backwards — a VM restore, an NTP step. Left alone, a
// future FirstSeenAt makes now.Sub negative and the subject never leaves
// Pending: stuck, silently, for as long as the skew lasts.
func TestReconcile_AFutureTimestampIsClampedRatherThanWedgingTheSubject(t *testing.T) {
	d := DefaultDwell()
	rec := persisted(AlertState{Phase: PhasePending, FirstSeenAt: restart.Add(time.Hour)})
	got, tr := Reconcile(rec, true, restart, d)
	if tr != TransitionNone {
		t.Fatalf("transition = %v, want none yet", tr)
	}
	if !got.FirstSeenAt.Equal(restart) {
		t.Fatalf("FirstSeenAt = %v, want it clamped to now", got.FirstSeenAt)
	}
	// And it now serves an ordinary dwell rather than never firing again.
	next := got.AlertState
	if tr := next.Advance(true, restart.Add(d.For), d); tr != TransitionFiring {
		t.Errorf("transition after a full dwell = %v, want firing", tr)
	}
}

// Future firings would inflate the flap count for as long as the skew lasts.
func TestReconcile_FutureFlapEntriesAreClampedToo(t *testing.T) {
	d := Dwell{For: time.Minute, Resolve: time.Minute, FlapCount: 3, FlapWindow: time.Hour}
	rec := persisted(AlertState{
		Phase:       PhaseFiring,
		FirstSeenAt: at(-5),
		Since:       at(-5),
		Fires:       []time.Time{restart.Add(time.Hour), restart.Add(2 * time.Hour)},
	})
	got, _ := Reconcile(rec, true, restart, d)
	for i, f := range got.Fires {
		if f.After(restart) {
			t.Errorf("Fires[%d] = %v, want it clamped to %v", i, f, restart)
		}
	}
}

// Reconcile is one observation like any other: calling it twice with the same
// inputs must not advance the machine twice.
func TestReconcile_IsIdempotentOnAnUnchangedSubject(t *testing.T) {
	rec := persisted(AlertState{Phase: PhaseFiring, FirstSeenAt: at(-30), Since: at(-20)})
	first, _ := Reconcile(rec, true, restart, DefaultDwell())
	second, tr := Reconcile(first, true, restart, DefaultDwell())
	if tr != TransitionNone {
		t.Errorf("second transition = %v, want none", tr)
	}
	if second.Phase != first.Phase || !second.Since.Equal(first.Since) {
		t.Errorf("second = %v/%v, want the first's %v/%v", second.Phase, second.Since, first.Phase, first.Since)
	}
}
