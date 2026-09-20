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
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

var t0 = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)

// judged is one subject-axis's verdict, as Pass takes them.
func judged(key leeway.TopologyKey, breached bool) Judgement {
	return Judgement{Subject: webSubject, Key: key, Breached: breached, Tier: leeway.TierB}
}

// only asserts that a pass produced exactly one outcome and returns it.
func only(t *testing.T, out []Outcome) Outcome {
	t.Helper()
	if len(out) != 1 {
		t.Fatalf("got %d outcomes, want 1: %+v", len(out), out)
	}
	return out[0]
}

func TestAlerts_AQuietSubjectGetsNoEntry(t *testing.T) {
	// The memory bound. Twenty thousand subjects across two axes, none of them
	// drifting, and the map stays empty — it is bounded by how much trouble a
	// cluster is in, not by how large it is.
	a := NewAlerts(leeway.DefaultDwell())
	if out := a.Pass([]Judgement{judged(zoneKey, false), judged(poolKey, false)}, t0, time.Minute); len(out) != 0 {
		t.Errorf("a clean pass produced %+v, want nothing", out)
	}
	if a.Len() != 0 {
		t.Errorf("Len = %d, want 0", a.Len())
	}
}

func TestAlerts_ABreachDwellsBeforeItFires(t *testing.T) {
	d := leeway.DefaultDwell()
	a := NewAlerts(d)

	first := only(t, a.Pass([]Judgement{judged(zoneKey, true)}, t0, time.Minute))
	if first.Transition != leeway.TransitionPending {
		t.Fatalf("first breach = %s, want pending", first.Transition)
	}
	if first.Gone {
		t.Error("a new episode reported itself gone")
	}

	// A minute short of the dwell is still nothing. The machine moved, but
	// nothing about it changed, so there is no outcome to persist.
	if out := a.Pass([]Judgement{judged(zoneKey, true)}, t0.Add(d.For-time.Minute), time.Minute); len(out) != 0 {
		t.Errorf("mid-dwell pass produced %+v, want nothing", out)
	}

	fired := only(t, a.Pass([]Judgement{judged(zoneKey, true)}, t0.Add(d.For), time.Minute))
	if fired.Transition != leeway.TransitionFiring {
		t.Fatalf("after the dwell = %s, want firing", fired.Transition)
	}
	// FirstSeenAt is the breach, not the fire: §8.5 reports "this has been
	// wrong since 12:00", and the ten minutes spent making sure are ours.
	if !fired.State.FirstSeenAt.Equal(t0) {
		t.Errorf("FirstSeenAt = %v, want the first breach at %v", fired.State.FirstSeenAt, t0)
	}
	if got, ok := a.StateOf(webSubject, zoneKey); !ok || got.Phase != leeway.PhaseFiring {
		t.Errorf("StateOf = %v/%v, want a firing episode", got.Phase, ok)
	}
}

func TestAlerts_ABreachInsideTheDwellIsAbandonedAndLeavesNoTrace(t *testing.T) {
	// The whole point of the dwell: a subject that twitches over the threshold
	// for a minute owes nobody anything, because nobody was told.
	d := leeway.DefaultDwell()
	a := NewAlerts(d)
	a.Pass([]Judgement{judged(zoneKey, true)}, t0, time.Minute)

	got := only(t, a.Pass([]Judgement{judged(zoneKey, false)}, t0.Add(time.Minute), time.Minute))
	if got.Transition != leeway.TransitionAbandoned {
		t.Errorf("transition = %s, want abandoned", got.Transition)
	}
	if !got.Gone {
		t.Error("an abandoned episode was not reported gone, so its store row would outlive it")
	}
	if a.Len() != 0 {
		t.Errorf("Len = %d after an abandoned episode, want 0", a.Len())
	}
}

func TestAlerts_AFiringSubjectResolvesThroughItsResolveDwell(t *testing.T) {
	d := leeway.DefaultDwell()
	a := NewAlerts(d)
	a.Pass([]Judgement{judged(zoneKey, true)}, t0, time.Minute)
	a.Pass([]Judgement{judged(zoneKey, true)}, t0.Add(d.For), time.Minute)

	clear := t0.Add(d.For + time.Minute)
	clearing := only(t, a.Pass([]Judgement{judged(zoneKey, false)}, clear, time.Minute))
	if clearing.Transition != leeway.TransitionClearing {
		t.Fatalf("transition = %s, want clearing", clearing.Transition)
	}
	if clearing.Gone {
		t.Error("a clearing episode is still outstanding and must not be deleted")
	}

	resolved := only(t, a.Pass([]Judgement{judged(zoneKey, false)}, clear.Add(d.Resolve), time.Minute))
	if resolved.Transition != leeway.TransitionResolved {
		t.Fatalf("transition = %s, want resolved", resolved.Transition)
	}
	if !resolved.Gone {
		t.Error("a resolved episode was not reported gone")
	}
}

func TestAlerts_EachAxisIsItsOwnEpisode(t *testing.T) {
	// §9.1's reason for keying on the pair: a Deployment can be perfectly
	// spread across zones and stacked three deep on one node.
	d := leeway.DefaultDwell()
	a := NewAlerts(d)
	a.Pass([]Judgement{judged(zoneKey, false), judged(poolKey, true)}, t0, time.Minute)

	if _, ok := a.StateOf(webSubject, zoneKey); ok {
		t.Error("the clean axis got an episode")
	}
	if st, ok := a.StateOf(webSubject, poolKey); !ok || st.Phase != leeway.PhasePending {
		t.Errorf("the breaching axis = %v/%v, want pending", st.Phase, ok)
	}
}

func TestAlerts_AnEpisodeWhoseSubjectVanishesIsDropped(t *testing.T) {
	// The only eviction rule this map has. A subject deleted mid-episode stops
	// appearing in the judgement set, and leaving its entry behind would dwell
	// forever against a cluster that has forgotten it.
	a := NewAlerts(leeway.DefaultDwell())
	a.Pass([]Judgement{judged(zoneKey, true)}, t0, time.Minute)

	got := only(t, a.Pass(nil, t0.Add(time.Minute), time.Minute))
	if !got.Gone {
		t.Error("a vanished subject's episode was kept")
	}
	if got.Transition != leeway.TransitionNone {
		t.Errorf("transition = %s, want none: the subject did not resolve, it disappeared", got.Transition)
	}
	if a.Len() != 0 {
		t.Errorf("Len = %d, want 0", a.Len())
	}
}

func TestAlerts_TheTierFollowsTheLatestVerdict(t *testing.T) {
	// The tier is not persisted and is not part of the machine: it is
	// recomputed from the cluster every evaluation, and an episode that opened
	// as an inferred Tier B and later acquired a declared constraint must
	// report the constraint.
	a := NewAlerts(leeway.DefaultDwell())
	a.Pass([]Judgement{judged(zoneKey, true)}, t0, time.Minute)
	a.Pass([]Judgement{{Subject: webSubject, Key: zoneKey, Breached: true, Tier: leeway.TierA}}, t0.Add(time.Minute), time.Minute)

	var tiers []leeway.Tier
	a.Each(func(_ leeway.SubjectRef, _ leeway.TopologyKey, _ leeway.AlertState, tier leeway.Tier) {
		tiers = append(tiers, tier)
	})
	if len(tiers) != 1 || tiers[0] != leeway.TierA {
		t.Errorf("tiers = %v, want [A]", tiers)
	}
}

// persisted builds one stored record for webSubject on the zone axis.
func persisted(phase leeway.AlertPhase, firstSeen time.Time) leeway.AlertRecord {
	return leeway.AlertRecord{
		Cluster:     "prod",
		SubjectKey:  webSubject.String(),
		TopologyKey: string(zoneKey),
		AlertState:  leeway.AlertState{Phase: phase, FirstSeenAt: firstSeen},
	}
}

func TestAlerts_ARestartDoesNotRestartTheDwell(t *testing.T) {
	// §9.1's one-sentence requirement: a thirty-minute dwell timer must not
	// restart its clock because the process did. The subject went Pending
	// nine minutes before the restart and is still breaching, so it fires a
	// minute later rather than eleven.
	d := leeway.DefaultDwell()
	a := NewAlerts(d)
	a.Load([]leeway.AlertRecord{persisted(leeway.PhasePending, t0)}, t0.Add(9*time.Minute))

	got := only(t, a.Pass([]Judgement{judged(zoneKey, true)}, t0.Add(d.For), time.Minute))
	if got.Transition != leeway.TransitionFiring {
		t.Errorf("transition = %s, want firing: the dwell elapsed across the restart", got.Transition)
	}
	if a.PendingLen() != 0 {
		t.Errorf("PendingLen = %d, want the record consumed", a.PendingLen())
	}
}

func TestAlerts_ARestartIntoAClearedFiringEpisodeStillOwesAResolve(t *testing.T) {
	d := leeway.DefaultDwell()
	a := NewAlerts(d)
	a.Load([]leeway.AlertRecord{persisted(leeway.PhaseFiring, t0)}, t0.Add(time.Hour))

	// Firing, now clear: this is a clearing, not a resolve — the finding is
	// still outstanding and owes its resolve dwell. Restoring it and then
	// silently dropping it would leave whoever received the finding waiting.
	got := only(t, a.Pass([]Judgement{judged(zoneKey, false)}, t0.Add(time.Hour), time.Minute))
	if got.Transition != leeway.TransitionClearing {
		t.Fatalf("transition = %s, want clearing", got.Transition)
	}
	if got.Gone {
		t.Error("an outstanding finding was deleted on restart")
	}
	if st, ok := a.StateOf(webSubject, zoneKey); !ok || st.Phase != leeway.PhaseResolving {
		t.Errorf("phase = %v/%v, want resolving", st.Phase, ok)
	}
}

func TestAlerts_ARecordThisBuildCannotReadIsDropped(t *testing.T) {
	// §9.2: an unreadable history costs one dwell, not the monitoring. Neither
	// a key from another build's encoding nor a phase this binary has no rule
	// for may reach the machine.
	a := NewAlerts(leeway.DefaultDwell())
	bad := persisted(leeway.PhasePending, t0)
	bad.SubjectKey = "not-a-subject-key/with/too/many/parts"
	if n := a.Load([]leeway.AlertRecord{bad}, t0); n != 0 {
		t.Errorf("Load accepted %d records, want 0", n)
	}

	// And a row belonging to the compute-class source, which shares this table.
	// It parses perfectly — PreferenceAxis is in the same subject vocabulary —
	// so only the kind tells them apart, and claiming it would reconcile it
	// against verdicts that never come and then DELETE it as unclaimed.
	other := persisted(leeway.PhaseFiring, t0)
	other.SubjectKey = leeway.SubjectRef{Kind: leeway.SubjectPreferenceAxis, Name: "n4-preferred"}.String()
	other.TopologyKey = "gke-computeclass/last-rank"
	a = NewAlerts(leeway.DefaultDwell())
	if n := a.Load([]leeway.AlertRecord{other}, t0); n != 0 {
		t.Errorf("Load claimed %d of another source's rows, want 0", n)
	}

	// A phase from a newer build does reach the machine — the key parsed — and
	// leeway.Reconcile's repair turns it into a clean start rather than a
	// guess at semantics this binary does not have.
	future := persisted(leeway.AlertPhase(200), t0)
	a = NewAlerts(leeway.DefaultDwell())
	a.Load([]leeway.AlertRecord{future}, t0)

	got := only(t, a.Pass([]Judgement{judged(zoneKey, true)}, t0.Add(time.Minute), time.Minute))
	if got.Transition != leeway.TransitionPending {
		t.Errorf("transition = %s, want a fresh pending", got.Transition)
	}
	if !got.State.FirstSeenAt.Equal(t0.Add(time.Minute)) {
		t.Errorf("FirstSeenAt = %v, want the dwell restarted at %v", got.State.FirstSeenAt, t0.Add(time.Minute))
	}
}

func TestAlerts_AnUnclaimedRecordIsDroppedAfterTheGrace(t *testing.T) {
	// The subject was deleted while we were down. Holding its dwell timer open
	// is the phantom episode §9.3's repair rules exist to prevent — but only
	// after the grace, because until then it is indistinguishable from a
	// subject the work queue has not reached yet.
	a := NewAlerts(leeway.DefaultDwell())
	a.Load([]leeway.AlertRecord{persisted(leeway.PhaseFiring, t0)}, t0)

	if out := a.Pass(nil, t0.Add(time.Minute), 5*time.Minute); len(out) != 0 {
		t.Fatalf("dropped a record inside the grace: %+v", out)
	}
	if a.PendingLen() != 1 {
		t.Fatalf("PendingLen = %d inside the grace, want 1", a.PendingLen())
	}

	got := only(t, a.Pass(nil, t0.Add(6*time.Minute), 5*time.Minute))
	if !got.Gone {
		t.Error("an unclaimed record was not reported gone, so its store row would be immortal")
	}
	if a.PendingLen() != 0 {
		t.Errorf("PendingLen = %d after the grace, want 0", a.PendingLen())
	}
}

func TestAlerts_APersistedEpisodeThatWentAwayIsEnded(t *testing.T) {
	d := leeway.DefaultDwell()
	a := NewAlerts(d)
	a.Load([]leeway.AlertRecord{persisted(leeway.PhasePending, t0)}, t0.Add(time.Hour))

	got := only(t, a.Pass([]Judgement{judged(zoneKey, false)}, t0.Add(time.Hour), time.Minute))
	if got.Transition != leeway.TransitionAbandoned {
		t.Errorf("transition = %s, want abandoned: it never fired, so nobody was told", got.Transition)
	}
	if !got.Gone {
		t.Error("the episode was not reported gone")
	}
	if a.Len() != 0 {
		t.Errorf("Len = %d, want 0", a.Len())
	}
}
