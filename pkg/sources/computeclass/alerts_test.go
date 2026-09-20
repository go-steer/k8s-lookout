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
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

var testAxis = leeway.AxisKey{Provider: leeway.ProviderGKEComputeClass, Name: "n4-preferred"}

// episodeKey is the rule half of an alertKey, taken from the verdict rather
// than spelled out: the episode identity is the verdict's to define, and a test
// that hard-coded it would keep passing after the two diverged.
func episodeKey(rule leeway.RankRule, focus leeway.Rank) string {
	return leeway.RankVerdict{Rule: rule, Focus: focus}.EpisodeKey()
}

// judgement builds a one-verdict pass for the named rule.
func judgement(axis leeway.AxisKey, rule leeway.RankRule, breached bool) rankJudgement {
	return rankJudgement{
		axis: axis,
		verdicts: []leeway.RankVerdict{{
			Rule: rule,
			// RankUnknown, not the zero Rank: an unfocused verdict is what every
			// rule but tier-unused produces, and rank 0 is a real tier whose
			// episode key is a different string.
			Focus:    leeway.RankUnknown,
			Kind:     rule.Kind(),
			Tier:     rule.Tier(),
			Breached: breached,
		}},
	}
}

func TestStateKey_RoundTrips(t *testing.T) {
	k := alertKey{Axis: testAxis, Rule: episodeKey(leeway.RankRuleLastRank, leeway.RankUnknown)}

	got, ok := parseStateKey(k.subjectKey(), k.stateKey())
	if !ok {
		t.Fatalf("parseStateKey(%q, %q) declined its own output", k.subjectKey(), k.stateKey())
	}
	if got != k {
		t.Errorf("round trip = %+v, want %+v", got, k)
	}
}

// TestParseStateKey_DeclinesAnotherSourcesRow is the guard that keeps two
// sources out of one shared table.
//
// topologydrift persists a workload subject and a topology key in the same two
// columns. Both parse as strings; only the subject KIND tells them apart, and
// claiming one of those rows would reconcile it against verdicts that never
// come and then delete it as unclaimed.
func TestParseStateKey_DeclinesAnotherSourcesRow(t *testing.T) {
	cases := []struct {
		name         string
		subject      string
		state        string
		whyRejecting string
	}{
		{"a Deployment subject", "Deployment/shop/api", "topology.kubernetes.io/zone", "topologydrift's row"},
		{"an unparseable subject", "not a subject ref", "gke/last_rank", "written by another build"},
		{"no provider", "PreferenceAxis/n4-preferred", "last_rank", "no separator"},
		{"an empty provider", "PreferenceAxis/n4-preferred", "/last_rank", "provider half missing"},
		{"an empty rule", "PreferenceAxis/n4-preferred", "gke-compute-class/", "rule half missing"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := parseStateKey(tc.subject, tc.state); ok {
				t.Errorf("claimed a row that is %s", tc.whyRejecting)
			}
		})
	}
}

func TestAlertsLoad_HoldsOnlyItsOwnRows(t *testing.T) {
	a := newRankAlerts(leeway.DefaultDwell())
	mine := alertKey{Axis: testAxis, Rule: episodeKey(leeway.RankRuleLastRank, leeway.RankUnknown)}

	n := a.Load([]leeway.AlertRecord{
		{SubjectKey: mine.subjectKey(), TopologyKey: mine.stateKey()},
		{SubjectKey: "Deployment/shop/api", TopologyKey: "topology.kubernetes.io/zone"},
		{SubjectKey: "garbage", TopologyKey: "garbage"},
	}, t0)
	if n != 1 {
		t.Fatalf("Load claimed %d records, want only the PreferenceAxis one", n)
	}
	if got := a.PendingLen(); got != 1 {
		t.Errorf("PendingLen = %d, want 1", got)
	}
}

// TestPass_DwellsBeforeItFiresAndAgainBeforeItResolves walks one episode end to
// end through the §8.2 machine this source reuses unchanged.
func TestPass_DwellsBeforeItFiresAndAgainBeforeItResolves(t *testing.T) {
	d := leeway.DefaultDwell()
	a := newRankAlerts(d)
	breaching := []rankJudgement{judgement(testAxis, leeway.RankRuleLastRank, true)}
	quiet := []rankJudgement{judgement(testAxis, leeway.RankRuleLastRank, false)}

	out := a.pass(breaching, t0, time.Hour)
	if len(out) != 1 || out[0].State.Phase != leeway.PhasePending {
		t.Fatalf("first breach = %+v, want one Pending episode", out)
	}
	if got := a.Len(); got != 1 {
		t.Fatalf("Len = %d, want the open episode", got)
	}

	// Still inside the For dwell: nothing moves, so nothing is reported.
	if out := a.pass(breaching, t0.Add(d.For/2), time.Hour); len(out) != 0 {
		t.Fatalf("mid-dwell pass reported %+v, want nothing", out)
	}

	out = a.pass(breaching, t0.Add(d.For+time.Second), time.Hour)
	if len(out) != 1 || out[0].Transition != leeway.TransitionFiring {
		t.Fatalf("after the dwell = %+v, want a Firing transition", out)
	}
	if out[0].Verdict == nil || out[0].Verdict.Rule != leeway.RankRuleLastRank {
		t.Errorf("the firing outcome did not carry the reading that produced it: %+v", out[0])
	}

	// Clean readings start the resolve dwell; the episode stays open through it
	// because the finding is still outstanding.
	now := t0.Add(d.For + time.Minute)
	a.pass(quiet, now, time.Hour)
	if got := a.Len(); got != 1 {
		t.Fatalf("Len = %d during the resolve dwell, want the episode still open", got)
	}

	out = a.pass(quiet, now.Add(d.Resolve+time.Second), time.Hour)
	if len(out) != 1 || !out[0].Gone {
		t.Fatalf("after the resolve dwell = %+v, want the episode gone", out)
	}
	if got := a.Len(); got != 0 {
		t.Errorf("Len = %d after resolution, want 0", got)
	}
}

// TestPass_KeepsOneEpisodePerRule. A class can be running almost entirely on
// its last rank AND have a tier nobody has touched in a month; resolving the
// first must not close the second.
func TestPass_KeepsOneEpisodePerRule(t *testing.T) {
	a := newRankAlerts(leeway.DefaultDwell())
	both := []rankJudgement{{
		axis: testAxis,
		verdicts: []leeway.RankVerdict{
			{Rule: leeway.RankRuleLastRank, Focus: leeway.RankUnknown, Breached: true},
			{Rule: leeway.RankRuleTierUnused, Focus: 1, Breached: true},
		},
	}}

	a.pass(both, t0, time.Hour)
	if got := a.Len(); got != 2 {
		t.Fatalf("Len = %d, want one episode per rule", got)
	}

	// The last-rank breach clears; the dead tier does not.
	half := []rankJudgement{{
		axis: testAxis,
		verdicts: []leeway.RankVerdict{
			{Rule: leeway.RankRuleLastRank, Focus: leeway.RankUnknown, Breached: false},
			{Rule: leeway.RankRuleTierUnused, Focus: 1, Breached: true},
		},
	}}
	d := leeway.DefaultDwell()
	a.pass(half, t0.Add(time.Minute), time.Hour)
	a.pass(half, t0.Add(time.Minute+d.Resolve+time.Second), time.Hour)

	if got := a.Len(); got != 1 {
		t.Fatalf("Len = %d, want the unused-tier episode still open", got)
	}
	if !a.open(alertKey{Axis: testAxis, Rule: episodeKey(leeway.RankRuleTierUnused, 1)}) {
		t.Errorf("the surviving episode is not the unused tier: %+v", a.byKey)
	}
}

// TestPass_DropsAnEpisodeNobodyIsJudgingAnymore. Absence from a pass is
// information: the class was deleted, or re-tiered so the rule no longer
// applies. Either way the dwell has nothing left to measure.
func TestPass_DropsAnEpisodeNobodyIsJudgingAnymore(t *testing.T) {
	a := newRankAlerts(leeway.DefaultDwell())
	a.pass([]rankJudgement{judgement(testAxis, leeway.RankRuleWedged, true)}, t0, time.Hour)

	out := a.pass(nil, t0.Add(time.Minute), time.Hour)
	if len(out) != 1 || !out[0].Gone {
		t.Fatalf("out = %+v, want the orphaned episode gone", out)
	}
	if out[0].Verdict != nil {
		t.Errorf("an episode dropped for absence carried a verdict: %+v", out[0].Verdict)
	}
	if got := a.Len(); got != 0 {
		t.Errorf("Len = %d, want 0", got)
	}
}

// TestPass_ReconcilesAPersistedEpisodeAgainstItsFirstVerdict — §9.3 step 6. The
// record cannot be reconciled at load because there are no verdicts at load.
func TestPass_ReconcilesAPersistedEpisodeAgainstItsFirstVerdict(t *testing.T) {
	d := leeway.DefaultDwell()
	k := alertKey{Axis: testAxis, Rule: episodeKey(leeway.RankRuleLastRank, leeway.RankUnknown)}

	t.Run("still breaching", func(t *testing.T) {
		a := newRankAlerts(d)
		a.Load([]leeway.AlertRecord{{
			SubjectKey:  k.subjectKey(),
			TopologyKey: k.stateKey(),
			AlertState:  leeway.AlertState{Phase: leeway.PhaseFiring, Since: t0.Add(-time.Hour)},
		}}, t0)

		out := a.pass([]rankJudgement{judgement(testAxis, leeway.RankRuleLastRank, true)}, t0, time.Hour)
		if len(out) != 1 || out[0].State.Phase != leeway.PhaseFiring {
			t.Fatalf("out = %+v, want the restored episode still firing", out)
		}
		if got := a.PendingLen(); got != 0 {
			t.Errorf("PendingLen = %d, want the record claimed", got)
		}
		if got := a.Len(); got != 1 {
			t.Errorf("Len = %d, want the restored episode live", got)
		}
	})

	t.Run("recovered while we were down", func(t *testing.T) {
		a := newRankAlerts(d)
		a.Load([]leeway.AlertRecord{{
			SubjectKey:  k.subjectKey(),
			TopologyKey: k.stateKey(),
			AlertState:  leeway.AlertState{Phase: leeway.PhaseFiring, Since: t0.Add(-time.Hour)},
		}}, t0)

		// The resolve window restarts from now rather than being credited with
		// the downtime: we did not observe the condition ending, we observed it
		// absent once. So the episode is Resolving, not gone.
		out := a.pass([]rankJudgement{judgement(testAxis, leeway.RankRuleLastRank, false)}, t0, time.Hour)
		if len(out) != 1 || out[0].State.Phase != leeway.PhaseResolving {
			t.Fatalf("out = %+v, want the restored episode resolving", out)
		}

		out = a.pass([]rankJudgement{judgement(testAxis, leeway.RankRuleLastRank, false)}, t0.Add(d.Resolve+time.Second), time.Hour)
		if len(out) != 1 || !out[0].Gone {
			t.Fatalf("out = %+v, want the episode closed out after the resolve dwell", out)
		}
	})

	t.Run("a record this build cannot read", func(t *testing.T) {
		a := newRankAlerts(d)
		a.Load([]leeway.AlertRecord{{
			SubjectKey:  k.subjectKey(),
			TopologyKey: k.stateKey(),
			// A phase a newer build invented. §9.2 prefers losing the history to
			// acting on a state we have no rule for, so the record is dropped and
			// the store owes it a delete.
			AlertState: leeway.AlertState{Phase: leeway.PhaseResolving + 1},
		}}, t0)

		out := a.pass([]rankJudgement{judgement(testAxis, leeway.RankRuleLastRank, false)}, t0, time.Hour)
		if len(out) != 1 || !out[0].Gone {
			t.Fatalf("out = %+v, want the unreadable record dropped", out)
		}
		if got := a.Len(); got != 0 {
			t.Errorf("Len = %d, want nothing restored from an unreadable record", got)
		}
	})
}

// TestPass_DiscardsRecordsNobodyClaimedOnceTheGraceElapses. Their classes did
// not come back, and a dwell timer held open for something the cluster has
// forgotten is the phantom episode §9.3 is written to avoid.
func TestPass_DiscardsRecordsNobodyClaimedOnceTheGraceElapses(t *testing.T) {
	a := newRankAlerts(leeway.DefaultDwell())
	k := alertKey{Axis: testAxis, Rule: episodeKey(leeway.RankRuleLastRank, leeway.RankUnknown)}
	a.Load([]leeway.AlertRecord{{SubjectKey: k.subjectKey(), TopologyKey: k.stateKey()}}, t0)

	if out := a.pass(nil, t0.Add(time.Minute), 15*time.Minute); len(out) != 0 {
		t.Fatalf("out = %+v inside the grace period, want nothing", out)
	}
	if got := a.PendingLen(); got != 1 {
		t.Fatalf("PendingLen = %d inside the grace period, want the record still held", got)
	}

	out := a.pass(nil, t0.Add(20*time.Minute), 15*time.Minute)
	if len(out) != 1 || !out[0].Gone || out[0].Key != k {
		t.Fatalf("out = %+v, want a delete for the unclaimed record", out)
	}
	if got := a.PendingLen(); got != 0 {
		t.Errorf("PendingLen = %d, want the records released", got)
	}
}

func TestOpen_ReportsAbsenceRatherThanAZeroMachine(t *testing.T) {
	a := newRankAlerts(leeway.DefaultDwell())
	if a.open(alertKey{Axis: testAxis, Rule: episodeKey(leeway.RankRuleLastRank, leeway.RankUnknown)}) {
		t.Error("open claimed an episode that was never opened")
	}
}
