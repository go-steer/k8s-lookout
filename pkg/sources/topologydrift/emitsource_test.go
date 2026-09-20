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
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// breachingSource is a source holding one scored, breaching subject-axis and a
// dwell short enough that two passes cross it, with everything it emits
// collected.
//
// The alert pass reads the wall clock (the machine's own tests own the dwell
// arithmetic), so the dwell is made vanishingly short rather than the clock
// being faked here: what these tests are about is what comes out, not when.
func breachingSource(t *testing.T, tier leeway.Tier, tierCSignals bool) (*Source, *[]sources.Signal) {
	t.Helper()
	cfg := Config{
		TopologyKeys: []leeway.TopologyKey{zoneKey},
		Dwell:        leeway.Dwell{For: time.Nanosecond, Resolve: time.Hour},
		TierCSignals: tierCSignals,
	}
	s := New(fake.NewSimpleClientset(), cfg)
	s.logf = func(string, ...any) {}

	s.state.OnNodeUpsert(node("n-a", "us-central1-a"))
	s.state.OnNodeUpsert(node("n-b", "us-central1-b"))
	// The subject has to be tracked before it can hold an evaluation, so it
	// gets the pods its scores describe.
	for _, name := range []string{"db-1", "db-2", "db-3", "db-4"} {
		s.state.OnPodAdd(pod(name, "prod", "n-a", ownedBy("StatefulSet", "db")))
	}

	eval := Evaluation{
		Key:      zoneKey,
		Eligible: evenlyEligible("us-central1-a", "us-central1-b"),
		Scores: leeway.Scores{
			Domains:    []leeway.Domain{"us-central1-a", "us-central1-b"},
			Actual:     []int64{4, 0},
			Expected:   []int64{2, 2},
			Drift:      0.5,
			Relocation: 2,
		},
		Verdict: leeway.Verdict{Breached: true, Tier: tier, Severity: severityOf(tier)},
	}
	s.state.SetEvaluations(dbSubject, []Evaluation{eval})

	var got []sources.Signal
	s.emit = func(sig sources.Signal) { got = append(got, sig) }
	return s, &got
}

// dbSubject is a StatefulSet, whose pods name their subject directly and so
// need no owner lookup through an informer this test does not start.
var dbSubject = leeway.SubjectRef{Kind: leeway.SubjectStatefulSet, Namespace: "prod", Name: "db"}

func severityOf(tier leeway.Tier) string {
	if tier == leeway.TierA {
		return "critical"
	}
	return "warning"
}

// fire runs the two passes an episode needs to cross the dwell.
func fire(t *testing.T, s *Source) {
	t.Helper()
	ctx := context.Background()
	s.runAlertPass(ctx)
	s.runAlertPass(ctx)
}

func TestSource_AFiringEpisodeReachesTheWire(t *testing.T) {
	s, got := breachingSource(t, leeway.TierB, false)
	fire(t, s)

	if len(*got) != 1 {
		t.Fatalf("emitted %d signals, want exactly one — the episode fires once, not once per pass: %+v", len(*got), *got)
	}
	sig := (*got)[0]
	if sig.Kind != leeway.KindPlacementDrift {
		t.Errorf("Kind = %q, want %q for a Tier B subject with no intent", sig.Kind, leeway.KindPlacementDrift)
	}
	if sig.Key.UID != findingUID(dbSubject, zoneKey) {
		t.Errorf("UID = %q, want the subject-axis key", sig.Key.UID)
	}
	if !strings.Contains(sig.Message, "us-central1-a 4/2") {
		t.Errorf("the message does not carry the domain table:\n%s", sig.Message)
	}

	// A third pass on the same still-breaching verdict must not re-emit: the
	// episode is already firing and §8.2 reports no transition.
	s.runAlertPass(context.Background())
	if len(*got) != 1 {
		t.Errorf("a firing episode re-emitted on the next pass: %+v", *got)
	}
}

func TestSource_ATierCFindingIsMetricsOnly(t *testing.T) {
	s, got := breachingSource(t, leeway.TierC, false)
	fire(t, s)
	if len(*got) != 0 {
		t.Errorf("a Tier C finding reached the wire without the opt-in: %+v", *got)
	}
	// It still ran through the machine, which is what makes the opt-in a
	// delivery decision and not a scoring one.
	if _, live := s.alerts.StateOf(dbSubject, zoneKey); !live {
		t.Error("the Tier C episode was not tracked at all")
	}
}

func TestSource_TierCSignalsOptsTierCIn(t *testing.T) {
	s, got := breachingSource(t, leeway.TierC, true)
	fire(t, s)
	if len(*got) != 1 {
		t.Fatalf("emitted %d signals with --topology-tier-c-signals on, want 1", len(*got))
	}
	// Still placement_drift, not baseline_breach: that kind needs an intent
	// from a learned baseline, and this evaluation carries none — it is Tier C
	// the original way, scored against an even apportionment. The opt-in
	// changes who is told, not what the finding is.
	if kind := (*got)[0].Kind; kind != leeway.KindPlacementDrift {
		t.Errorf("Kind = %q, want %q", kind, leeway.KindPlacementDrift)
	}
}

func TestSource_ASuppressedSubjectIsNeverEmitted(t *testing.T) {
	// §7.6: the verdict is not breached, so the machine never opens an episode
	// — the suppression is upstream of emission, and this asserts the two
	// layers agree rather than both having to be right independently.
	s, got := breachingSource(t, leeway.TierB, false)
	evals := s.state.EvaluationsOf(dbSubject)
	suppressed := evals[0]
	suppressed.Verdict = leeway.Verdict{Suppressed: true, Tier: leeway.TierB, Transient: leeway.TransientDomainOutage}
	s.state.SetEvaluations(dbSubject, []Evaluation{suppressed})

	fire(t, s)
	if len(*got) != 0 {
		t.Errorf("a suppressed subject was emitted: %+v", *got)
	}
}

func TestSource_EmitBeforeRunIsANoOp(t *testing.T) {
	// The callback is only set inside Run. A pass that somehow ran outside it
	// must drop the finding rather than panic on a nil func.
	s, _ := breachingSource(t, leeway.TierB, false)
	s.emit = nil
	fire(t, s)
}

func TestSource_AxisOf(t *testing.T) {
	evals := []Evaluation{{Key: regionKey}, {Key: zoneKey}}
	if got := axisOf(evals, zoneKey); got == nil || got.Key != zoneKey {
		t.Errorf("axisOf picked %+v, want the zone evaluation", got)
	}
	if got := axisOf(evals, poolKey); got != nil {
		t.Errorf("axisOf on an unscored axis = %+v, want nil", got)
	}
	if got := axisOf(nil, zoneKey); got != nil {
		t.Errorf("axisOf over no evaluations = %+v, want nil", got)
	}
}
