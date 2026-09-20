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
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

func TestFindingUID_RoundTrips(t *testing.T) {
	k := alertKey{Axis: testAxis, Rule: episodeKey(leeway.RankRuleTierUnused, 2)}

	uid := findingUID(k)
	got, ok := parseFindingUID(uid)
	if !ok {
		t.Fatalf("parseFindingUID(%q) declined its own output", uid)
	}
	if got != k {
		t.Errorf("round trip = %+v, want %+v", got, k)
	}
}

// TestParseFindingUID_DeclinesTopologyDriftsIncidents is why the prefix is
// `leeway-rank:` and not `leeway:`.
//
// Sharing a prefix would leave both sources answering for both sets of
// incidents, and the clearance observer's answer is destructive: an incident it
// claims but cannot find in its own state is reported recovered.
func TestParseFindingUID_DeclinesTopologyDriftsIncidents(t *testing.T) {
	cases := []struct {
		name string
		uid  string
	}{
		{"a topology-drift incident", "leeway:Deployment/shop/api|topology.kubernetes.io/zone"},
		{"another source entirely", "nodegroup:pool-1"},
		{"the prefix and nothing else", uidPrefix},
		{"no rule half", uidPrefix + "PreferenceAxis/n4-preferred"},
		{"a workload subject under our own prefix", uidPrefix + "Deployment/shop/api|gke-computeclass/last-rank"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := parseFindingUID(tc.uid); ok {
				t.Errorf("claimed %q", tc.uid)
			}
		})
	}
}

// TestFindingUID_IsNotAPrefixOfTopologyDrifts, in the other direction: the two
// prefixes must not be a prefix of each other, or CutPrefix succeeds and the
// guard is left to the parse.
func TestFindingUID_IsNotAPrefixOfTopologyDrifts(t *testing.T) {
	uid := findingUID(alertKey{Axis: testAxis, Rule: "last-rank"})
	if strings.HasPrefix(uid, "leeway:") {
		t.Errorf("%q begins with topologydrift's prefix", uid)
	}
}

func TestObjectKind_ComesFromTheProvider(t *testing.T) {
	if got := objectKind(leeway.ProviderGKEComputeClass); got != "ComputeClass" {
		t.Errorf("GKE object kind = %q, want ComputeClass", got)
	}
	// A provider with no mapping falls back to the neutral name rather than to
	// GKE's: guessing a second cloud's object kind would put a wrong `kind` in
	// a payload that reads as authoritative.
	if got := objectKind(leeway.Provider("karpenter")); got != "PreferenceAxis" {
		t.Errorf("unmapped provider = %q, want the neutral name", got)
	}
}

// TestFindingFor_RoutesTierCToMetricsOnlyUntilItIsTurnedOn — §8.3. The payload
// is built either way, because it is the argument for the decision.
func TestFindingFor_RoutesTierCToMetricsOnlyUntilItIsTurnedOn(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	j := &rankJudgement{
		axis:  testAxis,
		class: s.classes["n4-preferred"],
		window: leeway.RankWindow{
			Elapsed:    time.Hour,
			PodSeconds: map[leeway.Rank]float64{0: 3600},
		},
	}
	v := leeway.RankVerdict{
		Rule:     leeway.RankRuleTierUnused,
		Kind:     leeway.KindRankTierUnused,
		Focus:    2,
		Tier:     leeway.TierC,
		Breached: true,
		Reason:   "nothing has run at rank 2 for 30d",
	}

	f, d := s.findingFor(j, v, leeway.AlertState{Phase: leeway.PhaseFiring})
	if f.Kind != leeway.KindRankTierUnused {
		t.Errorf("finding kind = %q", f.Kind)
	}
	if d.Signal {
		t.Errorf("a Tier C verdict was routed to the wire by default: %+v", d)
	}
	if d.Reason == "" {
		t.Error("a suppressed delivery did not say why")
	}

	s.cfg.TierCSignals = true
	if _, d := s.findingFor(j, v, leeway.AlertState{Phase: leeway.PhaseFiring}); !d.Signal {
		t.Errorf("Tier C stayed suppressed with signals enabled: %+v", d)
	}
}

// TestSignalFor_PutsTheRuleInTheReason. The fingerprint's reasonClass is the
// reason, and the two rank_degraded rules share a kind — so without this an
// estate that tripped the floor and then the ceiling would look like one
// recurring incident rather than two conditions.
func TestSignalFor_PutsTheRuleInTheReason(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	j := &rankJudgement{axis: testAxis, class: s.classes["n4-preferred"]}
	k := alertKey{Axis: testAxis, Rule: episodeKey(leeway.RankRuleLastRank, leeway.RankUnknown)}

	f, _ := s.findingFor(j, leeway.RankVerdict{
		Rule:     leeway.RankRuleLastRank,
		Kind:     leeway.KindRankDegraded,
		Focus:    leeway.RankUnknown,
		Tier:     leeway.TierB,
		Breached: true,
		Reason:   "rank 2 share 0.98 is above the ceiling 0.90",
	}, leeway.AlertState{Phase: leeway.PhaseFiring, FirstSeenAt: t0})

	sig := signalFor(f, k, at(time.Hour))
	if sig.Source != engine.SourceSentinel {
		t.Errorf("source = %q, want the sentinel", sig.Source)
	}
	if sig.Key.Reason != f.Rule || sig.Key.Reason == "" {
		t.Errorf("reason = %q, want the judging rule", sig.Key.Reason)
	}
	if sig.Key.UID != findingUID(k) {
		t.Errorf("UID = %q, want %q", sig.Key.UID, findingUID(k))
	}
	if sig.KindOfObject != string(leeway.SubjectPreferenceAxis) || sig.Name != "n4-preferred" {
		t.Errorf("object = %s/%s, want the preference axis", sig.KindOfObject, sig.Name)
	}
	if sig.Namespace != "" {
		t.Errorf("namespace = %q, want empty: a compute class is cluster-scoped", sig.Namespace)
	}
	if !strings.Contains(sig.Message, "n4-preferred") {
		t.Errorf("message does not name the class: %q", sig.Message)
	}
	if !sig.FirstSeen.Equal(t0) || !sig.LastSeen.Equal(at(time.Hour)) {
		t.Errorf("times = %s..%s, want the episode's first-seen and now", sig.FirstSeen, sig.LastSeen)
	}
}

// TestClearance_AnswersOnlyForItsOwnIncidentsAndOnlyOnceSynced.
//
// Before the barrier every axis looks unoccupied, and reporting a fleet of
// findings recovered because we have not read the cluster yet is the one wrong
// answer that cannot be taken back.
func TestClearance_AnswersOnlyForItsOwnIncidentsAndOnlyOnceSynced(t *testing.T) {
	s := newTestSource(t)
	k := alertKey{Axis: testAxis, Rule: episodeKey(leeway.RankRuleLastRank, leeway.RankUnknown)}
	inc := engine.Incident{Key: engine.EventKey{UID: findingUID(k)}}

	if _, ok := s.Clearance(engine.Incident{Key: engine.EventKey{UID: "leeway:Deployment/shop/api|zone"}}); ok {
		t.Error("answered for topologydrift's incident")
	}
	if _, ok := s.Clearance(inc); ok {
		t.Error("answered before the caches synced")
	}

	s.mu.Lock()
	s.synced = true
	s.mu.Unlock()
	if _, ok := s.Clearance(inc); !ok {
		t.Error("declined its own incident once synced")
	}
}

func TestClearance_ReadsTheEpisodeAndThenTheClass(t *testing.T) {
	s := newTestSource(t)
	s.mu.Lock()
	s.synced = true
	s.mu.Unlock()
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)

	k := alertKey{Axis: testAxis, Rule: episodeKey(leeway.RankRuleLastRank, leeway.RankUnknown)}
	inc := engine.Incident{Key: engine.EventKey{UID: findingUID(k)}}

	// An open episode is the symptom, and PhaseResolving still reports Firing —
	// so the incident stays symptomatic for the whole of §8.2's resolve dwell.
	s.alerts.pass([]rankJudgement{judgement(testAxis, leeway.RankRuleLastRank, true)}, t0, time.Hour)
	c, ok := s.Clearance(inc)
	if !ok || c.Cleared {
		t.Fatalf("clearance = %+v (ok=%v) with the episode open, want symptomatic", c, ok)
	}

	// The episode ends: recovered.
	d := leeway.DefaultDwell()
	s.alerts.pass([]rankJudgement{judgement(testAxis, leeway.RankRuleLastRank, false)}, t0.Add(time.Minute), time.Hour)
	s.alerts.pass([]rankJudgement{judgement(testAxis, leeway.RankRuleLastRank, false)}, t0.Add(d.Resolve+2*time.Minute), time.Hour)
	c, ok = s.Clearance(inc)
	if !ok || !c.Cleared || c.Resolution != engine.ResolutionRecovered {
		t.Fatalf("clearance = %+v (ok=%v), want recovered", c, ok)
	}

	// The class itself goes: object_deleted, and StableSince stays zero because
	// dating the deletion from the sweep that noticed would be inventing a
	// timestamp.
	s.DeleteClass("n4-preferred", at(time.Hour))
	c, ok = s.Clearance(inc)
	if !ok || !c.Cleared || c.Resolution != engine.ResolutionObjectDeleted {
		t.Fatalf("clearance = %+v (ok=%v), want object_deleted", c, ok)
	}
	if !c.StableSince.IsZero() {
		t.Errorf("StableSince = %s on a deletion, want the zero time", c.StableSince)
	}
}

// TestClearanceObserver_IsTheSource — the seam engine wires, and a nil return
// would silently drop this source out of §7.4's recovery tracking.
func TestClearanceObserver_IsTheSource(t *testing.T) {
	s := newTestSource(t)
	if s.ClearanceObserver() == nil {
		t.Fatal("ClearanceObserver returned nil")
	}
}

// errRead is the store failure §9.2 says costs one dwell and nothing else.
var errRead = errors.New("store unavailable")

// recordingStore is an AlertStore that remembers what it was asked to do.
type recordingStore struct {
	load    []leeway.AlertRecord
	loadErr error

	puts    []leeway.AlertRecord
	deletes [][2]string
	err     error
}

func (r *recordingStore) LeewayAlertStates(context.Context, string) ([]leeway.AlertRecord, error) {
	return r.load, r.loadErr
}

func (r *recordingStore) PutLeewayAlertState(_ context.Context, rec leeway.AlertRecord) error {
	r.puts = append(r.puts, rec)
	return r.err
}

func (r *recordingStore) DeleteLeewayAlertState(_ context.Context, _, subject, state string) error {
	r.deletes = append(r.deletes, [2]string{subject, state})
	return r.err
}

// TestRunAlertPass_PersistsBeforeItEmits, and emits only on Firing.
//
// Both rules are topologydrift's: a finding on the wire the store does not know
// about becomes a duplicate at the next restart, and Resolved is answered by the
// clearance observer rather than by a second signal.
func TestRunAlertPass_PersistsBeforeItEmits(t *testing.T) {
	s := newTestSource(t)
	st := &recordingStore{}
	s.WithStore(st, "prod")

	var emitted []sources.Signal
	s.emit = func(sig sources.Signal) { emitted = append(emitted, sig) }

	// A class nothing can be provisioned for, with a pod stuck against it: the
	// one rule that reaches a verdict without waiting for a window.
	s.UpsertClass("locked", map[string]any{
		"priorities": []any{
			map[string]any{"machineFamily": "n4"},
			map[string]any{"machineFamily": "c3"},
		},
		"whenUnsatisfiable": "DoNotScaleUp",
	}, t0)
	s.UpsertPod(wedgedPod("stuck", "locked"), t0)

	now := t0
	s.now = func() time.Time { return now }
	s.runAlertPass(context.Background())

	if len(emitted) != 0 {
		t.Fatalf("emitted %d signal(s) while the episode was still Pending", len(emitted))
	}
	if len(st.puts) != 1 || st.puts[0].Phase != leeway.PhasePending {
		t.Fatalf("puts = %+v, want the Pending episode persisted", st.puts)
	}
	if st.puts[0].Cluster != "prod" {
		t.Errorf("record cluster = %q, want the one WithStore was given", st.puts[0].Cluster)
	}

	now = t0.Add(leeway.DefaultDwell().For + time.Minute)
	s.runAlertPass(context.Background())

	if len(emitted) != 1 {
		t.Fatalf("emitted %d signal(s) after the dwell, want one", len(emitted))
	}
	if emitted[0].Kind != leeway.KindRankWedged {
		t.Errorf("kind = %q, want the wedged finding", emitted[0].Kind)
	}
	if emitted[0].Severity != engine.SeverityCritical {
		t.Errorf("severity = %v, want Tier A's critical", emitted[0].Severity)
	}
	if n := len(st.puts); n != 2 || st.puts[1].Phase != leeway.PhaseFiring {
		t.Fatalf("puts = %+v, want the Firing episode persisted too", st.puts)
	}

	// The pod schedules; the episode resolves and the row is deleted rather
	// than stored in PhaseOK, so a recurrence reads as new.
	s.DeletePod(wedgedPod("stuck", "locked"), now)
	s.runAlertPass(context.Background()) // starts the resolve dwell
	now = now.Add(leeway.DefaultDwell().Resolve + time.Minute)
	s.runAlertPass(context.Background()) // ends it
	if len(st.deletes) == 0 {
		t.Error("the resolved episode was never deleted from the store")
	}
	if len(emitted) != 1 {
		t.Errorf("emitted %d signal(s); a resolution must not emit its own", len(emitted))
	}
}

// TestRun_DrivesTheAlertLoop. Every other test here calls runAlertPass
// directly, which would go on passing if the ticker were never wired — and a
// judging layer nothing calls is a source that exports its counters and never
// says anything.
func TestRun_DrivesTheAlertLoop(t *testing.T) {
	client := servedClient(pod("stuck", "", corev1.PodPending))
	dyn := dynClient(classObject(t, "locked", `{
	  "priorities": [{"machineFamily": "n4"}, {"machineFamily": "c3"}],
	  "whenUnsatisfiable": "DoNotScaleUp"
	}`))
	s := newRunnableSource(t, client, dyn)
	s.cfg.AlertInterval = 10 * time.Millisecond
	// No dwell at all, so one tick is enough to reach Firing. The dwell itself
	// is pinned in alerts_test.go; what is under test here is that a tick
	// happens.
	s.alerts = newRankAlerts(leeway.Dwell{For: 1, Resolve: time.Hour, FlapCount: 3, FlapWindow: time.Hour})

	emitted := make(chan sources.Signal, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, func(sig sources.Signal) { emitted <- sig }) }()

	// The wedged pod arrives after the informers start, so that the pass has
	// something to judge whichever order the caches sync in.
	deadline := time.Now().Add(5 * time.Second)
	for !s.HasSynced() {
		if time.Now().After(deadline) {
			t.Fatal("caches never synced")
		}
		time.Sleep(2 * time.Millisecond)
	}
	s.UpsertPod(wedgedPod("stuck", "locked"), time.Now())

	select {
	case sig := <-emitted:
		if sig.Kind != leeway.KindRankWedged {
			t.Errorf("kind = %q, want the wedged finding", sig.Kind)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the alert loop never emitted")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Run did not return within 5s of cancellation")
	}
}

// TestRunAlertPass_WithoutAStoreIsASupportedDeployment. §9.2: a missing history
// costs one dwell, never the monitoring.
func TestRunAlertPass_WithoutAStoreIsASupportedDeployment(t *testing.T) {
	s := newTestSource(t)
	s.WithStore(nil, "prod")
	if s.store != nil {
		t.Fatal("WithStore(nil) installed a store")
	}
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("worst", "n4-preferred", "n2", "2"), t0)
	s.UpsertPod(pod("p", "worst", corev1.PodRunning), t0)

	s.runAlertPass(context.Background()) // no panic, no store
	s.loadAlerts(context.Background())   // and nothing to load
}

// TestLoadAlerts_SurvivesAStoreThatCannotBeRead.
func TestLoadAlerts_SurvivesAStoreThatCannotBeRead(t *testing.T) {
	s := newTestSource(t)
	var logged []string
	s.logf = func(format string, args ...any) { logged = append(logged, format) }
	s.WithStore(&recordingStore{loadErr: errRead}, "prod")

	s.loadAlerts(context.Background())
	if len(logged) != 1 {
		t.Errorf("logged %d line(s), want one about the unreadable history", len(logged))
	}
	if s.alerts.PendingLen() != 0 {
		t.Errorf("PendingLen = %d after a failed read", s.alerts.PendingLen())
	}
}

func TestLoadAlerts_RestoresItsOwnRows(t *testing.T) {
	s := newTestSource(t)
	k := alertKey{Axis: testAxis, Rule: episodeKey(leeway.RankRuleLastRank, leeway.RankUnknown)}
	s.WithStore(&recordingStore{load: []leeway.AlertRecord{
		{SubjectKey: k.subjectKey(), TopologyKey: k.stateKey(),
			AlertState: leeway.AlertState{Phase: leeway.PhaseFiring, Since: t0}},
		{SubjectKey: "Deployment/shop/api", TopologyKey: "topology.kubernetes.io/zone"},
	}}, "prod")

	s.loadAlerts(context.Background())
	if got := s.alerts.PendingLen(); got != 1 {
		t.Errorf("PendingLen = %d, want only the rank episode held", got)
	}
}

// TestPersistOutcome_LogsRatherThanFailsOnAStoreError. The store is a
// convenience over a restart; a write that fails must not take the pass with
// it.
func TestPersistOutcome_LogsRatherThanFailsOnAStoreError(t *testing.T) {
	s := newTestSource(t)
	var logged int
	s.logf = func(string, ...any) { logged++ }
	s.WithStore(&recordingStore{err: errRead}, "prod")

	k := alertKey{Axis: testAxis, Rule: "last-rank"}
	s.persistOutcome(context.Background(), rankOutcome{
		Key: k, Transition: leeway.TransitionFiring, State: leeway.AlertState{Phase: leeway.PhaseFiring},
	}, t0)
	s.persistOutcome(context.Background(), rankOutcome{Key: k, Gone: true}, t0)
	if logged != 2 {
		t.Errorf("logged %d line(s), want one per failed write", logged)
	}

	// A move to nowhere is not written at all: the row already says this.
	before := len(s.store.(*recordingStore).puts)
	s.persistOutcome(context.Background(), rankOutcome{Key: k, Transition: leeway.TransitionNone}, t0)
	if got := len(s.store.(*recordingStore).puts); got != before {
		t.Errorf("puts = %d, want a no-op transition to write nothing", got)
	}
}

// TestEmitFinding_IsANoOpWithNothingToSay. The three nil checks are guards on
// paths that pass() can produce — an episode dropped for absence carries no
// verdict, and a source not yet Run has no emit function.
func TestEmitFinding_IsANoOpWithNothingToSay(t *testing.T) {
	s := newTestSource(t)
	o := rankOutcome{Key: alertKey{Axis: testAxis, Rule: "last-rank"}}

	s.emitFinding(nil, o, t0) // no emit function

	var emitted int
	s.emit = func(sources.Signal) { emitted++ }
	s.emitFinding(nil, o, t0)              // no judgement for the axis
	s.emitFinding(&rankJudgement{}, o, t0) // no verdict on the outcome
	if emitted != 0 {
		t.Errorf("emitted %d signal(s) from an outcome with nothing in it", emitted)
	}
}
