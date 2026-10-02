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

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// quickBaselines matures after three samples and no wait.
func quickBaselines() leeway.BaselineConfig {
	c := leeway.DefaultBaselineConfig()
	c.MinSamples = 3
	c.MinAge = time.Nanosecond
	return c
}

// baselineStore is an AlertStore that can also persist baselines, and
// remembers what it was asked to do.
type baselineStore struct {
	recordingStore

	rows    []leeway.BaselineRecord
	rowsErr error

	putBaselines [][]leeway.BaselineRecord
	deleted      [][2]string
	putErr       error
}

func (b *baselineStore) LeewayBaselines(context.Context, string) ([]leeway.BaselineRecord, error) {
	return b.rows, b.rowsErr
}

func (b *baselineStore) PutLeewayBaselines(_ context.Context, recs []leeway.BaselineRecord) error {
	b.putBaselines = append(b.putBaselines, recs)
	return b.putErr
}

func (b *baselineStore) DeleteLeewayBaseline(_ context.Context, _, subject, state string) error {
	b.deleted = append(b.deleted, [2]string{subject, state})
	return b.putErr
}

// rank0Hour is an hour spent entirely on the preferred tier.
var rank0Hour = leeway.RankWindow{Elapsed: time.Hour, PodSeconds: map[leeway.Rank]float64{0: 3600}}

func TestBaselineKey_RoundTripsAndRefusesOtherRows(t *testing.T) {
	sub, state := axisSubjectKey(testAxis), baselineStateKey(testAxis)
	if got, ok := parseBaselineKey(sub, state); !ok || got != testAxis {
		t.Fatalf("parseBaselineKey(%q, %q) = %v, %v; want %v", sub, state, got, ok, testAxis)
	}

	for _, tc := range []struct{ why, sub, state string }{
		{"topologydrift's own baseline", "Deployment/shop/api", "topology.kubernetes.io/zone"},
		{"an alert row on the same axis", sub, alertKey{Axis: testAxis, Rule: "last-rank"}.stateKey()},
		{"a rule with no provider", sub, "/" + baselineRule},
		{"garbage", "garbage", "garbage"},
	} {
		if _, ok := parseBaselineKey(tc.sub, tc.state); ok {
			t.Errorf("claimed a row that is %s", tc.why)
		}
	}
}

// TestBaselines_RoundTripThroughTheStore: what one process learned is what the
// next one starts from, and a quiet pass writes nothing.
func TestBaselines_RoundTripThroughTheStore(t *testing.T) {
	s := newTestSource(t)
	st := &baselineStore{}
	s.WithStore(st, "prod")
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	axis := s.classes["n4-preferred"].Axis

	for i := 1; i <= 3; i++ {
		s.baselines.observe(axis, rank0Hour, false, at(time.Duration(i)*time.Minute), s.cfg.Baselines)
	}
	s.flushBaselines(context.Background())
	if len(st.putBaselines) != 1 || len(st.putBaselines[0]) != 1 {
		t.Fatalf("puts = %+v, want one batch of one row", st.putBaselines)
	}
	rec := st.putBaselines[0][0]
	if rec.Cluster != "prod" || rec.SubjectKey != axisSubjectKey(testAxis) || rec.TopologyKey != baselineStateKey(testAxis) {
		t.Errorf("row identity = %q %q %q", rec.Cluster, rec.SubjectKey, rec.TopologyKey)
	}

	s.flushBaselines(context.Background())
	if len(st.putBaselines) != 1 {
		t.Errorf("a pass that learned nothing wrote %d more batches", len(st.putBaselines)-1)
	}

	next := newTestSource(t)
	next.now = func() time.Time { return at(4 * time.Minute) }
	next.WithStore(&baselineStore{rows: []leeway.BaselineRecord{
		rec,
		{Cluster: "prod", SubjectKey: "Deployment/shop/api", TopologyKey: "topology.kubernetes.io/zone"},
	}}, "prod")
	next.loadBaselines(context.Background())
	got := next.baselines.sets[testAxis]
	if got == nil || got.Samples != 3 {
		t.Fatalf("restored set = %+v, want the three samples back", got)
	}
	if len(next.baselines.sets) != 1 {
		t.Errorf("load claimed %d sets, want only its own row", len(next.baselines.sets))
	}
}

func TestLoadBaselines_SurvivesAStoreThatCannotBeRead(t *testing.T) {
	s := newTestSource(t)
	var logged []string
	s.logf = func(f string, _ ...any) { logged = append(logged, f) }
	s.WithStore(&baselineStore{rowsErr: errors.New("disk on fire")}, "prod")
	s.loadBaselines(context.Background())
	if len(s.baselines.sets) != 0 || len(logged) != 1 {
		t.Errorf("sets=%d logged=%v, want nothing restored and one log line", len(s.baselines.sets), logged)
	}
}

// An AlertStore that cannot hold baselines is a supported deployment: the rule
// learns in memory and nothing is written anywhere.
func TestBaselines_WithAnAlertOnlyStore(t *testing.T) {
	s := newTestSource(t)
	s.WithStore(&recordingStore{}, "prod")
	if s.baselineStore != nil {
		t.Fatal("an AlertStore without the baseline methods was taken as a BaselineStore")
	}
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	s.baselines.observe(s.classes["n4-preferred"].Axis, rank0Hour, false, at(time.Minute), s.cfg.Baselines)
	s.loadBaselines(context.Background())
	s.flushBaselines(context.Background())
}

func TestFlushBaselines_LogsRatherThanFailsOnAStoreError(t *testing.T) {
	s := newTestSource(t)
	var logged []string
	s.logf = func(f string, _ ...any) { logged = append(logged, f) }
	st := &baselineStore{putErr: errors.New("read-only")}
	s.WithStore(st, "prod")
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	s.baselines.observe(s.classes["n4-preferred"].Axis, rank0Hour, false, at(time.Minute), s.cfg.Baselines)
	s.baselines.retain(map[leeway.AxisKey]struct{}{})
	s.baselines.observe(s.classes["n4-preferred"].Axis, rank0Hour, false, at(2*time.Minute), s.cfg.Baselines)
	s.baselines.gone[leeway.AxisKey{Provider: leeway.ProviderGKEComputeClass, Name: "gone"}] = true
	s.flushBaselines(context.Background())
	if len(logged) != 2 {
		t.Errorf("logged %v, want one line for the put and one for the delete", logged)
	}
}

// TestJudgeAll_LearnsOnlyFromAFullWindow: the ring is short after a start, and
// a one-minute window seeding the estimate would make its first hours an
// estimate of that minute.
func TestJudgeAll_LearnsOnlyFromAFullWindow(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("best", "n4-preferred", "n4", "0"), t0)
	s.UpsertPod(pod("p", "best", "n4-preferred", corev1.PodRunning), t0)

	s.judgeAll(at(time.Minute))
	s.judgeAll(at(2 * time.Minute))
	if b := s.baselines.sets[testAxis]; b != nil {
		t.Fatalf("learned from a one-minute window: %+v", b)
	}
	s.judgeAll(at(7 * time.Minute))
	if b := s.baselines.sets[testAxis]; b == nil || b.Samples != 1 {
		t.Fatalf("set = %+v, want one sample once the window reached MinWindow", b)
	}
}

// TestJudgeAll_HoldsTheBaselineWhileTheClassIsDegraded, on any degradation
// rule and not tier-unused.
func TestJudgeAll_HoldsTheBaselineWhileTheClassIsDegraded(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("best", "n4-preferred", "n4", "0"), t0)
	s.UpsertPod(pod("p", "best", "n4-preferred", corev1.PodRunning), t0)
	s.judgeAll(at(time.Minute))
	s.judgeAll(at(10 * time.Minute))

	// A tier nobody draws on is a cost observation, and can stay open for a
	// month; it must not stop the axis learning.
	s.alerts.pass([]rankJudgement{judgement(testAxis, leeway.RankRuleTierUnused, true)}, at(10*time.Minute), time.Hour)
	s.judgeAll(at(11 * time.Minute))
	b := s.baselines.sets[testAxis]
	if b == nil || b.Samples != 2 || b.Frozen {
		t.Fatalf("set = %+v, want a second sample with only tier-unused open", b)
	}

	s.alerts.pass([]rankJudgement{
		judgement(testAxis, leeway.RankRuleTierUnused, true),
		judgement(testAxis, leeway.RankRuleLastRank, true),
	}, at(11*time.Minute), time.Hour)
	s.judgeAll(at(12 * time.Minute))
	if b.Samples != 2 || !b.Frozen {
		t.Errorf("set = %+v, want it held at two samples while last-rank is open", b)
	}
}

// TestJudgeAll_ForgetsTheBaselineOfADeletedClass and owes the store a delete,
// for the same reason the ring is forgotten: AxisKey is provider and name
// only, and a class of the same name coming back is a new class.
func TestJudgeAll_ForgetsTheBaselineOfADeletedClass(t *testing.T) {
	s := newTestSource(t)
	st := &baselineStore{}
	s.WithStore(st, "prod")
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("best", "n4-preferred", "n4", "0"), t0)
	s.UpsertPod(pod("p", "best", "n4-preferred", corev1.PodRunning), t0)
	s.judgeAll(at(time.Minute))
	s.judgeAll(at(10 * time.Minute))
	s.flushBaselines(context.Background())

	s.DeleteClass("n4-preferred", at(11*time.Minute))
	s.judgeAll(at(12 * time.Minute))
	s.flushBaselines(context.Background())
	if len(s.baselines.sets) != 0 {
		t.Errorf("sets = %d after the class was deleted, want 0", len(s.baselines.sets))
	}
	want := [2]string{axisSubjectKey(testAxis), baselineStateKey(testAxis)}
	if len(st.deleted) != 1 || st.deleted[0] != want {
		t.Errorf("deletes = %v, want %v", st.deleted, want)
	}
}

// TestJudgeAll_TheBaselineRuleSeesAClassWorseThanItsOwnNormal. Half the class
// on rank 1 is nothing the absolute rules object to — the last rank is 2 and
// rank 0 is still occupied — but for a class that has always run on rank 0 it
// is a real change, and it is Tier C: metrics-only until asked for.
func TestJudgeAll_TheBaselineRuleSeesAClassWorseThanItsOwnNormal(t *testing.T) {
	s := newTestSource(t)
	s.cfg.Baselines = quickBaselines()
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	axis := s.classes["n4-preferred"].Axis
	for i := 3; i >= 1; i-- {
		s.baselines.observe(axis, rank0Hour, false, t0.Add(-time.Duration(i)*time.Minute), s.cfg.Baselines)
	}

	s.UpsertNode(node("mid", "n4-preferred", "c3", "1"), t0)
	s.UpsertPod(pod("p", "mid", "n4-preferred", corev1.PodRunning), t0)
	s.judgeAll(at(time.Minute))
	js := s.judgeAll(at(10 * time.Minute))

	var v *leeway.RankVerdict
	for i := range js[0].verdicts {
		if js[0].verdicts[i].Rule == leeway.RankRuleBaseline {
			v = &js[0].verdicts[i]
		}
	}
	if v == nil {
		t.Fatal("no rank-baseline verdict was returned")
	}
	if !v.Breached || !strings.Contains(v.Reason, "learned") {
		t.Fatalf("rank-baseline = %+v, want a breach against the learned normal", v)
	}
	if v.Tier != leeway.TierC || v.Kind != leeway.KindRankDegraded {
		t.Errorf("tier=%v kind=%q, want Tier C %q", v.Tier, v.Kind, leeway.KindRankDegraded)
	}
	if _, d := s.findingFor(&js[0], *v, leeway.AlertState{Phase: leeway.PhaseFiring}); d.Signal {
		t.Errorf("rank-baseline reached the wire without --compute-class-tier-c-signals: %+v", d)
	}
	s.cfg.TierCSignals = true
	if _, d := s.findingFor(&js[0], *v, leeway.AlertState{Phase: leeway.PhaseFiring}); !d.Signal {
		t.Errorf("rank-baseline stayed suppressed with Tier C signals on: %+v", d)
	}
}
