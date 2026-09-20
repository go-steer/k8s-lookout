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
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// settledBaseline is what a subject that has sat evenly across the three zones
// for a day looks like on disk: converged shares, a deviation of nothing, and
// enough samples and age to be past both maturity gates.
//
// Seeded through the store rather than grown in the test, because the two
// things under test here are scoring and routing, and growing a real baseline
// would make every assertion below also depend on the estimator's arithmetic —
// which BaselineSet's own tests own. The restore path is the realistic one
// anyway: after the first day of any deployment, every baseline that scores
// anything arrived through §9.3 step 1.
func settledBaseline(now time.Time, shares ...float64) leeway.BaselineRecord {
	zones := []leeway.Domain{"us-central1-a", "us-central1-b", "us-central1-c"}
	rec := leeway.BaselineRecord{
		Cluster:     "prod",
		SubjectKey:  webSub.String(),
		TopologyKey: string(zoneKey),
		DevWeight:   1,
		Samples:     500,
		FirstSeen:   now.Add(-24 * time.Hour),
		UpdatedAt:   now,
	}
	for i, z := range zones {
		rec.Domains = append(rec.Domains, leeway.BaselineDomain{Domain: z, Share: shares[i]})
	}
	return rec
}

// settledLearning keeps the sampler and the flush out of the way — both tickers
// are set past any plausible test runtime — so that what is scored is exactly
// the seeded baseline and not a blend of it with whatever the live cluster
// happens to look like. Everything else is the shipped default, including the
// band width, because that is the number these tests are really about.
func settledLearning() Config {
	cfg := quickAlerts()
	cfg.BaselineSampleInterval = time.Hour
	cfg.BaselineFlushInterval = time.Hour
	cfg.Baselines = leeway.DefaultBaselineConfig()
	cfg.TierCSignals = true
	return cfg
}

// runEmitting is runAlerting with the emitted signals kept.
func runEmitting(t *testing.T, cfg Config, st AlertStore, objs ...runtime.Object) (*Source, func() []sources.Signal) {
	t.Helper()
	s := New(fake.NewSimpleClientset(objs...), cfg)
	s.logf = func(string, ...any) {}
	if st != nil {
		s.WithStore(st, cfg.Cluster)
	}

	var (
		mu  sync.Mutex
		got []sources.Signal
	)
	emit := func(sig sources.Signal) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, sig)
	}
	signals := func() []sources.Signal {
		mu.Lock()
		defer mu.Unlock()
		return append([]sources.Signal(nil), got...)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, emit) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v, want nil on cancellation", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Run did not return after cancellation")
		}
	})

	waitFor(t, "the source to sync", s.HasSynced)
	return s, signals
}

// TestSource_ALearnedBaselineBreachReachesTheWire is §14's Phase 5 exit
// criterion in miniature, and the first time any code path can produce
// leeway.baseline_breach: a Deployment that declared nothing has always sat
// evenly across three zones, all six replicas end up in one, and the finding
// says so as a Tier C baseline breach rather than as placement drift.
//
// The distinction is the point. Without the baseline the same placement is
// still a finding — scored against an even apportionment — but it is
// placement_drift, and the expectation it is measured against is one leeway
// invented. With the baseline the expectation is what the workload itself has
// been doing all week, which is the difference between "this is not evenly
// spread" and "this changed".
func TestSource_ALearnedBaselineBreachReachesTheWire(t *testing.T) {
	even := 1.0 / 3.0
	st := newFakeBaselineStore(settledBaseline(time.Now(), even, even, even))
	s, signals := runEmitting(t, settledLearning(), st, threeZoneCluster(webPods(6, "n-a")...)...)

	waitFor(t, "the baseline breach to reach the wire", func() bool { return len(signals()) > 0 })

	sigs := signals()
	if len(sigs) != 1 {
		t.Fatalf("emitted %d signals, want exactly one: %+v", len(sigs), sigs)
	}
	sig := sigs[0]
	if sig.Kind != leeway.KindBaselineBreach {
		t.Errorf("Kind = %q, want %q — the intent came from a learned baseline, so the kind follows it", sig.Kind, leeway.KindBaselineBreach)
	}
	if sig.Key.UID != findingUID(webSub, zoneKey) {
		t.Errorf("UID = %q, want the subject-axis key", sig.Key.UID)
	}
	// The learned share, not the even split, is what the finding quotes back:
	// 6 replicas over a learned third is an expectation of 2 in each zone.
	if !strings.Contains(sig.Message, "us-central1-a 6/2") {
		t.Errorf("the message does not carry the learned expectation:\n%s", sig.Message)
	}

	ev := axisOf(s.state.EvaluationsOf(webSub), zoneKey)
	if ev == nil {
		t.Fatal("no zone evaluation for the subject")
	}
	if ev.Intent == nil || ev.Intent.Source != leeway.SourceLearnedBaseline {
		t.Fatalf("intent = %+v, want the restored baseline", ev.Intent)
	}
	if ev.Verdict.Tier != leeway.TierC {
		t.Errorf("Tier = %v, want C — a baseline is measured, not declared", ev.Verdict.Tier)
	}
	if ev.Verdict.Kind != leeway.BreachBaseline {
		t.Errorf("Breach = %v, want the per-domain band rule rather than the drift rule", ev.Verdict.Kind)
	}
}

// TestSource_ABaselineBreachStaysMetricsOnlyByDefault keeps the Tier C opt-in
// where §8.3 puts it. A learned baseline makes Tier C *sharper*, not louder:
// the workload's owner still never asked to be watched, so the default stays
// what it was.
func TestSource_ABaselineBreachStaysMetricsOnlyByDefault(t *testing.T) {
	even := 1.0 / 3.0
	cfg := settledLearning()
	cfg.TierCSignals = false
	st := newFakeBaselineStore(settledBaseline(time.Now(), even, even, even))
	s, signals := runEmitting(t, cfg, st, threeZoneCluster(webPods(6, "n-a")...)...)

	waitFor(t, "the episode to open", func() bool {
		got, live := s.alerts.StateOf(webSub, zoneKey)
		return live && got.Phase == leeway.PhaseFiring
	})
	if sigs := signals(); len(sigs) != 0 {
		t.Errorf("a Tier C baseline breach reached the wire without the opt-in: %+v", sigs)
	}
}

// lopsidedPods is a subject sitting 4/1/1 across the three zones — lopsided
// enough that any fixed expectation calls it drift, and the placement the
// baseline in these two tests has learned as normal.
func lopsidedPods() []runtime.Object {
	return threeZoneCluster(webPods(6, "n-a", "n-a", "n-a", "n-a", "n-b", "n-c")...)
}

// TestSource_AWorkloadDoingWhatItAlwaysDoesIsNotABreach is the false-positive
// direction, and the reason learning is safe to leave on by default. Its pair
// is the test below, which runs the identical fixture with learning off and
// does get a finding — so this one cannot pass for the boring reason that
// nothing was scored.
func TestSource_AWorkloadDoingWhatItAlwaysDoesIsNotABreach(t *testing.T) {
	st := newFakeBaselineStore(settledBaseline(time.Now(), 4.0/6, 1.0/6, 1.0/6))
	s, signals := runEmitting(t, settledLearning(), st, lopsidedPods()...)

	waitFor(t, "the subject to be scored against its baseline", func() bool {
		ev := axisOf(s.state.EvaluationsOf(webSub), zoneKey)
		return ev != nil && ev.Intent != nil && ev.Intent.Source == leeway.SourceLearnedBaseline
	})
	// Several alert passes' worth of time, since the assertion is an absence.
	time.Sleep(50 * time.Millisecond)

	if sigs := signals(); len(sigs) != 0 {
		t.Errorf("a subject sitting exactly on its learned normal was reported: %+v", sigs)
	}
	if n := s.alerts.Len(); n != 0 {
		t.Errorf("%d open episode(s), want none", n)
	}
	// Drift is measured against the learned expectation, so it collapses to
	// nothing — which is the mechanism, not a coincidence: the baseline moved
	// the expectation onto the observation.
	if ev := axisOf(s.state.EvaluationsOf(webSub), zoneKey); ev == nil || ev.Scores.Drift != 0 {
		t.Errorf("drift = %v, want 0 against the subject's own learned shares", ev.Scores.Drift)
	}
}

// TestSource_LearningOffScoresAgainstTheAssumedDefault is the counterfactual
// for the test above, on the same fixture, and what
// --topology-learn-baselines=false has to mean: the baseline is still on disk,
// and it is not consulted. What scores the subject instead is the assumed
// cluster default — the entry §5.1 now ranks immediately below the baseline —
// and 4/1/1 against its even expectation is placement drift.
func TestSource_LearningOffScoresAgainstTheAssumedDefault(t *testing.T) {
	cfg := settledLearning()
	off := false
	cfg.LearnBaselines = &off
	st := newFakeBaselineStore(settledBaseline(time.Now(), 4.0/6, 1.0/6, 1.0/6))
	s, signals := runEmitting(t, cfg, st, lopsidedPods()...)

	waitFor(t, "the drift finding", func() bool { return len(signals()) > 0 })
	if kind := signals()[0].Kind; kind != leeway.KindPlacementDrift {
		t.Errorf("Kind = %q, want %q — nothing learned is in play", kind, leeway.KindPlacementDrift)
	}
	ev := axisOf(s.state.EvaluationsOf(webSub), zoneKey)
	if ev == nil {
		t.Fatal("no zone evaluation for the subject")
	}
	if ev.Intent == nil || ev.Intent.Source == leeway.SourceLearnedBaseline {
		t.Errorf("intent = %+v, want anything but the baseline the flag switched off", ev.Intent)
	}
	if ev.Scores.Drift <= leeway.DefaultThresholds().Drift {
		t.Errorf("drift = %v, want it over the fixed threshold", ev.Scores.Drift)
	}
}
