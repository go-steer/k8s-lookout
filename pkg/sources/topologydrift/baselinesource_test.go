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
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// fakeBaselineStore is a fakeStore that also carries §7.5's learned baselines,
// so a test can choose whether the source's store has the capability.
type fakeBaselineStore struct {
	*fakeStore

	bmu       sync.Mutex
	baselines map[string]leeway.BaselineRecord
	bseed     []leeway.BaselineRecord
	bdeletes  []string
	batched   []int // how many records each PutLeewayBaselines call carried
	breadErr  error
	bputErr   error
	bdelErr   error
}

func newFakeBaselineStore(seed ...leeway.BaselineRecord) *fakeBaselineStore {
	return &fakeBaselineStore{
		fakeStore: newFakeStore(),
		baselines: map[string]leeway.BaselineRecord{},
		bseed:     seed,
	}
}

func (f *fakeBaselineStore) LeewayBaselines(_ context.Context, _ string) ([]leeway.BaselineRecord, error) {
	f.bmu.Lock()
	defer f.bmu.Unlock()
	return f.bseed, f.breadErr
}

func (f *fakeBaselineStore) PutLeewayBaselines(_ context.Context, recs []leeway.BaselineRecord) error {
	f.bmu.Lock()
	defer f.bmu.Unlock()
	if f.bputErr != nil {
		return f.bputErr
	}
	f.batched = append(f.batched, len(recs))
	for _, rec := range recs {
		f.baselines[rowKey(rec.SubjectKey, rec.TopologyKey)] = rec
	}
	return nil
}

func (f *fakeBaselineStore) DeleteLeewayBaseline(_ context.Context, _, subjectKey, topologyKey string) error {
	f.bmu.Lock()
	defer f.bmu.Unlock()
	k := rowKey(subjectKey, topologyKey)
	f.bdeletes = append(f.bdeletes, k)
	if f.bdelErr != nil {
		return f.bdelErr
	}
	delete(f.baselines, k)
	return nil
}

func (f *fakeBaselineStore) baseline(sub leeway.SubjectRef, key leeway.TopologyKey) (leeway.BaselineRecord, bool) {
	f.bmu.Lock()
	defer f.bmu.Unlock()
	rec, ok := f.baselines[rowKey(sub.String(), string(key))]
	return rec, ok
}

func (f *fakeBaselineStore) biggestBatch() int {
	f.bmu.Lock()
	defer f.bmu.Unlock()
	var n int
	for _, sz := range f.batched {
		n = max(n, sz)
	}
	return n
}

func (f *fakeBaselineStore) baselineDeletes() []string {
	f.bmu.Lock()
	defer f.bmu.Unlock()
	return append([]string(nil), f.bdeletes...)
}

// quickLearning samples and flushes fast enough to watch inside a test, and
// matures a baseline in two samples instead of six hours.
func quickLearning() Config {
	cfg := quickAlerts()
	cfg.BaselineSampleInterval = 2 * time.Millisecond
	cfg.BaselineFlushInterval = 3 * time.Millisecond
	cfg.Baselines = leeway.BaselineConfig{
		HalfLife:   time.Minute,
		MinSamples: 2,
		MinAge:     time.Nanosecond,
	}
	return cfg
}

// apiPods is a second Deployment sharing the cluster, so the flush has more
// than one record to put in its one transaction.
func apiPods(count int, nodes ...string) []runtime.Object {
	out := []runtime.Object{replicaSet("api-64b8", "prod", "api")}
	for i := range count {
		out = append(out, pod(fmt.Sprintf("api-%d", i), "prod", nodes[i%len(nodes)], ownedBy("ReplicaSet", "api-64b8")))
	}
	return out
}

func TestSource_LearnsAPlacementAndBatchesItToTheStore(t *testing.T) {
	st := newFakeBaselineStore()
	objs := append(webPods(6, "n-a", "n-b", "n-c"), apiPods(3, "n-a", "n-b", "n-c")...)
	s, _ := runAlerting(t, quickLearning(), st, threeZoneCluster(objs...)...)

	key := baselineKey{Subject: webSub, Key: zoneKey}
	waitFor(t, "the baseline to mature and reach the store", func() bool {
		rec, ok := st.baseline(webSub, zoneKey)
		return ok && rec.Samples >= 2 && s.baselines.Intent(key, time.Now(), s.cfg.Baselines) != nil
	})

	rec, _ := st.baseline(webSub, zoneKey)
	if rec.Cluster != "prod" {
		t.Errorf("Cluster = %q, want prod: one store shared by a fleet has to keep the baselines apart", rec.Cluster)
	}
	if len(rec.Domains) != 3 || rec.Samples == 0 || rec.UpdatedAt.IsZero() {
		t.Fatalf("record = %+v, want three domains, a sample count and an age", rec)
	}
	// Six pods over three zones: the learned normal is a third each.
	for _, d := range rec.Domains {
		if d.Share < 0.3 || d.Share > 0.37 {
			t.Errorf("learned share for %s = %v, want about a third", d.Domain, d.Share)
		}
	}
	// And an evenly placed subject is exactly the case that must NOT alert, so
	// the learned intent has to come out with bands wide enough to hold it.
	in := s.baselines.Intent(key, time.Now(), s.cfg.Baselines)
	if in == nil || in.Source != leeway.SourceLearnedBaseline || len(in.Bands) != 3 {
		t.Fatalf("intent = %+v, want a learned one with three bands", in)
	}

	// §9.2's whole reason for a periodic flush rather than a write per sample:
	// what moved in one interval goes in one transaction. Two subjects are
	// sampled by the same tick, so they are dirty together and written together.
	waitFor(t, "two subjects to share one batch", func() bool { return st.biggestBatch() >= 2 })
}

// §7.5's central guard: a baseline that keeps learning through the episode it
// is being used to judge converges on the drift and stops calling it drift.
func TestSource_AFiringSubjectStopsLearning(t *testing.T) {
	st := newFakeBaselineStore()
	s, _ := runAlerting(t, quickLearning(), st, threeZoneCluster(webPods(6, "n-a")...)...)

	waitFor(t, "the episode to fire", func() bool {
		got, ok := s.alerts.StateOf(webSub, zoneKey)
		return ok && got.Phase.Firing()
	})
	waitFor(t, "the baseline to freeze", func() bool {
		return s.baselines.Stats(time.Now(), s.cfg.Baselines).Frozen == 1
	})

	// Frozen does not mean untracked — the set is still there, still written,
	// and still the thing that thaws when the episode clears.
	if n := s.baselines.Len(); n != 1 {
		t.Errorf("%d tracked baselines, want the frozen one to still be tracked", n)
	}
	waitFor(t, "the frozen set to be written", func() bool {
		_, ok := st.baseline(webSub, zoneKey)
		return ok
	})
}

// §9.3 step 1: what was learned before the restart comes back, so a rollout
// does not cost six hours of Tier C.
func TestSource_RestoresPersistedBaselines(t *testing.T) {
	now := time.Now()
	seed := leeway.BaselineRecord{
		Cluster: "prod", SubjectKey: webSub.String(), TopologyKey: string(zoneKey),
		Domains: []leeway.BaselineDomain{
			{Domain: "us-central1-a", Share: 0.5, Deviation: 0.02},
			{Domain: "us-central1-b", Share: 0.3, Deviation: 0.02},
			{Domain: "us-central1-c", Share: 0.2, Deviation: 0.02},
		},
		DevWeight: 0.9, Samples: 5000,
		FirstSeen: now.Add(-48 * time.Hour), UpdatedAt: now.Add(-time.Minute),
	}
	st := newFakeBaselineStore(seed)

	cfg := quickLearning()
	// Long enough that nothing is sampled during the test: what is asserted is
	// what the load put there, not what the source then learned over it.
	cfg.BaselineSampleInterval = time.Hour
	cfg.BaselineFlushInterval = time.Hour
	cfg.Baselines = leeway.BaselineConfig{}
	s, logs := runAlerting(t, cfg, st, threeZoneCluster(webPods(6, "n-a", "n-b", "n-c")...)...)

	waitFor(t, "the baseline to be restored", func() bool { return s.baselines.Len() == 1 })
	in := s.baselines.Intent(baselineKey{Subject: webSub, Key: zoneKey}, time.Now(), s.cfg.Baselines)
	if in == nil {
		t.Fatal("a mature persisted baseline came back unusable")
	}
	if got := in.ExplicitShares["us-central1-a"]; got != 0.5 {
		t.Errorf("restored share for zone a = %v, want the persisted 0.5", got)
	}
	if !strings.Contains(logs.all(), "restored 1 learned baseline") {
		t.Errorf("the restore was not logged: %s", logs.all())
	}
}

// Never refuse to start: a store that cannot be read costs six hours of
// relearning, not monitoring.
func TestSource_AnUnreadableBaselineStoreIsNotFatal(t *testing.T) {
	st := newFakeBaselineStore()
	st.breadErr = errors.New("database is locked")
	s, logs := runAlerting(t, quickLearning(), st, threeZoneCluster(webPods(6, "n-a", "n-b", "n-c")...)...)

	if !strings.Contains(logs.all(), "could not read persisted baselines") {
		t.Errorf("the read failure was not logged: %s", logs.all())
	}
	// And it keeps learning from scratch regardless.
	waitFor(t, "learning to start anyway", func() bool { return s.baselines.Len() > 0 })
}

// A failed flush is logged and dropped, not retried forever: the batch was
// drained and the next sample marks the same sets dirty again.
func TestSource_AFailedFlushKeepsLearning(t *testing.T) {
	st := newFakeBaselineStore()
	st.bputErr = errors.New("disk is full")
	s, logs := runAlerting(t, quickLearning(), st, threeZoneCluster(webPods(6, "n-a", "n-b", "n-c")...)...)

	waitFor(t, "the flush to fail", func() bool { return strings.Contains(logs.all(), "could not flush") })
	if s.baselines.Len() != 1 {
		t.Errorf("%d tracked baselines after a failed flush, want the estimate kept in memory", s.baselines.Len())
	}
}

// The store seam is optional in both directions: an AlertStore that does not
// carry baselines is a supported deployment, not a broken one.
func TestSource_AStoreWithoutTheBaselineCapabilityStillLearns(t *testing.T) {
	st := newFakeStore()
	s, _ := runAlerting(t, quickLearning(), st, threeZoneCluster(webPods(6, "n-a", "n-b", "n-c")...)...)

	waitFor(t, "learning to start", func() bool { return s.baselines.Len() > 0 })
	if s.baselineStore != nil {
		t.Error("an AlertStore-only store was adopted as a BaselineStore")
	}
}

func TestSource_LearningCanBeTurnedOff(t *testing.T) {
	off := false
	cfg := quickLearning()
	cfg.LearnBaselines = &off
	st := newFakeBaselineStore()
	s, _ := runAlerting(t, cfg, st, threeZoneCluster(webPods(6, "n-a", "n-b", "n-c")...)...)

	time.Sleep(50 * time.Millisecond)
	if n := s.baselines.Len(); n != 0 {
		t.Errorf("%d baselines learned with learning off", n)
	}
	if _, ok := st.baseline(webSub, zoneKey); ok {
		t.Error("a baseline was written with learning off")
	}
	// Still non-nil, so every read path stays total rather than nil-checking.
	if s.baselines == nil {
		t.Error("the baseline log is nil with learning off")
	}
}

// The reap is a sweep over what the index no longer scores — and it is held
// off for ReconcileGrace after a load, because at startup nothing has been
// scored yet and reaping on the first tick would delete every row just read.
func TestSource_TheReapWaitsForTheReconcileGrace(t *testing.T) {
	cfg := quickLearning()
	cfg.ReconcileGrace = time.Hour
	st := newFakeBaselineStore()
	s := New(fake.NewSimpleClientset(), cfg.normalize())
	s.WithStore(st, "prod")

	now := time.Now()
	s.loadBaselines(context.Background(), now)
	k := baselineKey{Subject: webSub, Key: zoneKey}
	s.baselines.Observe(k, logZones(), []int64{2, 2, 2}, now, s.cfg.Baselines)

	// Nothing is scored, so every key is a reap candidate — and none is reaped.
	s.sampleBaselines(now.Add(time.Minute))
	if s.baselines.Len() != 1 {
		t.Fatalf("a baseline was reaped inside the grace window (Len=%d)", s.baselines.Len())
	}

	// Past the grace, a subject nothing scores is gone, and the deletion is a
	// row deletion rather than an empty write — the next workload to take that
	// name must not inherit a predecessor's placement history.
	s.sampleBaselines(now.Add(2 * time.Hour))
	if s.baselines.Len() != 0 {
		t.Fatalf("a stale baseline survived the reap (Len=%d)", s.baselines.Len())
	}
	s.flushBaselines(context.Background())
	if got := st.baselineDeletes(); len(got) != 1 || got[0] != rowKey(webSub.String(), string(zoneKey)) {
		t.Errorf("deletes = %v, want the reaped subject-axis", got)
	}
	if _, ok := st.baseline(webSub, zoneKey); ok {
		t.Error("the reaped baseline is still in the store")
	}
}

// A deletion that will not go through is logged and left: the row is orphaned
// rather than the flush loop being wedged on it, and §9.2's prune is what
// eventually collects it.
func TestSource_AFailedDeleteIsLoggedAndDropped(t *testing.T) {
	cfg := quickLearning()
	st := newFakeBaselineStore()
	st.bdelErr = errors.New("database is locked")
	s := New(fake.NewSimpleClientset(), cfg.normalize())
	logs := &logCapture{}
	s.logf = logs.logf
	s.WithStore(st, "prod")

	now := time.Now()
	k := baselineKey{Subject: webSub, Key: zoneKey}
	s.baselines.Observe(k, logZones(), []int64{2, 2, 2}, now, s.cfg.Baselines)
	s.baselines.Forget(k)
	s.flushBaselines(context.Background())

	if !strings.Contains(logs.all(), "could not delete the learned baseline") {
		t.Errorf("the delete failure was not logged: %s", logs.all())
	}
}

// Cancellation is the common case for this process, and the final flush is
// what makes a graceful shutdown cost nothing.
func TestSource_TheFinalFlushSurvivesCancellation(t *testing.T) {
	cfg := quickLearning()
	cfg.BaselineFlushInterval = time.Hour // only the shutdown flush can write
	st := newFakeBaselineStore()
	s := New(fake.NewSimpleClientset(threeZoneCluster(webPods(6, "n-a", "n-b", "n-c")...)...), cfg.normalize())
	s.WithStore(st, "prod")

	now := time.Now()
	s.loadBaselines(context.Background(), now)
	k := baselineKey{Subject: webSub, Key: zoneKey}
	s.baselines.Observe(k, logZones(), []int64{2, 2, 2}, now, s.cfg.Baselines)

	// An already-cancelled context is the shape Run's deferred flush runs in,
	// and context.WithoutCancel is what keeps it from being a no-op.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.flushBaselines(context.WithoutCancel(ctx))

	if _, ok := st.baseline(webSub, zoneKey); !ok {
		t.Error("the shutdown flush wrote nothing")
	}
}
