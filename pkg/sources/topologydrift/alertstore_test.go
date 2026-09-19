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
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// webSub is the Deployment every fixture in this file drifts.
var webSub = leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "web"}

// fakeStore is an in-memory AlertStore that records what the source asked of
// it and can be made to fail either half of §9.1's contract.
type fakeStore struct {
	mu      sync.Mutex
	rows    map[string]leeway.AlertRecord
	seeded  []leeway.AlertRecord
	deletes []string
	readErr error
	putErr  error
	delErr  error
}

func newFakeStore(seed ...leeway.AlertRecord) *fakeStore {
	return &fakeStore{rows: map[string]leeway.AlertRecord{}, seeded: seed}
}

func rowKey(subjectKey, topologyKey string) string { return subjectKey + "|" + topologyKey }

func (f *fakeStore) LeewayAlertStates(_ context.Context, _ string) ([]leeway.AlertRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seeded, f.readErr
}

func (f *fakeStore) PutLeewayAlertState(_ context.Context, rec leeway.AlertRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.putErr != nil {
		return f.putErr
	}
	f.rows[rowKey(rec.SubjectKey, rec.TopologyKey)] = rec
	return nil
}

func (f *fakeStore) DeleteLeewayAlertState(_ context.Context, _, subjectKey, topologyKey string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := rowKey(subjectKey, topologyKey)
	f.deletes = append(f.deletes, k)
	if f.delErr != nil {
		return f.delErr
	}
	delete(f.rows, k)
	return nil
}

func (f *fakeStore) row(sub leeway.SubjectRef, key leeway.TopologyKey) (leeway.AlertRecord, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.rows[rowKey(sub.String(), string(key))]
	return rec, ok
}

func (f *fakeStore) deleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deletes...)
}

// webPods builds count replicas of the web Deployment, placing replica i on
// nodes[i%len(nodes)].
func webPods(count int, nodes ...string) []runtime.Object {
	out := make([]runtime.Object, 0, count)
	for i := range count {
		out = append(out, pod(fmt.Sprintf("web-%d", i), "prod", nodes[i%len(nodes)], ownedBy("ReplicaSet", "web-7c9f")))
	}
	return out
}

// threeZoneCluster is a cluster with one ready node in each of three zones, plus the
// ReplicaSet the pods hop through to reach their Deployment.
func threeZoneCluster(pods ...runtime.Object) []runtime.Object {
	objs := []runtime.Object{
		replicaSet("web-7c9f", "prod", "web"),
		node("n-a", "us-central1-a"), node("n-b", "us-central1-b"), node("n-c", "us-central1-c"),
	}
	return append(objs, pods...)
}

// quickAlerts is a config whose coalescing, dwell and alert tick are all short
// enough to watch inside a test.
func quickAlerts() Config {
	return Config{
		TopologyKeys:          []leeway.TopologyKey{zoneKey},
		CoalesceWindow:        time.Millisecond,
		RolloutCoalesceWindow: time.Millisecond,
		MaxCoalesceDelay:      5 * time.Millisecond,
		Dwell:                 leeway.Dwell{For: time.Millisecond, Resolve: time.Millisecond, FlapCount: 3, FlapWindow: time.Minute},
		AlertInterval:         5 * time.Millisecond,
		Cluster:               "prod",
	}
}

// runAlerting starts a source over objs with st as its store (nil for none)
// and blocks until it is synced. The returned capture holds everything logged.
func runAlerting(t *testing.T, cfg Config, st AlertStore, objs ...runtime.Object) (*Source, *logCapture) {
	t.Helper()
	s := New(fake.NewSimpleClientset(objs...), cfg)
	logs := &logCapture{}
	s.logf = logs.logf
	if st != nil {
		s.WithStore(st, cfg.Cluster)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, func(sources.Signal) {}) }()
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
	return s, logs
}

func TestSource_ADriftingSubjectRunsTheMachineAndPersistsIt(t *testing.T) {
	// Six replicas in one of three eligible zones: ρ = 0.67 against a 0.2
	// threshold, four pods away from where they should be. The episode has to
	// cross the dwell and land in the store, keyed by cluster.
	st := newFakeStore()
	s, _ := runAlerting(t, quickAlerts(), st, threeZoneCluster(webPods(6, "n-a")...)...)

	waitFor(t, "the episode to reach the store", func() bool {
		rec, ok := st.row(webSub, zoneKey)
		return ok && rec.Phase == leeway.PhaseFiring
	})

	rec, _ := st.row(webSub, zoneKey)
	if rec.Cluster != "prod" {
		t.Errorf("Cluster = %q, want %q: one store shared by a fleet has to keep the episodes apart", rec.Cluster, "prod")
	}
	if rec.FirstSeenAt.IsZero() || rec.Since.IsZero() {
		t.Errorf("state = %+v, want both timestamps set — carrying them across a restart is the whole point of the row", rec.AlertState)
	}
	if got, ok := s.alerts.StateOf(webSub, zoneKey); !ok || got.Phase != leeway.PhaseFiring {
		t.Errorf("in-memory phase = %v (present=%v), want firing", got.Phase, ok)
	}
}

func TestSource_AnEvenlyPlacedClusterPersistsNothing(t *testing.T) {
	// The default posture, and the bound that makes the table affordable: a
	// fleet that is behaving writes no rows at all.
	st := newFakeStore()
	s, _ := runAlerting(t, quickAlerts(), st, threeZoneCluster(webPods(6, "n-a", "n-b", "n-c")...)...)

	// There is no positive event to wait for, so the assertion is that nothing
	// appears in several passes' worth of time.
	time.Sleep(50 * time.Millisecond)
	if n := s.alerts.Len(); n != 0 {
		t.Errorf("%d open episode(s) on an evenly placed cluster, want none", n)
	}
	if rec, ok := st.row(webSub, zoneKey); ok {
		t.Errorf("an evenly placed subject was persisted as %+v", rec.AlertState)
	}
}

func TestSource_APersistedEpisodeIsRestoredAndFiresWithoutANewDwell(t *testing.T) {
	// §9.1 end to end: the dwell does not restart because the process did. The
	// dwell here is an hour, so a fresh Pending could not possibly fire inside
	// this test — only the restored one, which went Pending an hour ago, can.
	cfg := quickAlerts()
	cfg.Dwell = leeway.Dwell{For: time.Hour, Resolve: time.Hour}
	st := newFakeStore(leeway.AlertRecord{
		Cluster:     "prod",
		SubjectKey:  webSub.String(),
		TopologyKey: string(zoneKey),
		AlertState:  leeway.AlertState{Phase: leeway.PhasePending, FirstSeenAt: time.Now().Add(-time.Hour)},
	})

	s, logs := runAlerting(t, cfg, st, threeZoneCluster(webPods(6, "n-a")...)...)

	waitFor(t, "the restored episode to fire", func() bool {
		got, ok := s.alerts.StateOf(webSub, zoneKey)
		return ok && got.Phase == leeway.PhaseFiring
	})
	if !strings.Contains(logs.all(), "restored 1 open episode(s)") {
		t.Errorf("the restore was silent:\n%s", logs.all())
	}
}

func TestSource_AnUnreadableHistoryCostsADwellAndNotTheMonitoring(t *testing.T) {
	// §9.2 said out loud: a sentinel does not refuse to start over its history
	// file. The machine runs; the operator is told what it cost.
	st := newFakeStore()
	st.readErr = errors.New("database is locked")

	s, logs := runAlerting(t, quickAlerts(), st, threeZoneCluster(webPods(6, "n-a")...)...)

	waitFor(t, "the machine to run anyway", func() bool {
		_, ok := s.alerts.StateOf(webSub, zoneKey)
		return ok
	})
	if !strings.Contains(logs.all(), "could not read persisted alert state") {
		t.Errorf("the read failure was silent:\n%s", logs.all())
	}
}

func TestSource_AResolvedEpisodeIsDeletedRatherThanStoredAsOK(t *testing.T) {
	// An episode back in PhaseOK is an absent row, not a row saying "fine" —
	// otherwise the table grows to one row per subject that has ever drifted
	// and never shrinks again.
	st := newFakeStore()
	s, _ := runAlerting(t, quickAlerts(), st, threeZoneCluster(webPods(6, "n-a")...)...)

	waitFor(t, "the episode to fire", func() bool {
		rec, ok := st.row(webSub, zoneKey)
		return ok && rec.Phase == leeway.PhaseFiring
	})

	// Spread the pods out. Each move re-enqueues the subject, the next
	// evaluation scores it clean, and the resolve dwell is a millisecond.
	for i, n := range []string{"n-b", "n-b", "n-c", "n-c"} {
		s.state.OnPodAdd(pod(fmt.Sprintf("web-%d", i+2), "prod", n, ownedBy("ReplicaSet", "web-7c9f")))
	}

	waitFor(t, "the row to be deleted", func() bool {
		_, ok := st.row(webSub, zoneKey)
		return !ok
	})
	if got := st.deleted(); len(got) == 0 {
		t.Error("the row vanished without a delete having been asked for")
	}
	if n := s.alerts.Len(); n != 0 {
		t.Errorf("%d episode(s) still open after the subject came back to plan", n)
	}
}

func TestSource_AWriteFailureIsLoggedAndTheEpisodeSurvivesInMemory(t *testing.T) {
	// A store that cannot be written loses the dwell across a restart. That is
	// not a reason to stop tracking the episode in the process still running.
	st := newFakeStore()
	st.putErr = errors.New("disk full")

	s, logs := runAlerting(t, quickAlerts(), st, threeZoneCluster(webPods(6, "n-a")...)...)

	waitFor(t, "the episode to fire in memory", func() bool {
		got, ok := s.alerts.StateOf(webSub, zoneKey)
		return ok && got.Phase == leeway.PhaseFiring
	})
	waitFor(t, "the write failure to be reported", func() bool {
		return strings.Contains(logs.all(), "could not persist alert state")
	})
}

func TestSource_PersistOutcomeWritesOnlyMoves(t *testing.T) {
	// The three outcomes a pass can produce, driven straight at the writer.
	// The standing-still case is reachable from Pass only through reconcile —
	// a restored Pending that is still inside its dwell — and it is the one
	// that matters for load: twenty thousand subjects ticking every thirty
	// seconds must not become twenty thousand UPSERTs saying nothing changed.
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	firing := leeway.AlertState{Phase: leeway.PhaseFiring, FirstSeenAt: now.Add(-time.Hour), Since: now}

	t.Run("a transition is written", func(t *testing.T) {
		st := newFakeStore()
		s := New(fake.NewSimpleClientset(), quickAlerts())
		s.WithStore(st, "prod")
		s.persistOutcome(context.Background(), Outcome{
			Subject: webSub, Key: zoneKey, Transition: leeway.TransitionFiring, State: firing,
		}, now)

		rec, ok := st.row(webSub, zoneKey)
		if !ok {
			t.Fatal("nothing was written")
		}
		if rec.Phase != leeway.PhaseFiring || !rec.UpdatedAt.Equal(now) {
			t.Errorf("record = %+v (updated %v), want a firing row stamped %v", rec.AlertState, rec.UpdatedAt, now)
		}
	})

	t.Run("standing still is not written", func(t *testing.T) {
		st := newFakeStore()
		s := New(fake.NewSimpleClientset(), quickAlerts())
		s.WithStore(st, "prod")
		s.persistOutcome(context.Background(), Outcome{
			Subject: webSub, Key: zoneKey, Transition: leeway.TransitionNone, State: firing,
		}, now)

		if _, ok := st.row(webSub, zoneKey); ok {
			t.Error("an episode that did not move was written anyway")
		}
	})

	t.Run("a failed delete is reported and does not stop the pass", func(t *testing.T) {
		st := newFakeStore()
		st.delErr = errors.New("database is locked")
		s := New(fake.NewSimpleClientset(), quickAlerts())
		logs := &logCapture{}
		s.logf = logs.logf
		s.WithStore(st, "prod")
		s.persistOutcome(context.Background(), Outcome{
			Subject: webSub, Key: zoneKey, Transition: leeway.TransitionResolved, State: leeway.AlertState{}, Gone: true,
		}, now)

		if len(st.deleted()) != 1 {
			t.Errorf("deletes = %v, want the one that failed", st.deleted())
		}
		if !strings.Contains(logs.all(), "could not clear persisted alert state") {
			t.Errorf("the delete failure was silent:\n%s", logs.all())
		}
	})
}

func TestSource_WithoutAStoreTheMachineStillRuns(t *testing.T) {
	// The deployment with no --store, which is the default one. WithStore(nil)
	// is a no-op rather than a way to install a store that errors on every
	// call — see the typed-nil guard in the watch wiring.
	bare := New(fake.NewSimpleClientset(), quickAlerts())
	bare.WithStore(nil, "prod")
	if bare.store != nil {
		t.Fatal("WithStore(nil) installed a store")
	}

	s, _ := runAlerting(t, quickAlerts(), nil, threeZoneCluster(webPods(6, "n-a")...)...)
	waitFor(t, "the episode to fire", func() bool {
		got, ok := s.alerts.StateOf(webSub, zoneKey)
		return ok && got.Phase == leeway.PhaseFiring
	})
}
