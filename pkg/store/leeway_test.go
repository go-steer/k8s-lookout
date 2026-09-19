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

package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

const zoneKey = "topology.kubernetes.io/zone"

func testAlertRecord(subject string) leeway.AlertRecord {
	return leeway.AlertRecord{
		Cluster:     testCluster,
		SubjectKey:  subject,
		TopologyKey: zoneKey,
		AlertState: leeway.AlertState{
			Phase:       leeway.PhaseFiring,
			FirstSeenAt: t0.Add(-40 * time.Minute),
			Since:       t0.Add(-30 * time.Minute),
			Fires: []time.Time{
				t0.Add(-50 * time.Minute),
				t0.Add(-30 * time.Minute),
			},
		},
		UpdatedAt: t0,
	}
}

// The whole point of the table: a dwell timer survives the process. If
// FirstSeenAt or Since came back wrong, a restart would silently restart a
// 30-minute clock, which is the one thing §9.1 says must not happen.
func TestLeewayAlertState_RoundTrip(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	want := testAlertRecord("prod/Deployment/payments")
	if err := s.PutLeewayAlertState(ctx, want); err != nil {
		t.Fatalf("PutLeewayAlertState: %v", err)
	}
	got, err := s.LeewayAlertStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("LeewayAlertStates: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("read back %d records, want 1", len(got))
	}
	g := got[0]
	if g.Cluster != want.Cluster || g.SubjectKey != want.SubjectKey || g.TopologyKey != want.TopologyKey {
		t.Errorf("identity = %q/%q/%q, want %q/%q/%q",
			g.Cluster, g.SubjectKey, g.TopologyKey, want.Cluster, want.SubjectKey, want.TopologyKey)
	}
	if g.Phase != want.Phase {
		t.Errorf("Phase = %v, want %v", g.Phase, want.Phase)
	}
	if !g.FirstSeenAt.Equal(want.FirstSeenAt) || !g.Since.Equal(want.Since) {
		t.Errorf("timestamps = %v/%v, want %v/%v", g.FirstSeenAt, g.Since, want.FirstSeenAt, want.Since)
	}
	if !g.UpdatedAt.Equal(want.UpdatedAt) {
		t.Errorf("UpdatedAt = %v, want %v", g.UpdatedAt, want.UpdatedAt)
	}
	if len(g.Fires) != len(want.Fires) {
		t.Fatalf("Fires = %v, want %v", g.Fires, want.Fires)
	}
	for i := range g.Fires {
		if !g.Fires[i].Equal(want.Fires[i]) {
			t.Errorf("Fires[%d] = %v, want %v", i, g.Fires[i], want.Fires[i])
		}
	}
}

// A PhaseResolving record carries ClearSince and no Since worth reading; the
// nullable columns must come back as the zero time, not as the epoch.
func TestLeewayAlertState_UnsetTimestampsRoundTripAsZero(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	rec := leeway.AlertRecord{
		Cluster: testCluster, SubjectKey: "prod/Deployment/api", TopologyKey: zoneKey,
		AlertState: leeway.AlertState{Phase: leeway.PhasePending, FirstSeenAt: t0},
		UpdatedAt:  t0,
	}
	if err := s.PutLeewayAlertState(ctx, rec); err != nil {
		t.Fatalf("PutLeewayAlertState: %v", err)
	}
	got, err := s.LeewayAlertStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("LeewayAlertStates: %v", err)
	}
	if !got[0].Since.IsZero() || !got[0].ClearSince.IsZero() {
		t.Errorf("Since/ClearSince = %v/%v, want both zero", got[0].Since, got[0].ClearSince)
	}
	if len(got[0].Fires) != 0 {
		t.Errorf("Fires = %v, want empty", got[0].Fires)
	}
}

// One row per (cluster, subject, topology key): a workload can be perfectly
// spread across zones and stacked three-deep on one node, and those are two
// independent episodes.
func TestLeewayAlertState_TopologyKeyIsPartOfTheIdentity(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	zone := testAlertRecord("prod/Deployment/payments")
	host := zone
	host.TopologyKey = "kubernetes.io/hostname"
	host.Phase = leeway.PhasePending
	for _, rec := range []leeway.AlertRecord{zone, host} {
		if err := s.PutLeewayAlertState(ctx, rec); err != nil {
			t.Fatalf("PutLeewayAlertState: %v", err)
		}
	}
	got, err := s.LeewayAlertStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("LeewayAlertStates: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("read back %d records, want 2 — the topology key must not collapse them", len(got))
	}
}

func TestLeewayAlertState_PutReplacesInPlace(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	rec := testAlertRecord("prod/Deployment/payments")
	if err := s.PutLeewayAlertState(ctx, rec); err != nil {
		t.Fatalf("PutLeewayAlertState: %v", err)
	}
	rec.Phase = leeway.PhaseResolving
	rec.ClearSince = t0.Add(5 * time.Minute)
	rec.UpdatedAt = t0.Add(5 * time.Minute)
	if err := s.PutLeewayAlertState(ctx, rec); err != nil {
		t.Fatalf("second PutLeewayAlertState: %v", err)
	}
	got, err := s.LeewayAlertStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("LeewayAlertStates: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("read back %d records, want 1 — the upsert must not insert a second row", len(got))
	}
	if got[0].Phase != leeway.PhaseResolving || !got[0].ClearSince.Equal(rec.ClearSince) {
		t.Errorf("got %v/%v, want resolving at %v", got[0].Phase, got[0].ClearSince, rec.ClearSince)
	}
}

// Same rationale as FindingStates: two clusters sharing one file must not see
// each other's dwell timers, or one would reconcile the other's episodes away.
func TestLeewayAlertState_IsClusterScoped(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	mine := testAlertRecord("prod/Deployment/payments")
	theirs := mine
	theirs.Cluster = "prod-west"
	for _, rec := range []leeway.AlertRecord{mine, theirs} {
		if err := s.PutLeewayAlertState(ctx, rec); err != nil {
			t.Fatalf("PutLeewayAlertState: %v", err)
		}
	}
	got, err := s.LeewayAlertStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("LeewayAlertStates: %v", err)
	}
	if len(got) != 1 || got[0].Cluster != testCluster {
		t.Fatalf("read back %+v, want only the %s row", got, testCluster)
	}
}

func TestLeewayAlertState_ReadsAreOrdered(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	for _, name := range []string{"zebra", "apple", "mango"} {
		if err := s.PutLeewayAlertState(ctx, testAlertRecord("prod/Deployment/"+name)); err != nil {
			t.Fatalf("PutLeewayAlertState: %v", err)
		}
	}
	got, err := s.LeewayAlertStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("LeewayAlertStates: %v", err)
	}
	var last string
	for _, rec := range got {
		if rec.SubjectKey < last {
			t.Fatalf("subject %q came after %q — the reconcile pass must be deterministic", rec.SubjectKey, last)
		}
		last = rec.SubjectKey
	}
}

// Deleting is what ends an episode, and it is what keeps the table bounded by
// "how many subjects are drifting" rather than by how many subjects exist.
func TestLeewayAlertState_DeleteEndsTheEpisode(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	rec := testAlertRecord("prod/Deployment/payments")
	if err := s.PutLeewayAlertState(ctx, rec); err != nil {
		t.Fatalf("PutLeewayAlertState: %v", err)
	}
	if err := s.DeleteLeewayAlertState(ctx, rec.Cluster, rec.SubjectKey, rec.TopologyKey); err != nil {
		t.Fatalf("DeleteLeewayAlertState: %v", err)
	}
	got, err := s.LeewayAlertStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("LeewayAlertStates: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("read back %d records, want 0", len(got))
	}
	// Deleting what is already gone is the caller restating a fact, not an
	// error: an episode that resolves twice must not fail the second time.
	if err := s.DeleteLeewayAlertState(ctx, rec.Cluster, rec.SubjectKey, rec.TopologyKey); err != nil {
		t.Errorf("second DeleteLeewayAlertState: %v, want nil", err)
	}
}

func TestLeewayAlertState_PutRefusesAnIncompleteIdentity(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	for _, rec := range []leeway.AlertRecord{
		{Cluster: testCluster, TopologyKey: zoneKey},
		{Cluster: testCluster, SubjectKey: "prod/Deployment/api"},
	} {
		if err := s.PutLeewayAlertState(ctx, rec); err == nil {
			t.Errorf("PutLeewayAlertState(%+v) = nil, want an error", rec)
		}
	}
}

// A disabled store is the no-flag default and must cost nothing: reads answer
// "no state" so every subject starts with a fresh dwell, and writes say so
// rather than panicking.
func TestLeewayAlertState_NilStore(t *testing.T) {
	t.Parallel()
	var s *Store
	ctx := context.Background()

	got, err := s.LeewayAlertStates(ctx, testCluster)
	if got != nil || err != nil {
		t.Errorf("LeewayAlertStates on a nil store = %v, %v, want nil, nil", got, err)
	}
	if err := s.PutLeewayAlertState(ctx, testAlertRecord("x")); err == nil {
		t.Error("PutLeewayAlertState on a nil store = nil, want an error")
	}
	if err := s.DeleteLeewayAlertState(ctx, testCluster, "x", zoneKey); err == nil {
		t.Error("DeleteLeewayAlertState on a nil store = nil, want an error")
	}
}

// An OpenRead handle serves the read half and refuses both writes.
func TestLeewayAlertState_ReadOnlyStore(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "lookout.db")
	w, err := Open(path, WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	rec := testAlertRecord("prod/Deployment/payments")
	if err := w.PutLeewayAlertState(ctx, rec); err != nil {
		t.Fatalf("PutLeewayAlertState: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := OpenRead(path, WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("OpenRead: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	got, err := r.LeewayAlertStates(ctx, testCluster)
	if err != nil || len(got) != 1 {
		t.Fatalf("LeewayAlertStates = %v, %v, want the one row", got, err)
	}
	if err := r.PutLeewayAlertState(ctx, rec); !errors.Is(err, ErrReadOnlyStore) {
		t.Errorf("read-only Put = %v, want ErrReadOnlyStore", err)
	}
	if err := r.DeleteLeewayAlertState(ctx, rec.Cluster, rec.SubjectKey, rec.TopologyKey); !errors.Is(err, ErrReadOnlyStore) {
		t.Errorf("read-only Delete = %v, want ErrReadOnlyStore", err)
	}
}

// §9.2: never refuse to start. A store predating v7 answers "no state" on the
// read — degraded, with fresh dwell timers — and refuses the write rather than
// accepting one that would vanish.
func TestLeewayAlertState_PreV7Store(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	s.schemaVersion = leewayAlertSchemaVersion - 1
	ctx := context.Background()

	got, err := s.LeewayAlertStates(ctx, testCluster)
	if got != nil || err != nil {
		t.Errorf("pre-v7 read = %v, %v, want nil, nil", got, err)
	}
	if err := s.PutLeewayAlertState(ctx, testAlertRecord("x")); !errors.Is(err, ErrNoLeewayAlertState) {
		t.Errorf("pre-v7 Put = %v, want ErrNoLeewayAlertState", err)
	}
	if err := s.DeleteLeewayAlertState(ctx, testCluster, "x", zoneKey); !errors.Is(err, ErrNoLeewayAlertState) {
		t.Errorf("pre-v7 Delete = %v, want ErrNoLeewayAlertState", err)
	}
}

// A row written by a build that knows a phase this one does not must not fail
// the whole reconcile read. It decodes to the zero phase, which leeway's
// repair treats as "forget the episode" — one lost dwell, not a crashed sweep.
func TestLeewayAlertState_AnUnknownPhaseDecodesRatherThanFailing(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	if _, err := s.db.ExecContext(ctx, `INSERT INTO leeway_alert_state (
		cluster, subject_key, topology_key, phase, first_seen, fires, updated_at
	) VALUES (?,?,?,?,?,?,?)`,
		testCluster, "prod/Deployment/api", zoneKey, "quiescing",
		t0.UnixNano(), "[]", t0.UnixNano(),
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, err := s.LeewayAlertStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("LeewayAlertStates: %v", err)
	}
	if len(got) != 1 || got[0].Phase != leeway.PhaseOK {
		t.Fatalf("got %+v, want one record in the zero phase", got)
	}
}

// A corrupt flap history is a decode error, unlike an unknown phase: the JSON
// is ours and unparseable bytes mean the row is damaged rather than newer.
func TestLeewayAlertState_ACorruptFlapHistoryIsAnError(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	if _, err := s.db.ExecContext(ctx, `INSERT INTO leeway_alert_state (
		cluster, subject_key, topology_key, phase, fires, updated_at
	) VALUES (?,?,?,?,?,?)`,
		testCluster, "prod/Deployment/api", zoneKey, "firing", "{not json", t0.UnixNano(),
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := s.LeewayAlertStates(ctx, testCluster); err == nil {
		t.Fatal("LeewayAlertStates = nil error, want a decode failure")
	}
}

// An unset UpdatedAt takes the store's clock. Writing the zero time would
// persist the year 1 and make §9.3's downtime assessment read two millennia.
func TestLeewayAlertState_AnUnsetUpdatedAtTakesTheClock(t *testing.T) {
	t.Parallel()
	s, clock := openTest(t)
	ctx := context.Background()

	rec := testAlertRecord("prod/Deployment/payments")
	rec.UpdatedAt = time.Time{}
	if err := s.PutLeewayAlertState(ctx, rec); err != nil {
		t.Fatalf("PutLeewayAlertState: %v", err)
	}
	got, err := s.LeewayAlertStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("LeewayAlertStates: %v", err)
	}
	if !got[0].UpdatedAt.Equal(clock.Now()) {
		t.Errorf("UpdatedAt = %v, want the clock's %v", got[0].UpdatedAt, clock.Now())
	}
}
