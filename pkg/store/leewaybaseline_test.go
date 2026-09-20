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
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

func testBaselineRecord(subject string) leeway.BaselineRecord {
	return leeway.BaselineRecord{
		Cluster:     testCluster,
		SubjectKey:  subject,
		TopologyKey: zoneKey,
		Domains: []leeway.BaselineDomain{
			{Domain: "zone-a", Share: 0.5, Deviation: 0.04},
			{Domain: "zone-b", Share: 0.3, Deviation: 0.02},
			{Domain: "zone-c", Share: 0.2, Deviation: 0.03},
		},
		DevWeight: 0.61,
		Samples:   412,
		FirstSeen: t0.Add(-30 * time.Hour),
		UpdatedAt: t0,
	}
}

// The point of the table: twelve hours of learning survives a restart, and
// survives it precisely enough to rebuild the same bands. A share or a
// deviation coming back rounded would move every band with it.
func TestLeewayBaseline_RoundTrip(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	want := testBaselineRecord("prod/Deployment/payments")
	if err := s.PutLeewayBaselines(ctx, []leeway.BaselineRecord{want}); err != nil {
		t.Fatalf("PutLeewayBaselines: %v", err)
	}
	got, err := s.LeewayBaselines(ctx, testCluster)
	if err != nil {
		t.Fatalf("LeewayBaselines: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("read %d rows, want 1", len(got))
	}
	if got[0].DevWeight != want.DevWeight || got[0].Samples != want.Samples {
		t.Errorf("weight/samples = %v/%d, want %v/%d", got[0].DevWeight, got[0].Samples, want.DevWeight, want.Samples)
	}
	if !got[0].FirstSeen.Equal(want.FirstSeen) || !got[0].UpdatedAt.Equal(want.UpdatedAt) {
		t.Errorf("timestamps = %v/%v, want %v/%v", got[0].FirstSeen, got[0].UpdatedAt, want.FirstSeen, want.UpdatedAt)
	}
	for i, d := range want.Domains {
		if got[0].Domains[i] != d {
			t.Errorf("domain %d = %+v, want %+v", i, got[0].Domains[i], d)
		}
	}

	// And it rebuilds into a live estimator that answers the same bands.
	set := got[0].Set()
	if set == nil {
		t.Fatal("a round-tripped record rebuilt to nil")
	}
	cfg := leeway.DefaultBaselineConfig()
	if a, b := set.Band(0, t0, cfg), want.Set().Band(0, t0, cfg); a != b {
		t.Errorf("band after a round trip = %v, want %v", a, b)
	}
}

// One flush, one transaction, however many subjects — and a later flush
// replaces rather than duplicating, because the estimate is the row.
func TestLeewayBaseline_BatchUpsertsInOneTransaction(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	batch := []leeway.BaselineRecord{
		testBaselineRecord("prod/Deployment/a"),
		testBaselineRecord("prod/Deployment/b"),
		testBaselineRecord("prod/StatefulSet/c"),
	}
	if err := s.PutLeewayBaselines(ctx, batch); err != nil {
		t.Fatalf("first flush: %v", err)
	}

	batch[1].Samples = 999
	batch[1].UpdatedAt = t0.Add(time.Minute)
	if err := s.PutLeewayBaselines(ctx, batch); err != nil {
		t.Fatalf("second flush: %v", err)
	}

	got, err := s.LeewayBaselines(ctx, testCluster)
	if err != nil {
		t.Fatalf("LeewayBaselines: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("read %d rows after two flushes of three subjects, want 3", len(got))
	}
	if got[1].SubjectKey != "prod/Deployment/b" || got[1].Samples != 999 {
		t.Errorf("row 1 = %q/%d, want the updated prod/Deployment/b", got[1].SubjectKey, got[1].Samples)
	}

	// An empty flush is the common case on a settled cluster.
	if err := s.PutLeewayBaselines(ctx, nil); err != nil {
		t.Errorf("empty flush = %v, want nil", err)
	}
}

// A batch with one bad record writes nothing. Half a flush would leave some
// subjects on this interval's estimate and some on the last one, and
// UpdatedAt — which §9.3 step 7 reads as downtime — would be lying about
// exactly those.
func TestLeewayBaseline_ABadRecordRollsBackTheWholeBatch(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	batch := []leeway.BaselineRecord{
		testBaselineRecord("prod/Deployment/good"),
		{Cluster: testCluster, SubjectKey: "", TopologyKey: zoneKey},
	}
	if err := s.PutLeewayBaselines(ctx, batch); err == nil {
		t.Fatal("a record with no subject key was accepted")
	}
	got, err := s.LeewayBaselines(ctx, testCluster)
	if err != nil {
		t.Fatalf("LeewayBaselines: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("read %d rows after a failed batch, want 0 — the good record was committed anyway", len(got))
	}
}

func TestLeewayBaseline_DeleteAndPrune(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	old := testBaselineRecord("prod/Deployment/ancient")
	old.UpdatedAt = t0.Add(-48 * time.Hour)
	fresh := testBaselineRecord("prod/Deployment/live")
	if err := s.PutLeewayBaselines(ctx, []leeway.BaselineRecord{old, fresh}); err != nil {
		t.Fatalf("PutLeewayBaselines: %v", err)
	}

	n, err := s.PruneLeewayBaselines(ctx, testCluster, t0.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("PruneLeewayBaselines: %v", err)
	}
	if n != 1 {
		t.Errorf("pruned %d rows, want 1", n)
	}

	if err := s.DeleteLeewayBaseline(ctx, testCluster, fresh.SubjectKey, zoneKey); err != nil {
		t.Fatalf("DeleteLeewayBaseline: %v", err)
	}
	// Deleting what is already gone is what a torn-down namespace looks like
	// on the second pass.
	if err := s.DeleteLeewayBaseline(ctx, testCluster, fresh.SubjectKey, zoneKey); err != nil {
		t.Errorf("second Delete = %v, want nil", err)
	}
	got, err := s.LeewayBaselines(ctx, testCluster)
	if err != nil || len(got) != 0 {
		t.Errorf("LeewayBaselines = %v, %v, want no rows", got, err)
	}
}

// §9.2: never refuse to start. A store predating v8 answers "nothing learned"
// on the read — which §7.5 handles natively, since an absent baseline is an
// immature one — and refuses the write rather than accepting one that would
// vanish.
func TestLeewayBaseline_PreV8Store(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	s.schemaVersion = leewayBaselineSchemaVersion - 1
	ctx := context.Background()

	got, err := s.LeewayBaselines(ctx, testCluster)
	if got != nil || err != nil {
		t.Errorf("pre-v8 read = %v, %v, want nil, nil", got, err)
	}
	if err := s.PutLeewayBaselines(ctx, []leeway.BaselineRecord{testBaselineRecord("x")}); !errors.Is(err, ErrNoLeewayBaselines) {
		t.Errorf("pre-v8 Put = %v, want ErrNoLeewayBaselines", err)
	}
	if err := s.DeleteLeewayBaseline(ctx, testCluster, "x", zoneKey); !errors.Is(err, ErrNoLeewayBaselines) {
		t.Errorf("pre-v8 Delete = %v, want ErrNoLeewayBaselines", err)
	}
	if _, err := s.PruneLeewayBaselines(ctx, testCluster, t0); !errors.Is(err, ErrNoLeewayBaselines) {
		t.Errorf("pre-v8 Prune = %v, want ErrNoLeewayBaselines", err)
	}
}

func TestLeewayBaseline_NilAndReadOnlyStores(t *testing.T) {
	t.Parallel()
	var nilStore *Store
	ctx := context.Background()

	if got, err := nilStore.LeewayBaselines(ctx, testCluster); got != nil || err != nil {
		t.Errorf("nil read = %v, %v, want nil, nil", got, err)
	}
	if err := nilStore.PutLeewayBaselines(ctx, []leeway.BaselineRecord{testBaselineRecord("x")}); err == nil {
		t.Error("nil Put returned no error")
	}
	if err := nilStore.DeleteLeewayBaseline(ctx, testCluster, "x", zoneKey); err == nil {
		t.Error("nil Delete returned no error")
	}
	if _, err := nilStore.PruneLeewayBaselines(ctx, testCluster, t0); err == nil {
		t.Error("nil Prune returned no error")
	}

	path := filepath.Join(t.TempDir(), "lookout.db")
	w, err := Open(path, WithLogf(t.Logf), WithClock((&testClock{now: t0}).Now))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	rec := testBaselineRecord("prod/Deployment/ro")
	if err := w.PutLeewayBaselines(ctx, []leeway.BaselineRecord{rec}); err != nil {
		t.Fatalf("PutLeewayBaselines: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := OpenRead(path, WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("OpenRead: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	if got, err := r.LeewayBaselines(ctx, testCluster); err != nil || len(got) != 1 {
		t.Fatalf("read-only read = %v, %v, want the one row", got, err)
	}
	if err := r.PutLeewayBaselines(ctx, []leeway.BaselineRecord{rec}); !errors.Is(err, ErrReadOnlyStore) {
		t.Errorf("read-only Put = %v, want ErrReadOnlyStore", err)
	}
	if err := r.DeleteLeewayBaseline(ctx, testCluster, rec.SubjectKey, zoneKey); !errors.Is(err, ErrReadOnlyStore) {
		t.Errorf("read-only Delete = %v, want ErrReadOnlyStore", err)
	}
	if _, err := r.PruneLeewayBaselines(ctx, testCluster, t0); !errors.Is(err, ErrReadOnlyStore) {
		t.Errorf("read-only Prune = %v, want ErrReadOnlyStore", err)
	}
}

// A damaged row must not take the load pass down with it, and must not be
// half-believed either.
func TestLeewayBaseline_DamagedRowsAreRefusedNotRepaired(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	// A negative sample count would wrap to an enormous uint64 and satisfy
	// the maturity gate it exists to hold shut.
	if _, err := s.db.ExecContext(ctx, `INSERT INTO leeway_baseline (
		cluster, subject_key, topology_key, domains, dev_weight, samples, first_seen, updated_at
	) VALUES (?,?,?,?,?,?,?,?)`,
		testCluster, "prod/Deployment/wrapped", zoneKey,
		`[{"domain":"zone-a","share":1,"deviation":0}]`, 0.5, -5,
		t0.Add(-time.Hour).UnixNano(), t0.UnixNano(),
	); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := s.LeewayBaselines(ctx, testCluster)
	if err != nil {
		t.Fatalf("LeewayBaselines: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("read %d rows, want 1", len(got))
	}
	if got[0].Samples != 0 {
		t.Errorf("negative samples decoded to %d, want 0", got[0].Samples)
	}
	if got[0].Set() != nil {
		t.Error("a zero-sample record rebuilt into an estimator")
	}

	// The write side saturates rather than wrapping, for the same reason:
	// SQLite integers are signed, so an unclamped count past MaxInt64 would
	// come back negative, read as zero, and discard a converged baseline.
	huge := testBaselineRecord("prod/Deployment/huge")
	huge.Samples = math.MaxUint64
	if err := s.PutLeewayBaselines(ctx, []leeway.BaselineRecord{huge}); err != nil {
		t.Fatalf("PutLeewayBaselines: %v", err)
	}
	back, err := s.LeewayBaselines(ctx, testCluster)
	if err != nil {
		t.Fatalf("LeewayBaselines: %v", err)
	}
	var found bool
	for _, r := range back {
		if r.SubjectKey != huge.SubjectKey {
			continue
		}
		found = true
		if r.Samples != math.MaxInt64 {
			t.Errorf("saturated samples = %d, want %d", r.Samples, uint64(math.MaxInt64))
		}
	}
	if !found {
		t.Error("the saturated record did not come back")
	}

	// Domains that are not JSON are a decode error, not a silent empty set:
	// this one is not a row a newer build could have written, so there is no
	// forward-compatibility reading to prefer.
	if _, err := s.db.ExecContext(ctx, `INSERT INTO leeway_baseline (
		cluster, subject_key, topology_key, domains, dev_weight, samples, first_seen, updated_at
	) VALUES (?,?,?,?,?,?,?,?)`,
		testCluster, "prod/Deployment/garbage", zoneKey,
		`not json`, 0.5, 10, t0.UnixNano(), t0.UnixNano(),
	); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := s.LeewayBaselines(ctx, testCluster); err == nil {
		t.Error("unparseable domains read as a valid row")
	}
}
