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
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/memory"
)

func testFact() memory.DistilledFact {
	return memory.DistilledFact{
		Class: "capacity.stockout.recurrence",
		Scope: map[string]string{
			memory.ScopeZone:      "us-east1-b",
			memory.ScopeNodeGroup: "n2d-pool",
		},
		Statement:          "us-east1-b nodegroup n2d-pool: 3 stockouts in 7d",
		WindowStart:        t0.Add(-7 * 24 * time.Hour),
		WindowEnd:          t0,
		Occurrences:        3,
		DistinctObjects:    1,
		FirstSeen:          t0.Add(-5 * 24 * time.Hour),
		LastSeen:           t0.Add(-time.Hour),
		SourceFingerprints: []string{"sha256:bbbb", "sha256:aaaa"},
	}
}

// TestUpsertFact_InsertRoundTrip: a new fact round-trips through
// Facts with implementation-assigned Created/Updated stamps and the
// fingerprint list sorted + deduplicated.
func TestUpsertFact_InsertRoundTrip(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	stored, err := s.UpsertFact(ctx, testFact())
	if err != nil {
		t.Fatalf("UpsertFact: %v", err)
	}
	if !stored.Created.Equal(t0) || !stored.Updated.Equal(t0) {
		t.Errorf("stamps = created %s updated %s, want both %s", stored.Created, stored.Updated, t0)
	}

	got, err := s.Facts(ctx, memory.FactQuery{})
	if err != nil {
		t.Fatalf("Facts: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Facts returned %d, want 1", len(got))
	}
	f := got[0]
	if f.Class != "capacity.stockout.recurrence" || f.Statement != testFact().Statement {
		t.Errorf("round-trip lost identity: %+v", f)
	}
	if f.Scope[memory.ScopeZone] != "us-east1-b" || f.Scope[memory.ScopeNodeGroup] != "n2d-pool" {
		t.Errorf("scope round-trip: %v", f.Scope)
	}
	if f.Occurrences != 3 || f.DistinctObjects != 1 {
		t.Errorf("counts round-trip: %+v", f)
	}
	if !f.FirstSeen.Equal(testFact().FirstSeen) || !f.LastSeen.Equal(testFact().LastSeen) {
		t.Errorf("seen bounds round-trip: first %s last %s", f.FirstSeen, f.LastSeen)
	}
	if len(f.SourceFingerprints) != 2 || f.SourceFingerprints[0] != "sha256:aaaa" {
		t.Errorf("fingerprints not sorted+deduped: %v", f.SourceFingerprints)
	}
}

// TestUpsertFact_DedupeUpdates: same (class, scope) folds into the
// existing row — counts and window refresh, Created survives,
// Updated advances, fingerprints union — and the table never grows.
func TestUpsertFact_DedupeUpdates(t *testing.T) {
	t.Parallel()
	s, clock := openTest(t)
	ctx := context.Background()

	if _, err := s.UpsertFact(ctx, testFact()); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	clock.Advance(6 * time.Hour)

	next := testFact()
	next.Occurrences = 5
	next.Statement = "us-east1-b nodegroup n2d-pool: 5 stockouts in 7d"
	next.LastSeen = t0.Add(5 * time.Hour)
	next.SourceFingerprints = []string{"sha256:cccc", "sha256:aaaa"}
	stored, err := s.UpsertFact(ctx, next)
	if err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	if !stored.Created.Equal(t0) {
		t.Errorf("Created should survive updates: %s, want %s", stored.Created, t0)
	}
	if !stored.Updated.Equal(t0.Add(6 * time.Hour)) {
		t.Errorf("Updated = %s, want %s", stored.Updated, t0.Add(6*time.Hour))
	}

	got, err := s.Facts(ctx, memory.FactQuery{})
	if err != nil {
		t.Fatalf("Facts: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("dedupe failed: %d rows, want 1", len(got))
	}
	f := got[0]
	if f.Occurrences != 5 || f.Statement != next.Statement {
		t.Errorf("update did not refresh evidence: %+v", f)
	}
	want := []string{"sha256:aaaa", "sha256:bbbb", "sha256:cccc"}
	if len(f.SourceFingerprints) != len(want) {
		t.Fatalf("fingerprint union = %v, want %v", f.SourceFingerprints, want)
	}
	for i, fp := range want {
		if f.SourceFingerprints[i] != fp {
			t.Errorf("fingerprint union[%d] = %s, want %s", i, f.SourceFingerprints[i], fp)
		}
	}

	// A DIFFERENT scope is a different fact.
	other := testFact()
	other.Scope = map[string]string{memory.ScopeZone: "us-east1-c", memory.ScopeNodeGroup: "n2d-pool"}
	if _, err := s.UpsertFact(ctx, other); err != nil {
		t.Fatalf("third upsert: %v", err)
	}
	if got, _ := s.Facts(ctx, memory.FactQuery{}); len(got) != 2 {
		t.Errorf("distinct scope should insert: %d rows, want 2", len(got))
	}
}

// TestFacts_Filters: class, scope-subset, UpdatedSince, and Limit
// are AND-combined; results are newest-updated first.
func TestFacts_Filters(t *testing.T) {
	t.Parallel()
	s, clock := openTest(t)
	ctx := context.Background()

	if _, err := s.UpsertFact(ctx, testFact()); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	clock.Advance(time.Hour)
	crash := memory.DistilledFact{
		Class:     "workload.crashloop.recurrence",
		Scope:     map[string]string{memory.ScopeNamespace: "prod", memory.ScopeWorkload: "payment"},
		Statement: "prod/payment: recurring crashloops",
	}
	if _, err := s.UpsertFact(ctx, crash); err != nil {
		t.Fatalf("upsert crash: %v", err)
	}

	if got, _ := s.Facts(ctx, memory.FactQuery{Class: "workload.crashloop.recurrence"}); len(got) != 1 || got[0].Class != crash.Class {
		t.Errorf("class filter: %+v", got)
	}
	if got, _ := s.Facts(ctx, memory.FactQuery{Scope: map[string]string{memory.ScopeZone: "us-east1-b"}}); len(got) != 1 || got[0].Class != "capacity.stockout.recurrence" {
		t.Errorf("scope filter: %+v", got)
	}
	if got, _ := s.Facts(ctx, memory.FactQuery{Scope: map[string]string{memory.ScopeZone: "nowhere"}}); len(got) != 0 {
		t.Errorf("non-matching scope filter returned %d", len(got))
	}
	if got, _ := s.Facts(ctx, memory.FactQuery{UpdatedSince: t0.Add(30 * time.Minute)}); len(got) != 1 || got[0].Class != crash.Class {
		t.Errorf("UpdatedSince filter: %+v", got)
	}
	got, _ := s.Facts(ctx, memory.FactQuery{Limit: 1})
	if len(got) != 1 || got[0].Class != crash.Class {
		t.Errorf("Limit should keep newest-updated first: %+v", got)
	}
}

// TestUpsertFact_Validation: unusable facts are rejected before any
// SQL.
func TestUpsertFact_Validation(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	if _, err := s.UpsertFact(context.Background(), memory.DistilledFact{Class: "x"}); err == nil {
		t.Error("expected validation error for scopeless fact")
	}
}

// TestMemoryFacts_NilAndReadOnly: nil store reads answer nothing and
// writes error; OpenRead serves reads but refuses writes
// (ErrReadOnlyStore) — memory records are the resident sentinel's to
// write.
func TestMemoryFacts_NilAndReadOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var nilStore *Store
	if got, err := nilStore.Facts(ctx, memory.FactQuery{}); err != nil || got != nil {
		t.Errorf("nil store Facts = %v, %v; want nil, nil", got, err)
	}
	if _, err := nilStore.UpsertFact(ctx, testFact()); err == nil {
		t.Error("nil store UpsertFact should error")
	}

	path := filepath.Join(t.TempDir(), "lookout.db")
	w, err := Open(path, WithLogf(t.Logf), WithClock((&testClock{now: t0}).Now))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := w.UpsertFact(ctx, testFact()); err != nil {
		t.Fatalf("UpsertFact: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := OpenRead(path, WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("OpenRead: %v", err)
	}
	defer func() { _ = r.Close() }()
	if got, err := r.Facts(ctx, memory.FactQuery{}); err != nil || len(got) != 1 {
		t.Errorf("read-only Facts = %d, %v; want 1, nil", len(got), err)
	}
	if _, err := r.UpsertFact(ctx, testFact()); !errors.Is(err, ErrReadOnlyStore) {
		t.Errorf("read-only UpsertFact = %v, want ErrReadOnlyStore", err)
	}
}

// TestFacts_PreMemorySchemaStore: a read-only open of a store
// written before migration v3 answers "no facts", never "no such
// table".
func TestFacts_PreMemorySchemaStore(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", "file:"+url.PathEscape(path))
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_version (version INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create version table: %v", err)
	}
	for _, m := range migrations[:memorySchemaVersion-1] {
		if _, err := db.Exec(m); err != nil {
			t.Fatalf("apply old migration: %v", err)
		}
	}
	if _, err := db.Exec(`INSERT INTO schema_version (version) VALUES (?)`, memorySchemaVersion-1); err != nil {
		t.Fatalf("set version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	r, err := OpenRead(path, WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("OpenRead: %v", err)
	}
	defer func() { _ = r.Close() }()
	if got, err := r.Facts(context.Background(), memory.FactQuery{}); err != nil || got != nil {
		t.Errorf("pre-memory store Facts = %v, %v; want nil, nil", got, err)
	}
}

func testTriageRecord() memory.TriageStatusRecord {
	return memory.TriageStatusRecord{
		Fingerprint:         "sha256:crash",
		ResourceKey:         "Pod/prod/payment-7d5b9c6f4-x2k9q",
		Session:             "sid-42",
		Status:              memory.StatusTriaged,
		RootCauseHypothesis: "bad connection string",
		SeverityOverride:    "warning",
		Action:              "PR #402 opened",
	}
}

// TestTriageStatus_RoundTripAndUpsert: the §9.4 record round-trips;
// a rewrite for the same (fingerprint, resource_key) REPLACES it —
// the record is current state, not a journal.
func TestTriageStatus_RoundTripAndUpsert(t *testing.T) {
	t.Parallel()
	s, clock := openTest(t)
	ctx := context.Background()

	stored, err := s.UpsertTriageStatus(ctx, testTriageRecord())
	if err != nil {
		t.Fatalf("UpsertTriageStatus: %v", err)
	}
	if !stored.Updated.Equal(t0) {
		t.Errorf("Updated = %s, want store-assigned %s", stored.Updated, t0)
	}
	got, err := s.TriageStatuses(ctx, memory.TriageQuery{})
	if err != nil {
		t.Fatalf("TriageStatuses: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("records = %d, want 1", len(got))
	}
	if got[0] != stored {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", got[0], stored)
	}

	clock.Advance(time.Hour)
	next := testTriageRecord()
	next.Status = memory.StatusActioned
	next.Action = "PR #402 merged"
	if _, err := s.UpsertTriageStatus(ctx, next); err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	got, _ = s.TriageStatuses(ctx, memory.TriageQuery{})
	if len(got) != 1 {
		t.Fatalf("upsert duplicated: %d rows", len(got))
	}
	if got[0].Status != memory.StatusActioned || got[0].Action != "PR #402 merged" || !got[0].Updated.Equal(t0.Add(time.Hour)) {
		t.Errorf("upsert did not replace: %+v", got[0])
	}

	// A different resource under the same fingerprint is its own
	// record (the key is the §9.4 pair).
	other := testTriageRecord()
	other.ResourceKey = "Pod/prod/payment-7d5b9c6f4-m8t2z"
	if _, err := s.UpsertTriageStatus(ctx, other); err != nil {
		t.Fatalf("third upsert: %v", err)
	}
	if got, _ := s.TriageStatuses(ctx, memory.TriageQuery{}); len(got) != 2 {
		t.Errorf("distinct resource_key should insert: %d rows", len(got))
	}
}

// TestTriageStatus_QueryFilters: fingerprint, OpenOnly, and
// UpdatedSince are AND-combined.
func TestTriageStatus_QueryFilters(t *testing.T) {
	t.Parallel()
	s, clock := openTest(t)
	ctx := context.Background()

	if _, err := s.UpsertTriageStatus(ctx, testTriageRecord()); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	clock.Advance(time.Hour)
	resolved := testTriageRecord()
	resolved.Fingerprint = "sha256:other"
	resolved.Status = memory.StatusResolved
	if _, err := s.UpsertTriageStatus(ctx, resolved); err != nil {
		t.Fatalf("upsert resolved: %v", err)
	}

	if got, _ := s.TriageStatuses(ctx, memory.TriageQuery{Fingerprint: "sha256:crash"}); len(got) != 1 || got[0].Fingerprint != "sha256:crash" {
		t.Errorf("fingerprint filter: %+v", got)
	}
	if got, _ := s.TriageStatuses(ctx, memory.TriageQuery{OpenOnly: true}); len(got) != 1 || got[0].Status != memory.StatusTriaged {
		t.Errorf("OpenOnly filter: %+v", got)
	}
	if got, _ := s.TriageStatuses(ctx, memory.TriageQuery{UpdatedSince: t0.Add(30 * time.Minute)}); len(got) != 1 || got[0].Status != memory.StatusResolved {
		t.Errorf("UpdatedSince filter: %+v", got)
	}
	if got, _ := s.TriageStatuses(ctx, memory.TriageQuery{Limit: 1}); len(got) != 1 || got[0].Status != memory.StatusResolved {
		t.Errorf("Limit keeps newest-updated first: %+v", got)
	}
}

// TestResolveTriageStatus: the §9.4 automatic lifecycle — the flip
// targets (fingerprint, resource_key ∈ keys) only, is idempotent,
// and never touches other resources sharing the class fingerprint.
func TestResolveTriageStatus(t *testing.T) {
	t.Parallel()
	s, clock := openTest(t)
	ctx := context.Background()

	mine := testTriageRecord()
	sibling := testTriageRecord()
	sibling.ResourceKey = "Pod/prod/other-pod" // same class fingerprint, different incident
	for _, rec := range []memory.TriageStatusRecord{mine, sibling} {
		if _, err := s.UpsertTriageStatus(ctx, rec); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	clock.Advance(time.Hour)

	flipped, err := s.ResolveTriageStatus(ctx, "sha256:crash",
		"Pod/prod/payment-7d5b9c6f4-x2k9q", "ReplicaSet/prod/payment-7d5b9c6f4")
	if err != nil {
		t.Fatalf("ResolveTriageStatus: %v", err)
	}
	if flipped != 1 {
		t.Fatalf("flipped = %d, want exactly the incident's record", flipped)
	}
	open, _ := s.TriageStatuses(ctx, memory.TriageQuery{OpenOnly: true})
	if len(open) != 1 || open[0].ResourceKey != sibling.ResourceKey {
		t.Errorf("open records after flip = %+v, want only the sibling", open)
	}
	all, _ := s.TriageStatuses(ctx, memory.TriageQuery{Fingerprint: "sha256:crash"})
	for _, rec := range all {
		if rec.ResourceKey == mine.ResourceKey {
			if rec.Status != memory.StatusResolved || !rec.Updated.Equal(t0.Add(time.Hour)) {
				t.Errorf("flipped record = %+v", rec)
			}
		}
	}

	// Idempotent: a second recovery observation flips nothing.
	if flipped, err := s.ResolveTriageStatus(ctx, "sha256:crash", mine.ResourceKey); err != nil || flipped != 0 {
		t.Errorf("second flip = %d, %v; want 0, nil", flipped, err)
	}
	// Absent incident: no-op, no error.
	if flipped, err := s.ResolveTriageStatus(ctx, "sha256:unknown", "Pod/x/y"); err != nil || flipped != 0 {
		t.Errorf("unknown flip = %d, %v; want 0, nil", flipped, err)
	}
}

// TestTriageStatus_NilReadOnlyAndValidation mirrors the fact-side
// postures for the triage surface.
func TestTriageStatus_NilReadOnlyAndValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var nilStore *Store
	if got, err := nilStore.TriageStatuses(ctx, memory.TriageQuery{}); err != nil || got != nil {
		t.Errorf("nil store TriageStatuses = %v, %v", got, err)
	}
	if _, err := nilStore.UpsertTriageStatus(ctx, testTriageRecord()); err == nil {
		t.Error("nil store UpsertTriageStatus should error")
	}
	if n, err := nilStore.ResolveTriageStatus(ctx, "sha256:x", "Pod/a/b"); err != nil || n != 0 {
		t.Errorf("nil store ResolveTriageStatus = %d, %v", n, err)
	}

	path := filepath.Join(t.TempDir(), "lookout.db")
	w, err := Open(path, WithLogf(t.Logf), WithClock((&testClock{now: t0}).Now))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := w.UpsertTriageStatus(ctx, testTriageRecord()); err != nil {
		t.Fatalf("UpsertTriageStatus: %v", err)
	}
	if _, err := w.UpsertTriageStatus(ctx, memory.TriageStatusRecord{Fingerprint: "sha256:x"}); err == nil {
		t.Error("invalid record accepted")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := OpenRead(path, WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("OpenRead: %v", err)
	}
	defer func() { _ = r.Close() }()
	if got, err := r.TriageStatuses(ctx, memory.TriageQuery{}); err != nil || len(got) != 1 {
		t.Errorf("read-only TriageStatuses = %d, %v; want 1, nil", len(got), err)
	}
	if _, err := r.UpsertTriageStatus(ctx, testTriageRecord()); !errors.Is(err, ErrReadOnlyStore) {
		t.Errorf("read-only UpsertTriageStatus = %v, want ErrReadOnlyStore", err)
	}
	if _, err := r.ResolveTriageStatus(ctx, "sha256:crash", "Pod/prod/x"); !errors.Is(err, ErrReadOnlyStore) {
		t.Errorf("read-only ResolveTriageStatus = %v, want ErrReadOnlyStore", err)
	}
}

// TestTriageStatus_PreTriageSchemaStore: pre-v4 stores answer "no
// records" on the read path.
func TestTriageStatus_PreTriageSchemaStore(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", "file:"+url.PathEscape(path))
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_version (version INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create version table: %v", err)
	}
	for _, m := range migrations[:triageSchemaVersion-1] {
		if _, err := db.Exec(m); err != nil {
			t.Fatalf("apply old migration: %v", err)
		}
	}
	if _, err := db.Exec(`INSERT INTO schema_version (version) VALUES (?)`, triageSchemaVersion-1); err != nil {
		t.Fatalf("set version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}
	r, err := OpenRead(path, WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("OpenRead: %v", err)
	}
	defer func() { _ = r.Close() }()
	if got, err := r.TriageStatuses(context.Background(), memory.TriageQuery{}); err != nil || got != nil {
		t.Errorf("pre-triage store TriageStatuses = %v, %v; want nil, nil", got, err)
	}
}

// TestPrune_ExemptsTriageStatus: records live until their lifecycle
// resolves them — the §9.1 prune never deletes them.
func TestPrune_ExemptsTriageStatus(t *testing.T) {
	t.Parallel()
	s, clock := openTest(t, WithTTL(time.Hour))
	ctx := context.Background()
	if _, err := s.UpsertTriageStatus(ctx, testTriageRecord()); err != nil {
		t.Fatalf("UpsertTriageStatus: %v", err)
	}
	clock.Advance(48 * time.Hour)
	if _, err := s.PruneOnce(ctx); err != nil {
		t.Fatalf("PruneOnce: %v", err)
	}
	if got, err := s.TriageStatuses(ctx, memory.TriageQuery{}); err != nil || len(got) != 1 {
		t.Errorf("record pruned by TTL: %d records, %v; want 1 survivor", len(got), err)
	}
}

// TestPrune_ExemptsMemoryFacts: facts are durable memories, not
// telemetry — the §9.1 TTL prune must never delete them.
func TestPrune_ExemptsMemoryFacts(t *testing.T) {
	t.Parallel()
	s, clock := openTest(t, WithTTL(time.Hour))
	ctx := context.Background()
	if _, err := s.UpsertFact(ctx, testFact()); err != nil {
		t.Fatalf("UpsertFact: %v", err)
	}
	clock.Advance(48 * time.Hour)
	if _, err := s.PruneOnce(ctx); err != nil {
		t.Fatalf("PruneOnce: %v", err)
	}
	if got, err := s.Facts(ctx, memory.FactQuery{}); err != nil || len(got) != 1 {
		t.Errorf("fact pruned by TTL: %d facts, %v; want 1 survivor", len(got), err)
	}
}
