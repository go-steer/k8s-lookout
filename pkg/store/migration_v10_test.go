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
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/memory"
)

func seedDBAtVersion(t *testing.T, path string, version int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open seed sqlite db: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`CREATE TABLE schema_version (version INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create schema_version: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO schema_version (version) VALUES (?)`, version); err != nil {
		t.Fatalf("insert schema_version: %v", err)
	}
	for i := 0; i < version; i++ {
		if _, err := db.Exec(migrations[i]); err != nil {
			t.Fatalf("apply seed migration v%d: %v", i+1, err)
		}
	}
	if version >= 4 {
		if _, err := db.Exec(`INSERT INTO triage_status (
			fingerprint, resource_key, session, status, root_cause_hypothesis, severity_override, action, updated
		) VALUES ('sha256:legacy', 'Pod/default/legacy', 'sess-legacy', 'triaged', 'legacy hyp', 'warning', '', 1000)`); err != nil {
			t.Fatalf("seed legacy v4 triage row: %v", err)
		}
	}
}

func TestMigrationsFromV1ThroughV5ToLatestAndReadOnlyFallback(t *testing.T) {
	ctx := context.Background()
	for startVer := 1; startVer <= 5; startVer++ {
		startVer := startVer
		t.Run(fmt.Sprintf("upgrade_from_v%d", startVer), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), fmt.Sprintf("v%d.sqlite", startVer))
			seedDBAtVersion(t, path, startVer)

			// If v4+, verify read-only open BEFORE upgrading succeeds without missing-column errors (#659).
			if startVer >= 4 {
				ro, err := OpenRead(path)
				if err != nil {
					t.Fatalf("OpenRead pre-v10 DB: %v", err)
				}
				recs, err := ro.TriageStatuses(ctx, memory.TriageQuery{})
				if err != nil {
					t.Fatalf("TriageStatuses on pre-v10 read-only DB: %v", err)
				}
				if len(recs) != 1 || recs[0].Fingerprint != "sha256:legacy" {
					t.Fatalf("unexpected pre-v10 read-only records: %+v", recs)
				}
				_ = ro.Close()
			}

			// Upgrade DB via writable Open and verify latest schema version + v10 columns & index.
			s, err := Open(path)
			if err != nil {
				t.Fatalf("Open upgrade from v%d: %v", startVer, err)
			}
			defer func() { _ = s.Close() }()

			if s.schemaVersion != len(migrations) {
				t.Fatalf("schemaVersion = %d, want %d", s.schemaVersion, len(migrations))
			}

			rec, err := s.UpsertTriageStatus(ctx, memory.TriageStatusRecord{
				Fingerprint:     "sha256:v10-fp",
				ResourceKey:     "Pod/prod/web-1",
				Session:         "sess-v10",
				Status:          memory.StatusTriaged,
				CanonicalReason: "CrashLoopBackOff",
				Cluster:         "prod-cluster",
			})
			if err != nil {
				t.Fatalf("UpsertTriageStatus on upgraded v%d DB: %v", startVer, err)
			}

			got, err := s.TriageStatuses(ctx, memory.TriageQuery{Fingerprint: "sha256:v10-fp"})
			if err != nil {
				t.Fatalf("TriageStatuses on upgraded v%d DB: %v", startVer, err)
			}
			if len(got) != 1 || got[0].CanonicalReason != "CrashLoopBackOff" || got[0].Cluster != "prod-cluster" {
				t.Fatalf("round-tripped record = %+v, want CanonicalReason=CrashLoopBackOff Cluster=prod-cluster (from %+v)", got, rec)
			}

			var idxName string
			err = s.db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type='index' AND name='idx_occurrences_emitted_at'`).Scan(&idxName)
			if err != nil || idxName != "idx_occurrences_emitted_at" {
				t.Fatalf("missing idx_occurrences_emitted_at after upgrading from v%d: %v", startVer, err)
			}
		})
	}
}

func TestBatchedTTLPruneAndSnapshotFirstPruneOrder(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "prune.sqlite")
	s, err := Open(path, WithTTL(time.Hour), WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer func() { _ = s.Close() }()

	// Insert > ttlPruneBatch (6,200) expired occurrence rows + 5 live rows to verify batched loop deletes all expired rows cleanly (#658).
	expiredTS := now.Add(-2 * time.Hour).UnixNano()
	liveTS := now.Add(-10 * time.Minute).UnixNano()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	for i := 0; i < ttlPruneBatch+1200; i++ {
		if _, err := tx.ExecContext(ctx, `INSERT INTO occurrences (emitted_at, kind, source, severity, fingerprint, route, raw) VALUES (?, 'k8s-event', 'sentinel', 'warning', 'fp', 'stored', '{}')`, expiredTS); err != nil {
			t.Fatalf("insert expired row %d: %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		if _, err := tx.ExecContext(ctx, `INSERT INTO occurrences (emitted_at, kind, source, severity, fingerprint, route, raw) VALUES (?, 'k8s-event', 'sentinel', 'warning', 'fp', 'stored', '{}')`, liveTS); err != nil {
			t.Fatalf("insert live row %d: %v", i, err)
		}
	}
	// Seed 2 snapshots (1 older snapshot + 1 newest protected snapshot) and 1 graph_change for sizePruneStep ordering test.
	if _, err := tx.ExecContext(ctx, `INSERT INTO graph_snapshots (taken_at, resource_version, format_version, size_bytes, data) VALUES (?, 1, 2, 4, X'01020304')`, liveTS-100); err != nil {
		t.Fatalf("insert old snapshot: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO graph_snapshots (taken_at, resource_version, format_version, size_bytes, data) VALUES (?, 2, 2, 4, X'01020304')`, liveTS); err != nil {
		t.Fatalf("insert newest snapshot: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO graph_changes (at, generation, op, kind) VALUES (?, 2, 'Add', 'Pod')`, liveTS); err != nil {
		t.Fatalf("insert graph change: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	stats, err := s.PruneOnce(ctx)
	if err != nil {
		t.Fatalf("PruneOnce: %v", err)
	}
	if stats.TTLRows != ttlPruneBatch+1200 {
		t.Fatalf("TTLRows pruned = %d, want %d", stats.TTLRows, ttlPruneBatch+1200)
	}

	// Drain remaining 5 occurrence rows first in sizePruneStep, then verify old graph_snapshot is pruned BEFORE graph_changes (#658).
	if n, what, err := s.sizePruneStep(ctx); err != nil || what != "occurrence" || n != 5 {
		t.Fatalf("step 1 = (%d, %q, %v), want (5, occurrence, nil)", n, what, err)
	}
	if n, what, err := s.sizePruneStep(ctx); err != nil || what != "graph snapshot" || n != 1 {
		t.Fatalf("step 2 = (%d, %q, %v), want (1, graph snapshot, nil) — snapshots must prune before changes", n, what, err)
	}
	if n, what, err := s.sizePruneStep(ctx); err != nil || what != "graph change" || n != 1 {
		t.Fatalf("step 3 = (%d, %q, %v), want (1, graph change, nil)", n, what, err)
	}
}
