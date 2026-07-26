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

// Memory records (DESIGN.md §9.2): the sentinel store doubles as the
// in-tree backend for pkg/memory's record types until core-agent
// ships the shared Memory interface — see pkg/memory's package
// comment for the binding decision and the TODO naming what
// core-agent must expose. Distilled facts share the SQLite file but
// NOT the telemetry posture of occurrences: they are low-volume,
// durable records written synchronously (no buffered writer, no
// drop-on-full) and deliberately EXEMPT from the §9.1 TTL/size prune
// — a fact is a memory, not telemetry; PruneOnce never touches
// memory tables.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/memory"
)

// memorySchemaVersion is the first schema version carrying the
// memory_facts table (migration v3). Read-only opens of older
// sentinel stores answer "no facts" on reads and refuse writes.
const memorySchemaVersion = 3

// ErrReadOnlyStore is returned by memory-record writes on OpenRead
// handles (and, like every write, on read-only paths in general):
// memory records are written by the resident sentinel, not by
// one-shot CLI invocations pointed at its store.
var ErrReadOnlyStore = errors.New("store: memory records cannot be written through a read-only open")

// UpsertFact implements memory.FactWriter on the sentinel store:
// insert the fact, or — when a fact with the same (class, scope
// identity) exists — fold the new evidence into it (§9.2 dedupe:
// same pattern updates the window and counts, never duplicates).
// Synchronous on purpose: the distiller runs on a schedule, volume
// is low, and a failed write should surface at the call site.
func (s *Store) UpsertFact(ctx context.Context, fact memory.DistilledFact) (memory.DistilledFact, error) {
	if s == nil {
		return memory.DistilledFact{}, errors.New("store: memory facts need an open store (--store)")
	}
	if s.readOnly {
		return memory.DistilledFact{}, ErrReadOnlyStore
	}
	if err := fact.Validate(); err != nil {
		return memory.DistilledFact{}, err
	}
	now := s.clock().UTC()
	scopeKey := memory.ScopeKey(fact.Scope)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return memory.DistilledFact{}, err
	}
	defer func() { _ = tx.Rollback() }()

	var (
		id          int64
		created     int64
		prevFPsJSON string
	)
	err = tx.QueryRowContext(ctx, `SELECT id, created_at, source_fingerprints
		FROM memory_facts WHERE class = ? AND scope_key = ?`, fact.Class, scopeKey).
		Scan(&id, &created, &prevFPsJSON)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		fact.Created = now
		fact.Updated = now
		fact.SourceFingerprints = mergeFingerprints(nil, fact.SourceFingerprints)
		if _, err := tx.ExecContext(ctx, `INSERT INTO memory_facts (
			class, scope_key, scope, statement,
			window_start, window_end, occurrences, distinct_objects,
			first_seen, last_seen, source_fingerprints, created_at, updated_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			fact.Class, scopeKey, scopeJSON(fact.Scope), fact.Statement,
			fact.WindowStart.UTC().UnixNano(), fact.WindowEnd.UTC().UnixNano(),
			fact.Occurrences, fact.DistinctObjects,
			nullTime(fact.FirstSeen), nullTime(fact.LastSeen),
			fingerprintsJSON(fact.SourceFingerprints), now.UnixNano(), now.UnixNano(),
		); err != nil {
			return memory.DistilledFact{}, fmt.Errorf("store: insert fact %s %s: %w", fact.Class, scopeKey, err)
		}
	case err != nil:
		return memory.DistilledFact{}, err
	default:
		var prev []string
		_ = json.Unmarshal([]byte(prevFPsJSON), &prev)
		fact.SourceFingerprints = mergeFingerprints(prev, fact.SourceFingerprints)
		fact.Created = time.Unix(0, created).UTC()
		fact.Updated = now
		if _, err := tx.ExecContext(ctx, `UPDATE memory_facts SET
			scope = ?, statement = ?,
			window_start = ?, window_end = ?, occurrences = ?, distinct_objects = ?,
			first_seen = ?, last_seen = ?, source_fingerprints = ?, updated_at = ?
			WHERE id = ?`,
			scopeJSON(fact.Scope), fact.Statement,
			fact.WindowStart.UTC().UnixNano(), fact.WindowEnd.UTC().UnixNano(),
			fact.Occurrences, fact.DistinctObjects,
			nullTime(fact.FirstSeen), nullTime(fact.LastSeen),
			fingerprintsJSON(fact.SourceFingerprints), now.UnixNano(), id,
		); err != nil {
			return memory.DistilledFact{}, fmt.Errorf("store: update fact %s %s: %w", fact.Class, scopeKey, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return memory.DistilledFact{}, err
	}
	return fact, nil
}

// Facts implements memory.FactReader. Nil-safe and version-gated: a
// disabled store, or a read-only open of a store written before
// migration v3, answers "no facts" — never "no such table".
func (s *Store) Facts(ctx context.Context, q memory.FactQuery) ([]memory.DistilledFact, error) {
	if s == nil || s.schemaVersion < memorySchemaVersion {
		return nil, nil
	}
	// Static SQL with optional filters folded into the WHERE clause
	// (a zero-valued parameter disables its condition) — no dynamic
	// query assembly.
	const query = `SELECT class, scope, statement,
		window_start, window_end, occurrences, distinct_objects,
		first_seen, last_seen, source_fingerprints, created_at, updated_at
		FROM memory_facts
		WHERE (?1 = '' OR class = ?1)
		  AND (?2 = 0 OR updated_at >= ?2)
		ORDER BY updated_at DESC, id DESC`
	var sinceNs int64
	if !q.UpdatedSince.IsZero() {
		sinceNs = q.UpdatedSince.UTC().UnixNano()
	}
	rows, err := s.db.QueryContext(ctx, query, q.Class, sinceNs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []memory.DistilledFact
	for rows.Next() {
		f, err := scanFact(rows)
		if err != nil {
			return nil, err
		}
		if !scopeContains(f.Scope, q.Scope) {
			continue
		}
		out = append(out, f)
		if q.Limit > 0 && len(out) == q.Limit {
			break
		}
	}
	return out, rows.Err()
}

func scanFact(rows *sql.Rows) (memory.DistilledFact, error) {
	var (
		f                    memory.DistilledFact
		scope, fps           string
		wStart, wEnd         int64
		firstSeen, lastSeen  sql.NullInt64
		createdAt, updatedAt int64
	)
	if err := rows.Scan(
		&f.Class, &scope, &f.Statement,
		&wStart, &wEnd, &f.Occurrences, &f.DistinctObjects,
		&firstSeen, &lastSeen, &fps, &createdAt, &updatedAt,
	); err != nil {
		return memory.DistilledFact{}, err
	}
	if err := json.Unmarshal([]byte(scope), &f.Scope); err != nil {
		return memory.DistilledFact{}, fmt.Errorf("store: fact %s has unreadable scope %q: %w", f.Class, scope, err)
	}
	_ = json.Unmarshal([]byte(fps), &f.SourceFingerprints)
	f.WindowStart = time.Unix(0, wStart).UTC()
	f.WindowEnd = time.Unix(0, wEnd).UTC()
	if firstSeen.Valid {
		f.FirstSeen = time.Unix(0, firstSeen.Int64).UTC()
	}
	if lastSeen.Valid {
		f.LastSeen = time.Unix(0, lastSeen.Int64).UTC()
	}
	f.Created = time.Unix(0, createdAt).UTC()
	f.Updated = time.Unix(0, updatedAt).UTC()
	return f, nil
}

// scopeContains reports whether every filter entry is present in
// scope with an equal value.
func scopeContains(scope, filter map[string]string) bool {
	for k, v := range filter {
		if scope[k] != v {
			return false
		}
	}
	return true
}

// scopeJSON canonicalizes a scope map for storage: same encoding as
// the identity key (sorted keys, empty values dropped) so stored
// bytes are deterministic.
func scopeJSON(scope map[string]string) string { return memory.ScopeKey(scope) }

// fingerprintsJSON encodes the (sorted, deduplicated) fingerprint
// list; mergeFingerprints unions and caps it.
func fingerprintsJSON(fps []string) string {
	if len(fps) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(fps)
	return string(b)
}

func mergeFingerprints(prev, next []string) []string {
	merged := slices.Clone(prev)
	merged = append(merged, next...)
	slices.Sort(merged)
	merged = slices.Compact(merged)
	merged = slices.DeleteFunc(merged, func(s string) bool { return s == "" })
	if len(merged) > memory.MaxSourceFingerprints {
		merged = merged[:memory.MaxSourceFingerprints]
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}
