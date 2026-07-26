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

// triageSchemaVersion is the first schema version carrying the
// triage_status table (migration v4). Reads of older stores answer
// "no records"; writes refuse.
const triageSchemaVersion = 4

// UpsertTriageStatus implements memory.TriageWriter on the sentinel
// store: the §9.4 record for (fingerprint, resource_key) is REPLACED
// — it is current state, not a journal. Updated is assigned here.
//
// Writers: the sentinel in-process (the §7.4 recovery flip) and —
// since the M4-drill design change recorded in
// docs/triage-status-write-design.md — incident agents through
// `lookout triage status` (MCP: k8s_triage_status), the §9.4
// producer surface. Both go through this one upsert; WAL +
// busy-timeout absorb the CLI writer next to the resident sentinel.
// Daemon-mediated writes remain deferred until core-agent exposes
// the shared Memory surface this store stands in for (pkg/memory's
// TODO).
func (s *Store) UpsertTriageStatus(ctx context.Context, rec memory.TriageStatusRecord) (memory.TriageStatusRecord, error) {
	if s == nil {
		return memory.TriageStatusRecord{}, errors.New("store: triage-status records need an open store (--store)")
	}
	if s.readOnly {
		return memory.TriageStatusRecord{}, ErrReadOnlyStore
	}
	if err := rec.Validate(); err != nil {
		return memory.TriageStatusRecord{}, err
	}
	rec.Updated = s.clock().UTC()
	_, err := s.db.ExecContext(ctx, `INSERT INTO triage_status (
		fingerprint, resource_key, session, status,
		root_cause_hypothesis, severity_override, action, updated
	) VALUES (?,?,?,?,?,?,?,?)
	ON CONFLICT (fingerprint, resource_key) DO UPDATE SET
		session = excluded.session,
		status = excluded.status,
		root_cause_hypothesis = excluded.root_cause_hypothesis,
		severity_override = excluded.severity_override,
		action = excluded.action,
		updated = excluded.updated`,
		rec.Fingerprint, rec.ResourceKey, rec.Session, string(rec.Status),
		rec.RootCauseHypothesis, rec.SeverityOverride, rec.Action, rec.Updated.UnixNano(),
	)
	if err != nil {
		return memory.TriageStatusRecord{}, fmt.Errorf("store: upsert triage status (%s, %s): %w", rec.Fingerprint, rec.ResourceKey, err)
	}
	return rec, nil
}

// ResolveTriageStatus implements the §9.4 automatic lifecycle: when
// a §7.4 recovery inject says the symptom cleared, the open
// record(s) for the incident flip to resolved and join the §9.3
// corpus. Matching is (fingerprint, resource_key ∈ resourceKeys) —
// the fingerprint alone is class-level and would flip unrelated
// same-class incidents. Flipping an already-resolved (or absent)
// record is a no-op, never an error.
func (s *Store) ResolveTriageStatus(ctx context.Context, fingerprint string, resourceKeys ...string) (int, error) {
	if s == nil {
		return 0, nil
	}
	if s.readOnly {
		return 0, ErrReadOnlyStore
	}
	if fingerprint == "" || len(resourceKeys) == 0 {
		return 0, nil
	}
	now := s.clock().UTC().UnixNano()
	flipped := 0
	// One statement per key keeps the SQL static (no IN-list
	// assembly); the list is 1–2 entries (object key + controller
	// key).
	for _, key := range resourceKeys {
		if key == "" {
			continue
		}
		res, err := s.db.ExecContext(ctx, `UPDATE triage_status
			SET status = ?, updated = ?
			WHERE fingerprint = ? AND resource_key = ? AND status != ?`,
			string(memory.StatusResolved), now, fingerprint, key, string(memory.StatusResolved))
		if err != nil {
			return flipped, fmt.Errorf("store: resolve triage status (%s, %s): %w", fingerprint, key, err)
		}
		if n, err := res.RowsAffected(); err == nil {
			flipped += int(n)
		}
	}
	return flipped, nil
}

// TriageStatuses implements memory.TriageReader. Nil-safe and
// version-gated like Facts: disabled stores and pre-v4 stores answer
// "no records".
func (s *Store) TriageStatuses(ctx context.Context, q memory.TriageQuery) ([]memory.TriageStatusRecord, error) {
	if s == nil || s.schemaVersion < triageSchemaVersion {
		return nil, nil
	}
	// Static SQL, optional filters folded in (zero value disables).
	const query = `SELECT fingerprint, resource_key, session, status,
		root_cause_hypothesis, severity_override, action, updated
		FROM triage_status
		WHERE (?1 = '' OR fingerprint = ?1)
		  AND (?2 = 0 OR status != 'resolved')
		  AND (?3 = 0 OR updated >= ?3)
		ORDER BY updated DESC, fingerprint, resource_key`
	openOnly := 0
	if q.OpenOnly {
		openOnly = 1
	}
	var sinceNs int64
	if !q.UpdatedSince.IsZero() {
		sinceNs = q.UpdatedSince.UTC().UnixNano()
	}
	rows, err := s.db.QueryContext(ctx, query, q.Fingerprint, openOnly, sinceNs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []memory.TriageStatusRecord
	for rows.Next() {
		var (
			rec     memory.TriageStatusRecord
			status  string
			updated int64
		)
		if err := rows.Scan(
			&rec.Fingerprint, &rec.ResourceKey, &rec.Session, &status,
			&rec.RootCauseHypothesis, &rec.SeverityOverride, &rec.Action, &updated,
		); err != nil {
			return nil, err
		}
		rec.Status = memory.TriageStatus(status)
		rec.Updated = time.Unix(0, updated).UTC()
		out = append(out, rec)
		if q.Limit > 0 && len(out) == q.Limit {
			break
		}
	}
	return out, rows.Err()
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
