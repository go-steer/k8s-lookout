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
	"fmt"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/findings"
)

// findingStateSchemaVersion is the first schema version carrying the
// finding_state table (migration v6). Reads of older stores answer "no
// state" — which makes the first diff against an upgraded store report
// everything as `new`, the correct and only honest answer; writes
// refuse rather than silently no-op.
const findingStateSchemaVersion = 6

// ErrNoFindingState is returned when a write is attempted against a
// store too old to carry the table.
var ErrNoFindingState = errors.New("store: this store predates the finding-state schema (v6) — run a diff to create it, or point --store at a newer file")

// FindingStates returns the open subject rows for one cluster,
// ordered by subject key so a diff is deterministic without the caller
// re-sorting.
//
// The cluster scope is not a convenience filter — it is what lets two
// clusters share one store file without each diff run reporting the
// other's subjects as resolved. The empty label is a cluster like any
// other (the single-cluster default) and scopes to itself.
//
// Nil-safe and version-gated like TriageStatuses: a disabled store and
// a pre-v6 store both answer "no state".
func (s *Store) FindingStates(ctx context.Context, cluster string) ([]findings.State, error) {
	if s == nil || s.schemaVersion < findingStateSchemaVersion {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
		subject_key, fingerprint, cluster, namespace, kind_of_object,
		name, reason, severity, first_seen, last_seen, ack_until, ack_by
		FROM finding_state WHERE cluster = ? ORDER BY subject_key`, cluster)
	if err != nil {
		return nil, fmt.Errorf("store: read finding state: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []findings.State
	for rows.Next() {
		var (
			st                  findings.State
			firstSeen, lastSeen int64
			ackUntil            sql.NullInt64
		)
		if err := rows.Scan(
			&st.SubjectKey, &st.Fingerprint, &st.Cluster, &st.Namespace, &st.KindOfObject,
			&st.Name, &st.Reason, &st.Severity, &firstSeen, &lastSeen, &ackUntil, &st.AckBy,
		); err != nil {
			return nil, fmt.Errorf("store: scan finding state: %w", err)
		}
		st.FirstSeen = time.Unix(0, firstSeen).UTC()
		st.LastSeen = time.Unix(0, lastSeen).UTC()
		if ackUntil.Valid {
			st.AckUntil = time.Unix(0, ackUntil.Int64).UTC()
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// ReplaceFindingStates atomically swaps one cluster's open-subject set
// for next — the durable half of one findings.Diff run.
//
// Whole-set replacement rather than per-row upsert+delete because the
// diff's Next IS the complete new state for that cluster by
// construction, and the two halves must land together: a crash between
// "insert the new rows" and "delete the resolved ones" would leave
// resolved subjects behind, and the next run would report them ongoing
// forever. One transaction makes that unrepresentable.
//
// The swap is CLUSTER-SCOPED for the same reason FindingStates' read
// is: an unscoped rewrite would let a second cluster sharing the store
// silently resolve the first's findings. Rows in next that carry a
// different cluster are refused rather than written somewhere they
// would then be invisible.
//
// Volume is "how many things are broken right now" — tens, not
// millions — so the cost of rewriting the rows is irrelevant next to
// the correctness it buys.
func (s *Store) ReplaceFindingStates(ctx context.Context, cluster string, next []findings.State) error {
	if s == nil {
		return errors.New("store: finding state needs an open store (--store)")
	}
	if s.readOnly {
		return ErrReadOnlyStore
	}
	if s.schemaVersion < findingStateSchemaVersion {
		return ErrNoFindingState
	}
	for _, st := range next {
		if st.Cluster != cluster {
			return fmt.Errorf("store: finding state %q belongs to cluster %q, not %q — one swap covers one cluster", st.SubjectKey, st.Cluster, cluster)
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin finding-state swap: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op after a successful Commit

	if _, err := tx.ExecContext(ctx, `DELETE FROM finding_state WHERE cluster = ?`, cluster); err != nil {
		return fmt.Errorf("store: clear finding state: %w", err)
	}
	if len(next) > 0 {
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO finding_state (
			subject_key, fingerprint, cluster, namespace, kind_of_object,
			name, reason, severity, first_seen, last_seen, ack_until, ack_by
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`)
		if err != nil {
			return fmt.Errorf("store: prepare finding-state insert: %w", err)
		}
		defer func() { _ = stmt.Close() }()
		for _, st := range next {
			if _, err := stmt.ExecContext(ctx,
				st.SubjectKey, st.Fingerprint, st.Cluster, st.Namespace, st.KindOfObject,
				st.Name, st.Reason, st.Severity,
				st.FirstSeen.UTC().UnixNano(), st.LastSeen.UTC().UnixNano(),
				nullTime(st.AckUntil), st.AckBy,
			); err != nil {
				return fmt.Errorf("store: insert finding state %q: %w", st.SubjectKey, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit finding-state swap: %w", err)
	}
	return nil
}

// AckFinding opens an ack window on one subject, returning the updated
// row.
//
// The subject must already be in the state table. Acking an unknown
// subject is an ERROR rather than a row-creating upsert, for two
// reasons: an operator acking a finding is always acking one they were
// just shown, so an unknown key is a typo or a stale digest and saying
// so is more useful than silently accepting it; and a row created by
// an ack alone would be absent from the next report, be classified
// `resolved` on the spot, and be deleted — an ack that evaporated one
// run after it was taken, with no error anywhere.
//
// until is absolute; the caller resolves `--for 4h` against its own
// clock so the CLI, the store, and the diff all agree on one instant.
func (s *Store) AckFinding(ctx context.Context, subjectKey, by string, until time.Time) (findings.State, error) {
	if s == nil {
		return findings.State{}, errors.New("store: acks need an open store (--store)")
	}
	if s.readOnly {
		return findings.State{}, ErrReadOnlyStore
	}
	if s.schemaVersion < findingStateSchemaVersion {
		return findings.State{}, ErrNoFindingState
	}
	if subjectKey == "" {
		return findings.State{}, errors.New("store: ack needs a subject key")
	}
	if until.IsZero() {
		return findings.State{}, errors.New("store: ack needs an expiry")
	}

	res, err := s.db.ExecContext(ctx, `UPDATE finding_state
		SET ack_until = ?, ack_by = ? WHERE subject_key = ?`,
		until.UTC().UnixNano(), by, subjectKey)
	if err != nil {
		return findings.State{}, fmt.Errorf("store: ack %q: %w", subjectKey, err)
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return findings.State{}, fmt.Errorf("store: no open finding with subject key %q — acks name a subject from a diff, and a resolved subject's row is gone", subjectKey)
	}
	return s.findingState(ctx, subjectKey)
}

// UnackFinding clears any ack window on one subject, returning the
// updated row. Idempotent on an un-acked subject; an unknown subject
// is an error for the same reason AckFinding's is.
func (s *Store) UnackFinding(ctx context.Context, subjectKey string) (findings.State, error) {
	if s == nil {
		return findings.State{}, errors.New("store: acks need an open store (--store)")
	}
	if s.readOnly {
		return findings.State{}, ErrReadOnlyStore
	}
	if s.schemaVersion < findingStateSchemaVersion {
		return findings.State{}, ErrNoFindingState
	}
	res, err := s.db.ExecContext(ctx, `UPDATE finding_state
		SET ack_until = NULL, ack_by = '' WHERE subject_key = ?`, subjectKey)
	if err != nil {
		return findings.State{}, fmt.Errorf("store: unack %q: %w", subjectKey, err)
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return findings.State{}, fmt.Errorf("store: no open finding with subject key %q", subjectKey)
	}
	return s.findingState(ctx, subjectKey)
}

// findingState reads one row back after a write.
func (s *Store) findingState(ctx context.Context, subjectKey string) (findings.State, error) {
	var (
		st                  findings.State
		firstSeen, lastSeen int64
		ackUntil            sql.NullInt64
	)
	err := s.db.QueryRowContext(ctx, `SELECT
		subject_key, fingerprint, cluster, namespace, kind_of_object,
		name, reason, severity, first_seen, last_seen, ack_until, ack_by
		FROM finding_state WHERE subject_key = ?`, subjectKey).Scan(
		&st.SubjectKey, &st.Fingerprint, &st.Cluster, &st.Namespace, &st.KindOfObject,
		&st.Name, &st.Reason, &st.Severity, &firstSeen, &lastSeen, &ackUntil, &st.AckBy,
	)
	if err != nil {
		return findings.State{}, fmt.Errorf("store: read back finding state %q: %w", subjectKey, err)
	}
	st.FirstSeen = time.Unix(0, firstSeen).UTC()
	st.LastSeen = time.Unix(0, lastSeen).UTC()
	if ackUntil.Valid {
		st.AckUntil = time.Unix(0, ackUntil.Int64).UTC()
	}
	return st, nil
}
