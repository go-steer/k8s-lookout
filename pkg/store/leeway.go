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
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// leewayAlertSchemaVersion is the first schema version carrying the
// leeway_alert_state table (migration v7). Reads of older stores answer
// "no state", which starts every subject with a fresh dwell — degraded
// but running, which §9.2 requires: "never refuse to start — a missing
// history file should not mean no monitoring". Writes refuse rather than
// silently no-op, because a write that vanishes is exactly the failure
// persistence exists to prevent.
const leewayAlertSchemaVersion = 7

// ErrNoLeewayAlertState is returned when a write is attempted against a
// store too old to carry the table.
var ErrNoLeewayAlertState = errors.New("store: this store predates the leeway alert-state schema (v7) — point --store at a newer file")

// LeewayAlertStates returns one cluster's persisted alert states,
// ordered by subject and topology key so a reconcile pass is
// deterministic.
//
// Nil-safe and version-gated: a disabled store and a pre-v7 store both
// answer "no state", and §9.3 step 6's fourth row — no persisted state
// and still drifting — is a new Pending, so a cold start is correct
// rather than merely tolerable.
func (s *Store) LeewayAlertStates(ctx context.Context, cluster string) ([]leeway.AlertRecord, error) {
	if s == nil || s.schemaVersion < leewayAlertSchemaVersion {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
		cluster, subject_key, topology_key, phase,
		first_seen, since, clear_since, fires, updated_at
		FROM leeway_alert_state WHERE cluster = ?
		ORDER BY subject_key, topology_key`, cluster)
	if err != nil {
		return nil, fmt.Errorf("store: read leeway alert state: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []leeway.AlertRecord
	for rows.Next() {
		rec, err := scanLeewayAlert(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func scanLeewayAlert(rows *sql.Rows) (leeway.AlertRecord, error) {
	var (
		rec                          leeway.AlertRecord
		phase                        string
		firstSeen, since, clearSince sql.NullInt64
		firesJSON                    string
		updatedAt                    int64
	)
	if err := rows.Scan(
		&rec.Cluster, &rec.SubjectKey, &rec.TopologyKey, &phase,
		&firstSeen, &since, &clearSince, &firesJSON, &updatedAt,
	); err != nil {
		return leeway.AlertRecord{}, fmt.Errorf("store: scan leeway alert state: %w", err)
	}
	// An unrecognised phase is not a decode error. It is a row written by
	// a build that knows a state this one does not, and leeway.Reconcile's
	// rule for that is to forget the episode rather than act on a phase it
	// has no transitions for — so the zero phase is deliberately kept and
	// the row is returned, not dropped.
	rec.Phase, _ = leeway.ParseAlertPhase(phase)
	rec.FirstSeenAt = timeOrZero(firstSeen)
	rec.Since = timeOrZero(since)
	rec.ClearSince = timeOrZero(clearSince)
	rec.UpdatedAt = time.Unix(0, updatedAt).UTC()

	var nanos []int64
	if err := json.Unmarshal([]byte(firesJSON), &nanos); err != nil {
		return leeway.AlertRecord{}, fmt.Errorf("store: decode leeway flap history for %q: %w", rec.SubjectKey, err)
	}
	for _, n := range nanos {
		rec.Fires = append(rec.Fires, time.Unix(0, n).UTC())
	}
	return rec, nil
}

// PutLeewayAlertState writes one subject's alert state, replacing any
// previous row for the same (cluster, subject, topology key).
//
// Synchronous and durable, per §9.2: this deliberately does NOT go
// through the buffered writer that occurrences use. An occurrence lost
// to a buffer is one line missing from a history nobody is waiting on; a
// dwell transition lost to a buffer is a 30-minute timer that silently
// restarts, which is the single durability promise §9.1 makes.
//
// The caller supplies UpdatedAt rather than the store stamping it,
// because §9.3 step 7 measures downtime against it and the whole
// reconcile pass must agree on one instant — the same reason Advance
// takes `now` instead of reading the clock. An unset UpdatedAt falls
// back to the store's clock rather than writing a year-1 timestamp that
// would read as a decade of downtime.
func (s *Store) PutLeewayAlertState(ctx context.Context, rec leeway.AlertRecord) error {
	if s == nil {
		return errors.New("store: leeway alert state needs an open store (--store)")
	}
	if s.readOnly {
		return ErrReadOnlyStore
	}
	if s.schemaVersion < leewayAlertSchemaVersion {
		return ErrNoLeewayAlertState
	}
	if rec.SubjectKey == "" || rec.TopologyKey == "" {
		return fmt.Errorf("store: leeway alert state needs a subject key and a topology key, got %q/%q", rec.SubjectKey, rec.TopologyKey)
	}

	updated := rec.UpdatedAt
	if updated.IsZero() {
		updated = s.clock()
	}
	nanos := make([]int64, 0, len(rec.Fires))
	for _, t := range rec.Fires {
		nanos = append(nanos, t.UTC().UnixNano())
	}
	fires, err := json.Marshal(nanos)
	if err != nil {
		return fmt.Errorf("store: encode leeway flap history for %q: %w", rec.SubjectKey, err)
	}

	if _, err := s.db.ExecContext(ctx, `INSERT INTO leeway_alert_state (
		cluster, subject_key, topology_key, phase,
		first_seen, since, clear_since, fires, updated_at
	) VALUES (?,?,?,?,?,?,?,?,?)
	ON CONFLICT (cluster, subject_key, topology_key) DO UPDATE SET
		phase = excluded.phase,
		first_seen = excluded.first_seen,
		since = excluded.since,
		clear_since = excluded.clear_since,
		fires = excluded.fires,
		updated_at = excluded.updated_at`,
		rec.Cluster, rec.SubjectKey, rec.TopologyKey, rec.Phase.String(),
		nullTime(rec.FirstSeenAt), nullTime(rec.Since), nullTime(rec.ClearSince),
		string(fires), updated.UTC().UnixNano(),
	); err != nil {
		return fmt.Errorf("store: write leeway alert state %q: %w", rec.SubjectKey, err)
	}
	return nil
}

// DeleteLeewayAlertState removes one subject's row, which is what ends
// an episode.
//
// Deleting rather than persisting a row in PhaseOK is what keeps the
// table bounded by "how many subjects are drifting right now" instead of
// by how many subjects exist — a fleet-scale cluster has twenty thousand
// subjects and, on a good day, none of them drifting. The flap history
// goes with it, which is correct: flapWindow is an hour and an episode
// that resolved cleanly has, by the time anyone asks again, nothing in
// the window to count.
//
// Deleting a row that is not there is not an error. The caller is saying
// "this subject has no episode", and it already does not.
func (s *Store) DeleteLeewayAlertState(ctx context.Context, cluster, subjectKey, topologyKey string) error {
	if s == nil {
		return errors.New("store: leeway alert state needs an open store (--store)")
	}
	if s.readOnly {
		return ErrReadOnlyStore
	}
	if s.schemaVersion < leewayAlertSchemaVersion {
		return ErrNoLeewayAlertState
	}
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM leeway_alert_state WHERE cluster = ? AND subject_key = ? AND topology_key = ?`,
		cluster, subjectKey, topologyKey,
	); err != nil {
		return fmt.Errorf("store: delete leeway alert state %q: %w", subjectKey, err)
	}
	return nil
}

func timeOrZero(v sql.NullInt64) time.Time {
	if !v.Valid {
		return time.Time{}
	}
	return time.Unix(0, v.Int64).UTC()
}
