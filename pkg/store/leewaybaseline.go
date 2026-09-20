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
	"math"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// leewayBaselineSchemaVersion is the first schema version carrying the
// leeway_baseline table (migration v8).
//
// Reads of an older store answer "nothing learned", which is §9.2's
// degraded start: Tier A and B proceed on fresh dwells and Tier C is
// disabled until baselines relearn. Writes refuse, for the same reason
// v7's do — a write that vanishes is the failure persistence exists to
// prevent — except that here the refusal is soft in practice, because
// the flusher logs it once and carries on rather than failing the
// source.
const leewayBaselineSchemaVersion = 8

// ErrNoLeewayBaselines is returned when a write is attempted against a
// store too old to carry the table.
var ErrNoLeewayBaselines = errors.New("store: this store predates the leeway baseline schema (v8) — point --store at a newer file")

// LeewayBaselines returns one cluster's persisted baselines, ordered by
// subject and topology key so a load pass is deterministic.
//
// Nil-safe and version-gated, like LeewayAlertStates: a disabled store
// and a pre-v8 store both answer "nothing learned", which §7.5 handles
// natively — an absent baseline is an immature one, and an immature one
// means the subject is scored against an even apportionment exactly as
// it was before Phase 5.
func (s *Store) LeewayBaselines(ctx context.Context, cluster string) ([]leeway.BaselineRecord, error) {
	if s == nil || s.schemaVersion < leewayBaselineSchemaVersion {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
		cluster, subject_key, topology_key, domains,
		dev_weight, samples, first_seen, updated_at
		FROM leeway_baseline WHERE cluster = ?
		ORDER BY subject_key, topology_key`, cluster)
	if err != nil {
		return nil, fmt.Errorf("store: read leeway baselines: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []leeway.BaselineRecord
	for rows.Next() {
		rec, err := scanLeewayBaseline(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func scanLeewayBaseline(rows *sql.Rows) (leeway.BaselineRecord, error) {
	var (
		rec        leeway.BaselineRecord
		domainJSON string
		firstSeen  sql.NullInt64
		samples    int64
		updatedAt  int64
	)
	if err := rows.Scan(
		&rec.Cluster, &rec.SubjectKey, &rec.TopologyKey, &domainJSON,
		&rec.DevWeight, &samples, &firstSeen, &updatedAt,
	); err != nil {
		return leeway.BaselineRecord{}, fmt.Errorf("store: scan leeway baseline: %w", err)
	}
	if err := json.Unmarshal([]byte(domainJSON), &rec.Domains); err != nil {
		return leeway.BaselineRecord{}, fmt.Errorf("store: decode leeway baseline domains for %q: %w", rec.SubjectKey, err)
	}
	// A negative sample count is not representable in the estimator and
	// would wrap to an enormous uint64 — instantly satisfying the maturity
	// gate it exists to hold shut. Reading it as zero makes the record
	// rebuild as "nothing learned", which BaselineRecord.Set already
	// refuses.
	if samples > 0 {
		rec.Samples = uint64(samples)
	}
	rec.FirstSeen = timeOrZero(firstSeen)
	rec.UpdatedAt = time.Unix(0, updatedAt).UTC()
	return rec, nil
}

// PutLeewayBaselines writes a batch of baselines in ONE transaction.
//
// Batched rather than per-record because §9.2 puts baselines and alert
// state on deliberately different write policies: the alert path is
// synchronous because losing a dwell timer is the one thing we promised
// not to do, and this path is batched because losing thirty seconds of
// EWMA out of a twelve-hour half-life is not worth an fsync per subject.
// At fleet scale the difference is twenty thousand transactions per
// flush against one.
//
// The batch is all-or-nothing. A partial flush would leave some subjects
// with a fresh estimate and some with one from the previous interval,
// and since the whole set of a subject's domains lives in one row, the
// only thing a partial write can corrupt is *which* interval a subject
// is from — which is exactly what UpdatedAt is read as. One transaction
// keeps that honest.
//
// An empty batch is not an error; the flusher fires on a ticker and most
// ticks on a settled cluster have nothing to say.
func (s *Store) PutLeewayBaselines(ctx context.Context, recs []leeway.BaselineRecord) error {
	if s == nil {
		return errors.New("store: leeway baselines need an open store (--store)")
	}
	if s.readOnly {
		return ErrReadOnlyStore
	}
	if s.schemaVersion < leewayBaselineSchemaVersion {
		return ErrNoLeewayBaselines
	}
	if len(recs) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin leeway baseline batch: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `INSERT INTO leeway_baseline (
		cluster, subject_key, topology_key, domains,
		dev_weight, samples, first_seen, updated_at
	) VALUES (?,?,?,?,?,?,?,?)
	ON CONFLICT (cluster, subject_key, topology_key) DO UPDATE SET
		domains = excluded.domains,
		dev_weight = excluded.dev_weight,
		samples = excluded.samples,
		first_seen = excluded.first_seen,
		updated_at = excluded.updated_at`)
	if err != nil {
		return fmt.Errorf("store: prepare leeway baseline write: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, rec := range recs {
		if rec.SubjectKey == "" || rec.TopologyKey == "" {
			return fmt.Errorf("store: leeway baseline needs a subject key and a topology key, got %q/%q", rec.SubjectKey, rec.TopologyKey)
		}
		domains, err := json.Marshal(rec.Domains)
		if err != nil {
			return fmt.Errorf("store: encode leeway baseline domains for %q: %w", rec.SubjectKey, err)
		}
		updated := rec.UpdatedAt
		if updated.IsZero() {
			updated = s.clock()
		}
		// SQLite integers are signed, and the scan side reads a negative
		// count as zero — so a count past MaxInt64 would round-trip as
		// "nothing learned" and silently discard a converged baseline.
		// Saturating keeps it mature. Reaching this needs a sample every
		// nanosecond for nearly three centuries; the clamp is here so the
		// conversion has an answer, not because the number is plausible.
		samples := rec.Samples
		if samples > math.MaxInt64 {
			samples = math.MaxInt64
		}
		if _, err := stmt.ExecContext(ctx,
			rec.Cluster, rec.SubjectKey, rec.TopologyKey, string(domains),
			rec.DevWeight, int64(samples), nullTime(rec.FirstSeen), updated.UTC().UnixNano(),
		); err != nil {
			return fmt.Errorf("store: write leeway baseline %q: %w", rec.SubjectKey, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit leeway baseline batch: %w", err)
	}
	return nil
}

// DeleteLeewayBaseline removes one subject's learned normal, which is
// what a subject going away means: the next Deployment to carry that
// name is a different workload, and inheriting its predecessor's
// placement history would score it against something it never did.
//
// Deleting a row that is not there is not an error.
func (s *Store) DeleteLeewayBaseline(ctx context.Context, cluster, subjectKey, topologyKey string) error {
	if s == nil {
		return errors.New("store: leeway baselines need an open store (--store)")
	}
	if s.readOnly {
		return ErrReadOnlyStore
	}
	if s.schemaVersion < leewayBaselineSchemaVersion {
		return ErrNoLeewayBaselines
	}
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM leeway_baseline WHERE cluster = ? AND subject_key = ? AND topology_key = ?`,
		cluster, subjectKey, topologyKey,
	); err != nil {
		return fmt.Errorf("store: delete leeway baseline %q: %w", subjectKey, err)
	}
	return nil
}

// PruneLeewayBaselines deletes rows untouched since `before`, and
// reports how many went.
//
// This table needs a prune where leeway_alert_state does not. An alert
// row is deleted when its episode resolves, so the table is bounded by
// how many subjects are drifting right now; a baseline row exists for
// every subject ever sampled, and nothing deletes it when the subject
// quietly stops existing — a namespace torn down between restarts
// leaves no event to act on.
//
// A row older than §9.3 step 7's stale window costs nothing to discard:
// loading it restarts the maturity clock anyway, so the only thing
// keeping it buys is a seed for shares that, by construction, described
// a cluster more than a day ago.
func (s *Store) PruneLeewayBaselines(ctx context.Context, cluster string, before time.Time) (int64, error) {
	if s == nil {
		return 0, errors.New("store: leeway baselines need an open store (--store)")
	}
	if s.readOnly {
		return 0, ErrReadOnlyStore
	}
	if s.schemaVersion < leewayBaselineSchemaVersion {
		return 0, ErrNoLeewayBaselines
	}
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM leeway_baseline WHERE cluster = ? AND updated_at < ?`,
		cluster, before.UTC().UnixNano(),
	)
	if err != nil {
		return 0, fmt.Errorf("store: prune leeway baselines: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: prune leeway baselines: %w", err)
	}
	return n, nil
}
