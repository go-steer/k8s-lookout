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

// Graph history (DESIGN.md §6.6, stored per §9.1): periodic
// compressed snapshots of the topology plus the per-delta change
// log, in the SAME sentinel-local SQLite file as the occurrences —
// one embedded engine, same TTL policy. This is what turns "--at
// 20 minutes ago" into an answer: GraphAt resolves the nearest
// snapshot at or before the requested time and replays the logged
// deltas forward. The change log's FieldChanges summaries double as
// the data source for `triage changes` (GraphChanges) — one
// mechanism, two features.
//
// Write paths differ deliberately:
//
//   - RecordGraphChange rides the SAME buffered single-writer
//     goroutine as occurrences: it is called per applied informer
//     delta (hot path, under the graph writer's mutex) and must
//     never block — a full buffer drops the record loudly, exactly
//     like an occurrence. A dropped change record degrades --at
//     precision inside one snapshot interval, never correctness of
//     later times (the next snapshot re-baselines).
//   - PutGraphSnapshot is synchronous: it runs on the sentinel's
//     snapshot loop every ~5 minutes, and a failed snapshot should
//     be seen (returned) there, not logged from a queue.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/graph"
)

// graphHistorySchemaVersion is the first schema version carrying the
// graph_snapshots + graph_changes tables (migration v2). Read-only
// opens of older sentinel stores answer ErrNoHistory instead of
// "no such table".
const graphHistorySchemaVersion = 2

// ErrNoHistory is returned by GraphAt when the store holds no
// snapshot at or before the requested time — the store predates
// graph history, the sentinel ran without the graph feed, or the
// requested time is older than retention. Callers surface it as
// "answering live-only" (§6.6).
var ErrNoHistory = errors.New("store: no graph history at or before the requested time")

// GraphChange is one row of the §6.6 delta log as returned by
// GraphChanges — the `triage changes` shape: object identity, what
// happened, and the names/hashes/counts summary. The replay effect
// is deliberately not exposed here; it feeds GraphAt only.
type GraphChange struct {
	ID           int64
	At           time.Time
	Generation   uint64
	Op           string // "add" | "update" | "delete"
	Kind         string
	Namespace    string
	Name         string
	UID          string
	FieldChanges []graph.FieldChange
}

// changeRow is the fully materialized graph_changes insert, built on
// the caller's goroutine (RecordGraphChange) so the writer never
// touches the ChangeRecord.
type changeRow struct {
	at         int64
	generation int64
	op         string
	kind       string
	namespace  string
	name       string
	uid        string
	changes    []byte
	effect     []byte
}

// RecordGraphChange enqueues one §6.6 delta-log record. Same
// contract as Record: NEVER blocks, drops loudly on a full buffer
// (OnDrop "buffer_full"), nil-safe no-op when the store is disabled
// or read-only.
func (s *Store) RecordGraphChange(rec graph.ChangeRecord) {
	if s == nil || s.readOnly {
		return
	}
	changes := []byte("[]")
	if len(rec.FieldChanges) > 0 {
		if b, err := json.Marshal(rec.FieldChanges); err == nil {
			changes = b
		}
	}
	r := &changeRow{
		at:         rec.At.UTC().UnixNano(),
		generation: int64(rec.Generation), // #nosec G115 -- swap counter, nowhere near overflow
		op:         rec.Op.String(),
		kind:       rec.Kind.String(),
		namespace:  rec.Namespace,
		name:       rec.Name,
		uid:        rec.UID,
		changes:    changes,
		effect:     rec.Effect,
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return
	}
	select {
	case s.ch <- writeReq{change: r}:
	default:
		s.logf("store: writer buffer full — dropping graph change (%s %s %s/%s); --at precision degrades until the next snapshot re-baselines", r.op, r.kind, r.namespace, r.name)
		s.drop("buffer_full")
	}
}

// PutGraphSnapshot serializes and stores one topology snapshot,
// stamped with the store clock and the snapshot's generation (the
// graph's own monotonic swap counter — there is no such thing as one
// cluster-wide Kubernetes resourceVersion across informers, so the
// generation is the honest replay cursor; the column keeps the §6.6
// name). Synchronous by design (see the package comment above).
// Nil-safe no-op when disabled; an error when read-only.
func (s *Store) PutGraphSnapshot(ctx context.Context, snap *graph.Snapshot) error {
	if s == nil {
		return nil
	}
	if s.readOnly {
		return errors.New("store: read-only open cannot store snapshots")
	}
	data, err := snap.Encode()
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO graph_snapshots
		(taken_at, resource_version, format_version, size_bytes, data)
		VALUES (?,?,?,?,?)`,
		s.clock().UTC().UnixNano(),
		int64(snap.Generation()), // #nosec G115 -- swap counter
		graph.SnapshotFormatVersion,
		len(data),
		data,
	)
	if err != nil {
		return fmt.Errorf("store: put graph snapshot (generation %d): %w", snap.Generation(), err)
	}
	return nil
}

// GraphAt reconstructs the topology as of time at (§6.6): nearest
// snapshot with taken_at <= at, then every logged change with a
// LATER generation and at' <= at replayed forward, in log order. The
// boundary is inclusive on both cursors: a change stamped exactly at
// the requested time is part of the answer, and asking for exactly a
// snapshot's own time returns that snapshot verbatim. Nil-safe:
// a disabled store (and any store without history) answers
// ErrNoHistory.
func (s *Store) GraphAt(ctx context.Context, at time.Time) (*graph.Snapshot, error) {
	if s == nil {
		return nil, ErrNoHistory
	}
	if s.schemaVersion < graphHistorySchemaVersion {
		return nil, fmt.Errorf("%w (store schema v%d predates graph history)", ErrNoHistory, s.schemaVersion)
	}
	var (
		generation    int64
		formatVersion int
		data          []byte
	)
	err := s.db.QueryRowContext(ctx, `SELECT resource_version, format_version, data
		FROM graph_snapshots WHERE taken_at <= ?
		ORDER BY taken_at DESC, id DESC LIMIT 1`, at.UTC().UnixNano()).
		Scan(&generation, &formatVersion, &data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w (asked for %s)", ErrNoHistory, at.UTC().Format(time.RFC3339))
	}
	if err != nil {
		return nil, err
	}
	if formatVersion != graph.SnapshotFormatVersion {
		return nil, fmt.Errorf("store: graph snapshot format v%d not readable by this binary (reads v%d) — written by a different lookout version", formatVersion, graph.SnapshotFormatVersion)
	}
	base, err := graph.Restore(data)
	if err != nil {
		return nil, fmt.Errorf("store: restore graph snapshot (generation %d): %w", generation, err)
	}

	rows, err := s.db.QueryContext(ctx, `SELECT generation, effect FROM graph_changes
		WHERE generation > ? AND at <= ?
		ORDER BY generation ASC, id ASC`, generation, at.UTC().UnixNano())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	r := graph.NewReplayer(base)
	for rows.Next() {
		var gen int64
		var effect []byte
		if err := rows.Scan(&gen, &effect); err != nil {
			return nil, err
		}
		if err := r.Apply(uint64(gen), effect); err != nil { // #nosec G115 -- stored from uint64
			return nil, fmt.Errorf("store: replay graph change (generation %d): %w", gen, err)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return r.Snapshot(), nil
}

// GraphChanges returns the delta-log rows with from < at <= to in
// chronological order — the `triage changes` feed ("what changed in
// the window before onset"). Nil-safe: disabled and pre-history
// stores answer nothing.
func (s *Store) GraphChanges(ctx context.Context, from, to time.Time) ([]GraphChange, error) {
	if s == nil || s.schemaVersion < graphHistorySchemaVersion {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, at, generation, op, kind, namespace, name, uid, changes
		FROM graph_changes WHERE at > ? AND at <= ?
		ORDER BY at ASC, id ASC`, from.UTC().UnixNano(), to.UTC().UnixNano())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []GraphChange
	for rows.Next() {
		var (
			c       GraphChange
			at, gen int64
			changes []byte
		)
		if err := rows.Scan(&c.ID, &at, &gen, &c.Op, &c.Kind, &c.Namespace, &c.Name, &c.UID, &changes); err != nil {
			return nil, err
		}
		c.At = time.Unix(0, at).UTC()
		c.Generation = uint64(gen) // #nosec G115 -- stored from uint64
		if err := json.Unmarshal(changes, &c.FieldChanges); err != nil {
			return nil, fmt.Errorf("store: graph change %d: decode changes: %w", c.ID, err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
