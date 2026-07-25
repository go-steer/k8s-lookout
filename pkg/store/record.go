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
	"database/sql"
	"encoding/json"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// RouteOutcome names what the dispatcher DID with a signal — the
// §7.7 routing decision, recorded next to the signal so lookback
// queries distinguish "we paged on this" from "we noted it". Values
// are persisted; append-only.
type RouteOutcome string

const (
	// RouteInjected: a per-incident session inject (per-incident
	// mode's critical path, or ANY severity in shared mode).
	RouteInjected RouteOutcome = "injected"
	// RouteSuppressed: a dedup-window duplicate; no inject fired.
	RouteSuppressed RouteOutcome = "suppressed"
	// RouteStorm: this signal's arrival formed a §7.5 storm — the
	// one aggregate storm session was opened for it.
	RouteStorm RouteOutcome = "storm"
	// RouteStormMember: folded into an existing storm (late-arrival
	// attach); no session of its own.
	RouteStormMember RouteOutcome = "storm-member"
	// RouteWatchboard: batched onto the shared watchboard digest.
	RouteWatchboard RouteOutcome = "watchboard"
	// RouteInfoStored: the §7.7 info class — stored only, no inject
	// anywhere. This row IS the signal's entire footprint.
	RouteInfoStored RouteOutcome = "info-stored"
	// RouteResolved: a §7.4 outcome record (kind=resolved /
	// resolved.reverted) routed into its incident's session — kept in
	// the store because resolved rows are what the stability-window
	// and recommendation-history queries join against.
	RouteResolved RouteOutcome = "resolved"
)

// Outcome is the routing verdict recorded with a signal.
type Outcome struct {
	Route RouteOutcome
	// SessionID is the session the signal routed to, when one is
	// known ("" → NULL: dropped bindings, watchboard buffering before
	// its lazy session exists, dry-run).
	SessionID string
	// StormFingerprint is the owning storm's fingerprint for
	// storm/storm-member outcomes ("" → NULL otherwise).
	StormFingerprint string
}

// Occurrence is one stored row, as returned by the query surface.
// Nullable columns come back as Go zero values (empty string, zero
// time, nil ForecastETA).
type Occurrence struct {
	ID               int64
	EmittedAt        time.Time
	Kind             string
	Source           string
	Severity         engine.Severity
	Fingerprint      string
	Route            RouteOutcome
	Cluster          string
	Namespace        string
	KindOfObject     string
	Name             string
	UID              string
	Reason           string
	CanonicalReason  string
	Message          string
	Count            int
	FirstSeen        time.Time
	LastSeen         time.Time
	SessionID        string
	StormFingerprint string
	ForecastETA      *time.Time
	// Raw is the compact JSON of the emitted engine.Signal (message
	// capped, enrichment stripped — see newRow), kept for later
	// distillation (§9.2).
	Raw []byte
}

// row is the fully-materialized insert, built ON THE CALLER'S
// goroutine in Record so the writer never touches the Signal (its
// Labels map may be mutated by informer callbacks after dispatch).
type row struct {
	emittedAt   int64
	kind        string
	source      string
	severity    string
	fingerprint string
	route       string
	cluster     string
	namespace   string
	kindOfObj   string
	name        string
	uid         string
	reason      string
	canonical   string
	message     string
	count       int
	firstSeen   sql.NullInt64
	lastSeen    sql.NullInt64
	sessionID   sql.NullString
	stormFP     sql.NullString
	forecastETA sql.NullInt64
	raw         []byte
}

// writeReq is one writer-queue element: an occurrence row or a §6.6
// graph-change row to insert, or a Flush barrier (both nil) the
// writer acknowledges after committing everything queued before it.
type writeReq struct {
	row     *row
	change  *changeRow
	barrier chan struct{}
}

// Record enqueues sig with its routing outcome. NEVER blocks and
// never returns an error: on a full buffer the record is dropped with
// the OnDrop("buffer_full") hook and a loud log — the store is
// telemetry, and the inject pipeline must not stall on it (§9.1).
// Nil-safe: a disabled store discards silently.
func (s *Store) Record(sig engine.Signal, out Outcome) {
	if s == nil || s.readOnly {
		return
	}
	r := newRow(sig, out, s.clock())
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return
	}
	select {
	case s.ch <- writeReq{row: r}:
	default:
		s.logf("store: writer buffer full — dropping %s occurrence (kind=%s, %s/%s); telemetry only, the signal itself already routed", out.Route, sig.Kind, sig.Namespace, sig.Name)
		s.drop("buffer_full")
	}
}

// newRow flattens a Signal + Outcome into the insert row. The message
// is byte-capped at MaxMessageBytes; the raw blob is the compact JSON
// of the Signal with the SAME cap applied and the §7.6 enrichment
// bundle stripped — the bundle is reproducible from the cluster, up
// to 16 KiB per signal, and would dominate 30 days of retention for
// zero distillation value.
func newRow(sig engine.Signal, out Outcome, now time.Time) *row {
	if len(sig.Message) > MaxMessageBytes {
		sig.Message = sig.Message[:MaxMessageBytes]
	}
	sig.Enrichment = nil
	raw, err := json.Marshal(sig)
	if err != nil {
		// Signal is a plain struct of encodable fields; this cannot
		// fail today. Keep the row anyway — the columns still carry
		// the incident — with the error noted in the blob.
		raw = []byte(`{"marshal_error":true}`)
	}
	r := &row{
		emittedAt:   now.UTC().UnixNano(),
		kind:        sig.Kind,
		source:      sig.Source,
		severity:    string(sig.Severity),
		fingerprint: sig.Fingerprint,
		route:       string(out.Route),
		cluster:     sig.Cluster,
		namespace:   sig.Namespace,
		kindOfObj:   sig.KindOfObject,
		name:        sig.Name,
		uid:         sig.Key.UID,
		reason:      sig.Key.Reason,
		canonical:   engine.CanonicalReason(sig.Key.Reason),
		message:     sig.Message,
		count:       sig.Count,
		firstSeen:   nullTime(sig.FirstSeen),
		lastSeen:    nullTime(sig.LastSeen),
		sessionID:   nullString(out.SessionID),
		stormFP:     nullString(out.StormFingerprint),
		raw:         raw,
	}
	if sig.Forecast != nil && !sig.Forecast.ETA.IsZero() {
		r.forecastETA = sql.NullInt64{Int64: sig.Forecast.ETA.UTC().UnixNano(), Valid: true}
	}
	return r
}

// writer is the single writer goroutine: it drains the queue in
// batches (one transaction each) so a burst of signals costs one
// fsync, and acknowledges Flush barriers after the batch that
// precedes them commits.
func (s *Store) writer() {
	defer close(s.done)
	const maxBatch = 128
	for req := range s.ch {
		batch := make([]*row, 0, maxBatch)
		var changes []*changeRow
		var barriers []chan struct{}
		take := func(r writeReq) {
			switch {
			case r.row != nil:
				batch = append(batch, r.row)
			case r.change != nil:
				changes = append(changes, r.change)
			case r.barrier != nil:
				barriers = append(barriers, r.barrier)
			}
		}
		take(req)
	drain:
		for len(batch)+len(changes) < maxBatch {
			select {
			case more, ok := <-s.ch:
				if !ok {
					break drain
				}
				take(more)
			default:
				break drain
			}
		}
		s.insert(batch, changes)
		for _, b := range barriers {
			close(b)
		}
	}
}

// insert commits one batch of occurrence + graph-change rows in one
// transaction. A failed batch is LOST (telemetry, not system of
// record): logged loudly and counted via OnDrop("write_error") per
// record.
func (s *Store) insert(batch []*row, changes []*changeRow) {
	if len(batch) == 0 && len(changes) == 0 {
		return
	}
	err := func() error {
		tx, err := s.db.Begin()
		if err != nil {
			return err
		}
		if len(batch) > 0 {
			stmt, err := tx.Prepare(`INSERT INTO occurrences (
				emitted_at, kind, source, severity, fingerprint, route,
				cluster, namespace, kind_of_object, name, uid,
				reason, canonical_reason, message, count,
				first_seen, last_seen, session_id, storm_fingerprint,
				forecast_eta, raw
			) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			for _, r := range batch {
				if _, err := stmt.Exec(
					r.emittedAt, r.kind, r.source, r.severity, r.fingerprint, r.route,
					r.cluster, r.namespace, r.kindOfObj, r.name, r.uid,
					r.reason, r.canonical, r.message, r.count,
					r.firstSeen, r.lastSeen, r.sessionID, r.stormFP,
					r.forecastETA, r.raw,
				); err != nil {
					_ = stmt.Close()
					_ = tx.Rollback()
					return err
				}
			}
			_ = stmt.Close()
		}
		if len(changes) > 0 {
			stmt, err := tx.Prepare(`INSERT INTO graph_changes (
				at, generation, op, kind, namespace, name, uid, changes, effect
			) VALUES (?,?,?,?,?,?,?,?,?)`)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			for _, c := range changes {
				if _, err := stmt.Exec(
					c.at, c.generation, c.op, c.kind, c.namespace, c.name, c.uid, c.changes, c.effect,
				); err != nil {
					_ = stmt.Close()
					_ = tx.Rollback()
					return err
				}
			}
			_ = stmt.Close()
		}
		return tx.Commit()
	}()
	if err != nil {
		s.logf("store: batch insert of %d occurrence(s) + %d graph change(s) failed — records lost (telemetry only): %v", len(batch), len(changes), err)
		for range len(batch) + len(changes) {
			s.drop("write_error")
		}
		return
	}
	if s.hooks.OnWrite != nil {
		for _, r := range batch {
			s.hooks.OnWrite(RouteOutcome(r.route))
		}
	}
}

func (s *Store) drop(cause string) {
	if s.hooks.OnDrop != nil {
		s.hooks.OnDrop(cause)
	}
}

func nullTime(t time.Time) sql.NullInt64 {
	if t.IsZero() {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: t.UTC().UnixNano(), Valid: true}
}

func nullString(v string) sql.NullString {
	if v == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: v, Valid: true}
}
