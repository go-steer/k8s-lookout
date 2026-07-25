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
	"iter"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// The query surface is deliberately minimal — exactly what the M3
// consumers (storm-correlation lookback, §7.4 stability windows,
// digests) need; it grows with them, not ahead of them. Every method
// is nil-safe: the disabled store answers "nothing stored".

// occurrenceColumns is the SELECT list every reader shares, matching
// scanOccurrence field-for-field.
const occurrenceColumns = `id, emitted_at, kind, source, severity, fingerprint, route,
	cluster, namespace, kind_of_object, name, uid,
	reason, canonical_reason, message, count,
	first_seen, last_seen, session_id, storm_fingerprint, forecast_eta, raw`

// RecentByFingerprint returns occurrences of one incident class
// (§8 fingerprint) emitted at or after since, newest first — the
// storm-correlation lookback and digest join.
func (s *Store) RecentByFingerprint(ctx context.Context, fingerprint string, since time.Time) ([]Occurrence, error) {
	if s == nil {
		return nil, nil
	}
	return s.selectOccurrences(ctx, `SELECT `+occurrenceColumns+` FROM occurrences
		WHERE fingerprint = ? AND emitted_at >= ?
		ORDER BY emitted_at DESC, id DESC`, fingerprint, since.UTC().UnixNano())
}

// RecentByObject returns occurrences for one object UID emitted at or
// after since, newest first — the §7.4 resolved-stability-window and
// per-object history view.
func (s *Store) RecentByObject(ctx context.Context, uid string, since time.Time) ([]Occurrence, error) {
	if s == nil {
		return nil, nil
	}
	return s.selectOccurrences(ctx, `SELECT `+occurrenceColumns+` FROM occurrences
		WHERE uid = ? AND emitted_at >= ?
		ORDER BY emitted_at DESC, id DESC`, uid, since.UTC().UnixNano())
}

// CountsBySeverity returns the per-severity occurrence counts since
// the given time — the digest headline numbers.
func (s *Store) CountsBySeverity(ctx context.Context, since time.Time) (map[engine.Severity]int64, error) {
	if s == nil {
		return map[engine.Severity]int64{}, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT severity, COUNT(*) FROM occurrences
		WHERE emitted_at >= ? GROUP BY severity`, since.UTC().UnixNano())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make(map[engine.Severity]int64)
	for rows.Next() {
		var sev string
		var n int64
		if err := rows.Scan(&sev, &n); err != nil {
			return nil, err
		}
		out[engine.Severity(sev)] = n
	}
	return out, rows.Err()
}

// Recent iterates occurrences emitted at or after since, newest
// first, up to limit rows (limit <= 0 means no limit) — the digest
// walk. Iteration stops early when the yield func returns false; a
// query or scan failure surfaces as the final pair's non-nil error.
func (s *Store) Recent(ctx context.Context, since time.Time, limit int) iter.Seq2[Occurrence, error] {
	return func(yield func(Occurrence, error) bool) {
		if s == nil {
			return
		}
		if limit <= 0 {
			limit = -1 // SQLite: negative LIMIT = unlimited
		}
		rows, err := s.db.QueryContext(ctx, `SELECT `+occurrenceColumns+` FROM occurrences
			WHERE emitted_at >= ?
			ORDER BY emitted_at DESC, id DESC LIMIT ?`, since.UTC().UnixNano(), limit)
		if err != nil {
			yield(Occurrence{}, err)
			return
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			occ, err := scanOccurrence(rows)
			if err != nil {
				yield(Occurrence{}, err)
				return
			}
			if !yield(occ, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(Occurrence{}, err)
		}
	}
}

func (s *Store) selectOccurrences(ctx context.Context, query string, args ...any) ([]Occurrence, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Occurrence
	for rows.Next() {
		occ, err := scanOccurrence(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, occ)
	}
	return out, rows.Err()
}

func scanOccurrence(rows *sql.Rows) (Occurrence, error) {
	var (
		occ                 Occurrence
		emittedAt           int64
		severity, route     string
		firstSeen, lastSeen sql.NullInt64
		sessionID, stormFP  sql.NullString
		forecastETA         sql.NullInt64
	)
	if err := rows.Scan(
		&occ.ID, &emittedAt, &occ.Kind, &occ.Source, &severity, &occ.Fingerprint, &route,
		&occ.Cluster, &occ.Namespace, &occ.KindOfObject, &occ.Name, &occ.UID,
		&occ.Reason, &occ.CanonicalReason, &occ.Message, &occ.Count,
		&firstSeen, &lastSeen, &sessionID, &stormFP, &forecastETA, &occ.Raw,
	); err != nil {
		return Occurrence{}, err
	}
	occ.EmittedAt = time.Unix(0, emittedAt).UTC()
	occ.Severity = engine.Severity(severity)
	occ.Route = RouteOutcome(route)
	if firstSeen.Valid {
		occ.FirstSeen = time.Unix(0, firstSeen.Int64).UTC()
	}
	if lastSeen.Valid {
		occ.LastSeen = time.Unix(0, lastSeen.Int64).UTC()
	}
	occ.SessionID = sessionID.String
	occ.StormFingerprint = stormFP.String
	if forecastETA.Valid {
		eta := time.Unix(0, forecastETA.Int64).UTC()
		occ.ForecastETA = &eta
	}
	return occ, nil
}
