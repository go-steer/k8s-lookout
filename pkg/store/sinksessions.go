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
)

// sinkSessionsSchemaVersion is the first schema version carrying the
// sink_sessions table (migration v9).
const sinkSessionsSchemaVersion = 9

// SinkSession returns the incident id a sink last opened for
// (cluster, incidentKey), if any. Nil-safe and version-gated: a disabled
// store, or an older one, answers "never seen", and the sink opens a new
// session as it would without a store.
func (s *Store) SinkSession(ctx context.Context, cluster, incidentKey string) (string, bool, error) {
	if s == nil || s.schemaVersion < sinkSessionsSchemaVersion {
		return "", false, nil
	}
	var id string
	err := s.db.QueryRowContext(ctx,
		`SELECT incident_id FROM sink_sessions WHERE cluster = ? AND incident_key = ?`,
		cluster, incidentKey).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("store: read sink session %q: %w", incidentKey, err)
	}
	return id, true, nil
}

// PutSinkSession records that the sink delivered (cluster, incidentKey)
// to incident id, replacing any earlier id. Synchronous: one row per
// incident open, so a crash right after an open still reopens into it.
// A disabled or read-only store is a silent no-op, because the sink
// keeps its own in-memory copy either way.
func (s *Store) PutSinkSession(ctx context.Context, cluster, incidentKey, id string) error {
	if s == nil || s.readOnly || s.schemaVersion < sinkSessionsSchemaVersion {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO sink_sessions (cluster, incident_key, incident_id, updated_at)
		VALUES (?,?,?,?)
		ON CONFLICT (cluster, incident_key) DO UPDATE SET
			incident_id = excluded.incident_id,
			updated_at = excluded.updated_at`,
		cluster, incidentKey, id, s.clock().UTC().UnixNano())
	if err != nil {
		return fmt.Errorf("store: write sink session %q: %w", incidentKey, err)
	}
	return nil
}
