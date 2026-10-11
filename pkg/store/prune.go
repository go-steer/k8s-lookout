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
	"time"
)

// sizePruneBatch is how many oldest rows one size-prune iteration
// deletes before re-measuring. Small enough that we never overshoot
// the bound by much, large enough that a badly oversized store
// converges in few transactions.
const sizePruneBatch = 512

// ttlPruneBatch caps how many expired rows a single TTL delete statement
// removes per transaction so SQLite write locks are never held across tens
// of thousands of rows at once (#658).
const ttlPruneBatch = 5000

// PruneStats reports one prune pass. Rows removed by the TTL cutoff
// and by the size bound are counted separately — they are different
// operator signals (retention working as designed vs. the store
// outgrowing its budget).
type PruneStats struct {
	TTLRows  int64
	SizeRows int64
}

// PruneInterval is the loop cadence for a given TTL:
// min(1h, ttl/24). A 30-day TTL prunes hourly; a short test TTL
// prunes often enough that expiry is visible within the window.
func PruneInterval(ttl time.Duration) time.Duration {
	if iv := ttl / 24; iv < time.Hour {
		return iv
	}
	return time.Hour
}

// RunPrune runs PruneOnce on the PruneInterval cadence until ctx is
// cancelled. Nil-safe: a disabled store returns immediately. Run it
// in a goroutine; errors are logged (loudly — a failing prune means
// the bound is not being enforced) and the loop keeps going.
func (s *Store) RunPrune(ctx context.Context) {
	if s == nil || s.readOnly {
		return
	}
	t := time.NewTicker(PruneInterval(s.ttl))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := s.PruneOnce(ctx); err != nil {
				s.logf("store: prune failed (TTL/size bounds NOT enforced this cycle): %v", err)
			}
		}
	}
}

// PruneOnce enforces both bounds now, over ALL bounded tables
// (occurrences, graph_snapshots, graph_changes — §9.1: one store,
// one TTL policy): first the TTL cutoff in bounded batches, then the
// size bound (delete oldest rows in batches until the database's used
// pages fit the budget). graph_snapshots are pruned BEFORE graph_changes
// so retained older snapshots never lose their delta stream. The NEWEST
// graph snapshot is exempt from both bounds: with zero snapshots every
// logged change is unreplayable, so a sentinel quiet for longer than
// the TTL keeps exactly one baseline. Deletions are loud: logged and
// reported via OnPrune. Nil-safe; no-op on read-only opens.
func (s *Store) PruneOnce(ctx context.Context) (PruneStats, error) {
	if s == nil || s.readOnly {
		return PruneStats{}, nil
	}
	var stats PruneStats

	cutoff := s.clock().Add(-s.ttl).UTC().UnixNano()
	for _, ttl := range []struct{ what, query string }{
		{"occurrence", `DELETE FROM occurrences WHERE id IN
			(SELECT id FROM occurrences WHERE emitted_at < ? ORDER BY emitted_at ASC, id ASC LIMIT ?)`},
		{"graph snapshot", `DELETE FROM graph_snapshots WHERE id IN
			(SELECT id FROM graph_snapshots WHERE taken_at < ?
			 AND id NOT IN (SELECT id FROM graph_snapshots ORDER BY taken_at DESC, id DESC LIMIT 1)
			 ORDER BY taken_at ASC, id ASC LIMIT ?)`},
		{"graph change", `DELETE FROM graph_changes WHERE id IN
			(SELECT id FROM graph_changes WHERE at < ? ORDER BY at ASC, id ASC LIMIT ?)`},
		{"sink session", `DELETE FROM sink_sessions WHERE ROWID IN
			(SELECT ROWID FROM sink_sessions WHERE updated_at < ? ORDER BY updated_at ASC LIMIT ?)`},
	} {
		var totalDeleted int64
		for {
			res, err := s.db.ExecContext(ctx, ttl.query, cutoff, ttlPruneBatch)
			if err != nil {
				return stats, err
			}
			n, _ := res.RowsAffected()
			if n <= 0 {
				break
			}
			totalDeleted += n
			if n < ttlPruneBatch {
				break
			}
		}
		if totalDeleted > 0 {
			stats.TTLRows += totalDeleted
			s.vacuum(ctx)
			s.logf("store: pruned %d %s row(s) older than TTL %s", totalDeleted, ttl.what, s.ttl)
			if s.hooks.OnPrune != nil {
				s.hooks.OnPrune("ttl", totalDeleted)
			}
		}
	}

	for {
		used, err := s.usedBytes(ctx)
		if err != nil {
			return stats, err
		}
		if used <= s.max {
			break
		}
		n, what, err := s.sizePruneStep(ctx)
		if err != nil {
			return stats, err
		}
		if n == 0 {
			// Nothing left to delete: the file is over budget for
			// reasons rows cannot fix (WAL not yet checkpointed, the
			// one protected snapshot). Do not spin.
			s.logf("store: size %dB exceeds bound %dB but no prunable rows remain", used, s.max)
			break
		}
		stats.SizeRows += n
		s.vacuum(ctx)
		s.logf("store: size bound %dB exceeded (%dB used) — pruned %d oldest %s row(s)", s.max, used, n, what)
		if s.hooks.OnPrune != nil {
			s.hooks.OnPrune("size", n)
		}
	}
	return stats, nil
}

// sizePruneStep deletes one batch of the oldest rows, working through
// the tables in fixed order: occurrences first (highest volume,
// lowest replay value), then graph snapshots (always keeping the newest
// snapshot), then graph changes (#658).
func (s *Store) sizePruneStep(ctx context.Context) (int64, string, error) {
	for _, step := range []struct{ what, query string }{
		{"occurrence", `DELETE FROM occurrences WHERE id IN
			(SELECT id FROM occurrences ORDER BY emitted_at ASC, id ASC LIMIT ?)`},
		{"graph snapshot", `DELETE FROM graph_snapshots WHERE id IN
			(SELECT id FROM graph_snapshots WHERE id NOT IN
				(SELECT id FROM graph_snapshots ORDER BY taken_at DESC, id DESC LIMIT 1)
			 ORDER BY taken_at ASC, id ASC LIMIT ?)`},
		{"graph change", `DELETE FROM graph_changes WHERE id IN
			(SELECT id FROM graph_changes ORDER BY at ASC, id ASC LIMIT ?)`},
	} {
		res, err := s.db.ExecContext(ctx, step.query, sizePruneBatch)
		if err != nil {
			return 0, "", err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			return n, step.what, nil
		}
	}
	return 0, "", nil
}

// usedBytes measures the database's live payload: pages in use minus
// freelist, times page size. Freelist pages are excluded because a
// prune that only recycles pages must still count as having freed
// space — the file itself shrinks lazily via incremental_vacuum.
func (s *Store) usedBytes(ctx context.Context) (int64, error) {
	var pageCount, freelist, pageSize int64
	if err := s.db.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pageCount); err != nil {
		return 0, err
	}
	if err := s.db.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&freelist); err != nil {
		return 0, err
	}
	if err := s.db.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize); err != nil {
		return 0, err
	}
	return (pageCount - freelist) * pageSize, nil
}

// vacuum returns freed pages to the OS (auto_vacuum=INCREMENTAL is
// set at Open). Best-effort: failure only means the file stays large
// on disk; the usedBytes accounting is already freelist-aware.
func (s *Store) vacuum(ctx context.Context) {
	if _, err := s.db.ExecContext(ctx, `PRAGMA incremental_vacuum`); err != nil {
		s.logf("store: incremental_vacuum: %v", err)
	}
}
