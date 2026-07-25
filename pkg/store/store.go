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

// Package store is the sentinel-local raw-occurrence store (DESIGN.md
// §9.1): ONE bounded, TTL'd embedded SQLite database, living alongside
// the --dedup-persist file on the same volume, that holds every signal
// the watch-path pipeline emitted — including info-severity signals
// that never inject (§7.7) — together with the routing outcome each
// one received. It powers storm-correlation lookback, §7.4 resolved
// stability windows, digests, and recommendation history.
//
// It is TELEMETRY, NOT A SYSTEM OF RECORD (§9.1): the write path is a
// buffered single-writer goroutine that drops records (loudly, via the
// OnDrop hook) rather than ever back-pressuring the inject pipeline,
// retention defaults to 30 days, and a size bound prunes oldest-first
// when exceeded. Nothing downstream may treat an absent occurrence as
// evidence a signal did not fire.
//
// Driver choice: modernc.org/sqlite, the pure-Go, CGO-free SQLite
// translation. This is REQUIRED, not a preference — the release image
// is distroless static built with CGO_ENABLED=0, so mattn/go-sqlite3
// (cgo) cannot link into it. The databases the two drivers produce are
// ordinary, fully compatible SQLite files.
//
// Schema versioning is forward-only: schema_version carries the
// applied version and migrations append. Graph snapshots and the
// topology delta log (§6.6) land in a LATER change as SEPARATE tables
// via new migrations — nothing in the occurrences schema is shared
// with them beyond the file.
//
// The store path is always an explicit flag (`--store=`); there is no
// default location, and in particular never one under $HOME — a
// sentinel without the flag runs with a nil *Store, whose methods are
// all no-ops, so "disabled" costs nothing and needs no branching in
// callers.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	_ "modernc.org/sqlite" // pure-Go sqlite driver, registered as "sqlite"
)

// Defaults for Open options. DefaultTTL is the §9.1 normative 30 days.
const (
	DefaultTTL       = 720 * time.Hour
	DefaultMaxBytes  = 512 << 20 // 512 MiB
	DefaultBufferLen = 1024

	// MaxMessageBytes caps the stored (and raw-blob) message column.
	// Messages are sanitized UPSTREAM (§6.5 — the sanitizer runs on
	// every emit surface before signals reach the pipeline); the store
	// only bounds their size so one pathological event cannot bloat 30
	// days of retention. Byte-truncated, not rune-truncated: the cap
	// is a storage bound, not a display rule.
	MaxMessageBytes = 4096
)

// migrations is the forward-only schema history: index i upgrades the
// database FROM version i TO version i+1. Never edit or reorder an
// entry that has shipped — append. Graph snapshots + the §6.6 delta
// log arrive as later entries creating their own tables.
var migrations = []string{
	// v1: the §9.1 occurrences table. Times are UTC unix nanoseconds
	// (INTEGER); nullable time/text columns use NULL — never a zero
	// sentinel — so round-trips are exact. raw is the compact JSON of
	// the emitted engine.Signal (enrichment stripped, message capped;
	// see newRow) kept for later distillation (§9.2).
	`CREATE TABLE occurrences (
		id                INTEGER PRIMARY KEY,
		emitted_at        INTEGER NOT NULL,
		kind              TEXT NOT NULL,
		source            TEXT NOT NULL,
		severity          TEXT NOT NULL,
		fingerprint       TEXT NOT NULL,
		route             TEXT NOT NULL,
		cluster           TEXT NOT NULL DEFAULT '',
		namespace         TEXT NOT NULL DEFAULT '',
		kind_of_object    TEXT NOT NULL DEFAULT '',
		name              TEXT NOT NULL DEFAULT '',
		uid               TEXT NOT NULL DEFAULT '',
		reason            TEXT NOT NULL DEFAULT '',
		canonical_reason  TEXT NOT NULL DEFAULT '',
		message           TEXT NOT NULL DEFAULT '',
		count             INTEGER NOT NULL DEFAULT 0,
		first_seen        INTEGER,
		last_seen         INTEGER,
		session_id        TEXT,
		storm_fingerprint TEXT,
		forecast_eta      INTEGER,
		raw               BLOB NOT NULL
	);
	CREATE INDEX occurrences_fingerprint_emitted ON occurrences (fingerprint, emitted_at);
	CREATE INDEX occurrences_uid_emitted ON occurrences (uid, emitted_at);
	CREATE INDEX occurrences_severity_emitted ON occurrences (severity, emitted_at);`,
}

// Hooks are the store's observability seams: pkg/store carries no
// Prometheus dependency, the sentinel wires these to its metrics.
// Every hook may be nil. Hooks must be fast and must not call back
// into the Store.
type Hooks struct {
	// OnWrite fires after a record is durably committed, with its
	// routing outcome.
	OnWrite func(route RouteOutcome)
	// OnDrop fires when a record is LOST: cause "buffer_full" (the
	// writer buffer was full — telemetry, not system of record: we
	// drop, never block the pipeline) or "write_error" (the batch
	// insert failed).
	OnDrop func(cause string)
	// OnPrune fires after a prune pass removed rows, with the cause
	// ("ttl" or "size") and the number of rows deleted.
	OnPrune func(cause string, rows int64)
}

type options struct {
	ttl      time.Duration
	maxBytes int64
	bufLen   int
	clock    func() time.Time
	hooks    Hooks
	logf     func(format string, args ...any)
}

// Option configures Open.
type Option func(*options)

// WithTTL sets the retention window (default DefaultTTL, 30 days).
func WithTTL(d time.Duration) Option { return func(o *options) { o.ttl = d } }

// WithMaxBytes sets the size bound (default DefaultMaxBytes); when the
// database's used pages exceed it, prune deletes oldest rows first.
func WithMaxBytes(n int64) Option { return func(o *options) { o.maxBytes = n } }

// WithBufferLen sets the writer buffer capacity (default
// DefaultBufferLen). A full buffer drops records with OnDrop.
func WithBufferLen(n int) Option { return func(o *options) { o.bufLen = n } }

// WithClock injects the time source (default time.Now) — emitted_at
// stamps and prune cutoffs use it, so tests are deterministic.
func WithClock(now func() time.Time) Option { return func(o *options) { o.clock = now } }

// WithHooks wires the observability callbacks.
func WithHooks(h Hooks) Option { return func(o *options) { o.hooks = h } }

// WithLogf overrides the store's logger (default log.Printf). Drops
// and prunes are deliberately loud (§9.1: bounded and TTL'd means data
// disappears BY DESIGN — the logs say when and why).
func WithLogf(f func(format string, args ...any)) Option { return func(o *options) { o.logf = f } }

// Store is the open occurrence store. The zero value is not usable;
// call Open. A nil *Store is the DISABLED store: every method is a
// safe no-op (writes vanish, queries return nothing), so callers hold
// a *Store unconditionally and never branch on a flag.
type Store struct {
	db    *sql.DB
	ttl   time.Duration
	max   int64
	clock func() time.Time
	hooks Hooks
	logf  func(format string, args ...any)

	mu     sync.RWMutex // guards closed vs. concurrent Record/Flush
	closed bool
	ch     chan writeReq
	done   chan struct{} // closed when the writer goroutine exits
}

// Open opens (creating if absent) the occurrence store at path and
// applies any pending forward-only migrations. The file is an ordinary
// SQLite database in WAL mode with a 5s busy timeout; it should sit on
// the same volume as the --dedup-persist file (§9.1). path must be
// explicit — this package never invents a default location.
func Open(path string, opts ...Option) (*Store, error) {
	if path == "" {
		return nil, errors.New("store: path is required (an empty path means the store is disabled — use a nil *Store)")
	}
	o := &options{
		ttl:      DefaultTTL,
		maxBytes: DefaultMaxBytes,
		bufLen:   DefaultBufferLen,
		clock:    time.Now,
		logf:     log.Printf,
	}
	for _, opt := range opts {
		opt(o)
	}
	if o.ttl <= 0 {
		return nil, fmt.Errorf("store: ttl must be > 0 (got %s)", o.ttl)
	}
	if o.maxBytes < 1 {
		return nil, fmt.Errorf("store: max bytes must be >= 1 (got %d)", o.maxBytes)
	}
	if o.bufLen < 1 {
		return nil, fmt.Errorf("store: buffer length must be >= 1 (got %d)", o.bufLen)
	}

	// Pragmas ride the DSN so every pooled connection gets them.
	// auto_vacuum=INCREMENTAL must precede table creation to take
	// effect on a fresh file; it lets prune passes actually return
	// pages to the OS (incremental_vacuum) instead of only to the
	// freelist. journal_mode=WAL: concurrent readers during writes.
	// busy_timeout: writer vs. reader collisions wait, not error.
	dsn := "file:" + url.PathEscape(path) +
		"?_pragma=auto_vacuum(INCREMENTAL)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: migrate %s: %w", path, err)
	}
	s := &Store{
		db:    db,
		ttl:   o.ttl,
		max:   o.maxBytes,
		clock: o.clock,
		hooks: o.hooks,
		logf:  o.logf,
		ch:    make(chan writeReq, o.bufLen),
		done:  make(chan struct{}),
	}
	go s.writer()
	return s, nil
}

// migrate creates schema_version if needed and applies every pending
// migration, each in its own transaction. Forward-only: a database
// whose version exceeds this binary's migration count is from a NEWER
// lookout — refuse to touch it rather than corrupt it.
func migrate(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return err
	}
	version, err := readVersion(db)
	if err != nil {
		return err
	}
	if version > len(migrations) {
		return fmt.Errorf("database schema version %d is newer than this binary understands (max %d) — refusing forward-only migration backward", version, len(migrations))
	}
	for v := version; v < len(migrations); v++ {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(migrations[v]); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %d→%d: %w", v, v+1, err)
		}
		if _, err := tx.Exec(`DELETE FROM schema_version`); err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.Exec(`INSERT INTO schema_version (version) VALUES (?)`, v+1); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func readVersion(db *sql.DB) (int, error) {
	var v int
	err := db.QueryRow(`SELECT version FROM schema_version`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return v, err
}

// SchemaVersion reports the applied schema version. Nil-safe: a
// disabled store reports 0.
func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	if s == nil {
		return 0, nil
	}
	var v int
	err := s.db.QueryRowContext(ctx, `SELECT version FROM schema_version`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return v, err
}

// Flush blocks until every record enqueued BEFORE the call is
// committed (or dropped). Nil-safe no-op. Used by shutdown and tests;
// the hot path never waits.
func (s *Store) Flush() {
	if s == nil {
		return
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return
	}
	barrier := make(chan struct{})
	s.ch <- writeReq{barrier: barrier}
	s.mu.RUnlock()
	<-barrier
}

// Close flushes the writer buffer, stops the writer goroutine, and
// closes the database. Nil-safe no-op. Record calls racing Close are
// dropped silently, never a panic.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.ch)
	s.mu.Unlock()
	<-s.done
	return s.db.Close()
}
