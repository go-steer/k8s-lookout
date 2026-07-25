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
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// testClock is a settable deterministic time source.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

var t0 = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

// openTest opens a store on a temp path with a deterministic clock
// and quiet logs, closed via t.Cleanup.
func openTest(t *testing.T, opts ...Option) (*Store, *testClock) {
	t.Helper()
	clock := &testClock{now: t0}
	all := append([]Option{
		WithClock(clock.Now),
		WithLogf(t.Logf),
	}, opts...)
	s, err := Open(filepath.Join(t.TempDir(), "lookout.db"), all...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, clock
}

func fullSignal() engine.Signal {
	eta := time.Date(2026, 7, 25, 12, 14, 0, 0, time.UTC)
	return engine.Signal{
		Kind:        "saturation.forecast",
		Source:      engine.SourceSentinel,
		Severity:    engine.SeverityWarning,
		Fingerprint: "sha256:aaaa",
		Cluster:     "prod-east",
		Project:     "acme-prod",
		Zone:        "us-east1-b",
		TriageEvent: engine.TriageEvent{
			Key:           engine.EventKey{UID: "uid-1", Reason: "ErrImagePull"},
			Namespace:     "checkout",
			KindOfObject:  "Pod",
			Name:          "checkout-7b9d-x4kzq",
			Container:     "spec.containers{server}",
			Message:       "pod hits memory limit in ~14 min",
			FirstSeen:     time.Date(2026, 7, 25, 11, 0, 0, 0, time.UTC),
			LastSeen:      time.Date(2026, 7, 25, 11, 55, 0, 0, time.UTC),
			ControllerRef: "ReplicaSet/checkout-7b9d",
			Node:          "node-1",
			Labels:        map[string]string{"app": "checkout"},
			Count:         3,
		},
		Forecast: &engine.Forecast{ETA: eta, ConfidenceBasis: "linear-90m-window"},
	}
}

// TestOpen_MigratesFromEmpty: a fresh file lands on the current
// schema version, and reopening is a no-op (idempotent migrations).
func TestOpen_MigratesFromEmpty(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "lookout.db")
	s, err := Open(path, WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	v, err := s.SchemaVersion(context.Background())
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if want := len(migrations); v != want {
		t.Errorf("schema version = %d, want %d", v, want)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Reopen: no migration should run again, no error.
	s2, err := Open(path, WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()
	if v, err := s2.SchemaVersion(context.Background()); err != nil || v != len(migrations) {
		t.Errorf("reopen schema version = %d, %v; want %d, nil", v, err, len(migrations))
	}
}

// TestOpen_RefusesNewerSchema: forward-only means a downgrade must
// refuse loudly, never "migrate backward".
func TestOpen_RefusesNewerSchema(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "lookout.db")
	s, err := Open(path, WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s.db.Exec(`UPDATE schema_version SET version = ?`, len(migrations)+7); err != nil {
		t.Fatalf("bump version: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := Open(path, WithLogf(t.Logf)); err == nil || !strings.Contains(err.Error(), "newer") {
		t.Fatalf("Open on newer-schema db: err = %v, want refusal naming the newer schema", err)
	}
}

// TestOpen_RequiresPath: empty path is the caller's "disabled" state,
// not an Open input.
func TestOpen_RequiresPath(t *testing.T) {
	t.Parallel()
	if _, err := Open(""); err == nil {
		t.Fatal("Open(\"\") must error")
	}
}

// TestRecord_RoundTripAllFields writes a fully-populated signal and
// reads every column back, including the raw blob.
func TestRecord_RoundTripAllFields(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	sig := fullSignal()
	s.Record(sig, Outcome{Route: RouteStormMember, SessionID: "sess-9", StormFingerprint: "sha256:storm"})
	s.Flush()

	got, err := s.RecentByFingerprint(context.Background(), "sha256:aaaa", t0.Add(-time.Minute))
	if err != nil {
		t.Fatalf("RecentByFingerprint: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d occurrences, want 1", len(got))
	}
	occ := got[0]
	if occ.EmittedAt != t0 {
		t.Errorf("EmittedAt = %v, want %v", occ.EmittedAt, t0)
	}
	if occ.Kind != "saturation.forecast" || occ.Source != engine.SourceSentinel || occ.Severity != engine.SeverityWarning {
		t.Errorf("kind/source/severity = %q/%q/%q", occ.Kind, occ.Source, occ.Severity)
	}
	if occ.Route != RouteStormMember {
		t.Errorf("Route = %q, want storm-member", occ.Route)
	}
	if occ.Cluster != "prod-east" || occ.Namespace != "checkout" || occ.KindOfObject != "Pod" ||
		occ.Name != "checkout-7b9d-x4kzq" || occ.UID != "uid-1" {
		t.Errorf("object ref mismatch: %+v", occ)
	}
	if occ.Reason != "ErrImagePull" {
		t.Errorf("Reason = %q, want the RAW reason", occ.Reason)
	}
	if occ.CanonicalReason != "ImagePullBackOff" {
		t.Errorf("CanonicalReason = %q, want ImagePullBackOff (dedup family collapse)", occ.CanonicalReason)
	}
	if occ.Message != sig.Message || occ.Count != 3 {
		t.Errorf("message/count = %q/%d", occ.Message, occ.Count)
	}
	if !occ.FirstSeen.Equal(sig.FirstSeen) || !occ.LastSeen.Equal(sig.LastSeen) {
		t.Errorf("first/last seen = %v/%v, want %v/%v", occ.FirstSeen, occ.LastSeen, sig.FirstSeen, sig.LastSeen)
	}
	if occ.SessionID != "sess-9" || occ.StormFingerprint != "sha256:storm" {
		t.Errorf("session/stormfp = %q/%q", occ.SessionID, occ.StormFingerprint)
	}
	if occ.ForecastETA == nil || !occ.ForecastETA.Equal(sig.Forecast.ETA) {
		t.Errorf("ForecastETA = %v, want %v", occ.ForecastETA, sig.Forecast.ETA)
	}
	// The raw blob unmarshals back to the emitted Signal.
	var back engine.Signal
	if err := json.Unmarshal(occ.Raw, &back); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if back.Kind != sig.Kind || back.Key != sig.Key || back.Zone != "us-east1-b" ||
		back.Project != "acme-prod" || back.Labels["app"] != "checkout" ||
		back.Forecast == nil || back.Forecast.ConfidenceBasis != "linear-90m-window" {
		t.Errorf("raw round-trip mismatch: %+v", back)
	}
}

// TestRecord_RoundTripNULLs: a minimal signal stores NULLs (not zero
// sentinels) for the nullable columns and reads back as zero values.
func TestRecord_RoundTripNULLs(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	sig := engine.Signal{
		Kind:        engine.KindK8sEvent,
		Source:      engine.SourceSentinel,
		Severity:    engine.SeverityInfo,
		Fingerprint: "sha256:min",
		TriageEvent: engine.TriageEvent{
			Key: engine.EventKey{UID: "uid-min", Reason: "heartbeat"},
			// FirstSeen/LastSeen zero on purpose.
		},
	}
	s.Record(sig, Outcome{Route: RouteInfoStored})
	s.Flush()

	// NULL columns must really be NULL in SQL, not zero sentinels.
	var nulls int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM occurrences WHERE session_id IS NULL
		AND storm_fingerprint IS NULL AND forecast_eta IS NULL
		AND first_seen IS NULL AND last_seen IS NULL`).Scan(&nulls); err != nil {
		t.Fatalf("null probe: %v", err)
	}
	if nulls != 1 {
		t.Fatalf("nullable columns not NULL for the minimal signal")
	}

	got, err := s.RecentByObject(context.Background(), "uid-min", t0.Add(-time.Minute))
	if err != nil {
		t.Fatalf("RecentByObject: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d occurrences, want 1", len(got))
	}
	occ := got[0]
	if occ.SessionID != "" || occ.StormFingerprint != "" || occ.ForecastETA != nil {
		t.Errorf("nullable strings/eta = %q/%q/%v, want zero values", occ.SessionID, occ.StormFingerprint, occ.ForecastETA)
	}
	if !occ.FirstSeen.IsZero() || !occ.LastSeen.IsZero() {
		t.Errorf("zero times did not round-trip: %v / %v", occ.FirstSeen, occ.LastSeen)
	}
	if occ.Route != RouteInfoStored || occ.CanonicalReason != "heartbeat" {
		t.Errorf("route/canonical = %q/%q", occ.Route, occ.CanonicalReason)
	}
}

// TestRecord_MessageCapped: the message column and the raw blob both
// carry at most MaxMessageBytes of message.
func TestRecord_MessageCapped(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	sig := fullSignal()
	sig.Message = strings.Repeat("x", MaxMessageBytes+1000)
	s.Record(sig, Outcome{Route: RouteInjected, SessionID: "sess-1"})
	s.Flush()
	got, err := s.RecentByObject(context.Background(), "uid-1", t0.Add(-time.Minute))
	if err != nil || len(got) != 1 {
		t.Fatalf("RecentByObject: %v (%d rows)", err, len(got))
	}
	if len(got[0].Message) != MaxMessageBytes {
		t.Errorf("stored message = %d bytes, want cap %d", len(got[0].Message), MaxMessageBytes)
	}
	var back engine.Signal
	if err := json.Unmarshal(got[0].Raw, &back); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if len(back.Message) != MaxMessageBytes {
		t.Errorf("raw blob message = %d bytes, want cap %d", len(back.Message), MaxMessageBytes)
	}
}

// TestRecord_StripsEnrichment: the §7.6 bundle never lands in the raw
// blob (reproducible, huge, zero distillation value).
func TestRecord_StripsEnrichment(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	sig := fullSignal()
	sig.Enrichment = &engine.Enrichment{Bundle: "SECRET-BUNDLE-MARKER"}
	s.Record(sig, Outcome{Route: RouteInjected})
	s.Flush()
	got, err := s.RecentByObject(context.Background(), "uid-1", t0.Add(-time.Minute))
	if err != nil || len(got) != 1 {
		t.Fatalf("RecentByObject: %v (%d rows)", err, len(got))
	}
	if strings.Contains(string(got[0].Raw), "SECRET-BUNDLE-MARKER") {
		t.Errorf("enrichment bundle leaked into the raw blob")
	}
}

// TestPrune_TTL: rows older than the TTL are deleted on PruneOnce;
// newer rows stay; the prune is reported via OnPrune("ttl").
func TestPrune_TTL(t *testing.T) {
	t.Parallel()
	var pruneMu sync.Mutex
	pruned := map[string]int64{}
	s, clock := openTest(t,
		WithTTL(24*time.Hour),
		WithHooks(Hooks{OnPrune: func(cause string, rows int64) {
			pruneMu.Lock()
			defer pruneMu.Unlock()
			pruned[cause] += rows
		}}),
	)
	old := fullSignal()
	old.Key.UID = "uid-old"
	s.Record(old, Outcome{Route: RouteInjected})
	s.Flush()

	clock.Advance(25 * time.Hour) // old row now beyond TTL
	fresh := fullSignal()
	fresh.Key.UID = "uid-fresh"
	s.Record(fresh, Outcome{Route: RouteInjected})
	s.Flush()

	stats, err := s.PruneOnce(context.Background())
	if err != nil {
		t.Fatalf("PruneOnce: %v", err)
	}
	if stats.TTLRows != 1 || stats.SizeRows != 0 {
		t.Errorf("stats = %+v, want TTLRows=1 SizeRows=0", stats)
	}
	pruneMu.Lock()
	if pruned["ttl"] != 1 {
		t.Errorf("OnPrune ttl rows = %d, want 1", pruned["ttl"])
	}
	pruneMu.Unlock()

	if got, _ := s.RecentByObject(context.Background(), "uid-old", time.Time{}); len(got) != 0 {
		t.Errorf("expired row survived TTL prune")
	}
	if got, _ := s.RecentByObject(context.Background(), "uid-fresh", time.Time{}); len(got) != 1 {
		t.Errorf("fresh row was pruned")
	}
}

// TestPrune_SizeBound: when used pages exceed the bound, the OLDEST
// rows go first, until the store fits again.
func TestPrune_SizeBound(t *testing.T) {
	t.Parallel()
	var pruneMu sync.Mutex
	pruned := map[string]int64{}
	s, clock := openTest(t,
		WithMaxBytes(1<<20),
		WithHooks(Hooks{OnPrune: func(cause string, rows int64) {
			pruneMu.Lock()
			defer pruneMu.Unlock()
			pruned[cause] += rows
		}}),
	)
	// ~600 rows × ~3KiB message ≫ 1MiB.
	for i := 0; i < 600; i++ {
		sig := fullSignal()
		sig.Key.UID = fmt.Sprintf("uid-%04d", i)
		sig.Message = strings.Repeat("m", 3000)
		s.Record(sig, Outcome{Route: RouteInjected})
		clock.Advance(time.Second)
	}
	s.Flush()

	stats, err := s.PruneOnce(context.Background())
	if err != nil {
		t.Fatalf("PruneOnce: %v", err)
	}
	if stats.SizeRows == 0 {
		t.Fatalf("size prune removed nothing; stats = %+v", stats)
	}
	used, err := s.usedBytes(context.Background())
	if err != nil {
		t.Fatalf("usedBytes: %v", err)
	}
	if used > 1<<20 {
		t.Errorf("used = %d bytes, want <= bound %d", used, 1<<20)
	}
	pruneMu.Lock()
	if pruned["size"] != stats.SizeRows {
		t.Errorf("OnPrune size rows = %d, want %d", pruned["size"], stats.SizeRows)
	}
	pruneMu.Unlock()
	// Oldest-first: the first row must be gone, the last must remain.
	if got, _ := s.RecentByObject(context.Background(), "uid-0000", time.Time{}); len(got) != 0 {
		t.Errorf("oldest row survived a size prune")
	}
	if got, _ := s.RecentByObject(context.Background(), "uid-0599", time.Time{}); len(got) != 1 {
		t.Errorf("newest row did not survive a size prune")
	}
}

// TestPruneInterval pins min(1h, ttl/24).
func TestPruneInterval(t *testing.T) {
	t.Parallel()
	if got := PruneInterval(DefaultTTL); got != time.Hour {
		t.Errorf("PruneInterval(720h) = %v, want 1h", got)
	}
	if got := PruneInterval(24 * time.Minute); got != time.Minute {
		t.Errorf("PruneInterval(24m) = %v, want 1m", got)
	}
}

// TestRecord_BufferOverflowDrops: a full writer buffer drops the
// record with OnDrop("buffer_full") — Record never blocks. The writer
// is deliberately not running so the buffer state is deterministic.
func TestRecord_BufferOverflowDrops(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	drops := map[string]int{}
	s := &Store{
		clock: func() time.Time { return t0 },
		logf:  t.Logf,
		hooks: Hooks{OnDrop: func(cause string) {
			mu.Lock()
			defer mu.Unlock()
			drops[cause]++
		}},
		ch:   make(chan writeReq, 2),
		done: make(chan struct{}),
	}
	for i := 0; i < 5; i++ {
		s.Record(fullSignal(), Outcome{Route: RouteInjected})
	}
	mu.Lock()
	defer mu.Unlock()
	if drops["buffer_full"] != 3 {
		t.Errorf("buffer_full drops = %d, want 3 (buffer holds 2 of 5)", drops["buffer_full"])
	}
}

// TestConcurrentWriteAndQuery exercises writers vs. readers under
// -race: N goroutines record while M goroutines query; totals must
// reconcile after a flush.
func TestConcurrentWriteAndQuery(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	const writers, perWriter, readers = 8, 50, 4
	var writerWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		writerWG.Add(1)
		go func(w int) {
			defer writerWG.Done()
			for i := 0; i < perWriter; i++ {
				sig := fullSignal()
				sig.Key.UID = fmt.Sprintf("uid-%d-%d", w, i)
				s.Record(sig, Outcome{Route: RouteInjected, SessionID: "sess-1"})
			}
		}(w)
	}
	stop := make(chan struct{})
	var readerWG sync.WaitGroup
	for r := 0; r < readers; r++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := s.CountsBySeverity(context.Background(), time.Time{}); err != nil {
					t.Errorf("CountsBySeverity during writes: %v", err)
					return
				}
				if _, err := s.RecentByFingerprint(context.Background(), "sha256:aaaa", t0.Add(-time.Hour)); err != nil {
					t.Errorf("RecentByFingerprint during writes: %v", err)
					return
				}
			}
		}()
	}
	writerWG.Wait()
	close(stop)
	readerWG.Wait()
	s.Flush()

	counts, err := s.CountsBySeverity(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("CountsBySeverity: %v", err)
	}
	if counts[engine.SeverityWarning] != writers*perWriter {
		t.Errorf("stored = %d, want %d (no drops expected under default buffer)", counts[engine.SeverityWarning], writers*perWriter)
	}
}

// TestQueries: RecentByFingerprint/RecentByObject filter + order,
// CountsBySeverity groups, Recent honors its limit.
func TestQueries(t *testing.T) {
	t.Parallel()
	s, clock := openTest(t)
	mk := func(uid, fp string, sev engine.Severity) {
		sig := fullSignal()
		sig.Key.UID = uid
		sig.Fingerprint = fp
		sig.Severity = sev
		s.Record(sig, Outcome{Route: RouteInjected})
		clock.Advance(time.Minute)
	}
	mk("uid-a", "sha256:one", engine.SeverityCritical) // t0
	mk("uid-a", "sha256:one", engine.SeverityCritical) // t0+1m
	mk("uid-b", "sha256:two", engine.SeverityWarning)  // t0+2m
	mk("uid-c", "sha256:one", engine.SeverityInfo)     // t0+3m
	s.Flush()
	ctx := context.Background()

	byFP, err := s.RecentByFingerprint(ctx, "sha256:one", t0.Add(30*time.Second))
	if err != nil {
		t.Fatalf("RecentByFingerprint: %v", err)
	}
	if len(byFP) != 2 || byFP[0].UID != "uid-c" || byFP[1].UID != "uid-a" {
		t.Errorf("byFP: since-filter or newest-first order broken: %+v", byFP)
	}

	byObj, err := s.RecentByObject(ctx, "uid-a", time.Time{})
	if err != nil {
		t.Fatalf("RecentByObject: %v", err)
	}
	if len(byObj) != 2 {
		t.Errorf("byObj = %d rows, want 2", len(byObj))
	}

	counts, err := s.CountsBySeverity(ctx, time.Time{})
	if err != nil {
		t.Fatalf("CountsBySeverity: %v", err)
	}
	if counts[engine.SeverityCritical] != 2 || counts[engine.SeverityWarning] != 1 || counts[engine.SeverityInfo] != 1 {
		t.Errorf("counts = %v", counts)
	}

	var seen []string
	for occ, err := range s.Recent(ctx, time.Time{}, 3) {
		if err != nil {
			t.Fatalf("Recent: %v", err)
		}
		seen = append(seen, occ.UID)
	}
	if len(seen) != 3 || seen[0] != "uid-c" || seen[2] != "uid-a" {
		t.Errorf("Recent(limit=3) = %v, want newest-first capped at 3", seen)
	}
	// Early break must not error or leak.
	for range s.Recent(ctx, time.Time{}, 0) {
		break
	}
}

// TestDisabledStore_NoOps: a nil *Store is the disabled store — every
// method is safe and answers "nothing".
func TestDisabledStore_NoOps(t *testing.T) {
	t.Parallel()
	var s *Store
	s.Record(fullSignal(), Outcome{Route: RouteInjected})
	s.Flush()
	if err := s.Close(); err != nil {
		t.Errorf("nil Close: %v", err)
	}
	ctx := context.Background()
	s.RunPrune(ctx) // returns immediately, no panic
	if stats, err := s.PruneOnce(ctx); err != nil || stats != (PruneStats{}) {
		t.Errorf("nil PruneOnce = %+v, %v", stats, err)
	}
	if got, err := s.RecentByFingerprint(ctx, "x", time.Time{}); err != nil || len(got) != 0 {
		t.Errorf("nil RecentByFingerprint = %v, %v", got, err)
	}
	if got, err := s.RecentByObject(ctx, "x", time.Time{}); err != nil || len(got) != 0 {
		t.Errorf("nil RecentByObject = %v, %v", got, err)
	}
	if counts, err := s.CountsBySeverity(ctx, time.Time{}); err != nil || len(counts) != 0 {
		t.Errorf("nil CountsBySeverity = %v, %v", counts, err)
	}
	for range s.Recent(ctx, time.Time{}, 10) {
		t.Errorf("nil Recent yielded a row")
	}
	if v, err := s.SchemaVersion(ctx); err != nil || v != 0 {
		t.Errorf("nil SchemaVersion = %d, %v", v, err)
	}
}

// TestClose_FlushesAndIsIdempotent: records enqueued before Close are
// durable; double Close is fine; Record after Close is a silent no-op.
func TestClose_FlushesAndIsIdempotent(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "lookout.db")
	s, err := Open(path, WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s.Record(fullSignal(), Outcome{Route: RouteInjected})
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	s.Record(fullSignal(), Outcome{Route: RouteInjected}) // no panic

	s2, err := Open(path, WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()
	counts, err := s2.CountsBySeverity(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("CountsBySeverity: %v", err)
	}
	if counts[engine.SeverityWarning] != 1 {
		t.Errorf("row recorded before Close not durable: counts = %v", counts)
	}
}

// TestOnWriteHook fires per committed record with its route.
func TestOnWriteHook(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	writes := map[RouteOutcome]int{}
	s, _ := openTest(t, WithHooks(Hooks{OnWrite: func(route RouteOutcome) {
		mu.Lock()
		defer mu.Unlock()
		writes[route]++
	}}))
	s.Record(fullSignal(), Outcome{Route: RouteInjected})
	s.Record(fullSignal(), Outcome{Route: RouteSuppressed})
	s.Record(fullSignal(), Outcome{Route: RouteSuppressed})
	s.Flush()
	mu.Lock()
	defer mu.Unlock()
	if writes[RouteInjected] != 1 || writes[RouteSuppressed] != 2 {
		t.Errorf("OnWrite = %v, want injected:1 suppressed:2", writes)
	}
}
