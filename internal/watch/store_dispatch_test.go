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

package watch

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// TestStoreFlags pins the ADDITIVE §9.1 flag surface: --store defaults
// to "" (disabled — the M2 behavior, and no path is ever invented for
// the operator), --store-ttl to the design's 30 days, --store-max-mb
// to 512; bounds are validated in every mode.
func TestStoreFlags(t *testing.T) {
	t.Parallel()
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if f.store != "" {
		t.Errorf("default --store = %q, want \"\" (disabled; the store path is always explicit)", f.store)
	}
	if f.storeTTL != 720*time.Hour {
		t.Errorf("default --store-ttl = %v, want 720h (§9.1: 30 days)", f.storeTTL)
	}
	if f.storeMaxMB != 512 {
		t.Errorf("default --store-max-mb = %d, want 512", f.storeMaxMB)
	}

	for _, args := range [][]string{
		{"--dry-run", "--store-ttl=0"},
		{"--dry-run", "--store-ttl=-1h"},
		{"--dry-run", "--store-max-mb=0"},
	} {
		f, err := parseFlags(args)
		if err != nil {
			t.Fatalf("parseFlags(%v): %v", args, err)
		}
		if err := f.validate(); err == nil {
			t.Errorf("validate(%v) accepted a nonsensical store bound", args)
		}
	}
}

// openDispatchStore attaches a real store to a test dispatcher.
func openDispatchStore(t *testing.T, d *dispatcher) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "lookout.db"), store.WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	d.store = s
	return s
}

// occurrencesByRoute drains the store and indexes rows by route.
func occurrencesByRoute(t *testing.T, s *store.Store) map[store.RouteOutcome][]store.Occurrence {
	t.Helper()
	s.Flush()
	out := map[store.RouteOutcome][]store.Occurrence{}
	for occ, err := range s.Recent(context.Background(), time.Time{}, 0) {
		if err != nil {
			t.Fatalf("Recent: %v", err)
		}
		out[occ.Route] = append(out[occ.Route], occ)
	}
	return out
}

// TestDispatcher_RecordsEveryRouteOutcome: with --store wired, each
// dispatcher terminal records the signal with its §7.7/§9.1 routing
// outcome — injected, suppressed, watchboard, info-stored, resolved —
// and the info class is PERSISTED, not dropped, while the frozen
// info_dropped_total metric keeps counting the class.
func TestDispatcher_RecordsEveryRouteOutcome(t *testing.T) {
	t.Parallel()
	base, _ := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
	s := openDispatchStore(t, d)
	ctx := context.Background()

	orig := crashLoopSignal()
	d.DispatchSignal(ctx, orig)                                         // critical → injected (sess-1)
	d.DispatchSignal(ctx, crashLoopSignal())                            // duplicate → suppressed
	d.DispatchSignal(ctx, warningSignal(1))                             // warning → watchboard
	d.DispatchSignal(ctx, infoSignal())                                 // info → info-stored
	d.DispatchSignal(ctx, resolvedSignalFor(orig, engine.KindResolved)) // → resolved (sess-1)

	byRoute := occurrencesByRoute(t, s)
	if len(byRoute[store.RouteInjected]) != 1 || byRoute[store.RouteInjected][0].SessionID != "sess-1" {
		t.Errorf("injected rows = %+v, want 1 row bound to sess-1", byRoute[store.RouteInjected])
	}
	if len(byRoute[store.RouteSuppressed]) != 1 || byRoute[store.RouteSuppressed][0].SessionID != "sess-1" {
		t.Errorf("suppressed rows = %+v, want 1 row carrying the bound session", byRoute[store.RouteSuppressed])
	}
	if len(byRoute[store.RouteWatchboard]) != 1 || byRoute[store.RouteWatchboard][0].SessionID != "" {
		t.Errorf("watchboard rows = %+v, want 1 row with no session (lazy watchboard session)", byRoute[store.RouteWatchboard])
	}
	info := byRoute[store.RouteInfoStored]
	if len(info) != 1 || info[0].Kind != "custom.heartbeat" || info[0].Severity != engine.SeverityInfo {
		t.Fatalf("info-stored rows = %+v, want the custom.heartbeat signal persisted", info)
	}
	if len(byRoute[store.RouteResolved]) != 1 || byRoute[store.RouteResolved][0].SessionID != "sess-1" {
		t.Errorf("resolved rows = %+v, want 1 row bound to sess-1", byRoute[store.RouteResolved])
	}
	// Frozen contract: the metric counts the info class whether or
	// not the store persisted it (M2 tests pin the store-less path).
	if got := testutil.ToFloat64(d.metrics.infoDropped.WithLabelValues("custom.heartbeat")); got != 1 {
		t.Errorf("info_dropped_total = %v, want 1 with the store enabled too", got)
	}
	// The info row is queryable by its object UID — the §9.1 read
	// path digests use.
	rows, err := s.RecentByObject(context.Background(), info[0].UID, time.Time{})
	if err != nil || len(rows) != 1 {
		t.Errorf("RecentByObject(info uid) = %d rows, %v; want 1", len(rows), err)
	}
}

// TestDispatcher_StormOutcomesRecorded: the storm trigger records
// route=storm bound to the storm session; a late arrival records
// route=storm-member; both carry the storm fingerprint. Members that
// fired per-incident before the storm formed keep their original
// injected rows — the store is an event log, not current state.
func TestDispatcher_StormOutcomesRecorded(t *testing.T) {
	t.Parallel()
	base, _ := newRoutingFakeDaemon(t)
	d, sigs := newStormDispatcher(t, base, 4)
	s := openDispatchStore(t, d)
	ctx := context.Background()

	d.DispatchSignal(ctx, sigs[0]) // injected, sess-1
	d.DispatchSignal(ctx, sigs[1]) // injected, sess-2
	d.DispatchSignal(ctx, sigs[2]) // trigger: storm forms, sess-3
	d.DispatchSignal(ctx, sigs[3]) // late arrival: storm-member

	byRoute := occurrencesByRoute(t, s)
	if len(byRoute[store.RouteInjected]) != 2 {
		t.Errorf("injected rows = %d, want 2 (pre-storm members keep their history)", len(byRoute[store.RouteInjected]))
	}
	stormRows := byRoute[store.RouteStorm]
	if len(stormRows) != 1 {
		t.Fatalf("storm rows = %d, want 1 (the trigger)", len(stormRows))
	}
	if stormRows[0].SessionID != "sess-3" || stormRows[0].StormFingerprint == "" {
		t.Errorf("storm row = session %q, stormfp %q; want sess-3 with a fingerprint", stormRows[0].SessionID, stormRows[0].StormFingerprint)
	}
	members := byRoute[store.RouteStormMember]
	if len(members) != 1 {
		t.Fatalf("storm-member rows = %d, want 1 (the late arrival)", len(members))
	}
	if members[0].SessionID != "sess-3" || members[0].StormFingerprint != stormRows[0].StormFingerprint {
		t.Errorf("member row = session %q, stormfp %q; want the storm's sess-3 + fingerprint %q",
			members[0].SessionID, members[0].StormFingerprint, stormRows[0].StormFingerprint)
	}
}

// TestDispatcher_ResolvedWithoutBindingStillRecorded: a §7.4 outcome
// whose session binding is lost is dropped from injection (frozen M2
// behavior) but still recorded — NULL session — so stability windows
// stay complete across restarts without --dedup-persist.
func TestDispatcher_ResolvedWithoutBindingStillRecorded(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)
	s := openDispatchStore(t, d)

	d.DispatchSignal(context.Background(), resolvedSignalFor(crashLoopSignal(), engine.KindResolved))

	if len(*injects) != 0 {
		t.Fatalf("no inject expected for an unbound resolved signal, got %d", len(*injects))
	}
	byRoute := occurrencesByRoute(t, s)
	rows := byRoute[store.RouteResolved]
	if len(rows) != 1 || rows[0].SessionID != "" {
		t.Errorf("resolved rows = %+v, want 1 row with NULL session", rows)
	}
}

// TestDispatcher_NilStoreIsM2Behavior: without --store every record
// call is a no-op — this is the frozen default; the M2 dispatch tests
// (routing table, storm, recovery) all run with a nil store and pin
// the behavior in detail. Here we only prove nothing panics and the
// info drop still counts.
func TestDispatcher_NilStoreIsM2Behavior(t *testing.T) {
	t.Parallel()
	base, _ := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
	if d.store != nil {
		t.Fatal("test premise: dispatcher starts with a nil store")
	}
	d.DispatchSignal(context.Background(), infoSignal())
	if got := testutil.ToFloat64(d.metrics.infoDropped.WithLabelValues("custom.heartbeat")); got != 1 {
		t.Errorf("info_dropped_total = %v, want 1", got)
	}
}
