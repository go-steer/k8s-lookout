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

// Regression drills for issue #84 (the per-incident sibling of the
// #81 storm-path bug): when OpenIncident fails with sid=="" for a NEW
// incident, the dispatcher drops the event but the dedup entry
// Observe created stays, unbound — no BindIncident, no tracking.
// Every later event for the key is a DedupDuplicate with
// SessionID=="" and is suppressed. The documented recovery is the
// Observe case-2 retry safety net (new session once the window
// expires), but case 3 advances LastSeen on EVERY sub-window event,
// so a steady symptom stream denser than --dedup-window keeps its own
// suppression alive forever ("a steady symptom stream never exits the
// dedup window" — the dispatcher says so itself). With --dedup-persist
// the unbound entry even survives restarts.
//
// The invariant encoded here is mechanism-agnostic — freezing
// LastSeen for unbound entries, retrying the open on the next
// duplicate, and a repair sweep are all acceptable: after a failed
// create, with the daemon recovered, a steady sub-window event stream
// for the same key must result in a real agent session within a
// BOUNDED number of events (this drill: 20 events spanning ~2
// windows of real time), and no Append may ever be issued against an
// empty session id along the way.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// retryDrillWindow is the drill's --dedup-window equivalent. The
// dedup cache runs on the real clock (its test clock hook is
// engine-internal), so the drill compresses the 5m production window
// to 250ms and drives events every ~25ms: the stream stays an order
// of magnitude denser than the window (the case-3 "steady stream"
// premise) while the 20-event drill spans ~2 windows of wall time —
// bounded room for ANY of the acceptable fixes to have fired.
const (
	retryDrillWindow = 250 * time.Millisecond
	retryDrillEvents = 20
	retryDrillPace   = 25 * time.Millisecond
)

// newRetryDispatcher is newRecoveryDispatcher with a configurable
// dedup window and persist path — the drill needs a window short
// enough for real wall time to cross it.
func newRetryDispatcher(t *testing.T, base string, window time.Duration, persistPath string) *dispatcher {
	t.Helper()
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dc, err := engine.NewDedupCache(window, persistPath)
	if err != nil {
		t.Fatalf("NewDedupCache: %v", err)
	}
	return &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0)),
		dedup:    dc,
		injector: inj,
		metrics:  newMetrics(),
		cluster:  "prod-us-central1",
		mode:     "per-incident",
	}
}

// steadyEvent fabricates the i-th event of a steady single-key
// symptom stream: same incident key, strictly advancing k8s event
// timestamps (so Observe never takes the case-1 replay branch).
func steadyEvent(i int) engine.Signal {
	sig := crashLoopSignal()
	sig.LastSeen = sig.LastSeen.Add(time.Duration(i) * 30 * time.Second)
	return sig
}

// driveSteadyStream dispatches events 1..retryDrillEvents at the
// drill pace against a healthy daemon.
func driveSteadyStream(t *testing.T, d *dispatcher, firstIndex int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < retryDrillEvents; i++ {
		d.DispatchSignal(ctx, steadyEvent(firstIndex+i))
		time.Sleep(retryDrillPace)
	}
}

// assertStreamReachedSession is the shared invariant: after the drill
// the key must be bound to a real session, the daemon must have seen
// agent-session traffic, and nothing may have targeted an empty sid.
func assertStreamReachedSession(t *testing.T, d *dispatcher, fd *flakySessionDaemon) {
	t.Helper()
	key := crashLoopSignal().Key
	if sid, ok := d.dedup.LookupSession(key); !ok || sid == "" {
		t.Errorf("dedup binding for %v = (%q, %v), want a real session id — the unbound entry from the failed create is still suppressing the key (issue #84)",
			key, sid, ok)
	}
	for _, in := range fd.injectLog() {
		if in.SessionID == "" {
			t.Errorf("inject POSTed to /sessions//inject (empty session id) — body: %.120s", in.Body)
		}
	}
	if got := testutil.ToFloat64(d.metrics.injectErrors.WithLabelValues("CrashLoopBackOff", "inject")); got != 0 {
		t.Errorf("inject_errors{CrashLoopBackOff,inject} = %v, want 0 — an Append targeted an unusable session id (issue #84 / #81 convention)", got)
	}
	if fd.sessionsCreated() == 0 {
		t.Fatalf("%d steady sub-window events after daemon recovery (~%.1f dedup windows of wall time) produced ZERO sessions and ZERO agent-session traffic — one transient create failure suppresses the live symptom indefinitely (issue #84)",
			retryDrillEvents, float64(retryDrillEvents)*float64(retryDrillPace)/float64(retryDrillWindow))
	}
}

// TestDispatch_CreateFailureUnboundEntry_SteadyStreamStillPages is
// the issue #84 core repro: the daemon is down for the first sighting
// only (create fails, dedup entry left unbound), recovers immediately,
// and the symptom keeps firing at sub-window cadence. Within the
// bounded drill a real session must exist for the key.
func TestDispatch_CreateFailureUnboundEntry_SteadyStreamStillPages(t *testing.T) {
	t.Parallel()
	base, fd := newFlakySessionDaemon(t)
	d := newRetryDispatcher(t, base, retryDrillWindow, "")
	ctx := context.Background()

	// Event 1: daemon down — the single un-retried POST fails.
	fd.setDown(true)
	d.DispatchSignal(ctx, steadyEvent(0))
	fd.setDown(false)
	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("error")); got != 1 {
		t.Fatalf("setup: session create must fail exactly once, got sessionCreates{error}=%v", got)
	}
	if d.dedup.Len() != 1 {
		t.Fatalf("setup: dedup entries = %d, want 1 (Observe committed the entry before the failed open)", d.dedup.Len())
	}
	if _, ok := d.dedup.LookupSession(crashLoopSignal().Key); ok {
		t.Fatal("setup: entry must be UNBOUND after the failed create")
	}

	// Daemon recovered; the symptom stream continues denser than the
	// window.
	driveSteadyStream(t, d, 1)
	assertStreamReachedSession(t, d, fd)
}

// TestDispatch_CreateFailurePersistedUnboundEntry_StillPagesAfterRestart
// is the --dedup-persist variant the issue calls out as worse: the
// unbound entry survives a sentinel restart via Snapshot/restore, so
// even a rebooted sentinel keeps suppressing the live symptom. Same
// invariant across the restart boundary.
func TestDispatch_CreateFailurePersistedUnboundEntry_StillPagesAfterRestart(t *testing.T) {
	t.Parallel()
	path := t.TempDir() + "/dedup.json"
	base, fd := newFlakySessionDaemon(t)
	ctx := context.Background()

	// Generation 1: daemon down at the first sighting; the unbound
	// entry is snapshotted (the periodic Snapshot ticker / SIGTERM
	// path in production).
	d1 := newRetryDispatcher(t, base, retryDrillWindow, path)
	fd.setDown(true)
	d1.DispatchSignal(ctx, steadyEvent(0))
	fd.setDown(false)
	if got := testutil.ToFloat64(d1.metrics.sessionCreates.WithLabelValues("error")); got != 1 {
		t.Fatalf("setup: gen-1 create must fail exactly once, got sessionCreates{error}=%v", got)
	}
	if err := d1.dedup.Snapshot(); err != nil {
		t.Fatalf("setup: Snapshot: %v", err)
	}

	// Generation 2: sentinel restart — fresh cache hydrated from the
	// same persist path, daemon healthy the whole time.
	d2 := newRetryDispatcher(t, base, retryDrillWindow, path)
	if d2.dedup.Len() != 1 {
		t.Fatalf("setup: restored dedup entries = %d, want 1 (the unbound entry rides --dedup-persist)", d2.dedup.Len())
	}
	if _, ok := d2.dedup.LookupSession(crashLoopSignal().Key); ok {
		t.Fatal("setup: restored entry must still be UNBOUND")
	}

	driveSteadyStream(t, d2, 1)
	assertStreamReachedSession(t, d2, fd)
}
