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
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// Regression drills for issue #81: by the time the dispatcher handles
// a StormFormed verdict the correlator has already committed the
// storm, so when OpenIncident's single un-retried POST fails (daemon
// briefly down — plausible during exactly the node-failure burst that
// forms storms), stormFormed returns early with no session bound and
// no unwind. Every later incident on the same blast-radius key then
// attaches to the session-less storm: suppressed without a session of
// its own, its Append aimed at an empty session id, and the storm's
// idle TTL never elapsing while the burst refreshes it. One transient
// sink error silences the whole correlated class indefinitely.
//
// The invariant encoded here is deliberately mechanism-agnostic —
// unwind-on-failure, retry-on-attach, and retry-on-next-event are all
// acceptable: after the daemon RECOVERS, subsequent correlated
// incidents must reach an agent session within a bounded number of
// events, and no Append may ever be issued against an empty session
// id.

// formStormDuringOutage drives the issue #81 window against a 6-signal
// burst: two healthy per-incident opens (sess-1, sess-2), then the
// storm-forming third incident while the daemon is down (the ONE
// OpenIncident fails), then recovery. Returns the not-yet-dispatched
// tail of the burst (incidents 4-6, same blast-radius key).
func formStormDuringOutage(t *testing.T) (*dispatcher, *flakySessionDaemon, []engine.Signal) {
	t.Helper()
	base, fd := newFlakySessionDaemon(t)
	d, sigs := newStormDispatcher(t, base, 6)
	ctx := context.Background()

	d.DispatchSignal(ctx, sigs[0])
	d.DispatchSignal(ctx, sigs[1])
	if got := fd.sessionsCreated(); got != 2 {
		t.Fatalf("setup: want 2 per-incident sessions before the outage, got %d", got)
	}

	// The daemon is down for exactly the dispatch that forms the storm.
	fd.setDown(true)
	d.DispatchSignal(ctx, sigs[2])
	fd.setDown(false)
	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("error")); got != 1 {
		t.Fatalf("setup: the storm-session open must fail exactly once at formation, got sessionCreates{error}=%v", got)
	}
	if got := d.storm.ActiveStorms(); got != 1 {
		t.Fatalf("setup: ActiveStorms = %d, want 1 (the correlator committed the storm before the failed open)", got)
	}
	return d, fd, sigs[3:]
}

// TestStormDispatch_CreateFailureAtFormation_RecoveredDaemonStillPages
// is the issue #81 black-hole repro: after the formation-time open
// failure and daemon recovery, the burst continues with three more
// incidents on the same blast-radius key — each must be bound to a
// real agent session again (retried open, fresh formation, or any
// other mechanism), not suppressed into the session-less storm.
func TestStormDispatch_CreateFailureAtFormation_RecoveredDaemonStillPages(t *testing.T) {
	t.Parallel()
	d, fd, tail := formStormDuringOutage(t)
	ctx := context.Background()

	preRecovery := len(fd.injectLog())
	for _, sig := range tail {
		d.DispatchSignal(ctx, sig)
	}

	// Every post-recovery member must route SOMEWHERE an agent can
	// see: a binding to an empty (or missing) session id means its
	// followups and §7.4 outcome have no home.
	for _, sig := range tail {
		if sid, ok := d.dedup.LookupSession(sig.Key); !ok || sid == "" {
			t.Errorf("%s binding = (%q, %v), want a real session id — the member was suppressed into the session-less storm (issue #81)",
				sig.Name, sid, ok)
		}
	}
	post := fd.injectLog()[preRecovery:]
	if len(post) == 0 {
		t.Fatalf("3 correlated incidents dispatched after daemon recovery produced ZERO agent-session traffic — one transient create failure at formation black-holes the class indefinitely (issue #81)")
	}
}

// TestStormDispatch_CreateFailureAtFormation_NoEmptySessionIDAppends
// pins the second issue #81 acceptance line: at no point in the
// outage-and-recovery scenario may an Append be issued against an
// empty session id — neither on the wire (POST /sessions//inject)
// nor client-side (the injector rejects sid=="" before the POST and
// the dispatcher counts it as an inject error; with the daemon
// healthy for every post-recovery event, this scenario has no
// legitimate inject error).
func TestStormDispatch_CreateFailureAtFormation_NoEmptySessionIDAppends(t *testing.T) {
	t.Parallel()
	d, fd, tail := formStormDuringOutage(t)
	ctx := context.Background()
	for _, sig := range tail {
		d.DispatchSignal(ctx, sig)
	}

	for _, in := range fd.injectLog() {
		if in.SessionID == "" {
			t.Errorf("inject POSTed to /sessions//inject (empty session id) — body: %.120s", in.Body)
		}
	}
	if got := testutil.ToFloat64(d.metrics.injectErrors.WithLabelValues("CrashLoopBackOff", "inject")); got != 0 {
		t.Errorf("inject_errors{CrashLoopBackOff,inject} = %v, want 0 — member Appends were issued against the empty storm session id (issue #81)", got)
	}
	if got := testutil.ToFloat64(d.metrics.injectErrors.WithLabelValues("storm", "inject")); got != 0 {
		t.Errorf("inject_errors{storm,inject} = %v, want 0 — a storm followup Append targeted the empty storm session id (issue #81)", got)
	}
}
