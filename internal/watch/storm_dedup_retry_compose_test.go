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

// Regression drill for issue #104: the #94 storm retry-on-attach fix
// and the #96 dedup retry-on-duplicate fix do not compose.
//
// When a storm's formation-time OpenIncident fails (sink outage),
// stormFormed returns early and the storm is committed session-less. A
// late arrival then takes the StormAttached path; the #94 fix retries
// the storm open there, but while the sink is still down that retry
// also fails and stormAttached calls AttachToStorm(key, "", fingerprint,
// ref) — the member entry is now STORM-CLAIMED (Storm=fingerprint) yet
// SESSION-LESS (SessionID=="").
//
// A DUPLICATE never reaches the storm stage (correlation runs only on
// the fresh-incident path). After the sink recovers, the first
// duplicate of that storm-claimed critical-class key hits the #96 retry
// guard in DispatchSignal, which tests only mode/dryRun/
// result.SessionID==""/RouteStore/RouteWatchboard — never the entry's
// storm fingerprint. So the duplicate passes the guard, enters
// retryIncidentOpen, whose under-lock LookupSession recheck returns
// ("", false) for the sid=="" binding, and it opens a COMPETING
// per-incident session — BindIncident overwriting the storm binding,
// the N-session fan-out §7.5 exists to prevent.
//
// The invariant encoded here is mechanism-agnostic — suppressing the
// duplicate as a storm member, or retrying the STORM open on it, are
// both acceptable: what may never happen is a fresh per-incident
// (kind=k8s-event) session opened for a storm-claimed member.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestDispatch_StormClaimedDuplicate_NoCompetingIncidentSession drives a
// storm into the session-less state with a storm-claimed member, then
// dispatches a duplicate of that member after the sink recovers. No
// competing per-incident session may be opened for the member.
func TestDispatch_StormClaimedDuplicate_NoCompetingIncidentSession(t *testing.T) {
	t.Parallel()
	base, fd := newFlakySessionDaemon(t)
	d, sigs := newStormDispatcher(t, base, 4)
	ctx := context.Background()

	// Two healthy per-incident opens (sess-1, sess-2) before the storm.
	d.DispatchSignal(ctx, sigs[0])
	d.DispatchSignal(ctx, sigs[1])
	if got := fd.sessionsCreated(); got != 2 {
		t.Fatalf("setup: want 2 per-incident sessions before the outage, got %d", got)
	}

	// The sink outage covers BOTH the formation open and the first
	// attach's retry open.
	fd.setDown(true)

	// sigs[2] forms the storm; its one un-retried OpenIncident fails, so
	// stormFormed returns early — the storm is committed but session-less.
	d.DispatchSignal(ctx, sigs[2])
	if got := d.storm.ActiveStorms(); got != 1 {
		t.Fatalf("setup: ActiveStorms = %d, want 1 (the correlator committed the storm before the failed open)", got)
	}

	// sigs[3] is a late arrival on the same blast-radius key: StormAttached
	// → retryStormOpen (#94), which also fails while the sink is down →
	// AttachToStorm(key, "", fingerprint, ref). The member entry is now
	// storm-claimed yet session-less.
	memberKey := sigs[3].CanonicalKey()
	d.DispatchSignal(ctx, sigs[3])

	fd.setDown(false)

	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("error")); got != 2 {
		t.Fatalf("setup: want 2 failed opens while down (formation + attach retry), got sessionCreates{error}=%v", got)
	}
	if got := testutil.ToFloat64(d.metrics.stormMembers.WithLabelValues("attached")); got != 1 {
		t.Fatalf("setup: want 1 attached member (sigs[3] took the storm attach path), got stormMembers{attached}=%v", got)
	}
	if sid, ok := d.dedup.LookupSession(memberKey); ok || sid != "" {
		t.Fatalf("setup: storm member must be session-less after the failed attach, got LookupSession=(%q,%v)", sid, ok)
	}

	// Sink recovered. A DUPLICATE of the storm-claimed member arrives —
	// the same critical-class key, one advancing symptom event. Duplicates
	// bypass storm correlation, so the #96 retry guard decides its fate on
	// result.SessionID=="" alone, never the entry's storm fingerprint.
	dup := sigs[3]
	dup.LastSeen = dup.LastSeen.Add(30 * time.Second)
	d.DispatchSignal(ctx, dup)

	// Storm suppression must hold: NO competing per-incident session may
	// be opened for a storm member. Under the bug, retryIncidentOpen opens
	// a fresh session and delivers a kind=k8s-event open for pay-4 into it,
	// overwriting the storm binding (the §7.5 N-session fan-out).
	for _, in := range fd.injectLog() {
		msg := messageOf(t, in.Body)
		if strings.Contains(msg, `"kind":"k8s-event"`) && strings.Contains(msg, `"name":"pay-4"`) {
			t.Errorf("competing per-incident session (sid=%q) opened for storm member pay-4 — a DUPLICATE of a storm-claimed key passed the #96 retry guard and overwrote the storm binding (issue #104); payload: %.160s",
				in.SessionID, msg)
		}
	}
}
