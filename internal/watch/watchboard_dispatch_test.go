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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// boardClock is a settable fake clock for deterministic flush-age and
// wire-pin tests.
type boardClock struct{ now time.Time }

func (c *boardClock) Now() time.Time { return c.now }

// newBoardDispatcher wires a per-incident dispatcher with §7.7
// severity routing + watchboard over the routing fake daemon.
func newBoardDispatcher(t *testing.T, base string, batch int, flush time.Duration, rotate int) (*dispatcher, *boardClock) {
	t.Helper()
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	m := newMetrics()
	d := &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0)),
		dedup:    dedup,
		injector: inj,
		metrics:  m,
		cluster:  "prod-us-central1",
		mode:     "per-incident",
		routing:  engine.NewRoutingPolicy(nil),
	}
	clock := &boardClock{now: time.Date(2026, 7, 24, 11, 30, 0, 0, time.UTC)}
	d.board = newWatchboard(watchboardConfig{
		injector:      inj,
		metrics:       m,
		cluster:       "prod-us-central1",
		batch:         batch,
		flushInterval: flush,
		rotateAfter:   rotate,
	})
	d.board.clock = clock.Now
	d.board.bind = d.bindWatchboardIncident
	return d, clock
}

// warningSignal fabricates the i-th warning-class object-state signal,
// deterministic for the wire pins.
func warningSignal(i int) engine.Signal {
	ts := time.Date(2026, 7, 24, 11, 0, i, 0, time.UTC)
	return engine.Signal{
		Kind:     "objectstate.restart_burst",
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityWarning,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: fmt.Sprintf("wuid-%d", i), Reason: "restart_burst"},
			Namespace:    "shop",
			KindOfObject: "Pod",
			Name:         fmt.Sprintf("cart-%d", i),
			Message:      "3 container restarts within 10m0s",
			FirstSeen:    ts,
			LastSeen:     ts,
		},
	}
}

// infoSignal fabricates an info-class signal (no shipped source emits
// info yet — §7.7 info routing is exercised via a crafted signal and
// via --severity overrides).
func infoSignal() engine.Signal {
	sig := warningSignal(9)
	sig.Kind = "custom.heartbeat"
	sig.Key.Reason = "heartbeat"
	sig.Severity = engine.SeverityInfo
	return sig
}

// kindCounts tallies captured injects by payload "kind" per session.
func kindCounts(t *testing.T, injects []routedInject) map[string]map[string]int {
	t.Helper()
	out := map[string]map[string]int{}
	for _, in := range injects {
		var p struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal([]byte(messageOf(t, in.Body)), &p); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		if out[p.Kind] == nil {
			out[p.Kind] = map[string]int{}
		}
		out[p.Kind][in.SessionID]++
	}
	return out
}

// TestSeverityRoutingTable is the §7.7 routing matrix: each severity
// class in each mode. Per-incident mode routes by class; shared mode
// keeps its frozen contract — EVERY severity goes to --target-session
// and the watchboard machinery stays out of the path.
func TestSeverityRoutingTable(t *testing.T) {
	t.Parallel()

	t.Run("per-incident", func(t *testing.T) {
		t.Parallel()
		base, injects := newRoutingFakeDaemon(t)
		d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
		ctx := context.Background()

		d.DispatchSignal(ctx, crashLoopSignal()) // critical
		d.DispatchSignal(ctx, warningSignal(1))  // warning
		d.DispatchSignal(ctx, infoSignal())      // info

		// critical → per-incident session, exactly as before §7.7.
		if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 1 {
			t.Errorf("session creates = %v, want 1 (critical only)", got)
		}
		if len(*injects) != 1 || (*injects)[0].SessionID != "sess-1" {
			t.Fatalf("want exactly the critical inject in sess-1, got %+v", *injects)
		}
		if !strings.Contains(messageOf(t, (*injects)[0].Body), `"kind":"k8s-event"`) {
			t.Errorf("critical inject is not the frozen k8s-event payload")
		}
		// warning → buffered on the watchboard, no session opened.
		if got := testutil.ToFloat64(d.metrics.watchboardBuffered); got != 1 {
			t.Errorf("watchboard_buffered = %v, want 1", got)
		}
		if got := testutil.ToFloat64(d.metrics.watchboardEntries.WithLabelValues("objectstate.restart_burst")); got != 1 {
			t.Errorf("watchboard_entries = %v, want 1", got)
		}
		// info → counted + dropped (TODO(M3 store)), never silent.
		if got := testutil.ToFloat64(d.metrics.infoDropped.WithLabelValues("custom.heartbeat")); got != 1 {
			t.Errorf("info_dropped = %v, want 1", got)
		}
	})

	t.Run("shared", func(t *testing.T) {
		t.Parallel()
		base, injects := newRoutingFakeDaemon(t)
		d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
		d.mode = "shared"
		d.targetSid = "sess-shared"
		d.board = nil // realMain never builds a board in shared mode
		ctx := context.Background()

		d.DispatchSignal(ctx, crashLoopSignal())
		d.DispatchSignal(ctx, warningSignal(1))
		d.DispatchSignal(ctx, infoSignal())

		if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 0 {
			t.Errorf("shared mode must not create sessions, got %v", got)
		}
		if len(*injects) != 3 {
			t.Fatalf("shared mode routes ALL severities to --target-session: want 3 injects, got %d", len(*injects))
		}
		for _, in := range *injects {
			if in.SessionID != "sess-shared" {
				t.Errorf("inject landed in %q, want sess-shared", in.SessionID)
			}
		}
		if got := testutil.ToFloat64(d.metrics.infoDropped.WithLabelValues("custom.heartbeat")); got != 0 {
			t.Errorf("shared mode must not drop info signals, info_dropped = %v", got)
		}
	})
}

// TestSeverityOverride_RoutesByEffectiveClass: --severity overrides
// re-route a kind — a critical-by-default kind demoted to warning
// batches onto the watchboard; a warning kind demoted to info is
// counted + dropped; a warning kind promoted to critical opens a
// per-incident session.
func TestSeverityOverride_RoutesByEffectiveClass(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
	overrides, err := engine.ParseSeverityOverrides([]string{
		"k8s-event=warning,objectstate.restart_burst=critical",
		"custom.heartbeat=warning",
	})
	if err != nil {
		t.Fatalf("ParseSeverityOverrides: %v", err)
	}
	d.routing = engine.NewRoutingPolicy(overrides)
	ctx := context.Background()

	d.DispatchSignal(ctx, crashLoopSignal()) // critical demoted → watchboard
	d.DispatchSignal(ctx, warningSignal(1))  // warning promoted → per-incident
	d.DispatchSignal(ctx, infoSignal())      // info promoted → watchboard

	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 1 {
		t.Errorf("session creates = %v, want 1 (the promoted warning)", got)
	}
	if len(*injects) != 1 || !strings.Contains(messageOf(t, (*injects)[0].Body), `"kind":"objectstate.restart_burst"`) {
		t.Fatalf("want exactly the promoted signal injected per-incident, got %+v", *injects)
	}
	if got := testutil.ToFloat64(d.metrics.watchboardBuffered); got != 2 {
		t.Errorf("watchboard_buffered = %v, want 2 (demoted critical + promoted info)", got)
	}
}

// TestWatchboard_CountThresholdFlush: the digest flushes as soon as
// --watchboard-batch warnings are buffered; the session is created
// lazily at that first flush.
func TestWatchboard_CountThresholdFlush(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 5, time.Minute, 200)
	ctx := context.Background()

	for i := 1; i <= 4; i++ {
		d.DispatchSignal(ctx, warningSignal(i))
	}
	if len(*injects) != 0 {
		t.Fatalf("below the batch threshold nothing may flush, got %d injects", len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 0 {
		t.Fatalf("watchboard session must be created lazily, got %v creates", got)
	}
	d.DispatchSignal(ctx, warningSignal(5))
	if len(*injects) != 1 || (*injects)[0].SessionID != "sess-1" {
		t.Fatalf("want the digest flushed to the lazily-created sess-1, got %+v", *injects)
	}
	var digest inject.WatchboardDigestPayload
	if err := json.Unmarshal([]byte(messageOf(t, (*injects)[0].Body)), &digest); err != nil {
		t.Fatalf("unmarshal digest: %v", err)
	}
	if digest.Kind != inject.KindWatchboardDigest || len(digest.Entries) != 5 {
		t.Errorf("digest kind/entries = %q/%d, want watchboard.digest/5", digest.Kind, len(digest.Entries))
	}
	if got := testutil.ToFloat64(d.metrics.watchboardDigests); got != 1 {
		t.Errorf("watchboard_digests = %v, want 1", got)
	}
	if got := testutil.ToFloat64(d.metrics.watchboardBuffered); got != 0 {
		t.Errorf("watchboard_buffered = %v, want 0 after flush", got)
	}
	// Every flushed warning is bound to the watchboard session so
	// followups and §7.4 outcomes route there.
	for i := 1; i <= 5; i++ {
		if sid, ok := d.dedup.LookupSession(warningSignal(i).Key); !ok || sid != "sess-1" {
			t.Errorf("warning %d binding = (%q, %v), want (sess-1, true)", i, sid, ok)
		}
	}
}

// TestWatchboard_IntervalFlush: a partial buffer flushes once its
// oldest entry ages past --watchboard-flush, and not before.
func TestWatchboard_IntervalFlush(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, clock := newBoardDispatcher(t, base, 100, 60*time.Second, 200)
	ctx := context.Background()

	d.DispatchSignal(ctx, warningSignal(1))
	d.DispatchSignal(ctx, warningSignal(2))
	d.board.Tick(ctx)
	if len(*injects) != 0 {
		t.Fatalf("tick before the interval must not flush, got %d injects", len(*injects))
	}
	clock.now = clock.now.Add(61 * time.Second)
	d.board.Tick(ctx)
	if len(*injects) != 1 {
		t.Fatalf("tick past the interval must flush, got %d injects", len(*injects))
	}
	var digest inject.WatchboardDigestPayload
	if err := json.Unmarshal([]byte(messageOf(t, (*injects)[0].Body)), &digest); err != nil {
		t.Fatalf("unmarshal digest: %v", err)
	}
	if len(digest.Entries) != 2 {
		t.Errorf("digest entries = %d, want 2", len(digest.Entries))
	}
}

// TestWatchboard_MixedTriggers: whichever trigger fires first wins —
// an interval flush of a partial buffer, then a count flush of a full
// one, on the same board.
func TestWatchboard_MixedTriggers(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, clock := newBoardDispatcher(t, base, 3, 60*time.Second, 200)
	ctx := context.Background()

	// Interval first: 2 < batch, aged past the interval.
	d.DispatchSignal(ctx, warningSignal(1))
	d.DispatchSignal(ctx, warningSignal(2))
	clock.now = clock.now.Add(2 * time.Minute)
	d.board.Tick(ctx)
	// Count second: the 3rd..5th warnings hit the batch threshold
	// with no tick involved.
	for i := 3; i <= 5; i++ {
		d.DispatchSignal(ctx, warningSignal(i))
	}
	if len(*injects) != 2 {
		t.Fatalf("want 2 digests (interval, then count), got %d", len(*injects))
	}
	for i, wantEntries := range []int{2, 3} {
		var digest inject.WatchboardDigestPayload
		if err := json.Unmarshal([]byte(messageOf(t, (*injects)[i].Body)), &digest); err != nil {
			t.Fatalf("unmarshal digest %d: %v", i, err)
		}
		if len(digest.Entries) != wantEntries {
			t.Errorf("digest %d entries = %d, want %d", i, len(digest.Entries), wantEntries)
		}
		if digest.Sequence != i+1 {
			t.Errorf("digest %d sequence = %d, want %d", i, digest.Sequence, i+1)
		}
	}
	// Both digests landed in the SAME session — no spurious rotation.
	if (*injects)[0].SessionID != "sess-1" || (*injects)[1].SessionID != "sess-1" {
		t.Errorf("digests split across sessions: %+v", *injects)
	}
}

// TestWatchboard_SizeBasedRotation is the §15 Q2 decision under test:
// after --watchboard-rotate digest injects the NEXT flush opens a
// fresh session, the old session gets the kind=watchboard.rotated
// lineage record, and — critically — dedup bindings into the OLD
// session stay valid: bindings are per-incident, only NEW warnings go
// to the successor.
func TestWatchboard_SizeBasedRotation(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 1, time.Minute, 2)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		d.DispatchSignal(ctx, warningSignal(i)) // batch=1: every warning flushes
	}

	// Wire order: digest 1 → sess-1, digest 2 → sess-1, rotated →
	// sess-1, digest 3 → sess-2.
	if len(*injects) != 4 {
		t.Fatalf("want 4 injects (2 digests + rotated + successor digest), got %d", len(*injects))
	}
	counts := kindCounts(t, *injects)
	if counts["watchboard.digest"]["sess-1"] != 2 || counts["watchboard.digest"]["sess-2"] != 1 {
		t.Errorf("digest distribution = %v, want 2 in sess-1 + 1 in sess-2", counts["watchboard.digest"])
	}
	if counts["watchboard.rotated"]["sess-1"] != 1 {
		t.Errorf("rotated record = %v, want exactly 1 in the CLOSED session sess-1", counts["watchboard.rotated"])
	}
	var rotated inject.WatchboardRotatedPayload
	if err := json.Unmarshal([]byte(messageOf(t, (*injects)[2].Body)), &rotated); err != nil {
		t.Fatalf("unmarshal rotated: %v", err)
	}
	if rotated.SuccessorSessionID != "sess-2" || rotated.InjectsCount != 2 {
		t.Errorf("rotated successor/injects = %q/%d, want sess-2/2", rotated.SuccessorSessionID, rotated.InjectsCount)
	}
	if got := testutil.ToFloat64(d.metrics.watchboardRotations); got != 1 {
		t.Errorf("watchboard_rotations = %v, want 1", got)
	}

	// Lineage coordinates: successor digest is generation 2, sequence 1.
	var succ inject.WatchboardDigestPayload
	if err := json.Unmarshal([]byte(messageOf(t, (*injects)[3].Body)), &succ); err != nil {
		t.Fatalf("unmarshal successor digest: %v", err)
	}
	if succ.BoardGeneration != 2 || succ.Sequence != 1 {
		t.Errorf("successor digest generation/sequence = %d/%d, want 2/1", succ.BoardGeneration, succ.Sequence)
	}

	// Bindings survive rotation unchanged: incidents flushed before
	// the rotation keep routing to the old watchboard session.
	for i, wantSid := range map[int]string{1: "sess-1", 2: "sess-1", 3: "sess-2"} {
		if sid, ok := d.dedup.LookupSession(warningSignal(i).Key); !ok || sid != wantSid {
			t.Errorf("warning %d binding = (%q, %v), want (%s, true)", i, sid, ok, wantSid)
		}
	}
	// And a §7.4 outcome for a pre-rotation incident still lands in
	// the OLD session — the closed-loop routing follows the binding.
	res := resolvedSignalFor(warningSignal(1), engine.KindResolved)
	d.DispatchSignal(ctx, res)
	last := (*injects)[len(*injects)-1]
	if last.SessionID != "sess-1" || !strings.Contains(messageOf(t, last.Body), `"kind":"resolved"`) {
		t.Errorf("pre-rotation outcome landed in %q, want the old watchboard sess-1", last.SessionID)
	}
}

// TestWatchboardDigest_ExactWireShape pins the §7.7 kind=
// watchboard.digest payload byte-for-byte. SCHEMA-STABLE (AX +
// playbooks parse structurally): treat a failing pin as a breaking
// schema change, never as a test to update.
func TestWatchboardDigest_ExactWireShape(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 2, time.Minute, 200)
	ctx := context.Background()

	d.DispatchSignal(ctx, warningSignal(1))
	d.DispatchSignal(ctx, warningSignal(2))
	if len(*injects) != 1 {
		t.Fatalf("want 1 digest inject, got %d", len(*injects))
	}

	// Fingerprint(objectstate.restart_burst, restart_burst, Pod, "").
	const fp = "sha256:e869fa95d9251a5a36fcceaa7e081d48faac44c90e719df563b2d784f723db70"
	want := `{"kind":"watchboard.digest","cluster":"prod-us-central1","board_generation":1,"sequence":1,"window_start":"2026-07-24T11:30:00Z","window_end":"2026-07-24T11:30:00Z","entries":[` +
		`{"kind":"objectstate.restart_burst","fingerprint":"` + fp + `","reason":"restart_burst","namespace":"shop","kind_of_object":"Pod","name":"cart-1","uid":"wuid-1","count":1,"first_seen":"2026-07-24T11:00:01Z","last_seen":"2026-07-24T11:00:01Z"},` +
		`{"kind":"objectstate.restart_burst","fingerprint":"` + fp + `","reason":"restart_burst","namespace":"shop","kind_of_object":"Pod","name":"cart-2","uid":"wuid-2","count":1,"first_seen":"2026-07-24T11:00:02Z","last_seen":"2026-07-24T11:00:02Z"}]}`
	if got := messageOf(t, (*injects)[0].Body); got != want {
		t.Errorf("digest payload drifted from the schema-stable wire shape:\n got: %s\nwant: %s", got, want)
	}
}

// TestWatchboardRotated_ExactWireShape pins the §15 Q2 kind=
// watchboard.rotated lineage payload byte-for-byte. SCHEMA-STABLE.
func TestWatchboardRotated_ExactWireShape(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 1, time.Minute, 1)
	ctx := context.Background()

	d.DispatchSignal(ctx, warningSignal(1)) // digest 1 → sess-1
	d.DispatchSignal(ctx, warningSignal(2)) // rotation: rotated → sess-1, digest → sess-2
	if len(*injects) != 3 {
		t.Fatalf("want 3 injects, got %d", len(*injects))
	}
	want := `{"kind":"watchboard.rotated","cluster":"prod-us-central1","board_generation":1,"successor_session_id":"sess-2","injects_count":1,"rotated_at":"2026-07-24T11:30:00Z"}`
	if got := messageOf(t, (*injects)[1].Body); got != want {
		t.Errorf("rotated payload drifted from the schema-stable wire shape:\n got: %s\nwant: %s", got, want)
	}
	if (*injects)[1].SessionID != "sess-1" {
		t.Errorf("rotated record landed in %q, want the closed session sess-1", (*injects)[1].SessionID)
	}
}

// TestStormBypassesWarningRouting: storms ALWAYS open a session, even
// when every member — and therefore the storm itself — is
// warning-class. §7.5's purpose is ONE aggregate incident an agent
// works; a watchboard digest entry is not that. Members already
// sitting in the watchboard buffer stay on the digest for the record,
// but their bindings follow the storm.
func TestStormBypassesWarningRouting(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, sigs := newStormDispatcher(t, base, 3)
	// Attach §7.7 routing + a board that would swallow the warnings.
	m := d.metrics
	d.routing = engine.NewRoutingPolicy(nil)
	d.board = newWatchboard(watchboardConfig{
		injector:      d.injector,
		metrics:       m,
		cluster:       d.cluster,
		batch:         100,
		flushInterval: time.Minute,
		rotateAfter:   200,
	})
	d.board.bind = d.bindWatchboardIncident
	ctx := context.Background()

	for i := range sigs {
		sigs[i].Severity = engine.SeverityWarning
		d.DispatchSignal(ctx, sigs[i])
	}

	// The first 2 warnings buffered (no per-incident sessions); the
	// 3rd formed the storm, which MUST open its own session.
	if got := testutil.ToFloat64(m.sessionCreates.WithLabelValues("ok")); got != 1 {
		t.Fatalf("session creates = %v, want exactly 1 (the storm session)", got)
	}
	var storm inject.StormPayload
	if len(*injects) != 1 {
		t.Fatalf("want exactly the storm inject, got %d", len(*injects))
	}
	if err := json.Unmarshal([]byte(messageOf(t, (*injects)[0].Body)), &storm); err != nil {
		t.Fatalf("unmarshal storm: %v", err)
	}
	if storm.Kind != inject.KindStorm || storm.Severity != "warning" {
		t.Errorf("storm kind/severity = %q/%q, want storm/warning (max member severity, below the size escalator)", storm.Kind, storm.Severity)
	}

	// Flushing the board afterwards must NOT steal the members'
	// bindings: the storm claimed them, the digest is just the record
	// of the observed warnings.
	d.board.FlushNow(ctx)
	for _, sig := range sigs {
		if sid, ok := d.dedup.LookupSession(sig.Key); !ok || sid != "sess-1" {
			t.Errorf("member %s binding = (%q, %v), want the storm session (sess-1, true)", sig.Name, sid, ok)
		}
	}
}

// TestWatchboardKindConstants pins the watchboard wire kinds — the
// strings AX and playbook skills match.
func TestWatchboardKindConstants(t *testing.T) {
	t.Parallel()
	if inject.KindWatchboardDigest != "watchboard.digest" {
		t.Errorf("KindWatchboardDigest = %q", inject.KindWatchboardDigest)
	}
	if inject.KindWatchboardRotated != "watchboard.rotated" {
		t.Errorf("KindWatchboardRotated = %q", inject.KindWatchboardRotated)
	}
}

// TestDispatcher_NilRoutingKeepsLegacyPipeline: a dispatcher without
// the §7.7 stage (routing == nil, board == nil) behaves exactly as
// before this change — warnings open per-incident sessions. This is
// the seam the frozen M0/M2 unit tests rely on.
func TestDispatcher_NilRoutingKeepsLegacyPipeline(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)

	d.DispatchSignal(context.Background(), warningSignal(1))
	if len(*injects) != 1 || (*injects)[0].SessionID != "sess-1" {
		t.Fatalf("nil routing must keep the pre-§7.7 per-incident path, got %+v", *injects)
	}
}
