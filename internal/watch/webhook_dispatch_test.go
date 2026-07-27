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

// Webhook-sink dispatcher tests (docs/agent-sink-design.md): the full
// pipeline delivered into a generic webhook fixture. The *_ExactWire*
// pins here freeze the webhook wire contract the same way the
// core-agent envelope pins froze the daemon wire: POST /incidents and
// POST /incidents/<id>/events carry the signal-schema v1 payload JSON
// as the request body — the SAME bytes that ride inside the
// core-agent envelope's "message", never wrapped. Treat a failing pin
// as a breaking contract change, never as a test to update.

package watch

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// webhookHit is one captured POST against the webhook fixture.
type webhookHit struct {
	Path string
	Auth string
	Body string
}

// newWebhookFixture returns an httptest server speaking the generic
// webhook contract with counting ids (inc-1, inc-2, …), plus the
// capture slice. statusOnOpen lets error-path tests fail the open.
func newWebhookFixture(t *testing.T) (baseURL string, hits *[]webhookHit, failOpens *bool) {
	t.Helper()
	captured := make([]webhookHit, 0, 8)
	fail := false
	counter := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		captured = append(captured, webhookHit{Path: r.URL.Path, Auth: r.Header.Get("Authorization"), Body: string(body)})
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/incidents":
			if fail {
				http.Error(w, "receiver down", http.StatusServiceUnavailable)
				return
			}
			counter++
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"id":"inc-%d"}`, counter)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/incidents/") && strings.HasSuffix(r.URL.Path, "/events"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &captured, &fail
}

// newWebhookDispatcher wires a per-incident dispatcher over the
// webhook sink — the same shape as newRecoveryDispatcher, different
// sink.
func newWebhookDispatcher(t *testing.T, base, bearer string) *dispatcher {
	t.Helper()
	ws, err := inject.NewWebhookSink(inject.WebhookConfig{URL: base, BearerToken: bearer})
	if err != nil {
		t.Fatalf("NewWebhookSink: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	return &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0)),
		dedup:    dedup,
		injector: ws,
		metrics:  newMetrics(),
		cluster:  "prod-us-central1",
		mode:     "per-incident",
	}
}

// TestWebhookDispatch_ExactOpenWireShape pins the webhook OPEN for
// the frozen M0 kind: POST /incidents whose body is the k8s-event
// payload byte-for-byte — the exact bytes the core-agent envelope pin
// (TestDispatcher_ExactInjectPayloadWireShape) freezes as the
// envelope's inner message.
func TestWebhookDispatch_ExactOpenWireShape(t *testing.T) {
	t.Parallel()
	base, hits, _ := newWebhookFixture(t)
	d := newWebhookDispatcher(t, base, "tok_hook")

	d.DispatchSignal(context.Background(), crashLoopSignal())

	if len(*hits) != 1 {
		t.Fatalf("expected 1 webhook POST, got %d", len(*hits))
	}
	got := (*hits)[0]
	if got.Path != "/incidents" {
		t.Errorf("open path = %q, want /incidents", got.Path)
	}
	if got.Auth != "Bearer tok_hook" {
		t.Errorf("Authorization = %q, want Bearer tok_hook (--sink-token-env)", got.Auth)
	}
	want := `{"kind":"k8s-event","reason":"CrashLoopBackOff","namespace":"checkout","kind_of_object":"Pod","name":"checkout-svc-7b9d-x4kzq","container":"spec.containers{server}","uid":"abc-123","message":"Back-off restarting failed container","count":1,"first_seen":"2026-07-24T10:00:00Z","last_seen":"2026-07-24T10:05:00Z","cluster":"prod-us-central1","context":{"controller_ref":"ReplicaSet/checkout-svc-7b9d"},"type":"Warning"}`
	if got.Body != want {
		t.Errorf("webhook open body drifted from the frozen wire shape:\n got: %s\nwant: %s", got.Body, want)
	}
	// The receiver's id is the binding, same as a session id.
	if sid, ok := d.dedup.LookupSession(crashLoopSignal().Key); !ok || sid != "inc-1" {
		t.Errorf("binding = (%q, %v), want (inc-1, true)", sid, ok)
	}
	if testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")) != 1 {
		t.Errorf("session_creates{ok} must count webhook opens")
	}
}

// TestWebhookDispatch_ExactResolvedAppendWireShape pins the webhook
// APPEND for the §7.4 outcome record: POST /incidents/<id>/events,
// body = the resolved payload byte-for-byte (the exact inner-message
// bytes TestDispatchResolved_ExactWireShape freezes).
func TestWebhookDispatch_ExactResolvedAppendWireShape(t *testing.T) {
	t.Parallel()
	base, hits, _ := newWebhookFixture(t)
	d := newWebhookDispatcher(t, base, "")
	ctx := context.Background()

	orig := crashLoopSignal()
	d.DispatchSignal(ctx, orig)
	d.DispatchSignal(ctx, resolvedSignalFor(orig, engine.KindResolved))

	if len(*hits) != 2 {
		t.Fatalf("expected open + append, got %d POSTs", len(*hits))
	}
	got := (*hits)[1]
	if got.Path != "/incidents/inc-1/events" {
		t.Errorf("append path = %q, want /incidents/inc-1/events", got.Path)
	}
	want := `{"kind":"resolved","reason":"CrashLoopBackOff","namespace":"checkout","kind_of_object":"Pod","name":"checkout-svc-7b9d-x4kzq","container":"spec.containers{server}","uid":"abc-123","fingerprint":"sha256:1f4e6a7b8c9d0e1f2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091","cluster":"prod-us-central1","first_seen":"2026-07-24T10:00:00Z","resolved_at":"2026-07-24T10:07:30Z","cleared_after":"2m30s","observed_stable_for":"5m0s","resolution":"recovered","context":{"controller_ref":"ReplicaSet/checkout-svc-7b9d"}}`
	if got.Body != want {
		t.Errorf("webhook resolved append drifted from the frozen wire shape:\n got: %s\nwant: %s", got.Body, want)
	}
}

// TestWebhookDispatch_ExactStormOpenWireShape pins the webhook OPEN
// for the §7.5 aggregate: the storm payload as the /incidents body,
// with the pre-storm members' webhook ids riding representative_
// incidents, and the supersede pointers appended into the members'
// own incidents.
func TestWebhookDispatch_ExactStormOpenWireShape(t *testing.T) {
	t.Parallel()
	base, hits, _ := newWebhookFixture(t)
	ws, err := inject.NewWebhookSink(inject.WebhookConfig{URL: base})
	if err != nil {
		t.Fatalf("NewWebhookSink: %v", err)
	}
	// Reuse the scripted-topology storm dispatcher, swapping the sink.
	d, sigs := newStormDispatcher(t, "http://unused.invalid", 3)
	d.injector = ws
	ctx := context.Background()
	for _, sig := range sigs {
		d.DispatchSignal(ctx, sig)
	}

	// Wire order: open inc-1 (event), open inc-2 (event), open inc-3
	// (storm), append supersede → inc-1, append supersede → inc-2.
	if len(*hits) != 5 {
		t.Fatalf("expected 5 webhook POSTs, got %d", len(*hits))
	}
	const memberFP = "sha256:e2957792a0b3ad9e29db2051dbc69ff01dfe3a52da8dbb6d1331aa44fe946f8b" // Fingerprint(k8s-event, CrashLoopBackOff, Pod, "")
	const stormFP = "sha256:8fdc3aab7c6444c4a8c4baba5ddcac72d6db1310fd108a9fd6cc09c28e939264"  // Fingerprint(storm, CrashLoopBackOff, Node, "")

	if got := (*hits)[2].Path; got != "/incidents" {
		t.Errorf("storm open path = %q, want /incidents", got)
	}
	wantStorm := `{"kind":"storm","fingerprint":"` + stormFP + `","severity":"critical","cluster":"prod-us-central1","ancestor_kind":"Node","ancestor_name":"gke-a","reason":"CrashLoopBackOff","message":"Node gke-a: 3 incidents across 3 namespace(s) share this blast-radius key; 3 representative incident(s) attached; member sessions are suppressed and route here","affected_count":3,"namespaces_count":3,"first_seen":"2026-07-24T10:00:01Z","last_seen":"2026-07-24T10:00:03Z","representative_incidents":[{"fingerprint":"` + memberFP + `","reason":"CrashLoopBackOff","namespace":"shop","kind_of_object":"Pod","name":"pay-1","uid":"uid-1","session_id":"inc-1"},{"fingerprint":"` + memberFP + `","reason":"CrashLoopBackOff","namespace":"web","kind_of_object":"Pod","name":"pay-2","uid":"uid-2","session_id":"inc-2"},{"fingerprint":"` + memberFP + `","reason":"CrashLoopBackOff","namespace":"api","kind_of_object":"Pod","name":"pay-3","uid":"uid-3"}],"member_fingerprints":["` + memberFP + `","` + memberFP + `","` + memberFP + `"],"context":{"node":"gke-a"}}`
	if got := (*hits)[2].Body; got != wantStorm {
		t.Errorf("webhook storm open drifted from the frozen wire shape:\n got: %s\nwant: %s", got, wantStorm)
	}
	wantSuperseded := `{"kind":"storm.member_superseded","storm_fingerprint":"` + stormFP + `","storm_session_id":"inc-3","ancestor_kind":"Node","ancestor_name":"gke-a","cluster":"prod-us-central1","message":"this incident was folded into the Node gke-a storm (3 incidents); further followups and the outcome record route to the storm session","incident":{"fingerprint":"` + memberFP + `","reason":"CrashLoopBackOff","namespace":"shop","kind_of_object":"Pod","name":"pay-1","uid":"uid-1","session_id":"inc-1"}}`
	if got := (*hits)[3]; got.Path != "/incidents/inc-1/events" || got.Body != wantSuperseded {
		t.Errorf("webhook supersede append drifted:\npath: %s (want /incidents/inc-1/events)\n got: %s\nwant: %s", got.Path, got.Body, wantSuperseded)
	}
	// Every member routes to the storm incident now.
	for _, sig := range sigs {
		if sid, ok := d.dedup.LookupSession(sig.Key); !ok || sid != "inc-3" {
			t.Errorf("member %s binding = (%q, %v), want (inc-3, true)", sig.Name, sid, ok)
		}
	}
}

// TestWebhookDispatch_ExactWatchboardDigestWireShape pins the webhook
// OPEN for the §7.7 digest: a stateless sink has no create-empty
// verb, so the lazily-created watchboard incident opens WITH the
// first digest as the /incidents body — byte-identical to the digest
// the core-agent pin freezes.
func TestWebhookDispatch_ExactWatchboardDigestWireShape(t *testing.T) {
	t.Parallel()
	base, hits, _ := newWebhookFixture(t)
	d := newWebhookDispatcher(t, base, "")
	d.routing = engine.NewRoutingPolicy(nil)
	clock := &boardClock{now: time.Date(2026, 7, 24, 11, 30, 0, 0, time.UTC)}
	d.board = newWatchboard(watchboardConfig{
		injector:      d.injector,
		metrics:       d.metrics,
		cluster:       "prod-us-central1",
		batch:         2,
		flushInterval: time.Minute,
		rotateAfter:   200,
	})
	d.board.clock = clock.Now
	d.board.bind = d.bindWatchboardIncident
	ctx := context.Background()

	d.DispatchSignal(ctx, warningSignal(1))
	d.DispatchSignal(ctx, warningSignal(2))

	if len(*hits) != 1 {
		t.Fatalf("expected 1 webhook POST (the digest open), got %d", len(*hits))
	}
	if got := (*hits)[0].Path; got != "/incidents" {
		t.Errorf("digest open path = %q, want /incidents", got)
	}
	const fp = "sha256:e869fa95d9251a5a36fcceaa7e081d48faac44c90e719df563b2d784f723db70" // Fingerprint(objectstate.restart_burst, restart_burst, Pod, "")
	want := `{"kind":"watchboard.digest","cluster":"prod-us-central1","board_generation":1,"sequence":1,"window_start":"2026-07-24T11:30:00Z","window_end":"2026-07-24T11:30:00Z","entries":[` +
		`{"kind":"objectstate.restart_burst","fingerprint":"` + fp + `","reason":"restart_burst","namespace":"shop","kind_of_object":"Pod","name":"cart-1","uid":"wuid-1","count":1,"first_seen":"2026-07-24T11:00:01Z","last_seen":"2026-07-24T11:00:01Z"},` +
		`{"kind":"objectstate.restart_burst","fingerprint":"` + fp + `","reason":"restart_burst","namespace":"shop","kind_of_object":"Pod","name":"cart-2","uid":"wuid-2","count":1,"first_seen":"2026-07-24T11:00:02Z","last_seen":"2026-07-24T11:00:02Z"}]}`
	if got := (*hits)[0].Body; got != want {
		t.Errorf("webhook digest open drifted from the frozen wire shape:\n got: %s\nwant: %s", got, want)
	}
	// Flushed warnings bind to the digest's incident id.
	for i := 1; i <= 2; i++ {
		if sid, ok := d.dedup.LookupSession(warningSignal(i).Key); !ok || sid != "inc-1" {
			t.Errorf("warning %d binding = (%q, %v), want (inc-1, true)", i, sid, ok)
		}
	}
}

// TestWebhookDispatch_WatchboardRotationStatelessOrder: rotation on a
// stateless sink opens the successor WITH its first digest, then
// appends the kind=watchboard.rotated lineage pointer to the CLOSED
// incident — the one wire-order difference from the core-agent sink
// (whose §15 Q2 rotated-before-digest order is pinned separately and
// unchanged).
func TestWebhookDispatch_WatchboardRotationStatelessOrder(t *testing.T) {
	t.Parallel()
	base, hits, _ := newWebhookFixture(t)
	d := newWebhookDispatcher(t, base, "")
	d.routing = engine.NewRoutingPolicy(nil)
	clock := &boardClock{now: time.Date(2026, 7, 24, 11, 30, 0, 0, time.UTC)}
	d.board = newWatchboard(watchboardConfig{
		injector:      d.injector,
		metrics:       d.metrics,
		cluster:       "prod-us-central1",
		batch:         1,
		flushInterval: time.Minute,
		rotateAfter:   1,
	})
	d.board.clock = clock.Now
	d.board.bind = d.bindWatchboardIncident
	ctx := context.Background()

	d.DispatchSignal(ctx, warningSignal(1)) // digest 1 opens inc-1
	d.DispatchSignal(ctx, warningSignal(2)) // rotation: successor opens with digest 2, then rotated → inc-1

	if len(*hits) != 3 {
		t.Fatalf("expected 3 webhook POSTs, got %d", len(*hits))
	}
	if got := (*hits)[1].Path; got != "/incidents" {
		t.Errorf("successor open path = %q, want /incidents (digest rides the open)", got)
	}
	if !strings.Contains((*hits)[1].Body, `"kind":"watchboard.digest"`) || !strings.Contains((*hits)[1].Body, `"board_generation":2,"sequence":1`) {
		t.Errorf("successor open body should be the generation-2 digest, got: %s", (*hits)[1].Body)
	}
	if got := (*hits)[2].Path; got != "/incidents/inc-1/events" {
		t.Errorf("rotated lineage path = %q, want the closed incident /incidents/inc-1/events", got)
	}
	wantRotated := `{"kind":"watchboard.rotated","cluster":"prod-us-central1","board_generation":1,"successor_session_id":"inc-2","injects_count":1,"rotated_at":"2026-07-24T11:30:00Z"}`
	if got := (*hits)[2].Body; got != wantRotated {
		t.Errorf("webhook rotated append drifted from the frozen wire shape:\n got: %s\nwant: %s", got, wantRotated)
	}
	if testutil.ToFloat64(d.metrics.watchboardRotations) != 1 {
		t.Errorf("watchboard_rotations = %v, want 1", testutil.ToFloat64(d.metrics.watchboardRotations))
	}
	// Pre-rotation bindings stay with the closed incident; the new
	// warning binds to the successor.
	if sid, _ := d.dedup.LookupSession(warningSignal(1).Key); sid != "inc-1" {
		t.Errorf("warning 1 binding = %q, want inc-1", sid)
	}
	if sid, _ := d.dedup.LookupSession(warningSignal(2).Key); sid != "inc-2" {
		t.Errorf("warning 2 binding = %q, want inc-2", sid)
	}
}

// TestWebhookDispatch_StatelessReceiverKeepsPipelineRouting: a 2xx
// receiver that returns no id still gets the full closed loop — the
// locally generated id keeps dedup binding and §7.4 outcome routing
// alive; the receiver sees it only in the append path.
func TestWebhookDispatch_StatelessReceiverKeepsPipelineRouting(t *testing.T) {
	t.Parallel()
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusOK) // no body at all — fully stateless
	}))
	t.Cleanup(srv.Close)
	d := newWebhookDispatcher(t, srv.URL, "")
	ctx := context.Background()

	orig := crashLoopSignal()
	d.DispatchSignal(ctx, orig)
	sid, ok := d.dedup.LookupSession(orig.Key)
	if !ok || !strings.HasPrefix(sid, "local-") {
		t.Fatalf("binding = (%q, %v), want a local- id binding", sid, ok)
	}
	d.DispatchSignal(ctx, resolvedSignalFor(orig, engine.KindResolved))
	if len(paths) != 2 || paths[1] != "/incidents/"+sid+"/events" {
		t.Errorf("paths = %v, want the outcome appended to /incidents/%s/events", paths, sid)
	}
	if testutil.ToFloat64(d.metrics.recoveriesObserved.WithLabelValues("recovered")) != 1 {
		t.Error("recoveries_observed{recovered} must count on the stateless path")
	}
}

// TestWebhookDispatch_OpenFailureCountsAndDrops: a failed open leaves
// no binding and counts like a failed session create — same posture
// as the core-agent sink.
func TestWebhookDispatch_OpenFailureCountsAndDrops(t *testing.T) {
	t.Parallel()
	base, hits, failOpens := newWebhookFixture(t)
	*failOpens = true
	d := newWebhookDispatcher(t, base, "")

	d.DispatchSignal(context.Background(), crashLoopSignal())

	if len(*hits) != 1 {
		t.Fatalf("expected exactly 1 attempted POST (no retries), got %d", len(*hits))
	}
	if _, ok := d.dedup.LookupSession(crashLoopSignal().Key); ok {
		t.Error("a failed open must not leave a binding")
	}
	if testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("error")) != 1 {
		t.Error("session_creates{error} must count the failed open")
	}
	if testutil.ToFloat64(d.metrics.injectErrors.WithLabelValues("CrashLoopBackOff", "session_create")) != 1 {
		t.Error("inject_errors{session_create} must count the failed open")
	}
	if testutil.ToFloat64(d.metrics.eventsInjected.WithLabelValues("CrashLoopBackOff", "checkout")) != 0 {
		t.Error("a failed open must not count as injected")
	}
}

// TestWebhookDispatch_DedupSuppressionUnchanged: the pipeline stages
// in front of the sink behave identically — a duplicate within the
// window never reaches the receiver.
func TestWebhookDispatch_DedupSuppressionUnchanged(t *testing.T) {
	t.Parallel()
	base, hits, _ := newWebhookFixture(t)
	d := newWebhookDispatcher(t, base, "")
	ctx := context.Background()

	d.DispatchSignal(ctx, crashLoopSignal())
	d.DispatchSignal(ctx, crashLoopSignal())
	if len(*hits) != 1 {
		t.Errorf("duplicate must be dedup-suppressed before the sink: %d POSTs", len(*hits))
	}
}
