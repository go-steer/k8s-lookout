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

// routedInject is one captured POST /sessions/<sid>/inject with the
// session it targeted — the §7.4 routing tests need the path, which
// newFakeDaemon does not capture.
type routedInject struct {
	SessionID string
	Body      string
}

// newRoutingFakeDaemon is newFakeDaemon plus per-inject session
// capture. Session IDs count up: sess-1, sess-2, …
func newRoutingFakeDaemon(t *testing.T) (baseURL string, injects *[]routedInject) {
	t.Helper()
	captured := make([]routedInject, 0, 4)
	sessionCounter := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/sessions":
			sessionCounter++
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"app":"core-agent","user":"alice","sessionID":"sess-%d","url":"http://x"}`, sessionCounter)
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/sessions/") && strings.HasSuffix(r.URL.Path, "/inject"):
			sid := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/sessions/"), "/inject")
			body, _ := io.ReadAll(r.Body)
			captured = append(captured, routedInject{SessionID: sid, Body: string(body)})
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &captured
}

func newRecoveryDispatcher(t *testing.T, base string) *dispatcher {
	t.Helper()
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	return &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0)),
		dedup:    dedup,
		injector: inj,
		metrics:  newMetrics(),
		cluster:  "prod-us-central1",
		mode:     "per-incident",
	}
}

func crashLoopSignal() engine.Signal {
	return engine.Signal{
		Kind:     engine.KindK8sEvent,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:           engine.EventKey{UID: "abc-123", Reason: "CrashLoopBackOff"},
			Namespace:     "checkout",
			KindOfObject:  "Pod",
			Name:          "checkout-svc-7b9d-x4kzq",
			Container:     "spec.containers{server}",
			Message:       "Back-off restarting failed container",
			FirstSeen:     time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC),
			LastSeen:      time.Date(2026, 7, 24, 10, 5, 0, 0, time.UTC),
			ControllerRef: "ReplicaSet/checkout-svc-7b9d",
			Count:         1,
		},
	}
}

func resolvedSignalFor(orig engine.Signal, kind string) engine.Signal {
	return engine.Signal{
		Kind:        kind,
		Source:      engine.SourceSentinel,
		Severity:    engine.SeverityInfo,
		Fingerprint: "sha256:1f4e6a7b8c9d0e1f2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091",
		Cluster:     orig.Cluster,
		TriageEvent: engine.TriageEvent{
			Key:           orig.Key,
			Namespace:     orig.Namespace,
			KindOfObject:  orig.KindOfObject,
			Name:          orig.Name,
			Container:     orig.Container,
			ControllerRef: orig.ControllerRef,
			FirstSeen:     orig.FirstSeen,
		},
		Recovery: &engine.Recovery{
			ClearedAfter:      2*time.Minute + 30*time.Second,
			ObservedStableFor: 5 * time.Minute,
			Resolution:        engine.ResolutionRecovered,
			ResolvedAt:        time.Date(2026, 7, 24, 10, 7, 30, 0, time.UTC),
		},
	}
}

// TestDispatchResolved_RoutesToBoundSession: the incident opens
// sess-1; the resolved signal must land in sess-1 as a followup —
// the §7.4 closed loop.
func TestDispatchResolved_RoutesToBoundSession(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)
	ctx := context.Background()

	orig := crashLoopSignal()
	d.DispatchSignal(ctx, orig)
	if len(*injects) != 1 || (*injects)[0].SessionID != "sess-1" {
		t.Fatalf("setup: incident inject expected in sess-1, got %+v", *injects)
	}

	d.DispatchSignal(ctx, resolvedSignalFor(orig, engine.KindResolved))
	if len(*injects) != 2 {
		t.Fatalf("resolved inject missing: %d injects", len(*injects))
	}
	got := (*injects)[1]
	if got.SessionID != "sess-1" {
		t.Errorf("resolved routed to %q, want the incident's bound session sess-1", got.SessionID)
	}
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(got.Body), &envelope); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	var payload inject.ResolvedPayload
	if err := json.Unmarshal([]byte(envelope.Message), &payload); err != nil {
		t.Fatalf("unmarshal resolved payload: %v", err)
	}
	if payload.Kind != inject.KindResolved || payload.Resolution != "recovered" {
		t.Errorf("payload kind/resolution = %q/%q", payload.Kind, payload.Resolution)
	}
	if testutil.ToFloat64(d.metrics.recoveriesObserved.WithLabelValues("recovered")) != 1 {
		t.Error("recoveries_observed{resolution=recovered} not incremented")
	}
}

// TestDispatchResolved_RevertedRoutesAndCounts: resolved.reverted
// also lands in the bound session and bumps its own counter.
func TestDispatchResolved_RevertedRoutesAndCounts(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)
	ctx := context.Background()

	orig := crashLoopSignal()
	d.DispatchSignal(ctx, orig)
	rev := resolvedSignalFor(orig, engine.KindResolvedReverted)
	rev.Recovery.RevertedAfter = 90 * time.Second
	d.DispatchSignal(ctx, rev)

	if len(*injects) != 2 || (*injects)[1].SessionID != "sess-1" {
		t.Fatalf("reverted must route to sess-1, got %+v", *injects)
	}
	var envelope struct {
		Message string `json:"message"`
	}
	_ = json.Unmarshal([]byte((*injects)[1].Body), &envelope)
	var payload inject.ResolvedPayload
	if err := json.Unmarshal([]byte(envelope.Message), &payload); err != nil {
		t.Fatalf("unmarshal reverted payload: %v", err)
	}
	if payload.Kind != inject.KindResolvedReverted {
		t.Errorf("kind = %q", payload.Kind)
	}
	if payload.RevertedAfter != "1m30s" {
		t.Errorf("reverted_after = %q, want 1m30s", payload.RevertedAfter)
	}
	if testutil.ToFloat64(d.metrics.recoveriesReverted) != 1 {
		t.Error("recoveries_reverted not incremented")
	}
}

// TestDispatchResolved_UnknownSessionDropped: a resolved signal for
// an incident with no binding (sentinel restarted without
// --dedup-persist) is logged + counted + dropped — never a fresh
// session just to report a fix.
func TestDispatchResolved_UnknownSessionDropped(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)

	d.DispatchSignal(context.Background(), resolvedSignalFor(crashLoopSignal(), engine.KindResolved))
	if len(*injects) != 0 {
		t.Errorf("unknown-session resolved must be dropped, got %d injects", len(*injects))
	}
	if testutil.ToFloat64(d.metrics.recoveryDrops.WithLabelValues("unknown_session")) != 1 {
		t.Error("recovery_drops{cause=unknown_session} not incremented")
	}
	if testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")) != 0 {
		t.Error("a resolved signal must never create a session")
	}
}

// TestDispatchResolved_SharedModeRoutesToTargetSession: shared mode
// has no per-incident bindings; the outcome goes where the incident
// went — the shared session.
func TestDispatchResolved_SharedModeRoutesToTargetSession(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)
	d.mode = "shared"
	d.targetSid = "sess-shared"

	ctx := context.Background()
	orig := crashLoopSignal()
	d.DispatchSignal(ctx, orig)
	d.DispatchSignal(ctx, resolvedSignalFor(orig, engine.KindResolved))
	if len(*injects) != 2 || (*injects)[1].SessionID != "sess-shared" {
		t.Fatalf("shared-mode resolved must route to sess-shared, got %+v", *injects)
	}
}

// TestDispatchResolved_BypassesFilterAndDedup: a resolved signal is
// an outcome record, not a new incident — a reason allow-list that
// would reject "CrashLoopBackOff" must not block it, and it must not
// consume a dedup slot.
func TestDispatchResolved_BypassesFilterAndDedup(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)
	ctx := context.Background()

	orig := crashLoopSignal()
	d.DispatchSignal(ctx, orig)
	// Swap in a filter that rejects everything.
	d.filter = engine.NewFilter(engine.NewFilterConfig([]string{"NothingMatches"}, nil, nil, 0))
	before := d.dedup.Len()
	d.DispatchSignal(ctx, resolvedSignalFor(orig, engine.KindResolved))
	if len(*injects) != 2 {
		t.Errorf("resolved must bypass the reason filter, got %d injects", len(*injects))
	}
	if d.dedup.Len() != before {
		t.Errorf("resolved must not consume dedup slots: %d → %d", before, d.dedup.Len())
	}
}

// TestDispatcher_BindsIncidentAndTracks: a new incident lands in the
// dedup cache WITH its identity ref (restart survival) and in the
// recovery tracker (clearance watching).
func TestDispatcher_BindsIncidentAndTracks(t *testing.T) {
	t.Parallel()
	base, _ := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)
	var tracked []engine.Signal
	d.tracker = engine.NewRecoveryTracker(5*time.Minute, func(sig engine.Signal) { tracked = append(tracked, sig) })

	orig := crashLoopSignal()
	d.DispatchSignal(context.Background(), orig)

	if sid, ok := d.dedup.LookupSession(orig.Key); !ok || sid != "sess-1" {
		t.Errorf("LookupSession = (%q, %v), want (sess-1, true)", sid, ok)
	}
	bindings := d.dedup.Bindings()
	if len(bindings) != 1 {
		t.Fatalf("want 1 resumable binding, got %d", len(bindings))
	}
	if bindings[0].Ref.Namespace != "checkout" || bindings[0].Ref.ControllerRef != "ReplicaSet/checkout-svc-7b9d" {
		t.Errorf("binding ref incomplete: %+v", bindings[0].Ref)
	}
	if bindings[0].Ref.Fingerprint == "" {
		t.Error("binding must carry the stamped fingerprint for the outcome record")
	}
	if d.tracker.Len() != 1 {
		t.Errorf("tracker.Len = %d, want 1", d.tracker.Len())
	}
}

// TestSeedRecovery_RestoredBindingsResumeTracking exercises the
// restart path end to end at the cache/tracker seam: bindings written
// by one cache generation resume tracking in the next.
func TestSeedRecovery_RestoredBindingsResumeTracking(t *testing.T) {
	t.Parallel()
	path := t.TempDir() + "/dedup.json"
	c1, _ := engine.NewDedupCache(5*time.Minute, path)
	key := engine.EventKey{UID: "u-seed", Reason: "CrashLoopBackOff"}
	c1.Observe(key, time.Now())
	c1.BindIncident(key, "sess-9", engine.IncidentRef{Namespace: "ns", KindOfObject: "Pod", Name: "p", Fingerprint: "sha256:x"})
	if err := c1.Snapshot(); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	c2, _ := engine.NewDedupCache(5*time.Minute, path)
	tracker := engine.NewRecoveryTracker(5*time.Minute, func(engine.Signal) {})
	for _, b := range c2.Bindings() {
		tracker.Track(engine.Incident(b))
	}
	if tracker.Len() != 1 {
		t.Errorf("restored binding did not resume tracking: Len = %d", tracker.Len())
	}
}

// TestDispatchResolved_ExactWireShape pins the §9.3 outcome-record
// wire bytes, the resolved-kind companion to
// TestDispatcher_ExactInjectPayloadWireShape. The corpus harvester
// and playbook skills parse these payloads structurally; treat a
// failing pin as a breaking schema change, never as a test to update.
func TestDispatchResolved_ExactWireShape(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)
	d.mode = "shared"
	d.targetSid = "sess-shared"

	orig := crashLoopSignal()
	orig.Cluster = "prod-us-central1"
	d.DispatchSignal(context.Background(), resolvedSignalFor(orig, engine.KindResolved))
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects))
	}
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte((*injects)[0].Body), &envelope); err != nil {
		t.Fatalf("captured body isn't the inject envelope: %v", err)
	}
	want := `{"kind":"resolved","reason":"CrashLoopBackOff","namespace":"checkout","kind_of_object":"Pod","name":"checkout-svc-7b9d-x4kzq","container":"spec.containers{server}","uid":"abc-123","fingerprint":"sha256:1f4e6a7b8c9d0e1f2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091","cluster":"prod-us-central1","first_seen":"2026-07-24T10:00:00Z","resolved_at":"2026-07-24T10:07:30Z","cleared_after":"2m30s","observed_stable_for":"5m0s","resolution":"recovered","context":{"controller_ref":"ReplicaSet/checkout-svc-7b9d"}}`
	if envelope.Message != want {
		t.Errorf("resolved payload drifted from the schema-stable wire shape:\n got: %s\nwant: %s", envelope.Message, want)
	}
}

// TestResolvedKindConstantsAlignedWithWireContract pins engine's
// resolved kinds to inject's, mirroring the k8s-event pin.
func TestResolvedKindConstantsAlignedWithWireContract(t *testing.T) {
	t.Parallel()
	if engine.KindResolved != inject.KindResolved {
		t.Errorf("engine.KindResolved (%q) != inject.KindResolved (%q)", engine.KindResolved, inject.KindResolved)
	}
	if engine.KindResolvedReverted != inject.KindResolvedReverted {
		t.Errorf("engine.KindResolvedReverted (%q) != inject.KindResolvedReverted (%q)", engine.KindResolvedReverted, inject.KindResolvedReverted)
	}
}
