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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// newFakeDaemon returns an httptest server that impersonates the
// core-agent daemon's POST /sessions + POST /sessions/<sid>/inject
// endpoints. Returns the URL to point --daemon-url at, plus a
// pointer to a slice of received inject bodies for assertion.
func newFakeDaemon(t *testing.T) (baseURL string, capturedInjects *[]string, capturedAuth *[]string) {
	t.Helper()
	injects := make([]string, 0, 4)
	auths := make([]string, 0, 4)
	var sessionCounter int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auths = append(auths, r.Header.Get("Authorization")+"|"+r.Header.Get("X-Asserted-Caller"))
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/sessions":
			id := atomic.AddInt32(&sessionCounter, 1)
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"app":"core-agent","user":"alice","sessionID":"sess-%s","url":"http://x"}`, strings.Repeat("x", int(id)))
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/sessions/") && strings.HasSuffix(r.URL.Path, "/inject"):
			body, _ := io.ReadAll(r.Body)
			injects = append(injects, string(body))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &injects, &auths
}

// TestDispatcher_EndToEnd wires filter + dedup + injector against
// the fake daemon and verifies the whole pipeline. Two events for
// the same key → one CreateSession, one Inject.
func TestDispatcher_EndToEnd_DuplicateSuppressed(t *testing.T) {
	t.Parallel()
	base, injects, _ := newFakeDaemon(t)
	inj, err := inject.NewInjector(inject.Config{
		DaemonURL:      base,
		BearerToken:    "tok_test",
		AssertedCaller: "sre@example.com",
	})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{
		filter:    engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1)),
		dedup:     dedup,
		injector:  inj,
		metrics:   newMetrics(),
		cluster:   "test-cluster",
		mode:      "per-incident",
		targetSid: "",
		dryRun:    false,
	}
	ev := engine.TriageEvent{
		Key:       engine.EventKey{UID: "u1", Reason: "CrashLoopBackOff"},
		Namespace: "default",
		Name:      "pod-1",
		Message:   "flapping",
		Count:     1,
	}
	ctx := context.Background()
	disp.Dispatch(ctx, ev)
	disp.Dispatch(ctx, ev) // duplicate within window — should be suppressed
	if len(*injects) != 1 {
		t.Errorf("expected 1 inject (second is dedup-suppressed); got %d", len(*injects))
	}
}

func TestDispatcher_EndToEnd_FilterRejects(t *testing.T) {
	t.Parallel()
	base, injects, _ := newFakeDaemon(t)
	inj, _ := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "t", AssertedCaller: "a@b"})
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig([]string{"CrashLoopBackOff"}, nil, nil, 0, 1)),
		dedup:    dedup,
		injector: inj,
		metrics:  newMetrics(),
		cluster:  "test-cluster",
		mode:     "per-incident",
	}
	// Reason not in allow-list → dispatcher must silently drop.
	disp.Dispatch(context.Background(), engine.TriageEvent{
		Key:       engine.EventKey{UID: "u1", Reason: "SomeOtherReason"},
		Namespace: "default",
	})
	if len(*injects) != 0 {
		t.Errorf("filter should have blocked; got %d injects", len(*injects))
	}
}

func TestDispatcher_EndToEnd_SharedMode(t *testing.T) {
	t.Parallel()
	// Shared mode: no per-incident CreateSession call; every
	// event injects to the pre-configured target session.
	base, injects, _ := newFakeDaemon(t)
	inj, _ := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "t", AssertedCaller: "a@b"})
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{
		filter:    engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1)),
		dedup:     dedup,
		injector:  inj,
		metrics:   newMetrics(),
		cluster:   "test-cluster",
		mode:      "shared",
		targetSid: "sess-shared",
	}
	disp.Dispatch(context.Background(), engine.TriageEvent{
		Key:       engine.EventKey{UID: "u1", Reason: "CrashLoopBackOff"},
		Namespace: "default",
		Name:      "pod-1",
		Count:     1,
	})
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject in shared mode; got %d", len(*injects))
	}
	// Inject URL should have carried the target SessionID.
	if !strings.Contains((*injects)[0], "k8s-event") {
		t.Errorf("captured body missing kind marker: %q", (*injects)[0])
	}
}

// TestDispatcher_ExactInjectPayloadWireShape pins the exact JSON the
// dispatcher puts on the wire. The payload's field names, JSON tags,
// and the "k8s-event" kind constant are frozen — playbook skills
// pattern-match them — so any byte-level drift here is a breaking
// change to deployed skills, not an internal detail.
//
// Re-baselined 2026-07-27 (pre-consumer amendment, zero deployed
// consumers): the frozen shape gained the "type" field (k8s
// Event.Type, after "context" — kube-agents' watcher wire). See
// docs/signal-schema-v1.md §Amendments.
func TestDispatcher_ExactInjectPayloadWireShape(t *testing.T) {
	t.Parallel()
	base, injects, _ := newFakeDaemon(t)
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{
		filter:    engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1)),
		dedup:     dedup,
		injector:  inj,
		metrics:   newMetrics(),
		cluster:   "prod-us-central1",
		mode:      "shared",
		targetSid: "sess-shared",
	}
	disp.Dispatch(context.Background(), engine.TriageEvent{
		Key:           engine.EventKey{UID: "abc-123", Reason: "CrashLoopBackOff"},
		Namespace:     "checkout",
		KindOfObject:  "Pod",
		Name:          "checkout-svc-7b9d-x4kzq",
		Container:     "spec.containers{server}",
		Message:       "Back-off restarting failed container",
		FirstSeen:     time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC),
		LastSeen:      time.Date(2026, 7, 24, 10, 5, 0, 0, time.UTC),
		ControllerRef: "ReplicaSet/checkout-svc-7b9d",
		Node:          "node-1",
		Labels:        map[string]string{"team": "checkout"},
		Count:         1,
		Type:          "Warning",
	})
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects))
	}
	// Wrapped envelope: {"message": "<serialized payload>"} — the
	// inner string is the frozen payload, byte for byte.
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte((*injects)[0]), &envelope); err != nil {
		t.Fatalf("captured body isn't the inject envelope: %v (body=%q)", err, (*injects)[0])
	}
	want := `{"kind":"k8s-event","reason":"CrashLoopBackOff","namespace":"checkout","kind_of_object":"Pod","name":"checkout-svc-7b9d-x4kzq","container":"spec.containers{server}","uid":"abc-123","message":"Back-off restarting failed container","count":1,"first_seen":"2026-07-24T10:00:00Z","last_seen":"2026-07-24T10:05:00Z","cluster":"prod-us-central1","context":{"controller_ref":"ReplicaSet/checkout-svc-7b9d","node":"node-1","labels":{"team":"checkout"}},"type":"Warning"}`
	if envelope.Message != want {
		t.Errorf("inject payload wire shape drifted:\n got: %s\nwant: %s", envelope.Message, want)
	}
}

// TestDispatcher_LogsFireOnSuccess verifies the "fire <reason>
// pod=<ns>/<name> → sid=..." success line surfaces (#212 part 2).
// Before this landed the successful-inject case was silent, forcing
// operators to correlate client-go informer warnings with daemon
// session-list dumps to infer whether the watcher fired.
func TestDispatcher_LogsFireOnSuccess(t *testing.T) {
	// NOT t.Parallel — captureLogOutput swaps global log.Default()
	// writer; concurrent tests would race on it.
	logBuf, restoreLog := captureLogOutput(t)
	defer restoreLog()

	base, injects, _ := newFakeDaemon(t)
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1)),
		dedup:    dedup,
		injector: inj,
		metrics:  newMetrics(),
		cluster:  "test-cluster",
		mode:     "per-incident",
	}
	disp.Dispatch(context.Background(), engine.TriageEvent{
		Key:       engine.EventKey{UID: "u42", Reason: "ImagePullBackOff"},
		Namespace: "online-boutique",
		Name:      "paymentservice-abc123",
	})
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects))
	}
	log := logBuf.String()
	// Every field the operator actually needs — reason, target,
	// resulting sid, mode — must appear.
	wantSubstrings := []string{"fire ImagePullBackOff", "pod=online-boutique/paymentservice-abc123", "sid=", "mode=per-incident"}
	for _, s := range wantSubstrings {
		if !strings.Contains(log, s) {
			t.Errorf("fire log missing %q; got: %q", s, log)
		}
	}
}

// TestDispatcher_LogsDedupOnSuppress verifies the "dedup <reason>
// pod=<ns>/<name> (count=N, window active)" line fires when the
// same key repeats within the dedup window. Makes suppressed
// events visible so operators can distinguish "watcher missed
// the event" from "watcher saw + correctly deduped".
func TestDispatcher_LogsDedupOnSuppress(t *testing.T) {
	// NOT t.Parallel — see captureLogOutput note.
	logBuf, restoreLog := captureLogOutput(t)
	defer restoreLog()

	base, injects, _ := newFakeDaemon(t)
	inj, _ := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1)),
		dedup:    dedup,
		injector: inj,
		metrics:  newMetrics(),
		cluster:  "test-cluster",
		mode:     "per-incident",
	}
	ev := engine.TriageEvent{
		Key:       engine.EventKey{UID: "u7", Reason: "CrashLoopBackOff"},
		Namespace: "default",
		Name:      "flappy-pod",
		Count:     1,
	}
	ctx := context.Background()
	// First fire → dispatch → inject (no dedup log)
	disp.Dispatch(ctx, ev)
	// Reset log buffer so we only inspect the second-dispatch output.
	logBuf.Reset()
	// Second fire in the same window → dedup path
	disp.Dispatch(ctx, ev)
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject (second suppressed); got %d", len(*injects))
	}
	log := logBuf.String()
	wantSubstrings := []string{"dedup CrashLoopBackOff", "pod=default/flappy-pod", "count=2", "window active"}
	for _, s := range wantSubstrings {
		if !strings.Contains(log, s) {
			t.Errorf("dedup log missing %q; got: %q", s, log)
		}
	}
	// Fire log must NOT appear on the suppressed second dispatch.
	if strings.Contains(log, "fire CrashLoopBackOff") {
		t.Errorf("suppressed dispatch should not emit a fire log; got: %q", log)
	}
}

// TestDispatcher_BackoffDebounceSuppressesLeadingEdge is the issue
// #197 regression at dispatcher scale: with the SHIPPED default
// (backoff-min-count 3), a transient crash-loop event below the
// threshold is suppressed end-to-end — no session, no inject — while
// one that reaches the threshold fires. This proves the flag is wired
// through to the filter, not just that Filter.Accept honors it.
func TestDispatcher_BackoffDebounceSuppressesLeadingEdge(t *testing.T) {
	t.Parallel()
	base, injects, _ := newFakeDaemon(t)
	inj, _ := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{
		// Default config: backoff-min-count defaults to 3.
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 0)),
		dedup:    dedup,
		injector: inj,
		metrics:  newMetrics(),
		cluster:  "test-cluster",
		mode:     "per-incident",
	}
	ctx := context.Background()

	// A first, transient BackOff (Event.Count 1) — a startup blip that
	// will self-heal. Below the threshold: suppressed.
	disp.Dispatch(ctx, engine.TriageEvent{
		Key:       engine.EventKey{UID: "u-blip", Reason: "CrashLoopBackOff"},
		Namespace: "default",
		Name:      "blippy-pod",
		Message:   "Back-off restarting failed container",
		Count:     1,
	})
	if len(*injects) != 0 {
		t.Fatalf("count=1 crash-loop should be debounced; got %d injects", len(*injects))
	}

	// A genuine crash loop climbs past the threshold: it fires.
	disp.Dispatch(ctx, engine.TriageEvent{
		Key:       engine.EventKey{UID: "u-real", Reason: "CrashLoopBackOff"},
		Namespace: "default",
		Name:      "crashy-pod",
		Message:   "Back-off restarting failed container",
		Count:     3,
	})
	if len(*injects) != 1 {
		t.Fatalf("count=3 crash-loop should fire; got %d injects", len(*injects))
	}
}

// captureLogOutput swaps log.Default()'s writer for a bytes.Buffer
// and returns (buffer, restore-fn). Used by dispatcher-log tests.
func captureLogOutput(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(buf)
	// Drop the timestamp prefix so tests can grep for substrings
	// without hitting date-dependent noise.
	log.SetFlags(0)
	return buf, func() {
		log.SetOutput(prev)
		log.SetFlags(prevFlags)
	}
}

// -- parseFlags / validate coverage ------------------------------------

func TestParseFlags_MissingRequiredIsError(t *testing.T) {
	t.Parallel()
	// Neither --daemon-url nor --token-env, not --dry-run.
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err == nil {
		t.Error("validate should reject missing --daemon-url")
	}
}

func TestParseFlags_DryRunAllowsMissing(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Errorf("--dry-run should skip daemon-url + token-env requirements; got %v", err)
	}
}

func TestParseFlags_PerIncidentRequiresOwner(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--daemon-url", "http://x", "--token-env", "TOK"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err == nil {
		t.Error("per-incident mode should require --owner")
	}
}

func TestParseFlags_SharedRequiresTargetSession(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--daemon-url", "http://x", "--token-env", "TOK", "--mode", "shared"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err == nil {
		t.Error("shared mode should require --target-session")
	}
}

func TestSplitCSV(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"a", []string{"a"}},
		{"a,b,c", []string{"a", "b", "c"}},
		{"a, b , c", []string{"a", "b", "c"}},
		{"a,,b", []string{"a", "b"}},
	}
	for _, c := range cases {
		got := splitCSV(c.in)
		if !stringSlicesEqual(got, c.want) {
			t.Errorf("splitCSV(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDispatcher_PullFamilyOneSession is the pipeline-level proof of
// the message-aware reason canonicalization (adopted from kube-agents'
// watcher): the four reason/message variants kubelet emits for ONE
// image-pull failure open exactly ONE session — not four parallel
// sessions at 4x the spend — while every wire payload preserves its
// ORIGINAL event reason (canonicalization is a keying mechanism, never
// a payload rewrite).
func TestDispatcher_PullFamilyOneSession(t *testing.T) {
	t.Parallel()
	base, injects, _ := newFakeDaemon(t)
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig([]string{"Failed", "BackOff", "ErrImagePull"}, nil, nil, 0, 1)),
		dedup:    dedup,
		injector: inj,
		metrics:  newMetrics(),
		cluster:  "test-cluster",
		mode:     "per-incident",
	}
	ctx := context.Background()
	ts := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	variants := []struct {
		reason, message string
	}{
		{"Failed", `Failed to pull image "nginx:broken": rpc error: code = NotFound`},
		{"Failed", "Error: ErrImagePull"},
		{"BackOff", `Back-off pulling image "nginx:broken"`},
		{"Failed", "Error: ImagePullBackOff"},
	}
	for i, v := range variants {
		disp.Dispatch(ctx, engine.TriageEvent{
			Key:       engine.EventKey{UID: "pod-pull-1", Reason: v.reason},
			Namespace: "shop",
			Name:      "frontend-abc",
			Message:   v.message,
			LastSeen:  ts.Add(time.Duration(i) * time.Second),
			Count:     1,
		})
	}
	// One session's initial inject; the three other variants are
	// dedup-suppressed into the same slot.
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject (one incident for the whole pull family); got %d", len(*injects))
	}
	// The single wire payload carries the FIRST variant's original
	// reason — never the canonical class.
	if !strings.Contains((*injects)[0], `\"reason\":\"Failed\"`) && !strings.Contains((*injects)[0], `"reason":"Failed"`) {
		t.Errorf("wire payload must preserve the original event reason %q; got %q", "Failed", (*injects)[0])
	}
}
