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

package inject

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// webhookCall is one captured request against the fake receiver.
type webhookCall struct {
	Method string
	Path   string
	Auth   string
	CT     string
	Body   string
}

// newFakeReceiver returns an httptest server speaking the generic
// webhook contract: POST /incidents → 200 {"id":"inc-N"},
// POST /incidents/<id>/events → 200 {}.
func newFakeReceiver(t *testing.T) (baseURL string, calls *[]webhookCall) {
	t.Helper()
	captured := make([]webhookCall, 0, 4)
	counter := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		captured = append(captured, webhookCall{
			Method: r.Method,
			Path:   r.URL.Path,
			Auth:   r.Header.Get("Authorization"),
			CT:     r.Header.Get("Content-Type"),
			Body:   string(body),
		})
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/incidents":
			counter++
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"inc-` + strings.Repeat("x", counter) + `"}`))
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/incidents/") && strings.HasSuffix(r.URL.Path, "/events"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &captured
}

func TestWebhookSink_OpenIncident_PostsPayloadDirectly(t *testing.T) {
	t.Parallel()
	base, calls := newFakeReceiver(t)
	s, err := NewWebhookSink(WebhookConfig{URL: base, BearerToken: "tok_hook"})
	if err != nil {
		t.Fatalf("NewWebhookSink: %v", err)
	}
	payload := Payload{
		Kind:      KindEvent,
		Reason:    "CrashLoopBackOff",
		Namespace: "checkout",
		Name:      "checkout-svc-7b9d-x4kzq",
		UID:       "abc-123",
		Message:   "Back-off restarting failed container",
		Count:     1,
		FirstSeen: time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC),
		LastSeen:  time.Date(2026, 7, 24, 10, 5, 0, 0, time.UTC),
		Cluster:   "prod-us-central1",
	}
	id, err := s.OpenIncident(context.Background(), payload)
	if err != nil {
		t.Fatalf("OpenIncident: %v", err)
	}
	if id != "inc-x" {
		t.Errorf("id = %q, want the receiver-issued inc-x", id)
	}
	if len(*calls) != 1 {
		t.Fatalf("expected 1 request, got %d", len(*calls))
	}
	got := (*calls)[0]
	if got.Method != http.MethodPost || got.Path != "/incidents" {
		t.Errorf("open = %s %s, want POST /incidents", got.Method, got.Path)
	}
	if got.Auth != "Bearer tok_hook" {
		t.Errorf("Authorization = %q, want Bearer tok_hook", got.Auth)
	}
	if got.CT != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got.CT)
	}
	// The body is the payload JSON DIRECTLY — never wrapped in the
	// core-agent daemon's {"message": ...} envelope.
	if strings.Contains(got.Body, `"message":"{`) || strings.HasPrefix(got.Body, `{"message":`) {
		t.Errorf("body looks envelope-wrapped: %q", got.Body)
	}
	if !strings.HasPrefix(got.Body, `{"kind":"k8s-event","reason":"CrashLoopBackOff"`) {
		t.Errorf("body isn't the raw schema-v1 payload: %q", got.Body)
	}
}

func TestWebhookSink_Append_PostsToEventsPath(t *testing.T) {
	t.Parallel()
	base, calls := newFakeReceiver(t)
	s, err := NewWebhookSink(WebhookConfig{URL: base})
	if err != nil {
		t.Fatalf("NewWebhookSink: %v", err)
	}
	if err := s.Append(context.Background(), "inc-7", Payload{Kind: KindFollowup, Reason: "CrashLoopBackOff"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	got := (*calls)[0]
	if got.Path != "/incidents/inc-7/events" {
		t.Errorf("append path = %q, want /incidents/inc-7/events", got.Path)
	}
	// No --sink-token-env → unauthenticated POST, no header at all.
	if got.Auth != "" {
		t.Errorf("Authorization = %q, want unset without a bearer token", got.Auth)
	}
	if !strings.Contains(got.Body, `"kind":"k8s-event-followup"`) {
		t.Errorf("append body missing payload kind: %q", got.Body)
	}
}

func TestWebhookSink_Append_EmptyIDRejected(t *testing.T) {
	t.Parallel()
	base, _ := newFakeReceiver(t)
	s, _ := NewWebhookSink(WebhookConfig{URL: base})
	if err := s.Append(context.Background(), "", Payload{}); err == nil {
		t.Error("Append with an empty incident id must error")
	}
}

// TestWebhookSink_StatelessReceiverGetsLocalIDs: a 2xx open response
// without a parseable id gets a locally generated one — stateless
// receivers are allowed; the pipeline keeps its append routing.
func TestWebhookSink_StatelessReceiverGetsLocalIDs(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)
	s, _ := NewWebhookSink(WebhookConfig{URL: srv.URL})
	first, err := s.OpenIncident(context.Background(), Payload{Kind: KindEvent})
	if err != nil {
		t.Fatalf("OpenIncident: %v", err)
	}
	second, err := s.OpenIncident(context.Background(), Payload{Kind: KindEvent})
	if err != nil {
		t.Fatalf("OpenIncident: %v", err)
	}
	for _, id := range []string{first, second} {
		if !strings.HasPrefix(id, "local-") || len(id) != len("local-")+32 {
			t.Errorf("local id = %q, want local-<32 hex>", id)
		}
	}
	if first == second {
		t.Errorf("local ids must be unique per open; got %q twice", first)
	}
}

// TestWebhookSink_EmptyIDFieldGetsLocalID: {"id":""} counts as
// missing, same fallback.
func TestWebhookSink_EmptyIDFieldGetsLocalID(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":""}`))
	}))
	t.Cleanup(srv.Close)
	s, _ := NewWebhookSink(WebhookConfig{URL: srv.URL})
	id, err := s.OpenIncident(context.Background(), Payload{Kind: KindEvent})
	if err != nil {
		t.Fatalf("OpenIncident: %v", err)
	}
	if !strings.HasPrefix(id, "local-") {
		t.Errorf("id = %q, want a local- fallback for an empty receiver id", id)
	}
}

// TestWebhookSink_Non2xxIsErrorNoRetry: a failing POST surfaces the
// status + body and is attempted exactly once — the webhook sink's
// retry posture is identical to the core-agent client's (none).
func TestWebhookSink_Non2xxIsErrorNoRetry(t *testing.T) {
	t.Parallel()
	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		http.Error(w, "receiver exploded", http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)
	s, _ := NewWebhookSink(WebhookConfig{URL: srv.URL})

	id, err := s.OpenIncident(context.Background(), Payload{Kind: KindEvent})
	if err == nil || id != "" {
		t.Fatalf("OpenIncident on 502 = (%q, %v), want (\"\", error)", id, err)
	}
	if !strings.Contains(err.Error(), "502") || !strings.Contains(err.Error(), "receiver exploded") {
		t.Errorf("error should carry status + receiver body; got %v", err)
	}
	if attempts != 1 {
		t.Errorf("open attempted %d times, want exactly 1 (no retries)", attempts)
	}

	attempts = 0
	if err := s.Append(context.Background(), "inc-1", Payload{}); err == nil {
		t.Error("Append on 502 must error")
	}
	if attempts != 1 {
		t.Errorf("append attempted %d times, want exactly 1 (no retries)", attempts)
	}
}

func TestWebhookSink_ContextCancellationHonored(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	defer close(block)
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		<-block
	}))
	t.Cleanup(srv.Close)
	s, _ := NewWebhookSink(WebhookConfig{URL: srv.URL})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.OpenIncident(ctx, Payload{Kind: KindEvent}); err == nil {
		t.Error("expected error on cancelled context")
	}
}

func TestNewWebhookSink_ConfigValidation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		url  string
	}{
		{"empty url", ""},
		{"trailing slash", "https://hooks.example/"},
		{"missing scheme", "hooks.example:8443"},
		{"non-http scheme", "ftp://hooks.example"},
	}
	for _, c := range cases {
		if _, err := NewWebhookSink(WebhookConfig{URL: c.url}); err == nil {
			t.Errorf("%s (%q) should be rejected", c.name, c.url)
		}
	}
	if _, err := NewWebhookSink(WebhookConfig{URL: "https://hooks.example"}); err != nil {
		t.Errorf("valid https url rejected: %v", err)
	}
	// Plain http is ALLOWED at the sink level (remote receivers are
	// the point); main.go warns loudly at startup.
	if _, err := NewWebhookSink(WebhookConfig{URL: "http://hooks.internal:9099"}); err != nil {
		t.Errorf("plain http must be allowed (with a startup warning): %v", err)
	}
}

// TestWebhookSink_AppendEscapesID: exotic receiver ids ride the
// append URL as ONE escaped path segment, never re-interpreted as
// path structure.
func TestWebhookSink_AppendEscapesID(t *testing.T) {
	t.Parallel()
	var rawURI string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawURI = r.RequestURI
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	s, _ := NewWebhookSink(WebhookConfig{URL: srv.URL})
	if err := s.Append(context.Background(), "a/b c", Payload{}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if want := "/incidents/a%2Fb%20c/events"; rawURI != want {
		t.Errorf("append request URI = %q, want %q (id path-escaped as one segment)", rawURI, want)
	}
}
