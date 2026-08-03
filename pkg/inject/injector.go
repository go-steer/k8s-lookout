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

// Package inject implements the agent sinks the watch sentinel
// delivers incidents through — the core-agent daemon client (the
// default) and the generic webhook sink (webhook.go), both behind the
// two-verb Sink interface (sink.go) — plus the frozen wire types they
// POST (see payload.go).
package inject

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Config captures the daemon-side surface the sidecar posts
// against. Constructed from CLI flags in main.go; injected into the
// injector so tests can substitute their own httptest server.
type Config struct {
	// DaemonURL is the base URL (scheme + host + port) — e.g.
	// "http://127.0.0.1:7777" or "https://core-agent.example:7777".
	// The injector appends the path components; callers pass a
	// URL WITHOUT a trailing slash.
	DaemonURL string

	// BearerToken authenticates the sidecar to the daemon. Loaded
	// from an env var by main.go (`--token-env NAME`).
	BearerToken string

	// AssertedCaller is the identity the daemon stamps as session
	// Owner on POST /sessions. The sidecar must be listed in
	// attach.multi_session.proxy_identities in the daemon config
	// for this to be honored.
	AssertedCaller string

	// HTTPClient lets tests swap in a *http.Client that talks to
	// httptest.NewServer. Nil in production; main.go leaves it nil.
	HTTPClient *http.Client
}

// Injector wraps a small HTTP client that speaks the two daemon
// endpoints the sidecar needs: POST /sessions (creates a new
// per-incident session and returns the SessionID) and
// POST /sessions/<sid>/inject (queues a message on that session's
// inbox). No SSE, no auth introspection, no session-list — this is
// intentionally the thinnest wire client. It is the core-agent
// implementation of Sink (and of the SessionOpener capability); the
// typed Inject* methods below predate the Sink extraction and remain
// for compatibility — they and the Sink verbs share injectJSON, so
// the wire bytes are identical either way.
type Injector struct {
	cfg    Config
	client *http.Client
}

// Compile-time contract: the core-agent client is a Sink and can open
// empty sessions ahead of their first payload.
var (
	_ Sink          = (*Injector)(nil)
	_ SessionOpener = (*Injector)(nil)
)

// NewInjector constructs an Injector from the config. Validates the
// required fields early so misconfig fails fast.
func NewInjector(cfg Config) (*Injector, error) {
	if cfg.DaemonURL == "" {
		return nil, errors.New("injector: daemonURL is required")
	}
	if strings.HasSuffix(cfg.DaemonURL, "/") {
		return nil, fmt.Errorf("injector: daemonURL must not end with '/' (got %q)", cfg.DaemonURL)
	}
	if cfg.BearerToken == "" {
		return nil, errors.New("injector: bearerToken is required")
	}
	// AssertedCaller can be empty for daemons that don't run
	// multi-session (rare for this sidecar's use case but not the
	// injector's responsibility to enforce).
	client := cfg.HTTPClient
	if client == nil {
		// The shared sink transport (sink.go): 10s timeout,
		// otelhttp-wrapped so the outbound POST carries the current
		// span's traceparent across the process boundary. See #217.
		client = newSinkHTTPClient()
	}
	return &Injector{cfg: cfg, client: client}, nil
}

// OpenIncident implements Sink against the core-agent daemon: POST
// /sessions opens the per-incident session, then the initial payload
// is delivered through the same envelope Inject always sent — the
// wire bytes are identical to the pre-Sink CreateSession + Inject
// pair. When the session was created but the initial delivery failed,
// the SessionID is returned ALONGSIDE the error (see the Sink
// contract) so callers can keep the incident bound to the session —
// exactly the pre-Sink dispatcher behavior of binding on create and
// counting the inject error separately.
func (i *Injector) OpenIncident(ctx context.Context, payload any) (string, error) {
	sid, err := i.CreateSession(ctx)
	if err != nil {
		return "", err
	}
	return sid, i.injectJSON(ctx, sid, payload)
}

// Append implements Sink: deliver one payload into the existing
// incident session — the exact endpoint, envelope, and bytes of
// Inject.
func (i *Injector) Append(ctx context.Context, id string, payload any) error {
	return i.injectJSON(ctx, id, payload)
}

// createSessionResponse mirrors pkg/attach.createSessionResponse's
// JSON shape. Copy-paste of the field names (not an import) so the
// sidecar doesn't drag in pkg/attach as a compile-time dependency.
type createSessionResponse struct {
	AppName   string `json:"app"`
	UserID    string `json:"user"`
	SessionID string `json:"sessionID"`
	URL       string `json:"url"`
}

// CreateSession asks the daemon to create a new session owned by
// cfg.assertedCaller. Returns the new SessionID on success.
// Non-201 responses surface as an error carrying the daemon's
// response body for diagnostic clarity.
func (i *Injector) CreateSession(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, i.cfg.DaemonURL+"/sessions", nil)
	if err != nil {
		return "", fmt.Errorf("injector: build POST /sessions: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+i.cfg.BearerToken)
	// The daemon's browser-CSRF guard (core-agent #431) 415s any
	// state-changing request without this content type — including
	// bodyless ones like this create.
	req.Header.Set("Content-Type", "application/json")
	if i.cfg.AssertedCaller != "" {
		req.Header.Set("X-Asserted-Caller", i.cfg.AssertedCaller)
	}
	resp, err := i.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("injector: POST /sessions: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("injector: POST /sessions: status %d: %s", resp.StatusCode, string(body))
	}
	var payload createSessionResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("injector: decode POST /sessions response: %w", err)
	}
	if payload.SessionID == "" {
		return "", errors.New("injector: POST /sessions returned empty sessionID")
	}
	return payload.SessionID, nil
}

// injectMessageRequest is the JSON body for POST /sessions/<sid>/inject.
// Mirrors pkg/attach's injectRequest shape; the sidecar sends a
// stringified JSON payload as the "message" so playbook skills can
// json.Unmarshal it inside their tool calls.
type injectMessageRequest struct {
	Message string `json:"message"`
}

// Inject POSTs a payload to /sessions/<sid>/inject. The payload is
// JSON-encoded and wrapped in the inject-message envelope. On
// non-2xx response returns an error carrying the daemon's body.
func (i *Injector) Inject(ctx context.Context, sessionID string, payload Payload) error {
	return i.injectJSON(ctx, sessionID, payload)
}

// InjectResolved POSTs a §7.4 outcome record (kind=resolved /
// resolved.reverted) to the incident's bound session. Same envelope
// and endpoint as Inject — only the payload struct differs.
func (i *Injector) InjectResolved(ctx context.Context, sessionID string, payload ResolvedPayload) error {
	return i.injectJSON(ctx, sessionID, payload)
}

// InjectStorm POSTs the §7.5 aggregate storm payload into the storm's
// session. Same envelope and endpoint — only the payload differs.
func (i *Injector) InjectStorm(ctx context.Context, sessionID string, payload StormPayload) error {
	return i.injectJSON(ctx, sessionID, payload)
}

// InjectStormMember POSTs a storm membership record: kind=storm.member
// into the storm session (late arrival), or
// kind=storm.member_superseded into a member's own pre-storm session.
func (i *Injector) InjectStormMember(ctx context.Context, sessionID string, payload StormMemberPayload) error {
	return i.injectJSON(ctx, sessionID, payload)
}

// InjectStormUpdate POSTs a kind=storm.update size refresh into the
// storm's session when membership grows past a reporting threshold.
func (i *Injector) InjectStormUpdate(ctx context.Context, sessionID string, payload StormUpdatePayload) error {
	return i.injectJSON(ctx, sessionID, payload)
}

// InjectTriageRegressed POSTs the §9.4 regression-evidence record
// (kind=triage.regressed, M4 observation 3) into the downgraded
// incident's bound session. Evidence only — never a page.
func (i *Injector) InjectTriageRegressed(ctx context.Context, sessionID string, payload TriageRegressedPayload) error {
	return i.injectJSON(ctx, sessionID, payload)
}

// InjectFamilyMember POSTs the kind=family.member cross-source join
// notice (§10.3 correlation) into the incident's bound session: a
// signal from a different source family attached to the incident
// family this session owns.
func (i *Injector) InjectFamilyMember(ctx context.Context, sessionID string, payload FamilyMemberPayload) error {
	return i.injectJSON(ctx, sessionID, payload)
}

// InjectWatchboardDigest POSTs a §7.7 rolling digest of warning-class
// signals into the shared watchboard session. Same envelope and
// endpoint — only the payload differs.
func (i *Injector) InjectWatchboardDigest(ctx context.Context, sessionID string, payload WatchboardDigestPayload) error {
	return i.injectJSON(ctx, sessionID, payload)
}

// InjectWatchboardRotated POSTs the final kind=watchboard.rotated
// lineage record into a watchboard session that size-based rotation
// (§15 Q2) is closing.
func (i *Injector) InjectWatchboardRotated(ctx context.Context, sessionID string, payload WatchboardRotatedPayload) error {
	return i.injectJSON(ctx, sessionID, payload)
}

// injectJSON is the shared body of the Inject* methods: marshal the
// payload, wrap it in the inject-message envelope, POST it.
func (i *Injector) injectJSON(ctx context.Context, sessionID string, payload any) error {
	if sessionID == "" {
		return errors.New("injector: Inject: sessionID is required")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("injector: marshal payload: %w", err)
	}
	wrapped, err := json.Marshal(injectMessageRequest{Message: string(body)})
	if err != nil {
		return fmt.Errorf("injector: wrap inject envelope: %w", err)
	}
	// Shortcut form of the inject URL — /sessions/<sid>/inject —
	// works when the SessionID is unambiguous across registered
	// apps, which it always is in the sidecar's single-daemon
	// deployment model.
	url := i.cfg.DaemonURL + "/sessions/" + sessionID + "/inject"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(wrapped))
	if err != nil {
		return fmt.Errorf("injector: build POST inject: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+i.cfg.BearerToken)
	req.Header.Set("Content-Type", "application/json")
	if i.cfg.AssertedCaller != "" {
		req.Header.Set("X-Asserted-Caller", i.cfg.AssertedCaller)
	}
	resp, err := i.client.Do(req)
	if err != nil {
		return fmt.Errorf("injector: POST inject: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("injector: POST inject: status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}
