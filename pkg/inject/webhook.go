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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

// WebhookConfig captures the generic-receiver surface the webhook
// sink posts against. Constructed from CLI flags in main.go
// (--sink-url / --sink-token-env); injected so tests can substitute
// their own httptest server.
type WebhookConfig struct {
	// URL is the receiver base URL (scheme + host [+ path prefix]) —
	// e.g. "https://hooks.example.com/lookout". The sink appends the
	// path components (/incidents, /incidents/<id>/events); callers
	// pass a URL WITHOUT a trailing slash. https is strongly
	// recommended; plain http is allowed (remote receivers are the
	// point) and main.go warns loudly at startup.
	URL string

	// BearerToken, when non-empty, rides every POST as
	// "Authorization: Bearer <token>". Loaded from an env var by
	// main.go (--sink-token-env); empty means unauthenticated POSTs.
	BearerToken string

	// HTTPClient lets tests swap in a *http.Client that talks to
	// httptest.NewServer. Nil in production; main.go leaves it nil.
	HTTPClient *http.Client
}

// WebhookSink is the generic Sink (docs/agent-sink-design.md): any
// HTTP receiver that accepts two POSTs speaks it.
//
//	POST <url>/incidents             body = the signal-schema v1
//	                                 payload JSON — the SAME bytes
//	                                 that ride inside the core-agent
//	                                 envelope's "message", marshaled
//	                                 directly as the body, never
//	                                 wrapped in {"message": ...}.
//	→ 2xx, body {"id": "<opaque>"}   the incident id appends target.
//
//	POST <url>/incidents/<id>/events body = same payload shape.
//	→ 2xx.
//
// Stateless receivers are allowed: a 2xx open response whose body has
// no parseable {"id": ...} gets a locally generated id (logged once)
// so the pipeline keeps its append routing; such receivers see the id
// only in the append URL path and may ignore it.
//
// Transport posture is identical to the core-agent sink: the shared
// otelhttp-wrapped client (sink.go), 10s timeout, one attempt per
// POST, no retries — errors surface to the caller whose metrics and
// logs count them.
type WebhookSink struct {
	cfg    WebhookConfig
	client *http.Client
	// newLocalID is the stateless-receiver id generator; a test seam
	// defaulting to a random hex id with a "local-" prefix.
	newLocalID func() string
	// statelessOnce gates the missing-id log to a single line per
	// process — a stateless receiver returns no id on EVERY open.
	statelessOnce sync.Once
}

// Compile-time contract: the webhook sink is a Sink — and, unlike the
// core-agent client, deliberately NOT a SessionOpener (an incident at
// a generic receiver exists only by receiving its first payload).
var _ Sink = (*WebhookSink)(nil)

// NewWebhookSink constructs a WebhookSink from the config. Validates
// the required fields early so misconfig fails fast, mirroring
// NewInjector.
func NewWebhookSink(cfg WebhookConfig) (*WebhookSink, error) {
	if cfg.URL == "" {
		return nil, errors.New("webhook sink: url is required")
	}
	if strings.HasSuffix(cfg.URL, "/") {
		return nil, fmt.Errorf("webhook sink: url must not end with '/' (got %q)", cfg.URL)
	}
	if !strings.HasPrefix(cfg.URL, "http://") && !strings.HasPrefix(cfg.URL, "https://") {
		return nil, fmt.Errorf("webhook sink: url must start with http:// or https:// (got %q)", cfg.URL)
	}
	client := cfg.HTTPClient
	if client == nil {
		client = newSinkHTTPClient()
	}
	return &WebhookSink{cfg: cfg, client: client, newLocalID: randomLocalID}, nil
}

// webhookOpenResponse is the JSON body a receiver returns from POST
// /incidents. Only the id is contract; extra keys are ignored.
type webhookOpenResponse struct {
	ID string `json:"id"`
}

// OpenIncident implements Sink: POST <url>/incidents with the payload
// JSON as the body. On 2xx the receiver's {"id": ...} becomes the
// incident id; a missing or unparseable id on 2xx falls back to a
// locally generated one (logged once — stateless receivers allowed).
func (s *WebhookSink) OpenIncident(ctx context.Context, payload any) (string, error) {
	respBody, err := s.post(ctx, s.cfg.URL+"/incidents", payload)
	if err != nil {
		return "", err
	}
	var parsed webhookOpenResponse
	if uerr := json.Unmarshal(respBody, &parsed); uerr != nil || parsed.ID == "" {
		id := s.newLocalID()
		s.statelessOnce.Do(func() {
			log.Printf("webhook sink: receiver accepted POST /incidents without a parseable {\"id\":...} body — generating local incident ids (stateless receiver mode; appends carry ids the receiver never issued). Logged once.")
		})
		return id, nil
	}
	return parsed.ID, nil
}

// Append implements Sink: POST <url>/incidents/<id>/events with the
// payload JSON as the body — the same body shape as the open. The id
// is opaque (receiver-issued or locally generated) and rides
// path-escaped.
func (s *WebhookSink) Append(ctx context.Context, id string, payload any) error {
	if id == "" {
		return errors.New("webhook sink: Append: incident id is required")
	}
	_, err := s.post(ctx, s.cfg.URL+"/incidents/"+url.PathEscape(id)+"/events", payload)
	return err
}

// post marshals the payload DIRECTLY as the request body — the frozen
// signal-schema v1 bytes, no envelope — and POSTs it once. Non-2xx
// responses surface as an error carrying the receiver's body for
// diagnostic clarity, exactly like the core-agent client's.
func (s *WebhookSink) post(ctx context.Context, u string, payload any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("webhook sink: marshal payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("webhook sink: build POST %s: %w", u, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.cfg.BearerToken)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("webhook sink: POST %s: %w", u, err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("webhook sink: POST %s: status %d: %s", u, resp.StatusCode, string(respBody))
	}
	return respBody, nil
}

// randomLocalID generates the stateless-receiver fallback incident id:
// unambiguous prefix + 128 random bits, unique per open.
func randomLocalID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is effectively unreachable; keep the
		// pipeline alive rather than dropping the incident.
		return "local-unavailable"
	}
	return "local-" + hex.EncodeToString(b[:])
}
