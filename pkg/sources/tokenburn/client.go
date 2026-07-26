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

package tokenburn

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// CostStackClient is the §3 contract boundary as this source needs
// it: the core-agent cost/usage query API, distilled to the two
// reads the poll loop makes. The shipped implementation is
// HTTPClient against the daemon's attach listener (the surface is
// REAL and shipped — core-agent v2.7.0's GET /sessions and GET
// /sessions/{app}/{sid}/usage, pkg/attach handlers.go +
// handlers_operator.go, wire-documented in the attach-http reference
// under "UsageMetadata schema"); tests substitute fakes.
type CostStackClient interface {
	// Sessions lists the daemon's session inventory (active + idle).
	Sessions(ctx context.Context) ([]SessionRef, error)
	// Usage returns one session's cumulative spend totals.
	Usage(ctx context.Context, ref SessionRef) (Usage, error)
}

// SessionActive is the GET /sessions status of a session live in the
// daemon's in-memory registry — the only ones this source polls
// usage for (idle sessions are evicted/persisted rows; their tracker
// is not live).
const SessionActive = "active"

// SessionRef is one row of the daemon's GET /sessions response, as
// this source needs it.
type SessionRef struct {
	// App and ID are the daemon's (AppName, SessionID) pair — the
	// path segments of the qualified /sessions/{app}/{sid}/usage
	// endpoint.
	App string
	ID  string
	// Status is "active" or "idle" (see SessionActive).
	Status string
}

// Usage is the distilled cumulative spend of one session: the
// overall totals of the v2.7.0 UsageMetadata surface (#222).
type Usage struct {
	// TotalTokens is the session's cumulative BILLED tokens:
	// overall input_tokens + output_tokens + thoughts_tokens. Input
	// dominates by construction (each turn resubmits context) —
	// which is exactly right for a spend metric.
	TotalTokens int64
	// CostUSD is the daemon's own cumulative cost estimate (its
	// pricing catalog, cache-rate split applied).
	CostUSD float64
	// Turns is the cumulative model-call count.
	Turns int
}

// HTTPClient is the shipped CostStackClient: the daemon's attach
// listener, same base URL and bearer token as the inject path (§3 —
// the cost stack rides the same daemon lookout already talks to).
type HTTPClient struct {
	base  string
	token string
	hc    *http.Client
}

// NewHTTPClient builds a client for the daemon at baseURL (no
// trailing slash, same rule as --daemon-url). bearerToken may be
// empty for an authless daemon; when set it is sent as
// `Authorization: Bearer <token>` — the same header the injector
// uses (pkg/attach/auth.go accepts it alongside X-Attach-Token).
func NewHTTPClient(baseURL, bearerToken string) *HTTPClient {
	return &HTTPClient{
		base:  strings.TrimRight(baseURL, "/"),
		token: bearerToken,
		hc:    &http.Client{Timeout: 10 * time.Second},
	}
}

// Sessions implements CostStackClient over GET /sessions.
func (c *HTTPClient) Sessions(ctx context.Context) ([]SessionRef, error) {
	// sessionDescriptor rows, core-agent pkg/attach/handlers.go.
	var body struct {
		Sessions []struct {
			App       string `json:"app"`
			SessionID string `json:"sessionID"`
			Status    string `json:"status"`
		} `json:"sessions"`
	}
	if err := c.get(ctx, "/sessions", &body); err != nil {
		return nil, err
	}
	refs := make([]SessionRef, 0, len(body.Sessions))
	for _, row := range body.Sessions {
		refs = append(refs, SessionRef{App: row.App, ID: row.SessionID, Status: row.Status})
	}
	return refs, nil
}

// Usage implements CostStackClient over the qualified
// GET /sessions/{app}/{sid}/usage (attach.UsageInfo — only the
// overall totals are read; per_model/per_turn/digest_methods are
// tolerated and ignored).
func (c *HTTPClient) Usage(ctx context.Context, ref SessionRef) (Usage, error) {
	var body struct {
		Overall struct {
			InputTokens    int64   `json:"input_tokens"`
			OutputTokens   int64   `json:"output_tokens"`
			ThoughtsTokens int64   `json:"thoughts_tokens"`
			Turns          int     `json:"turns"`
			CostUSD        float64 `json:"cost_usd"`
		} `json:"overall"`
	}
	path := "/sessions/" + url.PathEscape(ref.App) + "/" + url.PathEscape(ref.ID) + "/usage"
	if ref.App == "" {
		// Single-segment shortcut form — a daemon that omits app in
		// its descriptors still resolves the session.
		path = "/sessions/" + url.PathEscape(ref.ID) + "/usage"
	}
	if err := c.get(ctx, path, &body); err != nil {
		return Usage{}, err
	}
	return Usage{
		TotalTokens: body.Overall.InputTokens + body.Overall.OutputTokens + body.Overall.ThoughtsTokens,
		CostUSD:     body.Overall.CostUSD,
		Turns:       body.Overall.Turns,
	}, nil
}

// get performs one authenticated GET and decodes the JSON response.
func (c *HTTPClient) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close() //nolint:errcheck // read-only body
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("GET %s: %s: %s", path, resp.Status, strings.TrimSpace(string(snippet)))
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("GET %s: decoding response: %w", path, err)
	}
	return nil
}
