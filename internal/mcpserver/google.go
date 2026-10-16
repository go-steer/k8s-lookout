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

package mcpserver

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/oauth2"

	"github.com/go-steer/steward/allow"
	"github.com/go-steer/steward/googletoken"
	"github.com/go-steer/steward/httpauth"
	"github.com/go-steer/steward/mcpauth"

	"github.com/go-steer/k8s-lookout/pkg/checks"
)

// Google-token mode (#699, docs/mcp-multi-tenant-design.md Part 2): every
// tool call reads as the person who made it. A Gemini Enterprise app calls
// with each person's own Google access token; steward checks it (issued to
// one of our OAuth clients, carrying cloud-platform, a verified account,
// admitted by the allow-list), and the call then runs with that token
// (kube.WithCallerToken), so what the person sees is what their own RBAC
// and IAM allow. lookout's own identity reads nothing on their behalf.

// cloudPlatformScope is the scope a caller's token must carry for lookout
// to read clusters and Cloud APIs with it.
const cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

// defaultMaxConcurrentPerCaller bounds one caller's calls in flight, inside
// the server-wide limit, so one busy agent cannot hold every slot.
const defaultMaxConcurrentPerCaller = 4

// GoogleOptions configures Google-token mode.
type GoogleOptions struct {
	// ClientIDs are the OAuth client IDs the Gemini Enterprise data store
	// is configured with. A token issued to any other client is refused.
	ClientIDs []string
	// Allow admits callers: exact emails, or "@domain" (matched on the
	// account's Google Workspace domain, never on the email suffix).
	Allow []string
	// AllowWrite admits callers to tools that change state (the
	// triage-status write, findings diff and ack, which update the store,
	// and k8s_net_probe_from when served). The gate is per tool, so it
	// also covers their read modes. Empty means no caller may.
	AllowWrite []string
	// Resource is this server's MCP URL as clients reach it, e.g.
	// https://lookout.example.com/mcp. https, except on loopback.
	Resource string
	// Logger receives steward's warning when Google cannot verify a token
	// (an outage, not a bad token). Nil means slog.Default().
	Logger *slog.Logger
	// TokenInfoURL and UserInfoURL replace Google's endpoints (tests).
	TokenInfoURL, UserInfoURL string
}

// GoogleAuth is a configured Google-token mode: the protector for the HTTP
// layer and the write allow-list for the tool layer.
type GoogleAuth struct {
	protector *httpauth.Protector
	write     *allow.List
	// path is the --resource-url path, the one path MCP is served at.
	path string
	// server is the server built WithGoogleAuth(g). Serve refuses to put
	// g's HTTP layer in front of any other server: that server's tools
	// would not read the caller's token, and every authenticated caller
	// would read as lookout.
	server *mcp.Server
}

// NewGoogleAuth validates opts and builds the mode.
func NewGoogleAuth(opts GoogleOptions) (*GoogleAuth, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	v, err := googletoken.New(googletoken.Config{
		ClientIDs:      opts.ClientIDs,
		RequiredScopes: []string{cloudPlatformScope},
		Logger:         logger,
		TokenInfoURL:   opts.TokenInfoURL,
		UserInfoURL:    opts.UserInfoURL,
	})
	if err != nil {
		return nil, fmt.Errorf("--google-client-id: %w", err)
	}
	admit, err := allow.Parse(opts.Allow)
	if err != nil {
		return nil, fmt.Errorf("--google-allow: %w", err)
	}
	a, err := googletoken.NewAuthenticator(v, admit)
	if err != nil {
		return nil, fmt.Errorf("--google-allow: %w", err)
	}
	write, err := allow.Parse(opts.AllowWrite)
	if err != nil {
		return nil, fmt.Errorf("--google-allow-write: %w", err)
	}
	p, err := httpauth.New(httpauth.Options{
		Authenticator: a,
		Resource:      opts.Resource,
		Scopes:        []string{"openid", "email", cloudPlatformScope},
		ResourceName:  "lookout",
		Logger:        logger,
	})
	if err != nil {
		return nil, fmt.Errorf("--resource-url: %w", err)
	}
	u, err := url.Parse(opts.Resource)
	if err != nil {
		return nil, fmt.Errorf("--resource-url: %w", err) // unreachable: httpauth.New parsed it
	}
	path := u.Path
	if path == "" {
		path = "/"
	}
	return &GoogleAuth{protector: p, write: write, path: path}, nil
}

// errNoCaller is a call in Google-token mode that carries no verified
// caller. The HTTP layer refuses those before the SDK sees them, so this
// is a guard, not a path: such a call must never run as lookout.
var errNoCaller = errors.New("no verified caller on this request; refusing to run it as lookout")

// caller returns who made req, and the token to read with, for a call to
// c. It refuses a write tool to a caller the write allow-list does not
// admit.
func (g *GoogleAuth) caller(req *mcp.CallToolRequest, c checks.Command) (googletoken.Identity, oauth2.TokenSource, error) {
	id, ok := mcpauth.Identity(req)
	ts, ok2 := mcpauth.TokenSource(req)
	if !ok || !ok2 {
		return googletoken.Identity{}, nil, errNoCaller
	}
	if c.Writes && !g.write.Admits(id.Email, id.HD) {
		return id, nil, fmt.Errorf("%s changes state, and %s is not admitted to tools that do (the operator's --google-allow-write)", c.MCPName, id.Email)
	}
	return id, ts, nil
}

// callerLimit bounds each caller's calls in flight. A caller over the
// limit is turned away at once rather than queued: an agent that fans out
// past it gets a clear error to back off on, and the slots stay free for
// everyone else.
type callerLimit struct {
	max      int
	mu       sync.Mutex
	inflight map[string]int
}

func newCallerLimit(max int) *callerLimit {
	return &callerLimit{max: max, inflight: map[string]int{}}
}

// acquire takes a slot for caller, or reports false if caller already has
// max calls in flight. release must be called exactly once after a true.
func (l *callerLimit) acquire(caller string) (release func(), ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inflight[caller] >= l.max {
		return nil, false
	}
	l.inflight[caller]++
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.inflight[caller]--; l.inflight[caller] <= 0 {
			delete(l.inflight, caller)
		}
	}, true
}
