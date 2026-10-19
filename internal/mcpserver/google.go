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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/go-steer/purser"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/oauth2"

	"github.com/go-steer/steward/allow"
	"github.com/go-steer/steward/authserver"
	"github.com/go-steer/steward/googletoken"
	"github.com/go-steer/steward/httpauth"
	"github.com/go-steer/steward/mcpauth"
	"github.com/go-steer/steward/seal"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
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
	// Allow admits callers: exact emails, "@domain" (matched on the
	// account's Google Workspace domain, never on the email suffix), or
	// "group:<group email>" (every member, directly or through nested
	// groups; needs GroupsServiceAccount).
	Allow []string
	// AllowWrite admits callers to tools that change state (the
	// triage-status write, findings diff and ack, which update the store,
	// and k8s_net_probe_from when served), with the same entries as
	// Allow. The gate is per tool, so it also covers their read modes.
	// Empty means no caller may.
	AllowWrite []string
	// GroupsServiceAccount is the service account group entries are
	// checked as, in the Cloud Identity Groups API: it holds the Groups
	// Reader admin role, and lookout impersonates it for those calls
	// only, with the read-only groups scope. Required when Allow or
	// AllowWrite has a group entry, refused when neither has one.
	GroupsServiceAccount string
	// Impersonate mints GroupsServiceAccount's tokens. Nil means
	// cloud.Impersonate (the GKE provider, -tags gke; without it, group
	// entries are refused).
	Impersonate cloud.Impersonator
	// CloudIdentityEndpoint replaces the Cloud Identity API's (tests).
	CloudIdentityEndpoint string
	// Resource is this server's MCP URL as clients reach it, e.g.
	// https://lookout.example.com/mcp. https, except on loopback.
	Resource string
	// Logger receives steward's warning when Google cannot verify a token
	// (an outage, not a bad token). Nil means slog.Default().
	Logger *slog.Logger
	// TokenInfoURL and UserInfoURL replace Google's endpoints (tests).
	TokenInfoURL, UserInfoURL string
	// AuthServer, when non-nil, adds Path B (#702): lookout's own OAuth
	// authorization server, for generic MCP clients (Claude Code,
	// gemini-cli, IDEs) that discover an authorization server and register
	// with it. People sign in with Google through it and read as
	// themselves exactly as Path A callers do; the same Allow list admits
	// them.
	AuthServer *AuthServerOptions
}

// Path B scopes: lookout's own, carried by every token its authorization
// server issues. A client is granted read unless it asks for more; a tool
// that changes state needs write as well as the --google-allow-write
// admission (docs/mcp-multi-tenant-design.md, "Three layers of
// authorization"). Google tokens (Path A) carry neither, and there the
// allow-lists alone decide.
const (
	ScopeRead  = "lookout:read"
	ScopeWrite = "lookout:write"
)

// AuthServerOptions configures Path B, lookout's OAuth 2.1 authorization
// server (steward authserver), mounted on the --listen handler.
type AuthServerOptions struct {
	// Issuer is the authorization server's public base URL, exactly as
	// clients reach it (e.g. https://lookout.example.com). https, except
	// on loopback. Its endpoints are served on the same listener as MCP.
	Issuer string
	// Google is lookout's own confidential OAuth client at Google, which
	// people sign in through. Its redirect URIs must include the server's
	// callback URL (CallbackURL).
	Google authserver.GoogleClient
	// Custody holds each person's Google grant: *authserver.VaultCustody
	// or authserver.SealedCustody.
	Custody authserver.Custody
	// Keys seals every token, code and registered client the server hands
	// out.
	Keys seal.KeyProvider
	// Clients are pre-registered clients. A client-credentials client is a
	// workload, governed by the --workload-rules (identity client:<id>);
	// it may not name a service_account here, since a workload's service
	// account is set in one place, its rule.
	Clients []authserver.StaticClient
	// OpenRegistration accepts clients that register themselves (DCR, or a
	// Client ID Metadata Document). Off: only Clients.
	OpenRegistration bool
	// Replay spends codes and tracks refresh-token families. Nil is an
	// in-memory cache, per replica.
	Replay authserver.ReplayCache
	// HTTPClient and CIMDClient replace steward's clients (tests).
	HTTPClient, CIMDClient *http.Client
}

// GoogleAuth is a configured Google-token mode: the protector for the HTTP
// layer and the write allow-list for the tool layer.
type GoogleAuth struct {
	protector *httpauth.Protector
	// write admits callers to tools that change state; nil when
	// --google-allow-write is empty, and no caller may.
	write *allow.Admitter
	// logger receives the detail of a write-gate group check that failed.
	logger *slog.Logger
	// groupsProbed is closed when the startup token mint for the groups
	// service account has finished (tests); nil without group entries.
	groupsProbed chan struct{}
	// as is Path B's authorization server, nil without it.
	as *authserver.Server
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
	list, err := allow.Parse(opts.Allow)
	if err != nil {
		return nil, fmt.Errorf("--google-allow: %w", err)
	}
	writeList, err := allow.Parse(opts.AllowWrite)
	if err != nil {
		return nil, fmt.Errorf("--google-allow-write: %w", err)
	}
	// One checker for both lists, so they share its membership cache.
	groups, probe, err := groupChecker(opts, list, writeList, logger)
	if err != nil {
		return nil, err
	}
	// One admitter for both paths, so the same people get in either way.
	admit, err := allow.NewAdmitter(list, groups)
	if err != nil {
		return nil, fmt.Errorf("--google-allow: %w", err)
	}
	a, err := googletoken.NewAdmittingAuthenticator(v, admit)
	if err != nil {
		return nil, fmt.Errorf("--google-allow: %w", err)
	}
	var write *allow.Admitter
	if !writeList.Empty() {
		if write, err = allow.NewAdmitter(writeList, groups); err != nil {
			return nil, fmt.Errorf("--google-allow-write: %w", err)
		}
	}
	popts := httpauth.Options{
		Authenticator: a,
		Resource:      opts.Resource,
		Scopes:        []string{"openid", "email", cloudPlatformScope},
		ResourceName:  "lookout",
		Logger:        logger,
	}
	var as *authserver.Server
	if b := opts.AuthServer; b != nil {
		as, err = newAuthServer(*b, opts.Resource, admit, logger)
		if err != nil {
			return nil, err
		}
		popts.AuthServer = as
		// The metadata names lookout's own authorization server and its
		// scopes; the 401 challenge asks for read only.
		popts.Scopes = nil
		// Every steward token must carry read; one without it is a 403
		// insufficient_scope, which starts a client's step-up.
		popts.RequiredScopes = []string{ScopeRead}
	}
	p, err := httpauth.New(popts)
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
	g := &GoogleAuth{protector: p, write: write, logger: logger, as: as, path: path}
	if probe != nil {
		g.groupsProbed = make(chan struct{})
		go probeGroupsAccount(probe, logger, g.groupsProbed)
	}
	return g, nil
}

// newAuthServer builds Path B's authorization server.
func newAuthServer(b AuthServerOptions, resource string, admit *allow.Admitter, logger *slog.Logger) (*authserver.Server, error) {
	for _, c := range b.Clients {
		if c.ServiceAccount != "" {
			// steward could seal the account's token into the client's
			// access tokens, but a workload's identity is set in one place:
			// its --workload-rules rule, the same rule that grants it tools
			// and clusters whichever way it authenticates.
			return nil, fmt.Errorf("--oauth-clients-file: client %q names a service_account; in lookout a workload's service account is set on its --workload-rules rule (identities: [%s])",
				c.ID, authserver.WorkloadSubject(c.ID))
		}
	}
	if len(b.Clients) == 0 && !b.OpenRegistration {
		return nil, errors.New("--oauth-issuer: no client could sign in: pre-register clients with --oauth-clients-file, or pass --oauth-open-registration")
	}
	as, err := authserver.New(authserver.Config{
		Issuer:           b.Issuer,
		Resource:         resource,
		Scopes:           []string{ScopeRead, ScopeWrite},
		DefaultScopes:    []string{ScopeRead},
		Google:           b.Google,
		Custody:          b.Custody,
		Admit:            admit,
		Keys:             b.Keys,
		Clients:          b.Clients,
		OpenRegistration: b.OpenRegistration,
		Replay:           b.Replay,
		ServiceName:      "lookout",
		HTTPClient:       b.HTTPClient,
		CIMDClient:       b.CIMDClient,
		Logger:           logger,
	})
	if err != nil {
		return nil, fmt.Errorf("--oauth-issuer: %w", err)
	}
	return as, nil
}

// CallbackURL is Path B's Google redirect URI, which the operator registers
// on the Google OAuth client; "" without Path B.
func (g *GoogleAuth) CallbackURL() string {
	if g == nil || g.as == nil {
		return ""
	}
	return g.as.CallbackURL()
}

// HasAuthServer reports whether Path B is configured.
func (g *GoogleAuth) HasAuthServer() bool { return g != nil && g.as != nil }

// errNoCaller is a call in Google-token mode that carries no verified
// caller. The HTTP layer refuses those before the SDK sees them, so this
// is a guard, not a path: such a call must never run as lookout.
var errNoCaller = errors.New("no verified caller on this request; refusing to run it as lookout")

// caller returns who made req, and the token to read with, for a call to
// c. It refuses a write tool to a caller the write allow-list does not
// admit, and, for a Path B token, to one not granted lookout:write.
func (g *GoogleAuth) caller(ctx context.Context, req *mcp.CallToolRequest, c checks.Command) (googletoken.Identity, oauth2.TokenSource, error) {
	id, ok := mcpauth.Identity(req)
	ts, ok2 := mcpauth.TokenSource(req)
	if !ok || !ok2 {
		return googletoken.Identity{}, nil, errNoCaller
	}
	if c.Writes {
		if err := g.admitWrite(ctx, id); err != nil {
			return id, nil, fmt.Errorf("%s changes state, and %w", c.MCPName, err)
		}
	}
	if p, ok := mcpauth.Principal(req); ok && c.Writes && !p.HasScope(ScopeWrite) {
		return id, nil, fmt.Errorf("%s changes state, and this sign-in was not granted the %s scope; sign in again asking for scope %q",
			c.MCPName, ScopeWrite, ScopeRead+" "+ScopeWrite)
	}
	return id, ts, nil
}

// admitWrite asks the write allow-list about id. A group lookup that
// failed is not a refusal: the caller is told to retry, and the detail
// goes to the operator's log rather than to the caller.
func (g *GoogleAuth) admitWrite(ctx context.Context, id googletoken.Identity) error {
	notAdmitted := fmt.Errorf("%s is not admitted to tools that do (the operator's --google-allow-write)", id.Email)
	if g.write == nil {
		return notAdmitted
	}
	switch err := g.write.Admit(ctx, id.Email, id.HD); {
	case errors.Is(err, allow.ErrNotAdmitted):
		return notAdmitted
	case err != nil:
		g.logger.WarnContext(ctx, "lookout mcp: --google-allow-write group check failed", "caller", id.Email, "err", err)
		return fmt.Errorf("whether %s may call tools that do could not be checked (a --google-allow-write group lookup failed); retry shortly", id.Email)
	}
	return nil
}

// hasStewardScope reports whether a steward-token caller's labels grant
// scope.
func hasStewardScope(c purser.Caller, scope string) bool {
	return slices.Contains(strings.Fields(c.Label(authserver.LabelScope)), scope)
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
	return l.acquireUpTo(caller, l.max)
}

// acquireUpTo is acquire with this call's own cap, for a limit whose
// size depends on the call (the per-cluster limit follows the size of the
// caller's cluster set).
func (l *callerLimit) acquireUpTo(caller string, max int) (release func(), ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inflight[caller] >= max {
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
