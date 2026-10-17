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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-steer/purser"
	"github.com/go-steer/purser/authn"
	"github.com/go-steer/purser/authn/mtls"
	"github.com/go-steer/purser/authz"
	"github.com/go-steer/steward/authserver"
	"github.com/go-steer/steward/mcpauth"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"golang.org/x/oauth2"
	"sigs.k8s.io/yaml"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// Workload callers (#701, docs/mcp-multi-tenant-design.md "Caller kinds"):
// a core-agent daemon or a watch-opened incident session calls the shared
// endpoint with its workload identity, a client certificate, instead of a
// shared bearer token. purser's mTLS authenticators verify the
// certificate and name the caller (a SPIFFE ID); the operator's rules
// file says which tools, and on a multi-cluster endpoint which clusters,
// each workload may call, and whether it reads as lookout or as a service
// account lookout impersonates for it.
//
// The mTLS listener is a second listener beside --listen, not a mode of
// it. purser's mTLS profiles demand a client certificate in the TLS
// handshake (that is what makes a missing or untrusted certificate a
// refused connection rather than a request some later layer must
// remember to refuse), while people reach --listen through a Gateway
// that terminates TLS and presents no certificate of theirs. One
// listener cannot do both without making the certificate optional, so
// each listener has exactly one way in: a request on the mTLS listener
// is a workload or nothing, and nothing falls through from one
// authenticator to another.

// WorkloadOptions configures workload callers.
type WorkloadOptions struct {
	// The standard-CA (PKI) profile: lookout's own certificate and key,
	// and the CA bundle client certificates must chain to. The caller's
	// identity is the certificate's single URI SAN.
	CertFile, KeyFile, ClientCAFile string
	// The SPIFFE profile: the trust domain, and the directory holding
	// lookout's own X509-SVID and the trust bundle in GKE's file layout
	// (certificates.pem, private_key.pem, ca_certificates.pem), re-read as
	// they rotate. The caller's identity is its verified SPIFFE ID.
	SPIFFETrustDomain, SPIFFEDir string
	// RulesFile is the --workload-rules YAML.
	RulesFile string
	// Registry is the served registry, against which rule tool
	// selections are resolved.
	Registry *checks.Registry
	// MultiCluster says whether rules must name clusters (a multi-cluster
	// endpoint) or must not (a single-cluster one).
	MultiCluster bool
	// Impersonate mints a service account's token for rules that name
	// one. Nil means cloud.Impersonate (the GKE provider, -tags gke).
	Impersonate cloud.Impersonator
	// Logger receives credential-reload failures. Nil means slog.Default().
	Logger *slog.Logger
	// ClientCredentialsOnly builds the rules with no mTLS listener: the
	// only workloads are OAuth client-credentials clients of lookout's
	// authorization server (Path B, #702), whose identity is
	// client:<client_id>. The certificate options must then be empty.
	ClientCredentialsOnly bool
}

// WorkloadAuth is a configured set of workload callers: the TLS config
// and authenticator for the mTLS listener, and the rules the tool layer
// applies.
type WorkloadAuth struct {
	tlsConfig   *tls.Config
	authn       authn.Authenticator
	rules       []workloadRule
	multi       bool
	impersonate cloud.Impersonator
	closer      func() error

	mu      sync.Mutex
	sources map[string]oauth2.TokenSource

	// server is the server built WithWorkloadAuth(w); Serve refuses to put
	// w's listener in front of any other server, whose tools would not
	// apply the rules.
	server *mcp.Server
}

// workloadRule is one parsed rule of the --workload-rules file.
type workloadRule struct {
	name           string
	match          authz.Matcher
	tools          map[string]bool
	write          bool
	clusters       []string
	serviceAccount string
}

// workloadRulesFile is the --workload-rules YAML. Keys are strict: a
// misspelt one is an error, not a rule that silently grants less (or
// more) than its author meant.
type workloadRulesFile struct {
	Rules []workloadRuleSpec `json:"rules"`
}

type workloadRuleSpec struct {
	// Name identifies the rule in errors. Required and unique.
	Name string `json:"name"`
	// Identities are exact caller identities (SPIFFE IDs, or the URI SAN
	// under the PKI profile).
	Identities []string `json:"identities,omitempty"`
	// TrustDomain and PathPrefix match a SPIFFE ID structurally (SPIFFE
	// profile only): every ID in the trust domain, under the path prefix
	// when one is given, anchored on whole path segments.
	TrustDomain string `json:"trust_domain,omitempty"`
	PathPrefix  string `json:"path_prefix,omitempty"`
	// Tools uses the --tools selection syntax: tool names, profile names,
	// "all", and "-name" to remove.
	Tools []string `json:"tools"`
	// Write grants the tools above that change state. Without it they are
	// refused even when named.
	Write bool `json:"write,omitempty"`
	// Clusters (multi-cluster endpoints only, and required there): the
	// cluster names or canonical project/location/name this rule allows,
	// or "*" for every cluster the endpoint serves.
	Clusters []string `json:"clusters,omitempty"`
	// ServiceAccount, when set, is the service account the call reads as,
	// through IAM Credentials generateAccessToken. Empty reads as lookout.
	ServiceAccount string `json:"service_account,omitempty"`
}

// NewWorkloadAuth validates opts, loads the rules and the credentials,
// and builds the mTLS listener's TLS config.
func NewWorkloadAuth(opts WorkloadOptions) (*WorkloadAuth, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	pki := opts.CertFile != "" || opts.KeyFile != "" || opts.ClientCAFile != ""
	spiffe := opts.SPIFFETrustDomain != ""
	if opts.ClientCredentialsOnly {
		if pki || spiffe || opts.SPIFFEDir != "" {
			return nil, errors.New("internal: certificate options without the mTLS listener")
		}
		w := &WorkloadAuth{multi: opts.MultiCluster, impersonate: opts.Impersonate, sources: map[string]oauth2.TokenSource{}, closer: func() error { return nil }}
		if w.impersonate == nil {
			w.impersonate = cloud.Impersonate
		}
		rules, err := loadWorkloadRules(opts.RulesFile, opts.Registry, opts.MultiCluster, false)
		if err != nil {
			return nil, err
		}
		w.rules = rules
		return w, nil
	}
	switch {
	case pki && spiffe:
		return nil, errors.New("--tls-cert-file/--tls-key-file/--client-ca-file and --spiffe-trust-domain are exclusive: pick one way to verify workload certificates")
	case !pki && !spiffe:
		return nil, errors.New("--mtls-listen needs a way to verify workload certificates: --tls-cert-file, --tls-key-file and --client-ca-file, or --spiffe-trust-domain")
	case pki && (opts.CertFile == "" || opts.KeyFile == "" || opts.ClientCAFile == ""):
		return nil, errors.New("--tls-cert-file, --tls-key-file and --client-ca-file go together: lookout's certificate, its key, and the CA bundle client certificates must chain to")
	}
	w := &WorkloadAuth{multi: opts.MultiCluster, impersonate: opts.Impersonate, sources: map[string]oauth2.TokenSource{}}
	if w.impersonate == nil {
		w.impersonate = cloud.Impersonate
	}
	rules, err := loadWorkloadRules(opts.RulesFile, opts.Registry, opts.MultiCluster, spiffe)
	if err != nil {
		return nil, err
	}
	w.rules = rules
	if pki {
		pool, err := loadCAPool(opts.ClientCAFile)
		if err != nil {
			return nil, err
		}
		kp, err := newKeyPairReloader(opts.CertFile, opts.KeyFile)
		if err != nil {
			return nil, err
		}
		cfg, a, err := mtls.NewPKI(mtls.PKIOptions{
			GetCertificate: kp.get,
			ClientCAs:      pool,
			Subject:        mtls.SubjectURISAN,
		})
		if err != nil {
			return nil, err
		}
		w.tlsConfig, w.authn, w.closer = cfg, a, func() error { return nil }
		return w, nil
	}
	td, err := spiffeid.TrustDomainFromString(opts.SPIFFETrustDomain)
	if err != nil {
		return nil, fmt.Errorf("--spiffe-trust-domain=%s: %w", opts.SPIFFETrustDomain, err)
	}
	dir := opts.SPIFFEDir
	if dir == "" {
		dir = mtls.GKECredentialsDir
	}
	src, err := mtls.NewSPIFFEFileSource(mtls.SPIFFEFileOptions{
		CertPath:    filepath.Join(dir, mtls.GKECertFile),
		KeyPath:     filepath.Join(dir, mtls.GKEKeyFile),
		BundlePath:  filepath.Join(dir, mtls.GKEBundleFile),
		TrustDomain: td,
		OnError: func(err error) {
			logger.Warn("lookout mcp: workload credentials did not reload; keeping the last good ones", "err", err)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("--spiffe-credentials-dir=%s: %w", dir, err)
	}
	cfg, a, err := mtls.NewSPIFFE(mtls.SPIFFEOptions{SVIDSource: src, BundleSource: src})
	if err != nil {
		_ = src.Close()
		return nil, err
	}
	w.tlsConfig, w.authn, w.closer = cfg, a, src.Close
	return w, nil
}

// Close stops reloading credentials.
func (w *WorkloadAuth) Close() error {
	if w == nil || w.closer == nil {
		return nil
	}
	return w.closer()
}

// NeedsImpersonation reports whether any rule reads as a service account.
func (w *WorkloadAuth) NeedsImpersonation() bool {
	return slices.ContainsFunc(w.rules, func(r workloadRule) bool { return r.serviceAccount != "" })
}

// RuleCount is the number of rules loaded, for the startup banner.
func (w *WorkloadAuth) RuleCount() int { return len(w.rules) }

func loadCAPool(path string) (*x509.CertPool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("--client-ca-file=%s: %v", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw) {
		return nil, fmt.Errorf("--client-ca-file=%s: no PEM certificates in the file", path)
	}
	return pool, nil
}

// keyPairReloader serves lookout's own certificate, re-read when either
// file changes (cert-manager renews a Secret in place), checked at most
// every keyPairCheck. A file that fails to load keeps the last good pair.
type keyPairReloader struct {
	certFile, keyFile string

	mu      sync.Mutex
	cert    *tls.Certificate
	stamp   string
	checked time.Time
}

const keyPairCheck = 30 * time.Second

func newKeyPairReloader(certFile, keyFile string) (*keyPairReloader, error) {
	r := &keyPairReloader{certFile: certFile, keyFile: keyFile}
	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *keyPairReloader) fileStamp() string {
	var b strings.Builder
	for _, f := range []string{r.certFile, r.keyFile} {
		if st, err := os.Stat(f); err == nil {
			fmt.Fprintf(&b, "%d/%d;", st.ModTime().UnixNano(), st.Size())
		}
	}
	return b.String()
}

func (r *keyPairReloader) load() error {
	stamp := r.fileStamp()
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		// The error names the files, never their contents.
		return fmt.Errorf("--tls-cert-file=%s / --tls-key-file=%s: %v", r.certFile, r.keyFile, err)
	}
	r.cert, r.stamp, r.checked = &cert, stamp, time.Now()
	return nil
}

func (r *keyPairReloader) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Since(r.checked) < keyPairCheck {
		return r.cert, nil
	}
	r.checked = time.Now()
	if r.fileStamp() != r.stamp {
		_ = r.load() // on failure the last good pair stays
	}
	return r.cert, nil
}

// loadWorkloadRules reads and validates the rules file.
func loadWorkloadRules(path string, reg *checks.Registry, multi, spiffe bool) ([]workloadRule, error) {
	if path == "" {
		return nil, errors.New("--mtls-listen needs --workload-rules: without rules no workload may call anything")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("--workload-rules=%s: %v", path, err)
	}
	var f workloadRulesFile
	if err := yaml.UnmarshalStrict(raw, &f); err != nil {
		return nil, fmt.Errorf("--workload-rules=%s: %v", path, err)
	}
	if len(f.Rules) == 0 {
		return nil, fmt.Errorf("--workload-rules=%s: no rules; without rules no workload may call anything", path)
	}
	seen := map[string]bool{}
	var out []workloadRule
	for i, spec := range f.Rules {
		r, err := parseWorkloadRule(spec, reg, multi, spiffe)
		if err != nil {
			name := spec.Name
			if name == "" {
				name = fmt.Sprintf("#%d", i+1)
			}
			return nil, fmt.Errorf("--workload-rules=%s: rule %s: %v", path, name, err)
		}
		if seen[r.name] {
			return nil, fmt.Errorf("--workload-rules=%s: rule %s is declared twice; names identify a rule in errors and must be unique", path, r.name)
		}
		seen[r.name] = true
		out = append(out, r)
	}
	return out, nil
}

func parseWorkloadRule(spec workloadRuleSpec, reg *checks.Registry, multi, spiffe bool) (workloadRule, error) {
	r := workloadRule{name: strings.TrimSpace(spec.Name), write: spec.Write, serviceAccount: strings.TrimSpace(spec.ServiceAccount)}
	if r.name == "" {
		return r, errors.New("no name")
	}
	var matchers []authz.Matcher
	var ids []string
	for _, id := range spec.Identities {
		if id = strings.TrimSpace(id); id == "" {
			return r, errors.New("an empty identity")
		}
		ids = append(ids, id)
	}
	if len(ids) > 0 {
		matchers = append(matchers, authz.MatchIdentity(ids...))
	}
	switch {
	case spec.PathPrefix != "" && spec.TrustDomain == "":
		return r, errors.New("path_prefix needs trust_domain: a SPIFFE path is only unique within one trust domain")
	case spec.TrustDomain != "" && !spiffe:
		return r, errors.New("trust_domain and path_prefix match SPIFFE IDs and need --spiffe-trust-domain; under --client-ca-file name callers by identities")
	case spec.TrustDomain != "":
		td, err := spiffeid.TrustDomainFromString(spec.TrustDomain)
		if err != nil {
			return r, fmt.Errorf("trust_domain %q: %v", spec.TrustDomain, err)
		}
		m := []authz.Matcher{authz.MatchLabel(mtls.LabelTrustDomain, td.Name())}
		if p := strings.Trim(spec.PathPrefix, "/"); p != "" {
			segs := strings.Split(p, "/")
			if slices.Contains(segs, "") {
				return r, fmt.Errorf("path_prefix %q has an empty segment", spec.PathPrefix)
			}
			m = append(m, authz.MatchPathPrefix(mtls.LabelPath, segs...))
		}
		matchers = append(matchers, authz.MatchAll(m...))
	}
	if len(matchers) == 0 {
		return r, errors.New("matches no caller: give identities, or trust_domain (with an optional path_prefix)")
	}
	r.match = authz.MatchAnyOf(matchers...)

	tools, err := resolveRuleTools(reg, spec.Tools)
	if err != nil {
		return r, err
	}
	r.tools = tools

	for _, c := range spec.Clusters {
		if c = strings.TrimSpace(c); c == "" {
			return r, errors.New("an empty cluster name")
		}
		r.clusters = append(r.clusters, c)
	}
	switch {
	case multi && len(r.clusters) == 0:
		return r, errors.New(`this is a multi-cluster endpoint: name the clusters the rule allows, or "*" for all of them`)
	case !multi && len(r.clusters) > 0:
		return r, errors.New("clusters only applies to a multi-cluster endpoint (--clusters or --clusters-from)")
	}
	if r.serviceAccount != "" && !strings.Contains(r.serviceAccount, "@") {
		return r, fmt.Errorf("service_account %q is not a service account email", r.serviceAccount)
	}
	return r, nil
}

// resolveRuleTools resolves a rule's tools with the --tools syntax. A
// privileged tool (k8s_net_probe_from) is never selectable there, so it is
// accepted here by its exact name; k8s_list_clusters is not a registered
// command and needs no grant (it lists only the clusters a rule allows).
func resolveRuleTools(reg *checks.Registry, tools []string) (map[string]bool, error) {
	out := map[string]bool{}
	var rest []string
	for _, t := range tools {
		t = strings.TrimSpace(t)
		if t == listClustersName {
			continue
		}
		if slices.ContainsFunc(reg.All(), func(c checks.Command) bool { return c.Privileged && !c.Hidden && c.MCPName == t }) {
			out[t] = true
			continue
		}
		rest = append(rest, t)
	}
	if len(rest) > 0 {
		set, err := ResolveTools(reg, "", strings.Join(rest, ","))
		if err != nil {
			return nil, err
		}
		for n := range set {
			out[n] = true
		}
	}
	if len(out) == 0 {
		return nil, errors.New("grants no tools: list tools, profiles or all")
	}
	return out, nil
}

// extraWorkload is the TokenInfo key the mTLS listener puts the verified
// workload caller under.
const extraWorkload = "lookout.workload-caller"

// placeholderBearer stands in for a token on mTLS requests. The SDK's
// RequireBearerToken is the only way to put a TokenInfo on a request,
// and the TokenInfo is how a caller reaches each tool call (a stateful
// session runs every call with the initialize request's context) and how
// the SDK binds a session to the caller who opened it. It is set only
// after the certificate has been verified, carries nothing, and is
// removed again before the SDK sees the request.
const placeholderBearer = "Bearer mtls-verified-caller" //nolint:gosec // G101: a placeholder that carries nothing, not a credential

// middleware authenticates each request on the mTLS listener by its
// client certificate, and carries the caller to the tool calls.
func (w *WorkloadAuth) middleware(next http.Handler) http.Handler {
	bind := auth.RequireBearerToken(func(ctx context.Context, _ string, _ *http.Request) (*auth.TokenInfo, error) {
		c, ok := ctx.Value(workloadCallerKey{}).(purser.Caller)
		if !ok {
			return nil, auth.ErrInvalidToken
		}
		return &auth.TokenInfo{UserID: c.Identity, Extra: map[string]any{extraWorkload: c}}, nil
	}, &auth.RequireBearerTokenOptions{AllowMissingExpiration: true})
	inner := bind(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		r.Header.Del("Authorization")
		next.ServeHTTP(rw, r)
	}))
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		c, err := w.authn.Authenticate(r)
		if err != nil || c.IsZero() {
			http.Error(rw, "unauthorized", http.StatusUnauthorized)
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), workloadCallerKey{}, c))
		r.Header = r.Header.Clone()
		// Whatever Authorization the client sent is dropped: on this
		// listener the certificate is the credential.
		r.Header.Set("Authorization", placeholderBearer)
		inner.ServeHTTP(rw, r)
	})
}

type workloadCallerKey struct{}

// workloadCallerOf returns the workload caller of a tool call: one that
// came through the mTLS listener, or an OAuth client-credentials client of
// lookout's authorization server (Path B, #702), whose identity is
// client:<client_id>. The latter is told apart from a person by steward's
// Principal.Workload, never by the purser auth source (every steward token
// reports oidc).
func workloadCallerOf(req mcp.Request) (purser.Caller, bool) {
	ti := tokenInfoOf(req)
	if ti == nil {
		return purser.Caller{}, false
	}
	if c, ok := ti.Extra[extraWorkload].(purser.Caller); ok && !c.IsZero() {
		return c, true
	}
	if p, ok := mcpauth.Principal(req); ok && p.Workload {
		c := p.Caller()
		return c, !c.IsZero()
	}
	return purser.Caller{}, false
}

func tokenInfoOf(req mcp.Request) *auth.TokenInfo {
	if req == nil {
		return nil
	}
	extra := req.GetExtra()
	if extra == nil {
		return nil
	}
	return extra.TokenInfo
}

// workloadCall is one tool call's workload caller and the rules that
// grant it so far.
type workloadCall struct {
	caller purser.Caller
	rules  []workloadRule
}

// allowTool returns the rules that match c and grant it tool cmd. A tool
// that changes state needs a rule that also says write: true.
func (w *WorkloadAuth) allowTool(c purser.Caller, cmd checks.Command) (*workloadCall, error) {
	var named, granted []workloadRule
	for _, r := range w.rules {
		if !r.match(c) || !r.tools[cmd.MCPName] {
			continue
		}
		named = append(named, r)
		if !cmd.Writes || r.write {
			granted = append(granted, r)
		}
	}
	switch {
	case len(granted) > 0 && cmd.Writes && c.Label(authserver.LabelKind) == authserver.KindWorkload && !hasStewardScope(c, ScopeWrite):
		// A client-credentials token (Path B) carries scopes as well: a
		// tool that changes state needs lookout:write on top of the rule.
		return nil, fmt.Errorf("%s changes state, and the access token of %s was not granted the %s scope", cmd.MCPName, c.Identity, ScopeWrite)
	case len(granted) > 0:
		return &workloadCall{caller: c, rules: granted}, nil
	case len(named) > 0:
		return nil, fmt.Errorf("%s changes state, and no rule granting it to %s says write: true (the operator's --workload-rules)", cmd.MCPName, c.Identity)
	default:
		return nil, fmt.Errorf("%s is not granted %s by the operator's --workload-rules", c.Identity, cmd.MCPName)
	}
}

// allowCluster narrows a call's rules to those allowing cluster e.
func (w *WorkloadAuth) allowCluster(call *workloadCall, e clusterEntry) error {
	var kept []workloadRule
	for _, r := range call.rules {
		if r.allowsCluster(e) {
			kept = append(kept, r)
		}
	}
	if len(kept) == 0 {
		return fmt.Errorf("%s is not granted cluster %s by the operator's --workload-rules", call.caller.Identity, e.canonical)
	}
	call.rules = kept
	return nil
}

func (r workloadRule) allowsCluster(e clusterEntry) bool {
	for _, c := range r.clusters {
		if c == "*" || c == e.canonical || c == e.name || c == e.ref.Name {
			return true
		}
	}
	return false
}

// listsCluster reports whether any rule matching c allows cluster e, for
// k8s_list_clusters, which lists only the clusters a workload may call.
func (w *WorkloadAuth) listsCluster(c purser.Caller, e clusterEntry) bool {
	for _, r := range w.rules {
		if r.match(c) && r.allowsCluster(e) {
			return true
		}
	}
	return false
}

// matchesAny reports whether any rule matches c at all.
func (w *WorkloadAuth) matchesAny(c purser.Caller) bool {
	return slices.ContainsFunc(w.rules, func(r workloadRule) bool { return r.match(c) })
}

// readAs puts the call's read identity on ctx: lookout's own (nothing to
// add) or the service account the granting rules name, through
// impersonation. endpoint is the cluster's control-plane endpoint on a
// multi-cluster endpoint, "" otherwise. Rules that disagree, or an
// impersonation that fails, fail the call: it never quietly reads as
// lookout instead.
func (w *WorkloadAuth) readAs(ctx context.Context, call *workloadCall, endpoint string) (context.Context, error) {
	ctx = kube.WithWorkloadCaller(ctx)
	var sas []string
	for _, r := range call.rules {
		if !slices.Contains(sas, r.serviceAccount) {
			sas = append(sas, r.serviceAccount)
		}
	}
	if len(sas) > 1 {
		names := make([]string, 0, len(call.rules))
		for _, r := range call.rules {
			names = append(names, r.name)
		}
		return ctx, fmt.Errorf("the rules granting this call to %s (%s) read as different identities; the operator's --workload-rules must agree on one service_account (or none)",
			call.caller.Identity, strings.Join(names, ", "))
	}
	sa := sas[0]
	if sa == "" {
		return ctx, nil
	}
	if endpoint != "" && !IsGKEEndpoint(endpoint) {
		return ctx, fmt.Errorf("refusing to send service account %s's token to %q: impersonated reads go only to GKE control-plane DNS endpoints (*.gke.goog)", sa, endpoint)
	}
	ts, err := w.tokenSource(ctx, sa)
	if err == nil {
		// Minted now, so a failure is this call's explicit error rather
		// than a confusing one from deep inside a check.
		_, err = ts.Token()
	}
	if err != nil {
		return ctx, fmt.Errorf("reading as service account %s for %s: %s", sa, call.caller.Identity, emit.MaskString(err.Error()))
	}
	return kube.WithCallerToken(ctx, ts), nil
}

// tokenSource returns the cached token source for sa, creating it once.
// A source that could not be created is not cached, so the next call
// tries again.
func (w *WorkloadAuth) tokenSource(ctx context.Context, sa string) (oauth2.TokenSource, error) {
	w.mu.Lock()
	ts := w.sources[sa]
	w.mu.Unlock()
	if ts != nil {
		return ts, nil
	}
	ts, err := w.impersonate(ctx, sa)
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if prev := w.sources[sa]; prev != nil {
		return prev, nil
	}
	w.sources[sa] = ts
	return ts, nil
}
