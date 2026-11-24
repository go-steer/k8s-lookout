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

package cloud

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"golang.org/x/oauth2"
)

const (
	// GoogleAuthEnv selects how lookout authenticates outbound Google Cloud and
	// GKE control-plane DNS requests ("auto", "adc", or "injected").
	GoogleAuthEnv = "LOOKOUT_GOOGLE_AUTH"
	// CoreAgentGoogleAuthEnv and MastGoogleAuthEnv are the upstream agent task
	// switches on Agent Substrate: when set to "injected" (and GoogleAuthEnv is
	// unset or "auto"), outbound requests send a static placeholder bearer token
	// that Substrate's egress gateway replaces with the real Google credential.
	CoreAgentGoogleAuthEnv = "CORE_AGENT_GOOGLE_AUTH"
	MastGoogleAuthEnv      = "MAST_GOOGLE_AUTH"

	// GoogleAuthAuto consults LOOKOUT_GOOGLE_AUTH, then CORE_AGENT_GOOGLE_AUTH
	// and MAST_GOOGLE_AUTH, falling back to Application Default Credentials.
	GoogleAuthAuto = "auto"
	// GoogleAuthADC uses Application Default Credentials even when an upstream
	// *_GOOGLE_AUTH=injected variable is present in the environment.
	GoogleAuthADC = "adc"
	// GoogleAuthInjected sends a static placeholder bearer token instead of
	// resolving Application Default Credentials or contacting the GCE metadata
	// server.
	GoogleAuthInjected = "injected"

	// InjectedBearerToken is the placeholder bearer token sent when injected
	// Google auth is active so Substrate's egress gateway injects the real token.
	InjectedBearerToken = "placeholder"

	// CredentialProxyURLEnv and CredentialProxyTokenFileEnv configure outbound
	// routing through a credential proxy (/v1/gcp/<host>/ for Google REST APIs
	// and /v1/k8s/<project>/<location>/<cluster> for Kubernetes API servers).
	CredentialProxyURLEnv       = "CREDENTIAL_PROXY_URL"        //nolint:gosec // env var name, not a credential
	CredentialProxyTokenFileEnv = "CREDENTIAL_PROXY_TOKEN_FILE" //nolint:gosec // env var name, not a credential
)

type googleAuthKey struct{}

// NormalizeGoogleAuth validates a --google-auth / LOOKOUT_GOOGLE_AUTH mode and
// returns its canonical spelling ("", "adc", or "injected").
func NormalizeGoogleAuth(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", GoogleAuthAuto:
		return "", nil
	case GoogleAuthADC, "default":
		return GoogleAuthADC, nil
	case GoogleAuthInjected:
		return GoogleAuthInjected, nil
	default:
		return "", fmt.Errorf("invalid Google auth mode %q (want %s, %s, or %s)", raw, GoogleAuthAuto, GoogleAuthADC, GoogleAuthInjected)
	}
}

// ValidateGoogleAuth validates an explicit mode and, when auto/empty, validates
// LOOKOUT_GOOGLE_AUTH if set in the environment.
func ValidateGoogleAuth(explicit string) error {
	mode, err := NormalizeGoogleAuth(explicit)
	if err != nil {
		return err
	}
	if mode != "" {
		return nil
	}
	if env := strings.TrimSpace(os.Getenv(GoogleAuthEnv)); env != "" {
		if _, err := NormalizeGoogleAuth(env); err != nil {
			return fmt.Errorf("%s: %w", GoogleAuthEnv, err)
		}
	}
	return nil
}

// WithGoogleAuth binds an explicit Google auth mode ("adc" or "injected") to
// ctx for the duration of an invocation. An empty or "auto" mode leaves
// environment resolution active.
func WithGoogleAuth(ctx context.Context, mode string) context.Context {
	norm, err := NormalizeGoogleAuth(mode)
	if err != nil || norm == "" {
		return ctx
	}
	return context.WithValue(ctx, googleAuthKey{}, norm)
}

// IsInjectedGoogleAuth reports whether ctx or the process environment enables
// injected Google auth.
func IsInjectedGoogleAuth(ctx context.Context) bool {
	if ctx != nil {
		if mode, ok := ctx.Value(googleAuthKey{}).(string); ok && mode != "" {
			return mode == GoogleAuthInjected
		}
	}
	if env := strings.TrimSpace(os.Getenv(GoogleAuthEnv)); env != "" {
		norm, err := NormalizeGoogleAuth(env)
		if err != nil {
			return false
		}
		if norm != "" {
			return norm == GoogleAuthInjected
		}
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv(CoreAgentGoogleAuthEnv)), GoogleAuthInjected) {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(os.Getenv(MastGoogleAuthEnv)), GoogleAuthInjected)
}

// InjectedTokenSource returns a static OAuth2 token source yielding the
// placeholder bearer token expected by an injecting egress gateway.
func InjectedTokenSource() oauth2.TokenSource {
	return oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: InjectedBearerToken,
		TokenType:   "Bearer",
	})
}

// CredentialProxy describes an outbound credential proxy configured via
// CREDENTIAL_PROXY_URL and optional CREDENTIAL_PROXY_TOKEN_FILE.
type CredentialProxy struct {
	// URL is the validated http(s) base URL with no trailing slash.
	URL string
	// TokenFile is the optional file holding the bearer token presented to the
	// proxy on each request; re-read per request so rotated tokens take effect.
	TokenFile string

	parsed *url.URL
}

// CredentialProxyFromEnv parses CREDENTIAL_PROXY_URL and
// CREDENTIAL_PROXY_TOKEN_FILE from the environment. Returns (nil, nil) when
// CREDENTIAL_PROXY_URL is unset.
func CredentialProxyFromEnv() (*CredentialProxy, error) {
	raw := strings.TrimRight(strings.TrimSpace(os.Getenv(CredentialProxyURLEnv)), "/")
	if raw == "" {
		return nil, nil
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("%s=%q: want an http(s) URL", CredentialProxyURLEnv, raw)
	}
	return &CredentialProxy{
		URL:       raw,
		TokenFile: strings.TrimSpace(os.Getenv(CredentialProxyTokenFileEnv)),
		parsed:    u,
	}, nil
}

// HasCredentialProxy reports whether CREDENTIAL_PROXY_URL is non-empty in the
// process environment.
func HasCredentialProxy() bool {
	return strings.TrimSpace(os.Getenv(CredentialProxyURLEnv)) != ""
}

// K8sHost returns the proxy URL prefix for one GKE cluster's Kubernetes API
// (/v1/k8s/<project>/<location>/<cluster>), validating every path segment so an
// untrusted cluster reference cannot traverse outside the route.
func (p *CredentialProxy) K8sHost(id ClusterID) (string, error) {
	if !ValidClusterID(id.Project, id.Location, id.Cluster) {
		return "", fmt.Errorf("invalid GKE cluster reference %q for credential proxy", id.Triple())
	}
	return fmt.Sprintf("%s/v1/k8s/%s/%s/%s",
		p.URL,
		url.PathEscape(id.Project),
		url.PathEscape(id.Location),
		url.PathEscape(id.Cluster),
	), nil
}

// CheckRedirect refuses cross-origin HTTP redirects so proxy credentials or
// requests routed through CREDENTIAL_PROXY_URL are never followed onto an
// untrusted host.
func (p *CredentialProxy) CheckRedirect(req *http.Request, _ []*http.Request) error {
	if !p.matchesTarget(req.URL) {
		return fmt.Errorf("refusing redirect from %s to %s://%s", CredentialProxyURLEnv, req.URL.Scheme, req.URL.Host)
	}
	return nil
}

func (p *CredentialProxy) matchesTarget(u *url.URL) bool {
	return u != nil && p != nil && p.parsed != nil &&
		strings.EqualFold(u.Scheme, p.parsed.Scheme) &&
		strings.EqualFold(u.Host, p.parsed.Host)
}

// WrapRoundTripper wraps base so every request to the credential proxy carries
// the current bearer token from p.TokenFile when configured. Requests targeting
// any other origin are refused so the proxy token never leaks on cross-origin
// redirects or misrouted URLs.
func (p *CredentialProxy) WrapRoundTripper(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if p.TokenFile == "" {
		return base
	}
	return &proxyBearerTransport{target: p.parsed, tokenFile: p.TokenFile, base: base}
}

// GCPTransport returns an http.RoundTripper that rewrites outbound Google REST
// API requests (https://<service>.googleapis.com/<path>) onto
// <CREDENTIAL_PROXY_URL>/v1/gcp/<service>.googleapis.com/<path> and attaches
// the proxy bearer token when CREDENTIAL_PROXY_TOKEN_FILE is set.
func (p *CredentialProxy) GCPTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &proxyGCPTransport{
		target: p.parsed,
		next:   p.WrapRoundTripper(base),
	}
}

type proxyBearerTransport struct {
	target    *url.URL
	tokenFile string
	base      http.RoundTripper
}

func (t *proxyBearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.target == nil || !strings.EqualFold(req.URL.Scheme, t.target.Scheme) || !strings.EqualFold(req.URL.Host, t.target.Host) {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, fmt.Errorf("refusing to send %s token to %s://%s", CredentialProxyTokenFileEnv, req.URL.Scheme, req.URL.Host)
	}
	raw, err := os.ReadFile(t.tokenFile) //nolint:gosec // operator-configured CREDENTIAL_PROXY_TOKEN_FILE path
	if err == nil && strings.TrimSpace(string(raw)) == "" {
		err = errors.New("token file is empty")
	}
	if err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, fmt.Errorf("reading %s (%s): %w", CredentialProxyTokenFileEnv, t.tokenFile, err)
	}
	out := req.Clone(req.Context())
	out.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(raw)))
	return t.base.RoundTrip(out)
}

type proxyGCPTransport struct {
	target *url.URL
	next   http.RoundTripper
}

func (t *proxyGCPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	host := req.URL.Hostname()
	if !strings.HasSuffix(host, ".googleapis.com") {
		if t.target != nil && strings.EqualFold(req.URL.Scheme, t.target.Scheme) && strings.EqualFold(req.URL.Host, t.target.Host) {
			out := req.Clone(req.Context())
			out.Header.Del("Authorization")
			return t.next.RoundTrip(out)
		}
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, fmt.Errorf("refusing non-Google API host %q on %s GCP transport", req.URL.Host, CredentialProxyURLEnv)
	}
	out := req.Clone(req.Context())
	basePath := strings.TrimRight(t.target.Path, "/")
	inPath := req.URL.Path
	if inPath == "" || inPath[0] != '/' {
		inPath = "/" + inPath
	}
	out.URL.Scheme = t.target.Scheme
	out.URL.Host = t.target.Host
	out.URL.Path = basePath + "/v1/gcp/" + host + inPath
	if req.URL.RawPath != "" {
		escBase := strings.TrimRight(t.target.EscapedPath(), "/")
		escIn := req.URL.EscapedPath()
		if escIn == "" || escIn[0] != '/' {
			escIn = "/" + escIn
		}
		out.URL.RawPath = escBase + "/v1/gcp/" + host + escIn
	} else {
		out.URL.RawPath = ""
	}
	out.Host = t.target.Host
	out.Header.Del("Authorization")
	return t.next.RoundTrip(out)
}
