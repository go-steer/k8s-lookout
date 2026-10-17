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
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/go-steer/steward/authserver"
	"github.com/go-steer/steward/seal"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// Path B (#702) end to end: lookout's own authorization server, a fake
// Google sign-in, and MCP calls with the tokens it issues, on one listener
// beside Path A.

// pathBHost is the issuer's and resource's host. The test's HTTP client
// dials it at the real listener, so the issuer can be configured before
// the listener's port is known.
const (
	pathBHost     = "localhost"
	pathBIssuer   = "http://" + pathBHost
	pathBResource = pathBIssuer + "/mcp"
	// pathBClientRedirect is where a test client receives its code; never
	// dialed.
	pathBClientRedirect = "http://127.0.0.1:1/callback"
	signInClientID      = "lookout-signin.apps.googleusercontent.com"
	signInClientSecret  = "GOCSPX-planted-client-secret"
	// keyringSecret is the sealing key, planted so the test can assert it
	// never appears anywhere.
	keyringSecret = "c2VjcmV0LWtleS1tYXRlcmlhbC0zMi1ieXRlcy0hISE="
	robotSecret   = "robot-planted-client-secret"
)

// fakeSignIn is Google's authorize and token endpoints, signing in whoever
// `who` names, and recording every Google access token it mints.
type fakeSignIn struct {
	srv *httptest.Server

	mu     sync.Mutex
	who    string
	codes  map[string]string
	minted map[string]string // email -> last token
}

func newFakeSignIn(t *testing.T) *fakeSignIn {
	t.Helper()
	f := &fakeSignIn{codes: map[string]string{}, minted: map[string]string{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /auth", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		code := "gcode-" + rand.Text()
		f.mu.Lock()
		f.codes[code] = q.Get("code_challenge")
		f.mu.Unlock()
		http.Redirect(w, r, q.Get("redirect_uri")+"?"+url.Values{"code": {code}, "state": {q.Get("state")}}.Encode(), http.StatusFound) //nolint:gosec // G710: a test fake standing in for Google
	})
	mux.HandleFunc("POST /token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		f.mu.Lock()
		defer f.mu.Unlock()
		sum := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
		if ch, ok := f.codes[r.PostForm.Get("code")]; !ok || ch != base64.RawURLEncoding.EncodeToString(sum[:]) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
			return
		}
		delete(f.codes, r.PostForm.Get("code"))
		email := f.who
		claims := map[string]any{"iss": "https://accounts.google.com", "aud": signInClientID, "sub": "sub-" + email,
			"email": email, "email_verified": true, "exp": 4102444800, "hd": email[strings.IndexByte(email, '@')+1:]}
		b, _ := json.Marshal(claims)
		tok := "ya29.signin-" + rand.Text()
		f.minted[email] = tok
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": tok, "expires_in": 3599, "refresh_token": "1//r-" + rand.Text(),
			"scope":    authserver.ScopeOpenID + " " + authserver.ScopeEmail + " " + authserver.ScopeCloudPlatform,
			"id_token": "e30." + base64.RawURLEncoding.EncodeToString(b) + ".sig",
		})
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeSignIn) as(email string) { f.mu.Lock(); f.who = email; f.mu.Unlock() }

func (f *fakeSignIn) token(email string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.minted[email]
}

func (f *fakeSignIn) google() authserver.GoogleClient {
	return authserver.GoogleClient{ClientID: signInClientID, ClientSecret: signInClientSecret,
		AuthURL: f.srv.URL + "/auth", TokenURL: f.srv.URL + "/token"}
}

type pathBServer struct {
	googleServer
	signIn *fakeSignIn
	// client dials pathBHost at the listener.
	client *http.Client
	jarred func() *http.Client
}

type pathBConfig struct {
	clients     []authserver.StaticClient
	rules       string // --workload-rules YAML; "" for none
	impersonate cloud.Impersonator
	clusterRead bool
}

func startPathBServer(t *testing.T, pc pathBConfig) pathBServer {
	t.Helper()
	base := fakeGoogle(t)
	signIn := newFakeSignIn(t)
	var mu sync.Mutex
	var logBuf, banner bytes.Buffer
	// steward's diagnostics (sign-ins, refusals) go to the banner's
	// stream, stderr in production, and are checked for secrets with it.
	diag := slog.New(slog.NewTextHandler(lockedWriter{&mu, &banner}, nil))
	keys, err := seal.ParseKeyring([]byte("k1 " + keyringSecret + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	g, err := NewGoogleAuth(GoogleOptions{
		ClientIDs: []string{googleClientID}, Allow: []string{"@cymbal.example"}, AllowWrite: []string{"alice@cymbal.example"},
		Resource: pathBResource, TokenInfoURL: base + "/tokeninfo", UserInfoURL: base + "/userinfo", Logger: diag,
		AuthServer: &AuthServerOptions{
			Issuer: pathBIssuer, Google: signIn.google(), Custody: authserver.SealedCustody{}, Keys: keys,
			Clients: pc.clients, OpenRegistration: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var workloads *WorkloadAuth
	if pc.rules != "" {
		path := filepath.Join(t.TempDir(), "rules.yaml")
		if err := os.WriteFile(path, []byte(pc.rules), 0o600); err != nil {
			t.Fatal(err)
		}
		workloads, err = NewWorkloadAuth(WorkloadOptions{RulesFile: path, Registry: googleRegistry(), ClientCredentialsOnly: true, Impersonate: pc.impersonate, Logger: diag})
		if err != nil {
			t.Fatal(err)
		}
	}
	kc, reviews := fakeAPIServer(t, pc.clusterRead)
	al := NewAccessLog(lockedWriter{&mu, &logBuf})
	server := New(googleRegistry(), "test", WithGoogleAuth(g), WithWorkloadAuth(workloads), WithAccessLog(al), WithSelection(kc, ""))
	ctx, cancel := context.WithCancel(context.Background())
	ready, done := make(chan string, 1), make(chan error, 1)
	go func() {
		done <- Serve(ctx, server, ServeOptions{Listen: "127.0.0.1:0", Google: g, Workloads: workloads, Ready: ready,
			Announce: lockedWriter{&mu, &banner}})
	}()
	t.Cleanup(func() { cancel(); <-done })
	addr := <-ready
	transport := func() *http.Transport {
		return &http.Transport{DialContext: func(ctx context.Context, network, a string) (net.Conn, error) {
			if a == pathBHost+":80" {
				a = addr
			}
			return (&net.Dialer{}).DialContext(ctx, network, a)
		}}
	}
	return pathBServer{
		googleServer: googleServer{url: "http://" + addr, log: &logBuf, mu: &mu, reviews: reviews, banner: &banner},
		signIn:       signIn,
		client:       &http.Client{Transport: transport()},
		jarred: func() *http.Client {
			jar, _ := cookiejar.New(nil)
			return &http.Client{Jar: jar, Transport: transport(), CheckRedirect: func(req *http.Request, _ []*http.Request) error {
				if req.URL.Host == "127.0.0.1:1" {
					return http.ErrUseLastResponse
				}
				return nil
			}}
		},
	}
}

var consentRequestRE = regexp.MustCompile(`name="request" value="([^"]+)"`)

// signIn registers a client by DCR, signs email in through the fake Google
// asking for scope ("" asks for none), approves the consent page, and
// returns lookout's access token, or the error the flow ended with.
func (s pathBServer) signInAs(t *testing.T, email, scope string) (string, error) {
	t.Helper()
	s.signIn.as(email)
	resp, err := s.client.Post(pathBIssuer+"/register", "application/json", strings.NewReader(`{"redirect_uris":["`+pathBClientRedirect+`"]}`))
	if err != nil {
		t.Fatal(err)
	}
	var reg struct {
		ClientID string `json:"client_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&reg)
	_ = resp.Body.Close()
	if reg.ClientID == "" {
		t.Fatalf("DCR: %d", resp.StatusCode)
	}
	verifier := rand.Text() + rand.Text()
	sum := sha256.Sum256([]byte(verifier))
	q := url.Values{
		"response_type": {"code"}, "client_id": {reg.ClientID}, "redirect_uri": {pathBClientRedirect},
		"code_challenge": {base64.RawURLEncoding.EncodeToString(sum[:])}, "code_challenge_method": {"S256"},
		"resource": {pathBResource}, "state": {"s"},
	}
	if scope != "" {
		q.Set("scope", scope)
	}
	browser := s.jarred()
	resp, err = browser.Get(pathBIssuer + "/authorize?" + q.Encode())
	code := ""
	for err == nil {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusFound {
			loc, perr := url.Parse(resp.Header.Get("Location"))
			if perr != nil {
				t.Fatal(perr)
			}
			if e := loc.Query().Get("error"); e != "" {
				return "", fmt.Errorf("authorization error %s", e)
			}
			code = loc.Query().Get("code")
			break
		}
		m := consentRequestRE.FindSubmatch(body)
		if m == nil {
			return "", fmt.Errorf("sign-in stopped on a page (%d)", resp.StatusCode)
		}
		resp, err = browser.PostForm(resp.Request.URL.String(), url.Values{"request": {html.UnescapeString(string(m[1]))}, "action": {"approve"}})
	}
	if err != nil {
		t.Fatal(err)
	}
	return s.token(t, url.Values{
		"grant_type": {"authorization_code"}, "client_id": {reg.ClientID}, "code": {code},
		"code_verifier": {verifier}, "redirect_uri": {pathBClientRedirect},
	}, "", "")
}

// token posts form to the token endpoint, with Basic credentials if id is
// set.
func (s pathBServer) token(t *testing.T, form url.Values, id, secret string) (string, error) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, pathBIssuer+"/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if id != "" {
		req.SetBasicAuth(id, secret)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var tr struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&tr)
	if tr.AccessToken == "" {
		return "", fmt.Errorf("token endpoint: %d %s", resp.StatusCode, tr.Error)
	}
	return tr.AccessToken, nil
}

func (s pathBServer) mustSignIn(t *testing.T, email, scope string) string {
	t.Helper()
	tok, err := s.signInAs(t, email, scope)
	if err != nil {
		t.Fatalf("%s signing in: %v", email, err)
	}
	return tok
}

func (s pathBServer) clientCredentials(t *testing.T, id, secret, scope string) string {
	t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}}
	if scope != "" {
		form.Set("scope", scope)
	}
	tok, err := s.token(t, form, id, secret)
	if err != nil {
		t.Fatalf("client credentials for %s: %v", id, err)
	}
	return tok
}

func (s pathBServer) logText() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.String() + s.banner.String()
}

func get(t *testing.T, u string) (int, string) {
	t.Helper()
	resp, err := http.Get(u) //nolint:gosec // G107: a test server's URL
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode, string(b)
}

// assertNoSecrets: no token, client secret or key material reaches a
// response body, the access log or the startup banner.
func assertNoSecrets(t *testing.T, where, text string, secrets ...string) {
	t.Helper()
	for _, sec := range append(secrets, signInClientSecret, keyringSecret, robotSecret) {
		if sec != "" && strings.Contains(text, sec) {
			t.Errorf("%s carries a secret (%.12s...):\n%s", where, sec, text)
		}
	}
	if strings.Contains(text, seal.Prefix) {
		t.Errorf("%s carries a sealed lookout token:\n%s", where, text)
	}
}

func TestPathBPersonEndToEnd(t *testing.T) {
	s := startPathBServer(t, pathBConfig{clusterRead: true})

	// The protected-resource metadata names lookout's own authorization
	// server and its scopes; the authorization server publishes RFC 8414
	// metadata with a registration endpoint.
	code, meta := get(t, s.url+"/.well-known/oauth-protected-resource/mcp")
	if code != http.StatusOK || !strings.Contains(meta, pathBIssuer) || !strings.Contains(meta, ScopeRead) || !strings.Contains(meta, ScopeWrite) {
		t.Fatalf("protected-resource metadata: %d %s", code, meta)
	}
	code, asMeta := get(t, s.url+"/.well-known/oauth-authorization-server")
	if code != http.StatusOK || !strings.Contains(asMeta, `"registration_endpoint"`) || !strings.Contains(asMeta, pathBIssuer+"/token") {
		t.Fatalf("authorization server metadata: %d %s", code, asMeta)
	}
	// A 401 asks for read only.
	if resp, _ := s.post(t, "/mcp", "", callBody("k8s_whoami")); resp.StatusCode != http.StatusUnauthorized ||
		!strings.Contains(resp.Header.Get("WWW-Authenticate"), ScopeRead) || strings.Contains(resp.Header.Get("WWW-Authenticate"), ScopeWrite) {
		t.Errorf("no token: %d %q", resp.StatusCode, resp.Header.Get("WWW-Authenticate"))
	}

	// An admitted person signs in and reads with their own Google token.
	alice := s.mustSignIn(t, "alice@cymbal.example", "")
	resp, body := s.post(t, "/mcp", alice, callBody("k8s_whoami"))
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "caller-token-fingerprint="+tokenFingerprint(s.signIn.token("alice@cymbal.example"))) {
		t.Fatalf("alice via Path B: %d %s", resp.StatusCode, body)
	}
	assertNoSecrets(t, "a tool result", body, alice, s.signIn.token("alice@cymbal.example"))

	// Her rows from the store are filtered by her own access, as on Path A:
	// the access review ran with her Google token.
	_, body = s.post(t, "/mcp", alice, callBody("k8s_storerows"))
	if !strings.Contains(body, "hidden=0") {
		t.Errorf("a person's store rows were not filtered: %s", body)
	}
	for _, tok := range s.reviews() {
		if tok != s.signIn.token("alice@cymbal.example") {
			t.Errorf("an access review carried %q, want alice's Google token", tok)
		}
	}

	// The write tool: alice is on --google-allow-write, but her sign-in
	// asked for no lookout:write.
	_, body = s.post(t, "/mcp", alice, callBody("k8s_write"))
	if !strings.Contains(body, `"isError":true`) || !strings.Contains(body, ScopeWrite) {
		t.Errorf("alice writing without lookout:write: %s", body)
	}
	aliceW := s.mustSignIn(t, "alice@cymbal.example", ScopeRead+" "+ScopeWrite)
	_, body = s.post(t, "/mcp", aliceW, callBody("k8s_write"))
	if strings.Contains(body, `"isError":true`) || !strings.Contains(body, tokenFingerprint(s.signIn.token("alice@cymbal.example"))) {
		t.Errorf("alice writing with lookout:write: %s", body)
	}

	// bob holds lookout:write but is not on --google-allow-write.
	bobW := s.mustSignIn(t, "bob@cymbal.example", ScopeRead+" "+ScopeWrite)
	_, body = s.post(t, "/mcp", bobW, callBody("k8s_write"))
	if !strings.Contains(body, `"isError":true`) || !strings.Contains(body, "google-allow-write") {
		t.Errorf("bob writing: %s", body)
	}

	// A person --google-allow does not admit cannot sign in.
	if tok, err := s.signInAs(t, "eve@evil.example", ""); err == nil || !strings.Contains(err.Error(), "access_denied") {
		t.Errorf("eve signed in (%q, %v)", tok, err)
	}

	// A token without lookout:read is refused at the HTTP layer with a
	// step-up challenge.
	writeOnly := s.mustSignIn(t, "alice@cymbal.example", ScopeWrite)
	if resp, _ := s.post(t, "/mcp", writeOnly, callBody("k8s_whoami")); resp.StatusCode != http.StatusForbidden ||
		!strings.Contains(resp.Header.Get("WWW-Authenticate"), "insufficient_scope") {
		t.Errorf("a token without lookout:read: %d %q", resp.StatusCode, resp.Header.Get("WWW-Authenticate"))
	}

	// Path A still works on the same endpoint, unchanged: a Gemini
	// Enterprise token reads as its person and writes on the allow-list
	// alone.
	_, body = s.post(t, "/mcp", "alice-token", callBody("k8s_write"))
	if strings.Contains(body, `"isError":true`) || !strings.Contains(body, tokenFingerprint("alice-token")) {
		t.Errorf("Path A beside Path B: %s", body)
	}

	logText := s.logText()
	for _, want := range []string{"caller=alice@cymbal.example", "caller=bob@cymbal.example", "tool=k8s_write", pathBIssuer + "/callback", "signed in", "refused sign-in"} {
		if !strings.Contains(logText, want) {
			t.Errorf("access log or banner lacks %q:\n%s", want, logText)
		}
	}
	assertNoSecrets(t, "the access log and banner", logText, alice, aliceW, bobW, writeOnly,
		s.signIn.token("alice@cymbal.example"), s.signIn.token("bob@cymbal.example"), "alice-token")
}

func TestPathBWorkloads(t *testing.T) {
	var mu sync.Mutex
	var impersonated []string
	failSA := "broken@p.iam.gserviceaccount.com"
	imp := func(_ context.Context, sa string) (oauth2.TokenSource, error) {
		mu.Lock()
		impersonated = append(impersonated, sa)
		mu.Unlock()
		if sa == failSA {
			return nil, errors.New("iam: permission denied minting for " + sa)
		}
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "sa-token-" + sa, Expiry: time.Now().Add(time.Hour)}), nil
	}
	rules := `rules:
- name: robot
  identities: [client:robot]
  tools: [k8s_whoami, k8s_write, k8s_storerows]
  write: true
  service_account: reader@p.iam.gserviceaccount.com
- name: plain
  identities: [client:plain]
  tools: [k8s_whoami]
- name: broken
  identities: [client:broken]
  tools: [k8s_whoami]
  service_account: ` + failSA + `
`
	cc := func(id string) authserver.StaticClient {
		return authserver.StaticClient{ID: id, Secret: robotSecret, ClientCredentials: true}
	}
	s := startPathBServer(t, pathBConfig{
		clients: []authserver.StaticClient{cc("robot"), cc("plain"), cc("broken"), cc("stranger")},
		rules:   rules, impersonate: imp, clusterRead: false,
	})

	// The rule's service account: the call reads as it.
	robot := s.clientCredentials(t, "robot", robotSecret, "")
	_, body := s.post(t, "/mcp", robot, callBody("k8s_whoami"))
	if !strings.Contains(body, "caller-token-fingerprint="+tokenFingerprint("sa-token-reader@p.iam.gserviceaccount.com")) {
		t.Errorf("robot did not read as its rule's service account: %s", body)
	}
	// Its store rows are not filtered, even though the call carries a
	// token (the service account's), and no access review is spent.
	_, body = s.post(t, "/mcp", robot, callBody("k8s_storerows"))
	if strings.Contains(body, "hidden=") || !strings.Contains(body, "caller-token-fingerprint=") {
		t.Errorf("a workload's store rows were filtered: %s", body)
	}
	if n := len(s.reviews()); n != 0 {
		t.Errorf("%d access reviews for a workload, want none", n)
	}
	// write: true is not enough without the lookout:write scope.
	_, body = s.post(t, "/mcp", robot, callBody("k8s_write"))
	if !strings.Contains(body, `"isError":true`) || !strings.Contains(body, ScopeWrite) {
		t.Errorf("robot writing without lookout:write: %s", body)
	}
	robotW := s.clientCredentials(t, "robot", robotSecret, ScopeRead+" "+ScopeWrite)
	_, body = s.post(t, "/mcp", robotW, callBody("k8s_write"))
	if strings.Contains(body, `"isError":true`) {
		t.Errorf("robot writing with lookout:write and write: true: %s", body)
	}

	// A rule without a service account reads as lookout: no caller token.
	plain := s.clientCredentials(t, "plain", robotSecret, ScopeRead+" "+ScopeWrite)
	_, body = s.post(t, "/mcp", plain, callBody("k8s_whoami"))
	if !strings.Contains(body, "caller-token-fingerprint=none") {
		t.Errorf("plain did not read as lookout: %s", body)
	}
	// ...and a tool its rule does not name, or a write without write: true,
	// is refused even with the scope.
	_, body = s.post(t, "/mcp", plain, callBody("k8s_write"))
	if !strings.Contains(body, `"isError":true`) || !strings.Contains(body, "workload-rules") {
		t.Errorf("plain writing: %s", body)
	}

	// No rule: nothing is granted.
	stranger := s.clientCredentials(t, "stranger", robotSecret, "")
	_, body = s.post(t, "/mcp", stranger, callBody("k8s_whoami"))
	if !strings.Contains(body, `"isError":true`) || !strings.Contains(body, "client:stranger is not granted") {
		t.Errorf("a client with no rule: %s", body)
	}

	// An impersonation that fails fails the call; it never reads as lookout.
	broken := s.clientCredentials(t, "broken", robotSecret, "")
	_, body = s.post(t, "/mcp", broken, callBody("k8s_whoami"))
	if !strings.Contains(body, `"isError":true`) || !strings.Contains(body, failSA) || strings.Contains(body, "caller-token-fingerprint") {
		t.Errorf("a failed impersonation: %s", body)
	}

	logText := s.logText()
	for _, want := range []string{"caller=client:robot", "caller=client:plain", "caller=client:stranger"} {
		if !strings.Contains(logText, want) {
			t.Errorf("access log lacks %q:\n%s", want, logText)
		}
	}
	assertNoSecrets(t, "the access log and banner", logText, robot, robotW, plain, stranger, broken, "sa-token-")
	mu.Lock()
	defer mu.Unlock()
	if len(impersonated) == 0 {
		t.Error("no service account was impersonated")
	}
}

func TestNewGoogleAuthPathBRefuses(t *testing.T) {
	t.Parallel()
	keys, err := seal.ParseKeyring([]byte("k1 " + keyringSecret + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	ok := func() GoogleOptions {
		return GoogleOptions{
			ClientIDs: []string{googleClientID}, Allow: []string{"@x.example"}, Resource: "https://l.example/mcp",
			AuthServer: &AuthServerOptions{
				Issuer: "https://l.example", Google: authserver.GoogleClient{ClientID: signInClientID, ClientSecret: signInClientSecret},
				Custody: authserver.SealedCustody{}, Keys: keys, OpenRegistration: true,
			},
		}
	}
	if _, err := NewGoogleAuth(ok()); err != nil {
		t.Fatalf("valid options refused: %v", err)
	}
	for name, tc := range map[string]struct {
		mutate func(*GoogleOptions)
		want   string
	}{
		"no way to register": {func(o *GoogleOptions) { o.AuthServer.OpenRegistration = false }, "no client could sign in"},
		"client names a service account": {func(o *GoogleOptions) {
			o.AuthServer.Clients = []authserver.StaticClient{{ID: "bot", Secret: "s", ClientCredentials: true, ServiceAccount: "sa@p.iam.gserviceaccount.com"}}
		}, "client:bot"},
		"plaintext issuer":  {func(o *GoogleOptions) { o.AuthServer.Issuer = "http://l.example" }, "--oauth-issuer"},
		"group allow entry": {func(o *GoogleOptions) { o.Allow = []string{"group:eng@x.example"} }, "group"},
		"group write entry": {func(o *GoogleOptions) { o.AllowWrite = []string{"group:eng@x.example"} }, "group"},
		"no Google client":  {func(o *GoogleOptions) { o.AuthServer.Google = authserver.GoogleClient{} }, "--oauth-issuer"},
		"no keys":           {func(o *GoogleOptions) { o.AuthServer.Keys = nil }, "--oauth-issuer"},
		"no custody":        {func(o *GoogleOptions) { o.AuthServer.Custody = nil }, "--oauth-issuer"},
	} {
		o := ok()
		tc.mutate(&o)
		_, err := NewGoogleAuth(o)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want an error naming %q", name, err, tc.want)
		}
		if err != nil {
			assertNoSecrets(t, name+": the error", err.Error())
		}
	}
}

// TestServeRefusesWorkloadsWithNoWayIn: rules served with neither the mTLS
// listener nor the authorization server would govern no one; it is a
// wiring mistake.
func TestServeRefusesWorkloadsWithNoWayIn(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "rules.yaml")
	if err := os.WriteFile(path, []byte("rules:\n- name: r\n  identities: [client:x]\n  tools: [k8s_whoami]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := NewWorkloadAuth(WorkloadOptions{RulesFile: path, Registry: googleRegistry(), ClientCredentialsOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	server := New(googleRegistry(), "test", WithWorkloadAuth(w))
	if err := Serve(t.Context(), server, ServeOptions{Listen: "127.0.0.1:0", Workloads: w}); err == nil || !strings.Contains(err.Error(), "--oauth-issuer") {
		t.Errorf("rules with no way in: %v", err)
	}
	// Nor can rules built without certificates serve the mTLS listener.
	if err := Serve(t.Context(), server, ServeOptions{MTLSListen: "127.0.0.1:0", Workloads: w}); err == nil || !strings.Contains(err.Error(), "--mtls-listen") {
		t.Errorf("certificate-less rules on the mTLS listener: %v", err)
	}
}
