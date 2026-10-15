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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

const googleClientID = "123-abc.apps.googleusercontent.com"

// googlePeople are the tokens the fake Google knows: token → email.
var googlePeople = map[string]string{
	"alice-token": "alice@cymbal.example", // admitted, may write
	"bob-token":   "bob@cymbal.example",   // admitted, may not write
	"eve-token":   "eve@evil.example",     // not admitted
}

func fakeGoogle(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := r.FormValue("access_token")
		if r.URL.Path == "/userinfo" {
			tok = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		}
		email, ok := googlePeople[tok]
		if !ok {
			http.Error(w, "{}", http.StatusBadRequest)
			return
		}
		hd := email[strings.IndexByte(email, '@')+1:]
		if r.URL.Path == "/userinfo" {
			_ = json.NewEncoder(w).Encode(map[string]string{"email": email, "hd": hd})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"aud": googleClientID, "azp": googleClientID, "sub": "sub-" + email, "email": email, "email_verified": "true",
			"exp":   strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10),
			"scope": "openid https://www.googleapis.com/auth/userinfo.email " + cloudPlatformScope,
		})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// tokenFingerprint is what the whoami command reports: never the token.
func tokenFingerprint(tok string) string {
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:4])
}

// googleRegistry serves k8s_whoami, which reports the caller token its
// invocation runs with (as a fingerprint), and k8s_write, a state-changing
// tool.
func googleRegistry() *checks.Registry {
	reg := checks.NewRegistry()
	report := func(ctx context.Context, inv emit.Invocation) (int, error) {
		got := "none"
		if ts, ok := kube.CallerTokenFrom(ctx); ok {
			if tok, err := ts.Token(); err == nil {
				got = tokenFingerprint(tok.AccessToken)
			}
		}
		return 1, inv.Out.Emit(emit.Finding{
			Kind: "fake.whoami", Severity: emit.SeverityInfo, KindOfObject: "Pod", Name: "whoami",
			Reason: "Caller", Message: "caller-token-fingerprint=" + got,
		})
	}
	reg.Register(checks.Command{
		Name: "whoami", MCPName: "k8s_whoami", Summary: "Reports the caller token; test scaffolding only.",
		Kinds: []checks.KindField{checks.Kind("fake.whoami", "synthetic; test scaffolding only", emit.SeverityInfo)},
		Run:   report,
	})
	reg.Register(checks.Command{
		Name: "write", MCPName: "k8s_write", Summary: "Changes state; test scaffolding only.", Writes: true,
		Kinds: []checks.KindField{checks.Kind("fake.whoami", "synthetic; test scaffolding only", emit.SeverityInfo)},
		Run:   report,
	})
	return reg
}

type googleServer struct {
	url string
	log *bytes.Buffer
	mu  *sync.Mutex
}

func startGoogleServer(t *testing.T) googleServer {
	t.Helper()
	base := fakeGoogle(t)
	g, err := NewGoogleAuth(GoogleOptions{
		ClientIDs: []string{googleClientID}, Allow: []string{"@cymbal.example"}, AllowWrite: []string{"alice@cymbal.example"},
		Resource: "http://127.0.0.1/mcp", TokenInfoURL: base + "/tokeninfo", UserInfoURL: base + "/userinfo",
	})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var logBuf bytes.Buffer
	al := NewAccessLog(lockedWriter{&mu, &logBuf})
	server := New(googleRegistry(), "test", WithGoogleAuth(g), WithAccessLog(al))
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, server, ServeOptions{Listen: "127.0.0.1:0", Google: g, Ready: ready})
	}()
	t.Cleanup(func() { cancel(); <-done })
	return googleServer{url: "http://" + <-ready, log: &logBuf, mu: &mu}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// post sends one JSON-RPC message with an optional bearer token.
func (s googleServer) post(t *testing.T, path, token, body string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, s.url+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Protocol-Version", "2025-06-18")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp, string(b)
}

func callBody(tool string) string {
	return `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"` + tool + `","arguments":{}}}`
}

// TestGoogleModeEndToEnd drives a real HTTP server in Google-token mode.
func TestGoogleModeEndToEnd(t *testing.T) {
	s := startGoogleServer(t)

	// The RFC 9728 metadata is public.
	resp, err := http.Get(s.url + "/.well-known/oauth-protected-resource/mcp")
	if err != nil {
		t.Fatal(err)
	}
	meta, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(meta), "accounts.google.com") {
		t.Fatalf("metadata: %d %s", resp.StatusCode, meta)
	}

	// No token, an unknown token, and an unadmitted caller are refused
	// before any tool runs.
	for _, tok := range []string{"", "nope", "eve-token"} {
		if resp, _ := s.post(t, "/mcp", tok, callBody("k8s_whoami")); resp.StatusCode != http.StatusUnauthorized ||
			!strings.Contains(resp.Header.Get("WWW-Authenticate"), "resource_metadata=") {
			t.Errorf("token %q: %d, challenge %q; want 401 naming the metadata", tok, resp.StatusCode, resp.Header.Get("WWW-Authenticate"))
		}
	}

	// Each call runs with its own caller's token, and the server is
	// stateless: no session ID is handed out.
	for tok := range map[string]bool{"alice-token": true, "bob-token": true} {
		resp, body := s.post(t, "/mcp", tok, callBody("k8s_whoami"))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d %s", tok, resp.StatusCode, body)
		}
		if want := "caller-token-fingerprint=" + tokenFingerprint(tok); !strings.Contains(body, want) {
			t.Errorf("%s: the tool did not run with this caller's token: %s", tok, body)
		}
		if strings.Contains(body, tok) {
			t.Errorf("%s: the token itself reached the response", tok)
		}
		if id := resp.Header.Get("Mcp-Session-Id"); id != "" {
			t.Errorf("%s: a stateless server handed out session %q", tok, id)
		}
	}

	// The write tool: refused to bob (admitted, not on the write list),
	// allowed to alice.
	_, body := s.post(t, "/mcp", "bob-token", callBody("k8s_write"))
	if !strings.Contains(body, `"isError":true`) || !strings.Contains(body, "google-allow-write") {
		t.Errorf("bob calling the write tool: %s", body)
	}
	_, body = s.post(t, "/mcp", "alice-token", callBody("k8s_write"))
	if strings.Contains(body, `"isError":true`) || !strings.Contains(body, tokenFingerprint("alice-token")) {
		t.Errorf("alice calling the write tool: %s", body)
	}

	// The access log names each caller, and never a token.
	s.mu.Lock()
	logText := s.log.String()
	s.mu.Unlock()
	for _, want := range []string{"caller=alice@cymbal.example", "caller=bob@cymbal.example", "tool=k8s_write"} {
		if !strings.Contains(logText, want) {
			t.Errorf("access log lacks %q:\n%s", want, logText)
		}
	}
	if strings.Contains(logText, "-token") {
		t.Errorf("access log carries a token:\n%s", logText)
	}
}

// TestGoogleModeConcurrentCallers: callers in flight at the same time each
// read with their own token. A regression that kept a token in shared
// state would cross them here (run under -race).
func TestGoogleModeConcurrentCallers(t *testing.T) {
	s := startGoogleServer(t)
	var wg sync.WaitGroup
	for _, tok := range []string{"alice-token", "bob-token"} {
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, body := s.post(t, "/mcp", tok, callBody("k8s_whoami"))
				if want := "caller-token-fingerprint=" + tokenFingerprint(tok); !strings.Contains(body, want) {
					t.Errorf("%s: got another caller's token, or none: %s", tok, body)
				}
			}()
		}
	}
	wg.Wait()
}

// TestGoogleModeServesOnlyTheResourcePath: MCP answers at the
// --resource-url path, the resource the metadata describes, and nowhere
// else.
func TestGoogleModeServesOnlyTheResourcePath(t *testing.T) {
	s := startGoogleServer(t)
	if resp, _ := s.post(t, "/elsewhere", "alice-token", callBody("k8s_whoami")); resp.StatusCode != http.StatusNotFound {
		t.Errorf("POST /elsewhere: %d, want 404", resp.StatusCode)
	}
}

// TestGoogleModePerCallerLimitEndToEnd: past the per-caller limit, one
// caller's extra call is turned away while another caller still gets in.
func TestGoogleModePerCallerLimitEndToEnd(t *testing.T) {
	base := fakeGoogle(t)
	g, err := NewGoogleAuth(GoogleOptions{
		ClientIDs: []string{googleClientID}, Allow: []string{"@cymbal.example"},
		Resource: "http://127.0.0.1/mcp", TokenInfoURL: base + "/tokeninfo", UserInfoURL: base + "/userinfo",
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 16)
	release := make(chan struct{})
	reg := checks.NewRegistry()
	reg.Register(checks.Command{
		Name: "block", MCPName: "k8s_block", Summary: "Blocks until released; test scaffolding only.",
		Kinds: []checks.KindField{checks.Kind("fake.block", "synthetic; test scaffolding only", emit.SeverityInfo)},
		Run: func(ctx context.Context, _ emit.Invocation) (int, error) {
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
			return 0, nil
		},
	})
	server := New(reg, "test", WithGoogleAuth(g))
	ctx, cancel := context.WithCancel(context.Background())
	ready, done := make(chan string, 1), make(chan error, 1)
	go func() { done <- Serve(ctx, server, ServeOptions{Listen: "127.0.0.1:0", Google: g, Ready: ready}) }()
	t.Cleanup(func() { cancel(); <-done })
	s := googleServer{url: "http://" + <-ready}

	results := make(chan string, defaultMaxConcurrentPerCaller+1)
	for range defaultMaxConcurrentPerCaller {
		go func() { _, body := s.post(t, "/mcp", "alice-token", callBody("k8s_block")); results <- body }()
	}
	for range defaultMaxConcurrentPerCaller {
		<-started // alice now holds every one of her slots
	}
	if _, body := s.post(t, "/mcp", "alice-token", callBody("k8s_block")); !strings.Contains(body, "calls in flight") {
		t.Errorf("alice past her limit: %s", body)
	}
	go func() { _, body := s.post(t, "/mcp", "bob-token", callBody("k8s_block")); results <- body }()
	select {
	case <-started: // bob got in while alice was at her limit
	case <-time.After(5 * time.Second):
		t.Error("bob was held back by alice's calls")
	}
	close(release)
	for range defaultMaxConcurrentPerCaller + 1 {
		if body := <-results; strings.Contains(body, "calls in flight") {
			t.Errorf("a call within the limit was refused: %s", body)
		}
	}
}

// TestServeRefusesGoogleInFrontOfUnboundServer: the HTTP layer of
// Google-token mode in front of a server whose tools do not read the
// caller's token would authenticate each caller and then read as lookout.
// Serve refuses it.
func TestServeRefusesGoogleInFrontOfUnboundServer(t *testing.T) {
	t.Parallel()
	g, err := NewGoogleAuth(GoogleOptions{ClientIDs: []string{googleClientID}, Allow: []string{"@x.example"}, Resource: "https://l.example/mcp"})
	if err != nil {
		t.Fatal(err)
	}
	unbound := New(googleRegistry(), "test") // no WithGoogleAuth
	err = Serve(t.Context(), unbound, ServeOptions{Listen: "127.0.0.1:0", Google: g})
	if err == nil || !strings.Contains(err.Error(), "WithGoogleAuth") {
		t.Errorf("Google HTTP layer in front of an unbound server: %v", err)
	}
}

// TestGoogleServerWithoutGoogleServeFailsClosed: the reverse mismatch, a
// server built WithGoogleAuth served without the Google HTTP layer, runs
// nothing as lookout: no request carries a caller, so every call is
// refused.
func TestGoogleServerWithoutGoogleServeFailsClosed(t *testing.T) {
	t.Parallel()
	g, err := NewGoogleAuth(GoogleOptions{ClientIDs: []string{googleClientID}, Allow: []string{"@x.example"}, Resource: "https://l.example/mcp"})
	if err != nil {
		t.Fatal(err)
	}
	cs := connect(t, New(googleRegistry(), "test", WithGoogleAuth(g)))
	res, err := callTool(t, cs, "k8s_whoami", nil)
	if err != nil {
		t.Fatalf("tools/call: %v", err)
	}
	if got := text(t, res); !res.IsError || !strings.Contains(got, "refusing to run it as lookout") {
		t.Errorf("a call with no caller ran, or was refused for the wrong reason: %s", got)
	}
}

func TestCallerLimit(t *testing.T) {
	t.Parallel()
	l := newCallerLimit(2)
	r1, ok1 := l.acquire("a")
	r2, ok2 := l.acquire("a")
	if !ok1 || !ok2 {
		t.Fatal("first two calls refused")
	}
	if _, ok := l.acquire("a"); ok {
		t.Error("a third concurrent call for the same caller was admitted")
	}
	if rb, ok := l.acquire("b"); !ok {
		t.Error("another caller was refused because of the first one's calls")
	} else {
		rb()
	}
	r1()
	if r3, ok := l.acquire("a"); !ok {
		t.Error("a slot freed by release was not reusable")
	} else {
		r3()
	}
	r2()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.inflight) != 0 {
		t.Errorf("released callers still tracked: %v", l.inflight)
	}
}

func TestNewGoogleAuthRefuses(t *testing.T) {
	t.Parallel()
	ok := GoogleOptions{ClientIDs: []string{googleClientID}, Allow: []string{"@x.example"}, Resource: "https://l.example/mcp"}
	if _, err := NewGoogleAuth(ok); err != nil {
		t.Fatalf("valid options refused: %v", err)
	}
	for name, mutate := range map[string]func(*GoogleOptions){
		"no client":          func(o *GoogleOptions) { o.ClientIDs = nil },
		"empty allow":        func(o *GoogleOptions) { o.Allow = nil },
		"bad allow":          func(o *GoogleOptions) { o.Allow = []string{"x.example"} },
		"bad write allow":    func(o *GoogleOptions) { o.AllowWrite = []string{"nope"} },
		"plaintext resource": func(o *GoogleOptions) { o.Resource = "http://l.example/mcp" },
	} {
		o := ok
		mutate(&o)
		if _, err := NewGoogleAuth(o); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestServeRefusesGoogleOnStdioAndWithBearer(t *testing.T) {
	t.Parallel()
	g, err := NewGoogleAuth(GoogleOptions{ClientIDs: []string{googleClientID}, Allow: []string{"@x.example"}, Resource: "https://l.example/mcp"})
	if err != nil {
		t.Fatal(err)
	}
	server := New(googleRegistry(), "test", WithGoogleAuth(g))
	if err := Serve(t.Context(), server, ServeOptions{Google: g}); err == nil || !strings.Contains(err.Error(), "--listen") {
		t.Errorf("stdio in Google mode: %v", err)
	}
	if err := Serve(t.Context(), server, ServeOptions{Listen: "127.0.0.1:0", Google: g, Auth: &BearerAuth{}}); err == nil ||
		!strings.Contains(err.Error(), "exclusive") {
		t.Errorf("Google mode with a bearer token: %v", err)
	}
}
