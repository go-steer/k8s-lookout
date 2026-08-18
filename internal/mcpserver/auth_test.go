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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// goodToken is long enough to pass minTokenLen and is not a value
// anybody would be tempted to copy into a deployment.
const goodToken = "GX7pTest0nlyToken-not-a-secret"

func writeToken(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadAuthToken(t *testing.T) {
	tests := []struct {
		name     string
		contents string
		wantErr  string
	}{
		{"plain", goodToken, ""},
		// Every way an operator will produce this file adds a newline.
		{"trailing newline", goodToken + "\n", ""},
		{"surrounding whitespace", "  " + goodToken + "\t\n", ""},
		{"empty", "", "empty"},
		{"whitespace only", "  \n", "empty"},
		{"two lines", goodToken + "\nsecond\n", "single line"},
		{"too short", "short", "at least"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			auth, err := LoadAuthToken(writeToken(t, tc.contents))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("LoadAuthToken() = %v, want nil", err)
				}
				if !auth.Authorize("Bearer " + goodToken) {
					t.Error("the loaded token does not authorize itself")
				}
				return
			}
			if err == nil {
				t.Fatalf("LoadAuthToken(%q) = nil, want an error", tc.contents)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err, tc.wantErr)
			}
			if !strings.Contains(err.Error(), "--auth-token-file=") {
				t.Errorf("error %q does not name the flag", err)
			}
		})
	}
}

func TestLoadAuthTokenReportsAMissingFile(t *testing.T) {
	if _, err := LoadAuthToken(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("expected an error for a file that does not exist")
	}
}

func TestBearerAuthAuthorize(t *testing.T) {
	auth, err := LoadAuthToken(writeToken(t, goodToken))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		header string
		want   bool
	}{
		{"Bearer " + goodToken, true},
		{"bearer " + goodToken, true}, // RFC 7235: the scheme is case-insensitive
		{"BEARER " + goodToken, true},
		{"Bearer  " + goodToken + " ", true}, // stray whitespace around the value

		{"", false},
		{goodToken, false},                   // no scheme
		{"Bearer", false},                    // scheme only
		{"Bearer ", false},                   // empty token
		{"Basic " + goodToken, false},        // wrong scheme
		{"Bearer " + goodToken + "x", false}, // longer
		{"Bearer " + goodToken[:20], false},  // a correct prefix
		{"Bearer " + strings.ToUpper(goodToken), false},
	}
	for _, tc := range tests {
		if got := auth.Authorize(tc.header); got != tc.want {
			t.Errorf("Authorize(%q) = %v, want %v", tc.header, got, tc.want)
		}
	}

	// The no-token case: nil authorizes everything, which is only
	// reachable on loopback or stdio.
	var none *BearerAuth
	if !none.Authorize("") {
		t.Error("a nil BearerAuth must not reject anything")
	}
}

func TestBearerAuthWrap(t *testing.T) {
	auth, err := LoadAuthToken(writeToken(t, goodToken))
	if err != nil {
		t.Fatal(err)
	}
	reached := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()
	auth.Wrap(next).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated request = %d, want 401", rec.Code)
	}
	if reached {
		t.Error("an unauthenticated request reached the MCP handler")
	}
	if got := rec.Header().Get("WWW-Authenticate"); !strings.HasPrefix(got, "Bearer") {
		t.Errorf("WWW-Authenticate = %q, want a Bearer challenge", got)
	}
	// The rejection says nothing about what is behind it.
	if body := rec.Body.String(); strings.Contains(body, "lookout") || strings.Contains(body, "k8s_") {
		t.Errorf("the 401 body describes the server: %q", body)
	}

	req = httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer "+goodToken)
	rec = httptest.NewRecorder()
	auth.Wrap(next).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !reached {
		t.Errorf("authenticated request = %d, reached = %v, want 200/true", rec.Code, reached)
	}

	// A nil auth returns the handler untouched rather than a wrapper
	// that always passes: same object, one code path.
	var none *BearerAuth
	if got := none.Wrap(next); got == nil {
		t.Error("nil.Wrap returned nil")
	}
}

func TestBindPolicy(t *testing.T) {
	tests := []struct {
		name    string
		policy  BindPolicy
		wantErr string
	}{
		{"loopback needs nothing", BindPolicy{Listen: "127.0.0.1:8383"}, ""},
		{"localhost needs nothing", BindPolicy{Listen: "localhost:0"}, ""},
		{"v6 loopback needs nothing", BindPolicy{Listen: "[::1]:8383"}, ""},
		{"loopback tolerates the extras", BindPolicy{
			Listen: "127.0.0.1:8383", AllowNonLoopback: true, HasAuthToken: true, HasAccessLog: true,
		}, ""},

		{"routable is refused by default", BindPolicy{Listen: "0.0.0.0:8383"}, "--allow-non-loopback"},
		{"a token alone does not change the bind", BindPolicy{
			Listen: "0.0.0.0:8383", HasAuthToken: true, HasAccessLog: true,
		}, "--allow-non-loopback"},
		{"the bind flag alone does not open an unauthenticated port", BindPolicy{
			Listen: "0.0.0.0:8383", AllowNonLoopback: true, HasAccessLog: true,
		}, "--auth-token-file"},
		{"off-host requires the access log", BindPolicy{
			Listen: "0.0.0.0:8383", AllowNonLoopback: true, HasAuthToken: true,
		}, "--access-log"},
		{"all three permit it", BindPolicy{
			Listen: "10.0.0.1:8383", AllowNonLoopback: true, HasAuthToken: true, HasAccessLog: true,
		}, ""},

		// A malformed address is a typo, not an exposure decision, and
		// must not be reported as one.
		{"missing port", BindPolicy{
			Listen: "10.0.0.1", AllowNonLoopback: true, HasAuthToken: true, HasAccessLog: true,
		}, "host:port"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.policy.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want an error mentioning %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

// bearerTransport adds an Authorization header to every request, so a
// real MCP client session can be pointed at an authenticated server.
type bearerTransport struct {
	token string
	base  http.RoundTripper
}

func (t bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	clone := r.Clone(r.Context())
	if t.token != "" {
		clone.Header.Set("Authorization", "Bearer "+t.token)
	}
	return t.base.RoundTrip(clone)
}

// TestServe_AuthenticatedHTTPRoundTrip is the end-to-end shape #282
// exists for: a bound listener that refuses an anonymous client and
// serves an authenticated one. It binds loopback — the routable case
// differs only in the policy check, which TestBindPolicy covers, and
// binding a routable address in a unit test is not something to do.
func TestServe_AuthenticatedHTTPRoundTrip(t *testing.T) {
	auth, err := LoadAuthToken(writeToken(t, goodToken))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var banner strings.Builder
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, New(testRegistry(t), "test"), ServeOptions{
			Listen:        "127.0.0.1:0",
			Auth:          auth,
			Announce:      &banner,
			AccessLogPath: "/dev/null",
			Ready:         ready,
		})
	}()

	var addr string
	select {
	case addr = <-ready:
	case err := <-done:
		t.Fatalf("Serve exited before ready: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the listener")
	}

	connect := func(token string) error {
		client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, nil)
		cs, err := client.Connect(ctx, &mcp.StreamableClientTransport{
			Endpoint:   "http://" + addr,
			HTTPClient: &http.Client{Transport: bearerTransport{token: token, base: http.DefaultTransport}},
			MaxRetries: -1,
		}, nil)
		if err != nil {
			return err
		}
		defer cs.Close()
		_, err = cs.ListTools(ctx, nil)
		return err
	}

	if err := connect(""); err == nil {
		t.Error("an anonymous client reached the tool list")
	}
	if err := connect("wrong-token-but-long-enough"); err == nil {
		t.Error("a client with the wrong token reached the tool list")
	}
	if err := connect(goodToken); err != nil {
		t.Errorf("an authenticated client was refused: %v", err)
	}

	if got := banner.String(); !strings.Contains(got, addr) {
		t.Errorf("startup banner %q does not name the bind address", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve did not shut down cleanly: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after cancellation")
	}
}

// TestAnnounceIsLoudOffHost: the banner is the last chance to tell an
// operator what they just opened, so the routable case must not read
// like the loopback one.
func TestAnnounceIsLoudOffHost(t *testing.T) {
	var loopback, routable strings.Builder
	announce(ServeOptions{Listen: "127.0.0.1:8383", Announce: &loopback}, "127.0.0.1:8383")
	announce(ServeOptions{
		Listen:        "0.0.0.0:8383",
		Announce:      &routable,
		AccessLogPath: "/var/log/lookout/mcp-access.log",
	}, "0.0.0.0:8383")

	if !strings.Contains(loopback.String(), "loopback") {
		t.Errorf("loopback banner = %q", loopback.String())
	}
	for _, want := range []string{"OFF-HOST", "authentication", "/var/log/lookout/mcp-access.log"} {
		if !strings.Contains(routable.String(), want) {
			t.Errorf("off-host banner %q does not mention %q", routable.String(), want)
		}
	}
}
