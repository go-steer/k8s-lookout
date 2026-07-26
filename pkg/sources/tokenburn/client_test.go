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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// serveFixtures stands in for the core-agent v2.7.0 attach listener:
// the testdata files are RECORDED response shapes of GET /sessions
// (pkg/attach handlers.go sessionDescriptor) and GET
// /sessions/{app}/{sid}/usage (attach.UsageInfo, the #222
// UsageMetadata schema from the attach-http reference), so the
// adapter is tested against the real wire contract (§13 recorded
// API fixtures behind a small client interface).
func serveFixtures(t *testing.T, wantToken string) *httptest.Server {
	t.Helper()
	fixture := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatalf("fixture %s: %v", name, err)
		}
		return b
	}
	mux := http.NewServeMux()
	auth := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if wantToken != "" && r.Header.Get("Authorization") != "Bearer "+wantToken {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			h(w, r)
		}
	}
	mux.HandleFunc("GET /sessions", auth(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(fixture("sessions.json"))
	}))
	mux.HandleFunc("GET /sessions/core-agent/incident-7f3a/usage", auth(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(fixture("usage.json"))
	}))
	mux.HandleFunc("GET /sessions/shortcut-only/usage", auth(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(fixture("usage.json"))
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestHTTPClient_SessionsFixture(t *testing.T) {
	t.Parallel()
	srv := serveFixtures(t, "tok-123")
	c := NewHTTPClient(srv.URL, "tok-123")
	refs, err := c.Sessions(context.Background())
	if err != nil {
		t.Fatalf("Sessions: %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("Sessions returned %d refs, want 3", len(refs))
	}
	want := []SessionRef{
		{App: "core-agent", ID: "incident-7f3a", Status: "active"},
		{App: "core-agent", ID: "watchboard-1", Status: "active"},
		{App: "core-agent", ID: "incident-old-91c2", Status: "idle"},
	}
	for i, w := range want {
		if refs[i] != w {
			t.Errorf("refs[%d] = %+v, want %+v", i, refs[i], w)
		}
	}
}

func TestHTTPClient_UsageFixture(t *testing.T) {
	t.Parallel()
	srv := serveFixtures(t, "tok-123")
	c := NewHTTPClient(srv.URL, "tok-123")
	u, err := c.Usage(context.Background(), SessionRef{App: "core-agent", ID: "incident-7f3a", Status: "active"})
	if err != nil {
		t.Fatalf("Usage: %v", err)
	}
	// Billed tokens = overall input (80846) + output (612) +
	// thoughts (420); the per_model / per_turn / digest_methods
	// blocks are tolerated and ignored.
	if u.TotalTokens != 81878 {
		t.Errorf("TotalTokens = %d, want 81878", u.TotalTokens)
	}
	if u.CostUSD != 0.085 {
		t.Errorf("CostUSD = %v, want 0.085", u.CostUSD)
	}
	if u.Turns != 2 {
		t.Errorf("Turns = %d, want 2", u.Turns)
	}
}

func TestHTTPClient_ShortcutPathWhenAppEmpty(t *testing.T) {
	t.Parallel()
	srv := serveFixtures(t, "")
	c := NewHTTPClient(srv.URL, "")
	if _, err := c.Usage(context.Background(), SessionRef{ID: "shortcut-only"}); err != nil {
		t.Fatalf("Usage via single-segment shortcut: %v", err)
	}
}

func TestHTTPClient_AuthAndErrorSurfaces(t *testing.T) {
	t.Parallel()
	srv := serveFixtures(t, "tok-123")
	// Wrong token → the 401 must surface as an error, not empty data.
	c := NewHTTPClient(srv.URL, "wrong")
	if _, err := c.Sessions(context.Background()); err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("Sessions with a bad token = %v, want a 401 error", err)
	}
	// Unknown route (pre-v2.7 daemon shape) → loud 404 error.
	ok := NewHTTPClient(srv.URL, "tok-123")
	_, err := ok.Usage(context.Background(), SessionRef{App: "core-agent", ID: "no-such"})
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Errorf("Usage on a missing endpoint = %v, want a 404 error", err)
	}
}
