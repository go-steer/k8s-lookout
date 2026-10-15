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

package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/steward/googletoken"
)

const (
	clientID = "123-abc.apps.googleusercontent.com"
	secret   = "ya29.planted-spike-token"
)

// fakeGoogle is tokeninfo, userinfo, the Container API, a GKE API server
// and the three Cloud APIs in one server. The API server answers the
// SelfSubjectReview with the caller's identity; compute answers 403 (token
// accepted, no permission); monitoring 401; logging 200.
func fakeGoogle(t *testing.T) *httptest.Server {
	t.Helper()
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		switch {
		case r.URL.Path == "/tokeninfo":
			if r.FormValue("access_token") != secret {
				http.Error(w, "{}", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{
				"aud": clientID, "azp": clientID, "sub": "1001", "email": "dana@cymbal.example", "email_verified": "true",
				"exp": strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10), "scope": "openid " + googletoken.EmailScope + " " + cloudScope,
			})
		case r.URL.Path == "/userinfo":
			_ = json.NewEncoder(w).Encode(map[string]string{"email": "dana@cymbal.example", "hd": "cymbal.example"})
		case bearer != secret:
			http.Error(w, "wrong token", http.StatusUnauthorized)
		case strings.HasPrefix(r.URL.Path, "/v1/projects/p/locations/l/clusters/c"):
			host := strings.TrimPrefix(srv.URL, "http://")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"controlPlaneEndpointsConfig": map[string]any{"dnsEndpointConfig": map[string]any{"endpoint": host}},
			})
		case r.URL.Path == "/apis/authentication.k8s.io/v1/selfsubjectreviews":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"userInfo": map[string]any{
				"username": "dana@cymbal.example", "groups": []string{"system:authenticated"}}}})
		case r.URL.Path == "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"allowed": false, "reason": "no RBAC binding"}})
		case strings.HasPrefix(r.URL.Path, "/compute/"):
			http.Error(w, `{"error":{"message":"permission denied"}}`, http.StatusForbidden)
		case strings.HasPrefix(r.URL.Path, "/v3/"):
			http.Error(w, `{"error":{"message":"invalid credentials"}}`, http.StatusUnauthorized)
		case r.URL.Path == "/v2/entries:list":
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func spikeFor(srv *httptest.Server) *Spike {
	return &Spike{
		Target: Target{Project: "p", Location: "l", Cluster: "c"},
		HTTP:   &http.Client{Timeout: 5 * time.Second}, Scheme: "http",
		ContainerBase: srv.URL, ComputeBase: srv.URL, MonitoringBase: srv.URL, LoggingBase: srv.URL,
	}
}

func verdicts(rs []Result) map[string]Result {
	m := map[string]Result{}
	for _, r := range rs {
		m[r.Check] = r
	}
	return m
}

func TestRunClassifies(t *testing.T) {
	t.Parallel()
	srv := fakeGoogle(t)
	id := googletoken.Identity{Subject: "1001", Email: "dana@cymbal.example", HD: "cymbal.example",
		Scopes: []string{"openid", cloudScope}, Expiry: time.Now().Add(time.Hour)}
	got := verdicts(spikeFor(srv).Run(context.Background(), id, secret))

	for check, want := range map[string]Verdict{
		"token.scopes":                 Accepted,
		"cloud.container.clusters.get": Accepted,
		"gke.dns-endpoint.authn":       Accepted,
		"gke.dns-endpoint.rbac":        Accepted,
		"gke.ip-endpoint.authn":        Skipped,  // the fake has no IP endpoint
		"cloud.compute":                Accepted, // 403: accepted, no permission
		"cloud.monitoring":             Rejected, // 401
		"cloud.logging":                Accepted,
		"identity":                     Info,
		"token.fingerprint":            Info,
		"token.lifetime":               Info,
	} {
		if got[check].Verdict != want {
			t.Errorf("%s: %s (%s), want %s", check, got[check].Verdict, got[check].Detail, want)
		}
	}
	if d := got["gke.dns-endpoint.authn"].Detail; !strings.Contains(d, "username=dana@cymbal.example") {
		t.Errorf("SelfSubjectReview detail %q does not name the person", d)
	}
	if d := got["gke.dns-endpoint.rbac"].Detail; !strings.Contains(d, "allowed=false") {
		t.Errorf("access review detail %q", d)
	}
}

func TestRunScopeMissingAndUnreachable(t *testing.T) {
	t.Parallel()
	s := &Spike{Target: Target{Project: "p", Location: "l", Cluster: "c"}, HTTP: &http.Client{Timeout: time.Second},
		ContainerBase: "http://127.0.0.1:1", ComputeBase: "http://127.0.0.1:1", MonitoringBase: "http://127.0.0.1:1", LoggingBase: "http://127.0.0.1:1"}
	got := verdicts(s.Run(context.Background(), googletoken.Identity{Scopes: []string{"openid"}}, secret))
	if got["token.scopes"].Verdict != Rejected {
		t.Errorf("no cloud-platform: %+v", got["token.scopes"])
	}
	for _, c := range []string{"cloud.container.clusters.get", "cloud.compute", "cloud.monitoring", "cloud.logging"} {
		if got[c].Verdict != Unreachable {
			t.Errorf("%s with nothing listening: %+v, want UNREACHABLE", c, got[c])
		}
	}
	if got["gke.dns-endpoint.authn"].Verdict != Skipped {
		t.Errorf("GKE checks without a cluster description: %+v", got["gke.dns-endpoint.authn"])
	}
}

// TestEndToEnd drives the real handler the way Gemini Enterprise would:
// initialize, then tools/call spike_report, with the planted token. The
// report must carry the verdicts and must never carry the token.
func TestEndToEnd(t *testing.T) {
	t.Parallel()
	srv := fakeGoogle(t)
	cfg := config{ClientID: clientID, Members: []string{"@cymbal.example"}, Resource: "http://127.0.0.1/mcp",
		Target: Target{Project: "p", Location: "l", Cluster: "c"}}
	var logs strings.Builder
	log := slog.New(slog.NewTextHandler(&logs, nil))
	h, metaPath, meta, err := newHandler(cfg, log, &testOverrides{
		TokenInfoURL: srv.URL + "/tokeninfo", UserInfoURL: srv.URL + "/userinfo", Spike: spikeFor(srv)})
	if err != nil {
		t.Fatal(err)
	}
	if metaPath != "/.well-known/oauth-protected-resource/mcp" {
		t.Errorf("metadata path %q", metaPath)
	}
	rec := httptest.NewRecorder()
	meta.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, metaPath, nil))
	if !strings.Contains(rec.Body.String(), "accounts.google.com") {
		t.Errorf("metadata: %s", rec.Body.String())
	}

	post := func(session, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Accept", "application/json, text/event-stream")
		r.Header.Set("Authorization", "Bearer "+secret)
		if session != "" {
			r.Header.Set("Mcp-Session-Id", session)
			r.Header.Set("Mcp-Protocol-Version", "2025-06-18")
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec
	}
	rec = post("", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"ge","version":"1"}}}`)
	session := rec.Header().Get("Mcp-Session-Id")
	if rec.Code != http.StatusOK || session == "" {
		t.Fatalf("initialize: %d", rec.Code)
	}
	post(session, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	rec = post(session, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"spike_report","arguments":{}}}`)
	body, _ := io.ReadAll(rec.Body)
	out := string(body)
	if rec.Code != http.StatusOK || !strings.Contains(out, "gke.dns-endpoint.authn") || !strings.Contains(out, "ACCEPTED") {
		t.Fatalf("tools/call: %d: %s", rec.Code, out)
	}
	if strings.Contains(out, "planted-spike") {
		t.Errorf("the report carries the token: %s", out)
	}
	if strings.Contains(logs.String(), "planted-spike") {
		t.Errorf("the log carries the token: %s", logs.String())
	}

	// No token, no report.
	r := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader("{}"))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("no token: %d, want 401", rec.Code)
	}
}

func TestParseCluster(t *testing.T) {
	t.Parallel()
	if got, err := parseCluster("projects/p/locations/us-central1/clusters/c"); err != nil || got != (Target{"p", "us-central1", "c"}) {
		t.Errorf("good: %+v, %v", got, err)
	}
	for _, bad := range []string{"", "p/l/c", "projects/p/zones/z/clusters/c", "projects//locations/l/clusters/c"} {
		if _, err := parseCluster(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("SPIKE_CLIENT_ID", "")
	t.Setenv("SPIKE_MEMBERS", "")
	t.Setenv("SPIKE_RESOURCE", "")
	t.Setenv("SPIKE_CLUSTER", "")
	if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), "SPIKE_CLIENT_ID") || !strings.Contains(err.Error(), "SPIKE_CLUSTER") {
		t.Errorf("empty env: %v", err)
	}
	t.Setenv("SPIKE_CLIENT_ID", clientID)
	t.Setenv("SPIKE_MEMBERS", " a@x.example , @y.example ")
	t.Setenv("SPIKE_RESOURCE", "https://s.example/mcp")
	t.Setenv("SPIKE_CLUSTER", "projects/p/locations/l/clusters/c")
	c, err := configFromEnv()
	if err != nil || len(c.Members) != 2 || c.Port != "8080" || resourcePath(c.Resource) != "/mcp" {
		t.Errorf("full env: %+v, %v", c, err)
	}
}
