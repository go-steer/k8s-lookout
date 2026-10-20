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
	"net/http"
	"net/http/httptest"
	"testing"
)

// An outage is not a refusal (purser#23; purser v0.3.0, steward v0.3.0):
// when Google, the group directory or the groups identity cannot answer,
// the caller gets 503 with Retry-After and no sign-in challenge, so an MCP
// client retries with the credential it holds instead of signing in again.

// assertUnavailable checks a 503 that invites a retry, not a sign-in.
func assertUnavailable(t *testing.T, what string, resp *http.Response, body string) {
	t.Helper()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" || resp.Header.Get("WWW-Authenticate") != "" {
		t.Errorf("%s: %d, Retry-After %q, WWW-Authenticate %q; want 503 with Retry-After and no challenge\n%s",
			what, resp.StatusCode, resp.Header.Get("Retry-After"), resp.Header.Get("WWW-Authenticate"), body)
	}
}

// TestGoogleOutageIs503: tokeninfo answering 503 on Path A.
func TestGoogleOutageIs503(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "{}", http.StatusServiceUnavailable)
	}))
	t.Cleanup(down.Close)
	s := startGoogleServerOpts(t, true, func(o *GoogleOptions) { o.TokenInfoURL = down.URL })
	resp, body := s.post(t, "/mcp", "alice-token", callBody("k8s_whoami"))
	assertUnavailable(t, "Google down", resp, body)
	// A bad credential is still a refusal: no token at all.
	if resp, _ := s.post(t, "/mcp", "", callBody("k8s_whoami")); resp.StatusCode != http.StatusUnauthorized || resp.Header.Get("WWW-Authenticate") == "" {
		t.Errorf("no token: %d, WWW-Authenticate %q; want 401 with a challenge", resp.StatusCode, resp.Header.Get("WWW-Authenticate"))
	}
}
