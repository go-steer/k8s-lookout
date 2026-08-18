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
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestValidateLoopback(t *testing.T) {
	tests := []struct {
		addr string
		ok   bool
	}{
		{"127.0.0.1:8383", true},
		{"127.0.0.1:0", true},
		{"[::1]:9000", true},
		{"localhost:8080", true},
		{"127.8.8.8:80", true}, // whole 127/8 is loopback

		{"0.0.0.0:8383", false},   // wildcard = every interface
		{"[::]:8383", false},      // v6 wildcard
		{":8383", false},          // empty host binds every interface
		{"192.168.1.5:80", false}, // routable
		{"10.0.0.1:80", false},
		{"example.com:443", false}, // hostnames other than localhost are not provably loopback
		{"metadata.google.internal:80", false},
		{"127.0.0.1", false}, // missing port
		{"", false},
	}
	for _, tc := range tests {
		err := ValidateLoopback(tc.addr)
		if tc.ok && err != nil {
			t.Errorf("ValidateLoopback(%q) = %v, want nil", tc.addr, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("ValidateLoopback(%q) = nil, want refusal", tc.addr)
		}
	}
}

// TestServe_RefusesNonLoopback checks the enforcement is wired into
// Serve itself, not just available: a routable bind address fails
// loudly before any listener is opened.
func TestServe_RefusesNonLoopback(t *testing.T) {
	err := Serve(t.Context(), New(testRegistry(t), "test"), ServeOptions{Listen: "0.0.0.0:0"})
	if err == nil {
		t.Fatal("Serve accepted a non-loopback bind")
	}
	if !strings.Contains(err.Error(), "non-loopback") {
		t.Errorf("refusal does not explain itself: %v", err)
	}
}

// TestServe_RechecksTheBindPolicy: the policy is enforced at the flag
// layer too, but Serve is the last place before a socket exists, and a
// caller that opts in to a routable bind without bringing a token must
// not get one.
func TestServe_RechecksTheBindPolicy(t *testing.T) {
	err := Serve(t.Context(), New(testRegistry(t), "test"), ServeOptions{
		Listen:           "0.0.0.0:0",
		AllowNonLoopback: true,
	})
	if err == nil {
		t.Fatal("Serve opened a routable bind with no auth token")
	}
	if !strings.Contains(err.Error(), "--auth-token-file") {
		t.Errorf("refusal does not name the missing flag: %v", err)
	}
}

// TestServe_LoopbackHTTPRoundTrip serves streamable HTTP on
// 127.0.0.1:0, lists tools over a real HTTP client session, and
// checks Serve shuts down cleanly on context cancellation.
func TestServe_LoopbackHTTPRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, New(testRegistry(t), "test"), ServeOptions{
			Listen: "127.0.0.1:0",
			Ready:  ready,
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

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, nil)
	cs, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: "http://" + addr}, nil)
	if err != nil {
		t.Fatalf("client connect over HTTP: %v", err)
	}
	tools, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("tools/list over HTTP: %v", err)
	}
	if len(tools.Tools) == 0 {
		t.Fatal("no tools served over HTTP")
	}
	_ = cs.Close()

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
