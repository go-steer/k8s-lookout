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
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-steer/k8s-lookout/internal/mcpserver"
	"github.com/go-steer/k8s-lookout/internal/version"
	"github.com/go-steer/k8s-lookout/pkg/checks"
)

// commonProperties are the §4.2 common flags as MCP schema property
// names; every tool's schema must carry all of them.
var commonProperties = []string{
	"namespace", "all_namespaces", "workload", "since", "format", "timeout",
}

// TestMCPServesEveryRegisteredCommand is the no-golden smoke test
// over the REAL default registry (this package imports every check
// implementation): each non-hidden command yields exactly one tool,
// named by its MCPName, with a resolvable object schema carrying the
// common flags, the command's own flags, and target iff positional.
// Commands merged later are covered automatically — they register,
// so they are asserted.
func TestMCPServesEveryRegisteredCommand(t *testing.T) {
	visible := map[string]checks.Command{}
	for _, c := range checks.All() {
		if !c.Hidden {
			visible[c.MCPName] = c
		}
	}
	if len(visible) == 0 {
		t.Fatal("default registry has no visible commands — check imports")
	}

	server := mcpserver.New(checks.Default(), version.Semver())
	serverT, clientT := mcp.NewInMemoryTransports()
	if _, err := server.Connect(t.Context(), serverT, nil); err != nil {
		t.Fatalf("server connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, nil)
	cs, err := client.Connect(t.Context(), clientT, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cs.Close()

	res, err := cs.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	if got, want := len(res.Tools), len(visible); got != want {
		t.Errorf("served %d tools, want %d (one per non-hidden command)", got, want)
	}

	for _, tool := range res.Tools {
		c, ok := visible[tool.Name]
		if !ok {
			t.Errorf("tool %q does not correspond to a registered command", tool.Name)
			continue
		}
		if !strings.HasPrefix(tool.Description, c.Summary) {
			t.Errorf("tool %q: description does not lead with the command summary", tool.Name)
		}
		if !strings.Contains(tool.Description, "scanned=<n> findings=<n> elapsed=<d>") {
			t.Errorf("tool %q: description does not state the §4.2 output contract", tool.Name)
		}

		// The wire schema must be a valid, resolvable 2020-12 object
		// schema (jsonschema.Resolve fails on structural errors).
		raw, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Errorf("tool %q: schema does not marshal: %v", tool.Name, err)
			continue
		}
		var schema jsonschema.Schema
		if err := json.Unmarshal(raw, &schema); err != nil {
			t.Errorf("tool %q: schema is not a JSON schema: %v", tool.Name, err)
			continue
		}
		if _, err := schema.Resolve(nil); err != nil {
			t.Errorf("tool %q: schema does not resolve: %v", tool.Name, err)
			continue
		}
		if schema.Type != "object" {
			t.Errorf("tool %q: schema type = %q, want object", tool.Name, schema.Type)
		}
		for _, p := range commonProperties {
			if schema.Properties[p] == nil {
				t.Errorf("tool %q: schema is missing common property %q", tool.Name, p)
			}
		}
		for _, f := range c.Flags {
			p := strings.ReplaceAll(f.Name, "-", "_")
			if schema.Properties[p] == nil {
				t.Errorf("tool %q: schema is missing declared flag property %q", tool.Name, p)
			}
		}
		if _, hasTarget := schema.Properties["target"]; hasTarget != (c.Positional != nil) {
			t.Errorf("tool %q: target property presence = %v, want %v", tool.Name, hasTarget, c.Positional != nil)
		}
	}
}

func TestMCPCommand_UsagePaths(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantCode int
		wantErr  string
	}{
		{"unknown flag", []string{"--bogus"}, 2, "Run 'lookout mcp --help'"},
		{"unexpected argument", []string{"stray"}, 2, `unexpected argument "stray"`},
		{"non-loopback listen", []string{"--listen=0.0.0.0:8383"}, 2, "non-loopback"},
		{"listen without port", []string{"--listen=127.0.0.1"}, 2, "host:port"},

		// #282: three separate mistakes, three separate refusals.
		{"routable bind names all three flags", []string{"--listen=0.0.0.0:8383"}, 2, "--allow-non-loopback"},
		{"a token alone does not change the bind", []string{
			"--listen=0.0.0.0:8383", "--auth-token-file=/dev/null", "--access-log=/dev/null",
		}, 2, "--allow-non-loopback"},
		{"the bind flag alone opens nothing", []string{
			"--listen=0.0.0.0:8383", "--allow-non-loopback", "--access-log=/dev/null",
		}, 2, "--auth-token-file"},
		{"off-host requires the access log", []string{
			"--listen=0.0.0.0:8383", "--allow-non-loopback", "--auth-token-file=/dev/null",
		}, 2, "--access-log"},

		// The HTTP-only flags are rejected on stdio, not ignored: a
		// token that authenticates nothing reads like one that does.
		{"auth token without listen", []string{"--auth-token-file=/dev/null"}, 2, "stdio transport"},
		{"allow-non-loopback without listen", []string{"--allow-non-loopback"}, 2, "stdio transport"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if got := mcpMain(t.Context(), tc.args, &stdout, &stderr); got != tc.wantCode {
				t.Errorf("exit code = %d, want %d (stderr: %s)", got, tc.wantCode, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantErr) {
				t.Errorf("stderr %q does not contain %q", stderr.String(), tc.wantErr)
			}
			if stdout.Len() != 0 {
				t.Errorf("usage error wrote to stdout: %q", stdout.String())
			}
		})
	}
}

// TestMCPAccessLogIsOpenedBeforeServing: an operator who asked to be
// able to answer "what did the agent call" must find out at startup
// that the path is unusable, not after the incident. The failure is a
// usage error, not a degraded run.
func TestMCPAccessLogIsOpenedBeforeServing(t *testing.T) {
	bad := filepath.Join(t.TempDir(), "no-such-dir", "access.log")
	var stdout, stderr bytes.Buffer
	if got := mcpMain(t.Context(), []string{"--access-log=" + bad}, &stdout, &stderr); got != 2 {
		t.Errorf("exit code = %d, want 2 (stderr: %s)", got, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--access-log=") {
		t.Errorf("stderr %q does not name the flag", stderr.String())
	}

	// A query that never serves anything does not create the file:
	// --list-tools answers from the registry and exits.
	quiet := filepath.Join(t.TempDir(), "access.log")
	stdout.Reset()
	stderr.Reset()
	if got := mcpMain(t.Context(), []string{"--list-tools", "--access-log=" + quiet}, &stdout, &stderr); got != 0 {
		t.Fatalf("--list-tools exit code = %d, want 0 (stderr: %s)", got, stderr.String())
	}
	if _, err := os.Stat(quiet); !os.IsNotExist(err) {
		t.Errorf("--list-tools created the access log (stat err = %v)", err)
	}
}

// TestMCPAuthTokenIsLoadedBeforeBinding: an unusable token file must
// fail at startup, not leave a listener open that rejects everything.
func TestMCPAuthTokenIsLoadedBeforeBinding(t *testing.T) {
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{
		"--listen=127.0.0.1:0",
		"--auth-token-file=" + token,
		"--access-log=" + filepath.Join(dir, "access.log"),
	}
	var stdout, stderr bytes.Buffer
	if got := mcpMain(t.Context(), args, &stdout, &stderr); got != 2 {
		t.Errorf("exit code = %d, want 2 (stderr: %s)", got, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--auth-token-file=") {
		t.Errorf("stderr %q does not name the flag", stderr.String())
	}
}

func TestMCPCommand_Help(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if got := mcpMain(t.Context(), []string{"--help"}, &stdout, &stderr); got != 0 {
		t.Fatalf("exit code = %d, want 0", got)
	}
	for _, want := range []string{
		"--listen", "stdio", "loopback", "--access-log",
		"--allow-non-loopback", "--auth-token-file",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("help does not mention %q:\n%s", want, stdout.String())
		}
	}
	if stderr.Len() != 0 {
		t.Errorf("--help wrote to stderr: %q", stderr.String())
	}
}
