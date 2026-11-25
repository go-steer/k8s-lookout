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
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-steer/k8s-lookout/internal/mcpserver"
	"github.com/go-steer/k8s-lookout/internal/version"
	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// commonPropertyFlags maps §4.2 common flags as MCP schema property
// names to their CLI flag names so tests can verify each tool carries
// exactly the common properties it accepts (#867).
var commonPropertyFlags = []struct {
	prop string
	flag string
}{
	{"namespace", "namespace"},
	{"all_namespaces", "A"},
	{"workload", "workload"},
	{"since", "since"},
	{"format", "format"},
	{"timeout", "timeout"},
}

// TestMCPServesEveryRegisteredCommand is the no-golden smoke test
// over the REAL default registry (this package imports every check
// implementation): each non-hidden command yields exactly one tool
// (omitting tools whose required server flag is unset), named by its
// MCPName, with a resolvable object schema carrying its applicable
// common flags, its own flags, and target iff positional.
// Commands merged later are covered automatically — they register,
// so they are asserted. Privileged commands are the one exception:
// they are never on the default surface (TestMCPProbeFrom*).
func TestMCPServesEveryRegisteredCommand(t *testing.T) {
	visible := map[string]checks.Command{}
	defaultVisible := map[string]checks.Command{}
	for _, c := range checks.All() {
		if !c.Hidden && !c.Privileged {
			visible[c.MCPName] = c
			if c.MCPRequiresServerFlag == "" {
				defaultVisible[c.MCPName] = c
			}
		}
	}
	if len(visible) == 0 {
		t.Fatal("default registry has no visible commands — check imports")
	}

	// Without --exemptions / --store, tools requiring those server flags
	// are omitted from the advertised surface (#867).
	bareRes := listToolsFor(t, mcpserver.New(checks.Default(), version.Semver()))
	if got, want := len(bareRes.Tools), len(defaultVisible); got != want {
		t.Errorf("unconfigured server served %d tools, want %d (omitting tools requiring unset server flags)", got, want)
	}
	bareNames := map[string]bool{}
	for _, tool := range bareRes.Tools {
		bareNames[tool.Name] = true
	}
	for _, hidden := range []string{"k8s_audit_exemptions", "k8s_findings_diff"} {
		if bareNames[hidden] {
			t.Errorf("unconfigured server must omit %q when its required server flag is unset", hidden)
		}
	}
	for _, kept := range []string{"k8s_triage_status", "k8s_cluster_health", "k8s_findings_ack"} {
		if !bareNames[kept] {
			t.Errorf("unconfigured server must still advertise %q", kept)
		}
	}

	// With both --exemptions and --store configured, every non-hidden,
	// non-privileged command is served.
	res := listToolsFor(t, mcpserver.New(checks.Default(), version.Semver(),
		mcpserver.WithExemptions("/tmp/exemptions.yaml"),
		mcpserver.WithStore("/tmp/lookout.db"),
	))
	if got, want := len(res.Tools), len(visible); got != want {
		t.Errorf("configured server served %d tools, want %d (one per non-hidden command)", got, want)
	}

	defaultAllNS := map[string]bool{
		"k8s_audit_all":          true,
		"k8s_audit_workloads":    true,
		"k8s_audit_hardening":    true,
		"k8s_audit_rbac":         true,
		"k8s_audit_netpol":       true,
		"k8s_audit_namespaces":   true,
		"k8s_audit_computeclass": true,
		"k8s_audit_ai":           true,
	}

	for _, tool := range res.Tools {
		c, ok := visible[tool.Name]
		if !ok {
			t.Errorf("tool %q does not correspond to a registered command", tool.Name)
			continue
		}
		if tool.Description == "" {
			t.Errorf("tool %q: empty description", tool.Name)
		}
		if !strings.Contains(tool.Description, "scanned=<n> findings=<n> elapsed=<d>") {
			t.Errorf("tool %q: description does not state the §4.2 output contract", tool.Name)
		}
		if strings.Contains(tool.Description, "--help") {
			t.Errorf("tool %q: description must not contain --help:\n%s", tool.Name, tool.Description)
		}
		// Strip quoted `lookout ...` CLI examples before checking that prose
		// names MCP properties rather than CLI flags.
		prose := stripBacktickLookout(tool.Description)
		for _, cliFlag := range []string{"--namespace", "--workload", " -A", "(-A"} {
			if strings.Contains(prose, cliFlag) {
				t.Errorf("tool %q: description prose still contains CLI flag %q:\n%s", tool.Name, cliFlag, tool.Description)
			}
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
		for _, p := range commonPropertyFlags {
			has := schema.Properties[p.prop] != nil
			want := c.AcceptsCommonFlag(p.flag)
			if has != want {
				t.Errorf("tool %q: common property %q present = %v, want %v", tool.Name, p.prop, has, want)
			}
		}
		if defaultAllNS[tool.Name] {
			if !c.MCPDefaultAllNamespaces {
				t.Errorf("tool %q: expected MCPDefaultAllNamespaces=true", tool.Name)
			}
			prop := schema.Properties["all_namespaces"]
			if prop == nil || string(prop.Default) != "true" {
				t.Errorf("tool %q: all_namespaces schema default = %s, want true", tool.Name, string(prop.Default))
			}
		} else if c.MCPDefaultAllNamespaces {
			t.Errorf("tool %q: unexpected MCPDefaultAllNamespaces=true", tool.Name)
		}
		for _, f := range c.Flags {
			if mcpserver.IsServerScopedFlag(f.Name) {
				continue
			}
			p := strings.ReplaceAll(f.Name, "-", "_")
			if schema.Properties[p] == nil {
				t.Errorf("tool %q: schema is missing declared flag property %q", tool.Name, p)
			}
		}
		for _, forbidden := range []string{"kubeconfig", "context", "exemptions", "store", "report"} {
			if schema.Properties[forbidden] != nil {
				t.Errorf("tool %q: schema must not expose server-scoped flag %q", tool.Name, forbidden)
			}
		}
		if _, hasTarget := schema.Properties["target"]; hasTarget != (c.Positional != nil) {
			t.Errorf("tool %q: target property presence = %v, want %v", tool.Name, hasTarget, c.Positional != nil)
		}
	}
}

func listToolsFor(t *testing.T, server *mcp.Server) *mcp.ListToolsResult {
	t.Helper()
	serverT, clientT := mcp.NewInMemoryTransports()
	if _, err := server.Connect(t.Context(), serverT, nil); err != nil {
		t.Fatalf("server connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, nil)
	cs, err := client.Connect(t.Context(), clientT, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	res, err := cs.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	return res
}

func stripBacktickLookout(s string) string {
	var b strings.Builder
	for {
		start := strings.Index(s, "`lookout ")
		if start < 0 {
			b.WriteString(s)
			return b.String()
		}
		end := strings.IndexByte(s[start+1:], '`')
		if end < 0 {
			b.WriteString(s)
			return b.String()
		}
		b.WriteString(s[:start])
		s = s[start+end+2:]
	}
}

// TestCLIAuditDefaultUnchanged pins that MCP's all_namespaces=true default
// and MCP-worded errors do not alter CLI behavior (docs/cli-stability-policy.md).
func TestCLIAuditDefaultUnchanged(t *testing.T) {
	c, ok := checks.Default().Lookup("audit namespaces")
	if !ok {
		t.Fatal("audit namespaces not registered")
	}
	var stdout, stderr bytes.Buffer
	code := emit.Run(t.Context(), c.RunConfig(&stdout, &stderr), nil)
	if code != emit.ExitUsage {
		t.Fatalf("CLI `lookout audit namespaces` exit = %d, want %d (stderr: %q)", code, emit.ExitUsage, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--namespace=<ns> or -A") || !strings.Contains(stderr.String(), "Run 'lookout audit namespaces --help' for usage.") {
		t.Errorf("CLI usage error changed unexpectedly: %q", stderr.String())
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

		// #747: 0 disables the response cap; below that is a mistake.
		{"negative response cap", []string{"--max-response-bytes=-1"}, 2, "--max-response-bytes must not be negative"},

		// #699: Google-token mode.
		{"google mode without listen", []string{
			"--google-client-id=c", "--google-allow=@x.example", "--resource-url=https://l.example/mcp",
		}, 2, "stdio transport"},
		{"google mode needs an allow-list", []string{
			"--listen=127.0.0.1:0", "--google-client-id=c", "--resource-url=https://l.example/mcp",
		}, 2, "--google-allow"},
		{"google mode needs the resource URL", []string{
			"--listen=127.0.0.1:0", "--google-client-id=c", "--google-allow=@x.example",
		}, 2, "--resource-url"},
		{"google mode and a shared token are exclusive", []string{
			"--listen=127.0.0.1:0", "--google-client-id=c", "--google-allow=@x.example",
			"--resource-url=https://l.example/mcp", "--auth-token-file=/dev/null",
		}, 2, "exclusive"},
		{"google flags without google mode", []string{"--google-allow=@x.example"}, 2, "--google-client-id"},
		{"resource URL without google mode", []string{"--resource-url=https://l.example/mcp"}, 2, "--google-client-id"},
		{"a malformed allow entry", []string{
			"--listen=127.0.0.1:0", "--google-client-id=c", "--google-allow=x.example",
			"--resource-url=https://l.example/mcp",
		}, 2, "--google-allow"},
		// Group entries need the service account they are checked as.
		{"a group entry without the groups service account", []string{
			"--listen=127.0.0.1:0", "--google-client-id=c", "--google-allow=group:sre@x.example",
			"--resource-url=https://l.example/mcp",
		}, 2, "--google-groups-service-account"},
		{"a group write entry without the groups service account", []string{
			"--listen=127.0.0.1:0", "--google-client-id=c", "--google-allow=@x.example",
			"--google-allow-write=group:sre@x.example", "--resource-url=https://l.example/mcp",
		}, 2, "--google-groups-service-account"},
		{"the groups service account without group entries", []string{
			"--listen=127.0.0.1:0", "--google-client-id=c", "--google-allow=@x.example",
			"--google-groups-service-account=g@p.iam.gserviceaccount.com", "--resource-url=https://l.example/mcp",
		}, 2, "neither --google-allow nor --google-allow-write"},
		{"a groups service account that is not one", []string{
			"--listen=127.0.0.1:0", "--google-client-id=c", "--google-allow=group:sre@x.example",
			"--google-groups-service-account=dana@x.example", "--resource-url=https://l.example/mcp",
		}, 2, "not a service account email"},
		{"the groups service account without google mode", []string{"--google-groups-service-account=g@p.iam.gserviceaccount.com"}, 2, "--google-client-id"},
		{"a plaintext resource URL", []string{
			"--listen=127.0.0.1:0", "--google-client-id=c", "--google-allow=@x.example",
			"--resource-url=http://l.example/mcp",
		}, 2, "--resource-url"},
		// #701: workload callers on the mTLS listener.
		{"workload flags without the mTLS listener", []string{"--workload-rules=/dev/null"}, 2, "--mtls-listen"},
		{"a client CA without the mTLS listener", []string{"--client-ca-file=/dev/null"}, 2, "--mtls-listen"},
		{"the mTLS listener needs rules", []string{"--mtls-listen=127.0.0.1:0", "--spiffe-trust-domain=example.test"}, 2, "--workload-rules"},
		{"the mTLS listener needs a way to verify", []string{"--mtls-listen=127.0.0.1:0", "--workload-rules=/dev/null"}, 2, "--spiffe-trust-domain"},
		{"two profiles", []string{
			"--mtls-listen=127.0.0.1:0", "--workload-rules=/dev/null", "--spiffe-trust-domain=example.test", "--client-ca-file=/dev/null",
		}, 2, "exclusive"},
		{"a credentials dir needs its trust domain", []string{
			"--mtls-listen=127.0.0.1:0", "--workload-rules=/dev/null", "--spiffe-credentials-dir=/tmp",
		}, 2, "--spiffe-trust-domain"},
		{"a routable mTLS listener needs the opt-in", []string{
			"--mtls-listen=0.0.0.0:8443", "--workload-rules=/dev/null", "--spiffe-trust-domain=example.test", "--access-log=/dev/null",
		}, 2, "--mtls-listen=\"0.0.0.0:8443\": refusing"},
		{"a routable mTLS listener needs the access log", []string{
			"--mtls-listen=0.0.0.0:8443", "--allow-non-loopback", "--workload-rules=/dev/null", "--spiffe-trust-domain=example.test",
		}, 2, "--access-log"},
		{"the two listeners need two addresses", []string{
			"--listen=127.0.0.1:8383", "--mtls-listen=127.0.0.1:8383", "--workload-rules=/dev/null", "--spiffe-trust-domain=example.test",
		}, 2, "different addresses"},
		{"google mode counts as authentication for the bind", []string{
			"--listen=0.0.0.0:8383", "--allow-non-loopback", "--google-client-id=c", "--google-allow=@x.example",
			"--resource-url=https://l.example/mcp",
		}, 2, "--access-log"},
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

// TestMCPGoogleModeServes: the real wiring starts Google-token mode with
// both halves bound (the HTTP layer and the tools that read each caller's
// token). If the server were built without WithGoogleAuth, Serve would
// refuse with an internal error instead of serving.
func TestMCPGoogleModeServes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	var stdout, stderr bytes.Buffer
	code := mcpMain(ctx, []string{
		"--listen=127.0.0.1:0", "--google-client-id=123-abc.apps.googleusercontent.com",
		"--google-allow=@x.example", "--resource-url=http://127.0.0.1/mcp",
	}, &stdout, &stderr)
	if code != 0 || strings.Contains(stderr.String(), "internal") {
		t.Fatalf("exit %d, stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "Google-token mode") {
		t.Errorf("no Google-token banner: %s", stderr.String())
	}
}

// TestMCPGoogleGroupsNeedTheGKEBuild: group entries are checked as an
// impersonated service account, which a build without the GKE provider
// cannot do; it refuses them at startup rather than never matching.
func TestMCPGoogleGroupsNeedTheGKEBuild(t *testing.T) {
	if cloud.CanImpersonate() {
		t.Skip("this build can impersonate")
	}
	var stdout, stderr bytes.Buffer
	code := mcpMain(t.Context(), []string{
		"--listen=127.0.0.1:0", "--google-client-id=c", "--google-allow=group:sre@x.example",
		"--google-groups-service-account=g@p.iam.gserviceaccount.com", "--resource-url=https://l.example/mcp",
	}, &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "-tags gke") || !strings.Contains(stderr.String(), "group: entries") {
		t.Errorf("exit %d, stderr: %s", code, stderr.String())
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
		"--kubeconfig", "--context", "--exemptions", "--store", "--report",
		"reverse proxy", "--max-response-bytes",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("help does not mention %q:\n%s", want, stdout.String())
		}
	}
	if stderr.Len() != 0 {
		t.Errorf("--help wrote to stderr: %q", stderr.String())
	}
}

func TestMCPWithReportPinsReportAndRejectsClientOverride(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "state.db")
	reportPath := filepath.Join(dir, "report.logfmt")
	reportBody := "kind=pod.crashloop severity=critical namespace=prod kind_of_object=Pod name=api-0 reason=CrashLoopBackOff message=\"back-off restarting failed container\"\nscanned=1 findings=1 elapsed=100ms\n"
	if err := os.WriteFile(reportPath, []byte(reportBody), 0o600); err != nil {
		t.Fatal(err)
	}

	server := mcpserver.New(checks.Default(), version.Semver(),
		mcpserver.WithStore(storePath),
		mcpserver.WithReport(reportPath),
	)
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

	// Caller-supplied "report" argument is rejected over JSON-RPC.
	_, err = cs.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      "k8s_findings_diff",
		Arguments: map[string]any{"report": "/etc/passwd"},
	})
	if err == nil || !strings.Contains(err.Error(), `unknown argument "report"`) {
		t.Fatalf("expected caller report argument rejection, got: %v", err)
	}

	// Operator-pinned --report and --store allow k8s_findings_diff to run cleanly.
	res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      "k8s_findings_diff",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("k8s_findings_diff failed: %v", err)
	}
	if res.IsError {
		t.Fatalf("k8s_findings_diff tool error: %v", res.Content)
	}
}

func TestVerifyVersionFallbackScript(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "dev", "ci", "presubmits", "verify-version-fallback")
	tests := []struct {
		name   string
		tag    string
		actual string
		wantOK bool
	}{
		{name: "minor release next minor fallback", tag: "v0.36.0", actual: "v0.37.0-dev", wantOK: true},
		{name: "minor release next patch fallback", tag: "v0.36.0", actual: "v0.36.1-dev", wantOK: true},
		{name: "patch release next minor fallback", tag: "v0.36.1", actual: "v0.37.0-dev", wantOK: true},
		{name: "patch release next patch fallback", tag: "v0.36.1", actual: "v0.36.2-dev", wantOK: true},
		{name: "minor release stale fallback", tag: "v0.36.0", actual: "v0.36.0-dev", wantOK: false},
		{name: "patch release stale patch fallback", tag: "v0.36.1", actual: "v0.36.1-dev", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(script)
			cmd.Dir = root
			cmd.Env = append(os.Environ(),
				"VERIFY_VERSION_FALLBACK_TAG="+tc.tag,
				"VERIFY_VERSION_FALLBACK_ACTUAL="+tc.actual,
			)
			out, err := cmd.CombinedOutput()
			if tc.wantOK && err != nil {
				t.Fatalf("expected exit 0 for tag=%s actual=%s, got err=%v:\n%s", tc.tag, tc.actual, err, out)
			}
			if !tc.wantOK && err == nil {
				t.Fatalf("expected non-zero exit for stale tag=%s actual=%s, got success:\n%s", tc.tag, tc.actual, out)
			}
		})
	}
}
