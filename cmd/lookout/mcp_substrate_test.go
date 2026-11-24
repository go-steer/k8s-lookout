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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

const testSubprocessEnv = "LOOKOUT_MCP_SUBPROCESS_TEST"

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestMCPSubprocessEntrypoint runs realMain inside a child process when
// LOOKOUT_MCP_SUBPROCESS_TEST=1 so TestMCPSubprocessSIGTERMExitsPromptlyWithOpenStdin
// exercises the full signal.NotifyContext + stdio transport lifecycle.
func TestMCPSubprocessEntrypoint(t *testing.T) {
	if os.Getenv(testSubprocessEnv) != "1" {
		return
	}
	idx := slices.Index(os.Args, "--")
	if idx < 0 {
		os.Exit(2)
	}
	os.Exit(realMain(os.Args[idx+1:]))
}

func TestMCPOfflineInitializeAndToolsListWithoutClusterReachability(t *testing.T) {
	var netHits atomic.Int32
	tripwire := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		netHits.Add(1)
		t.Errorf("unexpected network call during initialize / tools/list: %s %s", r.Method, r.URL.Path)
		http.Error(w, "offline boot check", http.StatusServiceUnavailable)
	}))
	defer tripwire.Close()

	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(tripwire.URL, "http://"))
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "no-kubeconfig"))
	t.Setenv("KUBERNETES_SERVICE_HOST", "")

	for _, tc := range []struct {
		name      string
		args      []string
		env       map[string]string
		wantMulti bool
	}{
		{
			name: "explicit --clusters-from with CREDENTIAL_PROXY_URL",
			args: []string{"--profile=audit", "--clusters-from=proj-a"},
			env: map[string]string{
				cloud.CredentialProxyURLEnv:  tripwire.URL,
				cloud.CoreAgentGoogleAuthEnv: "",
				"GOOGLE_CLOUD_PROJECT":       "",
			},
			wantMulti: true,
		},
		{
			name: "shipmind gke-chat mcp.json args with CORE_AGENT_GOOGLE_AUTH=injected",
			args: []string{"--profile=audit"},
			env: map[string]string{
				cloud.CredentialProxyURLEnv:  "",
				cloud.CoreAgentGoogleAuthEnv: "injected",
				"GOOGLE_CLOUD_PROJECT":       "smoke",
			},
			wantMulti: true,
		},
		{
			name: "default single-cluster mode stays unchanged when neither multi-cluster nor injected/proxy is set",
			args: []string{"--profile=audit"},
			env: map[string]string{
				cloud.CredentialProxyURLEnv:  "",
				cloud.CoreAgentGoogleAuthEnv: "",
				cloud.MastGoogleAuthEnv:      "",
				cloud.GoogleAuthEnv:          "",
				"GOOGLE_CLOUD_PROJECT":       "",
			},
			wantMulti: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			var stderr syncBuffer
			cmd := exec.CommandContext(ctx, os.Args[0], append([]string{"-test.run=^TestMCPSubprocessEntrypoint$", "--", "mcp"}, tc.args...)...)
			cmd.Env = append(os.Environ(), testSubprocessEnv+"=1")
			cmd.Stderr = &stderr

			client := mcp.NewClient(&mcp.Implementation{Name: "offline-smoke", Version: "0"}, nil)
			cs, err := client.Connect(ctx, &mcp.CommandTransport{Command: cmd}, nil)
			if err != nil {
				t.Fatalf("Connect (initialize): %v\nstderr: %s", err, stderr.String())
			}
			defer func() { _ = cs.Close() }()

			tools, err := cs.ListTools(ctx, nil)
			if err != nil {
				t.Fatalf("ListTools: %v\nstderr: %s", err, stderr.String())
			}
			if netHits.Load() != 0 {
				t.Fatalf("network hits during initialize + tools/list = %d, want 0", netHits.Load())
			}

			var toolNames []string
			hasClusterProp := false
			for _, tool := range tools.Tools {
				toolNames = append(toolNames, tool.Name)
				if tool.Name == "k8s_audit_all" {
					raw, _ := json.Marshal(tool.InputSchema)
					hasClusterProp = strings.Contains(string(raw), `"cluster":{`)
				}
			}
			hasListClusters := slices.Contains(toolNames, "k8s_list_clusters")
			if hasListClusters != tc.wantMulti {
				t.Errorf("k8s_list_clusters present = %t, want %t (tools=%v)", hasListClusters, tc.wantMulti, toolNames)
			}
			if hasClusterProp != tc.wantMulti {
				t.Errorf("k8s_audit_all has cluster property = %t, want %t", hasClusterProp, tc.wantMulti)
			}
		})
	}
}

func writeEmptyK8sList(w http.ResponseWriter, reqPath string) {
	apiVer := "v1"
	if idx := strings.Index(reqPath, "/apis/"); idx >= 0 {
		parts := strings.Split(reqPath[idx+len("/apis/"):], "/")
		if len(parts) >= 2 {
			apiVer = parts[0] + "/" + parts[1]
		}
	}
	_, _ = fmt.Fprintf(w, `{"apiVersion":%q,"items":[]}`, apiVer)
}

func TestMCPCredentialProxyEndToEndLazyWidenAndSecretRedaction(t *testing.T) {
	const superSecretToken = "ya" + "29.SUPERSECRET_CREDENTIAL_PROXY_TOKEN_9876543210"

	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+superSecretToken {
			http.Error(w, "unauthorized proxy request", http.StatusUnauthorized)
			return
		}
		// Enforce GET-only on /v1/k8s/ just like Substrate's credential proxy.
		if strings.HasPrefix(r.URL.Path, "/v1/k8s/") && r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = io.WriteString(w, "POST refused by GET-only credential proxy")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/gcp/container.googleapis.com/v1/projects/proj-a/locations/-/clusters":
			_, _ = io.WriteString(w, `{"clusters":[{"name":"prod","location":"us-central1"}]}`)
		case r.URL.Path == "/v1/gcp/container.googleapis.com/v1/projects/proj-b/locations/us-east1/clusters/stage":
			_, _ = io.WriteString(w, `{"name":"stage","location":"us-east1"}`)
		case r.URL.Path == "/v1/k8s/proj-b/us-east1/stage/api/v1/namespaces":
			_, _ = io.WriteString(w, `{"kind":"NamespaceList","apiVersion":"v1","items":[{"metadata":{"name":"default"}}]}`)
		case strings.HasPrefix(r.URL.Path, "/v1/k8s/proj-a/us-central1/prod/") ||
			strings.HasPrefix(r.URL.Path, "/v1/k8s/proj-b/us-east1/stage/"):
			writeEmptyK8sList(w, r.URL.Path)
		default:
			http.NotFound(w, r)
		}
	}))
	defer proxySrv.Close()

	tokFile := filepath.Join(t.TempDir(), "proxy.tok")
	if err := os.WriteFile(tokFile, []byte(superSecretToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	accessLog := filepath.Join(t.TempDir(), "access.log")

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	var stderr syncBuffer
	cmd := exec.CommandContext(ctx, os.Args[0],
		"-test.run=^TestMCPSubprocessEntrypoint$", "--",
		"mcp", "--profile=audit", "--access-log="+accessLog,
	)
	cmd.Env = append(os.Environ(),
		testSubprocessEnv+"=1",
		cloud.CredentialProxyURLEnv+"="+proxySrv.URL,
		cloud.CredentialProxyTokenFileEnv+"="+tokFile,
		"GOOGLE_CLOUD_PROJECT=proj-a",
		"KUBECONFIG="+filepath.Join(t.TempDir(), "missing"),
		"KUBERNETES_SERVICE_HOST=",
	)
	cmd.Stderr = &stderr

	client := mcp.NewClient(&mcp.Implementation{Name: "e2e-proxy-client", Version: "0"}, nil)
	cs, err := client.Connect(ctx, &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		t.Fatalf("Connect: %v\nstderr: %s", err, stderr.String())
	}
	defer func() { _ = cs.Close() }()

	// 1. k8s_list_clusters discovers proj-a/us-central1/prod and classifies access=read via GET fallback.
	listRes, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "k8s_list_clusters"})
	if err != nil || listRes.IsError {
		t.Fatalf("k8s_list_clusters: err=%v res=%+v\nstderr: %s", err, listRes, stderr.String())
	}
	listOut := firstTextContent(t, listRes)
	if !strings.Contains(listOut, "prod") || !strings.Contains(listOut, "access=read") {
		t.Errorf("k8s_list_clusters output missing prod with access=read:\n%s", listOut)
	}

	// 2. Calling a tool with an out-of-scope GKE resource path lazily widens scope without restart.
	auditRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "k8s_audit_namespaces",
		Arguments: map[string]any{
			"cluster":        "projects/proj-b/locations/us-east1/clusters/stage",
			"all_namespaces": true,
		},
	})
	if err != nil || auditRes.IsError {
		t.Fatalf("k8s_audit_namespaces on widened cluster: err=%v res=%+v\nstderr: %s", err, auditRes, stderr.String())
	}
	auditOut := firstTextContent(t, auditRes)
	if !strings.Contains(auditOut, "scanned=") {
		t.Errorf("k8s_audit_namespaces missing §4.2 summary line:\n%s", auditOut)
	}

	// 3. Subsequent k8s_list_clusters includes the newly widened cluster.
	listRes2, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "k8s_list_clusters"})
	if err != nil || listRes2.IsError {
		t.Fatalf("second k8s_list_clusters: err=%v res=%+v", err, listRes2)
	}
	listOut2 := firstTextContent(t, listRes2)
	if !strings.Contains(listOut2, "proj-b") || !strings.Contains(listOut2, "stage") {
		t.Errorf("second k8s_list_clusters did not include widened cluster:\n%s", listOut2)
	}

	// 4. Secret proxy token never appears in any MCP response, stderr diagnostic, or access log.
	logBytes, _ := os.ReadFile(accessLog)
	for surface, content := range map[string]string{
		"list_clusters":  listOut + "\n" + listOut2,
		"audit_response": auditOut,
		"stderr":         stderr.String(),
		"access_log":     string(logBytes),
	} {
		if strings.Contains(content, superSecretToken) {
			t.Errorf("secret proxy token leaked into %s!", surface)
		}
	}
}

func TestMCPSubprocessSIGTERMExitsPromptlyWithOpenStdin(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestMCPSubprocessEntrypoint$", "--", "mcp", "--profile=audit")
	cmd.Env = append(os.Environ(),
		testSubprocessEnv+"=1",
		cloud.CoreAgentGoogleAuthEnv+"=injected",
		"GOOGLE_CLOUD_PROJECT=smoke",
		"KUBECONFIG="+filepath.Join(t.TempDir(), "missing"),
		"KUBERNETES_SERVICE_HOST=",
	)

	// Hold stdin open across the entire test: core-agent sends SIGTERM before closing stdin.
	stdinW, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stdinW.Close() }()

	stdoutR, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr syncBuffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client := mcp.NewClient(&mcp.Implementation{Name: "sigterm-test", Version: "0"}, nil)
	cs, err := client.Connect(ctx, &mcp.IOTransport{Reader: stdoutR, Writer: stdinW}, nil)
	if err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("Connect over open pipes: %v\nstderr: %s", err, stderr.String())
	}
	if _, err := cs.ListTools(ctx, nil); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("ListTools: %v", err)
	}

	// Send SIGTERM WITHOUT closing stdinW or calling cs.Close()!
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("Signal(SIGTERM): %v", err)
	}

	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()

	select {
	case waitErr := <-waitDone:
		if waitErr != nil {
			t.Fatalf("lookout mcp exited non-zero on SIGTERM: %v\nstderr: %s", waitErr, stderr.String())
		}
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("lookout mcp hung on SIGTERM while stdin remained open\nstderr: %s", stderr.String())
	}
}

func firstTextContent(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			return tc.Text
		}
	}
	t.Fatalf("CallToolResult has no TextContent: %+v", res.Content)
	return ""
}

func TestCLIAuditInjectedAuthAndCredentialProxy(t *testing.T) {
	var seenAuth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/k8s/proj-a/us-central1/prod/api/v1/namespaces":
			_, _ = io.WriteString(w, `{"kind":"NamespaceList","apiVersion":"v1","items":[{"metadata":{"name":"default"}}]}`)
		case strings.HasPrefix(r.URL.Path, "/v1/k8s/proj-a/us-central1/prod/"):
			writeEmptyK8sList(w, r.URL.Path)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	// 1. lookout audit with --google-auth=injected rejects invalid values cleanly and
	// runs kubeconfig-free over CREDENTIAL_PROXY_URL with --context=<GKE resource path>.
	cmdBad := exec.CommandContext(t.Context(), os.Args[0],
		"-test.run=^TestMCPSubprocessEntrypoint$", "--",
		"audit", "namespaces", "-A", "--google-auth=bogus",
	)
	cmdBad.Env = append(os.Environ(), testSubprocessEnv+"=1")
	if err := cmdBad.Run(); err == nil {
		t.Fatal("lookout audit accepted --google-auth=bogus")
	} else if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 2 {
		t.Errorf("lookout audit --google-auth=bogus exit = %v, want exit code 2", err)
	}

	tokFile := filepath.Join(t.TempDir(), "proxy.tok")
	if err := os.WriteFile(tokFile, []byte("cli-proxy-tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmdProxy := exec.CommandContext(t.Context(), os.Args[0],
		"-test.run=^TestMCPSubprocessEntrypoint$", "--",
		"audit", "namespaces", "-A",
		"--context=projects/proj-a/locations/us-central1/clusters/prod",
	)
	cmdProxy.Env = append(os.Environ(),
		testSubprocessEnv+"=1",
		cloud.CredentialProxyURLEnv+"="+srv.URL,
		cloud.CredentialProxyTokenFileEnv+"="+tokFile,
		"KUBECONFIG="+filepath.Join(t.TempDir(), "missing"),
		"KUBERNETES_SERVICE_HOST=",
	)
	var out, errBuf bytes.Buffer
	cmdProxy.Stdout = &out
	cmdProxy.Stderr = &errBuf
	if err := cmdProxy.Run(); err != nil {
		t.Fatalf("lookout audit via CREDENTIAL_PROXY_URL failed: %v\nstderr: %s", err, errBuf.String())
	}
	if !strings.Contains(out.String(), "scanned=") {
		t.Errorf("lookout audit output missing §4.2 summary line:\n%s", out.String())
	}
	if got, _ := seenAuth.Load().(string); got != "Bearer cli-proxy-tok" {
		t.Errorf("Authorization = %q, want Bearer cli-proxy-tok", got)
	}

	// 2. --contexts with a stalled endpoint strictly honors --timeout (§4.2).
	stallDone := make(chan struct{})
	defer close(stallDone)
	stallSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-stallDone:
		case <-r.Context().Done():
		}
	}))
	defer stallSrv.Close()

	// Note: CREDENTIAL_PROXY_URL resolves Target offline and dials during the check,
	// while non-proxy GKE LookupCluster resolves during ResolveFleetTargetsContext;
	// test --timeout against a stalled proxy K8s endpoint on --contexts.
	cmdTimeout := exec.CommandContext(t.Context(), os.Args[0],
		"-test.run=^TestMCPSubprocessEntrypoint$", "--",
		"audit", "namespaces", "-A",
		"--contexts=projects/proj-a/locations/us-central1/clusters/prod",
		"--timeout=150ms",
	)
	cmdTimeout.Env = append(os.Environ(),
		testSubprocessEnv+"=1",
		cloud.CredentialProxyURLEnv+"="+stallSrv.URL,
		cloud.CredentialProxyTokenFileEnv+"="+tokFile,
		"KUBECONFIG="+filepath.Join(t.TempDir(), "missing"),
		"KUBERNETES_SERVICE_HOST=",
	)
	var tout, terr bytes.Buffer
	cmdTimeout.Stdout = &tout
	cmdTimeout.Stderr = &terr
	start := time.Now()
	_ = cmdTimeout.Run()
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("stalled --contexts took %s, ignoring --timeout=150ms", elapsed)
	}
}
