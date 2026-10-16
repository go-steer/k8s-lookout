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
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// fileEchoCommand reports the --store and --report it was given.
func fileEchoCommand() checks.Command {
	return checks.Command{
		Name:    "triage echofiles",
		MCPName: "k8s_echo_files",
		Summary: "Echoes its store and report paths; test scaffolding only.",
		Flags: []emit.FlagSpec{
			{Name: "store", Type: emit.FlagString, Default: "", Help: "store"},
			{Name: "report", Type: emit.FlagString, Default: "", Help: "report"},
		},
		Kinds: []checks.KindField{checks.Kind("fake.files", "synthetic; test scaffolding only", emit.SeverityInfo)},
		Output: []checks.OutputField{
			{Name: "store", Doc: "the --store value"},
			{Name: "report", Doc: "the --report value"},
		},
		Run: func(_ context.Context, inv emit.Invocation) (int, error) {
			return 1, inv.Out.Emit(emit.Finding{
				Kind: "fake.files", Severity: emit.SeverityInfo, KindOfObject: "Cluster", Name: "files",
				Reason: "Echo", Message: "echoes its files",
				Details: []emit.Field{{Key: "store", Value: "<" + inv.Flags.String("store") + ">"}, {Key: "report", Value: "<" + inv.Flags.String("report") + ">"}},
			})
		},
	}
}

// TestLocalCommandOnlyForTheLocalCluster: net probe measures from where
// lookout runs, so on a multi-cluster endpoint it is served only for
// that cluster, and for none when lookout cannot say where it runs.
func TestLocalCommandOnlyForTheLocalCluster(t *testing.T) {
	_, dir, reg := multiClusterServer(t, nil)
	local := targetEchoCommand("triage echolocal", "k8s_echo_local", false)
	local.Local = true
	r := reg()
	r.Register(local)

	cs := connect(t, New(r, "test", WithClusters(dir), WithLocalCluster("proj-a/us-central1/dev")))
	if res, err := callTool(t, cs, "k8s_echo_local", map[string]any{"cluster": "dev"}); err != nil || res.IsError {
		t.Errorf("the local cluster was refused: %v", err)
	}
	res, err := callTool(t, cs, "k8s_echo_local", map[string]any{"cluster": "prod@proj-b"})
	if err != nil || !res.IsError || !strings.Contains(text(t, res), "net probe-from") || !strings.Contains(text(t, res), "proj-b/us-east1/prod") {
		t.Errorf("another cluster: %v %+v", err, res)
	}

	cs = connect(t, New(r, "test", WithClusters(dir)))
	if res, _ := callTool(t, cs, "k8s_echo_local", map[string]any{"cluster": "dev"}); res == nil || !res.IsError {
		t.Error("with no known local cluster, a Local command ran")
	}
}

// TestPerClusterStoreAndReport: --store and --report are stems; each
// call reads its cluster's own file, named as a multi-cluster watch
// names it, and a cluster whose bare name repeats gets none (watch
// watches neither, so neither has a file).
func TestPerClusterStoreAndReport(t *testing.T) {
	_, dir, reg := multiClusterServer(t, nil)
	r := reg()
	r.Register(fileEchoCommand())
	cs := connect(t, New(r, "test", WithClusters(dir), WithStore("/var/lib/lookout/lookout.db"), WithReport("/var/lib/lookout/report.txt")))

	res, err := callTool(t, cs, "k8s_echo_files", map[string]any{"cluster": "dev"})
	if err != nil || res.IsError {
		t.Fatal(err)
	}
	out := text(t, res)
	if !strings.Contains(out, "store=</var/lib/lookout/lookout-dev.db>") || !strings.Contains(out, "report=</var/lib/lookout/report-dev.txt>") {
		t.Errorf("dev's files:\n%s", out)
	}
	res, err = callTool(t, cs, "k8s_echo_files", map[string]any{"cluster": "prod@proj-a"})
	if err != nil || res.IsError {
		t.Fatal(err)
	}
	if out := text(t, res); !strings.Contains(out, "store=<>") || !strings.Contains(out, "report=<>") {
		t.Errorf("an ambiguous name was given a file:\n%s", out)
	}
}

func TestClusterLimit(t *testing.T) {
	cfg := config{maxConcurrency: 8}
	for n, want := range map[int]int{0: 8, 1: 8, 3: 2, 4: 2, 100: 2} {
		if got := cfg.clusterLimit(n); got != want {
			t.Errorf("clusterLimit(%d) = %d, want %d", n, got, want)
		}
	}
	cfg.maxPerCluster = 5
	if got := cfg.clusterLimit(100); got != 5 {
		t.Errorf("explicit limit: %d", got)
	}
}

// TestPerClusterLimit: one cluster's calls past its cap are turned away
// while another cluster's still run.
func TestPerClusterLimit(t *testing.T) {
	_, dir, reg := multiClusterServer(t, nil) // three clusters: cap 8/3 → 2
	started, release := make(chan struct{}, 8), make(chan struct{})
	r := reg()
	r.Register(checks.Command{
		Name: "triage block", MCPName: "k8s_block", Summary: "Blocks until released; test scaffolding only.",
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
	cs := connect(t, New(r, "test", WithClusters(dir)))
	done := make(chan struct{}, 2)
	for range 2 {
		go func() { _, _ = callTool(t, cs, "k8s_block", map[string]any{"cluster": "dev"}); done <- struct{}{} }()
	}
	<-started
	<-started
	res, err := callTool(t, cs, "k8s_block", map[string]any{"cluster": "dev"})
	if err != nil || !res.IsError || !strings.Contains(text(t, res), "calls in flight") {
		t.Errorf("dev past its cap: %v %+v", err, res)
	}
	go func() {
		_, _ = callTool(t, cs, "k8s_block", map[string]any{"cluster": "prod@proj-b"})
		done <- struct{}{}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Error("another cluster was held back by dev's calls")
	}
	close(release)
	for range 3 {
		<-done
	}
}

// TestServeStateless: a stateless endpoint answers a call with no
// session, which is what lets replicas share one Service.
func TestServeStateless(t *testing.T) {
	for _, stateless := range []bool{true, false} {
		server := New(testRegistry(t), "test")
		ctx, cancel := context.WithCancel(context.Background())
		ready, done := make(chan string, 1), make(chan error, 1)
		go func() {
			done <- Serve(ctx, server, ServeOptions{Listen: "127.0.0.1:0", Ready: ready, Stateless: stateless})
		}()
		url := "http://" + <-ready
		req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		cancel()
		<-done
		answered := resp.StatusCode == http.StatusOK && strings.Contains(string(body), "k8s_fake_spec")
		if answered != stateless {
			t.Errorf("stateless=%t: status %d, answered without a session = %t", stateless, resp.StatusCode, answered)
		}
	}
}
