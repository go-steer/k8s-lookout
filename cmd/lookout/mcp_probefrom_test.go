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

// `net probe-from` is the one tool that changes a workload (#539), so
// it is off the MCP surface unless the operator turns it on with
// --probe-from-image (docs/in-pod-probe-design.md, "MCP exposure").
// k8s-sre-agent's read-only guard starts a bare `lookout mcp` and
// refuses to run if it sees a write tool it has not classified; these
// tests are what keep that agent working.

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-steer/k8s-lookout/internal/mcpserver"
	"github.com/go-steer/k8s-lookout/internal/version"
	"github.com/go-steer/k8s-lookout/pkg/checks"
)

const probeTool = "k8s_net_probe_from"

var probeImage = "ghcr.io/go-steer/lookout@sha256:" + strings.Repeat("0f", 32)

func listTools(t *testing.T, args ...string) (string, int, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := mcpMain(t.Context(), append([]string{"--list-tools"}, args...), &stdout, &stderr)
	return stdout.String(), code, stderr.String()
}

func TestMCPProbeFromIsOffByDefault(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--tools=all"},
		{"--profile=triage"},
		{"--tools=all,-k8s_scan"},
	} {
		out, code, stderr := listTools(t, args...)
		if code != 0 {
			t.Fatalf("%v: exit %d: %s", args, code, stderr)
		}
		if strings.Contains(out, probeTool) {
			t.Errorf("%v: %s is advertised without --probe-from-image:\n%s", args, probeTool, out)
		}
	}
}

func TestMCPProbeFromCannotBeSelectedByAClient(t *testing.T) {
	for _, args := range [][]string{
		{"--tools=" + probeTool},
		{"--tools=k8s_scan," + probeTool},
	} {
		_, code, stderr := listTools(t, args...)
		if code != 2 {
			t.Errorf("%v: exit %d, want 2", args, code)
		}
		if !strings.Contains(stderr, "never selected by --profile/--tools") {
			t.Errorf("%v: stderr does not explain why: %s", args, stderr)
		}
	}
}

func TestMCPProbeFromImageMustBeADigest(t *testing.T) {
	for _, img := range []string{
		"ghcr.io/go-steer/lookout:v0.33.0",
		"ghcr.io/go-steer/lookout",
		"ghcr.io/go-steer/lookout@sha256:abc",
	} {
		_, code, stderr := listTools(t, "--probe-from-image="+img)
		if code != 2 {
			t.Errorf("%s: exit %d, want 2 (stderr %s)", img, code, stderr)
		}
	}
}

func TestMCPProbeFromImageEnablesTheTool(t *testing.T) {
	out, code, stderr := listTools(t, "--probe-from-image="+probeImage)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if !strings.Contains(out, probeTool) {
		t.Errorf("%s not advertised under --probe-from-image:\n%s", probeTool, out)
	}
	if !strings.Contains(out, "k8s_scan") {
		t.Errorf("enabling the probe dropped the rest of the surface:\n%s", out)
	}

	// With a profile, it is that profile plus the probe.
	out, code, stderr = listTools(t, "--profile=triage", "--probe-from-image="+probeImage)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if !strings.Contains(out, probeTool) {
		t.Errorf("%s not advertised with --profile=triage:\n%s", probeTool, out)
	}
}

// TestMCPProbeFromToolShape checks what a client sees once the tool is
// on: not read-only, and no image argument — the operator fixed it.
func TestMCPProbeFromToolShape(t *testing.T) {
	reg, sel, err := enableProbeFrom(checks.Default(), nil, probeImage)
	if err != nil {
		t.Fatal(err)
	}
	server := mcpserver.New(reg, version.Semver(), mcpserver.WithTools(sel))
	serverT, clientT := mcp.NewInMemoryTransports()
	if _, err := server.Connect(t.Context(), serverT, nil); err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, nil)
	cs, err := client.Connect(t.Context(), clientT, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	res, err := cs.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var tool *mcp.Tool
	for _, tl := range res.Tools {
		if tl.Name == probeTool {
			tool = tl
		}
	}
	if tool == nil {
		t.Fatalf("%s not served", probeTool)
	}
	if tool.Annotations == nil || tool.Annotations.ReadOnlyHint {
		t.Errorf("%s must advertise ReadOnlyHint:false, got %+v", probeTool, tool.Annotations)
	}
	raw, err := json.Marshal(tool.InputSchema)
	if err != nil {
		t.Fatal(err)
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.Properties["image"] != nil {
		t.Errorf("%s exposes an image argument; the operator's --probe-from-image must fix it", probeTool)
	}
	for _, p := range []string{"pod", "dns", "tcp", "http", "probe_timeout"} {
		if schema.Properties[p] == nil {
			t.Errorf("%s schema lacks %q", probeTool, p)
		}
	}
}
