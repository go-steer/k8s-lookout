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

// The MCP tool profiles (issue #280) are a claim about the REAL
// command set — how many tools a profile has and what they cost —
// so they are asserted here, in the one package that imports every
// check implementation. The grammar itself is tested against a
// synthetic registry in internal/mcpserver.

import (
	"bytes"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/internal/mcpserver"
	"github.com/go-steer/k8s-lookout/pkg/checks"
)

// The point of the whole change is the byte count, so it is asserted
// rather than assumed: a profile costs materially less than the full
// surface it is drawn from.
func TestProfileIsCheaperThanTheFullSurface(t *testing.T) {
	reg := checks.Default()
	full := mcpserver.SchemaBytes(reg, nil)
	if full == 0 {
		t.Fatal("default registry advertises nothing — check imports")
	}
	for _, name := range checks.MCPProfileNames() {
		sel, err := mcpserver.ResolveTools(reg, name, "")
		if err != nil {
			t.Errorf("profile %q: %v", name, err)
			continue
		}
		size := mcpserver.SchemaBytes(reg, sel)
		t.Logf("%s: %d tools, %d bytes of schema (full: %d tools, %d bytes)",
			name, len(sel), size, len(mcpserver.Advertised(reg, nil)), full)
		if size >= full/2 {
			t.Errorf("profile %q is %d bytes against a full surface of %d — a profile is meant to be a fraction of the surface, not a trim",
				name, size, full)
		}
	}
}

// Every declared profile must have members: a profile nothing joined
// is a flag value that fails at the operator's terminal, and
// registration cannot catch it because a Command sees only itself.
func TestEveryProfileHasMembers(t *testing.T) {
	reg := checks.Default()
	for _, name := range checks.MCPProfileNames() {
		sel, err := mcpserver.ResolveTools(reg, name, "")
		if err != nil {
			t.Errorf("profile %q: %v", name, err)
			continue
		}
		if len(sel) < 2 {
			t.Errorf("profile %q has %d member(s) — a profile is a working surface, not a shortcut for one tool", name, len(sel))
		}
	}
}

// Profile names share a namespace with tool names in the --tools
// grammar, so a collision would make one of the two unreachable.
func TestProfileNamesDoNotCollideWithToolNames(t *testing.T) {
	reserved := map[string]bool{"all": true, checks.ProfileFull: true}
	for _, name := range checks.MCPProfileNames() {
		if reserved[name] {
			t.Errorf("profile %q shadows a reserved selection token", name)
		}
		for _, c := range checks.All() {
			if c.MCPName == name {
				t.Errorf("profile %q has the same name as the tool for `lookout %s`", name, c.Name)
			}
		}
	}
}

func TestProfileHelpFitsAndNamesEveryProfile(t *testing.T) {
	help := mcpserver.ProfileHelp(checks.Default())
	for _, name := range append(checks.MCPProfileNames(), checks.ProfileFull) {
		if !strings.Contains(help, name) {
			t.Errorf("--help profile table omits %q:\n%s", name, help)
		}
	}
	for _, line := range strings.Split(help, "\n") {
		if n := len([]rune(line)); n > 78 {
			t.Errorf("profile help line is %d columns:\n%s", n, line)
		}
	}
}

// The flags are only useful if the CLI plumbs them, so the surface is
// asserted through mcpMain rather than through the resolver.
func TestMCPListToolsHonorsTheSelection(t *testing.T) {
	run := func(args ...string) (int, string, string) {
		var stdout, stderr bytes.Buffer
		code := mcpMain(t.Context(), args, &stdout, &stderr)
		return code, stdout.String(), stderr.String()
	}

	code, full, stderr := run("--list-tools")
	if code != 0 {
		t.Fatalf("--list-tools exit = %d, stderr: %s", code, stderr)
	}
	if !strings.Contains(full, "k8s_audit_netpol") || !strings.Contains(full, "k8s_scan") {
		t.Errorf("the default listing is not the full surface:\n%s", full)
	}

	code, triage, stderr := run("--profile=triage", "--list-tools")
	if code != 0 {
		t.Fatalf("--profile=triage exit = %d, stderr: %s", code, stderr)
	}
	if !strings.Contains(triage, "k8s_scan") {
		t.Errorf("triage profile omits the entry point:\n%s", triage)
	}
	if strings.Contains(triage, "k8s_audit_netpol") {
		t.Errorf("triage profile advertises a posture tool:\n%s", triage)
	}

	code, trimmed, stderr := run("--profile=triage", "--tools=-k8s_scan", "--list-tools")
	if code != 0 {
		t.Fatalf("--tools removal exit = %d, stderr: %s", code, stderr)
	}
	if strings.Contains(trimmed, "k8s_scan") {
		t.Errorf("--tools=-k8s_scan did not remove it:\n%s", trimmed)
	}

	// A misspelled profile is a usage error naming what was accepted,
	// not a server that quietly comes up with everything loaded.
	code, stdout, stderr := run("--profile=trage", "--list-tools")
	if code != 2 {
		t.Errorf("a bogus profile exited %d, want 2 (stdout: %q)", code, stdout)
	}
	if !strings.Contains(stderr, "triage") {
		t.Errorf("the error does not name the profiles that do exist: %q", stderr)
	}
}
