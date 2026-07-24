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
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
)

// demoRegistry mounts the hidden demo check plus one visible sibling
// so group dispatch, listing, and help all have content.
func demoRegistry() *checks.Registry {
	r := checks.NewRegistry()
	r.Register(checktest.DemoCommand())
	visible := checktest.DemoCommand()
	visible.Name = "triage logs"
	visible.MCPName = "k8s_triage_logs"
	visible.Hidden = false
	r.Register(visible)
	top := checktest.DemoCommand()
	top.Name = "health"
	top.MCPName = "k8s_cluster_health"
	top.Hidden = false
	r.Register(top)
	return r
}

func TestCheckCommandsMounting(t *testing.T) {
	cmds := checkCommands(demoRegistry())
	var names []string
	for _, c := range cmds {
		names = append(names, c.name)
	}
	if got := strings.Join(names, ","); got != "triage,health" {
		t.Errorf("mounted commands = %s, want triage,health", got)
	}
	for _, c := range cmds {
		if c.summary == "" {
			t.Errorf("command %q mounted without a summary", c.name)
		}
	}
}

func TestCheckCommandsEmptyRegistry(t *testing.T) {
	if cmds := checkCommands(checks.NewRegistry()); len(cmds) != 0 {
		t.Errorf("empty registry should mount nothing, got %d commands", len(cmds))
	}
}

func TestRunGroupDispatch(t *testing.T) {
	reg := demoRegistry()
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string // substring; "" means stdout must be empty
	}{
		{"no subcommand is usage", nil, 2, ""},
		{"unknown subcommand is usage", []string{"nonesuch"}, 2, ""},
		{"help lists subcommands on stdout", []string{"--help"}, 0, "triage <command>"},
		{"hidden subcommand still runs", []string{"demo", "--count=1", "--timeout=5s"}, 0, "scanned=5 findings=1"},
		{"visible subcommand runs", []string{"logs", "--count=0"}, 0, "findings=0"},
		{"subcommand usage error propagates", []string{"demo", "--nonesuch"}, 2, ""},
		{"subcommand runtime error propagates", []string{"demo", "--fail"}, 1, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := runGroup(context.Background(), reg, "triage", tt.args, &stdout, &stderr)
			if code != tt.wantCode {
				t.Fatalf("exit = %d, want %d (stderr: %s)", code, tt.wantCode, stderr.String())
			}
			if tt.wantStdout == "" {
				// Findings emitted before a runtime error may
				// legitimately be on stdout, but no summary
				// line may terminate a failed run.
				if strings.Contains(stdout.String(), "scanned=") {
					t.Errorf("failed run must not emit a summary, stdout: %q", stdout.String())
				}
			} else if !strings.Contains(stdout.String(), tt.wantStdout) {
				t.Errorf("stdout = %q, want substring %q", stdout.String(), tt.wantStdout)
			}
			if code != 0 && stderr.Len() == 0 {
				t.Error("failures must be diagnosed on stderr")
			}
		})
	}
}

// TestGroupHelpListsOnlyVisible pins that hidden commands stay out
// of the group listing while remaining invocable.
func TestGroupHelpListsOnlyVisible(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runGroup(context.Background(), demoRegistry(), "triage", []string{"--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	if strings.Contains(stdout.String(), "demo") {
		t.Errorf("hidden command leaked into group help:\n%s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "logs") {
		t.Errorf("visible command missing from group help:\n%s", stdout.String())
	}
}
