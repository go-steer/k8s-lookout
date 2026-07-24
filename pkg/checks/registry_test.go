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

package checks

import (
	"context"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func nopRun(_ context.Context, _ emit.Invocation) (int, error) { return 0, nil }

func validCommand(name, mcpName string) Command {
	return Command{
		Name:    name,
		MCPName: mcpName,
		Summary: "Test command; when in doubt, do not reach for it.",
		Run:     nopRun,
	}
}

func TestRegistryRegisterLookupAll(t *testing.T) {
	r := NewRegistry()
	r.Register(validCommand("triage logs", "k8s_triage_logs"))
	r.Register(validCommand("triage delta", "k8s_triage_delta"))
	r.Register(validCommand("state edges", "k8s_state_edges"))
	r.Register(validCommand("bundle", "k8s_triage_workload"))
	hidden := validCommand("net demo", "k8s_net_demo")
	hidden.Hidden = true
	r.Register(hidden)

	if _, ok := r.Lookup("triage logs"); !ok {
		t.Error("Lookup(triage logs) failed")
	}
	if _, ok := r.Lookup("net demo"); !ok {
		t.Error("hidden commands must still resolve")
	}
	if _, ok := r.Lookup("nonesuch"); ok {
		t.Error("Lookup(nonesuch) should fail")
	}

	var names []string
	for _, c := range r.All() {
		names = append(names, c.Name)
	}
	want := "bundle,net demo,state edges,triage delta,triage logs"
	if got := strings.Join(names, ","); got != want {
		t.Errorf("All() = %s, want %s", got, want)
	}

	// Hidden commands must not surface a group or a listing entry.
	if got := strings.Join(r.Groups(), ","); got != "state,triage" {
		t.Errorf("Groups() = %s, want state,triage", got)
	}
	if got := len(r.GroupCommands("triage")); got != 2 {
		t.Errorf("GroupCommands(triage) = %d commands, want 2", got)
	}
	if tl := r.TopLevel(); len(tl) != 1 || tl[0].Name != "bundle" {
		t.Errorf("TopLevel() = %+v, want [bundle]", tl)
	}
}

func TestRegisterPanics(t *testing.T) {
	tests := []struct {
		name string
		mut  func(r *Registry)
	}{
		{"duplicate name", func(r *Registry) {
			r.Register(validCommand("triage logs", "k8s_triage_logs"))
			r.Register(validCommand("triage logs", "k8s_other"))
		}},
		{"duplicate MCP name", func(r *Registry) {
			r.Register(validCommand("triage logs", "k8s_triage_logs"))
			r.Register(validCommand("triage delta", "k8s_triage_logs"))
		}},
		{"invalid command", func(r *Registry) {
			r.Register(validCommand("Triage Logs", "k8s_triage_logs"))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("Register should panic")
				}
			}()
			tt.mut(NewRegistry())
		})
	}
}

func TestCommandValidate(t *testing.T) {
	base := func() Command { return validCommand("triage logs", "k8s_triage_logs") }
	tests := []struct {
		name string
		mut  func(*Command)
	}{
		{"three-word name", func(c *Command) { c.Name = "triage logs deep" }},
		{"unknown group", func(c *Command) { c.Name = "vessel logs" }},
		{"uppercase MCP name", func(c *Command) { c.MCPName = "K8sTriageLogs" }},
		{"empty summary", func(c *Command) { c.Summary = "" }},
		{"multi-line summary", func(c *Command) { c.Summary = "a\nb" }},
		{"nil Run", func(c *Command) { c.Run = nil }},
		{"positional with empty meta", func(c *Command) {
			c.Positional = &Positional{Meta: "", Doc: "d"}
		}},
		{"positional with spaced meta", func(c *Command) {
			c.Positional = &Positional{Meta: "<a> <b>", Doc: "d"}
		}},
		{"positional without doc", func(c *Command) {
			c.Positional = &Positional{Meta: "<ref>", Doc: ""}
		}},
		{"flag shadows common flag", func(c *Command) {
			c.Flags = []emit.FlagSpec{{Name: "format", Type: emit.FlagString, Help: "h"}}
		}},
		{"bad flag default", func(c *Command) {
			c.Flags = []emit.FlagSpec{{Name: "limit", Type: emit.FlagInt, Default: "lots", Help: "h"}}
		}},
		{"output field shadows envelope", func(c *Command) {
			c.Output = []OutputField{{Name: "reason", Doc: "d"}}
		}},
		{"output field bad charset", func(c *Command) {
			c.Output = []OutputField{{Name: "Restart-Count", Doc: "d"}}
		}},
		{"output field without doc", func(c *Command) {
			c.Output = []OutputField{{Name: "restarts", Doc: ""}}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := base()
			tt.mut(&c)
			if err := c.Validate(); err == nil {
				t.Errorf("Validate() should reject %+v", c)
			}
		})
	}
	if err := base().Validate(); err != nil {
		t.Errorf("valid command rejected: %v", err)
	}
}

func TestGroupAndSub(t *testing.T) {
	c := validCommand("triage logs", "k8s_triage_logs")
	if c.Group() != "triage" || c.Sub() != "logs" {
		t.Errorf("Group/Sub = %q/%q", c.Group(), c.Sub())
	}
	top := validCommand("bundle", "k8s_triage_workload")
	if top.Group() != "" || top.Sub() != "bundle" {
		t.Errorf("top-level Group/Sub = %q/%q", top.Group(), top.Sub())
	}
}

func TestGroupSummariesCoverDesignGroups(t *testing.T) {
	for _, g := range []string{"triage", "state", "stab", "perf", "cloud", "net"} {
		if GroupSummary(g) == "" {
			t.Errorf("group %q from DESIGN.md §4.1 has no summary", g)
		}
	}
}
