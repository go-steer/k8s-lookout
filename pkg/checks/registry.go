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
	"sort"
	"sync"
)

// Registry holds Command declarations. The package-level Default
// registry is what the multicall binary and the MCP server serve;
// tests build their own instances.
type Registry struct {
	mu   sync.RWMutex
	cmds map[string]Command
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{cmds: map[string]Command{}}
}

var defaultRegistry = NewRegistry()

// Default returns the process-wide registry that command
// implementations register into from their init functions.
func Default() *Registry { return defaultRegistry }

// Register adds a command. It panics on an invalid declaration or a
// duplicate name/MCP name — registration happens at init, so a panic
// here is a build-time bug report, mirroring the multicall
// dispatcher's own register().
func (r *Registry) Register(c Command) {
	if err := c.Validate(); err != nil {
		panic("checks.Register: " + err.Error())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.cmds[c.Name]; dup {
		panic("checks.Register: duplicate command " + c.Name)
	}
	for _, existing := range r.cmds {
		if existing.MCPName == c.MCPName {
			panic("checks.Register: duplicate MCP tool name " + c.MCPName)
		}
	}
	r.cmds[c.Name] = c
}

// Lookup resolves a full command name ("triage logs", "bundle"),
// including hidden commands.
func (r *Registry) Lookup(name string) (Command, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.cmds[name]
	return c, ok
}

// All returns every registered command, hidden included, sorted by
// name. Callers building listings filter Hidden themselves.
func (r *Registry) All() []Command {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Command, 0, len(r.cmds))
	for _, c := range r.cmds {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Groups returns the sorted groups that have at least one visible
// command — the set the multicall dispatcher mounts as `lookout
// <group> <sub>`.
func (r *Registry) Groups() []string {
	seen := map[string]bool{}
	var out []string
	for _, c := range r.All() {
		g := c.Group()
		if g == "" || c.Hidden || seen[g] {
			continue
		}
		seen[g] = true
		out = append(out, g)
	}
	sort.Strings(out)
	return out
}

// GroupCommands returns the visible commands of one group, sorted.
func (r *Registry) GroupCommands(group string) []Command {
	var out []Command
	for _, c := range r.All() {
		if c.Group() == group && !c.Hidden {
			out = append(out, c)
		}
	}
	return out
}

// TopLevel returns the visible commands without a group ("bundle",
// "health"), sorted.
func (r *Registry) TopLevel() []Command {
	var out []Command
	for _, c := range r.All() {
		if c.Group() == "" && !c.Hidden {
			out = append(out, c)
		}
	}
	return out
}

// Register, Lookup, and All are conveniences on the Default
// registry.
func Register(c Command)                 { defaultRegistry.Register(c) }
func Lookup(name string) (Command, bool) { return defaultRegistry.Lookup(name) }
func All() []Command                     { return defaultRegistry.All() }
