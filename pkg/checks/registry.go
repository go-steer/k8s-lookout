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

	"github.com/go-steer/k8s-lookout/pkg/emit"
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

// KindEntry is one row of the cluster-wide finding-kind glossary: the
// claim, every severity it can carry, and every command that emits it.
// Commands is plural because compositions re-emit their stages'
// findings verbatim — `scan`, `health` and `bundle` all speak kinds
// they do not own — and a reader hitting an unfamiliar kind in a
// report wants the full list of commands that could have produced it.
type KindEntry struct {
	KindField
	Commands []string
}

// KindGlossary is the union of every registered command's Kinds
// ledger, sorted by name: the whole vocabulary lookout can emit, in
// one list, derived from the declarations rather than restated
// alongside them (issue #278). It is what the generated glossary page
// renders and what a consumer building a kind-aware pipeline reads.
//
// Hidden commands are included. A kind is a kind whether or not the
// command that emits it is advertised, and a report carrying one is no
// less in need of an explanation.
func (r *Registry) KindGlossary() []KindEntry {
	byName := map[string]*KindEntry{}
	for _, c := range r.All() {
		for _, k := range c.Kinds {
			e, ok := byName[k.Name]
			if !ok {
				e = &KindEntry{KindField: k}
				byName[k.Name] = e
			}
			e.Severity = mergeSeverities(e.Severity, k.Severity)
			e.Commands = append(e.Commands, c.Name)
		}
	}
	out := make([]KindEntry, 0, len(byName))
	for _, e := range byName {
		sort.Strings(e.Commands)
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// mergeSeverities unions two severity sets back into the ledger's
// worst-first order, so a kind two commands declare differently still
// reads the way a single declaration would.
func mergeSeverities(a, b []string) []string {
	have := map[string]bool{}
	for _, s := range append(append([]string{}, a...), b...) {
		have[s] = true
	}
	var out []string
	for _, s := range emit.Severities() {
		if have[s] {
			out = append(out, s)
		}
	}
	return out
}

// Register, Lookup, and All are conveniences on the Default
// registry.
func Register(c Command)                 { defaultRegistry.Register(c) }
func Lookup(name string) (Command, bool) { return defaultRegistry.Lookup(name) }
func All() []Command                     { return defaultRegistry.All() }
