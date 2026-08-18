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
	"fmt"
	"sort"
	"strings"

	"github.com/go-steer/k8s-lookout/pkg/checks"
)

// Every tool lookout advertises costs the model tokens on every turn
// whether or not it is ever called, and — the part that is easy to
// miss — costs it accuracy: choosing among thirty similar-sounding
// options is harder than choosing among seven. Measured on a demo
// run, the full surface was 116,480 bytes of JSON schema, roughly 32k
// tokens, per call.
//
// So the surface is selectable. The default is unchanged (everything),
// because a server that quietly hid tools would be a worse failure
// than an expensive one; the win is opt-in.

// ResolveTools returns the set of MCP tool names to advertise for a
// --profile and --tools selection, or nil for "everything" — the
// default, and what New advertises when given no selection.
//
// The two flags are one left-to-right token list, --profile first,
// over a universe of tool names. A token is `all`/`full` (every
// advertisable tool), a declared profile name (its members), or a
// single MCP tool name; a `-` prefix removes instead of adds. That is
// the `all,-x` syntax `bundle --lists` already establishes, with
// profiles as the named groups.
//
//	--profile=triage                      the curated triage surface
//	--profile=triage --tools=-k8s_bundle  that, minus one tool
//	--tools=all,-k8s_perf_probe           everything but one
//	--tools=k8s_scan,k8s_triage_logs      exactly two
//
// An empty selection is nil rather than "every name", so the caller
// cannot tell an explicit full selection from the default and does
// not need to.
func ResolveTools(reg *checks.Registry, profile, tools string) (map[string]bool, error) {
	spec := strings.TrimSpace(profile)
	if t := strings.TrimSpace(tools); t != "" {
		if spec == "" {
			spec = t
		} else {
			spec += "," + t
		}
	}
	if spec == "" {
		return nil, nil
	}

	advertisable := map[string]checks.Command{}
	for _, c := range reg.All() {
		if !c.Hidden {
			advertisable[c.MCPName] = c
		}
	}

	set := map[string]bool{}
	for _, raw := range strings.Split(spec, ",") {
		tok := strings.TrimSpace(raw)
		if tok == "" {
			continue
		}
		remove := false
		if strings.HasPrefix(tok, "-") {
			remove, tok = true, strings.TrimSpace(tok[1:])
		}
		names, err := expand(tok, advertisable)
		if err != nil {
			return nil, err
		}
		for _, n := range names {
			if remove {
				delete(set, n)
			} else {
				set[n] = true
			}
		}
	}
	// Serving zero tools is never what anyone meant, and an MCP
	// client given an empty tool list has no way to say so: it just
	// behaves as though lookout is not installed.
	if len(set) == 0 {
		return nil, fmt.Errorf("the tool selection %q resolves to no tools at all; a server with an empty tool list is indistinguishable from a missing one", spec)
	}
	return set, nil
}

// expand resolves one selection token to the tool names it names.
func expand(tok string, advertisable map[string]checks.Command) ([]string, error) {
	if tok == "all" || tok == checks.ProfileFull {
		out := make([]string, 0, len(advertisable))
		for name := range advertisable {
			out = append(out, name)
		}
		sort.Strings(out)
		return out, nil
	}
	if checks.MCPProfileSummary(tok) != "" {
		var out []string
		for name, c := range advertisable {
			if c.InMCPProfile(tok) {
				out = append(out, name)
			}
		}
		sort.Strings(out)
		if len(out) == 0 {
			// Registration validates that a profile name exists; it
			// cannot validate that anything joined it.
			return nil, fmt.Errorf("profile %q has no members — nothing declares MCPProfiles: []string{%q}", tok, tok)
		}
		return out, nil
	}
	if _, ok := advertisable[tok]; ok {
		return []string{tok}, nil
	}
	return nil, fmt.Errorf("%q is neither a tool nor a profile (profiles: %s, all; tools: run `lookout mcp --list-tools`)",
		tok, strings.Join(checks.MCPProfileNames(), ", "))
}

// ProfileHelp renders the declared profiles for `lookout mcp --help`
// — name, tool count, schema cost, and what the surface is for — so
// the flag documents what it can be given rather than naming a
// concept and leaving the reader to grep. The cost column is there
// because it is the reason to pick anything but the default.
func ProfileHelp(reg *checks.Registry) string {
	var b strings.Builder
	line := func(name, summary string) {
		sel := profileSet(reg, name)
		cmds := Advertised(reg, sel)
		fmt.Fprintf(&b, "  %-8s %2d tools, %3d KB of schema\n", name, len(cmds), (SchemaBytes(reg, sel)+512)/1024)
		for _, l := range wrap(summary, 66) {
			fmt.Fprintf(&b, "           %s\n", l)
		}
	}
	line(checks.ProfileFull, "every registered command; the default, and what every client that asks for nothing gets")
	for _, name := range checks.MCPProfileNames() {
		line(name, checks.MCPProfileSummary(name))
	}
	return b.String()
}

// profileSet is the tool-name set for one profile, or nil for the
// full surface (which is what a nil selection means everywhere else).
func profileSet(reg *checks.Registry, profile string) map[string]bool {
	if profile == checks.ProfileFull {
		return nil
	}
	set := map[string]bool{}
	for _, c := range reg.All() {
		if !c.Hidden && c.InMCPProfile(profile) {
			set[c.MCPName] = true
		}
	}
	return set
}

// wrap greedily breaks text into lines of at most width runes. Help
// text is the one place in this binary that has a column budget, and
// a profile description long enough to be useful is long enough to
// need it.
func wrap(text string, width int) []string {
	var lines []string
	var cur []string
	n := 0
	for _, w := range strings.Fields(text) {
		if n > 0 && n+1+len([]rune(w)) > width {
			lines = append(lines, strings.Join(cur, " "))
			cur, n = nil, 0
		}
		if n > 0 {
			n++
		}
		cur = append(cur, w)
		n += len([]rune(w))
	}
	if len(cur) > 0 {
		lines = append(lines, strings.Join(cur, " "))
	}
	return lines
}
