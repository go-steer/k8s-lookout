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

// Package checks is the read-path command surface: implementations
// plus the metadata registry that is the single source of truth for
// every invocation surface (§4.3, §4.4.3). One Command declaration
// generates the CLI --help text, will generate the MCP JSON schema
// (mcp change), and stubs the skill reference docs — the §13
// contract tests keep emitted output and declared metadata from
// drifting apart.
package checks

import (
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// Command is one read-path command's metadata + implementation.
type Command struct {
	// Name is the CLI name under the multicall binary: either
	// "<group> <sub>" (e.g. "triage logs") or a bare top-level
	// name (e.g. "bundle").
	Name string
	// MCPName is the 1:1 MCP tool name (§4.3), e.g.
	// "k8s_triage_workload".
	MCPName string
	// Summary is the one-line when-to-use description, written as
	// a micro-skill (§4.4.1): when to reach for this, anchored to
	// a concept the model already knows — not a restatement of
	// the name.
	Summary string
	// Positional declares the command's positional argument, if it
	// takes one (at most one; multi-positional surfaces are not
	// part of the design). Content validation is the command's own
	// job — return emit.UsageErrorf for exit 2.
	Positional *Positional
	// Flags declares the command-specific flags. The §4.2 common
	// flags are implicit on every command and must not be
	// redeclared.
	Flags []emit.FlagSpec
	// Output is the glossary of Details keys this command may
	// emit. The envelope fields (emit.EnvelopeFields) are
	// implicit. Contract tests fail a command that emits an
	// undeclared key.
	Output []OutputField
	// Examples are complete invocations for the --help text, one
	// per line, without shell decoration.
	Examples []string
	// Run is the implementation, executed under emit.Run.
	Run emit.CheckFunc
	// GraphBacked marks commands that answer from the topology graph
	// and therefore accept the §6.6 point-in-time flags (--at,
	// --store) in addition to the §4.2 common set. Live-only
	// commands reject --at as an unknown flag.
	GraphBacked bool
	// Hidden commands resolve and run but are omitted from every
	// listing (used for test scaffolding).
	Hidden bool
}

// OutputField documents one Details key a command may emit.
type OutputField struct {
	Name string
	Doc  string
}

// Positional documents a command's positional argument for the
// usage line, --help, and (later) the MCP schema.
type Positional struct {
	// Meta is the placeholder shown in the usage line, e.g.
	// "<Kind>/[<namespace>/]<name>".
	Meta string
	// Doc explains the argument's syntax and defaults, --help
	// style: terse, exhaustive, written for an agent reader.
	Doc string
}

// Group returns the command's group ("" for top-level commands).
func (c Command) Group() string {
	if i := strings.IndexByte(c.Name, ' '); i >= 0 {
		return c.Name[:i]
	}
	return ""
}

// Sub returns the subcommand token within the group (the full name
// for top-level commands).
func (c Command) Sub() string {
	if i := strings.IndexByte(c.Name, ' '); i >= 0 {
		return c.Name[i+1:]
	}
	return c.Name
}

// groupDocs are the §4.1 command groups with their listing
// summaries. Registering a command under an unknown group is an
// error: new groups are a design-doc change first.
var groupDocs = map[string]string{
	"triage": "incident reads: everything abnormal, condensed logs/events, blast radius, what changed",
	"state":  "dependency + configuration verification: edges, webhooks, workload identity, volumes",
	"stab":   "stability reads: GitOps drift, node-drain blockers",
	"perf":   "control-plane and startup performance via Cloud Monitoring query packs",
	"cloud":  "GCP-side reads: stockouts, orphaned resources, IP space, quota",
	"net":    "active DNS/TCP/HTTP probes from inside the cluster",
}

// GroupSummary returns the listing summary for a §4.1 group.
func GroupSummary(group string) string { return groupDocs[group] }

var (
	namePattern    = regexp.MustCompile(`^[a-z][a-z0-9-]*( [a-z][a-z0-9-]*)?$`)
	mcpNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	outKeyPattern  = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

// Validate checks a Command declaration for the mistakes that would
// otherwise surface at an operator's terminal: malformed names,
// unknown groups, flag specs that shadow §4.2 common flags or carry
// unparseable defaults, and glossary keys outside the envelope's key
// charset. Register calls this and panics on error (registration is
// init-time; a bad declaration is a compile-adjacent bug).
func (c Command) Validate() error {
	if !namePattern.MatchString(c.Name) {
		return fmt.Errorf("command name %q must be '<group> <sub>' or a bare lowercase name", c.Name)
	}
	if g := c.Group(); g != "" {
		if _, ok := groupDocs[g]; !ok {
			return fmt.Errorf("command %q: unknown group %q (add it to §4.1 and groupDocs first)", c.Name, g)
		}
	}
	if !mcpNamePattern.MatchString(c.MCPName) {
		return fmt.Errorf("command %q: MCP tool name %q must be lowercase snake_case", c.Name, c.MCPName)
	}
	if c.Summary == "" || strings.ContainsRune(c.Summary, '\n') {
		return fmt.Errorf("command %q: summary must be one non-empty line", c.Name)
	}
	if c.Run == nil {
		return fmt.Errorf("command %q: nil Run", c.Name)
	}
	if p := c.Positional; p != nil {
		if p.Meta == "" || strings.ContainsAny(p.Meta, " \n") {
			return fmt.Errorf("command %q: positional meta must be one non-empty token, got %q", c.Name, p.Meta)
		}
		if p.Doc == "" || strings.ContainsRune(p.Doc, '\n') {
			return fmt.Errorf("command %q: positional doc must be one non-empty line", c.Name)
		}
	}
	validateSpecs := emit.ValidateSpecs
	if c.GraphBacked {
		// Graph-backed commands also reserve the §6.6 --at/--store
		// flag names.
		validateSpecs = emit.ValidateGraphBackedSpecs
	}
	if err := validateSpecs(c.Flags); err != nil {
		return fmt.Errorf("command %q: %w", c.Name, err)
	}
	seen := map[string]bool{}
	for _, e := range emit.EnvelopeFields() {
		seen[e] = true
	}
	for _, f := range c.Output {
		if !outKeyPattern.MatchString(f.Name) {
			return fmt.Errorf("command %q: output field %q must be lowercase snake_case", c.Name, f.Name)
		}
		if seen[f.Name] {
			return fmt.Errorf("command %q: output field %q duplicated (envelope fields are implicit)", c.Name, f.Name)
		}
		if f.Doc == "" {
			return fmt.Errorf("command %q: output field %q has no doc", c.Name, f.Name)
		}
		seen[f.Name] = true
	}
	return nil
}

// RunConfig returns the emit.RunConfig that executes this command
// under the §4.2 envelope. Both the CLI wiring and the test
// scaffolding go through here so there is exactly one place a
// command's metadata is bound to the runner.
func (c Command) RunConfig(stdout, stderr io.Writer) emit.RunConfig {
	maxArgs := 0
	if c.Positional != nil {
		maxArgs = 1
	}
	return emit.RunConfig{
		Name:        "lookout " + c.Name,
		Flags:       c.Flags,
		Check:       c.Run,
		Help:        c.Help(),
		MaxArgs:     maxArgs,
		GraphBacked: c.GraphBacked,
		Stdout:      stdout,
		Stderr:      stderr,
	}
}
