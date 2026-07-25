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
	"fmt"
	"strings"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// envelopeHelp is the shared closing paragraph of every command's
// --help: the §4.2 contract, stated once, identically everywhere.
// Written for an agent reader (§4.4.1): what to parse, what silence
// means, what the exit codes are.
const envelopeHelp = `Output: one finding per line (logfmt; --format=json for one JSON object
per line), keys in fixed order; healthy resources emit nothing. The final
line is always the summary: scanned=<n> findings=<n> elapsed=<d> —
findings=0 with a summary present means "scanned and healthy"; a stream
without a summary line is void. Exit 0 data, 1 runtime error (diagnostics
on stderr only), 2 usage.`

// EnvelopeContract returns the shared §4.2 output-contract paragraph
// verbatim, for surfaces generated outside this package (the skill
// reference stubs, §4.4.3) so the wording never forks from --help.
func EnvelopeContract() string { return envelopeHelp }

// Help generates the command's full --help text from its metadata —
// the same declaration that produces the MCP schema and skill
// reference stubs (§4.4.3), so the surfaces cannot drift.
func (c Command) Help() string {
	var b strings.Builder
	if c.Positional != nil {
		fmt.Fprintf(&b, "Usage: lookout %s %s [flags]\n\n%s\n", c.Name, c.Positional.Meta, c.Summary)
		fmt.Fprintf(&b, "\nArgument:\n  %s  %s\n", c.Positional.Meta, c.Positional.Doc)
	} else {
		fmt.Fprintf(&b, "Usage: lookout %s [flags]\n\n%s\n", c.Name, c.Summary)
	}

	if len(c.Flags) > 0 {
		b.WriteString("\nFlags:\n")
		writeFlagTable(&b, c.Flags)
	}
	b.WriteString("\nCommon flags (every lookout command):\n")
	writeFlagTable(&b, emit.CommonFlags())
	if c.GraphBacked {
		b.WriteString("\nGraph history flags (graph-backed commands, §6.6):\n")
		writeFlagTable(&b, emit.GraphHistoryFlags())
	}

	if len(c.Output) > 0 {
		b.WriteString("\nOutput fields (beyond the shared envelope fields " +
			strings.Join(emit.EnvelopeFields(), ", ") + "):\n")
		width := 0
		for _, f := range c.Output {
			width = max(width, len(f.Name))
		}
		for _, f := range c.Output {
			fmt.Fprintf(&b, "  %-*s  %s\n", width, f.Name, f.Doc)
		}
	}

	b.WriteString("\n" + envelopeHelp + "\n")

	if len(c.Examples) > 0 {
		b.WriteString("\nExamples:\n")
		for _, e := range c.Examples {
			fmt.Fprintf(&b, "  %s\n", e)
		}
	}
	return b.String()
}

// writeFlagTable renders flag specs as aligned `--name=<value>`
// rows. Defaults render inline (the agent should not have to guess
// what it gets when it omits a flag); zero defaults render the type
// placeholder instead.
func writeFlagTable(b *strings.Builder, specs []emit.FlagSpec) {
	rows := make([][2]string, 0, len(specs))
	width := 0
	for _, s := range specs {
		lhs := flagLHS(s)
		width = max(width, len(lhs))
		rows = append(rows, [2]string{lhs, s.Help})
	}
	for _, r := range rows {
		fmt.Fprintf(b, "  %-*s  %s\n", width, r[0], r[1])
	}
}

func flagLHS(s emit.FlagSpec) string {
	dash := "--"
	if len(s.Name) == 1 {
		dash = "-"
	}
	switch {
	case s.Type == emit.FlagBool:
		return dash + s.Name
	case s.Default != "" && s.Default != "0s" && s.Default != "0":
		return fmt.Sprintf("%s%s=%s", dash, s.Name, s.Default)
	default:
		return fmt.Sprintf("%s%s=<%s>", dash, s.Name, s.Type)
	}
}

// GroupHelp generates the listing for `lookout <group> --help`: the
// group's visible subcommands with their when-to-use summaries.
func GroupHelp(r *Registry, group string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Usage: lookout %s <command> [flags]\n\n%s — %s\n\nCommands:\n",
		group, group, GroupSummary(group))
	cmds := r.GroupCommands(group)
	width := 0
	for _, c := range cmds {
		width = max(width, len(c.Sub()))
	}
	for _, c := range cmds {
		fmt.Fprintf(&b, "  %-*s  %s\n", width, c.Sub(), c.Summary)
	}
	fmt.Fprintf(&b, "\nRun 'lookout %s <command> --help' for flags, output fields, and examples.\n", group)
	return b.String()
}
