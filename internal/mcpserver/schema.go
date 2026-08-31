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
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// targetProperty is the schema property carrying a command's
// positional argument (commands declare at most one — checks.Command).
const targetProperty = "target"

// workloadProperty is the scoping property every tool declares (the
// §4.2 --workload common flag). targetProperty is the property name on
// the three tools that take a positional instead, and clients
// generalize it to the other twenty — see argv (#232).
const workloadProperty = "workload"

// durationPattern documents the Go duration syntax on duration-typed
// properties. It is advisory (the authoritative validation is the
// command's own flag parsing, which reports invalid-params); the
// pattern plus the per-flag examples keep an MCP client from guessing
// ISO-8601 or bare seconds.
const durationPattern = `^-?([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`

// propertyName maps a FlagSpec name to its MCP schema property name.
// Flag names are CLI-shaped (may contain hyphens, and the §4.2
// all-namespaces flag is the single letter -A); MCP property names
// are lowercase snake_case to match tool-name conventions.
func propertyName(flag string) string {
	if flag == "A" {
		return "all_namespaces"
	}
	return strings.ReplaceAll(flag, "-", "_")
}

// schemaSpecs returns the §4.2 common flags (plus, for graph-backed
// commands, the §6.6 history flags) followed by the command's own
// flags: the complete flag surface behind a tool's schema, in the
// order the surfaces document them.
func schemaSpecs(c checks.Command) []emit.FlagSpec {
	specs := c.CommonFlags()
	if c.GraphBacked {
		specs = append(specs, emit.GraphHistoryFlags()...)
	}
	return append(specs, c.Flags...)
}

// inputSchema derives a tool's JSON schema mechanically from the
// command's FlagSpecs and Positional declaration — the same metadata
// that generates --help, so the two surfaces cannot drift (§4.4.3).
// Every property is optional: omitted properties get the flag
// defaults, exactly like an omitted CLI flag.
func inputSchema(c checks.Command) *jsonschema.Schema {
	props := map[string]*jsonschema.Schema{}
	for _, s := range schemaSpecs(c) {
		props[propertyName(s.Name)] = flagSchema(s)
	}
	if p := c.Positional; p != nil {
		props[targetProperty] = &jsonschema.Schema{
			Type:        "string",
			Description: p.Meta + " — " + p.Doc,
		}
	}
	return &jsonschema.Schema{Type: "object", Properties: props}
}

// flagSchema maps one FlagSpec to its property schema. The FlagType
// set is closed by design (pkg/emit): string→string, bool→boolean,
// int→integer, duration→string with a Go-duration pattern note.
func flagSchema(s emit.FlagSpec) *jsonschema.Schema {
	out := &jsonschema.Schema{Description: s.Help}
	switch s.Type {
	case emit.FlagBool:
		out.Type = "boolean"
	case emit.FlagInt:
		out.Type = "integer"
	case emit.FlagDuration:
		out.Type = "string"
		out.Pattern = durationPattern
		out.Description = s.Help + ` (Go duration, e.g. "90s", "5m")`
	default: // emit.FlagString; specs are validated at registration
		out.Type = "string"
	}
	out.Default = defaultJSON(s)
	return out
}

// defaultJSON renders the declared default for the schema, omitting
// type-zero defaults ("", 0, 0s, false) — announcing "the default is
// nothing" is noise, mirroring the --help flag table.
func defaultJSON(s emit.FlagSpec) json.RawMessage {
	switch s.Default {
	case "", "0", "0s", "false":
		return nil
	}
	switch s.Type {
	case emit.FlagBool, emit.FlagInt:
		return json.RawMessage(s.Default)
	default:
		return json.RawMessage(strconv.Quote(s.Default))
	}
}

// toolDescription composes the tool description as a micro-skill
// (§4.4.1): the when-to-use Summary first, then the §4.2 output
// contract as the tool result's shape, then the compact output-field
// glossary. All of it comes from the same Command metadata as --help.
func toolDescription(c checks.Command) string {
	var b strings.Builder
	b.WriteString(c.Summary)
	b.WriteString("\nOutput: one finding per line (logfmt; format=json for JSON)," +
		" terminated by the mandatory summary line" +
		" scanned=<n> findings=<n> elapsed=<d> — findings=0 with a summary" +
		" present means scanned-and-healthy; a result without a summary line" +
		" is void.")
	if len(c.Output) > 0 {
		b.WriteString("\nFields beyond the shared envelope (" +
			strings.Join(emit.EnvelopeFields(), ", ") + "): ")
		for i, f := range c.Output {
			if i > 0 {
				b.WriteString("; ")
			}
			b.WriteString(f.Name + " — " + f.Doc)
		}
		b.WriteString(".")
	}
	return b.String()
}

// argv maps MCP call arguments back onto the command's CLI argv. It
// rejects unknown properties and JSON types that do not match the
// declared FlagType; everything else (duration syntax, flag
// combinations, target syntax) is validated by the one authoritative
// parser, emit.Run, whose usage errors surface as invalid params.
//
// It is deliberately tolerant in one place: `target` is accepted as
// `workload` on the tools that have no positional (#232). `target` is
// the real property name on three tools, models generalize it to the
// rest, and a rejection there costs a full round trip and returns
// nothing — an agent eval measured a retry ladder of
// `{"request": …}`, `{"target": …}`, `{}`, `{"workload": …}` on one
// call. Only the canonical name is advertised in the schema, so the
// tool surface still has one vocabulary; the intake just does not
// punish a near miss, and the unknown-argument error below teaches
// the right name for every other guess.
func argv(c checks.Command, raw json.RawMessage) ([]string, error) {
	var args map[string]json.RawMessage
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &args); err != nil {
			return nil, fmt.Errorf("arguments must be a JSON object: %v", err)
		}
	}
	specs := map[string]emit.FlagSpec{}
	for _, s := range schemaSpecs(c) {
		specs[propertyName(s.Name)] = s
	}

	// Deterministic argv order (map iteration is not).
	props := make([]string, 0, len(args))
	for p := range args {
		props = append(props, p)
	}
	slices.Sort(props)

	var out []string
	var target *string
	for _, prop := range props {
		val := args[prop]
		if prop == targetProperty && c.Positional != nil {
			s, err := decodeString(prop, val)
			if err != nil {
				return nil, err
			}
			target = &s
			continue
		}
		name := prop
		if prop == targetProperty {
			if _, both := args[workloadProperty]; both {
				return nil, fmt.Errorf("arguments %q and %q name the same parameter for tool %s; pass only %q",
					targetProperty, workloadProperty, c.MCPName, workloadProperty)
			}
			if _, ok := specs[workloadProperty]; ok {
				name = workloadProperty
			}
		}
		spec, ok := specs[name]
		if !ok {
			return nil, unknownArgument(c, prop)
		}
		switch spec.Type {
		case emit.FlagBool:
			var b bool
			if err := json.Unmarshal(val, &b); err != nil {
				return nil, fmt.Errorf("argument %q must be a boolean", prop)
			}
			out = append(out, fmt.Sprintf("--%s=%t", spec.Name, b))
		case emit.FlagInt:
			var n int64
			if err := json.Unmarshal(val, &n); err != nil {
				return nil, fmt.Errorf("argument %q must be an integer", prop)
			}
			out = append(out, fmt.Sprintf("--%s=%d", spec.Name, n))
		default: // string, duration
			s, err := decodeString(prop, val)
			if err != nil {
				return nil, err
			}
			out = append(out, "--"+spec.Name+"="+s)
		}
	}
	if target != nil {
		out = append(out, *target)
	}
	return out, nil
}

// unknownArgument rejects a property the tool does not declare, and
// says what it should have been (#232).
//
// The no-argument errors on this surface already self-correct — "no
// scope: pass --namespace=<ns>, -A …, or --workload=…" tells the
// caller the move — and the unknown-argument path did not: it named
// the wrong parameter and stopped. Naming the nearest accepted
// property and then listing them all fixes every wrong guess in one
// round trip, not just the `target` one the alias above absorbs.
func unknownArgument(c checks.Command, prop string) error {
	accepted := acceptedProperties(c)
	msg := fmt.Sprintf("unknown argument %q for tool %s", prop, c.MCPName)
	if near := nearestProperty(prop, accepted); near != "" {
		msg += fmt.Sprintf("; did you mean %q?", near)
	}
	return fmt.Errorf("%s (accepts: %s)", msg, strings.Join(accepted, ", "))
}

// acceptedProperties lists a tool's schema properties, sorted.
func acceptedProperties(c checks.Command) []string {
	specs := schemaSpecs(c)
	out := make([]string, 0, len(specs)+1)
	for _, s := range specs {
		out = append(out, propertyName(s.Name))
	}
	if c.Positional != nil {
		out = append(out, targetProperty)
	}
	slices.Sort(out)
	return out
}

// nearestProperty picks the accepted property a wrong name most likely
// meant, or "" when nothing is close enough to suggest without
// guessing. A unique prefix match wins first ("form" → "format"), then
// a unique edit distance of at most two ("namespaces" → "namespace").
// Ambiguity suggests nothing: the full list is printed either way, and
// a wrong suggestion is worse than none.
func nearestProperty(prop string, accepted []string) string {
	prop = strings.ToLower(prop)
	var prefix []string
	for _, a := range accepted {
		if a != prop && strings.HasPrefix(a, prop) {
			prefix = append(prefix, a)
		}
	}
	if len(prefix) == 1 {
		return prefix[0]
	}

	best, bestDist, ties := "", 3, 0
	for _, a := range accepted {
		switch d := editDistance(prop, a); {
		case d < bestDist:
			best, bestDist, ties = a, d, 1
		case d == bestDist:
			ties++
		}
	}
	if ties != 1 {
		return ""
	}
	return best
}

// editDistance is Levenshtein distance, two rows rather than a full
// matrix. Property names are short; this runs once, on an error path.
func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}

func decodeString(prop string, val json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(val, &s); err != nil {
		return "", fmt.Errorf("argument %q must be a string", prop)
	}
	return s, nil
}
