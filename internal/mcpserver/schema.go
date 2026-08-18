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
		spec, ok := specs[prop]
		if !ok {
			return nil, fmt.Errorf("unknown argument %q for tool %s", prop, c.MCPName)
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

func decodeString(prop string, val json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(val, &s); err != nil {
		return "", fmt.Errorf("argument %q must be a string", prop)
	}
	return s, nil
}
