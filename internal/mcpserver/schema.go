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
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
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

// workloadProperty is the scoping property workload-scoped tools declare
// (the §4.2 --workload common flag). targetProperty is the property name
// on the tools that take a positional instead, and clients generalize it
// to workload-scoped tools — see argv (#232).
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

// serverScopedFlags are CLI flags that name host files or select a
// cluster credential context. On the MCP surface those belong to the
// operator starting `lookout mcp` (WithSelection, WithExemptions,
// WithStore), never to an untrusted model client: they are omitted
// from inputSchema and rejected by argv if sent.
var serverScopedFlags = map[string]bool{
	"kubeconfig":     true,
	"kubeconfig-dir": true,
	"context":        true,
	"contexts":       true,
	"concurrency":    true,
	"google-auth":    true,
	"exemptions":     true,
	"store":          true,
	"report":         true,
}

// IsServerScopedFlag reports whether flag is stripped from the MCP
// tool schema because it belongs to the server operator rather than
// the MCP caller.
func IsServerScopedFlag(flag string) bool {
	return serverScopedFlags[flag]
}

// schemaSpecs returns the §4.2 common flags applicable to c (plus, for
// graph-backed commands, the §6.6 history flags) followed by the
// command's own flags, minus serverScopedFlags and any common scope
// flags c declares in InapplicableCommonFlags (#867): the complete
// flag surface behind a tool's schema, in the order the surfaces
// document them.
func schemaSpecs(c checks.Command) []emit.FlagSpec {
	raw := c.CommonFlags()
	if c.GraphBacked {
		raw = append(raw, emit.GraphHistoryFlags()...)
	}
	raw = append(raw, c.Flags...)
	specs := make([]emit.FlagSpec, 0, len(raw))
	for _, s := range raw {
		if serverScopedFlags[s.Name] || !c.AcceptsCommonFlag(s.Name) {
			continue
		}
		specs = append(specs, s)
	}
	return specs
}

// inputSchema derives a tool's JSON schema mechanically from the
// command's FlagSpecs and Positional declaration — the same metadata
// that generates --help, so the two surfaces cannot drift (§4.4.3).
// Every property is optional: omitted properties get the flag
// defaults, with one amended MCP-only exception (DESIGN.md §4.3
// amendment, #867) — commands declaring MCPDefaultAllNamespaces
// default all_namespaces to true when no scope property is supplied,
// while CLI flag defaults stay unchanged.
func inputSchema(c checks.Command) *jsonschema.Schema {
	props := map[string]*jsonschema.Schema{}
	for _, s := range schemaSpecs(c) {
		props[propertyName(s.Name)] = flagSchema(c, s)
	}
	if p := c.Positional; p != nil {
		props[targetProperty] = &jsonschema.Schema{
			Type:        "string",
			Description: p.Meta + " — " + mcpWord(c, p.Doc),
		}
	}
	return &jsonschema.Schema{Type: "object", Properties: props}
}

// flagSchema maps one FlagSpec to its property schema. The FlagType
// set is closed by design (pkg/emit): string→string, bool→boolean,
// int→integer, duration→string with a Go-duration pattern note.
func flagSchema(c checks.Command, s emit.FlagSpec) *jsonschema.Schema {
	desc := mcpWord(c, s.Help)
	if c.MCPDefaultAllNamespaces && s.Name == "A" {
		if c.AcceptsCommonFlag("workload") {
			desc = "scan all namespaces (defaults to true on MCP when namespace and workload are omitted; pass all_namespaces=false to disable)"
		} else {
			desc = "scan all namespaces (defaults to true on MCP when namespace is omitted; pass all_namespaces=false to disable)"
		}
	}
	out := &jsonschema.Schema{Description: desc}
	switch s.Type {
	case emit.FlagBool:
		out.Type = "boolean"
	case emit.FlagInt:
		out.Type = "integer"
	case emit.FlagDuration:
		out.Type = "string"
		out.Pattern = durationPattern
		out.Description = desc + ` (Go duration, e.g. "90s", "5m")`
	default: // emit.FlagString; specs are validated at registration
		out.Type = "string"
	}
	out.Default = defaultJSON(s)
	if c.MCPDefaultAllNamespaces && s.Name == "A" {
		out.Default = json.RawMessage("true")
	}
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
// (§4.4.1): the when-to-use Summary first (with CLI flag tokens
// rendered as MCP JSON property names), then any MCP default-scope
// note, then the §4.2 output contract as the tool result's shape, then
// the compact output-field glossary. All of it comes from the same
// Command metadata as --help.
func toolDescription(c checks.Command) string {
	var b strings.Builder
	b.WriteString(mcpWord(c, c.Summary))
	if c.MCPDefaultAllNamespaces {
		if c.AcceptsCommonFlag("workload") {
			b.WriteString("\nDefault scope on MCP: all_namespaces=true when namespace and workload are omitted.")
		} else {
			b.WriteString("\nDefault scope on MCP: all_namespaces=true when namespace is omitted.")
		}
	}
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
			b.WriteString(f.Name + " — " + mcpWord(c, f.Doc))
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

	var (
		out              []string
		target           *string
		hasNamespace     bool
		hasAllNamespaces bool
		hasWorkload      bool
	)
	for _, prop := range props {
		val := args[prop]
		isNull := bytes.Equal(bytes.TrimSpace(val), []byte("null"))
		if prop == targetProperty && c.Positional != nil {
			if isNull {
				continue
			}
			s, err := decodeString(prop, val)
			if err != nil {
				return nil, err
			}
			target = &s
			continue
		}
		name := prop
		if prop == targetProperty {
			if _, ok := specs[workloadProperty]; ok {
				if isNull {
					continue
				}
				if w, both := args[workloadProperty]; both && !bytes.Equal(bytes.TrimSpace(w), []byte("null")) {
					return nil, fmt.Errorf("arguments %q and %q name the same parameter for tool %s; pass only %q",
						targetProperty, workloadProperty, c.MCPName, workloadProperty)
				}
				name = workloadProperty
			}
		}
		spec, ok := specs[name]
		if !ok {
			return nil, unknownArgument(c, prop)
		}
		if isNull {
			continue
		}
		switch spec.Type {
		case emit.FlagBool:
			var b bool
			if err := json.Unmarshal(val, &b); err != nil {
				return nil, fmt.Errorf("argument %q must be a boolean", prop)
			}
			if spec.Name == "A" {
				hasAllNamespaces = true
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
			if spec.Name == "namespace" && s != "" {
				hasNamespace = true
			}
			if spec.Name == "workload" && s != "" {
				hasWorkload = true
			}
			out = append(out, "--"+spec.Name+"="+s)
		}
	}
	if c.MCPDefaultAllNamespaces && !hasAllNamespaces && !hasNamespace && !hasWorkload {
		out = append(out, "--A=true")
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

var (
	// flagParseErrRe matches Go flag.FlagSet parse diagnostics such as
	// `invalid value "x" for flag -timeout: parse error`.
	flagParseErrRe = regexp.MustCompile(`\bfor flag -([A-Za-z][a-z0-9-]*)`)
	// longFlagTokenRe matches CLI long flags such as --namespace, --min-severity, or --A.
	longFlagTokenRe = regexp.MustCompile(`--([A-Za-z][a-z0-9-]*)`)
)

// mcpFlagMap returns CLI flag name -> MCP JSON property name for every
// non-server-scoped flag belonging to c (common flags, graph history
// flags when GraphBacked, and c's own flags). Server-scoped flags such
// as --exemptions and --store stay CLI-worded because they belong to
// `lookout mcp`, not to tool call arguments.
func mcpFlagMap(c checks.Command) map[string]string {
	flags := map[string]string{}
	add := func(specs []emit.FlagSpec) {
		for _, s := range specs {
			if serverScopedFlags[s.Name] {
				continue
			}
			flags[s.Name] = propertyName(s.Name)
		}
	}
	add(c.CommonFlags())
	if c.GraphBacked {
		add(emit.GraphHistoryFlags())
	}
	add(c.Flags)
	return flags
}

func isFlagIdentByte(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b == '-'
}

// mcpWord rewrites CLI flag syntax (-A, --namespace, --workload, and
// c's own --<flag> names) to MCP JSON property names on the MCP
// surface (#867). Flags outside c's MCP flag set (such as server-scoped
// --exemptions/--store or external kubectl flags) and backticked
// `lookout ...` CLI command snippets are left untouched; CLI `--help`
// pointers in prose are omitted.
func mcpWord(c checks.Command, text string) string {
	if !strings.ContainsRune(text, '-') {
		return text
	}
	text = strings.ReplaceAll(text, "; see the fidelity gap in --help", "")
	// Preserve backticked `lookout ...` CLI invocations verbatim so
	// command snippets are never mangled into hybrid CLI/MCP syntax.
	if strings.Contains(text, "`lookout ") {
		var b strings.Builder
		b.Grow(len(text))
		rest := text
		for {
			start := strings.Index(rest, "`lookout ")
			if start < 0 {
				b.WriteString(mcpWordSegment(c, rest))
				break
			}
			end := strings.IndexByte(rest[start+1:], '`')
			if end < 0 {
				b.WriteString(mcpWordSegment(c, rest))
				break
			}
			end += start + 2
			b.WriteString(mcpWordSegment(c, rest[:start]))
			b.WriteString(rest[start:end])
			rest = rest[end:]
		}
		return b.String()
	}
	return mcpWordSegment(c, text)
}

func mcpWordSegment(c checks.Command, text string) string {
	if !strings.ContainsRune(text, '-') {
		return text
	}
	flags := mcpFlagMap(c)

	// 1. Rewrite Go flag.FlagSet parse errors (`for flag -timeout`).
	text = flagParseErrRe.ReplaceAllStringFunc(text, func(m string) string {
		name := strings.TrimPrefix(m, "for flag -")
		if prop, ok := flags[name]; ok {
			return fmt.Sprintf("for argument %q", prop)
		}
		return m
	})

	// 2. Rewrite long flags (`--namespace`, `--min-severity`, `--A`) on
	// identifier boundaries so substrings like `--node-mode` or
	// `--scale-down-utilization-threshold` are never partially rewritten.
	if idx := longFlagTokenRe.FindAllStringSubmatchIndex(text, -1); len(idx) > 0 {
		var b strings.Builder
		b.Grow(len(text))
		last := 0
		for _, loc := range idx {
			start, end := loc[0], loc[1]
			nameStart, nameEnd := loc[2], loc[3]
			if start > 0 && isFlagIdentByte(text[start-1]) {
				continue
			}
			if end < len(text) && isFlagIdentByte(text[end]) {
				continue
			}
			prop, ok := flags[text[nameStart:nameEnd]]
			if !ok {
				continue
			}
			b.WriteString(text[last:start])
			b.WriteString(prop)
			last = end
		}
		if last > 0 {
			b.WriteString(text[last:])
			text = b.String()
		}
	}

	// 3. Rewrite standalone `-A` tokens to `all_namespaces`.
	if strings.Contains(text, "-A") {
		var b strings.Builder
		b.Grow(len(text) + 16)
		for i := 0; i < len(text); {
			if i+2 <= len(text) && text[i] == '-' && text[i+1] == 'A' {
				prevOK := i == 0 || !isFlagIdentByte(text[i-1])
				nextOK := i+2 == len(text) || !isFlagIdentByte(text[i+2])
				if prevOK && nextOK {
					b.WriteString("all_namespaces")
					i += 2
					continue
				}
			}
			b.WriteByte(text[i])
			i++
		}
		text = b.String()
	}

	return text
}

// mcpUsageDiagnostics formats ExitUsage stderr for the MCP surface:
// sanitized via emit.MaskString, stripped of any CLI `--help` trailer,
// and worded with MCP JSON property names instead of CLI flags (#867).
func mcpUsageDiagnostics(c checks.Command, stderr *bytes.Buffer) string {
	s := strings.TrimSpace(stderr.String())
	if s == "" {
		return "lookout " + c.Name + ": failed without diagnostics"
	}
	helpTrailer := fmt.Sprintf("\nRun 'lookout %s --help' for usage.", c.Name)
	s = strings.TrimSuffix(s, helpTrailer)
	return mcpWord(c, emit.MaskString(s))
}
