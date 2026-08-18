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
	"time"

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
	// Kinds is the ledger of finding kinds this command may emit
	// (issue #278). Contract tests fail a command that emits an
	// undeclared kind, or one at a severity it did not declare.
	Kinds []KindField
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
	// TimeoutDefault overrides the shared --timeout default
	// (emit.DefaultTimeout) for this command. Zero — every command
	// but `scan` — keeps the shared value. A composition that runs a
	// dozen checks under one invocation cannot live inside the
	// single-check budget; declaring the larger default here means
	// --help, the MCP schema, and the reference page all show the
	// number that is actually in force.
	TimeoutDefault time.Duration
	// Writes marks a command that MUTATES state rather than only
	// reading it (e.g. `triage status --status=...`, the §9.4
	// triage-status upsert). The MCP surface advertises such a tool
	// with ReadOnlyHint:false so a convention-following client does
	// not auto-approve it as a read (issue #105). Plain additive
	// field: the zero value (false) keeps every existing read-path
	// command exactly as it was.
	Writes bool
}

// OutputField documents one Details key a command may emit.
type OutputField struct {
	Name string
	Doc  string
}

// KindField documents one finding kind a command may emit.
//
// The kind is the most load-bearing string lookout writes. `findings
// diff` keys transitions on it, an operator writes an exemption
// against it, the sentinel routes on the signal it converts to, and a
// downstream consumer switches on it — so it is a public contract
// whether or not anything declares it. Before this ledger nothing
// did: a typo was a silent new kind, a rename broke every consumer
// with nothing failing in CI, and a new contributor could learn the
// vocabulary only by grep.
//
// Declaring kinds with the command is the same pattern Output already
// uses, applied to a second field: one declaration, validated at
// registration, enforced against emitted output by the §13 contract
// tests, and rendered into every documentation surface.
type KindField struct {
	// Name is the kind, "<owner>.<slug>" in lowercase snake_case
	// (e.g. "pod.crashloop"). The owner prefix names the family of
	// claims, not the command: `triage delta` and `health` both emit
	// pod.* on purpose, because a crashloop is a crashloop whichever
	// command found it.
	Name string
	// Doc is the one-line claim — what is true of the subject when
	// this kind appears. Written for a reader deciding whether to
	// act, not as a restatement of the name.
	Doc string
	// Severity is every severity this kind is emitted at, worst
	// first. Most kinds have exactly one; a kind whose level depends
	// on a threshold ("expiring" vs "expired", a saturation band)
	// declares each, and emitting one it did not declare fails the
	// contract test. Declaring the set rather than a single worst
	// value is what lets an operator tell "this can page you" from
	// "this is always informational".
	Severity []string
}

// Kind builds a KindField. It exists because the severity list reads
// far better variadic than as a slice literal, and a ledger is read
// far more often than it is written:
//
//	checks.Kind("edge.cert_expiring", "a TLS certificate expires inside the --cert-warn window", emit.SeverityWarning)
func Kind(name, doc string, severity ...string) KindField {
	return KindField{Name: name, Doc: doc, Severity: severity}
}

// CloudUnavailableKind is the ledger entry for the §2 degradation
// record every cloud-backed command emits when the provider
// capability it needs is not there. It lives here, rather than beside
// an emitter, because there is no single emitter: five packages
// hand-roll the same finding. One declaration keeps them saying the
// same thing.
func CloudUnavailableKind() KindField {
	return Kind("cloud.unavailable",
		"the cloud capability this check needs is unavailable, so nothing was examined — an explicit degradation record, never silence (§2, §11)",
		emit.SeverityInfo)
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

// CommonFlags returns the §4.2 common flags as this command accepts
// them — emit.CommonFlags with the --timeout default replaced when
// the command declares its own. Every surface that renders the common
// flag table (--help, the skill and site reference pages, the MCP
// input schema) goes through here so none of them can advertise a
// default the runner will not use.
func (c Command) CommonFlags() []emit.FlagSpec {
	return emit.CommonFlagsWith(c.TimeoutDefault)
}

// DefaultFlags returns the FlagValues this command sees when invoked
// with no flags: its own specs plus the common (and, when graph-
// backed, the §6.6 history) flags, each at its declared default. It
// is how a composition builds the child emit.Invocation it hands to
// Run — see emit.DefaultFlags.
func (c Command) DefaultFlags(overrides ...emit.Field) (emit.FlagValues, error) {
	specs := c.CommonFlags()
	if c.GraphBacked {
		specs = append(specs, emit.GraphHistoryFlags()...)
	}
	return emit.DefaultFlags(append(specs, c.Flags...), overrides...)
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
	"audit":    "best-practice posture: the absence of a safety net around a workload or cluster that is currently healthy — a different claim from the incident groups, which is why it is a different group",
	"triage":   "incident reads: everything abnormal, condensed logs/events, blast radius, what changed",
	"findings": "run-to-run finding state: diff two scans into transitions (new/ongoing/escalated/resolved), ack a subject for a window",
	"state":    "dependency + configuration verification: edges, webhooks, workload identity, volumes",
	"stab":     "stability reads: GitOps drift, node-drain blockers",
	"perf":     "control-plane and startup performance via Cloud Monitoring query packs",
	"cloud":    "GCP-side reads: stockouts, orphaned resources, IP space, quota",
	"net":      "active DNS/TCP/HTTP probes from inside the cluster",
}

// GroupSummary returns the listing summary for a §4.1 group.
func GroupSummary(group string) string { return groupDocs[group] }

var (
	namePattern     = regexp.MustCompile(`^[a-z][a-z0-9-]*( [a-z][a-z0-9-]*)?$`)
	mcpNamePattern  = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	outKeyPattern   = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	kindNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$`)
)

// validateKinds checks the finding-kind ledger: a well-formed
// <owner>.<slug> name, a real one-line claim, and a non-empty
// worst-first severity set drawn from the three levels.
func (c Command) validateKinds() error {
	if len(c.Kinds) == 0 {
		return fmt.Errorf("command %q: no Kinds declared — every command must name the finding kinds it emits (issue #278); "+
			"add checks.Kind(<kind>, <one-line claim>, <severities…>) for each", c.Name)
	}
	seen := map[string]bool{}
	for _, k := range c.Kinds {
		if !kindNamePattern.MatchString(k.Name) {
			return fmt.Errorf("command %q: finding kind %q must be <owner>.<slug> in lowercase snake_case", c.Name, k.Name)
		}
		if seen[k.Name] {
			return fmt.Errorf("command %q: finding kind %q declared twice", c.Name, k.Name)
		}
		seen[k.Name] = true
		if k.Doc == "" || strings.ContainsRune(k.Doc, '\n') {
			return fmt.Errorf("command %q: finding kind %q must have a one-line doc", c.Name, k.Name)
		}
		if len(k.Severity) == 0 {
			return fmt.Errorf("command %q: finding kind %q declares no severity", c.Name, k.Name)
		}
		for i, s := range k.Severity {
			if !emit.ValidSeverity(s) {
				return fmt.Errorf("command %q: finding kind %q declares severity %q, which is not one of %s",
					c.Name, k.Name, s, strings.Join(emit.Severities(), "/"))
			}
			if i > 0 && emit.SeverityRank(k.Severity[i-1]) >= emit.SeverityRank(s) {
				return fmt.Errorf("command %q: finding kind %q lists severities %s — they must be worst first and distinct",
					c.Name, k.Name, strings.Join(k.Severity, ","))
			}
		}
	}
	return nil
}

// EmitsKind reports whether the command declared kind, and at what
// severities. Consumers of the ledger (the contract tests, the
// generated glossary) go through here rather than scanning Kinds.
func (c Command) EmitsKind(kind string) (KindField, bool) {
	for _, k := range c.Kinds {
		if k.Name == kind {
			return k, true
		}
	}
	return KindField{}, false
}

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
	if c.TimeoutDefault < 0 {
		return fmt.Errorf("command %q: negative TimeoutDefault %s", c.Name, c.TimeoutDefault)
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
	if err := c.validateKinds(); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, e := range emit.EnvelopeFields() {
		seen[e] = true
	}
	for _, n := range emit.SummaryNoteFields() {
		seen[n] = true
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
//
// Stdin is left at its zero value (the runner defaults it to
// os.Stdin); callers that pipe a report in — checktest, and any
// future in-process host — set it on the returned config.
func (c Command) RunConfig(stdout, stderr io.Writer) emit.RunConfig {
	maxArgs := 0
	if c.Positional != nil {
		maxArgs = 1
	}
	return emit.RunConfig{
		Name:           "lookout " + c.Name,
		Flags:          c.Flags,
		Check:          c.Run,
		Help:           c.Help(),
		MaxArgs:        maxArgs,
		GraphBacked:    c.GraphBacked,
		TimeoutDefault: c.TimeoutDefault,
		Stdout:         stdout,
		Stderr:         stderr,
	}
}
