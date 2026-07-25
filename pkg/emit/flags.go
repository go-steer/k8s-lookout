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

package emit

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// FlagType is the declared type of a FlagSpec. The set is small and
// closed on purpose: every type maps mechanically to a JSON-schema
// type for the MCP surface (string→string, bool→boolean,
// int→integer, duration→string with a duration pattern), so command
// metadata stays the single source both surfaces generate from
// (§4.4.3).
type FlagType string

const (
	FlagString   FlagType = "string"
	FlagBool     FlagType = "bool"
	FlagInt      FlagType = "int"
	FlagDuration FlagType = "duration"
)

// FlagSpec declares one flag: the data half of flag parsing. The
// runner turns specs into a flag.FlagSet; pkg/checks turns the same
// specs into --help text and (in the mcp change) JSON schemas.
type FlagSpec struct {
	Name string
	Type FlagType
	// Default is the literal default value ("" means the type's
	// zero). It must parse under Type — registration fails
	// otherwise, so a typo'd default is caught at init, not at
	// first use.
	Default string
	Help    string
}

// CommonFlags returns the §4.2 flags every command accepts. They are
// parsed once by Run into a Scope; checks never see them as raw
// flags.
func CommonFlags() []FlagSpec {
	return []FlagSpec{
		{Name: "namespace", Type: FlagString, Default: "", Help: "limit the scan to one namespace"},
		{Name: "A", Type: FlagBool, Default: "false", Help: "scan all namespaces"},
		{Name: "workload", Type: FlagString, Default: "", Help: "target one workload as <Kind>/<namespace>/<name>, e.g. Deployment/prod/api"},
		{Name: "since", Type: FlagDuration, Default: "0s", Help: "how far back to look (0 = command default)"},
		{Name: "format", Type: FlagString, Default: "logfmt", Help: "output format: logfmt|json (one record per line either way)"},
		{Name: "timeout", Type: FlagDuration, Default: "10s", Help: "abort the invocation after this long (exit 1)"},
	}
}

// GraphHistoryFlags returns the §4.2/§6.6 point-in-time flags,
// registered ONLY for commands that declare themselves graph-backed
// (RunConfig.GraphBacked / checks.Command.GraphBacked): live-only
// commands reject --at as an unknown flag rather than silently
// ignoring a time the caller cares about. --at without --store is a
// usage error: history is a watch-path feature, and a one-shot CLI
// invocation can only serve point-in-time queries from a sentinel's
// store (§6.6) — otherwise commands answer live-only and say so in
// their summary line.
func GraphHistoryFlags() []FlagSpec {
	return []FlagSpec{
		{Name: "at", Type: FlagString, Default: "", Help: "answer as of this instant instead of live: RFC3339 (2026-07-25T10:00:00Z) or a duration ago (20m). Requires --store."},
		{Name: "store", Type: FlagString, Default: "", Help: "path to a sentinel's SQLite store (its --store file); source for --at point-in-time topology"},
	}
}

// ParseAt parses the --at flag value: empty means live (zero time),
// a Go duration means that long BEFORE now ("20m" = 20 minutes ago),
// anything else must be RFC3339.
func ParseAt(s string, now time.Time) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		if d < 0 {
			return time.Time{}, fmt.Errorf("--at duration must not be negative (%q means %q ago)", s, s)
		}
		return now.Add(-d), nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("--at must be RFC3339 (2026-07-25T10:00:00Z) or a duration ago (20m), got %q", s)
	}
	return t, nil
}

// registerSpecs adds specs to fs, parsing each declared default.
func registerSpecs(fs *flag.FlagSet, specs []FlagSpec) error {
	for _, s := range specs {
		if fs.Lookup(s.Name) != nil {
			return fmt.Errorf("flag --%s declared twice (collides with a common flag?)", s.Name)
		}
		switch s.Type {
		case FlagString:
			fs.String(s.Name, s.Default, s.Help)
		case FlagBool:
			d, err := parseBoolDefault(s.Default)
			if err != nil {
				return fmt.Errorf("flag --%s: %w", s.Name, err)
			}
			fs.Bool(s.Name, d, s.Help)
		case FlagInt:
			d, err := parseIntDefault(s.Default)
			if err != nil {
				return fmt.Errorf("flag --%s: %w", s.Name, err)
			}
			fs.Int(s.Name, d, s.Help)
		case FlagDuration:
			d, err := parseDurationDefault(s.Default)
			if err != nil {
				return fmt.Errorf("flag --%s: %w", s.Name, err)
			}
			fs.Duration(s.Name, d, s.Help)
		default:
			return fmt.Errorf("flag --%s: unknown type %q", s.Name, s.Type)
		}
	}
	return nil
}

func parseBoolDefault(s string) (bool, error) {
	if s == "" {
		return false, nil
	}
	return strconv.ParseBool(s)
}

func parseIntDefault(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.Atoi(s)
}

func parseDurationDefault(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

// ValidateSpecs checks a command's flag declarations without
// building a runner: names must be flag-shaped, types known,
// defaults parseable, and nothing may shadow a §4.2 common flag.
// pkg/checks calls this at registration so a bad spec fails at init,
// in tests, not on an operator's terminal.
func ValidateSpecs(specs []FlagSpec) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	if err := registerSpecs(fs, CommonFlags()); err != nil {
		return err
	}
	return registerSpecs(fs, specs)
}

// ValidateGraphBackedSpecs is ValidateSpecs for graph-backed
// commands: the §6.6 history flags are reserved too, so a command
// cannot shadow --at or --store.
func ValidateGraphBackedSpecs(specs []FlagSpec) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	if err := registerSpecs(fs, CommonFlags()); err != nil {
		return err
	}
	if err := registerSpecs(fs, GraphHistoryFlags()); err != nil {
		return err
	}
	return registerSpecs(fs, specs)
}

// FlagValues gives checks typed access to their command-specific
// flags after parsing. Lookups of undeclared names or with the wrong
// type panic: both are programmer errors a command's own unit test
// hits immediately.
type FlagValues struct {
	fs *flag.FlagSet
}

func (v FlagValues) String(name string) string          { return v.get(name).(string) }
func (v FlagValues) Bool(name string) bool              { return v.get(name).(bool) }
func (v FlagValues) Int(name string) int                { return v.get(name).(int) }
func (v FlagValues) Duration(name string) time.Duration { return v.get(name).(time.Duration) }

func (v FlagValues) get(name string) any {
	f := v.fs.Lookup(name)
	if f == nil {
		panic(fmt.Sprintf("flag --%s was not declared in the command's FlagSpecs", name))
	}
	return f.Value.(flag.Getter).Get()
}

// Scope is the parsed form of the §4.2 common flags, handed to every
// check. It carries exactly what the caller asked for; interpreting
// an empty scope (whole cluster vs. a command-specific default) is
// the check's decision, documented per command in its output
// glossary.
type Scope struct {
	// Namespace restricts to one namespace; empty means the
	// command's default scope. Mutually exclusive with
	// AllNamespaces (enforced at parse time).
	Namespace string
	// AllNamespaces is the -A flag.
	AllNamespaces bool
	// Workload targets one workload; zero value if --workload was
	// not given.
	Workload WorkloadRef
	// Since bounds the lookback window; 0 means the command's
	// default.
	Since time.Duration

	// At is the resolved --at instant for graph-backed commands
	// (§6.6): answer as of this time instead of live. Zero means
	// live. Only ever non-zero together with Store — Run rejects
	// --at without --store as a usage error.
	At time.Time
	// Store is the --store path (a sentinel's SQLite store) backing
	// At. May be set without At (the command then answers live and
	// may use the store for other reads).
	Store string
}

// WorkloadRef identifies one workload, parsed from
// --workload=<Kind>/<namespace>/<name>.
type WorkloadRef struct {
	Kind      string
	Namespace string
	Name      string
}

// IsZero reports whether no workload was targeted.
func (r WorkloadRef) IsZero() bool { return r == WorkloadRef{} }

func (r WorkloadRef) String() string {
	if r.IsZero() {
		return ""
	}
	return r.Kind + "/" + r.Namespace + "/" + r.Name
}

// ParseWorkload parses the --workload flag value. Empty input is a
// valid "no target".
func ParseWorkload(s string) (WorkloadRef, error) {
	if s == "" {
		return WorkloadRef{}, nil
	}
	parts := strings.Split(s, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return WorkloadRef{}, fmt.Errorf("invalid workload %q (want <Kind>/<namespace>/<name>)", s)
	}
	return WorkloadRef{Kind: parts[0], Namespace: parts[1], Name: parts[2]}, nil
}
