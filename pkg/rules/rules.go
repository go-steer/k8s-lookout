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

// Package rules evaluates posture checks written as CEL expressions in
// a YAML rule file, natively, against the same objects the Go
// detectors read, and emits the same emit.Finding records through the
// same registry, sanitizer and fingerprint path.
//
// This is the second arm of the CEL-rules pilot (see the architecture
// review of 2026-10-21): the question it answers is whether the
// predicate-only posture checks — template arithmetic, field presence,
// one or two joins — can be authored as data without giving up any of
// the guarantees a Go check has. The built-in rules ship compiled into
// the binary (//go:embed); a rule file on disk is an authoring format,
// never a runtime dependency.
//
// A rule is:
//
//	id:        rollout_drops_traffic          # file-local name
//	kind:      audit.rollout_drops_traffic    # registered finding kind
//	subjects:  [Deployment, StatefulSet, DaemonSet]
//	reason:    NoPreStopOnServingContainer
//	severity:  warning                        # or severity_expr
//	locals:                                   # named sub-expressions, in order
//	  - name: svcs
//	    expr: serving(services, object)
//	match:     svcs.size() > 0 && ...
//	message:   '"..." + string(svcs.size()) + "..."'
//	details:
//	  - name: services
//	    expr: string(svcs.size())
//	    doc:  Services routing to the workload ...
//
// Expressions see `object` (the workload as an unstructured map),
// `kind`, `services` (every Service in the namespace, as maps) and
// `thresholds` (the knobs), plus the helper functions in env.go. Every
// program runs under a cost limit; a rule that exceeds it is an error
// the command reports, never a finding silently skipped.
package rules

import (
	_ "embed"
	"fmt"
	"strings"

	"sigs.k8s.io/yaml"
)

// Rule is one check as the YAML file declares it.
type Rule struct {
	ID           string   `json:"id"`
	Kind         string   `json:"kind"`
	Doc          string   `json:"doc"`
	Subjects     []string `json:"subjects"`
	Reason       string   `json:"reason"`
	Severity     string   `json:"severity"`
	SeverityExpr string   `json:"severity_expr"`
	Severities   []string `json:"severities"`
	Locals       []Local  `json:"locals"`
	Match        string   `json:"match"`
	Message      string   `json:"message"`
	Details      []Detail `json:"details"`
}

// Local is a named sub-expression later expressions may refer to.
type Local struct {
	Name string `json:"name"`
	Expr string `json:"expr"`
}

// Detail is one key=value field a finding carries, with its glossary doc.
type Detail struct {
	Name string `json:"name"`
	Expr string `json:"expr"`
	Doc  string `json:"doc"`
}

// Set is a parsed rule file.
type Set struct {
	// Thresholds are the knobs the rules read as `thresholds.<name>`:
	// the name lists and numbers a Go check would keep in a package
	// var. Declared in the file so a rule and its constants travel
	// together.
	Thresholds map[string]any `json:"thresholds"`
	// Locals are shared sub-expressions every rule may refer to —
	// the Services selecting this workload, say — evaluated once per
	// subject rather than once per rule.
	Locals []Local `json:"locals"`
	Rules  []Rule  `json:"rules"`
}

//go:embed builtin/audit.yaml
var builtinYAML []byte

// Builtin returns the rules compiled into the binary. It panics on a
// malformed file, at init, like a bad checks.Command declaration: a
// broken built-in should fail every test in the tree.
func Builtin() *Set {
	set, err := Load(builtinYAML)
	if err != nil {
		panic("rules: built-in rule file: " + err.Error())
	}
	return set
}

// Load parses and validates a rule file.
func Load(b []byte) (*Set, error) {
	var set Set
	if err := yaml.Unmarshal(b, &set); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for i, r := range set.Rules {
		where := fmt.Sprintf("rules[%d] (%s)", i, r.ID)
		switch {
		case r.ID == "":
			return nil, fmt.Errorf("rules[%d]: id is required", i)
		case seen[r.ID]:
			return nil, fmt.Errorf("%s: duplicate id", where)
		case !strings.HasPrefix(r.Kind, "audit."):
			return nil, fmt.Errorf("%s: kind must be audit.<slug>, got %q", where, r.Kind)
		case len(r.Subjects) == 0:
			return nil, fmt.Errorf("%s: subjects is required", where)
		case r.Reason == "":
			return nil, fmt.Errorf("%s: reason is required", where)
		case r.Match == "":
			return nil, fmt.Errorf("%s: match is required", where)
		case r.Message == "":
			return nil, fmt.Errorf("%s: message is required", where)
		case r.Severity == "" && r.SeverityExpr == "":
			return nil, fmt.Errorf("%s: severity or severity_expr is required", where)
		case r.Doc == "":
			return nil, fmt.Errorf("%s: doc is required (it is the kind's ledger entry)", where)
		}
		seen[r.ID] = true
		if len(r.Severities) == 0 {
			if r.Severity == "" {
				return nil, fmt.Errorf("%s: severities must list the values severity_expr can produce", where)
			}
			set.Rules[i].Severities = []string{r.Severity}
		}
		for _, d := range r.Details {
			if d.Name == "" || d.Expr == "" || d.Doc == "" {
				return nil, fmt.Errorf("%s: every detail needs name, expr and doc", where)
			}
		}
	}
	return &set, nil
}
