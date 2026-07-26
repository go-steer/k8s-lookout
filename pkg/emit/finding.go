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

// Package emit implements the §4.2 output contract shared by every
// read-path command: findings on stdout as flat, ordered key=value
// records (logfmt by default, one JSON object per line with
// --format=json), a mandatory terminating summary line
// (`scanned=<n> findings=<n> elapsed=<d>`), diagnostics on stderr
// only, and exit codes 0 data / 1 runtime / 2 usage.
//
// Zero nominal state: healthy resources emit nothing; empty fields
// are omitted from records; an empty scan is still explicit via the
// summary line, never implicit.
package emit

import (
	"fmt"
	"regexp"
)

// Severity* are the values checks stamp on Finding.Severity. They
// match the sentinel's §7.7 routing levels ("critical" opens a
// dedicated incident session; "warning" goes to the watchboard) so a
// scan finding and a sentinel signal for the same symptom compare
// directly.
const (
	SeverityInfo     = "info"
	SeverityWarning  = "warning"
	SeverityCritical = "critical"
)

// Finding is one abnormal observation: a flat, ordered key=value
// record. The named fields mirror the §8 Signal schema
// (kind_of_object, reason, message, …) because read-path findings
// are Signals with source:"scan" — keeping the names aligned makes
// that conversion a field copy, not a mapping table.
//
// Empty fields are omitted on every surface (zero nominal state);
// Kind is the only required field.
type Finding struct {
	// Kind classifies the finding, dot-namespaced by the owning
	// check (e.g. "pod.crashloop", "quota.exhausted").
	Kind string
	// Severity is one of the Severity* constants.
	Severity string
	// Namespace, KindOfObject, Name identify the subject resource.
	// KindOfObject keeps the §8 wire name kind_of_object to avoid
	// colliding with the finding's own Kind.
	Namespace    string
	KindOfObject string
	Name         string
	// Reason is the machine-matchable cause (CamelCase, mirroring
	// k8s Event.Reason where one exists); Message is the
	// human/agent-readable one-liner.
	Reason  string
	Message string
	// Fingerprint is the §8 incident-class hash
	// (docs/signal-schema-v1.md): set on scan findings that describe
	// a symptom class the sentinel could also push, via
	// engine.ScanFingerprint, so the push and pull paths dedupe on
	// one key. Empty (and omitted — zero nominal state) on findings
	// with no incident-class identity: scorecard lines, inventory
	// records, probe results.
	Fingerprint string
	// Details carries check-specific fields, emitted after the
	// named fields in declared order. Keys must match keyPattern
	// and be declared in the owning command's output glossary
	// (enforced by the §13 contract tests).
	Details []Field
}

// Field is one detail key=value pair. Values are always strings;
// checks format numbers/durations themselves so output is
// deterministic and golden-testable.
type Field struct {
	Key   string
	Value string
}

// keyPattern is the charset contract for every emitted key:
// lowercase snake_case starting with a letter. Keeping keys this
// boring means logfmt never needs key quoting and JSON property
// names are valid identifiers everywhere.
var keyPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// EnvelopeFields returns the finding keys owned by the envelope
// itself, in emission order. Contract tests treat these as
// implicitly declared for every command; only Details keys must
// appear in a command's output glossary.
func EnvelopeFields() []string {
	return []string{"kind", "severity", "namespace", "kind_of_object", "name", "reason", "message", "fingerprint"}
}

// pairs flattens the finding into its ordered key=value records,
// omitting empty values (zero nominal state applies to fields too).
func (f Finding) pairs() []Field {
	out := make([]Field, 0, 8+len(f.Details))
	add := func(k, v string) {
		if v != "" {
			out = append(out, Field{Key: k, Value: v})
		}
	}
	add("kind", f.Kind)
	add("severity", f.Severity)
	add("namespace", f.Namespace)
	add("kind_of_object", f.KindOfObject)
	add("name", f.Name)
	add("reason", f.Reason)
	add("message", f.Message)
	add("fingerprint", f.Fingerprint)
	for _, d := range f.Details {
		add(d.Key, d.Value)
	}
	return out
}

// validate rejects findings that would break the envelope: a missing
// kind (meaningless record) or a detail key outside the charset
// contract. Checks are internal callers, so this failing fast is a
// test-time bug catcher, not an operator-facing error path.
func (f Finding) validate() error {
	if f.Kind == "" {
		return fmt.Errorf("finding has no kind: %+v", f)
	}
	for _, d := range f.Details {
		if !keyPattern.MatchString(d.Key) {
			return fmt.Errorf("finding detail key %q does not match %s", d.Key, keyPattern)
		}
	}
	return nil
}
