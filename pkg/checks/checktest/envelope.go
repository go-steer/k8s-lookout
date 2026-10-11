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

package checktest

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// ParsedFinding holds the structured envelope fields plus any additional
// key=value pairs parsed from a single emitted finding line.
type ParsedFinding struct {
	Kind         string
	Severity     string
	Namespace    string
	KindOfObject string
	Name         string
	Reason       string
	Message      string
	Fingerprint  string
	Fields       map[string]string
}

// ParsedOutput captures the structured findings and mandatory trailing summary
// line returned by a check invocation.
type ParsedOutput struct {
	Findings []ParsedFinding
	Scanned  int
	Reported int
	Elapsed  string
	Notes    map[string]string
}

// FindingMatch specifies targeted structural attributes of an expected or
// forbidden finding without coupling detection tests to prose message phrasing.
// Empty fields match any value.
type FindingMatch struct {
	Kind         string
	Namespace    string
	KindOfObject string
	Name         string
	Severity     string
	Reason       string
}

// Matches reports whether f satisfies every non-empty selector field in m.
func (m FindingMatch) Matches(f ParsedFinding) bool {
	if m.Kind != "" && f.Kind != m.Kind {
		return false
	}
	if m.Namespace != "" && f.Namespace != m.Namespace {
		return false
	}
	if m.KindOfObject != "" && f.KindOfObject != m.KindOfObject {
		return false
	}
	if m.Name != "" && f.Name != m.Name {
		return false
	}
	if m.Severity != "" && f.Severity != m.Severity {
		return false
	}
	if m.Reason != "" && f.Reason != m.Reason {
		return false
	}
	return true
}

func (m FindingMatch) isEmpty() bool {
	return m.Kind == "" &&
		m.Namespace == "" &&
		m.KindOfObject == "" &&
		m.Name == "" &&
		m.Severity == "" &&
		m.Reason == ""
}

// AssertEnvelopeContract executes c in both logfmt and JSON wire formats and
// enforces the full §4.2 / §6.5 wire envelope invariants:
//   - exit code 0 (data) and empty stderr across both encodings;
//   - schema, summary line (`scanned=N findings=N elapsed=D`), Output glossary,
//     and Kinds ledger compliance via Verify;
//   - secret redaction invariant (no raw private keys or unredacted credential flags);
//   - structural tuple parity between logfmt and JSON finding lines.
//
// Returns the parsed structured output so detection tests can assert positive
// and negative findings via AssertHasFinding / AssertNoFinding without coupling
// to verbatim prose snapshots (#654).
func AssertEnvelopeContract(t *testing.T, c checks.Command, args ...string) ParsedOutput {
	t.Helper()
	logfmtRes := Run(t, c, append([]string{"--format=logfmt"}, args...)...)
	if logfmtRes.Code != emit.ExitData {
		t.Fatalf("lookout %s (logfmt): exit=%d, want 0; stderr: %s", c.Name, logfmtRes.Code, logfmtRes.Stderr)
	}
	if logfmtRes.Stderr != "" {
		t.Errorf("lookout %s (logfmt): unexpected stderr diagnostics on exit 0: %q", c.Name, logfmtRes.Stderr)
	}
	if err := Verify(c, logfmtRes.Stdout, emit.FormatLogfmt); err != nil {
		t.Fatalf("lookout %s (logfmt): envelope contract failed: %v\nstdout:\n%s", c.Name, err, logfmtRes.Stdout)
	}
	assertNoUnredactedSecrets(t, c.Name, emit.FormatLogfmt, logfmtRes.Stdout)

	jsonRes := Run(t, c, append([]string{"--format=json"}, args...)...)
	if jsonRes.Code != emit.ExitData {
		t.Fatalf("lookout %s (json): exit=%d, want 0; stderr: %s", c.Name, jsonRes.Code, jsonRes.Stderr)
	}
	if jsonRes.Stderr != "" {
		t.Errorf("lookout %s (json): unexpected stderr diagnostics on exit 0: %q", c.Name, jsonRes.Stderr)
	}
	if err := Verify(c, jsonRes.Stdout, emit.FormatJSON); err != nil {
		t.Fatalf("lookout %s (json): envelope contract failed: %v\nstdout:\n%s", c.Name, err, jsonRes.Stdout)
	}
	assertNoUnredactedSecrets(t, c.Name, emit.FormatJSON, jsonRes.Stdout)

	parsedLogfmt := parseOutput(t, logfmtRes.Stdout, emit.FormatLogfmt)
	parsedJSON := parseOutput(t, jsonRes.Stdout, emit.FormatJSON)
	assertWireTupleParity(t, c.Name, parsedLogfmt, parsedJSON)

	return parsedLogfmt
}

// AssertHasFinding asserts that out contains at least one finding matching want
// and returns the first matched ParsedFinding.
func AssertHasFinding(t *testing.T, out ParsedOutput, want FindingMatch) ParsedFinding {
	t.Helper()
	for _, f := range out.Findings {
		if want.Matches(f) {
			return f
		}
	}
	t.Fatalf("expected finding matching %+v, but none matched among %d finding(s): %s",
		want, len(out.Findings), formatFindingTuples(out.Findings))
	return ParsedFinding{}
}

// AssertNoFinding asserts that out contains zero findings matching unwanted.
// Passing an empty FindingMatch{} asserts zero findings overall (`findings=0`).
func AssertNoFinding(t *testing.T, out ParsedOutput, unwanted FindingMatch) {
	t.Helper()
	if unwanted.isEmpty() {
		if len(out.Findings) != 0 || out.Reported != 0 {
			t.Fatalf("expected zero findings (findings=0), got %d: %s",
				len(out.Findings), formatFindingTuples(out.Findings))
		}
		return
	}
	for _, f := range out.Findings {
		if unwanted.Matches(f) {
			t.Fatalf("unexpected false-positive finding matching %+v: found %s",
				unwanted, formatTuple(f))
		}
	}
}

func assertNoUnredactedSecrets(t *testing.T, cmdName string, format emit.Format, stdout string) {
	t.Helper()
	for _, forbidden := range []string{
		"BEGIN RSA PRIVATE KEY",
		"BEGIN EC PRIVATE KEY",
		"BEGIN PRIVATE KEY",
		"hunter2",
	} {
		if strings.Contains(stdout, forbidden) {
			t.Errorf("lookout %s (%s): unredacted secret pattern %q reached stdout", cmdName, format, forbidden)
		}
	}
}

func assertWireTupleParity(t *testing.T, cmdName string, lf, js ParsedOutput) {
	t.Helper()
	if lf.Reported != js.Reported || len(lf.Findings) != len(js.Findings) {
		t.Fatalf("lookout %s: logfmt/json finding count mismatch: logfmt=%d json=%d",
			cmdName, len(lf.Findings), len(js.Findings))
	}
	for i := range lf.Findings {
		a, b := lf.Findings[i], js.Findings[i]
		if a.Kind != b.Kind || a.Severity != b.Severity || a.Namespace != b.Namespace ||
			a.KindOfObject != b.KindOfObject || a.Name != b.Name || a.Reason != b.Reason {
			t.Errorf("lookout %s: finding[%d] wire parity mismatch: logfmt=%s vs json=%s",
				cmdName, i, formatTuple(a), formatTuple(b))
		}
	}
}

func parseOutput(t *testing.T, stdout string, format emit.Format) ParsedOutput {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	summaryLine, findingLines := lines[len(lines)-1], lines[:len(lines)-1]

	out := ParsedOutput{
		Notes: map[string]string{},
	}
	sumFields, err := lineFields(summaryLine, format)
	if err != nil {
		t.Fatalf("parse summary %q: %v", summaryLine, err)
	}
	for _, f := range sumFields {
		switch f.Key {
		case "scanned":
			out.Scanned, _ = strconv.Atoi(f.Value)
		case "findings":
			out.Reported, _ = strconv.Atoi(f.Value)
		case "elapsed":
			out.Elapsed = f.Value
		default:
			out.Notes[f.Key] = f.Value
		}
	}

	out.Findings = make([]ParsedFinding, 0, len(findingLines))
	for _, line := range findingLines {
		fields, err := lineFields(line, format)
		if err != nil {
			t.Fatalf("parse finding %q: %v", line, err)
		}
		pf := ParsedFinding{Fields: make(map[string]string, len(fields))}
		for _, kv := range fields {
			pf.Fields[kv.Key] = kv.Value
			switch kv.Key {
			case "kind":
				pf.Kind = kv.Value
			case "severity":
				pf.Severity = kv.Value
			case "namespace":
				pf.Namespace = kv.Value
			case "kind_of_object":
				pf.KindOfObject = kv.Value
			case "name":
				pf.Name = kv.Value
			case "reason":
				pf.Reason = kv.Value
			case "message":
				pf.Message = kv.Value
			case "fingerprint":
				pf.Fingerprint = kv.Value
			}
		}
		out.Findings = append(out.Findings, pf)
	}
	return out
}

func formatTuple(f ParsedFinding) string {
	return fmt.Sprintf("(kind=%s ns=%s obj=%s/%s severity=%s reason=%s)",
		f.Kind, f.Namespace, f.KindOfObject, f.Name, f.Severity, f.Reason)
}

func formatFindingTuples(fs []ParsedFinding) string {
	if len(fs) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(fs))
	for _, f := range fs {
		parts = append(parts, formatTuple(f))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}
