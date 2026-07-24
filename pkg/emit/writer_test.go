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
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite golden files")

// goldenFindings covers the encoding surface: full records, values
// needing quoting/escaping, omitted empty fields, and detail fields.
var goldenFindings = []Finding{
	{
		Kind:         "pod.crashloop",
		Severity:     SeverityCritical,
		Namespace:    "prod",
		KindOfObject: "Pod",
		Name:         "api-7d9c4b-x2n8p",
		Reason:       "CrashLoopBackOff",
		Message:      "back-off 5m0s restarting failed container",
		Details: []Field{
			{Key: "restarts", Value: "17"},
			{Key: "container", Value: "api"},
		},
	},
	{
		// Values with spaces, quotes, and JSON-hostile characters.
		Kind:    "quota.exhausted",
		Message: `used 100% of quota "CPUS" in us-east1 — scale-up blocked`,
		Details: []Field{
			{Key: "limit", Value: "="},
			{Key: "note", Value: "tab\there"},
		},
	},
	{
		// Sparse finding: empty fields must be omitted entirely.
		Kind:   "node.pressure",
		Name:   "gke-pool-1-node-3",
		Reason: "MemoryPressure",
	},
}

func checkGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading golden file (run 'go test ./pkg/emit -update' to create): %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("output does not match %s:\ngot:\n%s\nwant:\n%s", path, got, want)
	}
}

func encodeAll(t *testing.T, format Format, findings []Finding, scanned int, elapsed time.Duration) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, format)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range findings {
		if err := w.Emit(f); err != nil {
			t.Fatalf("Emit(%+v): %v", f, err)
		}
	}
	if err := w.Summary(scanned, elapsed); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestWriterGoldenLogfmt(t *testing.T) {
	got := encodeAll(t, FormatLogfmt, goldenFindings, 412, 1200*time.Millisecond)
	checkGolden(t, "findings.logfmt.golden", got)
}

func TestWriterGoldenJSON(t *testing.T) {
	got := encodeAll(t, FormatJSON, goldenFindings, 412, 1200*time.Millisecond)
	checkGolden(t, "findings.json.golden", got)

	// Every line must be valid standalone JSON.
	for _, line := range strings.Split(strings.TrimSuffix(string(got), "\n"), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Errorf("line %q is not valid JSON: %v", line, err)
		}
	}
}

func TestWriterEmptyScanIsExplicit(t *testing.T) {
	got := encodeAll(t, FormatLogfmt, nil, 42, 100*time.Millisecond)
	want := "scanned=42 findings=0 elapsed=100ms\n"
	if string(got) != want {
		t.Errorf("empty scan output = %q, want %q", got, want)
	}

	got = encodeAll(t, FormatJSON, nil, 42, 100*time.Millisecond)
	want = `{"scanned":42,"findings":0,"elapsed":"100ms"}` + "\n"
	if string(got) != want {
		t.Errorf("empty scan output (json) = %q, want %q", got, want)
	}
}

func TestWriterCountsFindingsItself(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, FormatLogfmt)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := w.Emit(Finding{Kind: "x.y"}); err != nil {
			t.Fatal(err)
		}
	}
	if w.Findings() != 3 {
		t.Fatalf("Findings() = %d, want 3", w.Findings())
	}
	if err := w.Summary(9, time.Second); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(buf.String(), "scanned=9 findings=3 elapsed=1s\n") {
		t.Errorf("summary line missing or wrong: %q", buf.String())
	}
}

// TestWriterSanitizerSeam proves every finding flows through the
// sanitizer hook — the seam the §6.5 sanitizer lands behind.
func TestWriterSanitizerSeam(t *testing.T) {
	var buf bytes.Buffer
	redact := func(f Finding) Finding {
		f.Message = "[REDACTED]"
		return f
	}
	w, err := NewWriter(&buf, FormatLogfmt, WithSanitizer(redact))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Emit(Finding{Kind: "x.y", Message: "password=hunter2"}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if strings.Contains(out, "hunter2") {
		t.Fatalf("sanitizer was bypassed: %q", out)
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Fatalf("sanitized value missing: %q", out)
	}
}

func TestWriterRejectsInvalidFindings(t *testing.T) {
	w, err := NewWriter(&bytes.Buffer{}, FormatLogfmt)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Emit(Finding{}); err == nil {
		t.Error("kindless finding should be rejected")
	}
	bad := Finding{Kind: "x.y", Details: []Field{{Key: "Bad-Key", Value: "v"}}}
	if err := w.Emit(bad); err == nil {
		t.Error("detail key outside [a-z0-9_] should be rejected")
	}
	if w.Findings() != 0 {
		t.Errorf("rejected findings must not count, got %d", w.Findings())
	}
}

func TestNewWriterRejectsUnknownFormat(t *testing.T) {
	if _, err := NewWriter(&bytes.Buffer{}, Format("yaml")); err == nil {
		t.Error("NewWriter should reject unknown formats")
	}
	if _, err := ParseFormat("yaml"); err == nil {
		t.Error("ParseFormat should reject unknown formats")
	}
}
