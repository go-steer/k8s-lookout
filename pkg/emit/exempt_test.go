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
	"strings"
	"testing"
	"time"
)

// stubExempter covers one (kind, namespace, name) triple.
type stubExempter struct {
	kind, namespace, name string
	reason, expires       string
	calls                 int
}

func (s *stubExempter) Exempt(kind, namespace, name string) (string, string, bool) {
	s.calls++
	if kind == s.kind && namespace == s.namespace && name == s.name {
		return s.reason, s.expires, true
	}
	return "", "", false
}

func newStub() *stubExempter {
	return &stubExempter{
		kind: "audit.no_pdb", namespace: "prod", name: "legacy-api",
		reason:  "vendor appliance, replacement tracked in PLAT-8812",
		expires: "2026-10-15",
	}
}

// The whole point of the mechanism: an exempted finding is ANNOTATED,
// not withheld. It is still emitted, still counted in findings=, and
// still carries every field it otherwise would.
func TestExemptAnnotatesNeverDrops(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, FormatLogfmt, WithExemptions(newStub()))
	if err != nil {
		t.Fatal(err)
	}
	f := Finding{
		Kind: "audit.no_pdb", Severity: SeverityWarning,
		Namespace: "prod", KindOfObject: "Deployment", Name: "legacy-api",
		Reason: "NoPodDisruptionBudget", Message: "no PodDisruptionBudget selects this workload",
		Details: []Field{{Key: "replicas", Value: "1"}},
	}
	if err := w.Emit(f); err != nil {
		t.Fatal(err)
	}
	if err := w.Summary(1, time.Second); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want a finding line and a summary line, got %q", buf.String())
	}
	for _, want := range []string{
		`kind=audit.no_pdb`,
		`name=legacy-api`,
		`replicas=1`,
		`exempt_reason="vendor appliance, replacement tracked in PLAT-8812"`,
		`exempt_expires=2026-10-15`,
	} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("finding line %q missing %s", lines[0], want)
		}
	}
	if !strings.Contains(lines[1], "findings=1") {
		t.Errorf("an exempted finding must still be counted: %q", lines[1])
	}
	if !strings.HasSuffix(lines[1], "exempt=1") {
		t.Errorf("summary %q must end with exempt=1", lines[1])
	}
}

// The annotation slots in after fingerprint and before Details, in both
// encodings — record order is part of the §4.2 contract.
func TestExemptFieldOrder(t *testing.T) {
	f := Finding{
		Kind: "audit.no_pdb", Namespace: "prod", Name: "legacy-api",
		Fingerprint:   "sha256:abc",
		ExemptReason:  "why",
		ExemptExpires: "2026-10-15",
		Details:       []Field{{Key: "replicas", Value: "1"}},
	}
	var keys []string
	for _, p := range f.pairs() {
		keys = append(keys, p.Key)
	}
	want := []string{"kind", "namespace", "name", "fingerprint", "exempt_reason", "exempt_expires", "replicas"}
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Errorf("field order = %v, want %v", keys, want)
	}
}

// exempt=0 is a fact worth stating and is NOT the same fact as "no
// exemption file was configured", which emits no key at all.
func TestExemptSummaryKeyPresence(t *testing.T) {
	for _, tc := range []struct {
		name         string
		opts         []WriterOption
		wantContains string
		wantAbsent   string
	}{
		{
			name:         "file supplied, nothing matched",
			opts:         []WriterOption{WithExemptions(newStub())},
			wantContains: "exempt=0",
		},
		{
			name:       "no file supplied",
			wantAbsent: "exempt=",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w, err := NewWriter(&buf, FormatLogfmt, tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Emit(Finding{Kind: "audit.no_pdb", Namespace: "staging", Name: "api"}); err != nil {
				t.Fatal(err)
			}
			if err := w.Summary(1, time.Second); err != nil {
				t.Fatal(err)
			}
			out := buf.String()
			if tc.wantContains != "" && !strings.Contains(out, tc.wantContains) {
				t.Errorf("output %q missing %q", out, tc.wantContains)
			}
			if tc.wantAbsent != "" && strings.Contains(out, tc.wantAbsent) {
				t.Errorf("output %q must not contain %q", out, tc.wantAbsent)
			}
			if strings.Contains(out, "exempt_reason") {
				t.Errorf("an unmatched finding must not be annotated: %q", out)
			}
		})
	}
}

func TestExemptSummaryJSON(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, FormatJSON, WithExemptions(newStub()))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Emit(Finding{Kind: "audit.no_pdb", Namespace: "prod", Name: "legacy-api"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Summary(1, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	want := `{"scanned":1,"findings":1,"elapsed":"100ms","exempt":"1"}`
	if lines[1] != want {
		t.Errorf("summary = %s, want %s", lines[1], want)
	}
}

// The exempt count is appended AFTER any §6.6 command notes, so a
// consumer reading positionally still finds source=/at= where it
// expects them.
func TestExemptCountFollowsCommandNotes(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, FormatLogfmt, WithExemptions(newStub()))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Note("source", "live"); err != nil {
		t.Fatal(err)
	}
	if err := w.Summary(0, time.Second); err != nil {
		t.Fatal(err)
	}
	want := "scanned=0 findings=0 elapsed=1s source=live exempt=0\n"
	if buf.String() != want {
		t.Errorf("summary = %q, want %q", buf.String(), want)
	}
}

// A check must not be able to forge the exempt count.
func TestNoteRejectsWriterOwnedKeys(t *testing.T) {
	w, err := NewWriter(&bytes.Buffer{}, FormatLogfmt)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range SummaryNoteFields() {
		if err := w.Note(key, "999"); err == nil {
			t.Errorf("Note(%q) should be rejected: the Writer owns that key", key)
		}
	}
}

// Annotation happens AFTER sanitizing, so a check cannot launder a
// secret through the exemption path, and the exemption matcher sees the
// sanitized identity.
func TestExemptRunsAfterSanitizer(t *testing.T) {
	var buf bytes.Buffer
	renamer := func(f Finding) Finding {
		f.Name = "legacy-api"
		return f
	}
	w, err := NewWriter(&buf, FormatLogfmt, WithSanitizer(renamer), WithExemptions(newStub()))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Emit(Finding{Kind: "audit.no_pdb", Namespace: "prod", Name: "pre-sanitize"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "exempt_reason=") {
		t.Errorf("matcher must see the sanitized finding: %q", buf.String())
	}
}

// A rejected finding must not inflate the exempt count any more than it
// inflates the findings count.
func TestExemptDoesNotCountRejectedFindings(t *testing.T) {
	w, err := NewWriter(&bytes.Buffer{}, FormatLogfmt, WithExemptions(newStub()))
	if err != nil {
		t.Fatal(err)
	}
	bad := Finding{
		Kind: "audit.no_pdb", Namespace: "prod", Name: "legacy-api",
		Details: []Field{{Key: "Bad-Key", Value: "v"}},
	}
	if err := w.Emit(bad); err == nil {
		t.Fatal("expected the malformed finding to be rejected")
	}
	if w.Findings() != 0 || w.Exempted() != 0 {
		t.Errorf("findings=%d exempted=%d, want 0/0", w.Findings(), w.Exempted())
	}
}

// A nil Exempter is the same as not passing one: no annotation, and no
// exempt= key on the summary. (`WithExemptions(nil)` is what a caller
// writes when it passes through an optional value.)
func TestWithExemptionsNil(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, FormatLogfmt, WithExemptions(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Emit(Finding{Kind: "audit.no_pdb"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Summary(1, time.Second); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "exempt") {
		t.Errorf("a nil Exempter must behave as no Exempter: %q", buf.String())
	}
}

// The envelope owns exempt_reason/exempt_expires, so no command has to
// (or may) declare them in its output glossary.
func TestExemptFieldsAreEnvelopeFields(t *testing.T) {
	got := map[string]bool{}
	for _, f := range EnvelopeFields() {
		got[f] = true
	}
	for _, want := range []string{"exempt_reason", "exempt_expires"} {
		if !got[want] {
			t.Errorf("%q must be an envelope field", want)
		}
	}
}
