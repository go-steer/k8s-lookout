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

type stubPolicyRule struct {
	enabled  bool
	severity Severity
}

type stubPolicy struct {
	name  string
	rules map[string]stubPolicyRule
}

func (s *stubPolicy) Name() string {
	if s == nil {
		return ""
	}
	return s.name
}

func (s *stubPolicy) Decide(kind, reason string, sev Severity) (bool, Severity) {
	if s == nil {
		return true, sev
	}
	if reason != "" {
		if r, ok := s.rules[kind+"/"+reason]; ok {
			out := sev
			if r.severity != "" {
				out = r.severity
			}
			return r.enabled, out
		}
	}
	if r, ok := s.rules[kind]; ok {
		out := sev
		if r.severity != "" {
			out = r.severity
		}
		return r.enabled, out
	}
	return true, sev
}

func TestWriterPolicySuppressesAndRewritesSeverity(t *testing.T) {
	t.Parallel()
	pol := &stubPolicy{
		name: "strict",
		rules: map[string]stubPolicyRule{
			"audit.no_liveness_probe": {enabled: false},
			"audit.no_spread":         {enabled: true, severity: SeverityWarning},
			"audit.liveness_preempts_readiness/LivenessDeadlineTiesReadiness": {enabled: true, severity: SeverityInfo},
			"audit.liveness_preempts_readiness":                               {enabled: true, severity: SeverityCritical},
		},
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, FormatLogfmt, WithPolicy(pol))
	if err != nil {
		t.Fatal(err)
	}
	var watched []Finding
	w.Watch(func(f Finding) { watched = append(watched, f) })

	inputs := []Finding{
		{Kind: "audit.no_liveness_probe", Severity: SeverityInfo, Namespace: "prod", Name: "api", Reason: "NoLivenessProbe"},
		{Kind: "audit.no_spread", Severity: SeverityInfo, Namespace: "prod", Name: "api", Reason: "NoSpread"},
		{Kind: "audit.liveness_preempts_readiness", Severity: SeverityWarning, Namespace: "prod", Name: "api", Reason: "LivenessDeadlineTiesReadiness"},
		{Kind: "audit.liveness_preempts_readiness", Severity: SeverityWarning, Namespace: "prod", Name: "web", Reason: "LivenessFiresFirst"},
	}
	for _, f := range inputs {
		if err := w.Emit(f); err != nil {
			t.Fatalf("Emit(%+v): %v", f, err)
		}
	}
	if err := w.Summary(4, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	if w.Findings() != 3 {
		t.Errorf("Findings() = %d, want 3", w.Findings())
	}
	if w.Suppressed() != 1 {
		t.Errorf("Suppressed() = %d, want 1", w.Suppressed())
	}
	if len(watched) != 3 {
		t.Errorf("watched %d findings, want 3 (suppressed findings must not reach watchers)", len(watched))
	}

	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("want 3 finding lines + 1 summary line, got:\n%s", buf.String())
	}
	if strings.Contains(buf.String(), "audit.no_liveness_probe") {
		t.Errorf("suppressed finding should not be written:\n%s", buf.String())
	}
	if !strings.Contains(lines[0], "kind=audit.no_spread severity=warning") {
		t.Errorf("line 0 = %q, want promoted severity=warning", lines[0])
	}
	if !strings.Contains(lines[1], "kind=audit.liveness_preempts_readiness severity=info") {
		t.Errorf("line 1 = %q, want <kind>/<reason> demoted severity=info", lines[1])
	}
	if !strings.Contains(lines[2], "kind=audit.liveness_preempts_readiness severity=critical") {
		t.Errorf("line 2 = %q, want <kind> promoted severity=critical", lines[2])
	}
	wantSummary := "scanned=4 findings=3 elapsed=100ms policy=strict suppressed=1"
	if lines[3] != wantSummary {
		t.Errorf("summary = %q, want %q", lines[3], wantSummary)
	}
}

func TestWriterPolicySummaryJSON(t *testing.T) {
	t.Parallel()
	pol := &stubPolicy{
		name: "onboarding",
		rules: map[string]stubPolicyRule{
			"audit.no_pdb": {enabled: false},
		},
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, FormatJSON, WithPolicy(pol))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Emit(Finding{Kind: "audit.no_pdb", Severity: SeverityWarning}); err != nil {
		t.Fatal(err)
	}
	if err := w.Emit(Finding{Kind: "audit.podsecurity_gaps", Severity: SeverityWarning}); err != nil {
		t.Fatal(err)
	}
	if err := w.Summary(2, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 1 finding + 1 summary, got:\n%s", buf.String())
	}
	want := `{"scanned":2,"findings":1,"elapsed":"100ms","policy":"onboarding","suppressed":"1"}`
	if lines[1] != want {
		t.Errorf("JSON summary = %s, want %s", lines[1], want)
	}
}

func TestWriterPolicyNeverSuppressesUnavailableOrScanMeta(t *testing.T) {
	t.Parallel()
	// A policy that disables everything it is asked about.
	dropAll := &stubPolicy{
		name: "none",
		rules: map[string]stubPolicyRule{
			"read.unavailable":   {enabled: false},
			"cloud.unavailable":  {enabled: false},
			"scan.check_skipped": {enabled: false},
			"scan.check_failed":  {enabled: false},
			"scan.incomplete":    {enabled: false},
			"audit.no_pdb":       {enabled: false},
		},
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, FormatLogfmt, WithPolicy(dropAll))
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{
		"read.unavailable",
		"cloud.unavailable",
		"scan.check_skipped",
		"scan.check_failed",
		"scan.incomplete",
		"audit.no_pdb",
	} {
		if err := w.Emit(Finding{Kind: kind, Severity: SeverityInfo, Message: "msg"}); err != nil {
			t.Fatalf("Emit(%s): %v", kind, err)
		}
	}
	if w.Findings() != 5 {
		t.Errorf("Findings() = %d, want 5 (all unavailable and scan.* findings must survive)", w.Findings())
	}
	if w.Suppressed() != 1 {
		t.Errorf("Suppressed() = %d, want 1 (only audit.no_pdb)", w.Suppressed())
	}
}

func TestWriterPolicyNilAndTypedNil(t *testing.T) {
	t.Parallel()
	var typedNil *stubPolicy
	for _, opt := range []WriterOption{WithPolicy(nil), WithPolicy(typedNil)} {
		var buf bytes.Buffer
		w, err := NewWriter(&buf, FormatLogfmt, opt)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Emit(Finding{Kind: "audit.no_pdb", Severity: SeverityWarning}); err != nil {
			t.Fatal(err)
		}
		if err := w.Summary(1, time.Second); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(buf.String(), "policy=") || strings.Contains(buf.String(), "suppressed=") {
			t.Errorf("nil Policy must not emit policy= or suppressed=:\n%s", buf.String())
		}
	}
}

func TestPolicySummaryKeysAreReserved(t *testing.T) {
	t.Parallel()
	w, err := NewWriter(&bytes.Buffer{}, FormatLogfmt)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"policy", "suppressed"} {
		if err := w.Note(key, "x"); err == nil {
			t.Errorf("Note(%q) should be rejected: the Writer owns that key", key)
		}
	}
}
