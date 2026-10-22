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

func TestFormatFindingsStructuredMarkdownAndSummary(t *testing.T) {
	f, err := ParseFormat("findings")
	if err != nil || f != FormatFindings {
		t.Fatalf("ParseFormat(findings) = (%q, %v), want (%q, nil)", f, err, FormatFindings)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, FormatFindings)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Emit(Finding{
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
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Summary(42, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{
		"### [CRITICAL] pod.crashloop (`CrashLoopBackOff`) — `Pod/prod/api-7d9c4b-x2n8p`",
		"- **Status:** FAIL",
		"- **Detail:** back-off 5m0s restarting failed container",
		"- **Evidence:** `restarts=17`, `container=api`",
		"### Summary",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	if !strings.HasSuffix(out, "scanned=42 findings=1 elapsed=100ms\n") {
		t.Errorf("output must terminate with standard summary line, got:\n%s", out)
	}
}

func TestFormatFindingsSanitizesSecrets(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, FormatFindings)
	if err != nil {
		t.Fatal(err)
	}
	secretToken := "ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if err := w.Emit(Finding{
		Kind:         "edge.missing_key",
		Severity:     SeverityCritical,
		Namespace:    "prod",
		KindOfObject: "Secret",
		Name:         "db-cred",
		Reason:       "CreateContainerConfigError",
		Message:      "token " + secretToken + " rejected",
		Details: []Field{
			{Key: "note", Value: "password=" + secretToken},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Summary(5, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if strings.Contains(out, secretToken) {
		t.Fatalf("secret leaked in FormatFindings output:\n%s", out)
	}
	if !strings.Contains(out, "[REDACTED") {
		t.Errorf("expected redacted marker in FormatFindings output:\n%s", out)
	}
}

func TestFormatFindingsExemptionAnnotation(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, FormatFindings, WithExemptions(newStub()))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Emit(Finding{
		Kind:         "audit.no_pdb",
		Severity:     SeverityWarning,
		Namespace:    "prod",
		KindOfObject: "Deployment",
		Name:         "legacy-api",
		Reason:       "NoPodDisruptionBudget",
		Message:      "no PodDisruptionBudget selects this workload",
		Details:      []Field{{Key: "replicas", Value: "1"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Summary(1, time.Second); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{
		"### [WARNING] audit.no_pdb (`NoPodDisruptionBudget`) — `Deployment/prod/legacy-api`",
		"- **Status:** EXEMPT",
		"- **Exemption:** vendor appliance, replacement tracked in PLAT-8812 (expires `2026-10-15`)",
		"`replicas=1`",
		"`exempt_reason=vendor appliance, replacement tracked in PLAT-8812`",
		"`exempt_expires=2026-10-15`",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	if !strings.HasSuffix(out, "scanned=1 findings=1 elapsed=1s exempt=1\n") {
		t.Errorf("summary line missing or wrong:\n%s", out)
	}
}
