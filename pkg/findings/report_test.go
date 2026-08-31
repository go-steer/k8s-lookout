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

package findings

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// renderReport runs findings through the REAL emit.Writer, so these
// tests parse exactly what a `lookout health` pipe would deliver
// rather than a hand-written approximation of it. A hand-rolled
// fixture would keep passing after an encoder change; this will not.
func renderReport(t *testing.T, format emit.Format, fs []emit.Finding) string {
	t.Helper()
	var buf bytes.Buffer
	w, err := emit.NewWriter(&buf, format)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, f := range fs {
		if err := w.Emit(f); err != nil {
			t.Fatalf("Emit(%+v): %v", f, err)
		}
	}
	if err := w.Summary(len(fs), 250*time.Millisecond); err != nil {
		t.Fatalf("Summary: %v", err)
	}
	return buf.String()
}

var reportFindings = []emit.Finding{
	{
		Kind:         "pod.crashloop",
		Severity:     emit.SeverityCritical,
		Namespace:    "prod",
		KindOfObject: "Pod",
		Name:         "payment-backend-7d9f8-x9k2l",
		Reason:       "CrashLoopBackOff",
		Message:      `back-off 5m0s restarting failed container=app pod="payment-backend"`,
		Fingerprint:  "sha256:abc123",
	},
	{
		Kind:         "pod.imagepull",
		Severity:     emit.SeverityWarning,
		Namespace:    "staging",
		KindOfObject: "Pod",
		Name:         "checkout-2bv4d-m6n8p",
		Reason:       "ErrImagePull",
		Message:      "pull access denied",
		Fingerprint:  "sha256:def456",
	},
	{
		// Cluster-scoped: no namespace. Exercises the empty-segment
		// path in SubjectKey.
		Kind:         "node.notready",
		Severity:     emit.SeverityCritical,
		KindOfObject: "Node",
		Name:         "gke-pool-a-vmxq",
		Reason:       "NodeNotReady",
		Fingerprint:  "sha256:789abc",
	},
}

func TestParseReportBothFormats(t *testing.T) {
	for _, format := range []emit.Format{emit.FormatLogfmt, emit.FormatJSON} {
		t.Run(string(format), func(t *testing.T) {
			got, _, err := ParseReport(strings.NewReader(renderReport(t, format, reportFindings)), "")
			if err != nil {
				t.Fatalf("ParseReport: %v", err)
			}
			if len(got) != len(reportFindings) {
				t.Fatalf("got %d observations, want %d (the summary line must be skipped, findings must not be)",
					len(got), len(reportFindings))
			}
			for i, want := range reportFindings {
				o := got[i]
				if o.Namespace != want.Namespace || o.KindOfObject != want.KindOfObject || o.Name != want.Name {
					t.Errorf("observation %d identity = (%q, %q, %q), want (%q, %q, %q)",
						i, o.Namespace, o.KindOfObject, o.Name, want.Namespace, want.KindOfObject, want.Name)
				}
				if o.Severity != want.Severity {
					t.Errorf("observation %d severity = %q, want %q", i, o.Severity, want.Severity)
				}
				if o.Fingerprint != want.Fingerprint {
					t.Errorf("observation %d fingerprint = %q, want %q", i, o.Fingerprint, want.Fingerprint)
				}
				if o.Message != want.Message {
					t.Errorf("observation %d message = %q, want %q", i, o.Message, want.Message)
				}
			}
		})
	}
}

// TestParseReportFormatsAgree: the two encodings are the same report,
// so they must produce byte-identical observations. This is the test
// that catches a logfmt quoting bug — the crashloop message above
// contains a space, an `=`, and a `"`, which is exactly the value
// emit quotes with strconv.Quote.
func TestParseReportFormatsAgree(t *testing.T) {
	fromLogfmt, _, err := ParseReport(strings.NewReader(renderReport(t, emit.FormatLogfmt, reportFindings)), "prod-east")
	if err != nil {
		t.Fatalf("logfmt: %v", err)
	}
	fromJSON, _, err := ParseReport(strings.NewReader(renderReport(t, emit.FormatJSON, reportFindings)), "prod-east")
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	if len(fromLogfmt) != len(fromJSON) {
		t.Fatalf("logfmt gave %d observations, json gave %d", len(fromLogfmt), len(fromJSON))
	}
	for i := range fromLogfmt {
		if fromLogfmt[i] != fromJSON[i] {
			t.Errorf("observation %d differs between formats:\n logfmt: %+v\n   json: %+v",
				i, fromLogfmt[i], fromJSON[i])
		}
	}
}

func TestParseReportSubjectKeys(t *testing.T) {
	got, _, err := ParseReport(strings.NewReader(renderReport(t, emit.FormatLogfmt, reportFindings)), "prod-east")
	if err != nil {
		t.Fatalf("ParseReport: %v", err)
	}
	want := []string{
		"prod-east/prod/Pod/payment-backend/CrashLoopBackOff",
		// ErrImagePull canonicalizes into the ImagePullBackOff family,
		// matching engine.ScanFingerprint — so the pull path and the
		// push path agree on one subject.
		"prod-east/staging/Pod/checkout/ImagePullBackOff",
		// Cluster-scoped: the namespace segment is empty, not elided.
		"prod-east//Node/gke-pool-a-vmxq/NodeNotReady",
	}
	for i, w := range want {
		if got[i].SubjectKey != w {
			t.Errorf("observation %d subject key = %q, want %q", i, got[i].SubjectKey, w)
		}
	}
}

// TestParseReportCanonicalizesLikeScanFingerprint pins the agreement
// that keeps push and pull on one subject: the reason baked into a
// subject key must be the same class engine.ScanFingerprint hashes.
func TestParseReportCanonicalizesLikeScanFingerprint(t *testing.T) {
	for _, raw := range []string{"ErrImagePull", "ImagePullBackOff", "CrashLoopBackOff", "OOMKilled"} {
		o := ObservationOf(emit.Finding{
			Kind: "pod.x", KindOfObject: "Pod", Name: "api", Reason: raw,
		}, "")
		if want := engine.CanonicalReason(raw); o.Reason != want {
			t.Errorf("reason %q canonicalized to %q, want %q", raw, o.Reason, want)
		}
		// And the derived key must carry that same class.
		if !strings.HasSuffix(o.SubjectKey, "/"+engine.CanonicalReason(raw)) {
			t.Errorf("subject key %q does not end in the canonical reason for %q", o.SubjectKey, raw)
		}
	}
}

func TestParseReportSkipsBlankAndSummaryOnly(t *testing.T) {
	// A clean run: zero nominal state means the stream is nothing but
	// the summary line.
	got, _, err := ParseReport(strings.NewReader("scanned=412 findings=0 elapsed=1.2s\n"), "")
	if err != nil {
		t.Fatalf("ParseReport: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d observations from a clean report, want 0", len(got))
	}

	// JSON summary encodes scanned/findings as NUMBERS, which is why
	// the parser decodes through RawMessage.
	got, _, err = ParseReport(strings.NewReader(`{"scanned":412,"findings":0,"elapsed":"1.2s"}`+"\n\n"), "")
	if err != nil {
		t.Fatalf("ParseReport(json summary): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d observations, want 0", len(got))
	}
}

func TestParseReportSummaryWithNotes(t *testing.T) {
	// §6.6 graph-backed commands stamp source=/at= notes onto the
	// summary; those must not be mistaken for a finding.
	for _, line := range []string{
		"scanned=10 findings=2 elapsed=1s source=history at=2026-08-13T12:00:00Z\n",
		`{"scanned":10,"findings":2,"elapsed":"1s","source":"history"}` + "\n",
	} {
		got, _, err := ParseReport(strings.NewReader(line), "")
		if err != nil {
			t.Fatalf("ParseReport(%q): %v", line, err)
		}
		if len(got) != 0 {
			t.Errorf("ParseReport(%q) returned %d observations, want 0", line, len(got))
		}
	}
}

// TestParseReportSkipsRecordsWithNoObject: a §4.2 stream carries
// narration as well as findings — health's scorecard rows, scan's
// check_skipped/check_failed/incomplete, the *.unavailable notices —
// and none of those name an object. Diffed, they all composed the
// same empty subject key and collided (#247). They are skipped and
// counted; anything that names an object is not.
func TestParseReportSkipsRecordsWithNoObject(t *testing.T) {
	in := strings.Join([]string{
		`kind=health.category severity=info category=nodes status=healthy`,
		`kind=health.category severity=critical category=crashloops status=degraded`,
		`kind=scan.check_skipped severity=info reason=NotApplicable check="state edges"`,
		`kind=cloud.unavailable severity=info reason=NoCredentials`,
		`kind=pod.crashloop severity=critical namespace=prod kind_of_object=Pod name=api-1 reason=CrashLoopBackOff`,
		// Cluster-scoped and namespace-less, but it still names an
		// object, so it is a subject like any other.
		`kind=node.notready severity=critical kind_of_object=Node name=gke-pool-a-vmxq reason=NodeNotReady`,
		`scanned=6 findings=6 elapsed=1s`,
	}, "\n") + "\n"

	got, skipped, err := ParseReport(strings.NewReader(in), "prod-east")
	if err != nil {
		t.Fatalf("ParseReport: %v", err)
	}
	if skipped != 4 {
		t.Errorf("skipped = %d, want 4", skipped)
	}
	want := []string{
		"prod-east/prod/Pod/api-1/CrashLoopBackOff",
		"prod-east//Node/gke-pool-a-vmxq/NodeNotReady",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d observations, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].SubjectKey != w {
			t.Errorf("observation %d subject key = %q, want %q", i, got[i].SubjectKey, w)
		}
	}
}

// TestParseReportRejectsUnparseableRecords: silently dropping a record
// would report a false `resolved` transition, which is precisely the
// lie this package exists to prevent. Every one of these must be an
// error, not a skip.
func TestParseReportRejectsUnparseableRecords(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
	}{
		{"kindless record", "severity=critical namespace=prod name=api\n"},
		{"malformed json", `{"kind":"pod.crashloop",` + "\n"},
		{"logfmt without =", "this is not logfmt\n"},
		{"unterminated quote", `kind=pod.x message="never closed` + "\n"},
		{"empty key", "=value kind=pod.x\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := ParseReport(strings.NewReader(tc.in), ""); err == nil {
				t.Errorf("ParseReport(%q) succeeded, want an error", tc.in)
			}
		})
	}
}

// TestParseReportErrorNamesTheLine: an operator debugging a pipe needs
// to know WHICH line, and must not be handed a megabyte of it.
func TestParseReportErrorNamesTheLine(t *testing.T) {
	in := "kind=pod.a name=x\nkind=pod.b name=y\nbroken-line-no-equals\n"
	_, _, err := ParseReport(strings.NewReader(in), "")
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "line 3") {
		t.Errorf("error %q does not name line 3", err)
	}

	long := "kind=pod.x message=" + strings.Repeat("z", 5000) + " " + strings.Repeat("q", 5000) + "\n"
	if _, _, err := ParseReport(strings.NewReader(long+"nope\n"), ""); err != nil {
		if len(err.Error()) > 500 {
			t.Errorf("error message is %d bytes — a malformed long line must not become a huge error", len(err.Error()))
		}
	}
}

// TestParseReportRoundTripsThroughDiff is the end-to-end shape a
// consumer actually runs: render a report, parse it, diff it against
// the previous run's state.
func TestParseReportRoundTripsThroughDiff(t *testing.T) {
	run1, _, err := ParseReport(strings.NewReader(renderReport(t, emit.FormatLogfmt, reportFindings)), "")
	if err != nil {
		t.Fatalf("run 1: %v", err)
	}
	first := Diff(nil, run1, t0)
	if len(first.Changes) != 3 {
		t.Fatalf("first run: %d changes, want 3", len(first.Changes))
	}
	for _, c := range first.Changes {
		if c.Transition != TransitionNew {
			t.Errorf("first run: %q is %q, want new", c.SubjectKey, c.Transition)
		}
	}

	// Second run: the crash-looping pod was rescheduled under a new
	// suffix, the node recovered, the pull failure persists.
	second := make([]emit.Finding, 0, 2)
	second = append(second, reportFindings[0], reportFindings[1])
	second[0].Name = "payment-backend-7d9f8-q4m7p"

	run2, _, err := ParseReport(strings.NewReader(renderReport(t, emit.FormatLogfmt, second)), "")
	if err != nil {
		t.Fatalf("run 2: %v", err)
	}
	res := Diff(first.Next, run2, t1)

	counts := map[Transition]int{}
	for _, c := range res.Changes {
		counts[c.Transition]++
	}
	if counts[TransitionOngoing] != 2 {
		t.Errorf("ongoing = %d, want 2 (the reschedule must not read as resolved+new): %+v", counts[TransitionOngoing], res.Changes)
	}
	if counts[TransitionResolved] != 1 {
		t.Errorf("resolved = %d, want 1 (the node)", counts[TransitionResolved])
	}
	if counts[TransitionNew] != 0 {
		t.Errorf("new = %d, want 0", counts[TransitionNew])
	}
}
