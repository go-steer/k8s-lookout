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

package memory

import (
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

var joinNow = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

func crashFinding() emit.Finding {
	return emit.Finding{
		Kind:         "pod.crashloop",
		Severity:     emit.SeverityCritical,
		Namespace:    "prod",
		KindOfObject: "Pod",
		Name:         "payment-7d5b9c6f4-x2k9q",
		Reason:       "CrashLoopBackOff",
		Message:      "container restarted 12 times",
	}
}

// scanFingerprint is the §8 candidate the joiner derives for a
// finding: the M0-frozen reactive kind + canonical reason + object
// class, zone empty (see Joiner.Match).
func scanFingerprint(reason, kindOfObject string) string {
	return engine.Fingerprint(engine.KindK8sEvent, engine.CanonicalReason(reason), kindOfObject, "")
}

// TestJoiner_FingerprintMatch: the record written from a k8s-event
// incident joins the scan finding for the same symptom class, and
// the finding gains the triage fields + the agent's severity
// judgment. This is the §14 M4 exit shape at unit level: the scan
// reports triaged reality, not a fresh unknown.
func TestJoiner_FingerprintMatch(t *testing.T) {
	t.Parallel()
	rec := TriageStatusRecord{
		Fingerprint:         scanFingerprint("CrashLoopBackOff", "Pod"),
		ResourceKey:         "Pod/prod/payment-7d5b9c6f4-x2k9q",
		Session:             "sid-42",
		Status:              StatusTriaged,
		RootCauseHypothesis: "bad connection string in release 2026-07-25a",
		SeverityOverride:    "warning",
		Action:              "PR #402 opened",
		Updated:             joinNow.Add(-15 * time.Minute),
	}
	j := NewJoiner([]TriageStatusRecord{rec}, joinNow)
	f := crashFinding()
	if !j.Annotate(&f) {
		t.Fatal("finding did not match the record")
	}
	if f.Severity != emit.SeverityWarning {
		t.Errorf("severity = %s, want the agent's override (warning)", f.Severity)
	}
	want := map[string]string{
		DetailTriageStatus:    "triaged",
		DetailTriageRootCause: "bad connection string in release 2026-07-25a",
		DetailTriageAction:    "PR #402 opened",
		DetailTriageSession:   "sid-42",
		DetailTriageAge:       "15m0s",
	}
	got := map[string]string{}
	for _, d := range f.Details {
		got[d.Key] = d.Value
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("detail %s = %q, want %q", k, got[k], v)
		}
	}
}

// TestJoiner_ResourceKeyMatch: records for incidents opened by
// leading-indicator kinds (fingerprints a scan cannot reproduce)
// still join via the object-precise resource key — including the
// §9.4 example's group/version-prefixed form.
func TestJoiner_ResourceKeyMatch(t *testing.T) {
	t.Parallel()
	rec := TriageStatusRecord{
		Fingerprint: "sha256:objectstate-kind-fingerprint",
		ResourceKey: "v1/Pod/prod/payment-7d5b9c6f4-x2k9q",
		Status:      StatusActioned,
		Updated:     joinNow.Add(-time.Hour),
	}
	j := NewJoiner([]TriageStatusRecord{rec}, joinNow)
	f := crashFinding()
	if !j.Annotate(&f) {
		t.Fatal("resource-key join failed")
	}
	if f.Severity != emit.SeverityCritical {
		t.Errorf("no override on the record — severity must stay %s, got %s", emit.SeverityCritical, f.Severity)
	}
}

// TestJoiner_EscalatedPinsCritical: escalated stays hot — it beats
// both the scan's class and any leftover severity_override.
func TestJoiner_EscalatedPinsCritical(t *testing.T) {
	t.Parallel()
	rec := TriageStatusRecord{
		Fingerprint:      scanFingerprint("CrashLoopBackOff", "Pod"),
		ResourceKey:      "Pod/prod/payment-7d5b9c6f4-x2k9q",
		Status:           StatusEscalated,
		SeverityOverride: "info",
		Updated:          joinNow.Add(-time.Minute),
	}
	j := NewJoiner([]TriageStatusRecord{rec}, joinNow)
	f := crashFinding()
	f.Severity = emit.SeverityWarning
	if !j.Annotate(&f) {
		t.Fatal("no match")
	}
	if f.Severity != emit.SeverityCritical {
		t.Errorf("escalated must pin critical, got %s", f.Severity)
	}
}

// TestJoiner_ResolvedAndUnmatchedUntouched: resolved records are
// corpus, not current truth; unmatched findings stay byte-identical.
func TestJoiner_ResolvedAndUnmatchedUntouched(t *testing.T) {
	t.Parallel()
	resolved := TriageStatusRecord{
		Fingerprint:      scanFingerprint("CrashLoopBackOff", "Pod"),
		ResourceKey:      "Pod/prod/payment-7d5b9c6f4-x2k9q",
		Status:           StatusResolved,
		SeverityOverride: "info",
		Updated:          joinNow,
	}
	j := NewJoiner([]TriageStatusRecord{resolved}, joinNow)
	if j.Len() != 0 {
		t.Errorf("joiner kept %d resolved record(s)", j.Len())
	}
	f := crashFinding()
	if j.Annotate(&f) {
		t.Error("resolved record must not match")
	}
	if f.Severity != emit.SeverityCritical || len(f.Details) != 0 {
		t.Errorf("unmatched finding mutated: %+v", f)
	}
}

// TestJoiner_FreshestRecordWins: with several open records matching
// (a re-triage rewrote history under a different resource key form),
// the most recently updated one is the truth.
func TestJoiner_FreshestRecordWins(t *testing.T) {
	t.Parallel()
	old := TriageStatusRecord{
		Fingerprint: scanFingerprint("CrashLoopBackOff", "Pod"),
		ResourceKey: "Pod/prod/payment-7d5b9c6f4-x2k9q",
		Status:      StatusInvestigating,
		Updated:     joinNow.Add(-2 * time.Hour),
	}
	fresh := old
	fresh.Status = StatusTriaged
	fresh.RootCauseHypothesis = "OOM under peak load"
	fresh.ResourceKey = "v1/Pod/prod/payment-7d5b9c6f4-x2k9q"
	fresh.Updated = joinNow.Add(-5 * time.Minute)
	j := NewJoiner([]TriageStatusRecord{old, fresh}, joinNow)
	rec, ok := j.Match("Pod", "prod", "payment-7d5b9c6f4-x2k9q", "CrashLoopBackOff")
	if !ok || rec.Status != StatusTriaged {
		t.Errorf("got %+v, want the freshest (triaged) record", rec)
	}
}
