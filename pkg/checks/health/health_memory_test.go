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

package health_test

// The §14 M4 exit criterion, as a test: "a health scan run
// mid-incident reports the triage state, not a fresh unknown."
// Fixture: a crashlooping pod (the raw symptom) + the §9.4
// triage-status record an incident agent wrote (the triaged
// reality) → the scan's finding carries the diagnosis, the paper
// trail, and the agent's severity judgment.

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// seedTriageStore writes records into a fresh sentinel store file
// and returns its path (updated stamps land one hour before the
// scan's fixedNow, so triage_age is deterministic).
func seedTriageStore(t *testing.T, records ...memory.TriageStatusRecord) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lookout.db")
	clock := fixedNow.Add(-time.Hour)
	s, err := store.Open(path, store.WithLogf(t.Logf), store.WithClock(func() time.Time { return clock }))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	for _, rec := range records {
		if _, err := s.UpsertTriageStatus(context.Background(), rec); err != nil {
			t.Fatalf("UpsertTriageStatus: %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return path
}

// TestHealth_MemoryMergedMidIncident is the M4 exit shape: the scan
// still sees the symptom, and reports it TRIAGED — diagnosis, paper
// trail, session, record age — with severity honoring the agent's
// downgrade all the way into the scorecard.
func TestHealth_MemoryMergedMidIncident(t *testing.T) {
	t.Parallel()
	rec := memory.TriageStatusRecord{
		// The fingerprint the incident's inject payloads carried:
		// the M0 reactive kind + the canonical reason class.
		Fingerprint:         engine.Fingerprint(engine.KindK8sEvent, engine.CanonicalReason("CrashLoopBackOff"), "Pod", ""),
		ResourceKey:         memory.ResourceKey("Pod", "prod", "payment-1"),
		Session:             "sid-42",
		Status:              memory.StatusTriaged,
		RootCauseHypothesis: "bad connection string in release 2026-07a",
		SeverityOverride:    "warning",
		Action:              "PR #402 opened",
	}
	storePath := seedTriageStore(t, rec)
	cmd := testCommand(crashloopPod("prod", "payment-1"))

	res := checktest.Run(t, cmd, "--store="+storePath)
	if res.Code != 0 {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	finding := lineContaining(t, res.Stdout, "kind=pod.crashloop")
	for _, want := range []string{
		"severity=warning", // the agent's judgment, not the raw critical
		"triage_status=triaged",
		`triage_root_cause="bad connection string in release 2026-07a"`,
		`triage_action="PR #402 opened"`,
		"triage_session=sid-42",
		"triage_age=1h0m0s",
	} {
		if !strings.Contains(finding, want) {
			t.Errorf("mid-incident finding missing %q:\n%s", want, finding)
		}
	}
	// The scorecard line sees triaged reality too: crashloops is
	// degraded at the OVERRIDDEN severity.
	card := lineContaining(t, res.Stdout, "category=crashloops")
	if !strings.Contains(card, "status=degraded") || !strings.Contains(card, "severity=warning") {
		t.Errorf("scorecard line should reflect the override:\n%s", card)
	}
}

// TestHealth_MergeLeavesUnmatchedUntouched: the record pins ONE
// resource — a second pod crashlooping in the same class stays a
// fresh critical unknown, and without --store nothing changes at
// all.
func TestHealth_MergeLeavesUnmatchedUntouched(t *testing.T) {
	t.Parallel()
	rec := memory.TriageStatusRecord{
		Fingerprint:      engine.Fingerprint(engine.KindK8sEvent, "CrashLoopBackOff", "Pod", ""),
		ResourceKey:      memory.ResourceKey("Pod", "prod", "payment-1"),
		Status:           memory.StatusTriaged,
		SeverityOverride: "warning",
	}
	storePath := seedTriageStore(t, rec)
	cmd := testCommand(crashloopPod("prod", "payment-1"), crashloopPod("prod", "other-2"))

	res := checktest.Run(t, cmd, "--store="+storePath)
	if res.Code != 0 {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	other := lineContaining(t, res.Stdout, "name=other-2")
	if !strings.Contains(other, "severity=critical") || strings.Contains(other, "triage_") {
		t.Errorf("unmatched finding must stay a fresh critical unknown:\n%s", other)
	}

	// No --store: no merge, byte-untouched raw scan.
	plain := checktest.Run(t, cmd)
	if strings.Contains(plain.Stdout, "triage_") {
		t.Errorf("run without --store carries triage fields:\n%s", plain.Stdout)
	}
}

// TestHealth_MergeIgnoresResolvedRecords: §9.4 lifecycle — once
// recovery flipped the record, the scan reports the symptom at face
// value again (a recurrence is a NEW unknown, §7.4 reverted flow).
func TestHealth_MergeIgnoresResolvedRecords(t *testing.T) {
	t.Parallel()
	rec := memory.TriageStatusRecord{
		Fingerprint:      engine.Fingerprint(engine.KindK8sEvent, "CrashLoopBackOff", "Pod", ""),
		ResourceKey:      memory.ResourceKey("Pod", "prod", "payment-1"),
		Status:           memory.StatusResolved,
		SeverityOverride: "warning",
	}
	storePath := seedTriageStore(t, rec)
	cmd := testCommand(crashloopPod("prod", "payment-1"))
	res := checktest.Run(t, cmd, "--store="+storePath)
	finding := lineContaining(t, res.Stdout, "kind=pod.crashloop")
	if !strings.Contains(finding, "severity=critical") || strings.Contains(finding, "triage_") {
		t.Errorf("resolved record must not steer the scan:\n%s", finding)
	}
}

// TestHealth_MemoryMergeContract: the merged output still honors the
// declared §4.2 schema (the triage_* keys are in the glossary).
func TestHealth_MemoryMergeContract(t *testing.T) {
	t.Parallel()
	rec := memory.TriageStatusRecord{
		Fingerprint:         engine.Fingerprint(engine.KindK8sEvent, "CrashLoopBackOff", "Pod", ""),
		ResourceKey:         memory.ResourceKey("Pod", "prod", "payment-1"),
		Session:             "sid-42",
		Status:              memory.StatusEscalated,
		RootCauseHypothesis: "node-local disk pressure",
		Action:              "paged #platform-oncall",
	}
	storePath := seedTriageStore(t, rec)
	cmd := testCommand(crashloopPod("prod", "payment-1"))
	checktest.VerifyContract(t, cmd, "--store="+storePath)
}

// lineContaining returns the first stdout line containing needle.
func lineContaining(t *testing.T, stdout, needle string) string {
	t.Helper()
	for _, line := range strings.Split(stdout, "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}
	t.Fatalf("no line containing %q in:\n%s", needle, stdout)
	return ""
}
