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

package bundle_test

// §9.4 memory merge for `lookout bundle`: an incident bundle
// regenerated mid-triage joins the sentinel's open triage-status
// records, so its findings — the head included — carry the
// diagnosis instead of re-presenting the raw symptom.

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// seedTriageStore mirrors the health-side helper: a sentinel store
// file holding the given records, stamped one hour before fixedNow.
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

// TestBundle_MemoryMerge: with --store, the target's record joins
// the bundle — the head finding gains the triage fields, workload-
// keyed via the §9.4 resource key.
func TestBundle_MemoryMerge(t *testing.T) {
	t.Parallel()
	rec := memory.TriageStatusRecord{
		Fingerprint:         "sha256:incident-class",
		ResourceKey:         memory.ResourceKey("Deployment", "prod", "api"),
		Session:             "sid-42",
		Status:              memory.StatusActioned,
		RootCauseHypothesis: "missing config key log.level",
		Action:              "PR #77 adds the key",
	}
	storePath := seedTriageStore(t, rec)
	res := checktest.Run(t, testCommand(brokenLogs(), brokenObjects()...),
		"--workload=Deployment/prod/api", "--store="+storePath)
	if res.Code != 0 {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	head := ""
	for _, line := range strings.Split(res.Stdout, "\n") {
		if strings.Contains(line, "kind=bundle.target") {
			head = line
			break
		}
	}
	if head == "" {
		t.Fatalf("no bundle.target head in:\n%s", res.Stdout)
	}
	for _, want := range []string{
		"triage_status=actioned",
		`triage_root_cause="missing config key log.level"`,
		`triage_action="PR #77 adds the key"`,
		"triage_session=sid-42",
		"triage_age=1h0m0s",
	} {
		if !strings.Contains(head, want) {
			t.Errorf("head finding missing %q:\n%s", want, head)
		}
	}

	// Without --store the bundle is byte-untouched.
	plain := checktest.Run(t, testCommand(brokenLogs(), brokenObjects()...), "--workload=Deployment/prod/api")
	if strings.Contains(plain.Stdout, "triage_") {
		t.Errorf("run without --store carries triage fields:\n%s", plain.Stdout)
	}
}

// TestBundle_MemoryMergeContract: merged output still honors the
// declared schema.
func TestBundle_MemoryMergeContract(t *testing.T) {
	t.Parallel()
	rec := memory.TriageStatusRecord{
		Fingerprint: "sha256:incident-class",
		ResourceKey: memory.ResourceKey("Deployment", "prod", "api"),
		Status:      memory.StatusInvestigating,
	}
	storePath := seedTriageStore(t, rec)
	checktest.VerifyContract(t, testCommand(brokenLogs(), brokenObjects()...),
		"--workload=Deployment/prod/api", "--store="+storePath)
}
