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

package triage

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

const (
	testFP  = "sha256:e2957792a0b3ad9e29db2051dbc69ff01dfe3a52da8dbb6d1331aa44fe946f8b"
	testKey = "Pod/triagelab/checkout-697567895d-2gglt"
)

// newStatusStore creates a sentinel-shaped store file (migrations
// applied) and returns its path.
func newStatusStore(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lookout.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("store.Close: %v", err)
	}
	return path
}

// TestStatusWriteReadRoundTrip drives the producer surface end to
// end: the write lands through the §9.4 TriageWriter (visible to
// every store reader), and the read mode prints it back.
func TestStatusWriteReadRoundTrip(t *testing.T) {
	path := newStatusStore(t)
	cmd := StatusCommand()

	res := checktest.Run(t, cmd,
		"--store="+path,
		"--fingerprint="+testFP,
		"--resource="+testKey,
		"--session=sess-0004",
		"--status=triaged",
		"--severity-override=warning",
		"--root-cause=DB connection string invalid in checkout-config",
		"--action=fix PR opened; config rollout pending",
	)
	if res.Code != emit.ExitData {
		t.Fatalf("write exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	for _, want := range []string{
		"kind=triage.status",
		"namespace=triagelab",
		"kind_of_object=Pod",
		"name=checkout-697567895d-2gglt",
		"triage_status=triaged",
		"severity_override=warning",
		"triage_session=sess-0004",
		"scanned=1 findings=1",
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("write output missing %q:\n%s", want, res.Stdout)
		}
	}

	// The record is store-visible through the same reader the
	// sentinel's routing and `health --store` use.
	s, err := store.OpenRead(path)
	if err != nil {
		t.Fatalf("OpenRead: %v", err)
	}
	recs, err := s.TriageStatuses(context.Background(), memory.TriageQuery{Fingerprint: testFP, OpenOnly: true})
	_ = s.Close()
	if err != nil {
		t.Fatalf("TriageStatuses: %v", err)
	}
	if len(recs) != 1 || recs[0].ResourceKey != testKey || recs[0].Status != memory.StatusTriaged {
		t.Fatalf("stored records = %+v, want the written triaged record", recs)
	}

	// Read mode: no --status → print the current record(s).
	for _, args := range [][]string{
		{"--store=" + path, "--fingerprint=" + testFP},
		{"--store=" + path, "--resource=" + testKey},
	} {
		res = checktest.Run(t, cmd, args...)
		if res.Code != emit.ExitData {
			t.Fatalf("read %v exit = %d, stderr: %s", args, res.Code, res.Stderr)
		}
		if !strings.Contains(res.Stdout, "triage_status=triaged") || !strings.Contains(res.Stdout, "findings=1") {
			t.Errorf("read %v output missing the record:\n%s", args, res.Stdout)
		}
	}
}

// TestStatusContract runs the §13 output-contract scaffold over both
// modes: every emitted key is declared.
func TestStatusContract(t *testing.T) {
	path := newStatusStore(t)
	cmd := StatusCommand()
	checktest.VerifyContract(t, cmd,
		"--store="+path, "--fingerprint="+testFP, "--resource="+testKey,
		"--status=actioned", "--action=PR #402 opened")
	checktest.VerifyContract(t, cmd, "--store="+path, "--fingerprint="+testFP)
}

// TestStatusUsageErrors pins the exit-2 misuse surface, including
// the design-note pointer the M4 observation asked for.
func TestStatusUsageErrors(t *testing.T) {
	path := newStatusStore(t)
	cmd := StatusCommand()
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no store", []string{"--fingerprint=" + testFP, "--status=triaged", "--resource=" + testKey},
			"docs/triage-status-write-design.md"},
		{"write without identity", []string{"--store=" + path, "--status=triaged"},
			"--fingerprint"},
		{"bad status", []string{"--store=" + path, "--fingerprint=" + testFP, "--resource=" + testKey, "--status=fixed"},
			"investigating|triaged|actioned|escalated"},
		{"agent-written resolved", []string{"--store=" + path, "--fingerprint=" + testFP, "--resource=" + testKey, "--status=resolved"},
			"recovery flip"},
		{"bad override", []string{"--store=" + path, "--fingerprint=" + testFP, "--resource=" + testKey, "--status=triaged", "--severity-override=page"},
			"critical|warning|info"},
		{"read without selector", []string{"--store=" + path},
			"--fingerprint and/or --resource"},
	}
	for _, tc := range cases {
		res := checktest.Run(t, cmd, tc.args...)
		if res.Code != emit.ExitUsage {
			t.Errorf("%s: exit = %d, want %d (stderr: %s)", tc.name, res.Code, emit.ExitUsage, res.Stderr)
			continue
		}
		if !strings.Contains(res.Stderr, tc.want) {
			t.Errorf("%s: stderr %q missing %q", tc.name, res.Stderr, tc.want)
		}
		if strings.Contains(res.Stdout, "findings=") {
			t.Errorf("%s: usage error wrote a summary line to stdout: %q", tc.name, res.Stdout)
		}
	}
}
