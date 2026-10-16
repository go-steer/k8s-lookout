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

package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/memory"
)

// TestTriageRecordedBy_V10ReadOnlyThenUpgrade: a v10 store (before
// recorded_by) still reads read-only with RecordedBy empty; upgraded, it
// stores and returns recorded_by, and an upsert without one clears it.
func TestTriageRecordedBy_V10ReadOnlyThenUpgrade(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v10.sqlite")
	seedDBAtVersion(t, path, triageReasonSchemaVersion)

	ro, err := OpenRead(path)
	if err != nil {
		t.Fatalf("OpenRead v10: %v", err)
	}
	recs, err := ro.TriageStatuses(ctx, memory.TriageQuery{})
	if err != nil || len(recs) != 1 || recs[0].RecordedBy != "" {
		t.Fatalf("v10 read-only: %+v, %v", recs, err)
	}
	_ = ro.Close()

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open upgrade: %v", err)
	}
	defer func() { _ = s.Close() }()
	if s.schemaVersion != triageRecordedBySchemaVersion || s.schemaVersion != len(migrations) {
		t.Fatalf("schemaVersion = %d, want %d (latest)", s.schemaVersion, triageRecordedBySchemaVersion)
	}
	rec := memory.TriageStatusRecord{
		Fingerprint: "sha256:v11", ResourceKey: "Pod/prod/web-1", Status: memory.StatusTriaged,
		RecordedBy: "dana@cymbal.example",
	}
	if _, err := s.UpsertTriageStatus(ctx, rec); err != nil {
		t.Fatal(err)
	}
	got, err := s.TriageStatuses(ctx, memory.TriageQuery{Fingerprint: "sha256:v11"})
	if err != nil || len(got) != 1 || got[0].RecordedBy != "dana@cymbal.example" {
		t.Fatalf("round trip: %+v, %v", got, err)
	}

	// A later write by someone else (or by nobody we know) replaces it:
	// the record is current state, and so is who last wrote it.
	rec.RecordedBy = ""
	rec.Status = memory.StatusActioned
	if _, err := s.UpsertTriageStatus(ctx, rec); err != nil {
		t.Fatal(err)
	}
	got, _ = s.TriageStatuses(ctx, memory.TriageQuery{Fingerprint: "sha256:v11"})
	if len(got) != 1 || got[0].RecordedBy != "" || got[0].Status != memory.StatusActioned {
		t.Fatalf("upsert without a caller: %+v", got)
	}
}
