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
	"encoding/json"
	"testing"
	"time"
)

// TestTriageStatusRecord_SchemaGolden pins the §9.4 wire shape
// byte-exact against the schema in DESIGN.md — field names are the
// doc's example verbatim. A failing pin is a breaking change to the
// record contract incident playbooks and scans share.
func TestTriageStatusRecord_SchemaGolden(t *testing.T) {
	t.Parallel()
	rec := TriageStatusRecord{
		Fingerprint:         "sha256:1f4e6a7b",
		ResourceKey:         "Deployment/prod/payment-service",
		Session:             "sid-42",
		Status:              StatusTriaged,
		RootCauseHypothesis: "DB connection pool exhausted (max_connections 100/100)",
		SeverityOverride:    "warning",
		Action:              "PR #402 opened; escalated to #platform-db",
		Updated:             time.Date(2026, 7, 25, 8, 0, 0, 0, time.UTC),
	}
	got, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"fingerprint":"sha256:1f4e6a7b",` +
		`"resource_key":"Deployment/prod/payment-service",` +
		`"session":"sid-42","status":"triaged",` +
		`"root_cause_hypothesis":"DB connection pool exhausted (max_connections 100/100)",` +
		`"severity_override":"warning",` +
		`"action":"PR #402 opened; escalated to #platform-db",` +
		`"updated":"2026-07-25T08:00:00Z"}`
	if string(got) != want {
		t.Errorf("TriageStatusRecord wire shape drifted (§9.4 SCHEMA-STABLE):\n got %s\nwant %s", got, want)
	}
}

func TestTriageStatus_OpenAndValid(t *testing.T) {
	t.Parallel()
	for _, s := range []TriageStatus{StatusInvestigating, StatusTriaged, StatusActioned, StatusEscalated} {
		if !s.Valid() || !s.Open() {
			t.Errorf("%s: want valid and open", s)
		}
	}
	if !StatusResolved.Valid() || StatusResolved.Open() {
		t.Error("resolved: want valid and NOT open (corpus, not current truth)")
	}
	if TriageStatus("paged").Valid() {
		t.Error("undefined status accepted")
	}
}

func TestTriageStatusRecord_Validate(t *testing.T) {
	t.Parallel()
	ok := TriageStatusRecord{Fingerprint: "sha256:x", ResourceKey: "Pod/ns/p", Status: StatusInvestigating}
	if err := ok.Validate(); err != nil {
		t.Errorf("valid record rejected: %v", err)
	}
	bad := map[string]TriageStatusRecord{
		"no fingerprint": {ResourceKey: "Pod/ns/p", Status: StatusTriaged},
		"no resource":    {Fingerprint: "sha256:x", Status: StatusTriaged},
		"bad status":     {Fingerprint: "sha256:x", ResourceKey: "Pod/ns/p", Status: "diagnosed"},
		"bad override":   {Fingerprint: "sha256:x", ResourceKey: "Pod/ns/p", Status: StatusTriaged, SeverityOverride: "page"},
	}
	for name, rec := range bad {
		if err := rec.Validate(); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}

func TestResourceKeyAndMatches(t *testing.T) {
	t.Parallel()
	if got := ResourceKey("Deployment", "prod", "payment"); got != "Deployment/prod/payment" {
		t.Errorf("ResourceKey = %q", got)
	}
	if got := ResourceKey("Node", "", "gke-node-1"); got != "Node//gke-node-1" {
		t.Errorf("cluster-scoped ResourceKey = %q (namespace segment stays, empty)", got)
	}
	rec := TriageStatusRecord{ResourceKey: "Deployment/prod/payment"}
	if !rec.MatchesResource("Deployment/prod/payment") {
		t.Error("exact key must match")
	}
	if rec.MatchesResource("Deployment/prod/other") {
		t.Error("different name must not match")
	}
	// §9.4's example carries a group/version prefix; the canonical
	// key drops it, and matching tolerates records written WITH it.
	gv := TriageStatusRecord{ResourceKey: "apps/v1/Deployment/prod/payment"}
	if !gv.MatchesResource("Deployment/prod/payment") {
		t.Error("group/version-prefixed record must match the canonical key")
	}
	if gv.MatchesResource("ployment/prod/payment") {
		t.Error("suffix match must respect segment boundaries")
	}
}
