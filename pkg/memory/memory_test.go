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
	"strings"
	"testing"
	"time"
)

// TestDistilledFact_SchemaGolden pins the wire encoding byte-exact.
// The JSON shape is a schema-stable contract (§9.2): agents and fleet
// consumers
// consume these records outside this repo. A failing pin is a
// BREAKING CHANGE to review, never a test to update casually —
// evolution is additive-only.
func TestDistilledFact_SchemaGolden(t *testing.T) {
	t.Parallel()
	f := DistilledFact{
		Class: "capacity.stockout.recurrence",
		Scope: map[string]string{
			ScopeZone:      "us-east1-b",
			ScopeNodeGroup: "n2d-pool",
			ScopeCluster:   "prod-east",
		},
		Statement:          "us-east1-b nodegroup n2d-pool: 3 stockouts in 7d (first 2026-07-20T10:00:00Z, last 2026-07-25T09:00:00Z)",
		WindowStart:        time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC),
		WindowEnd:          time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC),
		Occurrences:        3,
		DistinctObjects:    1,
		FirstSeen:          time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC),
		LastSeen:           time.Date(2026, 7, 25, 9, 0, 0, 0, time.UTC),
		SourceFingerprints: []string{"sha256:aaaa"},
		Created:            time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC),
		Updated:            time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC),
	}
	got, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"class":"capacity.stockout.recurrence",` +
		`"scope":{"cluster":"prod-east","nodegroup":"n2d-pool","zone":"us-east1-b"},` +
		`"statement":"us-east1-b nodegroup n2d-pool: 3 stockouts in 7d (first 2026-07-20T10:00:00Z, last 2026-07-25T09:00:00Z)",` +
		`"window_start":"2026-07-18T12:00:00Z","window_end":"2026-07-25T12:00:00Z",` +
		`"occurrences":3,"distinct_objects":1,` +
		`"first_seen":"2026-07-20T10:00:00Z","last_seen":"2026-07-25T09:00:00Z",` +
		`"source_fingerprints":["sha256:aaaa"],` +
		`"created":"2026-07-25T12:00:00Z","updated":"2026-07-25T12:00:00Z"}`
	if string(got) != want {
		t.Errorf("DistilledFact wire shape drifted (SCHEMA-STABLE contract):\n got %s\nwant %s", got, want)
	}
}

// TestDistilledFact_OmitemptyFields: the optional fields drop out of
// the encoding entirely — consumers must not depend on their
// presence.
func TestDistilledFact_OmitemptyFields(t *testing.T) {
	t.Parallel()
	f := DistilledFact{
		Class:     "workload.crashloop.recurrence",
		Scope:     map[string]string{ScopeWorkload: "payment"},
		Statement: "payment: recurring",
	}
	got, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, absent := range []string{"distinct_objects", "source_fingerprints"} {
		if strings.Contains(string(got), absent) {
			t.Errorf("zero-valued %q should be omitted, got %s", absent, got)
		}
	}
}

func TestScopeKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		scope map[string]string
		want  string
	}{
		{"sorted keys", map[string]string{"zone": "z", "cluster": "c"}, `{"cluster":"c","zone":"z"}`},
		{"empty values dropped", map[string]string{"zone": "z", "project": ""}, `{"zone":"z"}`},
		{"nil scope", nil, `{}`},
		{"quoting is JSON", map[string]string{"k": `va"l`}, `{"k":"va\"l"}`},
	}
	for _, tt := range tests {
		if got := ScopeKey(tt.scope); got != tt.want {
			t.Errorf("%s: ScopeKey = %s, want %s", tt.name, got, tt.want)
		}
	}
	// Determinism: two maps with the same content produce identical
	// keys regardless of insertion order.
	a := map[string]string{"a": "1", "b": "2", "c": "3"}
	b := map[string]string{"c": "3", "a": "1", "b": "2"}
	if ScopeKey(a) != ScopeKey(b) {
		t.Errorf("ScopeKey not deterministic: %s vs %s", ScopeKey(a), ScopeKey(b))
	}
}

func TestDistilledFact_Validate(t *testing.T) {
	t.Parallel()
	ok := DistilledFact{Class: "c.x", Scope: map[string]string{"zone": "z"}, Statement: "s"}
	if err := ok.Validate(); err != nil {
		t.Errorf("valid fact rejected: %v", err)
	}
	for name, f := range map[string]DistilledFact{
		"no class":     {Scope: map[string]string{"zone": "z"}, Statement: "s"},
		"no scope":     {Class: "c.x", Statement: "s"},
		"empty scope":  {Class: "c.x", Scope: map[string]string{"zone": ""}, Statement: "s"},
		"no statement": {Class: "c.x", Scope: map[string]string{"zone": "z"}},
	} {
		if err := f.Validate(); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}
