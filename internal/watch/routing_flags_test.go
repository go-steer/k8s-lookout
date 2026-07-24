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

package watch

import (
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// TestSeverityRoutingFlags pins the ADDITIVE §7.7 flag surface:
// defaults keep every existing deployment's behavior (no overrides;
// watchboard thresholds per the design), and the values are
// validated.
func TestSeverityRoutingFlags(t *testing.T) {
	t.Parallel()
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if len(f.severity.values) != 0 {
		t.Errorf("default --severity must be empty (source-stamped defaults rule), got %v", f.severity.values)
	}
	if f.watchboardBatch != 5 {
		t.Errorf("default watchboard-batch = %d, want 5", f.watchboardBatch)
	}
	if f.watchboardFlush != 60*time.Second {
		t.Errorf("default watchboard-flush = %v, want 60s", f.watchboardFlush)
	}
	if f.watchboardRotate != 200 {
		t.Errorf("default watchboard-rotate = %d, want 200 (§15 Q2, size-based)", f.watchboardRotate)
	}

	// --severity is repeatable and additive; validate() parses it.
	on, err := parseFlags([]string{
		"--dry-run",
		"--severity=k8s-event=warning,objectstate.restart_burst=info",
		"--severity=objectstate.node_flapping=critical",
		"--watchboard-batch=10", "--watchboard-flush=30s", "--watchboard-rotate=50",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := on.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	want := map[string]engine.Severity{
		"k8s-event":                 engine.SeverityWarning,
		"objectstate.restart_burst": engine.SeverityInfo,
		"objectstate.node_flapping": engine.SeverityCritical,
	}
	if len(on.severityOverrides) != len(want) {
		t.Fatalf("severityOverrides = %v, want %v", on.severityOverrides, want)
	}
	for kind, sev := range want {
		if on.severityOverrides[kind] != sev {
			t.Errorf("severityOverrides[%q] = %q, want %q", kind, on.severityOverrides[kind], sev)
		}
	}
	if on.watchboardBatch != 10 || on.watchboardFlush != 30*time.Second || on.watchboardRotate != 50 {
		t.Errorf("watchboard flags not honored: %+v", on)
	}

	// Validation rejects malformed overrides and nonsensical bounds
	// in every mode (config errors, like --sources).
	rejected := [][]string{
		{"--dry-run", "--severity=k8s-event"},                                      // missing =level
		{"--dry-run", "--severity=k8s-event=fatal"},                                // unknown level
		{"--dry-run", "--severity==critical"},                                      // empty kind
		{"--dry-run", "--severity=k8s-event=warning", "--severity=k8s-event=info"}, // ambiguous repeat
		{"--dry-run", "--watchboard-batch=0"},
		{"--dry-run", "--watchboard-flush=0"},
		{"--dry-run", "--watchboard-flush=-10s"},
		{"--dry-run", "--watchboard-rotate=0"},
	}
	for _, args := range rejected {
		bad, err := parseFlags(args)
		if err != nil {
			t.Fatalf("parseFlags(%v): %v", args, err)
		}
		if err := bad.validate(); err == nil {
			t.Errorf("validate(%v): want rejection", args)
		}
	}
}
