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
)

// TestFlagSurfaceFrozen pins the exact flag surface of `lookout watch`
// to the k8s-event-watcher it replaced. The M0 exit criterion is that
// an existing watcher deployment swaps images with zero config change,
// so every flag name here is load-bearing: removing or renaming one is
// a breaking change to running deployments, not a refactor.
//
// Adding a NEW flag is fine — add it to the list.
func TestFlagSurfaceFrozen(t *testing.T) {
	frozen := []string{
		"daemon-url", "token-env",
		"mode", "target-session", "owner",
		"reason", "namespace", "exclude-namespace",
		"dedup-window", "dedup-persist", "unhealthy-min-count",
		"in-cluster", "kubeconfig", "cluster-name",
		"log-level", "dry-run", "metrics-addr", "snapshot-interval",
		"otel-exporter",
	}
	args := make([]string, 0, len(frozen))
	for _, name := range frozen {
		switch name {
		case "dedup-window":
			args = append(args, "--"+name+"=5m")
		case "snapshot-interval":
			args = append(args, "--"+name+"=30s")
		case "unhealthy-min-count":
			args = append(args, "--"+name+"=3")
		case "in-cluster", "dry-run":
			args = append(args, "--"+name)
		default:
			args = append(args, "--"+name+"=x")
		}
	}
	if _, err := parseFlags(args); err != nil {
		t.Fatalf("a frozen flag was rejected: %v", err)
	}

	// Defaults are part of the deployment contract too: a config that
	// omitted a flag must keep behaving identically.
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if f.mode != "per-incident" {
		t.Errorf("default mode = %q, want per-incident", f.mode)
	}
	if f.dedupWindow != 5*time.Minute {
		t.Errorf("default dedup-window = %v, want 5m", f.dedupWindow)
	}
	if f.unhealthyMinCount != 3 {
		t.Errorf("default unhealthy-min-count = %d, want 3", f.unhealthyMinCount)
	}
	if f.logLevel != "info" {
		t.Errorf("default log-level = %q, want info", f.logLevel)
	}
	if f.otelExporter != "none" {
		t.Errorf("default otel-exporter = %q, want none", f.otelExporter)
	}
	if f.snapshotInterval != 30*time.Second {
		t.Errorf("default snapshot-interval = %v, want 30s", f.snapshotInterval)
	}
}
