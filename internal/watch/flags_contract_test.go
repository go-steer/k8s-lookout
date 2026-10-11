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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// frozenWatchFlags is the M0 predecessor flag set governed by the Frozen tier
// of docs/cli-stability-policy.md (§Settled decisions #2).
var frozenWatchFlags = []string{
	"daemon-url", "token-env",
	"mode", "target-session", "owner",
	"reason", "namespace", "exclude-namespace",
	"dedup-window", "dedup-persist", "unhealthy-min-count",
	"recovery-stable-for",
	"in-cluster", "kubeconfig", "cluster-name",
	"log-level", "dry-run", "metrics-addr", "snapshot-interval",
	"otel-exporter",
}

func watchFlagStabilityTier(f FlagDoc) string {
	if slices.Contains(frozenWatchFlags, f.Name) {
		return "frozen"
	}
	if strings.Contains(strings.ToLower(f.Help), "(experimental)") {
		return "experimental"
	}
	return "stable"
}

// TestFlagSurfaceFrozen pins the core flag surface of `lookout watch`.
// Every flag name here is load-bearing: an existing deployment must be
// able to upgrade the image with zero config change, so removing or
// renaming one is a breaking change to running deployments, not a
// refactor.
//
// Adding a NEW flag is fine — add it to the list.
func TestFlagSurfaceFrozen(t *testing.T) {
	args := make([]string, 0, len(frozenWatchFlags))
	for _, name := range frozenWatchFlags {
		switch name {
		case "dedup-window", "recovery-stable-for":
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
	if f.backoffMinCount != 3 {
		t.Errorf("default backoff-min-count = %d, want 3", f.backoffMinCount)
	}
	if f.imagePullTransientMin != 3 {
		t.Errorf("default imagepull-transient-min-count = %d, want 3", f.imagePullTransientMin)
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
	if f.recoveryStableFor != 5*time.Minute {
		t.Errorf("default recovery-stable-for = %v, want 5m", f.recoveryStableFor)
	}
}

// TestFlagInventory_GoldenContract locks every flag declared by `lookout watch`
// (name, type, default value, and CLI stability policy tier per
// docs/cli-stability-policy.md) against testdata/watch_flags.golden (#645).
//
// Any addition, rename, type change, default change, or stability-tier change
// requires an explicit golden review (`UPDATE_GOLDEN=1 go test ./internal/watch -run TestFlagInventory`).
func TestFlagInventory_GoldenContract(t *testing.T) {
	inv := FlagInventory()
	if len(inv) < 100 {
		t.Fatalf("expected >=100 watch flags in FlagInventory(), got %d", len(inv))
	}

	var b strings.Builder
	b.WriteString("# lookout watch CLI flag contract (docs/cli-stability-policy.md)\n")
	b.WriteString("# Format: <name>\\t<type>\\t<default>\\t<stability>\n")
	for _, fl := range inv {
		fmt.Fprintf(&b, "%s\t%s\t%q\t%s\n", fl.Name, fl.Type, fl.Default, watchFlagStabilityTier(fl))
	}
	got := b.String()

	goldenPath := filepath.Join("testdata", "watch_flags.golden")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatalf("mkdir testdata: %v", err)
		}
		if err := os.WriteFile(goldenPath, []byte(got), 0o644); err != nil {
			t.Fatalf("write %s: %v", goldenPath, err)
		}
	}

	wantBytes, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read %s: %v (run UPDATE_GOLDEN=1 go test ./internal/watch -run TestFlagInventory)", goldenPath, err)
	}
	want := string(wantBytes)
	if got != want {
		gotLines := strings.Split(strings.TrimSuffix(got, "\n"), "\n")
		wantLines := strings.Split(strings.TrimSuffix(want, "\n"), "\n")
		maxLen := max(len(gotLines), len(wantLines))
		var diff strings.Builder
		for i := range maxLen {
			var gLine, wLine string
			if i < len(gotLines) {
				gLine = gotLines[i]
			}
			if i < len(wantLines) {
				wLine = wantLines[i]
			}
			if gLine != wLine {
				fmt.Fprintf(&diff, "line %d:\n  want: %s\n  got:  %s\n", i+1, wLine, gLine)
			}
		}
		t.Fatalf("watch flag surface drifted from %s:\n%s\n(to accept intentional changes: UPDATE_GOLDEN=1 go test ./internal/watch -run TestFlagInventory)", goldenPath, diff.String())
	}
}
