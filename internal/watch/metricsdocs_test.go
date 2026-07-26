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
	"reflect"
	"strings"
	"testing"
)

// TestMetricsInventoryComplete is the presence check behind the
// generated docs-site metrics page: every collector field of the
// metrics struct must have exactly one MetricsInventory row, with a
// derived (non-empty, correctly prefixed) name and help. Adding a
// metric to metrics.go without extending the MetricsInventory
// enumeration fails here — and the sitedoc drift test then fails
// until dev/tools/gen-site-docs is re-run.
func TestMetricsInventoryComplete(t *testing.T) {
	inv := MetricsInventory()

	// One row per collector field (every field except `registry`).
	collectors := reflect.TypeOf(metrics{}).NumField() - 1
	if len(inv) != collectors {
		t.Fatalf("MetricsInventory has %d rows, metrics struct has %d collector fields — add the missing row(s) in metricsdocs.go", len(inv), collectors)
	}

	seen := map[string]bool{}
	for _, d := range inv {
		if !strings.HasPrefix(d.Name, "k8s_event_watcher_") {
			t.Errorf("metric %q: name not derived (missing the frozen k8s_event_watcher_ prefix)", d.Name)
		}
		if seen[d.Name] {
			t.Errorf("metric %q listed twice", d.Name)
		}
		seen[d.Name] = true
		if d.Help == "" {
			t.Errorf("metric %q: empty help — Desc parse failed in describeCollector", d.Name)
		}
		switch d.Type {
		case "counter", "gauge", "histogram":
		default:
			t.Errorf("metric %q: unknown type %q", d.Name, d.Type)
		}
	}
}

// TestFlagInventoryCoversFrozenSurface ties the derived flag
// inventory back to the M0 freeze: every flag TestFlagSurfaceFrozen
// pins must appear in FlagInventory (the docs page can gain flags,
// never lose the frozen ones), and every inventory row carries help.
func TestFlagInventoryCoversFrozenSurface(t *testing.T) {
	inv := FlagInventory()
	byName := map[string]FlagDoc{}
	for _, f := range inv {
		if f.Help == "" {
			t.Errorf("flag --%s: empty help", f.Name)
		}
		byName[f.Name] = f
	}
	frozen := []string{
		"daemon-url", "token-env",
		"mode", "target-session", "owner",
		"reason", "namespace", "exclude-namespace",
		"dedup-window", "dedup-persist", "unhealthy-min-count",
		"recovery-stable-for",
		"in-cluster", "kubeconfig", "cluster-name",
		"log-level", "dry-run", "metrics-addr", "snapshot-interval",
		"otel-exporter",
	}
	for _, name := range frozen {
		if _, ok := byName[name]; !ok {
			t.Errorf("frozen flag --%s missing from FlagInventory", name)
		}
	}
}
