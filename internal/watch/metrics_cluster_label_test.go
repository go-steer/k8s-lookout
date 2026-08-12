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

	"github.com/prometheus/client_golang/prometheus"
)

// newMetricsFor stamps a cluster="<name>" label on every series, and two
// runner bundles register the same metric names into one shared registry
// without collision because the label VALUE differs. This is the
// multi-cluster metrics contract (docs/multi-cluster-design.md).
func TestNewMetricsForStampsClusterLabel(t *testing.T) {
	reg := prometheus.NewRegistry()
	a := newMetricsFor(reg, "prod-us")
	b := newMetricsFor(reg, "prod-eu") // same names, different cluster — must not panic on re-register

	a.eventsSeen.WithLabelValues("BackOff", "default").Inc()
	b.eventsSeen.WithLabelValues("BackOff", "default").Inc()

	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	seen := map[string]bool{}
	for _, fam := range fams {
		if fam.GetName() != "lookout_events_seen_total" {
			continue
		}
		for _, metric := range fam.GetMetric() {
			var cluster string
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "cluster" {
					cluster = lp.GetValue()
				}
			}
			if cluster == "" {
				t.Errorf("series has no cluster label: %v", metric.GetLabel())
			}
			seen[cluster] = true
		}
	}
	if !seen["prod-us"] || !seen["prod-eu"] {
		t.Errorf("want both cluster series present, got %v", seen)
	}
}

// The bare newMetrics() constructor (tests, single default registry)
// carries no cluster label — its /metrics output is unchanged.
func TestNewMetricsHasNoClusterLabel(t *testing.T) {
	m := newMetrics()
	m.eventsSeen.WithLabelValues("BackOff", "default").Inc()
	fams, err := m.registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, fam := range fams {
		for _, metric := range fam.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "cluster" {
					t.Errorf("%s unexpectedly carries a cluster label", fam.GetName())
				}
			}
		}
	}
}
