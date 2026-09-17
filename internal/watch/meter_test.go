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
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-steer/k8s-lookout/internal/telemetry"
)

func testRunner(t *testing.T, cluster string, argv ...string) *runner {
	t.Helper()
	f, err := parseFlags(argv)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	return newRunner(f, cluster, nil, "", prometheus.NewRegistry())
}

// TestRunnerMeter_NilUntilAttached covers the shape every test that
// drives run directly relies on: a runner with no meter is a runner
// with no telemetry, not a runner that panics. An unmeasured counter is
// not worth failing a source over.
func TestRunnerMeter_NilUntilAttached(t *testing.T) {
	r := testRunner(t, "c1")
	if m := r.meter("scope"); m != nil {
		t.Errorf("meter before attachMeter = %v, want nil", m)
	}
	// newRunner installs the no-op shutdown so realMain's deferred
	// flush is safe even for a runner whose attachMeter never ran.
	if err := r.meterShutdown(context.Background()); err != nil {
		t.Errorf("no-op meterShutdown: %v", err)
	}
}

// TestAttachMeter_ExportsThroughTheSharedRegistry is the end of the
// §8.4 pull path as the sentinel actually assembles it: a runner's
// instruments reach the one registry serveMetrics scrapes, under this
// runner's cluster label.
func TestAttachMeter_ExportsThroughTheSharedRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := testRunner(t, "c1")
	if err := r.attachMeter(context.Background(), reg); err != nil {
		t.Fatalf("attachMeter: %v", err)
	}
	t.Cleanup(func() { _ = r.meterShutdown(context.Background()) })

	m := r.meter("scope")
	if m == nil {
		t.Fatalf("meter after attachMeter = nil")
	}
	c, err := m.Int64Counter("probe.hits")
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	c.Add(context.Background(), 1)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var labels []string
	for _, f := range families {
		if f.GetName() != "probe_hits_total" {
			continue
		}
		for _, lp := range f.GetMetric()[0].GetLabel() {
			labels = append(labels, lp.GetName()+"="+lp.GetValue())
		}
	}
	if len(labels) == 0 {
		t.Fatalf("probe_hits_total absent from the shared registry")
	}
	var hasCluster bool
	for _, l := range labels {
		if l == "cluster=c1" {
			hasCluster = true
		}
	}
	if !hasCluster {
		t.Errorf("labels = %q, want cluster=c1 — the bridge lost the wrapper", labels)
	}
}

// TestAttachMeter_OneRegistryManyClusters is the multi-cluster case
// (#208): N runners, one process-global registry. Each bridge is a
// collector, so registering them without distinct const labels would be
// a duplicate-registration error at startup rather than a wrong number
// later.
func TestAttachMeter_OneRegistryManyClusters(t *testing.T) {
	reg := prometheus.NewRegistry()
	for _, cluster := range []string{"c1", "c2", "c3"} {
		r := testRunner(t, cluster)
		if err := r.attachMeter(context.Background(), reg); err != nil {
			t.Fatalf("attachMeter(%s): %v", cluster, err)
		}
		t.Cleanup(func() { _ = r.meterShutdown(context.Background()) })
	}
}

// TestAttachMeter_RejectsABadExporterMode keeps the misconfiguration
// loud: realMain fails startup on this rather than running a sentinel
// whose operator believes telemetry is being shipped.
//
// A bad --otel-exporter never reaches here — telemetry.Setup rejects it
// several steps earlier in realMain. What does reach here is
// OTEL_METRICS_EXPORTER, which the trace setup does not read, so this is
// the only place that mode is checked at all.
func TestAttachMeter_RejectsABadExporterMode(t *testing.T) {
	t.Setenv(telemetry.MetricsExporterEnvVar, "gcm")
	r := testRunner(t, "c1", "--otel-exporter=console")
	if err := r.attachMeter(context.Background(), prometheus.NewRegistry()); err == nil {
		t.Fatalf("attachMeter with an unknown exporter mode: want an error")
	}
}
