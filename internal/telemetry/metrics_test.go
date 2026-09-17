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

package telemetry

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/metric"
)

// gatherNames returns the metric names a registry currently exposes.
func gatherNames(t *testing.T, reg *prometheus.Registry) []string {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := make([]string, 0, len(families))
	for _, f := range families {
		out = append(out, f.GetName())
	}
	slices.Sort(out)
	return out
}

// declare records one sample through mp so the bridge has something to
// export, and returns the registry's view.
func declare(t *testing.T, mp metric.MeterProvider, reg *prometheus.Registry) []string {
	t.Helper()
	c, err := mp.Meter("test").Int64Counter("probe.hits")
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	c.Add(context.Background(), 1)
	return gatherNames(t, reg)
}

// TestSetupMetrics_PullReaderIsUnconditional is the property the whole
// function exists to hold: /metrics is the sentinel's contract, and
// --otel-exporter is a statement about shipping telemetry OFF-box. No
// value of it may empty the scrape endpoint.
func TestSetupMetrics_PullReaderIsUnconditional(t *testing.T) {
	for _, mode := range []string{"", ModeNone, ModeConsole} {
		t.Run("mode="+mode, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			mp, shutdown, err := SetupMetrics(context.Background(), MetricsOptions{
				Mode:       mode,
				Registerer: reg,
				Cluster:    "c1",
			})
			if err != nil {
				t.Fatalf("SetupMetrics: %v", err)
			}
			t.Cleanup(func() { _ = shutdown(context.Background()) })

			if got := declare(t, mp, reg); !slices.Contains(got, "probe_hits_total") {
				t.Errorf("registry exposes %q, want probe_hits_total", got)
			}
		})
	}
}

// TestSetupMetrics_BridgesIntoTheCallersRegistry pins that the bridge
// registers INTO the registry it is handed rather than standing up a
// second one — that is what puts OTel-declared metrics beside the
// hand-rolled collectors in one scrape, on one port, under one label.
func TestSetupMetrics_BridgesIntoTheCallersRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	wrapped := prometheus.WrapRegistererWith(prometheus.Labels{"cluster": "c1"}, reg)
	mp, shutdown, err := SetupMetrics(context.Background(), MetricsOptions{
		Mode:       ModeNone,
		Registerer: wrapped,
		Cluster:    "c1",
	})
	if err != nil {
		t.Fatalf("SetupMetrics: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	c, err := mp.Meter("test").Int64Counter("probe.hits")
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	c.Add(context.Background(), 1)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var found bool
	for _, f := range families {
		if f.GetName() != "probe_hits_total" {
			continue
		}
		found = true
		// The wrapper's const label has to survive the bridge, or a
		// multi-cluster process serves N indistinguishable series.
		var labels []string
		for _, lp := range f.GetMetric()[0].GetLabel() {
			labels = append(labels, lp.GetName()+"="+lp.GetValue())
		}
		if !slices.Contains(labels, "cluster=c1") {
			t.Errorf("labels = %q, want a cluster=c1 among them", labels)
		}
	}
	if !found {
		t.Errorf("probe_hits_total not in the caller's registry")
	}
}

// TestSetupMetrics_TwoProvidersOneRegistry covers the multi-cluster
// shape: one registry, one provider per cluster, distinguished by the
// caller's label wrapper. A second bridge registering the same series
// unwrapped would be a duplicate-registration error, so this is the
// test that would catch dropping the wrapper.
func TestSetupMetrics_TwoProvidersOneRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	for _, cluster := range []string{"c1", "c2"} {
		_, shutdown, err := SetupMetrics(context.Background(), MetricsOptions{
			Mode:       ModeNone,
			Registerer: prometheus.WrapRegistererWith(prometheus.Labels{"cluster": cluster}, reg),
			Cluster:    cluster,
		})
		if err != nil {
			t.Fatalf("SetupMetrics(%s): %v", cluster, err)
		}
		t.Cleanup(func() { _ = shutdown(context.Background()) })
	}
}

func TestSetupMetrics_RejectsAMissingRegisterer(t *testing.T) {
	_, shutdown, err := SetupMetrics(context.Background(), MetricsOptions{Mode: ModeNone})
	if err == nil {
		t.Fatalf("SetupMetrics with no Registerer: want an error")
	}
	// The error path still returns a callable shutdown, so a caller that
	// defers before checking does not nil-panic on the way out.
	if shutdown == nil {
		t.Fatalf("shutdown is nil on the error path")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("no-op shutdown: %v", err)
	}
}

func TestSetupMetrics_RejectsAnUnknownMode(t *testing.T) {
	_, _, err := SetupMetrics(context.Background(), MetricsOptions{
		Mode:       "gcm",
		Registerer: prometheus.NewRegistry(),
	})
	if err == nil {
		t.Fatalf("SetupMetrics(mode=gcm): want an error")
	}
	if !strings.Contains(err.Error(), `"gcm"`) {
		t.Errorf("error %q does not name the rejected mode", err)
	}
}

// TestSetupMetrics_EnvOverridesTheFlag mirrors Setup's treatment of
// OTEL_TRACES_EXPORTER, and for the same reason: a fleet shares one
// manifest and differs by a per-Deployment env patch.
func TestSetupMetrics_EnvOverridesTheFlag(t *testing.T) {
	t.Setenv(MetricsExporterEnvVar, "gcm")
	_, _, err := SetupMetrics(context.Background(), MetricsOptions{
		Mode:       ModeNone, // valid; the env var is what must be read
		Registerer: prometheus.NewRegistry(),
	})
	if err == nil {
		t.Fatalf("want the env var's invalid mode to be rejected")
	}
	if !strings.Contains(err.Error(), `"gcm"`) {
		t.Errorf("error %q does not name the mode from %s", err, MetricsExporterEnvVar)
	}
}

func TestNewMetricsResource_CarriesTheCluster(t *testing.T) {
	res, err := newMetricsResource("std-simian-test")
	if err != nil {
		t.Fatalf("newMetricsResource: %v", err)
	}
	var got string
	for _, kv := range res.Attributes() {
		if string(kv.Key) == "k8s.cluster.name" {
			got = kv.Value.AsString()
		}
	}
	if got != "std-simian-test" {
		t.Errorf("k8s.cluster.name = %q, want std-simian-test", got)
	}

	// An unnamed cluster must not stamp an empty attribute — an empty
	// value is a distinct series, not an absent one.
	base, err := newMetricsResource("  ")
	if err != nil {
		t.Fatalf("newMetricsResource(blank): %v", err)
	}
	for _, kv := range base.Attributes() {
		if string(kv.Key) == "k8s.cluster.name" {
			t.Errorf("blank cluster still stamped k8s.cluster.name=%q", kv.Value.AsString())
		}
	}
}
