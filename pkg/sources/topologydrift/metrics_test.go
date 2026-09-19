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

package topologydrift

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	v1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// promHarness is the §8.4 export pipeline in miniature: OTEL-native
// instruments on a MeterProvider whose pull reader is registered into a
// Prometheus registry the process already owns.
type promHarness struct {
	registry *prometheus.Registry
	in       *instruments
}

func newPromHarness(t *testing.T, opts metricsOptions) *promHarness {
	t.Helper()
	reg := prometheus.NewRegistry()
	exporter, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		otelprom.WithoutTargetInfo(),
		otelprom.WithoutScopeInfo(),
	)
	if err != nil {
		t.Fatalf("otelprom.New: %v", err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	opts.Meter = mp.Meter(MeterName)
	in, err := newInstruments(opts)
	if err != nil {
		t.Fatalf("newInstruments: %v", err)
	}
	t.Cleanup(func() { _ = in.Close() })
	return &promHarness{registry: reg, in: in}
}

// names returns the metric names the registry currently exposes.
func (h *promHarness) names(t *testing.T) []string {
	t.Helper()
	families, err := h.registry.Gather()
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

// family returns the gathered family with the given name.
func (h *promHarness) family(t *testing.T, name string) *dto.MetricFamily {
	t.Helper()
	families, err := h.registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

func fullOptions() metricsOptions {
	return metricsOptions{
		PerDomainSeries: true,
		SubjectCounts: func() map[leeway.SubjectKind]int64 {
			return map[leeway.SubjectKind]int64{leeway.SubjectDeployment: 3, leeway.SubjectDaemonSet: 1}
		},
		DomainNodes: func() map[leeway.TopologyKey]map[leeway.Domain]int64 {
			return map[leeway.TopologyKey]map[leeway.Domain]int64{
				zoneKey: {"us-central1-a": 7, "us-central1-b": 5},
			}
		},
		DomainObjects: func(yield countObserver) {
			yield(subA, zoneKey, "us-central1-a", leeway.StateRunning, 4)
			yield(subA, zoneKey, "us-central1-b", leeway.StatePending, 1)
		},
		Intents: func(yield intentObserver) {
			skew := int32(1)
			yield(subA, zoneKey, &leeway.Intent{
				TopologyKey:       zoneKey,
				Mode:              leeway.ModeSpread,
				Source:            leeway.SourceTopologySpreadConstraint,
				MaxSkew:           &skew,
				WhenUnsatisfiable: v1.DoNotSchedule,
				Confidence:        leeway.ConfidenceDeclared,
				Weighting:         leeway.WeightEqual,
			})
		},
	}
}

// TestInstrumentNames_PrometheusSpelling pins the derived Prometheus names
// against the real exporter.
//
// §8.4 declares instruments in OpenTelemetry form and lets the exporter derive
// the Prometheus spelling, on the grounds that hand-maintaining two lists
// guarantees drift. The derivation is a dependency's behaviour, and the one
// piece of it that is easy to get wrong is the unit: it is injected into the
// *middle* of the name, ahead of any `_total`, so an instrument that sets
// WithUnit gets a name nobody chose. This test is what turns that from a
// paragraph in a design document into something a dependency bump cannot break
// silently.
func TestInstrumentNames_PrometheusSpelling(t *testing.T) {
	h := newPromHarness(t, fullOptions())
	ctx := context.Background()
	h.in.recordEvent(ctx, resourcePod, time.Unix(1_700_000_000, 0))
	h.in.recordEvaluation(ctx, leeway.SubjectDeployment, 250*time.Millisecond)
	// A counter with no Add is not exported at all, so the one metric we hope
	// never moves in production has to move here or it cannot be checked.
	h.in.recordMismatch(ctx, leeway.SubjectDeployment)

	want := []string{
		"lookout_leeway_domain_objects",
		"lookout_leeway_domain_ready_nodes",
		"lookout_leeway_intent_info",
		// Unit "s" lands before nothing at all on a gauge, so the name reads
		// as the Prometheus convention for a unix timestamp.
		"lookout_leeway_last_event_timestamp_seconds",
		"lookout_leeway_evaluation_duration_seconds",
		"lookout_leeway_subjects_tracked",
		// The other half of the unit hazard: `_total` is appended to a
		// monotonic counter, and it is appended AFTER any unit. This one sets
		// no unit, so the declared name simply gains the suffix.
		"lookout_leeway_counter_mismatch_total",
	}
	slices.Sort(want)

	if got := h.names(t); !slices.Equal(got, want) {
		t.Errorf("exported metric names:\n got %q\nwant %q", got, want)
	}
}

// TestMetricDocs_MatchTheExporter is what makes MetricDocs safe to publish.
//
// The docs generator cannot derive these rows the way it derives every other
// sentinel metric — there is no prometheus.Collector to Describe — so the
// names, types and labels are written by hand. This gathers a real bridged
// registry with every optional series turned on and holds the hand-written
// rows to what actually came out.
func TestMetricDocs_MatchTheExporter(t *testing.T) {
	h := newPromHarness(t, fullOptions())
	ctx := context.Background()
	h.in.recordEvent(ctx, resourcePod, time.Unix(1_700_000_000, 0))
	h.in.recordEvaluation(ctx, leeway.SubjectDeployment, 250*time.Millisecond)
	// A counter with no Add is not exported at all, so the one metric we hope
	// never moves in production has to move here or it cannot be checked.
	h.in.recordMismatch(ctx, leeway.SubjectDeployment)

	docs := MetricDocs()
	names := make([]string, 0, len(docs))
	for _, d := range docs {
		names = append(names, d.Name)
	}
	slices.Sort(names)
	if got := h.names(t); !slices.Equal(got, names) {
		t.Fatalf("MetricDocs names:\n got %q\nwant %q", names, got)
	}

	promType := map[dto.MetricType]string{
		dto.MetricType_GAUGE:     "gauge",
		dto.MetricType_COUNTER:   "counter",
		dto.MetricType_HISTOGRAM: "histogram",
	}
	for _, d := range docs {
		t.Run(d.Name, func(t *testing.T) {
			fam := h.family(t, d.Name)
			if fam == nil {
				t.Fatalf("not exported")
			}
			if got := promType[fam.GetType()]; got != d.Type {
				t.Errorf("type = %q, doc says %q", got, d.Type)
			}
			if got := fam.GetHelp(); got != d.Help {
				t.Errorf("help =\n %q\ndoc says\n %q", got, d.Help)
			}
			var got []string
			for _, lp := range fam.GetMetric()[0].GetLabel() {
				got = append(got, lp.GetName())
			}
			want := slices.Clone(d.Labels)
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("labels = %q, doc says %q", got, want)
			}
		})
	}
}

// TestMetricDocs_OptionalMatchesTheFlag pins the Optional column: exactly the
// rows marked optional are the ones missing from a default-configured scrape.
func TestMetricDocs_OptionalMatchesTheFlag(t *testing.T) {
	opts := fullOptions()
	opts.PerDomainSeries = false
	h := newPromHarness(t, opts)
	ctx := context.Background()
	h.in.recordEvent(ctx, resourcePod, time.Unix(1_700_000_000, 0))
	h.in.recordEvaluation(ctx, leeway.SubjectDeployment, 250*time.Millisecond)
	// A counter with no Add is not exported at all, so the one metric we hope
	// never moves in production has to move here or it cannot be checked.
	h.in.recordMismatch(ctx, leeway.SubjectDeployment)

	exported := h.names(t)
	for _, d := range MetricDocs() {
		present := slices.Contains(exported, d.Name)
		if present == d.Optional {
			t.Errorf("%s: exported=%v with PerDomainSeries off, but Optional=%v", d.Name, present, d.Optional)
		}
	}
}

func TestInstruments_ObservableGaugesCarryTheirAttributes(t *testing.T) {
	h := newPromHarness(t, fullOptions())

	t.Run("subjects_tracked is split by kind", func(t *testing.T) {
		f := h.family(t, "lookout_leeway_subjects_tracked")
		if f == nil {
			t.Fatal("family absent")
		}
		got := map[string]float64{}
		for _, m := range f.GetMetric() {
			got[labelValue(m, "subject_kind")] = m.GetGauge().GetValue()
		}
		if got["Deployment"] != 3 || got["DaemonSet"] != 1 {
			t.Errorf("subjects_tracked = %v, want Deployment 3 and DaemonSet 1", got)
		}
	})

	t.Run("domain_ready_nodes is split by axis and domain", func(t *testing.T) {
		f := h.family(t, "lookout_leeway_domain_ready_nodes")
		if f == nil {
			t.Fatal("family absent")
		}
		if len(f.GetMetric()) != 2 {
			t.Fatalf("got %d series, want 2", len(f.GetMetric()))
		}
		for _, m := range f.GetMetric() {
			if labelValue(m, "topology_key") != string(zoneKey) {
				t.Errorf("topology_key = %q, want %q", labelValue(m, "topology_key"), zoneKey)
			}
			if d := labelValue(m, "domain"); d != "us-central1-a" && d != "us-central1-b" {
				t.Errorf("unexpected domain %q", d)
			}
		}
	})

	t.Run("domain_objects carries the subject and the state", func(t *testing.T) {
		f := h.family(t, "lookout_leeway_domain_objects")
		if f == nil {
			t.Fatal("family absent")
		}
		var running *dto.Metric
		for _, m := range f.GetMetric() {
			if labelValue(m, "state") == "running" {
				running = m
			}
		}
		if running == nil {
			t.Fatal("no running series")
		}
		for key, want := range map[string]string{
			"subject_kind": "Deployment",
			"namespace":    "prod",
			"subject":      "a",
			"domain":       "us-central1-a",
		} {
			if got := labelValue(running, key); got != want {
				t.Errorf("%s = %q, want %q", key, got, want)
			}
		}
		if running.GetGauge().GetValue() != 4 {
			t.Errorf("value = %v, want 4", running.GetGauge().GetValue())
		}
	})
}

// labelValue reads one label off a gathered metric.
func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

func TestInstruments_PerDomainSeriesIsOffByDefault(t *testing.T) {
	// The expensive series must not appear unless asked for. §8.4: at the
	// baseline row this is ~480k series against ~3.5k for everything else,
	// and under managed Prometheus that is a bill rather than a failure, so
	// nothing about the default posture announces itself.
	opts := fullOptions()
	opts.PerDomainSeries = false
	h := newPromHarness(t, opts)

	if slices.Contains(h.names(t), "lookout_leeway_domain_objects") {
		t.Error("domain_objects was exported with PerDomainSeries off")
	}
	if !slices.Contains(h.names(t), "lookout_leeway_subjects_tracked") {
		t.Error("the cheap aggregate series went missing with PerDomainSeries off")
	}
}

func TestInstruments_MissingCallbacksAreNotAPanic(t *testing.T) {
	// A source constructed without state wired in — which is what a partially
	// built process looks like — must scrape clean rather than crash the
	// handler serving /metrics for every other source.
	h := newPromHarness(t, metricsOptions{PerDomainSeries: true})
	if got := h.names(t); len(got) != 0 {
		t.Errorf("names = %q, want nothing observed", got)
	}
}

func TestInstruments_NilMeterUsesTheNoopProvider(t *testing.T) {
	in, err := newInstruments(metricsOptions{})
	if err != nil {
		t.Fatalf("newInstruments with a nil meter: %v", err)
	}
	// Recording against the no-op provider must be safe, since this is the
	// shape every unit test and every telemetry-less process runs in.
	in.recordEvent(context.Background(), resourceNode, time.Now())
	in.recordEvaluation(context.Background(), leeway.SubjectJob, time.Second)
	if err := in.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestInstruments_CloseIsIdempotentAndNilSafe(t *testing.T) {
	var nilIn *instruments
	if err := nilIn.Close(); err != nil {
		t.Errorf("Close on a nil *instruments: %v", err)
	}
	// Run's deferred Close can fire on a path where the source already closed.
	h := newPromHarness(t, metricsOptions{})
	if err := h.in.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := h.in.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestInstruments_NilRecordersAreSafe(t *testing.T) {
	var in *instruments
	in.recordEvent(context.Background(), resourcePod, time.Now())
	in.recordEvaluation(context.Background(), leeway.SubjectDeployment, time.Second)
	in.recordMismatch(context.Background(), leeway.SubjectDeployment)
}

// brokenMeter fails to declare one named instrument and otherwise behaves like
// the no-op meter. It exists because every declaration in newInstruments
// returns an error that no configuration can trigger, and an unexercised error
// path in startup code is where a nil-pointer panic hides.
type brokenMeter struct {
	metric.Meter
	fail string
}

var errDeclare = errors.New("declare failed")

func (m brokenMeter) Int64Gauge(name string, opts ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	if name == m.fail {
		return nil, errDeclare
	}
	return m.Meter.Int64Gauge(name, opts...)
}

func (m brokenMeter) Float64Histogram(name string, opts ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	if name == m.fail {
		return nil, errDeclare
	}
	return m.Meter.Float64Histogram(name, opts...)
}

func (m brokenMeter) Int64Counter(name string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	if name == m.fail {
		return nil, errDeclare
	}
	return m.Meter.Int64Counter(name, opts...)
}

func (m brokenMeter) Int64ObservableGauge(name string, opts ...metric.Int64ObservableGaugeOption) (metric.Int64ObservableGauge, error) {
	if name == m.fail {
		return nil, errDeclare
	}
	return m.Meter.Int64ObservableGauge(name, opts...)
}

func (m brokenMeter) RegisterCallback(f metric.Callback, insts ...metric.Observable) (metric.Registration, error) {
	if m.fail == "callback" {
		return nil, errDeclare
	}
	return m.Meter.RegisterCallback(f, insts...)
}

func TestNewInstruments_ReportsDeclarationFailures(t *testing.T) {
	for _, fail := range []string{
		metricLastEvent,
		metricEvalDuration,
		metricCounterMismatch,
		metricSubjectsTracked,
		metricDomainReadyNodes,
		metricDomainObjects,
		"callback",
	} {
		t.Run(fail, func(t *testing.T) {
			meter := brokenMeter{Meter: noop.NewMeterProvider().Meter(MeterName), fail: fail}
			in, err := newInstruments(metricsOptions{Meter: meter})
			if err == nil {
				t.Fatalf("newInstruments succeeded with %s failing", fail)
			}
			if in != nil {
				t.Error("a failed newInstruments returned a non-nil result")
			}
			if !errors.Is(err, errDeclare) {
				t.Errorf("error %v does not wrap the cause", err)
			}
			if !strings.Contains(err.Error(), "topologydrift") {
				t.Errorf("error %q does not name the source", err)
			}
		})
	}
}

// TestSkewLabel: a required podAntiAffinity is a ceiling of one per domain and
// not a skew of one, so the label has to be able to say which bound it is
// reporting — and "none" rather than a number it invented when there is neither.
func TestSkewLabel(t *testing.T) {
	for name, tc := range map[string]struct {
		in   *leeway.Intent
		want string
	}{
		"max skew":       {&leeway.Intent{MaxSkew: ptr(int32(3))}, "3"},
		"max per domain": {&leeway.Intent{MaxPerDomain: ptr(int64(1))}, "max-per-domain=1"},
		"neither":        {&leeway.Intent{}, "none"},
		// An intent carrying both is a spread constraint on an axis that also
		// bounds a domain; the skew is the tighter statement and wins.
		"both": {&leeway.Intent{MaxSkew: ptr(int32(2)), MaxPerDomain: ptr(int64(1))}, "2"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := skewLabel(tc.in); got != tc.want {
				t.Errorf("skewLabel = %q, want %q", got, tc.want)
			}
		})
	}
}
