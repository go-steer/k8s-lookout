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
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// ---------------------------------------------------------------------
// test exporters
// ---------------------------------------------------------------------

// fakeExporter is an sdkmetric.Exporter whose Export is whatever the
// test says it is.
type fakeExporter struct {
	export func(context.Context, *metricdata.ResourceMetrics) error
}

func (f *fakeExporter) Temporality(k sdkmetric.InstrumentKind) metricdata.Temporality {
	return sdkmetric.DefaultTemporalitySelector(k)
}

func (f *fakeExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(k)
}

func (f *fakeExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	if f.export == nil {
		return nil
	}
	return f.export(ctx, rm)
}

func (f *fakeExporter) ForceFlush(context.Context) error { return nil }
func (f *fakeExporter) Shutdown(context.Context) error   { return nil }

// blockingExporter wedges in Export until it is released, and does NOT
// honour the context while it is wedged. That non-cooperation is the
// point: the claim under test is that the recording path cannot block
// on the exporter, and an exporter that politely returns on ctx would
// prove a weaker thing (that the deadline works) than the one asserted.
type blockingExporter struct {
	fakeExporter
	entered     chan struct{}
	enterOnce   sync.Once
	release     chan struct{}
	releaseOnce sync.Once
}

func newBlockingExporter() *blockingExporter {
	b := &blockingExporter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	b.export = func(context.Context, *metricdata.ResourceMetrics) error {
		b.enterOnce.Do(func() { close(b.entered) })
		<-b.release
		return nil
	}
	return b
}

func (b *blockingExporter) unblock() { b.releaseOnce.Do(func() { close(b.release) }) }

// ---------------------------------------------------------------------
// registry helpers
// ---------------------------------------------------------------------

// sample reads one series out of a registry: the value of metric `name`
// whose labels are a superset of `match`.
func sample(t *testing.T, reg *prometheus.Registry, name string, match map[string]string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !labelsMatch(m, match) {
				continue
			}
			switch f.GetType() {
			case dto.MetricType_COUNTER:
				return m.GetCounter().GetValue(), true
			case dto.MetricType_GAUGE:
				return m.GetGauge().GetValue(), true
			default:
				t.Fatalf("%s: unsupported type %s", name, f.GetType())
			}
		}
	}
	return 0, false
}

func labelsMatch(m *dto.Metric, match map[string]string) bool {
	for k, want := range match {
		var found bool
		for _, lp := range m.GetLabel() {
			if lp.GetName() == k && lp.GetValue() == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func mustSample(t *testing.T, reg *prometheus.Registry, name string, match map[string]string) float64 {
	t.Helper()
	v, ok := sample(t, reg, name, match)
	if !ok {
		t.Fatalf("no series %s%v in the registry", name, match)
	}
	return v
}

// ---------------------------------------------------------------------
// the deadline invariant
// ---------------------------------------------------------------------

// TestExportTimeout_IsAlwaysStrictlyBelowTheInterval is the structural
// half of "a black-holed collector cannot stop the export loop". The
// reader collects, exports, and waits for the next tick in one
// goroutine, so a deadline at or above the interval lets a single
// wedged export own that loop forever — and the SLIs that would say so
// stop moving at the same instant.
func TestExportTimeout_IsAlwaysStrictlyBelowTheInterval(t *testing.T) {
	for _, interval := range []time.Duration{
		time.Millisecond,
		10 * time.Millisecond,
		time.Second,
		5 * time.Second,
		11 * time.Second,
		30 * time.Second,
		DefaultPushInterval,
		5 * time.Minute,
		time.Hour,
	} {
		got := exportTimeout(interval)
		if got <= 0 {
			t.Errorf("exportTimeout(%s) = %s, want a positive deadline", interval, got)
		}
		if got >= interval {
			t.Errorf("exportTimeout(%s) = %s, want strictly less than the interval", interval, got)
		}
	}
}

// TestExportTimeout_TracksTheIntervalWithinItsBounds pins the shape as
// well as the invariant: halve, then clamp. Without this, a change that
// returned a constant 1ms would satisfy the invariant above and make
// every export on a healthy-but-distant collector fail.
func TestExportTimeout_TracksTheIntervalWithinItsBounds(t *testing.T) {
	for _, tc := range []struct {
		interval time.Duration
		want     time.Duration
	}{
		{time.Minute, 30 * time.Second},      // the SDK's own defaults
		{10 * time.Minute, 30 * time.Second}, // clamped at the ceiling
		{40 * time.Second, 20 * time.Second}, // half
		{8 * time.Second, 5 * time.Second},   // half is below the floor
		{4 * time.Second, 3 * time.Second},   // the floor exceeds the interval
	} {
		if got := exportTimeout(tc.interval); got != tc.want {
			t.Errorf("exportTimeout(%s) = %s, want %s", tc.interval, got, tc.want)
		}
	}
}

// TestResolvePushInterval_PrefersTheOptionThenTheEnvironment pins the
// precedence. Explicit-wins matters because the option is the test seam:
// if a stray OTEL_METRIC_EXPORT_INTERVAL in someone's shell could
// override it, the timing tests below would be flaky for reasons nobody
// would find.
func TestResolvePushInterval_PrefersTheOptionThenTheEnvironment(t *testing.T) {
	if got := resolvePushInterval(0); got != DefaultPushInterval {
		t.Errorf("unset: got %s, want the package default %s", got, DefaultPushInterval)
	}

	t.Setenv(metricIntervalEnvVar, "15000")
	if got := resolvePushInterval(0); got != 15*time.Second {
		t.Errorf("from the environment: got %s, want 15s", got)
	}
	if got := resolvePushInterval(3 * time.Second); got != 3*time.Second {
		t.Errorf("explicit option: got %s, want 3s - the option must beat the environment", got)
	}

	// A malformed value is ignored rather than fatal: a typo in a
	// telemetry variable must not stop the watch from starting.
	for _, bad := range []string{"", "  ", "30s", "-1", "0", "nope"} {
		t.Setenv(metricIntervalEnvVar, bad)
		if got := resolvePushInterval(0); got != DefaultPushInterval {
			t.Errorf("%s=%q: got %s, want the default %s", metricIntervalEnvVar, bad, got, DefaultPushInterval)
		}
	}
}

// TestResolveExportTimeout_HonoursTheEnvironmentUpToTheInvariant is the
// reason this package reads the variable at all: the SDK applies the
// environment BEFORE the options, so passing WithTimeout would otherwise
// discard an operator's setting silently. Honour it — but never past the
// point where one stalled export swallows the next tick.
func TestResolveExportTimeout_HonoursTheEnvironmentUpToTheInvariant(t *testing.T) {
	// Unset: the derived value, unchanged.
	if got := resolveExportTimeout(time.Minute); got != exportTimeout(time.Minute) {
		t.Errorf("unset: got %s, want the derived %s", got, exportTimeout(time.Minute))
	}

	for _, tc := range []struct {
		raw      string
		interval time.Duration
		want     time.Duration
	}{
		{"5000", time.Minute, 5 * time.Second},               // honoured as-is
		{"45000", time.Minute, 45 * time.Second},             // above the derived value, still under the ceiling
		{"90000", time.Minute, 45 * time.Second},             // above the interval: capped
		{"60000", time.Minute, 45 * time.Second},             // equal to the interval: capped, the invariant is strict
		{"30000", 10 * time.Second, 7500 * time.Millisecond}, // a short interval cannot be overridden into blocking
		{"garbage", time.Minute, 30 * time.Second},           // ignored, falls back to derived
	} {
		t.Setenv(metricTimeoutEnvVar, tc.raw)
		got := resolveExportTimeout(tc.interval)
		if got != tc.want {
			t.Errorf("%s=%s with interval %s: got %s, want %s", metricTimeoutEnvVar, tc.raw, tc.interval, got, tc.want)
		}
		if got >= tc.interval {
			t.Errorf("%s=%s with interval %s: deadline %s is not strictly below the interval", metricTimeoutEnvVar, tc.raw, tc.interval, got)
		}
	}
}

// TestGuardedExporter_BoundsTheExportItself checks the inner half of
// the belt-and-braces: the guard applies its own deadline, so dropping
// the reader's WithTimeout in a refactor still leaves the export
// bounded.
func TestGuardedExporter_BoundsTheExportItself(t *testing.T) {
	var deadline time.Time
	var ok bool
	g := newGuardedExporter(&fakeExporter{
		export: func(ctx context.Context, _ *metricdata.ResourceMetrics) error {
			deadline, ok = ctx.Deadline()
			return nil
		},
	}, 2*time.Second, newExportSLIs())

	if err := g.Export(context.Background(), &metricdata.ResourceMetrics{}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if !ok {
		t.Fatalf("the inner exporter was handed a context with no deadline")
	}
	if d := time.Until(deadline); d <= 0 || d > 2*time.Second {
		t.Errorf("deadline is %s away, want (0, 2s]", d)
	}
}

// ---------------------------------------------------------------------
// the SLIs
// ---------------------------------------------------------------------

func TestGuardedExporter_CountsExportsAndDroppedPoints(t *testing.T) {
	reg := prometheus.NewRegistry()
	slis := newExportSLIs()
	if err := slis.register(reg); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Both outcomes exist at zero before anything happens: "no export
	// has failed" and "nothing is exporting" are different states and an
	// absent series cannot tell them apart.
	if got := mustSample(t, reg, "lookout_otlp_exports_total", map[string]string{"outcome": "failed"}); got != 0 {
		t.Errorf("pre-registered failed counter = %v, want 0", got)
	}

	batch := &metricdata.ResourceMetrics{ScopeMetrics: []metricdata.ScopeMetrics{{
		Metrics: []metricdata.Metrics{
			{Data: metricdata.Sum[int64]{DataPoints: make([]metricdata.DataPoint[int64], 3)}},
			{Data: metricdata.Gauge[float64]{DataPoints: make([]metricdata.DataPoint[float64], 4)}},
		},
	}}}

	boom := errors.New("collector unreachable")
	var fail bool
	g := newGuardedExporter(&fakeExporter{
		export: func(context.Context, *metricdata.ResourceMetrics) error {
			if fail {
				return boom
			}
			return nil
		},
	}, time.Second, slis)
	g.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	if err := g.Export(context.Background(), batch); err != nil {
		t.Fatalf("Export: %v", err)
	}
	fail = true
	if err := g.Export(context.Background(), batch); !errors.Is(err, boom) {
		t.Fatalf("Export = %v, want the inner error propagated (ForceFlush and Shutdown report it)", err)
	}
	if err := g.Export(context.Background(), batch); !errors.Is(err, boom) {
		t.Fatalf("Export = %v, want the inner error", err)
	}

	for _, tc := range []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"lookout_otlp_exports_total", map[string]string{"outcome": "ok"}, 1},
		{"lookout_otlp_exports_total", map[string]string{"outcome": "failed"}, 2},
		{"lookout_otlp_points_exported_total", nil, 7},
		{"lookout_otlp_points_dropped_total", nil, 14},
		{"lookout_otlp_export_last_success_timestamp_seconds", nil, 1_700_000_000},
		// Zero between exports, so a sustained 1 really does mean wedged.
		{"lookout_otlp_export_inflight", nil, 0},
	} {
		if got := mustSample(t, reg, tc.name, tc.labels); got != tc.want {
			t.Errorf("%s%v = %v, want %v", tc.name, tc.labels, got, tc.want)
		}
	}
}

// TestCountDataPoints_CoversEveryAggregationShape keeps the drop
// counter honest across the aggregation types the SDK can hand us. An
// uncounted shape reads as zero drops, which is the one wrong answer
// this metric can give.
func TestCountDataPoints_CoversEveryAggregationShape(t *testing.T) {
	for name, a := range map[string]metricdata.Aggregation{
		"gauge_int64":     metricdata.Gauge[int64]{DataPoints: make([]metricdata.DataPoint[int64], 2)},
		"gauge_float64":   metricdata.Gauge[float64]{DataPoints: make([]metricdata.DataPoint[float64], 2)},
		"sum_int64":       metricdata.Sum[int64]{DataPoints: make([]metricdata.DataPoint[int64], 2)},
		"sum_float64":     metricdata.Sum[float64]{DataPoints: make([]metricdata.DataPoint[float64], 2)},
		"hist_int64":      metricdata.Histogram[int64]{DataPoints: make([]metricdata.HistogramDataPoint[int64], 2)},
		"hist_float64":    metricdata.Histogram[float64]{DataPoints: make([]metricdata.HistogramDataPoint[float64], 2)},
		"exphist_int64":   metricdata.ExponentialHistogram[int64]{DataPoints: make([]metricdata.ExponentialHistogramDataPoint[int64], 2)},
		"exphist_float64": metricdata.ExponentialHistogram[float64]{DataPoints: make([]metricdata.ExponentialHistogramDataPoint[float64], 2)},
		"summary":         metricdata.Summary{DataPoints: make([]metricdata.SummaryDataPoint, 2)},
	} {
		if got := dataPointCount(a); got != 2 {
			t.Errorf("%s: dataPointCount = %d, want 2", name, got)
		}
	}
	if got := countDataPoints(nil); got != 0 {
		t.Errorf("countDataPoints(nil) = %d, want 0", got)
	}
	// An aggregation shape a later SDK adds must not read as no drops.
	if got := dataPointCount(struct{ metricdata.Aggregation }{}); got == 0 {
		t.Errorf("an unrecognised aggregation counted as zero points")
	}
}

// TestExportSLIs_EveryFieldIsEnumerated is what makes ExportSLIDocs
// complete by construction: a new SLI that is not in collectors() is
// neither registered nor documented, and both failures are silent.
func TestExportSLIs_EveryFieldIsEnumerated(t *testing.T) {
	s := newExportSLIs()
	v := reflect.ValueOf(*s)
	if got, want := len(s.collectors()), v.NumField(); got != want {
		t.Fatalf("collectors() lists %d, exportSLIs has %d fields — add the new one to collectors() (and so to ExportSLIDocs)", got, want)
	}
	if got, want := len(ExportSLIDocs()), v.NumField(); got != want {
		t.Fatalf("ExportSLIDocs has %d rows, exportSLIs has %d fields", got, want)
	}
	for i := range v.NumField() {
		if v.Field(i).IsNil() {
			t.Errorf("exportSLIs.%s is nil", v.Type().Field(i).Name)
		}
	}
	// Distinct collectors, not the same one listed N times.
	seen := map[any]bool{}
	for _, c := range s.collectors() {
		if seen[c] {
			t.Errorf("collectors() lists the same collector twice")
		}
		seen[c] = true
	}
}

// TestExportSLIDocs_MatchTheRegisteredCollectors ties the docs rows to
// the real registry, the way topologydrift's MetricDocs test does: the
// names on the generated metrics page are the names a scrape returns.
func TestExportSLIDocs_MatchTheRegisteredCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	for _, d := range ExportSLIDocs() {
		if err := reg.Register(d.Collector); err != nil {
			t.Fatalf("Register: %v", err)
		}
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	// Counters with no observation do not gather, so only the gauges are
	// visible here; that is enough to prove the rows describe real
	// collectors on a real registry rather than strings.
	for _, f := range families {
		var known bool
		for _, d := range ExportSLIDocs() {
			ch := make(chan *prometheus.Desc, 2)
			d.Collector.Describe(ch)
			close(ch)
			if desc := <-ch; desc != nil && strings.Contains(desc.String(), `"`+f.GetName()+`"`) {
				known = true
			}
		}
		if !known {
			t.Errorf("gathered %q, which no ExportSLIDocs row describes", f.GetName())
		}
	}
	for _, d := range ExportSLIDocs() {
		switch d.Type {
		case "counter", "gauge":
		default:
			t.Errorf("row type %q is not one the metrics page renders", d.Type)
		}
	}
}

// ---------------------------------------------------------------------
// the property the issue asks for by name
// ---------------------------------------------------------------------

// TestOTLPExport_TheRecordingPathCannotBlockOnTheExporter is #476's
// "Done when" test: it fails if the recording path ever becomes able to
// block on the exporter.
//
// A blocked recording path turns a collector outage into a DETECTION
// outage, which inverts the tool — the scoring pass must not slow down
// because something downstream is down. The exporter here wedges
// forever and ignores its context, and the assertion is not a timing
// one: the recording and the scrape are required to finish BEFORE the
// test releases the export, so completing is proof rather than a race
// that happened to go the right way.
func TestOTLPExport_TheRecordingPathCannotBlockOnTheExporter(t *testing.T) {
	blocked := newBlockingExporter()
	t.Cleanup(blocked.unblock)

	reg := prometheus.NewRegistry()
	mp, shutdown, err := SetupMetrics(context.Background(), MetricsOptions{
		Mode:         ModeOTLP,
		Registerer:   reg,
		Cluster:      "c1",
		PushInterval: 20 * time.Millisecond,
		pushExporter: blocked,
	})
	if err != nil {
		t.Fatalf("SetupMetrics: %v", err)
	}

	ctx := context.Background()
	counter, err := mp.Meter("test").Int64Counter("probe.hits")
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	gauge, err := mp.Meter("test").Float64Gauge("probe.level")
	if err != nil {
		t.Fatalf("Float64Gauge: %v", err)
	}
	counter.Add(ctx, 1) // so the first collect has something to export

	select {
	case <-blocked.entered:
	case <-time.After(30 * time.Second):
		t.Fatalf("the periodic reader never called Export")
	}

	// From here until unblock() below, an export is wedged.
	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		for i := range 100_000 {
			counter.Add(ctx, 1)
			gauge.Record(ctx, float64(i))
		}
		// /metrics stays unaffected: the pull reader is a second reader
		// over the same provider and has no reason to degrade when the
		// push side cannot reach anything.
		_, gerr := reg.Gather()
		done <- result{gerr}
	}()

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Gather while an export was wedged: %v", r.err)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("recording 100k points and scraping /metrics did not finish while an OTLP export was wedged — the recording path can now block on the exporter")
	}

	// Only now is the exporter allowed to return, which is what makes
	// the completion above mean what it says.
	blocked.unblock()

	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := shutdown(shutCtx); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

// TestOTLPExport_BothReadersReportTheSameValues is §12.1's consistency
// row. Two readers over one provider is the arrangement that lets the
// alerting pipeline not depend solely on OTLP; it is only worth having
// if the two agree, because an operator comparing a Grafana panel
// against a scrape has no way to tell a stale push from a wrong one.
func TestOTLPExport_BothReadersReportTheSameValues(t *testing.T) {
	var mu sync.Mutex
	pushed := map[string]float64{}
	capture := &fakeExporter{
		export: func(_ context.Context, rm *metricdata.ResourceMetrics) error {
			mu.Lock()
			defer mu.Unlock()
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
						for _, dp := range sum.DataPoints {
							pushed[m.Name] = float64(dp.Value)
						}
					}
				}
			}
			return nil
		},
	}

	reg := prometheus.NewRegistry()
	mp, shutdown, err := SetupMetrics(context.Background(), MetricsOptions{
		Mode:         ModeOTLP,
		Registerer:   reg,
		Cluster:      "c1",
		PushInterval: time.Hour, // never ticks; the flush below is the export
		pushExporter: capture,
	})
	if err != nil {
		t.Fatalf("SetupMetrics: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	counter, err := mp.Meter("test").Int64Counter("probe.hits")
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	for range 7 {
		counter.Add(context.Background(), 1)
	}

	sdkMP, ok := mp.(*sdkmetric.MeterProvider)
	if !ok {
		t.Fatalf("MeterProvider is %T, want the SDK's", mp)
	}
	if err := sdkMP.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	mu.Lock()
	push := pushed["probe.hits"]
	mu.Unlock()
	pull := mustSample(t, reg, "probe_hits_total", nil)
	if push != 7 || pull != 7 {
		t.Errorf("push reader saw %v, pull reader saw %v, want 7 from both", push, pull)
	}

	// And the export SLIs agree with the flush that just happened.
	if got := mustSample(t, reg, "lookout_otlp_exports_total", map[string]string{"outcome": "ok"}); got < 1 {
		t.Errorf("exports_total{ok} = %v after a successful flush, want at least 1", got)
	}
	if got := mustSample(t, reg, "lookout_otlp_export_last_success_timestamp_seconds", nil); got == 0 {
		t.Errorf("last_success_timestamp_seconds is still zero after a successful flush")
	}
}

// TestOTLPExport_ADeadCollectorRaisesTheDropSLIs is the small-scale
// version of the soak: a failing export has to be legible from the
// scrape endpoint, which is the surface that still works when the
// collector is the thing that is down.
func TestOTLPExport_ADeadCollectorRaisesTheDropSLIs(t *testing.T) {
	dead := &fakeExporter{
		export: func(context.Context, *metricdata.ResourceMetrics) error {
			return errors.New("connection refused")
		},
	}
	reg := prometheus.NewRegistry()
	mp, shutdown, err := SetupMetrics(context.Background(), MetricsOptions{
		Mode:         ModeOTLP,
		Registerer:   reg,
		Cluster:      "c1",
		PushInterval: time.Hour,
		pushExporter: dead,
	})
	if err != nil {
		t.Fatalf("SetupMetrics: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	counter, err := mp.Meter("test").Int64Counter("probe.hits")
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	counter.Add(context.Background(), 1)

	sdkMP, ok := mp.(*sdkmetric.MeterProvider)
	if !ok {
		t.Fatalf("MeterProvider is %T, want the SDK's", mp)
	}
	if err := sdkMP.ForceFlush(context.Background()); err == nil {
		t.Errorf("ForceFlush swallowed the export failure")
	}

	if got := mustSample(t, reg, "lookout_otlp_exports_total", map[string]string{"outcome": "failed"}); got < 1 {
		t.Errorf("exports_total{failed} = %v, want at least 1", got)
	}
	if got := mustSample(t, reg, "lookout_otlp_points_dropped_total", nil); got < 1 {
		t.Errorf("points_dropped_total = %v, want at least 1", got)
	}
	// The pull endpoint is untouched by the push side's failure.
	if got := mustSample(t, reg, "probe_hits_total", nil); got != 1 {
		t.Errorf("probe_hits_total = %v on /metrics, want 1 — the pull reader degraded with the push one", got)
	}
}
