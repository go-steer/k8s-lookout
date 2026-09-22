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

// The OTLP push path's hardening (leeway design §8.4).
//
// The failure being designed against is an exporter with an unbounded
// queue and an unreachable endpoint, which is an OOM. Leeway's whole
// output is metrics, so its export path carries more points than any
// other source's, and the deployment most likely to black-hole its
// collector is the metrics-only one where leeway IS the product.
//
// The bound is structural rather than something added here, and it is
// worth stating plainly because "bounded queue, drop on full" describes
// a design this SDK does not have:
//
//   - A PeriodicReader holds NO queue of batches. On each tick it
//     collects the aggregation state into one ResourceMetrics and calls
//     Export synchronously, in its own goroutine. There is nothing to
//     grow: the pending data is one point per series, whether the last
//     hundred exports succeeded or none of them did.
//   - So the memory bound is the CARDINALITY bound (#475's controls),
//     not a queue depth, and "drop on full" is "the failed batch is
//     gone" — the reader does not re-offer it.
//   - And recording cannot block on the exporter, because the export
//     happens after collect returns and holds no lock a recorder wants.
//
// What this file adds is therefore: proof (the tests), visibility (the
// SLIs below — a dead collector has to be legible without reading logs
// or trusting the backend that is down), and the two deadline settings
// that stop a configuration from reintroducing the problem.
//
// One nuance the help strings cannot carry: temporality here is
// cumulative, so a failed export loses the SAMPLE, not the VALUE. The
// next export that lands carries the full running total. Dropped points
// mean the graph has a hole, not that the counter reset.

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The bounds on the per-export deadline.
//
// maxExportTimeout matches the SDK's own default, which is the number
// collectors are tuned against. minExportTimeout exists because a
// deadline shorter than a TCP handshake plus a TLS negotiation turns a
// merely slow collector into a permanently failing one.
const (
	maxExportTimeout = 30 * time.Second
	minExportTimeout = 5 * time.Second
)

// exportTimeout returns the per-attempt deadline for a given push
// interval.
//
// The invariant is that it is STRICTLY less than the interval, and that
// is the whole point of computing it rather than taking the SDK default.
// The reader's loop is sequential: collect, export, wait for the next
// tick. A deadline at or above the interval lets one black-holed export
// own the loop indefinitely, so the ticks it misses are not late — they
// never happen, and the export SLIs below stop moving at exactly the
// moment an operator needs them to. The SDK's defaults (30s deadline,
// 60s interval) satisfy this; the pair an operator can reach for —
// OTEL_METRIC_EXPORT_INTERVAL shortened without OTEL_METRIC_EXPORT_TIMEOUT
// — does not, which is why both are resolved here instead of left to the
// SDK (see resolvePushInterval and resolveExportTimeout).
//
// Half the interval, because the remaining half is the budget for the
// collect that precedes it and for a retry.
func exportTimeout(interval time.Duration) time.Duration {
	t := interval / 2
	if t > maxExportTimeout {
		t = maxExportTimeout
	}
	if t < minExportTimeout {
		t = minExportTimeout
	}
	// Intervals shorter than the floor — tests, mostly, but nothing
	// forbids one — would otherwise get a deadline larger than the
	// interval, which is the one thing this function exists to prevent.
	if t >= interval {
		t = interval * 3 / 4
	}
	if t <= 0 {
		t = time.Millisecond
	}
	return t
}

// The OTel-standard millisecond knobs for the push reader's cadence and
// its per-export deadline.
//
// The SDK reads both itself, but it reads them FIRST and then lets
// explicit options overwrite them (newPeriodicReaderConfig seeds from
// the environment, then applies the option list). Since this package has
// to pass WithInterval and WithTimeout — the deadline is derived from
// the interval, and deriving it from an interval the reader is not
// actually using is precisely the arrangement exportTimeout exists to
// rule out — leaving them to the SDK would silently ignore both. So they
// are resolved here, and the invariant is applied on top.
const (
	metricIntervalEnvVar = "OTEL_METRIC_EXPORT_INTERVAL"
	metricTimeoutEnvVar  = "OTEL_METRIC_EXPORT_TIMEOUT"
)

// envMillis reads an OTel-standard duration-in-milliseconds variable.
// A malformed value is reported and ignored rather than fatal: telemetry
// configuration should not be able to stop the watch from starting.
func envMillis(key string) (time.Duration, bool) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return 0, false
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		fmt.Fprintf(os.Stderr, "lookout: telemetry: %s=%s is not a positive whole number of milliseconds - ignoring\n", key, raw)
		return 0, false
	}
	return time.Duration(ms) * time.Millisecond, true
}

// resolvePushInterval picks the export cadence: an explicit option wins,
// then the environment, then the package default. Explicit-wins matches
// the SDK's own precedence, and the only callers that set the option are
// tests, which need it to be deterministic.
func resolvePushInterval(opt time.Duration) time.Duration {
	if opt > 0 {
		return opt
	}
	if d, ok := envMillis(metricIntervalEnvVar); ok {
		return d
	}
	return DefaultPushInterval
}

// resolveExportTimeout honours an operator-set deadline while keeping it
// strictly below the interval.
//
// A request above the ceiling is clamped rather than refused. Asking for
// a long deadline is an operator saying their collector is slow, and the
// right answer to that is a longer interval — which the same operator can
// also set, and which then raises this ceiling with it. Refusing to start
// over a telemetry knob would be a worse trade.
func resolveExportTimeout(interval time.Duration) time.Duration {
	requested, ok := envMillis(metricTimeoutEnvVar)
	if !ok {
		return exportTimeout(interval)
	}
	ceiling := interval * 3 / 4
	if ceiling <= 0 {
		ceiling = time.Millisecond
	}
	if requested > ceiling {
		fmt.Fprintf(os.Stderr, "lookout: telemetry: %s is %s but the export interval is %s - capping the deadline at %s so a stalled export cannot swallow the next tick\n",
			metricTimeoutEnvVar, requested, interval, ceiling)
		return ceiling
	}
	return requested
}

// Help strings. Kept as constants so the docs enumeration and the
// registered collectors cannot disagree, and quote-free because the
// metrics-page generator parses them out of Desc.String() (see
// internal/watch.descRe).
const (
	helpExports = "Total OTLP metric export attempts, by outcome (ok: the collector accepted the batch; failed: it did not, and the batch was dropped). A rising failed rate means the OTLP backend is stale; the scrape endpoint is unaffected."

	helpPointsExported = "Total metric data points the OTLP exporter delivered to the collector."

	helpPointsDropped = "Total metric data points discarded because their OTLP export failed or ran out of time. There is no retry queue by design, so a dead collector costs samples rather than memory; temporality is cumulative, so what is lost is the sample and not the counter value."

	helpLastSuccess = "Unix timestamp of the last OTLP metric export the collector accepted; zero until the first one. Alert on its age rather than on the failure counter alone, which stays flat when the export path stops running at all."

	helpInflight = "1 while an OTLP metric export is in flight, 0 otherwise. A reading stuck at 1 across scrapes is an export wedged against its deadline."
)

// exportSLIs is how the push path reports on itself.
//
// These are plain Prometheus collectors on the sentinel's own registry,
// deliberately NOT instruments on the MeterProvider they describe. An
// export-failure counter that only reaches the operator over the export
// path that is failing tells nobody anything; the pull endpoint is the
// one surface guaranteed to still work when the collector is gone,
// which is the same argument §8.4 makes for keeping the pull reader
// unconditional.
type exportSLIs struct {
	exports        *prometheus.CounterVec
	pointsExported prometheus.Counter
	pointsDropped  prometheus.Counter
	lastSuccess    prometheus.Gauge
	inflight       prometheus.Gauge
}

func newExportSLIs() *exportSLIs {
	return &exportSLIs{
		exports: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lookout_otlp_exports_total",
			Help: helpExports,
		}, []string{"outcome"}),
		pointsExported: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lookout_otlp_points_exported_total",
			Help: helpPointsExported,
		}),
		pointsDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lookout_otlp_points_dropped_total",
			Help: helpPointsDropped,
		}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lookout_otlp_export_last_success_timestamp_seconds",
			Help: helpLastSuccess,
		}),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lookout_otlp_export_inflight",
			Help: helpInflight,
		}),
	}
}

// collectors enumerates the struct's fields for registration and for
// the docs rows. Hand-written rather than reflected so the ORDER is
// chosen; TestExportSLIs_EveryFieldIsEnumerated fails when a field is
// added without one.
func (s *exportSLIs) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		s.exports,
		s.pointsExported,
		s.pointsDropped,
		s.lastSuccess,
		s.inflight,
	}
}

// register puts every SLI on the caller's registry.
//
// The counters are pre-initialised at zero for both outcomes: a reader
// has to be able to tell "no export has failed" from "nothing is
// exporting", and a CounterVec child that has never been incremented
// does not exist as a series.
func (s *exportSLIs) register(reg prometheus.Registerer) error {
	for _, c := range s.collectors() {
		if err := reg.Register(c); err != nil {
			return fmt.Errorf("telemetry: metrics: otlp export SLIs: %w", err)
		}
	}
	s.exports.WithLabelValues("ok")
	s.exports.WithLabelValues("failed")
	return nil
}

// ExportSLIDoc documents one OTLP export SLI for the generated metrics
// reference. The name and help are derived from the collector by the
// caller, the way every other sentinel metric's row is; only the type
// and label columns are stamped, because the Prometheus client does not
// expose them before first observation.
type ExportSLIDoc struct {
	Collector prometheus.Collector
	Type      string
	Labels    []string
}

// ExportSLIDocs returns a row per OTLP export SLI.
//
// Every one of them is optional on the generated page: unlike the
// cardinality and drift series, which read zero to prove they are
// looking, these describe a pipeline that does not exist unless
// --otel-exporter=otlp built one. A zeroed export counter on a process
// with no exporter would claim an export path that was never wired.
func ExportSLIDocs() []ExportSLIDoc {
	s := newExportSLIs()
	return []ExportSLIDoc{
		{s.exports, "counter", []string{"outcome"}},
		{s.pointsExported, "counter", nil},
		{s.pointsDropped, "counter", nil},
		{s.lastSuccess, "gauge", nil},
		{s.inflight, "gauge", nil},
	}
}

// guardedExporter is the sdkmetric.Exporter the periodic reader drives:
// the real OTLP exporter, plus a deadline and the SLIs.
//
// The deadline is applied here as well as on the reader on purpose. The
// reader's covers collect-plus-export and is the one that keeps the
// loop ticking; this one covers the export alone and survives somebody
// dropping the reader option, which is the easier of the two to lose in
// a refactor.
type guardedExporter struct {
	inner   sdkmetric.Exporter
	timeout time.Duration
	slis    *exportSLIs
	now     func() time.Time
}

func newGuardedExporter(inner sdkmetric.Exporter, timeout time.Duration, slis *exportSLIs) *guardedExporter {
	return &guardedExporter{inner: inner, timeout: timeout, slis: slis, now: time.Now}
}

func (g *guardedExporter) Temporality(k sdkmetric.InstrumentKind) metricdata.Temporality {
	return g.inner.Temporality(k)
}

func (g *guardedExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return g.inner.Aggregation(k)
}

// Export sends one batch and records what happened to it.
//
// The error is returned rather than swallowed. It is what ForceFlush
// and Shutdown propagate to their callers, and a shutdown that reports
// success after losing its final flush is worse than a noisy one. The
// SDK's error handler additionally prints it, at most once per
// interval; the SLIs are what to alert on, because they survive a log
// pipeline that is down for the same reason the collector is.
func (g *guardedExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	points := countDataPoints(rm)

	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	g.slis.inflight.Set(1)
	err := g.inner.Export(ctx, rm)
	g.slis.inflight.Set(0)

	if err != nil {
		g.slis.exports.WithLabelValues("failed").Inc()
		g.slis.pointsDropped.Add(float64(points))
		return err
	}
	g.slis.exports.WithLabelValues("ok").Inc()
	g.slis.pointsExported.Add(float64(points))
	g.slis.lastSuccess.Set(float64(g.now().UnixNano()) / float64(time.Second))
	return nil
}

func (g *guardedExporter) ForceFlush(ctx context.Context) error { return g.inner.ForceFlush(ctx) }

func (g *guardedExporter) Shutdown(ctx context.Context) error { return g.inner.Shutdown(ctx) }

// countDataPoints is how many points a batch carried, which is what
// makes the drop counter a number an operator can reason about: one
// failed export is one line on a graph, but ten thousand lost points is
// a scale.
func countDataPoints(rm *metricdata.ResourceMetrics) int {
	if rm == nil {
		return 0
	}
	n := 0
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			n += dataPointCount(m.Data)
		}
	}
	return n
}

// dataPointCount takes `any` rather than metricdata.Aggregation
// deliberately. The interface is sealed by an unexported method, so a
// parameter of that type cannot be handed a shape from outside the SDK
// — and the default branch below, which is the one that matters, would
// then be untestable as well as unreachable.
func dataPointCount(a any) int {
	switch v := a.(type) {
	case metricdata.Gauge[int64]:
		return len(v.DataPoints)
	case metricdata.Gauge[float64]:
		return len(v.DataPoints)
	case metricdata.Sum[int64]:
		return len(v.DataPoints)
	case metricdata.Sum[float64]:
		return len(v.DataPoints)
	case metricdata.Histogram[int64]:
		return len(v.DataPoints)
	case metricdata.Histogram[float64]:
		return len(v.DataPoints)
	case metricdata.ExponentialHistogram[int64]:
		return len(v.DataPoints)
	case metricdata.ExponentialHistogram[float64]:
		return len(v.DataPoints)
	case metricdata.Summary:
		return len(v.DataPoints)
	case nil:
		return 0
	default:
		// An aggregation shape a later SDK adds still moved data.
		// Counting it as one point keeps an unknown type reading as a
		// small undercount rather than as no drops at all.
		return 1
	}
}
