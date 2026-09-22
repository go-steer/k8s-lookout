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
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// MetricsExporterEnvVar names the OTel-standard env var that overrides
// the flag-supplied exporter mode for METRICS, the way
// TracesExporterEnvVar does for spans. Same reason: fleet deployments
// share one manifest and differ by a per-Deployment env patch.
const MetricsExporterEnvVar = "OTEL_METRICS_EXPORTER"

// DefaultPushInterval is how often the OTLP reader exports. One minute
// matches the collection interval leeway's design assumes and keeps the
// push path far cheaper than the scrape path, which is the one built
// for sub-minute resolution.
const DefaultPushInterval = time.Minute

// MetricsOptions configures SetupMetrics.
type MetricsOptions struct {
	// Mode is the `--otel-exporter` value. It selects the PUSH reader
	// only; see SetupMetrics on why the pull reader ignores it.
	Mode string

	// Registerer is where the Prometheus bridge registers. Required:
	// a MeterProvider with no pull reader would silently drop every
	// instrument declared against it out of /metrics.
	Registerer prometheus.Registerer

	// Cluster is stamped on the resource as k8s.cluster.name. The
	// Prometheus side does not read it — the caller already wraps the
	// Registerer with a cluster label — but OTLP has no such wrapper,
	// so without this a multi-cluster process would push N streams that
	// no consumer could tell apart.
	Cluster string

	// PushInterval overrides DefaultPushInterval. Ignored when no push
	// reader is configured.
	PushInterval time.Duration

	// pushExporter replaces the OTLP exporter the otlp mode would build.
	// A test seam, unexported so only this package can set it: it is
	// what lets the hardening tests drive the REAL reader wiring — the
	// deadline, the guard, the SLIs — instead of a copy of it that can
	// pass while the shipped path regresses.
	pushExporter sdkmetric.Exporter
}

// SetupMetrics builds a MeterProvider that exports the SAME instruments
// two ways (leeway design §8.4: "instrument once, export twice").
//
// The pull reader is unconditional. Instruments declared on the OTel API
// have to reach the /metrics endpoint the sentinel already serves
// whatever the operator set --otel-exporter to, because that endpoint is
// the sentinel's metrics contract; a mode flag about shipping telemetry
// off-box should not be able to empty it. The bridge registers INTO the
// caller's registry rather than standing up a second one, so the OTel
// metrics land beside the hand-rolled prometheus collectors in one
// scrape, with one cluster label, from one port.
//
// The push reader is what the mode selects, and only "otlp" configures
// one. "console" deliberately does not: the pull endpoint is already the
// local-debug path for metrics, and a periodic dump of every series to
// stdout is noise that costs a dependency.
//
// The returned shutdown function flushes and stops both readers; callers
// MUST call it. Unlike Setup, this does NOT install a global provider —
// the provider is per-runner (its resource and its Prometheus label
// differ per cluster), and a global would make "which cluster is this"
// unanswerable in a multi-cluster process.
func SetupMetrics(ctx context.Context, opts MetricsOptions) (metric.MeterProvider, func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	if opts.Registerer == nil {
		return nil, noop, fmt.Errorf("telemetry: metrics: no Registerer (programming error)")
	}

	mode := opts.Mode
	if envMode := strings.TrimSpace(os.Getenv(MetricsExporterEnvVar)); envMode != "" {
		mode = envMode
	}
	switch mode {
	case "", ModeNone, ModeConsole, ModeOTLP:
	default:
		return nil, noop, fmt.Errorf("telemetry: metrics: unknown mode %q (want console/otlp/none)", mode)
	}

	// WithoutTargetInfo and WithoutScopeInfo: both add a synthetic
	// series per scope describing the SDK rather than the cluster, and
	// neither is meaningful next to hand-registered collectors that have
	// no such thing. WithoutCounterSuffixes is NOT set — `_total` is the
	// Prometheus convention and the suffix is what makes these names
	// match their siblings in the same registry.
	promReader, err := otelprom.New(
		otelprom.WithRegisterer(opts.Registerer),
		otelprom.WithoutTargetInfo(),
		otelprom.WithoutScopeInfo(),
	)
	if err != nil {
		return nil, noop, fmt.Errorf("telemetry: metrics: prometheus bridge: %w", err)
	}

	res, err := newMetricsResource(opts.Cluster)
	if err != nil {
		return nil, noop, err
	}

	providerOpts := []sdkmetric.Option{
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(promReader),
	}
	if mode == ModeOTLP {
		interval := resolvePushInterval(opts.PushInterval)
		timeout := resolveExportTimeout(interval)

		exp := opts.pushExporter
		if exp == nil {
			var eerr error
			// The retry budget is bounded by the same deadline rather
			// than left at the SDK's 60s default, which disagrees with
			// every interval shorter than a minute: an attempt cancelled
			// mid-retry is a drop nobody chose, and the last thing the
			// exporter does before the deadline should be giving up, not
			// starting again.
			exp, eerr = otlpmetrichttp.New(ctx, otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{
				Enabled:         true,
				InitialInterval: time.Second,
				MaxInterval:     timeout / 2,
				MaxElapsedTime:  timeout,
			}))
			if eerr != nil {
				return nil, noop, fmt.Errorf("telemetry: metrics: otlp exporter: %w", eerr)
			}
		}

		// §8.4's export SLIs, on the PULL registry. See exportSLIs on
		// why they are not instruments on the provider they describe.
		slis := newExportSLIs()
		if serr := slis.register(opts.Registerer); serr != nil {
			return nil, noop, serr
		}

		providerOpts = append(providerOpts, sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(
				newGuardedExporter(exp, timeout, slis),
				sdkmetric.WithInterval(interval),
				// Strictly below the interval, always: see exportTimeout.
				sdkmetric.WithTimeout(timeout),
			)))
		fmt.Fprintf(os.Stderr, "lookout: telemetry: OTLP HTTP metric exporter → %s (every %s, %s deadline)\n",
			otlpMetricsEndpoint(), interval, timeout)
	}

	mp := sdkmetric.NewMeterProvider(providerOpts...)
	return mp, mp.Shutdown, nil
}

// newMetricsResource is newResource plus the cluster identity. Kept
// separate rather than given a parameter because spans carry the cluster
// on the span itself, and widening the trace resource would change every
// existing span's identity for no gain.
func newMetricsResource(cluster string) (*resource.Resource, error) {
	base, err := newResource()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cluster) == "" {
		return base, nil
	}
	res, err := resource.Merge(base, resource.NewSchemaless(
		attribute.String("k8s.cluster.name", cluster)))
	if err != nil {
		return nil, fmt.Errorf("telemetry: metrics: resource: %w", err)
	}
	return res, nil
}

// otlpMetricsEndpoint renders the OTLP metrics target for the startup
// log, resolved the way the exporter itself resolves it.
func otlpMetricsEndpoint() string {
	if v := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")); v != "" {
		return v
	}
	return "http://localhost:4318 (spec default)"
}
