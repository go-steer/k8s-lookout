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
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// MeterName is the instrumentation scope leeway's metrics are declared under.
const MeterName = "github.com/go-steer/k8s-lookout/pkg/sources/topologydrift"

// Instrument names, in OpenTelemetry form (§8.4).
//
// The Prometheus spelling is *derived*, never written down twice: the exporter
// turns `.` into `_`, appends `_total` to monotonic counters, and — the part
// that bites — injects the unit into the middle of the name, ahead of any
// `_total`. So `lookout.leeway.evaluation_duration` with unit `s` reaches
// /metrics as `lookout_leeway_evaluation_duration_seconds`, which is the name
// we want and the reason the instrument is named `_duration` rather than
// `_duration_seconds`. TestInstrumentNames_PrometheusSpelling pins every one of
// these against the real exporter so the derivation cannot drift under a
// dependency bump.
const (
	metricSubjectsTracked  = "lookout.leeway.subjects_tracked"
	metricDomainReadyNodes = "lookout.leeway.domain_ready_nodes"
	metricDomainObjects    = "lookout.leeway.domain_objects"
	metricLastEvent        = "lookout.leeway.last_event_timestamp"
	metricEvalDuration     = "lookout.leeway.evaluation_duration"
)

// Attribute keys. Kept as typed keys rather than literals so a typo is a
// compile error in one place instead of a split series in production.
var (
	attrSubjectKind = attribute.Key("subject_kind")
	attrTopologyKey = attribute.Key("topology_key")
	attrDomain      = attribute.Key("domain")
	attrState       = attribute.Key("state")
	attrNamespace   = attribute.Key("namespace")
	attrSubject     = attribute.Key("subject")
	attrResource    = attribute.Key("resource")
)

// Event resources for metricLastEvent.
const (
	resourcePod  = "pod"
	resourceNode = "node"
)

// countObserver is handed each per-domain count during a scrape.
type countObserver func(sub leeway.SubjectRef, key leeway.TopologyKey, domain leeway.Domain, state leeway.CountState, n int64)

// metricsOptions wires the observable instruments to the state they read.
//
// The callbacks are pulled at scrape time rather than pushed on every event:
// a counter that moves 3,000 times a second between two scrapes should cost
// two reads, not 6,000 writes.
type metricsOptions struct {
	// Meter is where instruments are declared. A nil meter takes the no-op
	// provider, so a source constructed without telemetry still runs.
	Meter metric.Meter

	// PerDomainSeries turns on metricDomainObjects.
	//
	// Off by default, and that default is the whole point (§8.4): the series
	// count is subjects × keys × domains × states, which at the 20k subjects
	// of §6.6's baseline row is ~480k series against ~3.5k for everything
	// else here. Under Google Managed Prometheus that is not a scrape that
	// fails, it is a bill that grows, so the cheap posture has to be what a
	// deployment gets for free. Phase 3 replaces this boolean with §8.4's
	// `perDomainSeriesMinDrift` floor, which needs a drift figure that does
	// not exist until intent inference lands.
	PerDomainSeries bool

	// SubjectCounts returns the tracked subject count per kind.
	SubjectCounts func() map[leeway.SubjectKind]int64

	// DomainNodes returns the usable node count per domain, per axis.
	DomainNodes func() map[leeway.TopologyKey]map[leeway.Domain]int64

	// DomainObjects walks every non-zero per-domain count. Only called when
	// PerDomainSeries is set.
	DomainObjects func(countObserver)
}

// instruments holds leeway's OTEL-native metric instruments (§8.4).
//
// Everything is declared against the OpenTelemetry metric API, never against
// a Prometheus registry directly. That is what lets one declaration reach both
// the pull reader bridged into the sentinel's existing registry and the OTLP
// push reader, with no second bookkeeping path to keep in sync.
type instruments struct {
	lastEvent    metric.Int64Gauge
	evalDuration metric.Float64Histogram

	// reg holds the observable-instrument callback so Close can unregister it.
	reg metric.Registration
}

// newInstruments declares the instruments and registers the scrape-time
// callbacks. The caller must Close the result to unregister them.
func newInstruments(opts metricsOptions) (*instruments, error) {
	meter := opts.Meter
	if meter == nil {
		meter = noop.NewMeterProvider().Meter(MeterName)
	}

	in := &instruments{}
	var err error

	// Unit "s" on a gauge of a wall-clock instant: the exported name becomes
	// ..._last_event_timestamp_seconds, which is the Prometheus convention for
	// a unix timestamp.
	if in.lastEvent, err = meter.Int64Gauge(metricLastEvent,
		metric.WithUnit("s"),
		metric.WithDescription("Unix time of the last informer event leeway processed, per resource."),
	); err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricLastEvent, err)
	}
	if in.evalDuration, err = meter.Float64Histogram(metricEvalDuration,
		metric.WithUnit("s"),
		metric.WithDescription("Time spent evaluating one coalesced subject."),
	); err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricEvalDuration, err)
	}

	subjects, err := meter.Int64ObservableGauge(metricSubjectsTracked,
		metric.WithDescription("Subjects with a tracked distribution, by kind."))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricSubjectsTracked, err)
	}
	readyNodes, err := meter.Int64ObservableGauge(metricDomainReadyNodes,
		metric.WithDescription("Usable nodes per topology domain."))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricDomainReadyNodes, err)
	}
	objects, err := meter.Int64ObservableGauge(metricDomainObjects,
		metric.WithDescription("Objects counted per subject, topology domain and scheduling state."))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricDomainObjects, err)
	}

	// One callback for all three observables: the SDK invokes it once per
	// collection, so the three reads see the same moment rather than three
	// moments a scrape apart.
	in.reg, err = meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			opts.observe(o, subjects, readyNodes, objects)
			return nil
		},
		subjects, readyNodes, objects,
	)
	if err != nil {
		return nil, fmt.Errorf("topologydrift: register metric callback: %w", err)
	}
	return in, nil
}

// observe fills the three observable gauges from the wired callbacks.
func (opts metricsOptions) observe(o metric.Observer, subjects, readyNodes, objects metric.Int64ObservableGauge) {
	if opts.SubjectCounts != nil {
		for kind, n := range opts.SubjectCounts() {
			o.ObserveInt64(subjects, n, metric.WithAttributes(attrSubjectKind.String(string(kind))))
		}
	}
	if opts.DomainNodes != nil {
		for key, byDomain := range opts.DomainNodes() {
			for domain, n := range byDomain {
				o.ObserveInt64(readyNodes, n, metric.WithAttributes(
					attrTopologyKey.String(string(key)),
					attrDomain.String(string(domain)),
				))
			}
		}
	}
	if opts.PerDomainSeries && opts.DomainObjects != nil {
		opts.DomainObjects(func(sub leeway.SubjectRef, key leeway.TopologyKey, domain leeway.Domain, state leeway.CountState, n int64) {
			o.ObserveInt64(objects, n, metric.WithAttributes(
				attrSubjectKind.String(string(sub.Kind)),
				attrNamespace.String(sub.Namespace),
				attrSubject.String(sub.Name),
				attrTopologyKey.String(string(key)),
				attrDomain.String(string(domain)),
				attrState.String(state.String()),
			))
		})
	}
}

// recordEvent stamps the arrival of an informer event for resource.
//
// Nil-safe, like recordEvaluation: the informer handlers are registered before
// the instruments are declared, and a source driven directly by a unit test has
// no instruments at all. Dropping a sample is the right failure here — an
// unmeasured event is better than a source that panics on one.
func (in *instruments) recordEvent(ctx context.Context, resource string, at time.Time) {
	if in == nil {
		return
	}
	in.lastEvent.Record(ctx, at.Unix(), metric.WithAttributes(attrResource.String(resource)))
}

// recordEvaluation records how long one coalesced evaluation took.
func (in *instruments) recordEvaluation(ctx context.Context, kind leeway.SubjectKind, d time.Duration) {
	if in == nil {
		return
	}
	in.evalDuration.Record(ctx, d.Seconds(), metric.WithAttributes(attrSubjectKind.String(string(kind))))
}

// Close unregisters the observable callback. Safe on a nil receiver and safe
// to call more than once.
func (in *instruments) Close() error {
	if in == nil || in.reg == nil {
		return nil
	}
	reg := in.reg
	in.reg = nil
	return reg.Unregister()
}
