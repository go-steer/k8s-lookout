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
	"strconv"
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
	metricDomainExpected   = "lookout.leeway.domain_expected"
	metricObservedSkew     = "lookout.leeway.observed_skew"
	metricExcessSkew       = "lookout.leeway.excess_skew"
	metricDrift            = "lookout.leeway.drift"
	metricMaxDomainShare   = "lookout.leeway.max_domain_share"
	metricRelocation       = "lookout.leeway.relocation_distance"
	metricLastEvent        = "lookout.leeway.last_event_timestamp"
	metricEvalDuration     = "lookout.leeway.evaluation_duration"
	metricCounterMismatch  = "lookout.leeway.counter_mismatch"
	metricIntentInfo       = "lookout.leeway.intent_info"
	metricAlertState       = "lookout.leeway.alert_state"
	metricTransient        = "lookout.leeway.transient_subjects"
)

// Instrument descriptions. Hoisted to constants because they are the help
// string on BOTH sides of the export — the one the exporter writes into
// /metrics and the one MetricDocs hands the docs generator — and a help
// string that says two different things is worse than one that says nothing.
const (
	descSubjectsTracked  = "Subjects with a tracked distribution, by kind."
	descDomainReadyNodes = "Usable nodes per topology domain."
	descDomainObjects    = "Objects counted per subject, topology domain and scheduling state."
	descDomainExpected   = "Objects §7.2 apportioned to each topology domain, the expectation domain_objects is scored against."
	descObservedSkew     = "S, the difference between the fullest and emptiest eligible domain (leeway §7.3)."
	descExcessSkew       = "E, observed skew beyond what the arithmetic and the declared bound allow (leeway §7.3). " +
		"Zero is the normal reading: a subject that cannot be spread any more evenly than it already is scores zero here " +
		"however lopsided S looks, which is the whole reason drift is not alerted on S."
	descDrift          = "ρ, the fraction of a subject's objects that would have to move to meet its expectation (leeway §7.3)."
	descMaxDomainShare = "The share of a subject's objects held by its fullest domain (leeway §7.3)."
	descRelocation     = "R, the number of objects that would have to move to meet the expectation (leeway §7.3)."
	descLastEvent      = "Unix time of the last informer event leeway processed, per resource."
	descEvalDuration   = "Time spent evaluating one coalesced subject."
	descIntentInfo     = "Placement intent inferred for a subject on one topology axis, as labels on a constant 1 (leeway §5.1). " +
		"Only subjects that expressed an intent are present: a workload with no spread constraint, anti-affinity or affinity " +
		"has no row here, which is what makes the series count a property of the estate's declarations rather than of its size. " +
		"`source` is what the intent was read from and `confidence` how much that source is worth — `assumed` means k8s-lookout " +
		"guessed a cluster default it could not read, and every finding derived from it rests on that guess."
	descAlertState = "Where one subject-axis sits in the §8.2 dwell machine: 1 pending, 2 firing. " +
		"Only subjects with an open episode are present — a subject that is not drifting has no row rather than a zero, " +
		"which keeps the series bounded by how much trouble a cluster is in rather than by how large it is. " +
		"A resolving subject (clear, but inside the resolve dwell) still reads 2, because its finding is still outstanding."
	descTransient = "Subject-axes whose judgement §7.6 suppressed or relaxed, by the transient state responsible. " +
		"This is the series to look at before believing a quiet estate: a fleet-wide `domain-outage` row is leeway declining to page " +
		"four hundred workloads about one dead zone, and a `cluster-warmup` row that never clears is a sentinel that never synced. " +
		"Only the axes under a transient are present, so zero rows is the healthy reading."
	descCounterMismatch = "Subjects whose incremental distribution disagreed with a rebuild from the pod cache and were repaired in place, by kind (leeway §6.5). " +
		"The alert to write: threshold zero. The two numbers are two computations of the same thing, so any non-zero rate is a BUG IN K8S-LOOKOUT " +
		"and not a cluster condition — every finding derived from the drifted counters until it is fixed is wrong in the same direction. " +
		"The repair keeps the next hour's numbers usable; it is not a fix."
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
	attrSource      = attribute.Key("source")
	attrConfidence  = attribute.Key("confidence")
	attrWeighting   = attribute.Key("weighting")
	attrMaxSkew     = attribute.Key("max_skew")
	attrMode        = attribute.Key("mode")
	attrTier        = attribute.Key("tier")
	attrPhase       = attribute.Key("phase")
	attrTransient   = attribute.Key("transient")
)

// MetricDoc documents one series leeway exports, in its PROMETHEUS spelling —
// the name an operator greps for, not the OpenTelemetry name it was declared
// under.
type MetricDoc struct {
	Name   string   // fully-qualified Prometheus metric name
	Type   string   // gauge | counter | histogram
	Labels []string // variable label names, nil for unlabeled
	Help   string   // the exported help string, verbatim
	// Optional is true for a series §8.4's per-domain gate can withhold: it is
	// present for the subjects the gate admits and absent for the rest, so a
	// scrape of a healthy estate may not carry it at all. The docs generator
	// renders that rather than letting a reader conclude the metric is broken.
	Optional bool
}

// MetricDocs returns every series leeway exports, for the generated metrics
// reference.
//
// This list exists because the sentinel's docs generator derives names and
// help from live prometheus.Collector.Describe, and these metrics have no
// Collector: they are declared on the OpenTelemetry API and reach the registry
// through a bridge (§8.4). Writing them out is the price of that, and the risk
// is the obvious one — a hand-kept list drifting from what the exporter
// actually emits. Two things hold it: the help strings are the same constants
// the instruments are declared with, and TestMetricDocs_MatchTheExporter
// gathers a real bridged registry and compares names, types and labels
// against these rows.
func MetricDocs() []MetricDoc {
	return []MetricDoc{
		{
			Name:   "lookout_leeway_subjects_tracked",
			Type:   "gauge",
			Labels: []string{"subject_kind"},
			Help:   descSubjectsTracked,
		},
		{
			Name:   "lookout_leeway_domain_ready_nodes",
			Type:   "gauge",
			Labels: []string{"topology_key", "domain"},
			Help:   descDomainReadyNodes,
		},
		{
			Name:     "lookout_leeway_domain_objects",
			Type:     "gauge",
			Labels:   []string{"namespace", "subject", "subject_kind", "topology_key", "domain", "state"},
			Help:     descDomainObjects,
			Optional: true,
		},
		{
			Name:     "lookout_leeway_domain_expected",
			Type:     "gauge",
			Labels:   []string{"namespace", "subject", "subject_kind", "topology_key", "domain"},
			Help:     descDomainExpected,
			Optional: true,
		},
		{
			Name:   "lookout_leeway_observed_skew",
			Type:   "gauge",
			Labels: []string{"namespace", "subject", "subject_kind", "topology_key"},
			Help:   descObservedSkew,
		},
		{
			Name:   "lookout_leeway_excess_skew",
			Type:   "gauge",
			Labels: []string{"namespace", "subject", "subject_kind", "topology_key"},
			Help:   descExcessSkew,
		},
		{
			Name:   "lookout_leeway_drift",
			Type:   "gauge",
			Labels: []string{"namespace", "subject", "subject_kind", "topology_key"},
			Help:   descDrift,
		},
		{
			Name:   "lookout_leeway_max_domain_share",
			Type:   "gauge",
			Labels: []string{"namespace", "subject", "subject_kind", "topology_key"},
			Help:   descMaxDomainShare,
		},
		{
			Name:   "lookout_leeway_relocation_distance",
			Type:   "gauge",
			Labels: []string{"namespace", "subject", "subject_kind", "topology_key"},
			Help:   descRelocation,
		},
		{
			Name:   "lookout_leeway_intent_info",
			Type:   "gauge",
			Labels: []string{"namespace", "subject", "subject_kind", "topology_key", "mode", "source", "confidence", "weighting", "max_skew"},
			Help:   descIntentInfo,
		},
		{
			Name:   "lookout_leeway_alert_state",
			Type:   "gauge",
			Labels: []string{"namespace", "subject", "subject_kind", "topology_key", "tier", "phase"},
			Help:   descAlertState,
		},
		{
			Name:   "lookout_leeway_transient_subjects",
			Type:   "gauge",
			Labels: []string{"topology_key", "transient"},
			Help:   descTransient,
		},
		{
			Name:   "lookout_leeway_last_event_timestamp_seconds",
			Type:   "gauge",
			Labels: []string{"resource"},
			Help:   descLastEvent,
		},
		{
			Name:   "lookout_leeway_evaluation_duration_seconds",
			Type:   "histogram",
			Labels: []string{"subject_kind"},
			Help:   descEvalDuration,
		},
		{
			Name:   "lookout_leeway_counter_mismatch_total",
			Type:   "counter",
			Labels: []string{"subject_kind"},
			Help:   descCounterMismatch,
		},
	}
}

// Event resources for metricLastEvent.
const (
	resourcePod  = "pod"
	resourceNode = "node"
)

// countObserver is handed each per-domain count during a scrape.
type countObserver func(sub leeway.SubjectRef, key leeway.TopologyKey, domain leeway.Domain, state leeway.CountState, n int64)

// intentObserver is handed each resolved intent during a scrape.
type intentObserver func(sub leeway.SubjectRef, key leeway.TopologyKey, in *leeway.Intent)

// alertObserver is handed each open episode during a scrape.
type alertObserver func(sub leeway.SubjectRef, key leeway.TopologyKey, st leeway.AlertState, tier leeway.Tier)

// evalObserver is handed each scored axis during a scrape. The Evaluation is
// borrowed for the duration of the call and must not be retained.
type evalObserver func(sub leeway.SubjectRef, ev *Evaluation)

// DefaultPerDomainSeriesMinDrift is §8.4's per-domain cardinality floor: a
// subject drifting by at least 5% gets its per-domain breakdown exported, and
// one that is not does not.
const DefaultPerDomainSeriesMinDrift = 0.05

// PerDomainGate is §8.4's cardinality gate on the per-domain series.
//
// The series count of a per-domain breakdown is subjects × keys × domains
// × states, which at §6.6's 20k-subject baseline is ~480k series against ~3.5k
// for the aggregates — and under Google Managed Prometheus that is not a scrape
// that fails but a bill that grows (§13 S5). So the breakdown is exported for
// the subjects somebody is actually going to look at, and the gate is what
// decides which those are.
//
// Phase 2 shipped this as a plain boolean because the floor needs a drift
// figure and there was none until intent inference landed. The boolean survives
// as All, for a small estate that would rather have everything.
type PerDomainGate struct {
	// All exports the breakdown for every counted subject, scored or not.
	All bool

	// MinDrift is the floor a scored subject must reach. Zero admits every
	// scored subject; a subject that was never scored is never admitted by the
	// floor, because a subject with no drift figure is not one the floor can
	// say anything about.
	MinDrift float64
}

// Admits reports whether a subject's per-domain series should be exported,
// given its highest drift across axes and whether it was scored at all.
func (g PerDomainGate) Admits(drift float64, scored bool) bool {
	return g.All || (scored && drift >= g.MinDrift)
}

// metricsOptions wires the observable instruments to the state they read.
//
// The callbacks are pulled at scrape time rather than pushed on every event:
// a counter that moves 3,000 times a second between two scrapes should cost
// two reads, not 6,000 writes.
type metricsOptions struct {
	// Meter is where instruments are declared. A nil meter takes the no-op
	// provider, so a source constructed without telemetry still runs.
	Meter metric.Meter

	// PerDomain gates metricDomainObjects and metricDomainExpected.
	PerDomain PerDomainGate

	// SubjectCounts returns the tracked subject count per kind.
	SubjectCounts func() map[leeway.SubjectKind]int64

	// DomainNodes returns the usable node count per domain, per axis.
	DomainNodes func() map[leeway.TopologyKey]map[leeway.Domain]int64

	// DomainObjects walks every non-zero per-domain count of every subject the
	// gate admits.
	DomainObjects func(PerDomainGate, countObserver)

	// Evaluations walks each scored subject's §7.3 result, one call per axis.
	Evaluations func(evalObserver)

	// Intents walks each tracked subject's resolved intent, one call per axis.
	//
	// Ungated, unlike DomainObjects, because the cardinality argument that
	// gates that one does not apply: this is subjects × *axes they declared an
	// intent on*, not subjects × keys × domains × states, and a subject that
	// declared nothing contributes nothing. On an estate where placement intent
	// is the exception the series count is a small fraction of the subject
	// count; on one where every workload declares a zone spread it is at most
	// the subject count, which is the same order as subjects_tracked.
	Intents func(intentObserver)

	// Alerts walks every open §8.2 episode.
	//
	// Ungated for the same reason as Intents and a stronger one: the series is
	// bounded by how many subject-axes are in an episode right now, which on a
	// healthy estate is zero. A gate on a metric that only exists when
	// something is wrong would withhold precisely the rows somebody is looking
	// for.
	Alerts func(alertObserver)
}

// instruments holds leeway's OTEL-native metric instruments (§8.4).
//
// Everything is declared against the OpenTelemetry metric API, never against
// a Prometheus registry directly. That is what lets one declaration reach both
// the pull reader bridged into the sentinel's existing registry and the OTLP
// push reader, with no second bookkeeping path to keep in sync.
type instruments struct {
	lastEvent       metric.Int64Gauge
	evalDuration    metric.Float64Histogram
	counterMismatch metric.Int64Counter

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
		metric.WithDescription(descLastEvent),
	); err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricLastEvent, err)
	}
	if in.evalDuration, err = meter.Float64Histogram(metricEvalDuration,
		metric.WithUnit("s"),
		metric.WithDescription(descEvalDuration),
	); err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricEvalDuration, err)
	}

	// The SLI of §6.5. It has no sensible threshold: any non-zero rate is a bug
	// in the delta rules, because the incremental counters and a rebuild from
	// the pod cache are two computations of the same number. Declared here and
	// not in the PR that built the counters, so that no window existed in which
	// an instrument shipped that nothing could ever increment.
	if in.counterMismatch, err = meter.Int64Counter(metricCounterMismatch,
		metric.WithDescription(descCounterMismatch),
	); err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricCounterMismatch, err)
	}

	subjects, err := meter.Int64ObservableGauge(metricSubjectsTracked,
		metric.WithDescription(descSubjectsTracked))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricSubjectsTracked, err)
	}
	readyNodes, err := meter.Int64ObservableGauge(metricDomainReadyNodes,
		metric.WithDescription(descDomainReadyNodes))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricDomainReadyNodes, err)
	}
	objects, err := meter.Int64ObservableGauge(metricDomainObjects,
		metric.WithDescription(descDomainObjects))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricDomainObjects, err)
	}
	intents, err := meter.Int64ObservableGauge(metricIntentInfo,
		metric.WithDescription(descIntentInfo))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricIntentInfo, err)
	}
	expected, err := meter.Int64ObservableGauge(metricDomainExpected,
		metric.WithDescription(descDomainExpected))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricDomainExpected, err)
	}

	observedSkew, err := meter.Int64ObservableGauge(metricObservedSkew,
		metric.WithDescription(descObservedSkew))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricObservedSkew, err)
	}
	excessSkew, err := meter.Int64ObservableGauge(metricExcessSkew,
		metric.WithDescription(descExcessSkew))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricExcessSkew, err)
	}
	relocation, err := meter.Int64ObservableGauge(metricRelocation,
		metric.WithDescription(descRelocation))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricRelocation, err)
	}
	drift, err := meter.Float64ObservableGauge(metricDrift,
		metric.WithDescription(descDrift))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricDrift, err)
	}
	maxDomainShare, err := meter.Float64ObservableGauge(metricMaxDomainShare,
		metric.WithDescription(descMaxDomainShare))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricMaxDomainShare, err)
	}

	alertState, err := meter.Int64ObservableGauge(metricAlertState,
		metric.WithDescription(descAlertState))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricAlertState, err)
	}

	transient, err := meter.Int64ObservableGauge(metricTransient,
		metric.WithDescription(descTransient))
	if err != nil {
		return nil, fmt.Errorf("topologydrift: declare %s: %w", metricTransient, err)
	}

	gauges := observables{
		alertState:     alertState,
		transient:      transient,
		subjects:       subjects,
		readyNodes:     readyNodes,
		objects:        objects,
		intents:        intents,
		expected:       expected,
		observedSkew:   observedSkew,
		excessSkew:     excessSkew,
		relocation:     relocation,
		drift:          drift,
		maxDomainShare: maxDomainShare,
	}

	// One callback for every observable: the SDK invokes it once per
	// collection, so the reads see the same moment rather than a series of
	// moments a scrape apart.
	in.reg, err = meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			opts.observe(o, gauges)
			return nil
		},
		gauges.all()...,
	)
	if err != nil {
		return nil, fmt.Errorf("topologydrift: register metric callback: %w", err)
	}
	return in, nil
}

// observables is the set of observable gauges one scrape fills, carried as a
// struct so that adding one does not lengthen a positional argument list that
// is already four instruments of the same type — the shape where a
// transposition compiles and silently swaps two series.
type observables struct {
	subjects   metric.Int64ObservableGauge
	readyNodes metric.Int64ObservableGauge
	objects    metric.Int64ObservableGauge
	intents    metric.Int64ObservableGauge
	expected   metric.Int64ObservableGauge

	observedSkew   metric.Int64ObservableGauge
	excessSkew     metric.Int64ObservableGauge
	relocation     metric.Int64ObservableGauge
	drift          metric.Float64ObservableGauge
	maxDomainShare metric.Float64ObservableGauge

	alertState metric.Int64ObservableGauge
	transient  metric.Int64ObservableGauge
}

// all is every instrument the callback fills, for RegisterCallback. Kept
// beside the struct so that adding a field and forgetting to register it is
// one edit away from being noticed rather than a series that is silently never
// collected.
func (g observables) all() []metric.Observable {
	return []metric.Observable{
		g.subjects, g.readyNodes, g.objects, g.intents, g.expected,
		g.observedSkew, g.excessSkew, g.relocation, g.drift, g.maxDomainShare,
		g.alertState, g.transient,
	}
}

// observe fills the observable gauges from the wired callbacks.
func (opts metricsOptions) observe(o metric.Observer, g observables) {
	if opts.SubjectCounts != nil {
		for kind, n := range opts.SubjectCounts() {
			o.ObserveInt64(g.subjects, n, metric.WithAttributes(attrSubjectKind.String(string(kind))))
		}
	}
	if opts.DomainNodes != nil {
		for key, byDomain := range opts.DomainNodes() {
			for domain, n := range byDomain {
				o.ObserveInt64(g.readyNodes, n, metric.WithAttributes(
					attrTopologyKey.String(string(key)),
					attrDomain.String(string(domain)),
				))
			}
		}
	}
	if opts.DomainObjects != nil {
		opts.DomainObjects(opts.PerDomain, func(sub leeway.SubjectRef, key leeway.TopologyKey, domain leeway.Domain, state leeway.CountState, n int64) {
			o.ObserveInt64(g.objects, n, metric.WithAttributes(
				attrSubjectKind.String(string(sub.Kind)),
				attrNamespace.String(sub.Namespace),
				attrSubject.String(sub.Name),
				attrTopologyKey.String(string(key)),
				attrDomain.String(string(domain)),
				attrState.String(state.String()),
			))
		})
	}
	if opts.Evaluations != nil {
		// §7.6 is counted here rather than through an observer of its own
		// because it is an aggregate of the same walk: a per-subject
		// suppression series would be subjects × axes rows saying the same
		// thing about the cluster, and the question the row answers — "is
		// leeway holding its tongue, and about what" — is a fleet question.
		byTransient := map[transientBucket]int64{}
		opts.Evaluations(func(sub leeway.SubjectRef, ev *Evaluation) {
			opts.observeScores(o, g, sub, ev)
			if ev != nil && ev.Suppression.State != leeway.TransientNone {
				byTransient[transientBucket{key: ev.Key, state: ev.Suppression.State}]++
			}
		})
		for b, n := range byTransient {
			o.ObserveInt64(g.transient, n, metric.WithAttributes(
				attrTopologyKey.String(string(b.key)),
				attrTransient.String(b.state.String()),
			))
		}
	}
	if opts.Intents != nil {
		opts.Intents(func(sub leeway.SubjectRef, key leeway.TopologyKey, in *leeway.Intent) {
			if in == nil {
				return
			}
			o.ObserveInt64(g.intents, 1, metric.WithAttributes(
				attrSubjectKind.String(string(sub.Kind)),
				attrNamespace.String(sub.Namespace),
				attrSubject.String(sub.Name),
				attrTopologyKey.String(string(key)),
				attrMode.String(in.Mode.String()),
				attrSource.String(in.Source.String()),
				attrConfidence.String(in.Confidence.String()),
				attrWeighting.String(in.Weighting.String()),
				attrMaxSkew.String(skewLabel(in)),
			))
		})
	}
	if opts.Alerts != nil {
		opts.Alerts(func(sub leeway.SubjectRef, key leeway.TopologyKey, st leeway.AlertState, tier leeway.Tier) {
			o.ObserveInt64(g.alertState, alertLevel(st.Phase), metric.WithAttributes(
				attrSubjectKind.String(string(sub.Kind)),
				attrNamespace.String(sub.Namespace),
				attrSubject.String(sub.Name),
				attrTopologyKey.String(string(key)),
				attrTier.String(tier.String()),
				attrPhase.String(st.Phase.String()),
			))
		})
	}
}

// transientBucket is one row of the §7.6 gauge: an axis and the state
// suppressing or relaxing it.
type transientBucket struct {
	key   leeway.TopologyKey
	state leeway.TransientState
}

// alertLevel is §8.4's 0/1/2 encoding of a phase.
//
// Resolving reads 2 rather than a fourth value: the machine is in its resolve
// dwell, which means the finding is still outstanding and a dashboard counting
// `alert_state == 2` is counting exactly the episodes somebody has been told
// about and not yet told are over. The distinction is not lost — `phase` is a
// label, so `alert_state{phase="resolving"}` picks them out — but the *value*
// answers the question the value is asked, which is "how many findings are
// live".
func alertLevel(p leeway.AlertPhase) int64 {
	if p.Firing() {
		return 2
	}
	if p == leeway.PhasePending {
		return 1
	}
	return 0
}

// observeScores fills the §7.3 series for one scored axis.
//
// **Only an evaluable axis is exported.** A subject below
// minReplicasForScoring, or with no eligible domains, or one whose intent says
// Ignore has no drift figure — Score gates it and leaves the fields at zero —
// and publishing that zero would be five series per axis asserting a perfectly
// balanced workload for every single-replica Deployment in the estate. It is
// both the wrong statement and, at §6.6's baseline, the majority of the
// cardinality. The gate itself is not lost: a reader asking why a subject has
// no drift series finds it tracked in subjects_tracked and absent here, which
// is the same answer.
//
// The per-domain expectation rides the same gate as domain_objects, because the
// two are a numerator and a denominator: exporting one without the other gives
// a dashboard a number it cannot draw a comparison from.
func (opts metricsOptions) observeScores(o metric.Observer, g observables, sub leeway.SubjectRef, ev *Evaluation) {
	if ev == nil || !ev.Scores.Evaluable {
		return
	}
	attrs := metric.WithAttributes(
		attrSubjectKind.String(string(sub.Kind)),
		attrNamespace.String(sub.Namespace),
		attrSubject.String(sub.Name),
		attrTopologyKey.String(string(ev.Key)),
	)
	o.ObserveInt64(g.observedSkew, ev.Scores.ObservedSkew, attrs)
	o.ObserveInt64(g.excessSkew, ev.Scores.ExcessSkew, attrs)
	o.ObserveInt64(g.relocation, ev.Scores.Relocation, attrs)
	o.ObserveFloat64(g.drift, ev.Scores.Drift, attrs)
	o.ObserveFloat64(g.maxDomainShare, ev.Scores.MaxDomainShare, attrs)

	if !opts.PerDomain.Admits(ev.Scores.Drift, true) {
		return
	}
	for i, domain := range ev.Scores.Domains {
		if i >= len(ev.Scores.Expected) {
			break
		}
		o.ObserveInt64(g.expected, ev.Scores.Expected[i], metric.WithAttributes(
			attrSubjectKind.String(string(sub.Kind)),
			attrNamespace.String(sub.Namespace),
			attrSubject.String(sub.Name),
			attrTopologyKey.String(string(ev.Key)),
			attrDomain.String(string(domain)),
		))
	}
}

// skewLabel renders an intent's bound for the max_skew label.
//
// Three cases, and they must stay distinguishable: a skew bound, a per-domain
// ceiling from a required podAntiAffinity — which is not a skew of one and must
// not be reported as one (§5.1) — and no bound at all. "none" rather than an
// empty string, because an empty label value is indistinguishable from an
// absent label in a PromQL matcher.
func skewLabel(in *leeway.Intent) string {
	if in.MaxSkew != nil {
		return strconv.FormatInt(int64(*in.MaxSkew), 10)
	}
	if in.MaxPerDomain != nil {
		return "max-per-domain=" + strconv.FormatInt(*in.MaxPerDomain, 10)
	}
	return "none"
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

// recordMismatch counts one subject whose counters had drifted.
func (in *instruments) recordMismatch(ctx context.Context, kind leeway.SubjectKind) {
	if in == nil {
		return
	}
	in.counterMismatch.Add(ctx, 1, metric.WithAttributes(attrSubjectKind.String(string(kind))))
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
