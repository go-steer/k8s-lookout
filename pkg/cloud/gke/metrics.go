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

//go:build gke || allproviders

package gke

// The Cloud-Monitoring-backed cloud.MetricsBackend (M5): the query
// engine behind `perf probe` packs and `triage top --history` (§5).
// Consumers speak the backend-neutral cloud.SeriesQuery shape (§15
// Q4); this file owns the translation to Monitoring — metric-type
// strings, monitored resources, aligners/reducers, label paths — so
// nothing above the §2 provider boundary ever learns a
// Cloud-Monitoring-only construct.
//
// Absence is a first-class answer here: a control-plane metric
// (mtEntry.controlPlane) that returns zero series gets its metric
// descriptor probed, and a 404 becomes a wrapped
// cloud.ErrMetricAbsent naming the metric — GKE control-plane
// metrics are opt-in per cluster, and "not enabled" must surface as
// the explicit pack_unavailable finding, never as a silently empty
// scan (§2, §11).

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	monitoring "google.golang.org/api/monitoring/v3"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// mtEntry translates one backend-neutral metric name to its Cloud
// Monitoring realization. Metric-type strings, resources, kinds, and
// labels are AUTHORED FROM THE GOOGLE CLOUD DOCS (§13 provenance —
// same discipline as the recorded fixtures):
//
//   - GKE control-plane metrics ("Collect and view control plane
//     metrics", cloud.google.com/kubernetes-engine/docs/how-to/
//     control-plane-metrics): every control-plane metric is ingested
//     via Managed Service for Prometheus as
//     prometheus.googleapis.com/<oss name>/<kind suffix> on the
//     prometheus_target resource; labels keep their OSS names as
//     metric labels.
//   - GKE system metrics ("GKE system metrics",
//     cloud.google.com/monitoring/api/metrics_kubernetes):
//     kubernetes.io/… on k8s_pod / k8s_container; the pod/container
//     identity lives in RESOURCE labels (namespace_name, pod_name,
//     container_name).
type mtEntry struct {
	// metricType / resourceType are the Monitoring filter values.
	metricType   string
	resourceType string
	// distribution marks histogram/distribution-valued metrics:
	// percentile reductions align them with ALIGN_DELTA first.
	distribution bool
	// aligner is the default perSeriesAligner when the query asks
	// for neither Rate nor a distribution Percentile.
	aligner string
	// scale multiplies every flattened point value — the
	// neutral-unit conversion (e.g. cores/s → millicores).
	scale float64
	// labels maps neutral label names to Monitoring field paths
	// ("metric.labels.X" / "resource.labels.X"), used both in
	// filters and as aggregation groupByFields.
	labels map[string]string
	// controlPlane marks metrics that exist only when GKE
	// control-plane metrics are enabled on the cluster — the
	// absence-detection (descriptor probe → cloud.ErrMetricAbsent)
	// applies to exactly these.
	controlPlane bool
}

// controlPlaneLabels are the OSS label names the control-plane packs
// group and filter by; on prometheus_target they are metric labels.
func controlPlaneLabels(names ...string) map[string]string {
	m := make(map[string]string, len(names))
	for _, n := range names {
		m[n] = "metric.labels." + n
	}
	return m
}

// k8sContainerLabels is the neutral→resource-label mapping shared by
// the `triage top --history` metrics (k8s_container resource).
var k8sContainerLabels = map[string]string{
	"namespace": "resource.labels.namespace_name",
	"pod":       "resource.labels.pod_name",
	"container": "resource.labels.container_name",
}

// mtTable is the neutral-name translation table. Every neutral name
// a consumer may query MUST appear here — an unknown name is a
// programming/spec error (a pack naming a metric no backend
// translates), reported as such, never as cloud.ErrMetricAbsent.
var mtTable = map[string]mtEntry{
	// GKE control-plane API server metrics (control-plane-metrics
	// doc, "API server metrics" table: apiserver_request_duration_
	// seconds → /histogram, apiserver_request_total → /counter,
	// apiserver_flowcontrol_current_inqueue_requests → /gauge; all
	// on prometheus_target).
	"apiserver_request_duration_seconds": {
		metricType:   "prometheus.googleapis.com/apiserver_request_duration_seconds/histogram",
		resourceType: "prometheus_target",
		distribution: true,
		aligner:      "ALIGN_DELTA",
		scale:        1,
		labels:       controlPlaneLabels("verb", "resource"),
		controlPlane: true,
	},
	"apiserver_flowcontrol_current_inqueue_requests": {
		metricType:   "prometheus.googleapis.com/apiserver_flowcontrol_current_inqueue_requests/gauge",
		resourceType: "prometheus_target",
		aligner:      "ALIGN_MEAN",
		scale:        1,
		labels:       controlPlaneLabels("priority_level"),
		controlPlane: true,
	},
	"apiserver_request_total": {
		metricType:   "prometheus.googleapis.com/apiserver_request_total/counter",
		resourceType: "prometheus_target",
		aligner:      "ALIGN_DELTA",
		scale:        1,
		labels:       controlPlaneLabels("verb", "code"),
		controlPlane: true,
	},
	// etcd WAL fsync latency: the control-plane-metrics doc ships NO
	// etcd fsync metric today (its package covers API server /
	// scheduler / controller manager only), so this entry carries
	// the mechanical Managed-Prometheus name the OSS metric would
	// ingest under. On current GKE the descriptor probe reports it
	// absent and `perf probe` degrades to the explicit
	// pack_unavailable finding — the §2 honest answer, and the entry
	// lights up the day Google ships the metric.
	"etcd_disk_wal_fsync_duration_seconds": {
		metricType:   "prometheus.googleapis.com/etcd_disk_wal_fsync_duration_seconds/histogram",
		resourceType: "prometheus_target",
		distribution: true,
		aligner:      "ALIGN_DELTA",
		scale:        1,
		labels:       controlPlaneLabels(),
		controlPlane: true,
	},
	// etcd database size: the control-plane-metrics doc's
	// realization of the OSS etcd_mvcc_db_total_size_in_bytes is
	// apiserver_storage_size_bytes — "Size of the storage database
	// file physically allocated in bytes" (GA, Gauge, Int64, By) —
	// the number GKE's ~6GiB etcd quota is enforced against.
	"etcd_mvcc_db_total_size_in_bytes": {
		metricType:   "prometheus.googleapis.com/apiserver_storage_size_bytes/gauge",
		resourceType: "prometheus_target",
		aligner:      "ALIGN_MEAN",
		scale:        1,
		labels:       controlPlaneLabels(),
		controlPlane: true,
	},
	// GKE system metric (metrics_kubernetes doc: pod/latencies/
	// pod_first_ready — GA, Gauge, Double, seconds, k8s_pod):
	// end-to-end pod startup latency including image pulls. Always
	// on — no controlPlane gate.
	"pod/latencies/pod_first_ready": {
		metricType:   "kubernetes.io/pod/latencies/pod_first_ready",
		resourceType: "k8s_pod",
		aligner:      "ALIGN_MEAN",
		scale:        1,
		labels: map[string]string{
			"namespace": "resource.labels.namespace_name",
			"pod":       "resource.labels.pod_name",
		},
	},
	// The two `triage top --history` names (pkg/checks/top). Memory
	// is a plain gauge (metrics_kubernetes: container/memory/
	// used_bytes — Gauge, Int64, By).
	"container/memory/used_bytes": {
		metricType:   "kubernetes.io/container/memory/used_bytes",
		resourceType: "k8s_container",
		aligner:      "ALIGN_MEAN",
		scale:        1,
		labels:       k8sContainerLabels,
	},
	// CPU: the neutral name IS a rate ("used_millicores"), so the
	// cumulative core_usage_time counter (metrics_kubernetes:
	// Cumulative, Double, s{CPU}) is ALIGN_RATE'd by default and
	// scaled ×1000 (cores/s → millicores).
	"container/cpu/used_millicores": {
		metricType:   "kubernetes.io/container/cpu/core_usage_time",
		resourceType: "k8s_container",
		aligner:      "ALIGN_RATE",
		scale:        1000,
		labels:       k8sContainerLabels,
	},
}

// mtKnownMetrics returns the sorted neutral names, for the
// unknown-metric error message.
func mtKnownMetrics() []string {
	names := make([]string, 0, len(mtTable))
	for n := range mtTable {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// metricsBackend implements cloud.MetricsBackend over the two §13
// small client interfaces in metricsclient.go.
type metricsBackend struct {
	series      mtSeriesAPI
	descriptors mtDescriptorAPI
}

// newMetricsBackend wires the production Monitoring clients.
func newMetricsBackend(p *Provider) *metricsBackend {
	client := newMTClient(p.project)
	return &metricsBackend{series: client, descriptors: client}
}

// defaultMetricsStep is the alignment period when the query does not
// name one — Monitoring requires an alignment period for every
// aligner, and 60s matches the GKE metric sampling interval.
const defaultMetricsStep = time.Minute

// QuerySeries implements cloud.MetricsBackend: translate the neutral
// query, run the aggregated list, flatten. See the package comment
// for the absence-detection contract.
func (b *metricsBackend) QuerySeries(ctx context.Context, q cloud.SeriesQuery) ([]cloud.Series, error) {
	entry, ok := mtTable[q.Metric]
	if !ok {
		return nil, fmt.Errorf("gke: unknown metric %q (a pack/spec bug, not workspace absence): known metrics are %s",
			q.Metric, strings.Join(mtKnownMetrics(), ", "))
	}
	agg, err := mtAggregation(q, entry)
	if err != nil {
		return nil, err
	}
	filter, err := mtFilter(q, entry)
	if err != nil {
		return nil, err
	}
	series, err := b.series.ListAggregatedSeries(ctx, filter, q.Window, agg)
	if err != nil {
		return nil, fmt.Errorf("gke: listing %s series: %w", q.Metric, err)
	}
	if len(series) == 0 && entry.controlPlane {
		// Zero series from an opt-in metric package is ambiguous:
		// quiet metric, or collection never enabled? The descriptor
		// probe disambiguates — present descriptor + no data is a
		// normal empty result; absent descriptor is the §2 explicit
		// unavailability.
		absent, err := b.metricAbsent(ctx, entry.metricType)
		if err != nil {
			return nil, fmt.Errorf("gke: probing %s descriptor: %w", q.Metric, err)
		}
		if absent {
			return nil, fmt.Errorf("gke: metric %s (%s) is not in this project's workspace — GKE control-plane metrics not enabled: %w",
				q.Metric, entry.metricType, cloud.ErrMetricAbsent)
		}
	}
	return mtFlatten(q.Metric, entry, series), nil
}

// metricAbsent probes the metric descriptor; a 404 is a positive
// "does not exist in this workspace".
func (b *metricsBackend) metricAbsent(ctx context.Context, metricType string) (bool, error) {
	_, err := b.descriptors.GetMetricDescriptor(ctx, metricType)
	if err == nil {
		return false, nil
	}
	if isNotFound(err) {
		return true, nil
	}
	return false, err
}

// mtAggregation builds the Monitoring aggregation for one neutral
// query, mapping each §15 Q4 construct exactly once:
// Rate → ALIGN_RATE; Percentile → REDUCE_PERCENTILE_NN (over
// ALIGN_DELTA for distributions); GroupBy → crossSeriesReducer +
// groupByFields.
func mtAggregation(q cloud.SeriesQuery, entry mtEntry) (mtAggregationParams, error) {
	step := q.Step
	if step <= 0 {
		step = defaultMetricsStep
	}
	agg := mtAggregationParams{AlignmentPeriod: step, PerSeriesAligner: entry.aligner}
	if q.Rate {
		agg.PerSeriesAligner = "ALIGN_RATE"
	}
	switch q.Percentile {
	case 0: // raw values
		if q.GroupBy != nil {
			// Non-percentile aggregation sums — the PromQL
			// sum by(...) shape every current consumer means.
			agg.CrossSeriesReducer = "REDUCE_SUM"
		}
	case 50, 95, 99:
		if q.Rate {
			return mtAggregationParams{}, fmt.Errorf("gke: %s: Rate and Percentile are mutually exclusive in one query", q.Metric)
		}
		agg.CrossSeriesReducer = fmt.Sprintf("REDUCE_PERCENTILE_%d", q.Percentile)
		if entry.distribution {
			agg.PerSeriesAligner = "ALIGN_DELTA"
		}
	default:
		return mtAggregationParams{}, fmt.Errorf("gke: %s: unsupported percentile %d (0, 50, 95, or 99)", q.Metric, q.Percentile)
	}
	if agg.CrossSeriesReducer != "" {
		fields, err := mtLabelPaths(q.GroupBy, entry, q.Metric)
		if err != nil {
			return mtAggregationParams{}, err
		}
		agg.GroupByFields = fields
	}
	return agg, nil
}

// mtFilter builds the Monitoring filter: metric type, resource type,
// then the exact-match matcher clauses in sorted-key order (so the
// string is deterministic and fixture-assertable).
func mtFilter(q cloud.SeriesQuery, entry mtEntry) (string, error) {
	clauses := []string{
		fmt.Sprintf("metric.type=%q", entry.metricType),
		fmt.Sprintf("resource.type=%q", entry.resourceType),
	}
	keys := make([]string, 0, len(q.Matchers))
	for k := range q.Matchers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		path, ok := entry.labels[k]
		if !ok {
			return "", fmt.Errorf("gke: %s has no label %q: known labels are %s", q.Metric, k, mtKnownLabels(entry))
		}
		clauses = append(clauses, fmt.Sprintf("%s=%q", path, q.Matchers[k]))
	}
	return strings.Join(clauses, " AND "), nil
}

// mtLabelPaths maps neutral GroupBy labels through the entry's label
// table.
func mtLabelPaths(groupBy []string, entry mtEntry, metric string) ([]string, error) {
	out := make([]string, 0, len(groupBy))
	for _, g := range groupBy {
		path, ok := entry.labels[g]
		if !ok {
			return nil, fmt.Errorf("gke: %s has no label %q to group by: known labels are %s", metric, g, mtKnownLabels(entry))
		}
		out = append(out, path)
	}
	return out, nil
}

func mtKnownLabels(entry mtEntry) string {
	if len(entry.labels) == 0 {
		return "(none)"
	}
	names := make([]string, 0, len(entry.labels))
	for n := range entry.labels {
		names = append(names, n)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// mtFlatten converts Monitoring TimeSeries to the boundary shape:
// neutral metric name, neutral labels (only names the entry maps —
// consumers stay backend-neutral), ascending-time points scaled to
// the neutral unit. Int64 and double points are handled (percentile
// reductions come back as doubles even over int64 gauges); malformed
// points are skipped, never fatal (mirrors pqFlattenPoints).
func mtFlatten(metric string, entry mtEntry, series []*monitoring.TimeSeries) []cloud.Series {
	var out []cloud.Series
	for _, ts := range series {
		if ts == nil {
			continue
		}
		s := cloud.Series{Metric: metric, Labels: mtNeutralLabels(entry, ts)}
		for _, p := range ts.Points {
			if p == nil || p.Interval == nil || p.Value == nil {
				continue
			}
			at, err := time.Parse(time.RFC3339, p.Interval.EndTime)
			if err != nil {
				continue // one malformed point must not blind the series
			}
			var v float64
			switch {
			case p.Value.Int64Value != nil:
				v = float64(*p.Value.Int64Value)
			case p.Value.DoubleValue != nil:
				v = *p.Value.DoubleValue
			default:
				continue
			}
			s.Points = append(s.Points, cloud.Point{Time: at, Value: v * entry.scale})
		}
		// Monitoring returns points newest-first; the boundary sorts
		// so consumers never re-learn that (same contract as
		// QuotaAPI.History).
		sort.Slice(s.Points, func(i, j int) bool { return s.Points[i].Time.Before(s.Points[j].Time) })
		out = append(out, s)
	}
	return out
}

// mtNeutralLabels reverse-maps the returned series' labels through
// the entry table: only labels the boundary named going in come back
// out, under their neutral names.
func mtNeutralLabels(entry mtEntry, ts *monitoring.TimeSeries) map[string]string {
	labels := map[string]string{}
	lookup := func(path string) (string, bool) {
		switch {
		case strings.HasPrefix(path, "metric.labels.") && ts.Metric != nil:
			v, ok := ts.Metric.Labels[strings.TrimPrefix(path, "metric.labels.")]
			return v, ok
		case strings.HasPrefix(path, "resource.labels.") && ts.Resource != nil:
			v, ok := ts.Resource.Labels[strings.TrimPrefix(path, "resource.labels.")]
			return v, ok
		}
		return "", false
	}
	for neutral, path := range entry.labels {
		if v, ok := lookup(path); ok {
			labels[neutral] = v
		}
	}
	if len(labels) == 0 {
		return nil
	}
	return labels
}
