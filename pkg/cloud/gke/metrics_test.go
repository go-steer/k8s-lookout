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

// Recorded-fixture tests for the M5 metrics backend per DESIGN.md
// §13: the aggregated-list and descriptor-probe clients stay behind
// small interfaces (metricsclient.go) and these tests replay
// testdata/monitoring-perf-*.json fixtures AUTHORED FROM THE
// DOCUMENTED response formats (each fixture's _comment names its doc
// page). What is pinned here is the §15 Q4 translation itself:
// SeriesQuery shape → exact filter/aggregation params, point
// flattening/scaling/ordering, and the absence contract
// (cloud.ErrMetricAbsent on a positively missing descriptor; a
// present descriptor with no data is a normal empty result).

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/api/googleapi"
	monitoring "google.golang.org/api/monitoring/v3"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

var mtTestWindow = cloud.TimeWindow{
	Start: time.Date(2026, 7, 25, 11, 0, 0, 0, time.UTC),
	End:   time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC),
}

// fixtureMTSeries replays canned TimeSeries through mtSeriesAPI,
// recording every call's translation params.
type fixtureMTSeries struct {
	series []*monitoring.TimeSeries
	err    error

	filters []string
	windows []cloud.TimeWindow
	aggs    []mtAggregationParams
}

func (f *fixtureMTSeries) ListAggregatedSeries(_ context.Context, filter string, w cloud.TimeWindow, agg mtAggregationParams) ([]*monitoring.TimeSeries, error) {
	f.filters = append(f.filters, filter)
	f.windows = append(f.windows, w)
	f.aggs = append(f.aggs, agg)
	if f.err != nil {
		return nil, f.err
	}
	return f.series, nil
}

// fixtureMTDescriptors replays the descriptor probe: present, or a
// fixed error — the recorded 404 (the positive-absence answer) or a
// non-404 failure.
type fixtureMTDescriptors struct {
	err   error
	asked []string
}

func (f *fixtureMTDescriptors) GetMetricDescriptor(_ context.Context, metricType string) (*monitoring.MetricDescriptor, error) {
	f.asked = append(f.asked, metricType)
	if f.err != nil {
		return nil, f.err
	}
	return &monitoring.MetricDescriptor{Type: metricType}, nil
}

// descriptor404 rebuilds the googleapi.Error the discovery client
// decodes from the recorded 404 body
// (monitoring-metricdescriptor-404.json).
func descriptor404(t *testing.T) *googleapi.Error {
	t.Helper()
	var envelope struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(readFixture(t, "monitoring-metricdescriptor-404.json"), &envelope); err != nil {
		t.Fatalf("404 fixture: %v", err)
	}
	if envelope.Error.Code != 404 {
		t.Fatalf("404 fixture code = %d (shape contract)", envelope.Error.Code)
	}
	return &googleapi.Error{Code: envelope.Error.Code, Message: envelope.Error.Message}
}

func fixtureBackend(series []*monitoring.TimeSeries) (*metricsBackend, *fixtureMTSeries, *fixtureMTDescriptors) {
	s := &fixtureMTSeries{series: series}
	d := &fixtureMTDescriptors{}
	return &metricsBackend{series: s, descriptors: d}, s, d
}

// TestMetricsQuery_ApiserverP99 pins the flagship translation:
// Percentile 99 over a distribution → ALIGN_DELTA +
// REDUCE_PERCENTILE_99, GroupBy through the metric-label mapping,
// Step as the alignment period, and flattening to ascending double
// points with neutral labels.
func TestMetricsQuery_ApiserverP99(t *testing.T) {
	b, s, _ := fixtureBackend(loadSeriesFixture(t, "monitoring-perf-apiserver-p99.json"))
	got, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{
		Metric:     "apiserver_request_duration_seconds",
		Window:     mtTestWindow,
		Step:       5 * time.Minute,
		GroupBy:    []string{"verb", "resource"},
		Percentile: 99,
	})
	if err != nil {
		t.Fatalf("QuerySeries: %v", err)
	}
	if len(s.filters) != 1 {
		t.Fatalf("ListAggregatedSeries called %d times, want 1", len(s.filters))
	}
	for _, want := range []string{
		`metric.type="prometheus.googleapis.com/apiserver_request_duration_seconds/histogram"`,
		`resource.type="prometheus_target"`,
	} {
		if !strings.Contains(s.filters[0], want) {
			t.Errorf("filter = %s\n  missing %q", s.filters[0], want)
		}
	}
	agg := s.aggs[0]
	if agg.PerSeriesAligner != "ALIGN_DELTA" || agg.CrossSeriesReducer != "REDUCE_PERCENTILE_99" {
		t.Errorf("aggregation = %+v, want ALIGN_DELTA + REDUCE_PERCENTILE_99", agg)
	}
	if agg.AlignmentPeriod != 5*time.Minute {
		t.Errorf("alignment period = %s, want the query's 5m step", agg.AlignmentPeriod)
	}
	if len(agg.GroupByFields) != 2 || agg.GroupByFields[0] != "metric.labels.verb" || agg.GroupByFields[1] != "metric.labels.resource" {
		t.Errorf("groupByFields = %v, want the mapped metric label paths in query order", agg.GroupByFields)
	}
	if !s.windows[0].Start.Equal(mtTestWindow.Start) || !s.windows[0].End.Equal(mtTestWindow.End) {
		t.Errorf("window = %+v, want the caller's %+v", s.windows[0], mtTestWindow)
	}

	if len(got) != 2 {
		t.Fatalf("series = %d, want 2", len(got))
	}
	list := got[0]
	if list.Metric != "apiserver_request_duration_seconds" {
		t.Errorf("series metric = %q, want the NEUTRAL name back", list.Metric)
	}
	if list.Labels["verb"] != "LIST" || list.Labels["resource"] != "pods" {
		t.Errorf("labels = %v, want neutral verb/resource", list.Labels)
	}
	wantVals := []float64{0.75, 2.25, 4.5} // fixture is newest-first; boundary sorts ascending
	if len(list.Points) != len(wantVals) {
		t.Fatalf("points = %d, want %d", len(list.Points), len(wantVals))
	}
	for i, want := range wantVals {
		if list.Points[i].Value != want {
			t.Errorf("point[%d] = %v, want %v", i, list.Points[i].Value, want)
		}
		if i > 0 && !list.Points[i].Time.After(list.Points[i-1].Time) {
			t.Errorf("points not ascending at %d", i)
		}
	}
}

// TestMetricsQuery_RateWithMatchersAndTotalGroupBy pins the APF
// reject-rate shape: Rate → ALIGN_RATE, an exact-match metric-label
// clause for the 429 matcher, and the non-nil-but-empty GroupBy →
// REDUCE_SUM into one series with no groupByFields.
func TestMetricsQuery_RateWithMatchersAndTotalGroupBy(t *testing.T) {
	b, s, _ := fixtureBackend(loadSeriesFixture(t, "monitoring-perf-apf-429rate.json"))
	got, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{
		Metric:   "apiserver_request_total",
		Window:   mtTestWindow,
		Step:     5 * time.Minute,
		Matchers: map[string]string{"code": "429"},
		GroupBy:  []string{},
		Rate:     true,
	})
	if err != nil {
		t.Fatalf("QuerySeries: %v", err)
	}
	if want := `metric.labels.code="429"`; !strings.Contains(s.filters[0], want) {
		t.Errorf("filter = %s\n  missing %q", s.filters[0], want)
	}
	agg := s.aggs[0]
	if agg.PerSeriesAligner != "ALIGN_RATE" || agg.CrossSeriesReducer != "REDUCE_SUM" || len(agg.GroupByFields) != 0 {
		t.Errorf("aggregation = %+v, want ALIGN_RATE + REDUCE_SUM with no group fields", agg)
	}
	if len(got) != 1 || len(got[0].Labels) != 0 {
		t.Fatalf("series = %+v, want one label-free merged series", got)
	}
	if got[0].Points[0].Value != 0.4 || got[0].Points[1].Value != 1.8 {
		t.Errorf("points = %+v, want ascending 0.4, 1.8", got[0].Points)
	}
}

// TestMetricsQuery_APFInqueueGrouping: gauge + GroupBy without a
// percentile sums per retained label (the PromQL sum by(...)).
func TestMetricsQuery_APFInqueueGrouping(t *testing.T) {
	b, s, _ := fixtureBackend(loadSeriesFixture(t, "monitoring-perf-apf-inqueue.json"))
	got, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{
		Metric:  "apiserver_flowcontrol_current_inqueue_requests",
		Window:  mtTestWindow,
		GroupBy: []string{"priority_level"},
	})
	if err != nil {
		t.Fatalf("QuerySeries: %v", err)
	}
	agg := s.aggs[0]
	if agg.PerSeriesAligner != "ALIGN_MEAN" || agg.CrossSeriesReducer != "REDUCE_SUM" {
		t.Errorf("aggregation = %+v, want the gauge default ALIGN_MEAN + REDUCE_SUM", agg)
	}
	if agg.AlignmentPeriod != defaultMetricsStep {
		t.Errorf("alignment period = %s, want the %s default for a stepless query", agg.AlignmentPeriod, defaultMetricsStep)
	}
	if len(agg.GroupByFields) != 1 || agg.GroupByFields[0] != "metric.labels.priority_level" {
		t.Errorf("groupByFields = %v", agg.GroupByFields)
	}
	if len(got) != 2 || got[0].Labels["priority_level"] != "workload-low" {
		t.Errorf("series = %+v, want two priority_level-labeled series", got)
	}
}

// TestMetricsQuery_ContainerMatcherMapping: the `triage top
// --history` shape — k8s_container identity labels map to RESOURCE
// label clauses, raw series (nil GroupBy → no reducer).
func TestMetricsQuery_ContainerMatcherMapping(t *testing.T) {
	b, s, _ := fixtureBackend(nil)
	_, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{
		Metric:   "container/memory/used_bytes",
		Window:   mtTestWindow,
		Step:     time.Minute,
		Matchers: map[string]string{"namespace": "prod", "pod": "api-2", "container": "app"},
	})
	if err != nil {
		t.Fatalf("QuerySeries: %v", err)
	}
	for _, want := range []string{
		`metric.type="kubernetes.io/container/memory/used_bytes"`,
		`resource.type="k8s_container"`,
		`resource.labels.namespace_name="prod"`,
		`resource.labels.pod_name="api-2"`,
		`resource.labels.container_name="app"`,
	} {
		if !strings.Contains(s.filters[0], want) {
			t.Errorf("filter = %s\n  missing %q", s.filters[0], want)
		}
	}
	agg := s.aggs[0]
	if agg.PerSeriesAligner != "ALIGN_MEAN" || agg.CrossSeriesReducer != "" {
		t.Errorf("aggregation = %+v, want plain ALIGN_MEAN with no reducer", agg)
	}
}

// TestMetricsQuery_CPUScale: the used_millicores neutral metric is
// intrinsically a rate — default aligner ALIGN_RATE on
// core_usage_time and every point scaled ×1000 (cores/s →
// millicores).
func TestMetricsQuery_CPUScale(t *testing.T) {
	series := []*monitoring.TimeSeries{{
		Metric:   &monitoring.Metric{Type: "kubernetes.io/container/cpu/core_usage_time"},
		Resource: &monitoring.MonitoredResource{Type: "k8s_container", Labels: map[string]string{"pod_name": "api-2"}},
		Points: []*monitoring.Point{
			{Interval: &monitoring.TimeInterval{EndTime: "2026-07-25T12:00:00Z"}, Value: &monitoring.TypedValue{DoubleValue: googleapi.Float64(0.25)}},
		},
	}}
	b, s, _ := fixtureBackend(series)
	got, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{
		Metric: "container/cpu/used_millicores",
		Window: mtTestWindow,
	})
	if err != nil {
		t.Fatalf("QuerySeries: %v", err)
	}
	if s.aggs[0].PerSeriesAligner != "ALIGN_RATE" {
		t.Errorf("aligner = %q, want the entry's intrinsic ALIGN_RATE", s.aggs[0].PerSeriesAligner)
	}
	if len(got) != 1 || len(got[0].Points) != 1 || got[0].Points[0].Value != 250 {
		t.Errorf("series = %+v, want one point scaled to 250 millicores", got)
	}
	if got[0].Labels["pod"] != "api-2" {
		t.Errorf("labels = %v, want the resource label back under its neutral name", got[0].Labels)
	}
}

// TestMetricsQuery_Int64Flattening: raw gauge int64 points (etcd db
// size realization) flatten with exact values, ascending.
func TestMetricsQuery_Int64Flattening(t *testing.T) {
	b, _, _ := fixtureBackend(loadSeriesFixture(t, "monitoring-perf-etcd-dbsize.json"))
	got, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{
		Metric: "etcd_mvcc_db_total_size_in_bytes",
		Window: mtTestWindow,
	})
	if err != nil {
		t.Fatalf("QuerySeries: %v", err)
	}
	if len(got) != 1 || len(got[0].Points) != 2 {
		t.Fatalf("series = %+v, want one series with two points", got)
	}
	if got[0].Points[0].Value != 4294967296 || got[0].Points[1].Value != 4831838208 {
		t.Errorf("points = %+v, want ascending 4GiB then 4.5GiB", got[0].Points)
	}
}

// TestMetricsQuery_StartupP95: pod_first_ready under
// REDUCE_PERCENTILE_95 — a non-distribution gauge keeps its default
// aligner (no ALIGN_DELTA), one merged double series.
func TestMetricsQuery_StartupP95(t *testing.T) {
	b, s, d := fixtureBackend(loadSeriesFixture(t, "monitoring-perf-startup-p95.json"))
	got, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{
		Metric:     "pod/latencies/pod_first_ready",
		Window:     mtTestWindow,
		Percentile: 95,
		GroupBy:    []string{},
	})
	if err != nil {
		t.Fatalf("QuerySeries: %v", err)
	}
	agg := s.aggs[0]
	if agg.PerSeriesAligner != "ALIGN_MEAN" || agg.CrossSeriesReducer != "REDUCE_PERCENTILE_95" {
		t.Errorf("aggregation = %+v, want ALIGN_MEAN + REDUCE_PERCENTILE_95", agg)
	}
	if len(got) != 1 || len(got[0].Points) != 3 || got[0].Points[0].Value != 48.5 {
		t.Errorf("series = %+v, want ascending p95 seconds starting at 48.5", got)
	}
	if len(d.asked) != 0 {
		t.Errorf("descriptor probed for an always-on system metric with data: %v", d.asked)
	}
}

// TestMetricsQuery_AbsentControlPlaneMetric: zero series + absent
// descriptor → wrapped cloud.ErrMetricAbsent naming the metric (the
// §2 positive answer `perf probe` turns into pack_unavailable).
func TestMetricsQuery_AbsentControlPlaneMetric(t *testing.T) {
	b, _, d := fixtureBackend(nil)
	d.err = descriptor404(t)
	_, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{
		Metric:     "etcd_disk_wal_fsync_duration_seconds",
		Window:     mtTestWindow,
		Percentile: 99,
		GroupBy:    []string{},
	})
	if !errors.Is(err, cloud.ErrMetricAbsent) {
		t.Fatalf("error = %v, want cloud.ErrMetricAbsent", err)
	}
	if !strings.Contains(err.Error(), "etcd_disk_wal_fsync_duration_seconds") {
		t.Errorf("error %q does not name the metric", err)
	}
	if len(d.asked) != 1 || d.asked[0] != "prometheus.googleapis.com/etcd_disk_wal_fsync_duration_seconds/histogram" {
		t.Errorf("descriptor asked = %v, want the full Monitoring type", d.asked)
	}
}

// TestMetricsQuery_EmptyWithDescriptorPresent: a present descriptor
// with no data in the window is a NORMAL empty result — a quiet
// metric, not unavailability.
func TestMetricsQuery_EmptyWithDescriptorPresent(t *testing.T) {
	b, _, d := fixtureBackend(nil)
	got, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{
		Metric:     "apiserver_request_duration_seconds",
		Window:     mtTestWindow,
		Percentile: 99,
		GroupBy:    []string{"verb"},
	})
	if err != nil {
		t.Fatalf("QuerySeries: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("series = %+v, want empty", got)
	}
	if len(d.asked) != 1 {
		t.Errorf("descriptor probes = %v, want exactly one", d.asked)
	}
}

// TestMetricsQuery_UnknownMetricIsSpecError: an untranslated neutral
// name is a programming/spec error listing the known names — NOT
// cloud.ErrMetricAbsent.
func TestMetricsQuery_UnknownMetricIsSpecError(t *testing.T) {
	b, s, _ := fixtureBackend(nil)
	_, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{Metric: "nope", Window: mtTestWindow})
	if err == nil || errors.Is(err, cloud.ErrMetricAbsent) {
		t.Fatalf("error = %v, want a plain spec error", err)
	}
	if !strings.Contains(err.Error(), "apiserver_request_duration_seconds") {
		t.Errorf("error %q should list the known metric names", err)
	}
	if len(s.filters) != 0 {
		t.Errorf("unknown metric still hit the API: %v", s.filters)
	}
}

// TestMetricsQuery_BadShapes: unsupported percentiles and
// rate+percentile combinations fail before any API call.
func TestMetricsQuery_BadShapes(t *testing.T) {
	b, s, _ := fixtureBackend(nil)
	if _, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{
		Metric: "apiserver_request_duration_seconds", Window: mtTestWindow, Percentile: 90,
	}); err == nil || !strings.Contains(err.Error(), "percentile") {
		t.Errorf("percentile 90: error = %v, want the supported-values report", err)
	}
	if _, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{
		Metric: "apiserver_request_total", Window: mtTestWindow, Percentile: 99, Rate: true,
	}); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("rate+percentile: error = %v, want the exclusivity report", err)
	}
	if _, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{
		Metric: "apiserver_request_total", Window: mtTestWindow, Matchers: map[string]string{"nope": "x"},
	}); err == nil || !strings.Contains(err.Error(), "known labels") {
		t.Errorf("unknown matcher: error = %v, want the known-labels report", err)
	}
	if len(s.filters) != 0 {
		t.Errorf("bad shapes still hit the API: %v", s.filters)
	}
}

// TestMetricsQuery_EtcdFsyncFixtureFlattens: the fsync fixture (see
// its provenance caveat) flattens like any distribution percentile —
// the mapping is ready for the day GKE ships the metric.
func TestMetricsQuery_EtcdFsyncFixtureFlattens(t *testing.T) {
	b, _, _ := fixtureBackend(loadSeriesFixture(t, "monitoring-perf-etcd-fsync.json"))
	got, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{
		Metric:     "etcd_disk_wal_fsync_duration_seconds",
		Window:     mtTestWindow,
		Percentile: 99,
		GroupBy:    []string{},
	})
	if err != nil {
		t.Fatalf("QuerySeries: %v", err)
	}
	if len(got) != 1 || len(got[0].Points) != 2 || got[0].Points[1].Value != 0.024 {
		t.Errorf("series = %+v, want the fixture's p99 seconds ascending", got)
	}
}

// TestMetricsQuery_DescriptorProbeErrorPropagates: a non-404 probe
// failure is a runtime error, never mistaken for absence.
func TestMetricsQuery_DescriptorProbeErrorPropagates(t *testing.T) {
	b, _, d := fixtureBackend(nil)
	d.err = errors.New("monitoring 503")
	_, err := b.QuerySeries(context.Background(), cloud.SeriesQuery{
		Metric: "apiserver_request_total", Window: mtTestWindow, Rate: true, GroupBy: []string{},
	})
	if err == nil || errors.Is(err, cloud.ErrMetricAbsent) {
		t.Fatalf("error = %v, want the probe failure, not absence", err)
	}
}
