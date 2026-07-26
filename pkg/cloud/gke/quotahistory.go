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

// The Monitoring half of the quota API (§10.2): usage-vs-limit
// series from the serviceruntime quota metrics, which is what turns
// "at 87%" into "exhausted in ~6 days at current slope" — the fit
// itself lives in the quota source (pkg/sources/quota); this file
// only fetches and flattens the series.

import (
	"context"
	"fmt"
	"sort"
	"time"

	monitoring "google.golang.org/api/monitoring/v3"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// The serviceruntime quota metric types (§10.2). Allocation-quota
// usage is written ~daily; the limit series changes only when an
// increase lands — which is exactly the event the §10.3 write path
// exists to cause.
const (
	pqUsageMetric = "serviceruntime.googleapis.com/quota/allocation/usage"
	pqLimitMetric = "serviceruntime.googleapis.com/quota/limit"
)

// pqSeriesAPI is the small client interface over Monitoring
// timeSeries.list; production is pqSeriesClient (quotaclients.go),
// tests replay recorded ListTimeSeriesResponse fixtures.
type pqSeriesAPI interface {
	ListSeries(ctx context.Context, filter string, w cloud.TimeWindow) ([]*monitoring.TimeSeries, error)
}

// History implements cloud.QuotaAPI: the usage and limit series for
// one quota over the window, oldest point first (Monitoring returns
// newest-first; the boundary sorts so consumers never re-learn
// that). A quota with no recorded series yields empty slices, not an
// error — "no history yet" is a normal state for a quiet quota and
// the source degrades to threshold-only judgment.
func (a *quotaAPI) History(ctx context.Context, name, scope string, w cloud.TimeWindow) (cloud.QuotaHistory, error) {
	if a.series == nil {
		return cloud.QuotaHistory{}, fmt.Errorf("gke: quota history for %s/%s: no Monitoring series backend wired (programming error — use newQuotaAPI)", scope, name)
	}
	hist := cloud.QuotaHistory{Name: name, Scope: scope}
	usage, err := a.series.ListSeries(ctx, pqSeriesFilter(pqUsageMetric, name, scope), w)
	if err != nil {
		return cloud.QuotaHistory{}, fmt.Errorf("gke: quota usage series for %s/%s: %w", scope, name, err)
	}
	hist.Usage = pqFlattenPoints(usage)
	limit, err := a.series.ListSeries(ctx, pqSeriesFilter(pqLimitMetric, name, scope), w)
	if err != nil {
		return cloud.QuotaHistory{}, fmt.Errorf("gke: quota limit series for %s/%s: %w", scope, name, err)
	}
	hist.Limit = pqFlattenPoints(limit)
	return hist, nil
}

// pqSeriesFilter builds the Monitoring filter for one quota metric's
// series: the serviceruntime metric type, the consumer_quota
// resource, the quota_metric label (pqQuotaMetric maps "CPUS" →
// "compute.googleapis.com/cpus"), and the location (region, or
// "global").
func pqSeriesFilter(metricType, name, scope string) string {
	return fmt.Sprintf(
		`metric.type=%q AND resource.type="consumer_quota" AND metric.label.quota_metric=%q AND resource.label.location=%q`,
		metricType, pqQuotaMetric(name), scope)
}

// pqFlattenPoints merges the matched series' points into one
// ascending-time slice. The filter pins (metric, quota_metric,
// location), so multiple matches only occur for sub-dimensioned
// limits (per-user etc.); merging keeps every observation and the
// source's regression is robust to it.
func pqFlattenPoints(series []*monitoring.TimeSeries) []cloud.Point {
	var out []cloud.Point
	for _, ts := range series {
		if ts == nil {
			continue
		}
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
			out = append(out, cloud.Point{Time: at, Value: v})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time.Before(out[j].Time) })
	return out
}
