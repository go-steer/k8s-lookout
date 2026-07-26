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

// Production client behind the metrics backend's two §13 small
// interfaces (metrics.go): aggregated timeSeries.list and the
// metricDescriptors.get existence probe. Same SDK decision as
// quotaclients.go — the google.golang.org/api/monitoring/v3 REST
// discovery client, already a direct dependency, whose JSON wire
// shapes ARE the recorded-fixture format. ADC-authenticated, dialed
// lazily (lazyClient) so provider construction stays cheap and
// offline-safe.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/api/googleapi"
	monitoring "google.golang.org/api/monitoring/v3"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// mtAggregationParams is the aggregation half of one aggregated
// timeSeries.list call, carried as data so tests can assert the
// exact translation per SeriesQuery shape.
type mtAggregationParams struct {
	AlignmentPeriod    time.Duration
	PerSeriesAligner   string
	CrossSeriesReducer string   // "" = no cross-series reduction
	GroupByFields      []string // only with CrossSeriesReducer
}

// mtSeriesAPI is the small client interface over aggregated
// Monitoring timeSeries.list; production is mtClient, tests replay
// recorded ListTimeSeriesResponse fixtures.
type mtSeriesAPI interface {
	ListAggregatedSeries(ctx context.Context, filter string, w cloud.TimeWindow, agg mtAggregationParams) ([]*monitoring.TimeSeries, error)
}

// mtDescriptorAPI is the metric-existence probe over
// metricDescriptors.get; a 404 means the metric type is not in the
// project's workspace (metrics.go turns that into
// cloud.ErrMetricAbsent).
type mtDescriptorAPI interface {
	GetMetricDescriptor(ctx context.Context, metricType string) (*monitoring.MetricDescriptor, error)
}

// mtClient is the production mtSeriesAPI + mtDescriptorAPI.
type mtClient struct {
	project string
	svc     func(ctx context.Context) (*monitoring.Service, error)
}

func newMTClient(project string) *mtClient {
	return &mtClient{
		project: project,
		svc:     lazyClient(func(ctx context.Context) (*monitoring.Service, error) { return monitoring.NewService(ctx) }),
	}
}

func (c *mtClient) ListAggregatedSeries(ctx context.Context, filter string, w cloud.TimeWindow, agg mtAggregationParams) ([]*monitoring.TimeSeries, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	call := svc.Projects.TimeSeries.List("projects/" + c.project).
		Filter(filter).
		IntervalStartTime(w.Start.UTC().Format(time.RFC3339)).
		IntervalEndTime(w.End.UTC().Format(time.RFC3339)).
		AggregationAlignmentPeriod(fmt.Sprintf("%ds", int64(agg.AlignmentPeriod/time.Second))).
		AggregationPerSeriesAligner(agg.PerSeriesAligner)
	if agg.CrossSeriesReducer != "" {
		call = call.AggregationCrossSeriesReducer(agg.CrossSeriesReducer).
			AggregationGroupByFields(agg.GroupByFields...)
	}
	var out []*monitoring.TimeSeries
	err = call.Pages(ctx, func(resp *monitoring.ListTimeSeriesResponse) error {
		out = append(out, resp.TimeSeries...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mtClient) GetMetricDescriptor(ctx context.Context, metricType string) (*monitoring.MetricDescriptor, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	return svc.Projects.MetricDescriptors.
		Get("projects/" + c.project + "/metricDescriptors/" + metricType).
		Context(ctx).Do()
}

// isNotFound reports whether err is an HTTP 404 from the discovery
// client — the descriptor probe's "positively absent" answer.
func isNotFound(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == 404
}
