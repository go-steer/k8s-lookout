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

// Production clients behind the quota API's metadata and series
// interfaces (quotametadata.go / quotahistory.go; the compute
// inventory client lives in quota.go), tag-guarded with the rest of
// this package — the default lookout build links none of this
// (cmd/lookout's nogcp conformance tests keep that honest). Both
// authenticate via Application Default Credentials (on GKE:
// workload identity) and dial lazily on first call, so provider
// construction stays cheap and offline-safe.
//
// SDK note (decision documented for the M4 quota-source change):
// monitoring uses the google.golang.org/api REST discovery client —
// the module is already a direct dependency and its JSON wire shapes
// ARE the recorded-fixture format (compute.go documents the same
// choice for the rest of the M4 capabilities). The Cloud Quotas
// metadata read uses cloud.google.com/go/cloudquotas/apiv1 because
// the pinned google.golang.org/api release ships no cloudquotas
// discovery client; the GAPIC speaks to the same
// cloudquotas.googleapis.com surface DESIGN.md §10.2 names, and
// cloud.google.com/go clients have precedent here (logadmin.go).
// READ-ONLY by design: only ListQuotaInfos is reachable — no
// QuotaPreference create exists anywhere in this repository (§10.3:
// lookout drafts, core-agent's permission gate files).

import (
	"context"
	"errors"
	"time"

	cloudquotas "cloud.google.com/go/cloudquotas/apiv1"
	"cloud.google.com/go/cloudquotas/apiv1/cloudquotaspb"
	"google.golang.org/api/iterator"
	monitoring "google.golang.org/api/monitoring/v3"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// pqMetadataClient is the production pqMetadataAPI.
type pqMetadataClient struct {
	project string
	client  func(ctx context.Context) (*cloudquotas.Client, error)
}

func newPQMetadataClient(project string) *pqMetadataClient {
	return &pqMetadataClient{
		project: project,
		client:  lazyClient(func(ctx context.Context) (*cloudquotas.Client, error) { return cloudquotas.NewClient(ctx) }),
	}
}

func (c *pqMetadataClient) ComputeQuotaInfos(ctx context.Context) ([]*cloudquotaspb.QuotaInfo, error) {
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}
	it := client.ListQuotaInfos(ctx, &cloudquotaspb.ListQuotaInfosRequest{
		Parent: "projects/" + c.project + "/locations/global/services/compute.googleapis.com",
	})
	var out []*cloudquotaspb.QuotaInfo
	for {
		info, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
}

// pqSeriesClient is the production pqSeriesAPI.
type pqSeriesClient struct {
	project string
	svc     func(ctx context.Context) (*monitoring.Service, error)
}

func newPQSeriesClient(project string) *pqSeriesClient {
	return &pqSeriesClient{
		project: project,
		svc:     lazyClient(func(ctx context.Context) (*monitoring.Service, error) { return monitoring.NewService(ctx) }),
	}
}

func (c *pqSeriesClient) ListSeries(ctx context.Context, filter string, w cloud.TimeWindow) ([]*monitoring.TimeSeries, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	call := svc.Projects.TimeSeries.List("projects/" + c.project).
		Filter(filter).
		IntervalStartTime(w.Start.UTC().Format(time.RFC3339)).
		IntervalEndTime(w.End.UTC().Format(time.RFC3339))
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
