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

// QuotaAPI implementation (`cloud quota` and the resident quota
// source, DESIGN.md §10.2): the "cheap 80%" — usage/limit pairs from
// compute projects.get (global quotas) and regions.get (regional
// quotas for the provider's region) — plus, since the quota source
// landed, Cloud Quotas metadata (canonical increase-request ids,
// quotametadata.go) and the Monitoring usage-vs-limit series behind
// History (quotahistory.go). Each surface stays behind its own §13
// small client interface (production clients: gceQuotaClient below
// and quotaclients.go), replayable from recorded fixtures.

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/cloudquotas/apiv1/cloudquotaspb"
	compute "google.golang.org/api/compute/v1"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// quotaComputeAPI is the §13 small client interface over the two
// Compute API calls the inventory needs; production is
// gceQuotaClient, tests replay recorded response fixtures.
type quotaComputeAPI interface {
	// GetProject fetches the project record (global quotas).
	GetProject(ctx context.Context) (*compute.Project, error)
	// GetRegion fetches one region record (regional quotas).
	GetRegion(ctx context.Context, region string) (*compute.Region, error)
}

// quotaAPI implements cloud.QuotaAPI.
type quotaAPI struct {
	location string
	gce      quotaComputeAPI
	// metadata/series are the quota source's two further surfaces
	// (§10.2): Cloud Quotas metadata for canonical increase-request
	// ids and Monitoring for the usage-vs-limit history. nil-
	// tolerated (metadata enrichment is best-effort; History without
	// a series backend is a programming error it reports).
	metadata pqMetadataAPI
	series   pqSeriesAPI

	// metaMu/metaByMetric cache the Cloud Quotas metadata after the
	// first successful fetch — quota definitions change on release
	// timescales, not poll timescales. A failed fetch is retried on
	// the next Quotas call (see quotametadata.go).
	metaMu       sync.Mutex
	metaByMetric map[string]*cloudquotaspb.QuotaInfo
}

func newQuotaAPI(p *Provider) *quotaAPI {
	return &quotaAPI{
		location: p.location,
		gce:      newGCEQuotaClient(p.project),
		metadata: newPQMetadataClient(p.project),
		series:   newPQSeriesClient(p.project),
	}
}

// Quotas implements cloud.QuotaAPI: global quotas always; the
// provider region's quotas when a location is known. A provider
// with no location still yields the global inventory — per-project
// deployments (§11) may legitimately not pin one.
func (a *quotaAPI) Quotas(ctx context.Context) ([]cloud.QuotaUsage, error) {
	proj, err := a.gce.GetProject(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading project quotas: %w", err)
	}
	out := appendQuotas(nil, proj.Quotas, "global")

	if region := locationRegion(a.location); region != "" {
		reg, err := a.gce.GetRegion(ctx, region)
		if err != nil {
			return nil, fmt.Errorf("reading region %q quotas: %w", region, err)
		}
		out = appendQuotas(out, reg.Quotas, region)
	}
	a.enrichIDs(ctx, out)
	return out, nil
}

func appendQuotas(dst []cloud.QuotaUsage, quotas []*compute.Quota, scope string) []cloud.QuotaUsage {
	for _, q := range quotas {
		if q == nil {
			continue
		}
		dst = append(dst, cloud.QuotaUsage{
			Name:  q.Metric,
			Scope: scope,
			Usage: q.Usage,
			Limit: q.Limit,
		})
	}
	return dst
}

// gceQuotaClient is the production quotaComputeAPI.
type gceQuotaClient struct {
	project string
	svc     func(ctx context.Context) (*compute.Service, error)
}

func newGCEQuotaClient(project string) *gceQuotaClient {
	return &gceQuotaClient{
		project: project,
		svc:     lazyClient(func(ctx context.Context) (*compute.Service, error) { return compute.NewService(ctx) }),
	}
}

func (c *gceQuotaClient) GetProject(ctx context.Context) (*compute.Project, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	return svc.Projects.Get(c.project).Context(ctx).Do()
}

func (c *gceQuotaClient) GetRegion(ctx context.Context, region string) (*compute.Region, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	return svc.Regions.Get(c.project, region).Context(ctx).Do()
}
