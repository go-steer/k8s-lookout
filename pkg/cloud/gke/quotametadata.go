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

// The Cloud Quotas metadata half of the quota API (§10.2):
// cloudquotas.googleapis.com QuotaInfo records, used to enrich each
// inventory row with the canonical "<service>/<quotaId>" increase-
// request identifier the §10.3 draft names. READ-ONLY: nothing in
// this repository creates a QuotaPreference — the draft is filed by
// the agent through core-agent's permission gate.

import (
	"context"
	"strings"

	"cloud.google.com/go/cloudquotas/apiv1/cloudquotaspb"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// pqMetadataAPI is the small client interface (§13) over the Cloud
// Quotas metadata read; production is pqMetadataClient
// (quotaclients.go), tests replay recorded QuotaInfo fixtures.
type pqMetadataAPI interface {
	// ComputeQuotaInfos lists the QuotaInfo records for
	// compute.googleapis.com in this project.
	ComputeQuotaInfos(ctx context.Context) ([]*cloudquotaspb.QuotaInfo, error)
}

// enrichIDs stamps each row's canonical Cloud Quotas ID
// ("<service>/<quotaId>", e.g.
// "compute.googleapis.com/CpusPerProjectPerRegion") by joining the
// compute metric name to the QuotaInfo whose Metric matches
// pqQuotaMetric(name). Best-effort by contract (cloud.QuotaUsage.ID
// is optional enrichment): with no metadata backend wired, or on
// metadata error, the rows keep ID == "" and consumers fall back to
// the quota name.
func (a *quotaAPI) enrichIDs(ctx context.Context, rows []cloud.QuotaUsage) {
	if a.metadata == nil {
		return
	}
	byMetric := a.quotaInfoIndex(ctx)
	if byMetric == nil {
		return
	}
	for i := range rows {
		info, ok := byMetric[pqQuotaMetric(rows[i].Name)]
		if !ok {
			continue
		}
		service := info.GetService()
		if service == "" {
			service = "compute.googleapis.com"
		}
		rows[i].ID = service + "/" + info.GetQuotaId()
		// MetricUnit "1" is the dimensionless count marker — noise,
		// not a unit; anything else ("MByte", …) is worth carrying.
		if rows[i].Unit == "" && info.GetMetricUnit() != "" && info.GetMetricUnit() != "1" {
			rows[i].Unit = info.GetMetricUnit()
		}
	}
}

// quotaInfoIndex returns the cached metric→QuotaInfo index, fetching
// it once on demand. nil when metadata is (still) unreachable.
func (a *quotaAPI) quotaInfoIndex(ctx context.Context) map[string]*cloudquotaspb.QuotaInfo {
	a.metaMu.Lock()
	defer a.metaMu.Unlock()
	if a.metaByMetric != nil {
		return a.metaByMetric
	}
	infos, err := a.metadata.ComputeQuotaInfos(ctx)
	if err != nil {
		return nil // retried next call; rows stay un-enriched
	}
	idx := make(map[string]*cloudquotaspb.QuotaInfo, len(infos))
	for _, info := range infos {
		if info.GetMetric() != "" {
			idx[info.GetMetric()] = info
		}
	}
	a.metaByMetric = idx
	return idx
}

// pqQuotaMetric maps a compute quota name to the metric identifier
// the Cloud Quotas and Monitoring surfaces key on: "CPUS" →
// "compute.googleapis.com/cpus". Names that already carry a service
// path pass through lowercased.
func pqQuotaMetric(name string) string {
	if strings.Contains(name, "/") {
		return strings.ToLower(name)
	}
	return "compute.googleapis.com/" + strings.ToLower(name)
}
