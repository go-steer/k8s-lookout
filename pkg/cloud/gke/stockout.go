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

// StockoutAPI implementation (`cloud stockout`, DESIGN.md §5):
// ZONE_RESOURCE_POOL_EXHAUSTED extraction from Cloud Logging. The
// signal is the compute.instances.insert admin-activity audit
// entry whose status message carries the stockout code — the same
// failure the GKE CA visibility logs (§10.1 source 3, the capacity
// source's input) report per-MIG as GCE_STOCKOUT; this read goes to
// the audit log directly so it also sees stockouts on VMs no
// autoscaler asked for.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	logging "google.golang.org/api/logging/v2"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// stockoutCode is the GCE operation error code extracted. The
// _WITH_DETAILS variant shares the prefix, so substring matching
// catches both.
const stockoutCode = "ZONE_RESOURCE_POOL_EXHAUSTED"

// stockoutMaxPages bounds the Cloud Logging paging loop: a window
// with >20k matching failures is a storm the resident capacity
// source owns; the point-in-time read reports what fits its budget.
const stockoutMaxPages = 20

// stockoutEntryLister is the §13 small client interface over the
// one Cloud Logging call the sweep needs; production is
// entries.list, tests replay recorded response fixtures.
type stockoutEntryLister interface {
	ListEntries(ctx context.Context, req *logging.ListLogEntriesRequest) (*logging.ListLogEntriesResponse, error)
}

// stockoutAPI implements cloud.StockoutAPI.
type stockoutAPI struct {
	project string
	entries stockoutEntryLister
}

func newStockoutAPI(p *Provider) *stockoutAPI {
	svc := lazyClient(func(ctx context.Context) (*logging.Service, error) {
		return logging.NewService(ctx)
	})
	return &stockoutAPI{
		project: p.project,
		entries: &loggingEntryClient{svc: svc},
	}
}

// loggingEntryClient is the production stockoutEntryLister.
type loggingEntryClient struct {
	svc func(ctx context.Context) (*logging.Service, error)
}

func (c *loggingEntryClient) ListEntries(ctx context.Context, req *logging.ListLogEntriesRequest) (*logging.ListLogEntriesResponse, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloud logging client: %w", err)
	}
	return svc.Entries.List(req).Context(ctx).Do()
}

// stockoutFilter is the Cloud Logging filter for window w: failed
// compute.instances.insert admin-activity entries carrying the
// stockout code.
func stockoutFilter(project string, w cloud.TimeWindow) string {
	return fmt.Sprintf(
		`logName="projects/%s/logs/cloudaudit.googleapis.com%%2Factivity"`+"\n"+
			`resource.type="gce_instance"`+"\n"+
			`protoPayload.methodName:"compute.instances.insert"`+"\n"+
			`protoPayload.status.message:%q`+"\n"+
			`timestamp>=%q AND timestamp<%q`,
		project, stockoutCode,
		w.Start.UTC().Format(time.RFC3339), w.End.UTC().Format(time.RFC3339))
}

// auditPayload is the slice of the audit-log proto payload the
// extraction reads (the payload arrives as JSON in the REST
// response).
type auditPayload struct {
	Status struct {
		Message string `json:"message"`
	} `json:"status"`
	Request struct {
		MachineType string `json:"machineType"`
	} `json:"request"`
}

// Stockouts implements cloud.StockoutAPI.
func (s *stockoutAPI) Stockouts(ctx context.Context, w cloud.TimeWindow) ([]cloud.Stockout, error) {
	req := &logging.ListLogEntriesRequest{
		ResourceNames: []string{"projects/" + s.project},
		Filter:        stockoutFilter(s.project, w),
		OrderBy:       "timestamp desc",
		PageSize:      1000,
	}
	var out []cloud.Stockout
	for page := 0; page < stockoutMaxPages; page++ {
		resp, err := s.entries.ListEntries(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("listing stockout log entries: %w", err)
		}
		for _, e := range resp.Entries {
			rec, ok := parseStockoutEntry(e)
			if ok {
				out = append(out, rec)
			}
		}
		if resp.NextPageToken == "" {
			break
		}
		req.PageToken = resp.NextPageToken
	}
	return out, nil
}

// parseStockoutEntry extracts one record; entries the filter let
// through but that lack the code (or a parsable payload) are
// dropped — the filter is the contract, this is the belt.
func parseStockoutEntry(e *logging.LogEntry) (cloud.Stockout, bool) {
	var zero cloud.Stockout
	if e == nil || len(e.ProtoPayload) == 0 {
		return zero, false
	}
	var p auditPayload
	if err := json.Unmarshal(e.ProtoPayload, &p); err != nil {
		return zero, false
	}
	if !strings.Contains(p.Status.Message, stockoutCode) {
		return zero, false
	}
	t, err := time.Parse(time.RFC3339Nano, e.Timestamp)
	if err != nil {
		return zero, false
	}
	rec := cloud.Stockout{
		Time:        t,
		MachineType: resourceTail(p.Request.MachineType),
		Message:     p.Status.Message,
	}
	if e.Resource != nil {
		rec.Zone = e.Resource.Labels["zone"]
	}
	return rec, true
}
