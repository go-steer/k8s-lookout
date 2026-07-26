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

// §13: recorded JSON fixtures (authored from the API references —
// provenance stated in each fixture's "//" header) replayed through
// the small client interfaces; no live-project calls.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	logging "google.golang.org/api/logging/v2"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// loadJSON unmarshals one testdata fixture into v.
func loadJSON(t *testing.T, name string, v any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("parsing fixture %s: %v", name, err)
	}
}

var stockoutWindow = cloud.TimeWindow{
	Start: time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC),
	End:   time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC),
}

// fakeEntryLister replays recorded pages and captures requests.
type fakeEntryLister struct {
	pages []*logging.ListLogEntriesResponse
	reqs  []*logging.ListLogEntriesRequest
}

func (f *fakeEntryLister) ListEntries(_ context.Context, req *logging.ListLogEntriesRequest) (*logging.ListLogEntriesResponse, error) {
	f.reqs = append(f.reqs, req)
	if len(f.pages) == 0 {
		return &logging.ListLogEntriesResponse{}, nil
	}
	page := f.pages[0]
	f.pages = f.pages[1:]
	return page, nil
}

func TestStockoutFilterShape(t *testing.T) {
	f := stockoutFilter("proj-1", stockoutWindow)
	for _, want := range []string{
		`logName="projects/proj-1/logs/cloudaudit.googleapis.com%2Factivity"`,
		`resource.type="gce_instance"`,
		`protoPayload.methodName:"compute.instances.insert"`,
		`protoPayload.status.message:"ZONE_RESOURCE_POOL_EXHAUSTED"`,
		`timestamp>="2026-07-24T12:00:00Z" AND timestamp<"2026-07-25T12:00:00Z"`,
	} {
		if !strings.Contains(f, want) {
			t.Errorf("filter missing %q:\n%s", want, f)
		}
	}
}

func TestStockoutsFromRecordedEntries(t *testing.T) {
	var resp logging.ListLogEntriesResponse
	loadJSON(t, "logging-entries-stockout.json", &resp)
	lister := &fakeEntryLister{pages: []*logging.ListLogEntriesResponse{&resp}}
	api := &stockoutAPI{project: "proj-1", entries: lister}

	got, err := api.Stockouts(context.Background(), stockoutWindow)
	if err != nil {
		t.Fatalf("Stockouts: %v", err)
	}
	// The fixture carries three entries; the QUOTA_EXCEEDED one is
	// dropped by the parser belt.
	if len(got) != 2 {
		t.Fatalf("records = %d, want 2 (non-stockout entry dropped): %+v", len(got), got)
	}
	first := got[0]
	if first.Zone != "us-east1-b" || first.MachineType != "n2-standard-16" {
		t.Errorf("record 0 = %+v, want us-east1-b/n2-standard-16 (machine type from the request URL tail)", first)
	}
	if !first.Time.Equal(time.Date(2026, 7, 25, 11, 30, 12, 345678000, time.UTC)) {
		t.Errorf("record 0 time = %v, want the entry timestamp", first.Time)
	}
	// The _WITH_DETAILS variant matches by prefix; partial machine
	// type URL still resolves to its tail.
	second := got[1]
	if second.Zone != "us-east1-c" || second.MachineType != "a2-highgpu-1g" ||
		!strings.HasPrefix(second.Message, "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS") {
		t.Errorf("record 1 = %+v, want the _WITH_DETAILS variant from us-east1-c", second)
	}

	// Request shape: one page, scoped to the project, filter bound
	// to the window.
	if len(lister.reqs) != 1 {
		t.Fatalf("requests = %d, want 1 (no next page token)", len(lister.reqs))
	}
	req := lister.reqs[0]
	if len(req.ResourceNames) != 1 || req.ResourceNames[0] != "projects/proj-1" {
		t.Errorf("resource names = %v, want [projects/proj-1]", req.ResourceNames)
	}
	if req.OrderBy != "timestamp desc" || req.PageSize != 1000 {
		t.Errorf("req order/page = %q/%d, want timestamp desc / 1000", req.OrderBy, req.PageSize)
	}
}

func TestStockoutsPaging(t *testing.T) {
	entry := func(ts string) *logging.LogEntry {
		return &logging.LogEntry{
			Timestamp:    ts,
			ProtoPayload: []byte(`{"status":{"message":"ZONE_RESOURCE_POOL_EXHAUSTED"},"request":{"machineType":"zones/z/machineTypes/e2-medium"}}`),
		}
	}
	lister := &fakeEntryLister{pages: []*logging.ListLogEntriesResponse{
		{Entries: []*logging.LogEntry{entry("2026-07-25T10:00:00Z")}, NextPageToken: "page-2"},
		{Entries: []*logging.LogEntry{entry("2026-07-25T09:00:00Z")}},
	}}
	api := &stockoutAPI{project: "proj-1", entries: lister}
	got, err := api.Stockouts(context.Background(), stockoutWindow)
	if err != nil {
		t.Fatalf("Stockouts: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("records = %d, want both pages consumed", len(got))
	}
	if len(lister.reqs) != 2 || lister.reqs[1].PageToken != "page-2" {
		t.Errorf("paging requests = %+v, want the second carrying page-2", lister.reqs)
	}
}
