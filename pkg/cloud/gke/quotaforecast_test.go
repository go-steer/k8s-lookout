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

// Recorded-fixture tests for the quota source's provider surfaces
// per DESIGN.md §13: Cloud Quotas metadata and Monitoring series
// each stay behind a small client interface (quotametadata.go /
// quotahistory.go), and these tests replay testdata/*.json fixtures
// AUTHORED FROM THE DOCUMENTED response formats (see each fixture's
// _comment). The compute inventory half is covered by quota_test.go;
// no live-project tests anywhere.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/cloudquotas/apiv1/cloudquotaspb"
	monitoring "google.golang.org/api/monitoring/v3"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return raw
}

// fixtureMetadata replays the cloudquotas QuotaInfo fixture through
// pqMetadataAPI. The records are protojson — the GAPIC's wire
// encoding of the same cloudquotas.googleapis.com surface.
type fixtureMetadata struct {
	infos []*cloudquotaspb.QuotaInfo
	err   error
	calls int
}

func newFixtureMetadata(t *testing.T) *fixtureMetadata {
	t.Helper()
	var envelope struct {
		QuotaInfos []json.RawMessage `json:"quotaInfos"`
	}
	if err := json.Unmarshal(readFixture(t, "cloudquotas-quotainfos.json"), &envelope); err != nil {
		t.Fatalf("quotainfos fixture: %v", err)
	}
	if len(envelope.QuotaInfos) == 0 {
		t.Fatal("quotainfos fixture has no records (shape contract)")
	}
	f := &fixtureMetadata{}
	unmarshal := protojson.UnmarshalOptions{DiscardUnknown: true}
	for _, raw := range envelope.QuotaInfos {
		info := &cloudquotaspb.QuotaInfo{}
		if err := unmarshal.Unmarshal(raw, info); err != nil {
			t.Fatalf("quotainfos fixture record is not a QuotaInfo: %v", err)
		}
		if info.GetQuotaId() == "" || info.GetMetric() == "" {
			t.Fatal("QuotaInfo fixture missing quotaId/metric (shape contract)")
		}
		f.infos = append(f.infos, info)
	}
	return f
}

func (f *fixtureMetadata) ComputeQuotaInfos(context.Context) ([]*cloudquotaspb.QuotaInfo, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.infos, nil
}

// fixtureSeries replays the two Monitoring fixtures through
// pqSeriesAPI, routing on the metric.type in the filter.
type fixtureSeries struct {
	usage, limit []*monitoring.TimeSeries
	err          error
	filters      []string
	windows      []cloud.TimeWindow
}

func loadSeriesFixture(t *testing.T, name string) []*monitoring.TimeSeries {
	t.Helper()
	var resp monitoring.ListTimeSeriesResponse
	if err := json.Unmarshal(readFixture(t, name), &resp); err != nil {
		t.Fatalf("series fixture %s: %v", name, err)
	}
	if len(resp.TimeSeries) == 0 || len(resp.TimeSeries[0].Points) == 0 {
		t.Fatalf("series fixture %s has no points (shape contract)", name)
	}
	return resp.TimeSeries
}

func newFixtureSeries(t *testing.T) *fixtureSeries {
	t.Helper()
	return &fixtureSeries{
		usage: loadSeriesFixture(t, "monitoring-quota-usage.json"),
		limit: loadSeriesFixture(t, "monitoring-quota-limit.json"),
	}
}

func (f *fixtureSeries) ListSeries(_ context.Context, filter string, w cloud.TimeWindow) ([]*monitoring.TimeSeries, error) {
	f.filters = append(f.filters, filter)
	f.windows = append(f.windows, w)
	if f.err != nil {
		return nil, f.err
	}
	if strings.Contains(filter, pqUsageMetric) {
		return f.usage, nil
	}
	return f.limit, nil
}

func fixtureFullQuotaAPI(t *testing.T, location string) (*quotaAPI, *fixtureMetadata, *fixtureSeries) {
	t.Helper()
	m, s := newFixtureMetadata(t), newFixtureSeries(t)
	return &quotaAPI{
		location: location,
		gce:      &fixtureQuotaGCE{t: t},
		metadata: m,
		series:   s,
	}, m, s
}

// TestQuotas_MetadataStampsCanonicalIDs: the Cloud Quotas metadata
// join stamps the canonical <service>/<quotaId> increase-request
// identifier (§10.3 draft input) where a record matches the compute
// metric, and leaves ID empty — the documented name fallback —
// where none does.
func TestQuotas_MetadataStampsCanonicalIDs(t *testing.T) {
	api, _, _ := fixtureFullQuotaAPI(t, "us-east1-b")
	rows, err := api.Quotas(context.Background())
	if err != nil {
		t.Fatalf("Quotas: %v", err)
	}
	byKey := map[string]cloud.QuotaUsage{}
	for _, r := range rows {
		byKey[r.Name+"/"+r.Scope] = r
	}
	if got := byKey["CPUS/us-east1"].ID; got != "compute.googleapis.com/CpusPerProjectPerRegion" {
		t.Errorf("CPUS ID = %q, want the canonical Cloud Quotas <service>/<quotaId>", got)
	}
	if got := byKey["CPUS_ALL_REGIONS/global"].ID; got != "compute.googleapis.com/CpusAllRegionsPerProject" {
		t.Errorf("CPUS_ALL_REGIONS ID = %q, want CpusAllRegionsPerProject via the cpus_all_regions metric", got)
	}
	var unmatched int
	for _, r := range rows {
		if r.ID == "" {
			unmatched++
		}
	}
	if unmatched == 0 {
		t.Error("every row matched metadata — the fixture must leave some rows on the name fallback")
	}
}

// TestQuotas_MetadataBestEffort: metadata being unreachable degrades
// IDs to empty WITHOUT failing the inventory, and is retried (not
// negatively cached) on the next call; a successful fetch is cached.
func TestQuotas_MetadataBestEffort(t *testing.T) {
	api, m, _ := fixtureFullQuotaAPI(t, "us-east1")
	m.err = errors.New("cloudquotas 403")
	rows, err := api.Quotas(context.Background())
	if err != nil {
		t.Fatalf("Quotas must not fail on metadata errors: %v", err)
	}
	for _, r := range rows {
		if r.ID != "" {
			t.Errorf("row %s carries ID %q despite metadata failure", r.Name, r.ID)
		}
	}
	m.err = nil
	rows, err = api.Quotas(context.Background())
	if err != nil {
		t.Fatalf("Quotas: %v", err)
	}
	var enriched bool
	for _, r := range rows {
		enriched = enriched || r.ID != ""
	}
	if !enriched {
		t.Error("metadata recovered but no row was enriched (failed fetch must be retried)")
	}
	if m.calls != 2 {
		t.Errorf("metadata fetched %d times, want 2 (retry after failure, then cached)", m.calls)
	}
	if _, err := api.Quotas(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.calls != 2 {
		t.Errorf("metadata fetched %d times after third Quotas call, want the cache to hold", m.calls)
	}
}

// TestHistory_FixtureSeries: the Monitoring fixtures flatten to
// ascending usage/limit points with exact values, and the filters
// carry the §10.2 serviceruntime metric types, the lowercased
// quota_metric mapping, and the location.
func TestHistory_FixtureSeries(t *testing.T) {
	api, _, s := fixtureFullQuotaAPI(t, "us-east1")
	w := cloud.TimeWindow{
		Start: time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC),
	}
	hist, err := api.History(context.Background(), "CPUS", "us-east1", w)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	wantUsage := []float64{1724, 1774, 1824} // fixture is newest-first; boundary sorts ascending
	if len(hist.Usage) != len(wantUsage) {
		t.Fatalf("usage points = %d, want %d", len(hist.Usage), len(wantUsage))
	}
	for i, want := range wantUsage {
		if hist.Usage[i].Value != want {
			t.Errorf("usage[%d] = %v, want %v", i, hist.Usage[i].Value, want)
		}
		if i > 0 && !hist.Usage[i].Time.After(hist.Usage[i-1].Time) {
			t.Errorf("usage points not ascending at %d: %v then %v", i, hist.Usage[i-1].Time, hist.Usage[i].Time)
		}
	}
	if len(hist.Limit) != 2 || hist.Limit[0].Value != 2000 || hist.Limit[1].Value != 2000 {
		t.Errorf("limit points = %+v, want two 2000 samples", hist.Limit)
	}
	if len(s.filters) != 2 {
		t.Fatalf("ListSeries called %d times, want 2 (usage + limit)", len(s.filters))
	}
	for i, mustContain := range [][]string{
		{pqUsageMetric, `metric.label.quota_metric="compute.googleapis.com/cpus"`, `resource.label.location="us-east1"`, `resource.type="consumer_quota"`},
		{pqLimitMetric, `metric.label.quota_metric="compute.googleapis.com/cpus"`},
	} {
		for _, want := range mustContain {
			if !strings.Contains(s.filters[i], want) {
				t.Errorf("filter[%d] = %s\n  missing %q", i, s.filters[i], want)
			}
		}
	}
	for _, got := range s.windows {
		if !got.Start.Equal(w.Start) || !got.End.Equal(w.End) {
			t.Errorf("series window = %+v, want the caller's %+v", got, w)
		}
	}
}

// TestHistory_EmptyIsNotAnError: a quota with no recorded series is
// a normal state (the source degrades to threshold-only judgment).
func TestHistory_EmptyIsNotAnError(t *testing.T) {
	api, _, s := fixtureFullQuotaAPI(t, "us-east1")
	s.usage, s.limit = nil, nil
	hist, err := api.History(context.Background(), "CPUS", "us-east1", cloud.TimeWindow{})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(hist.Usage) != 0 || len(hist.Limit) != 0 {
		t.Errorf("history = %+v, want empty slices", hist)
	}
}

// TestHistory_ErrorPropagates: Monitoring failing is an error the
// poll loop must see (edge-throttled log, threshold-only judgment).
func TestHistory_ErrorPropagates(t *testing.T) {
	api, _, s := fixtureFullQuotaAPI(t, "us-east1")
	s.err = errors.New("monitoring 429")
	if _, err := api.History(context.Background(), "CPUS", "us-east1", cloud.TimeWindow{}); err == nil {
		t.Fatal("History must surface series errors")
	}
}

// TestPQQuotaMetric pins the metric mapping the filters and the
// metadata join depend on.
func TestPQQuotaMetric(t *testing.T) {
	t.Parallel()
	if got := pqQuotaMetric("CPUS"); got != "compute.googleapis.com/cpus" {
		t.Errorf("pqQuotaMetric(CPUS) = %q", got)
	}
	if got := pqQuotaMetric("compute.googleapis.com/N2_CPUS"); got != "compute.googleapis.com/n2_cpus" {
		t.Errorf("pqQuotaMetric(service-qualified) = %q", got)
	}
}
