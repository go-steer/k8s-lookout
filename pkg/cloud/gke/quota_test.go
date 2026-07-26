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

import (
	"context"
	"strings"
	"testing"

	compute "google.golang.org/api/compute/v1"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// quotaFixtures is the compute-quotas.json bundle.
type quotaFixtures struct {
	Project *compute.Project `json:"project"`
	Region  *compute.Region  `json:"region"`
}

type fixtureQuotaGCE struct {
	t            *testing.T
	regionsAsked []string
}

func (f *fixtureQuotaGCE) GetProject(context.Context) (*compute.Project, error) {
	var fx quotaFixtures
	loadJSON(f.t, "compute-quotas.json", &fx)
	return fx.Project, nil
}

func (f *fixtureQuotaGCE) GetRegion(_ context.Context, region string) (*compute.Region, error) {
	f.regionsAsked = append(f.regionsAsked, region)
	var fx quotaFixtures
	loadJSON(f.t, "compute-quotas.json", &fx)
	return fx.Region, nil
}

func TestQuotasFromRecordedProjectAndRegion(t *testing.T) {
	gce := &fixtureQuotaGCE{t: t}
	api := &quotaAPI{location: "us-east1-b", gce: gce}
	got, err := api.Quotas(context.Background())
	if err != nil {
		t.Fatalf("Quotas: %v", err)
	}
	if len(gce.regionsAsked) != 1 || gce.regionsAsked[0] != "us-east1" {
		t.Errorf("regions asked = %v, want [us-east1] derived from the zonal location", gce.regionsAsked)
	}
	// 3 global + 3 regional, scopes stamped.
	if len(got) != 6 {
		t.Fatalf("quotas = %d, want 6: %+v", len(got), got)
	}
	byName := map[string]cloud.QuotaUsage{}
	for _, q := range got {
		byName[q.Scope+"/"+q.Name] = q
	}
	if q := byName["global/CPUS_ALL_REGIONS"]; q.Usage != 620 || q.Limit != 1200 {
		t.Errorf("global CPUS_ALL_REGIONS = %+v, want 620/1200", q)
	}
	if q := byName["us-east1/CPUS"]; q.Usage != 588 || q.Limit != 600 {
		t.Errorf("us-east1 CPUS = %+v, want 588/600", q)
	}
	// The zero-limit quota passes through untouched — rating it is
	// the command's policy, not the provider's.
	if q, ok := byName["us-east1/NVIDIA_H100_GPUS"]; !ok || q.Limit != 0 {
		t.Errorf("zero-limit quota = %+v, want present with limit 0", q)
	}
}

func TestQuotasWithoutLocationAreGlobalOnly(t *testing.T) {
	gce := &fixtureQuotaGCE{t: t}
	api := &quotaAPI{location: "", gce: gce}
	got, err := api.Quotas(context.Background())
	if err != nil {
		t.Fatalf("Quotas: %v", err)
	}
	if len(gce.regionsAsked) != 0 {
		t.Errorf("regions asked = %v, want none without a location", gce.regionsAsked)
	}
	for _, q := range got {
		if q.Scope != "global" {
			t.Errorf("quota %+v, want global scope only", q)
		}
	}
	if len(got) != 3 {
		t.Errorf("quotas = %d, want the 3 global ones", len(got))
	}
}

func TestQuotaHistoryIsExplicitlyUnserved(t *testing.T) {
	api := &quotaAPI{location: "us-east1", gce: &fixtureQuotaGCE{t: t}}
	_, err := api.History(context.Background(), "CPUS", "us-east1", cloud.TimeWindow{})
	if err == nil || !strings.Contains(err.Error(), "quota source") {
		t.Errorf("History error = %v, want the explicit §10.2 pointer", err)
	}
}
