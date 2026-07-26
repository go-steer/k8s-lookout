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
	"fmt"
	"testing"
	"time"

	compute "google.golang.org/api/compute/v1"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// lbObjects is the compute-lb-objects.json fixture bundle: GET
// response bodies keyed by resource name.
type lbObjects struct {
	BackendServices       map[string]*compute.BackendService       `json:"backendServices"`
	URLMaps               map[string]*compute.UrlMap               `json:"urlMaps"`
	TargetHTTPProxies     map[string]*compute.TargetHttpProxy      `json:"targetHttpProxies"`
	TargetPools           map[string]*compute.TargetPool           `json:"targetPools"`
	NetworkEndpointGroups map[string]*compute.NetworkEndpointGroup `json:"networkEndpointGroups"`
}

// fixtureGCE implements orphanComputeAPI from the recorded fixtures,
// flattening aggregates through the same helpers production paging
// uses.
type fixtureGCE struct {
	t   *testing.T
	lbs lbObjects
}

func newFixtureGCE(t *testing.T) *fixtureGCE {
	t.Helper()
	f := &fixtureGCE{t: t}
	loadJSON(t, "compute-lb-objects.json", &f.lbs)
	return f
}

func (f *fixtureGCE) ListDisks(context.Context) ([]*compute.Disk, error) {
	var page compute.DiskAggregatedList
	loadJSON(f.t, "compute-disks-aggregated.json", &page)
	return flattenDiskAggregate(&page), nil
}

func (f *fixtureGCE) ListForwardingRules(context.Context) ([]*compute.ForwardingRule, error) {
	var page compute.ForwardingRuleAggregatedList
	loadJSON(f.t, "compute-forwardingrules-aggregated.json", &page)
	return flattenRuleAggregate(&page), nil
}

func (f *fixtureGCE) GetBackendService(_ context.Context, _, name string) (*compute.BackendService, error) {
	if bs := f.lbs.BackendServices[name]; bs != nil {
		return bs, nil
	}
	return nil, fmt.Errorf("no fixture backend service %q", name)
}

func (f *fixtureGCE) GetProxyURLMap(_ context.Context, _, kind, name string) (string, error) {
	if kind != "targetHttpProxies" {
		return "", fmt.Errorf("unexpected proxy kind %q", kind)
	}
	if p := f.lbs.TargetHTTPProxies[name]; p != nil {
		return p.UrlMap, nil
	}
	return "", fmt.Errorf("no fixture proxy %q", name)
}

func (f *fixtureGCE) GetURLMap(_ context.Context, _, name string) (*compute.UrlMap, error) {
	if um := f.lbs.URLMaps[name]; um != nil {
		return um, nil
	}
	return nil, fmt.Errorf("no fixture url map %q", name)
}

func (f *fixtureGCE) GetTargetPool(_ context.Context, region, name string) (*compute.TargetPool, error) {
	if region != "us-east1" {
		return nil, fmt.Errorf("unexpected target pool region %q", region)
	}
	if tp := f.lbs.TargetPools[name]; tp != nil {
		return tp, nil
	}
	return nil, fmt.Errorf("no fixture target pool %q", name)
}

func (f *fixtureGCE) GroupSize(_ context.Context, group string) (int64, error) {
	if neg := f.lbs.NetworkEndpointGroups[resourceTail(group)]; neg != nil {
		return neg.Size, nil
	}
	return 0, fmt.Errorf("no fixture group %q", group)
}

func TestOrphanDisksFromRecordedAggregate(t *testing.T) {
	api := &orphanAPI{gce: newFixtureGCE(t)}
	got, err := api.OrphanDisks(context.Background())
	if err != nil {
		t.Fatalf("OrphanDisks: %v", err)
	}
	// attached-boot has users, still-creating is not READY; the two
	// unattached READY disks remain (zone-key order).
	if len(got) != 2 {
		t.Fatalf("disks = %+v, want detached-ssd and never-attached", got)
	}
	ssd := got[0]
	if ssd.Name != "detached-ssd" || ssd.Zone != "us-east1-b" || ssd.SizeGB != 500 || ssd.Type != "pd-ssd" {
		t.Errorf("disk 0 = %+v, want detached-ssd us-east1-b 500GB pd-ssd (URL tails resolved)", ssd)
	}
	if !ssd.UnusedSince.Equal(time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("detached-ssd UnusedSince = %v, want the lastDetachTimestamp", ssd.UnusedSince)
	}
	never := got[1]
	if never.Name != "never-attached" || never.Zone != "us-east1-c" {
		t.Fatalf("disk 1 = %+v, want never-attached", never)
	}
	// Never attached → dated from creation (offset timestamps parse
	// too: the fixture uses the -07:00 form the API emits).
	if !never.UnusedSince.Equal(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("never-attached UnusedSince = %v, want its creationTimestamp", never.UnusedSince)
	}
}

func TestOrphanLoadBalancersFromRecordedObjects(t *testing.T) {
	api := &orphanAPI{gce: newFixtureGCE(t)}
	got, err := api.OrphanLoadBalancers(context.Background())
	if err != nil {
		t.Fatalf("OrphanLoadBalancers: %v", err)
	}
	// ilb-empty (backend service → empty NEG) and netlb-ghost
	// (empty target pool) are orphans. ingress-live resolves proxy →
	// url map → web-bs → NEGs with 3 endpoints; netlb-live's pool
	// has an instance; ssl-unjudged's target kind is skipped.
	if len(got) != 2 {
		t.Fatalf("orphans = %+v, want ilb-empty and netlb-ghost only", got)
	}
	byName := map[string]cloud.OrphanLoadBalancer{}
	for _, o := range got {
		byName[o.Name] = o
	}
	ilb := byName["ilb-empty"]
	if ilb.Region != "us-east1" || ilb.Reason != "backend service empty-bs has 0 endpoints across all groups" {
		t.Errorf("ilb-empty = %+v, want region+judgment", ilb)
	}
	ghost := byName["netlb-ghost"]
	if ghost.Region != "us-east1" || ghost.Reason != "target pool ghost-pool has no instances" {
		t.Errorf("netlb-ghost = %+v, want the empty-pool judgment", ghost)
	}
}

func TestURLMapServicesDeduplicated(t *testing.T) {
	var lbs lbObjects
	loadJSON(t, "compute-lb-objects.json", &lbs)
	services := urlMapServices(lbs.URLMaps["web-um"])
	// default service + path matcher default + path rule all point
	// at web-bs → exactly one entry.
	if len(services) != 1 || resourceTail(services[0]) != "web-bs" {
		t.Errorf("urlMapServices = %v, want the single deduplicated web-bs", services)
	}
}
