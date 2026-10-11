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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/option"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// lbObjects is the compute-lb-objects.json fixture bundle: GET
// response bodies keyed by resource name.
type lbObjects struct {
	BackendServices       map[string]*compute.BackendService       `json:"backendServices"`
	URLMaps               map[string]*compute.UrlMap               `json:"urlMaps"`
	TargetHTTPProxies     map[string]*compute.TargetHttpProxy      `json:"targetHttpProxies"`
	TargetHTTPSProxies    map[string]*compute.TargetHttpsProxy     `json:"targetHttpsProxies"`
	TargetPools           map[string]*compute.TargetPool           `json:"targetPools"`
	NetworkEndpointGroups map[string]*compute.NetworkEndpointGroup `json:"networkEndpointGroups"`
	InstanceGroups        map[string]*compute.InstanceGroup        `json:"instanceGroups"`
}

// fixtureGCE implements orphanComputeAPI from the recorded fixtures,
// flattening aggregates through the same helpers production paging
// uses. Every GET checks the scope it was asked for against the
// recorded object's own location, so a regional object fetched as
// global or zonal (#612) fails the way the live API would.
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

// checkScope fails when the requested scope ("" = global) is not the
// recorded object's region.
func checkScope(what, name, scope, recordedRegion string) error {
	if want := resourceTail(recordedRegion); scope != want {
		return fmt.Errorf("%s %q fetched with scope %q, recorded in %q", what, name, scope, want)
	}
	return nil
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

func (f *fixtureGCE) ListAddresses(context.Context) ([]*compute.Address, error) {
	var page compute.AddressAggregatedList
	loadJSON(f.t, "compute-addresses-aggregated.json", &page)
	return flattenAddressAggregate(&page), nil
}

func (f *fixtureGCE) GetBackendService(_ context.Context, service string) (*compute.BackendService, error) {
	name := resourceTail(service)
	bs := f.lbs.BackendServices[name]
	if bs == nil {
		return nil, fmt.Errorf("no fixture backend service %q", name)
	}
	return bs, checkScope("backend service", name, resourceScopeValue(service, "regions"), bs.Region)
}

func (f *fixtureGCE) GetProxyURLMap(_ context.Context, scope, kind, name string) (string, error) {
	switch kind {
	case "targetHttpProxies":
		if p := f.lbs.TargetHTTPProxies[name]; p != nil {
			return p.UrlMap, checkScope("proxy", name, scope, p.Region)
		}
	case "targetHttpsProxies":
		if p := f.lbs.TargetHTTPSProxies[name]; p != nil {
			return p.UrlMap, checkScope("proxy", name, scope, p.Region)
		}
	default:
		return "", fmt.Errorf("unexpected proxy kind %q", kind)
	}
	return "", fmt.Errorf("no fixture proxy %q", name)
}

func (f *fixtureGCE) GetURLMap(_ context.Context, scope, name string) (*compute.UrlMap, error) {
	if um := f.lbs.URLMaps[name]; um != nil {
		return um, checkScope("url map", name, scope, um.Region)
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

func (f *fixtureGCE) GetBackendGroup(_ context.Context, group string) (backendGroup, error) {
	collection, ok := backendGroupShape(group)
	if !ok {
		f.t.Errorf("GetBackendGroup called with unfetchable group URL %q", group)
		return backendGroup{}, fmt.Errorf("unfetchable group %q", group)
	}
	name := resourceTail(group)
	switch collection {
	case collectionNEG:
		if neg := f.lbs.NetworkEndpointGroups[name]; neg != nil {
			return backendGroup{EndpointType: neg.NetworkEndpointType, Size: neg.Size}, nil
		}
	case collectionInstanceGroup:
		if ig := f.lbs.InstanceGroups[name]; ig != nil {
			scope := resourceScopeValue(group, "zones")
			if scope == "" {
				scope = resourceScopeValue(group, "regions")
			}
			if want := resourceTail(ig.Zone + ig.Region); scope != want {
				return backendGroup{}, fmt.Errorf("instance group %q fetched in %q, recorded in %q", name, scope, want)
			}
			return backendGroup{Size: ig.Size}, nil
		}
	}
	return backendGroup{}, fmt.Errorf("no fixture group %q", group)
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

func TestOrphanAddressesFromRecordedAggregate(t *testing.T) {
	api := &orphanAPI{gce: newFixtureGCE(t)}
	got, err := api.OrphanAddresses(context.Background())
	if err != nil {
		t.Fatalf("OrphanAddresses: %v", err)
	}
	// live-ingress-ip is IN_USE, ilb-vip is INTERNAL (not billed
	// idle), being-reserved is still RESERVING; the us-west1 scope is
	// an empty-page warning. Two idle external addresses remain, in
	// scope-key order (global sorts before regions/…).
	if len(got) != 2 {
		t.Fatalf("addresses = %+v, want old-ingress-ip and spare-egress-ip", got)
	}
	global := got[0]
	if global.Name != "old-ingress-ip" || global.Region != "global" || global.Address != "34.120.10.20" || global.Tier != "PREMIUM" {
		t.Errorf("address 0 = %+v, want old-ingress-ip global 34.120.10.20 PREMIUM (empty addressType is EXTERNAL)", global)
	}
	if !global.ReservedSince.Equal(time.Date(2026, 3, 1, 17, 0, 0, 0, time.UTC)) {
		t.Errorf("old-ingress-ip ReservedSince = %v, want its creationTimestamp", global.ReservedSince)
	}
	regional := got[1]
	if regional.Name != "spare-egress-ip" || regional.Region != "us-east1" || regional.Tier != "STANDARD" {
		t.Errorf("address 1 = %+v, want spare-egress-ip in us-east1 (URL tail resolved)", regional)
	}
}

func TestOrphanLoadBalancersFromRecordedObjects(t *testing.T) {
	api := &orphanAPI{gce: newFixtureGCE(t)}
	got, err := api.OrphanLoadBalancers(context.Background())
	if err != nil {
		t.Fatalf("OrphanLoadBalancers: %v", err)
	}
	// Orphans: ilb-empty (backend service → empty NEG), netlb-ghost
	// (empty target pool), mig-empty-http (global HTTP → empty zonal
	// and regional instance groups).
	//
	// Live: ingress-live (proxy → url map → web-bs → NEGs with 3
	// endpoints), netlb-live (pool has an instance), the #612 rule
	// (global HTTP → global backend service → regional MIG of 3),
	// origin-http (route rule → internet NEG with an endpoint).
	//
	// Skipped, not guessed: ssl-unjudged (target kind), redirect-http
	// (redirect-only url map), bucket-http (backend bucket),
	// serverless-https (serverless NEG, size always 0), ilb7-mixed
	// (regional HTTPS: an empty zonal NEG beside a serverless NEG —
	// the serverless side cannot be counted, so no verdict).
	byName := map[string]cloud.OrphanLoadBalancer{}
	for _, o := range got {
		byName[o.Name] = o
	}
	if len(got) != 3 {
		t.Fatalf("orphans = %+v, want ilb-empty, netlb-ghost and mig-empty-http only", got)
	}
	ilb := byName["ilb-empty"]
	if ilb.Region != "us-east1" || ilb.Reason != "backend service empty-bs has 0 endpoints across all groups" {
		t.Errorf("ilb-empty = %+v, want region+judgment", ilb)
	}
	ghost := byName["netlb-ghost"]
	if ghost.Region != "us-east1" || ghost.Reason != "target pool ghost-pool has no instances" {
		t.Errorf("netlb-ghost = %+v, want the empty-pool judgment", ghost)
	}
	mig := byName["mig-empty-http"]
	if mig.Region != "global" || mig.Reason != "backend service mig-empty-bs has 0 endpoints across all groups" {
		t.Errorf("mig-empty-http = %+v, want the global empty-instance-group judgment", mig)
	}
}

// failingGCE wraps the fixtures and fails one backend service GET,
// as a live 400/500 on one rule's chain would.
type failingGCE struct {
	*fixtureGCE
	service string
	err     error
}

func (f *failingGCE) GetBackendService(ctx context.Context, service string) (*compute.BackendService, error) {
	if resourceTail(service) == f.service {
		return nil, f.err
	}
	return f.fixtureGCE.GetBackendService(ctx, service)
}

// TestOrphanLoadBalancersRuleErrorNamesRule: a GET failing on one
// rule's chain fails the class with the rule named, for the command
// to degrade to a class=lbs unavailable (cloudcheck owns that rule).
func TestOrphanLoadBalancersRuleErrorNamesRule(t *testing.T) {
	api := &orphanAPI{gce: &failingGCE{
		fixtureGCE: newFixtureGCE(t),
		service:    "hypercomputer-w1-traffic-backend",
		err:        errors.New("googleapi: Error 500: Internal Error, backendError"),
	}}
	_, err := api.OrphanLoadBalancers(context.Background())
	if err == nil || !strings.Contains(err.Error(), `"hypercomputer-w1-traffic-http-forwarding-rule"`) ||
		!strings.Contains(err.Error(), "Error 500") {
		t.Fatalf("err = %v, want the 500 wrapped with the rule's name", err)
	}
	if _, denied := cloud.PermissionDenied(err); denied {
		t.Errorf("err = %v classified as a permission refusal; a 500 is not one", err)
	}
}

func TestURLMapRoutes(t *testing.T) {
	var lbs lbObjects
	loadJSON(t, "compute-lb-objects.json", &lbs)
	cases := []struct {
		urlMap        string
		wantServices  []string
		wantElsewhere bool
	}{
		// default service + path matcher default + path rule all
		// point at web-bs → exactly one entry.
		{"web-um", []string{"web-bs"}, false},
		// A route rule's weighted backend services count.
		{"origin-um", []string{"origin-bs"}, false},
		{"redirect-um", nil, true},
		{"bucket-um", nil, true},
		{"ilb7-um", []string{"ilb7-bs"}, false},
	}
	for _, c := range cases {
		services, elsewhere := urlMapRoutes(lbs.URLMaps[c.urlMap])
		var tails []string
		for _, s := range services {
			tails = append(tails, resourceTail(s))
		}
		if !slices.Equal(tails, c.wantServices) || elsewhere != c.wantElsewhere {
			t.Errorf("urlMapRoutes(%s) = %v, %v; want %v, %v", c.urlMap, tails, elsewhere, c.wantServices, c.wantElsewhere)
		}
	}
}

func TestBackendGroupShape(t *testing.T) {
	const base = "https://www.googleapis.com/compute/v1/projects/proj-1/"
	cases := []struct {
		group          string
		wantCollection string
		wantOK         bool
	}{
		{base + "zones/us-east1-b/networkEndpointGroups/n", collectionNEG, true},
		{base + "regions/us-east1/networkEndpointGroups/n", collectionNEG, true},
		{base + "global/networkEndpointGroups/n", collectionNEG, true},
		{base + "zones/us-east1-b/instanceGroups/ig", collectionInstanceGroup, true},
		{base + "regions/us-east1/instanceGroups/mig", collectionInstanceGroup, true},
		// No global instance groups exist; never fetched zone="".
		{base + "global/instanceGroups/ig", collectionInstanceGroup, false},
		{base + "global/backendBuckets/b", "", false},
	}
	for _, c := range cases {
		collection, ok := backendGroupShape(c.group)
		if collection != c.wantCollection || ok != c.wantOK {
			t.Errorf("backendGroupShape(%s) = %q, %v; want %q, %v", c.group, collection, ok, c.wantCollection, c.wantOK)
		}
	}
}

// TestGCEOrphanClientBackendGroupPaths drives the production client
// against a recording HTTP server: each group URL shape reaches its
// own Compute collection — a regional instance group the regional
// API, never the zonal one with zone="" (#612) — and a URL naming
// another project is fetched there.
func TestGCEOrphanClientBackendGroupPaths(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/networkEndpointGroups/"):
			_, _ = w.Write([]byte(`{"networkEndpointType":"GCE_VM_IP_PORT","size":2}`))
		case strings.Contains(r.URL.Path, "/backendServices/"):
			_, _ = w.Write([]byte(`{"name":"bs"}`))
		default:
			_, _ = w.Write([]byte(`{"size":3}`))
		}
	}))
	defer srv.Close()
	c := &gceOrphanClient{
		project: "proj-1",
		svc: func(ctx context.Context) (*compute.Service, error) {
			return compute.NewService(ctx, option.WithEndpoint(srv.URL+"/compute/v1/"), option.WithoutAuthentication())
		},
	}
	const base = "https://www.googleapis.com/compute/v1/projects/"
	groups := []struct{ url, wantPath string }{
		{base + "proj-1/regions/us-central1/instanceGroups/hypercomputer-w1-mig", "/compute/v1/projects/proj-1/regions/us-central1/instanceGroups/hypercomputer-w1-mig"},
		{base + "proj-1/zones/us-east1-b/instanceGroups/ig", "/compute/v1/projects/proj-1/zones/us-east1-b/instanceGroups/ig"},
		{base + "proj-1/zones/us-east1-b/networkEndpointGroups/n", "/compute/v1/projects/proj-1/zones/us-east1-b/networkEndpointGroups/n"},
		{base + "proj-1/regions/us-east1/networkEndpointGroups/run", "/compute/v1/projects/proj-1/regions/us-east1/networkEndpointGroups/run"},
		{base + "proj-1/global/networkEndpointGroups/origin", "/compute/v1/projects/proj-1/global/networkEndpointGroups/origin"},
	}
	ctx := context.Background()
	for _, g := range groups {
		paths = nil
		if _, err := c.GetBackendGroup(ctx, g.url); err != nil {
			t.Errorf("GetBackendGroup(%s): %v", g.url, err)
			continue
		}
		if len(paths) != 1 || paths[0] != g.wantPath {
			t.Errorf("GetBackendGroup(%s) requested %v, want %s", g.url, paths, g.wantPath)
		}
	}
	paths = nil
	if _, err := c.GetBackendGroup(ctx, base+"proj-1/global/instanceGroups/ig"); err == nil || len(paths) != 0 {
		t.Errorf("unscoped instance group: err=%v requests=%v, want an error and no request", err, paths)
	}
	paths = nil
	if _, err := c.GetBackendService(ctx, base+"shared-proj/global/backendServices/bs"); err != nil {
		t.Fatalf("GetBackendService: %v", err)
	}
	if len(paths) != 1 || paths[0] != "/compute/v1/projects/shared-proj/global/backendServices/bs" {
		t.Errorf("cross-project backend service requested %v, want it fetched from shared-proj", paths)
	}
}
