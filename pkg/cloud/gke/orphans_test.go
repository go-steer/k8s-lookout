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
// recorded object's own location — zone, region or global — so a
// regional object fetched as global or zonal (#612) fails the way the
// live API would; GETs by URL (backend services and groups) check the
// URL's project too.
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

// fixtureProject is the project every recorded object lives in.
const fixtureProject = "proj-1"

// checkURL fails when a GET-by-URL names another project than the
// fixtures', or a zone/region other than the recorded object's ("" on
// both sides = global).
func checkURL(what, u, recordedZone, recordedRegion string) error {
	if p := resourceScopeValue(u, "projects"); p != fixtureProject {
		return fmt.Errorf("%s %q fetched from project %q, recorded in %q", what, resourceTail(u), p, fixtureProject)
	}
	gotZone, gotRegion := resourceScopeValue(u, "zones"), resourceScopeValue(u, "regions")
	if gotZone != resourceTail(recordedZone) || gotRegion != resourceTail(recordedRegion) {
		return fmt.Errorf("%s %q fetched as zone=%q region=%q, recorded as zone=%q region=%q",
			what, resourceTail(u), gotZone, gotRegion, resourceTail(recordedZone), resourceTail(recordedRegion))
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
	return bs, checkURL("backend service", service, "", bs.Region)
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
			return backendGroup{EndpointType: neg.NetworkEndpointType, Size: neg.Size}, checkURL("NEG", group, neg.Zone, neg.Region)
		}
	case collectionInstanceGroup:
		if ig := f.lbs.InstanceGroups[name]; ig != nil {
			return backendGroup{Size: ig.Size}, checkURL("instance group", group, ig.Zone, ig.Region)
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
	sweep, err := api.OrphanLoadBalancers(context.Background())
	if err != nil {
		t.Fatalf("OrphanLoadBalancers: %v", err)
	}
	if len(sweep.Unresolved) != 0 {
		t.Fatalf("unresolved = %+v, want every recorded rule resolved", sweep.Unresolved)
	}
	got := sweep.Orphans
	// Orphans: ilb-empty (backend service → empty NEG), netlb-ghost
	// (empty target pool), mig-empty-http (global HTTP → empty zonal
	// and regional instance groups).
	//
	// Live: ingress-live and ingress-live-https (HTTP and HTTPS
	// proxies → one url map → web-bs → NEGs with 3 endpoints),
	// netlb-live (pool has an instance), the #612 rule
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
// as a live 404 (deleted between the list and the get) or 500 on one
// rule's chain would.
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

// TestOrphanLoadBalancersRuleErrorCostsTheRuleOnly: a GET failing on
// one rule's chain makes that rule unresolved, error kept, and every
// other rule is still judged — the orphans included (#612).
func TestOrphanLoadBalancersRuleErrorCostsTheRuleOnly(t *testing.T) {
	api := &orphanAPI{gce: &failingGCE{
		fixtureGCE: newFixtureGCE(t),
		service:    "hypercomputer-w1-traffic-backend",
		err:        errors.New("googleapi: Error 404: The resource 'hypercomputer-w1-traffic-backend' was not found, notFound"),
	}}
	sweep, err := api.OrphanLoadBalancers(context.Background())
	if err != nil {
		t.Fatalf("err = %v, want the sweep to succeed around the failed rule", err)
	}
	if len(sweep.Unresolved) != 1 {
		t.Fatalf("unresolved = %+v, want the #612 rule only", sweep.Unresolved)
	}
	u := sweep.Unresolved[0]
	if u.Name != "hypercomputer-w1-traffic-http-forwarding-rule" || u.Region != "global" || !strings.Contains(u.Err.Error(), "Error 404") {
		t.Errorf("unresolved = %+v, want the rule, global, with the 404", u)
	}
	if _, denied := cloud.PermissionDenied(u.Err); denied {
		t.Errorf("err = %v classified as a permission refusal; a 404 is not one", u.Err)
	}
	if len(sweep.Orphans) != 3 {
		t.Errorf("orphans = %+v, want the three recorded orphans still judged", sweep.Orphans)
	}
}

// TestOrphanLoadBalancersContextEndsTheClass: a call cut off by the
// context is not one rule's failure — every later rule would fail the
// same way — so it fails the class, as before.
func TestOrphanLoadBalancersContextEndsTheClass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	api := &orphanAPI{gce: &failingGCE{fixtureGCE: newFixtureGCE(t), service: "web-bs", err: context.Canceled}}
	if _, err := api.OrphanLoadBalancers(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want the cancellation to fail the class", err)
	}
}

// countingGCE counts the GETs that reach the fixtures, and serves
// only the given rules.
type countingGCE struct {
	*fixtureGCE
	rules []string
	calls map[string]int
	// serviceErr fails the named backend services' GETs.
	serviceErr map[string]error
}

func (c *countingGCE) ListForwardingRules(ctx context.Context) ([]*compute.ForwardingRule, error) {
	all, err := c.fixtureGCE.ListForwardingRules(ctx)
	var out []*compute.ForwardingRule
	for _, r := range all {
		if slices.Contains(c.rules, r.Name) {
			out = append(out, r)
		}
	}
	return out, err
}

func (c *countingGCE) GetProxyURLMap(ctx context.Context, scope, kind, name string) (string, error) {
	c.calls["proxy"]++
	return c.fixtureGCE.GetProxyURLMap(ctx, scope, kind, name)
}

func (c *countingGCE) GetURLMap(ctx context.Context, scope, name string) (*compute.UrlMap, error) {
	c.calls["urlMap"]++
	return c.fixtureGCE.GetURLMap(ctx, scope, name)
}

func (c *countingGCE) GetBackendService(ctx context.Context, service string) (*compute.BackendService, error) {
	c.calls["backendService"]++
	if err := c.serviceErr[resourceTail(service)]; err != nil {
		return nil, err
	}
	return c.fixtureGCE.GetBackendService(ctx, service)
}

func (c *countingGCE) GetBackendGroup(ctx context.Context, group string) (backendGroup, error) {
	c.calls["group"]++
	return c.fixtureGCE.GetBackendGroup(ctx, group)
}

// TestOrphanLoadBalancersSharedURLMapFetchedOnce: a GKE Ingress's
// HTTP and HTTPS rules have their own proxies but share the URL map
// and everything behind it, which one sweep fetches once.
func TestOrphanLoadBalancersSharedURLMapFetchedOnce(t *testing.T) {
	gce := &countingGCE{fixtureGCE: newFixtureGCE(t), rules: []string{"ingress-live", "ingress-live-https"}, calls: map[string]int{}}
	sweep, err := (&orphanAPI{gce: gce}).OrphanLoadBalancers(context.Background())
	if err != nil || len(sweep.Orphans) != 0 || len(sweep.Unresolved) != 0 {
		t.Fatalf("sweep = %+v, %v; want both rules judged live", sweep, err)
	}
	// Two proxies; one url map (web-um), one backend service (web-bs),
	// its two NEGs.
	want := map[string]int{"proxy": 2, "urlMap": 1, "backendService": 1, "group": 2}
	for k, n := range want {
		if gce.calls[k] != n {
			t.Errorf("%s GETs = %d, want %d (calls: %v)", k, gce.calls[k], n, gce.calls)
		}
	}

	// The memo holds failures too: a backend service both rules share
	// that answers 404 is asked once, and both rules come back
	// unresolved.
	gce = &countingGCE{fixtureGCE: newFixtureGCE(t), rules: gce.rules, calls: map[string]int{},
		serviceErr: map[string]error{"web-bs": errors.New("googleapi: Error 404: notFound")}}
	sweep, err = (&orphanAPI{gce: gce}).OrphanLoadBalancers(context.Background())
	if err != nil || len(sweep.Unresolved) != 2 {
		t.Fatalf("sweep = %+v, %v; want both rules unresolved", sweep, err)
	}
	if gce.calls["backendService"] != 1 {
		t.Errorf("failing backend service fetched %d times, want once", gce.calls["backendService"])
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
	if _, err := c.GetBackendGroup(ctx, base+"shared-proj/zones/us-east1-b/networkEndpointGroups/n"); err != nil {
		t.Fatalf("GetBackendGroup: %v", err)
	}
	if len(paths) != 1 || paths[0] != "/compute/v1/projects/shared-proj/zones/us-east1-b/networkEndpointGroups/n" {
		t.Errorf("cross-project NEG requested %v, want it fetched from shared-proj", paths)
	}
	paths = nil
	if _, err := c.GetBackendGroup(ctx, base+"shared-proj/regions/us-central1/instanceGroups/mig"); err != nil {
		t.Fatalf("GetBackendGroup: %v", err)
	}
	if len(paths) != 1 || paths[0] != "/compute/v1/projects/shared-proj/regions/us-central1/instanceGroups/mig" {
		t.Errorf("cross-project instance group requested %v, want it fetched from shared-proj", paths)
	}
	paths = nil
	if _, err := c.GetBackendService(ctx, base+"shared-proj/global/backendServices/bs"); err != nil {
		t.Fatalf("GetBackendService: %v", err)
	}
	if len(paths) != 1 || paths[0] != "/compute/v1/projects/shared-proj/global/backendServices/bs" {
		t.Errorf("cross-project backend service requested %v, want it fetched from shared-proj", paths)
	}
}
