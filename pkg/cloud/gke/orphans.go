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

// OrphanAPI implementation (`cloud orphans`, DESIGN.md §5).
//
// Disks: every READY disk with no users (attachments) is returned;
// the age policy is the command's (--min-age), so this side only
// dates the idleness (lastDetachTimestamp, else creationTimestamp).
//
// Addresses: every static address that is RESERVED (reserved, not in
// use), has no users, and is EXTERNAL — an unused external static IP
// is billed by the hour, while an internal one is not billed idle and
// so is not waste. An empty addressType is the API default, EXTERNAL.
// Like disks, the age policy is the command's; this side dates the
// reservation from creationTimestamp, since the API records no "last
// released" time. The aggregated list carries global addresses under
// its "global" scope key alongside the regional ones.
//
// Load balancers: a forwarding rule is orphaned when every backend
// it routes to resolves to zero endpoints. Resolution follows the
// rule's shape, global or regional alike (#612):
//
//   - backendService rules (L4 ILB / L7-ILB via backend service):
//     the backend service's NEG/instance-group sizes;
//   - target-proxy rules (GKE Ingress, global or regional HTTP/HTTPS
//     proxy → URL map): every backend service the URL map can route
//     to, through default services, path rules, route rules and
//     weighted route actions;
//   - targetPool rules (legacy network LB): the pool's instance list.
//
// A backend is judged only when its group's size counts what the
// load balancer routes to:
//
//   - instance groups, zonal or regional: size is the instance count;
//   - NEGs, zonal, regional or global, whose networkEndpointType holds
//     attached endpoints (GCE_VM_IP_PORT, GCE_VM_IP, GCE_VM_IP_PORTMAP,
//     NON_GCP_PRIVATE_IP_PORT, INTERNET_IP_PORT, INTERNET_FQDN_PORT):
//     size is the endpoint count.
//
// Everything else is unjudgeable, never guessed at: SERVERLESS and
// PRIVATE_SERVICE_CONNECT NEGs (size is always 0 — they front a
// service, not endpoints), any other or absent endpoint type, a group
// URL of no known shape, and URL-map destinations that are not
// backend services (backend buckets, URL redirects). The verdict:
// any judged endpoint → not orphaned; else anything unjudgeable →
// skipped; else zero endpoints → orphaned. So a rule is reported only
// when every destination was judged and all were empty, and no zonal
// call is made without a zone (#612: a regional instance group once
// reached the zonal API with zone="").
//
// The judgment counts endpoint PRESENCE, not health: a rule whose
// NEGs have endpoints is not orphaned even if they are all
// unhealthy — that is an outage (state edges / triage territory),
// not an orphan. Rules with target kinds outside the three shapes
// above (SSL/TCP/gRPC proxies, target instances, VPN/IPsec, Private
// Service Connect) are skipped the same way.
//
// A 403 permission refusal on any call a class makes is wrapped into
// cloud.ErrPermissionDenied (classifyPermission), naming the
// permission — from the API's message, else the list permission the
// class needs — so the command degrades that one class to an
// explicit unavailable instead of failing the whole sweep.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	compute "google.golang.org/api/compute/v1"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// orphanComputeAPI is the §13 small client interface over the
// Compute API calls the sweep needs. Production is gceOrphanClient;
// tests replay recorded response fixtures.
type orphanComputeAPI interface {
	// ListDisks returns every disk in the project (flattened
	// aggregated list).
	ListDisks(ctx context.Context) ([]*compute.Disk, error)
	// ListForwardingRules returns every forwarding rule in the
	// project (flattened aggregated list).
	ListForwardingRules(ctx context.Context) ([]*compute.ForwardingRule, error)
	// ListAddresses returns every static address in the project,
	// regional and global (flattened aggregated list).
	ListAddresses(ctx context.Context) ([]*compute.Address, error)
	// GetBackendService fetches a backend service by its URL,
	// global or regional (the URL's project is honored: a URL map
	// may reference a service in another project).
	GetBackendService(ctx context.Context, service string) (*compute.BackendService, error)
	// GetProxyURLMap resolves a target HTTP(S) proxy to its URL map
	// URL; kind is "targetHttpProxies" or "targetHttpsProxies",
	// scope a region or "" for global.
	GetProxyURLMap(ctx context.Context, scope, kind, name string) (string, error)
	// GetURLMap fetches a URL map; scope is a region or "" global.
	GetURLMap(ctx context.Context, scope, name string) (*compute.UrlMap, error)
	// GetTargetPool fetches a legacy target pool.
	GetTargetPool(ctx context.Context, region, name string) (*compute.TargetPool, error)
	// GetBackendGroup fetches a backend group by its URL. Callers
	// pass only URLs backendGroupShape accepts, so every call has the
	// zone, region or global scope it needs.
	GetBackendGroup(ctx context.Context, group string) (backendGroup, error)
}

// backendGroup is what the verdict needs from a backend's group.
type backendGroup struct {
	// EndpointType is a NEG's networkEndpointType; empty for
	// instance groups.
	EndpointType string
	// Size is the NEG's endpoint count or the group's instance count.
	Size int64
}

// countedEndpointTypes are the NEG types whose size counts attached
// endpoints a load balancer routes to. SERVERLESS and
// PRIVATE_SERVICE_CONNECT are deliberately absent: their size is
// always 0 however live the service behind them.
var countedEndpointTypes = map[string]bool{
	"GCE_VM_IP_PORT":          true,
	"GCE_VM_IP":               true,
	"GCE_VM_IP_PORTMAP":       true,
	"NON_GCP_PRIVATE_IP_PORT": true,
	"INTERNET_IP_PORT":        true,
	"INTERNET_FQDN_PORT":      true,
}

// Backend group URL collections.
const (
	collectionNEG           = "networkEndpointGroups"
	collectionInstanceGroup = "instanceGroups"
)

// backendGroupShape reports a group URL's collection and whether it
// has a shape GetBackendGroup can fetch: a NEG (zonal, regional or
// global) or an instance group with a zone or a region (there are no
// global instance groups).
func backendGroupShape(group string) (collection string, ok bool) {
	switch {
	case strings.Contains(group, "/"+collectionNEG+"/"):
		return collectionNEG, true
	case strings.Contains(group, "/"+collectionInstanceGroup+"/"):
		scoped := resourceScopeValue(group, "zones") != "" || resourceScopeValue(group, "regions") != ""
		return collectionInstanceGroup, scoped
	default:
		return "", false
	}
}

// orphanAPI implements cloud.OrphanAPI.
type orphanAPI struct {
	gce orphanComputeAPI
}

func newOrphanAPI(p *Provider) *orphanAPI {
	return &orphanAPI{gce: newGCEOrphanClient(p.project)}
}

// OrphanDisks implements cloud.OrphanAPI: unattached READY disks,
// dated. Age filtering is the caller's policy (see cloud.OrphanAPI).
func (o *orphanAPI) OrphanDisks(ctx context.Context) ([]cloud.OrphanDisk, error) {
	disks, err := o.gce.ListDisks(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing disks: %w", classifyPermission(err, "compute.disks.list"))
	}
	var out []cloud.OrphanDisk
	for _, d := range disks {
		if d == nil || len(d.Users) > 0 || d.Status != "READY" {
			continue
		}
		out = append(out, cloud.OrphanDisk{
			Name:        d.Name,
			Zone:        resourceTail(d.Zone),
			SizeGB:      d.SizeGb,
			Type:        resourceTail(d.Type),
			UnusedSince: parseGCPTime(d.LastDetachTimestamp, d.CreationTimestamp),
		})
	}
	return out, nil
}

// OrphanAddresses implements cloud.OrphanAPI: reserved, unused,
// external static addresses, dated from their reservation. Age
// filtering is the caller's policy (see cloud.OrphanAPI).
func (o *orphanAPI) OrphanAddresses(ctx context.Context) ([]cloud.OrphanAddress, error) {
	addrs, err := o.gce.ListAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing addresses: %w", classifyPermission(err, "compute.addresses.list"))
	}
	var out []cloud.OrphanAddress
	for _, a := range addrs {
		if a == nil || a.Status != "RESERVED" || len(a.Users) > 0 {
			continue
		}
		if a.AddressType != "" && a.AddressType != "EXTERNAL" {
			continue // internal statics are not billed idle
		}
		region := "global"
		if a.Region != "" {
			region = resourceTail(a.Region)
		}
		out = append(out, cloud.OrphanAddress{
			Name:          a.Name,
			Region:        region,
			Address:       a.Address,
			Tier:          a.NetworkTier,
			ReservedSince: parseGCPTime(a.CreationTimestamp),
		})
	}
	return out, nil
}

// OrphanLoadBalancers implements cloud.OrphanAPI per the package
// comment's resolution rules.
func (o *orphanAPI) OrphanLoadBalancers(ctx context.Context) ([]cloud.OrphanLoadBalancer, error) {
	rules, err := o.gce.ListForwardingRules(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing forwarding rules: %w", classifyPermission(err, "compute.forwardingRules.list"))
	}
	var out []cloud.OrphanLoadBalancer
	for _, r := range rules {
		if r == nil {
			continue
		}
		orphaned, reason, err := o.judgeRule(ctx, r)
		if err != nil {
			return nil, fmt.Errorf("resolving forwarding rule %q: %w", r.Name, classifyPermission(err, ""))
		}
		if orphaned {
			out = append(out, cloud.OrphanLoadBalancer{
				Name:   r.Name,
				Region: ruleRegion(r),
				Reason: reason,
			})
		}
	}
	return out, nil
}

// judgeRule resolves one forwarding rule to a verdict; a rule it
// cannot judge is (false, "", nil) — skipped, not an error.
func (o *orphanAPI) judgeRule(ctx context.Context, r *compute.ForwardingRule) (orphaned bool, reason string, err error) {
	switch {
	case r.BackendService != "":
		return o.judgeBackendServices(ctx, []string{r.BackendService})

	case strings.Contains(r.Target, "/targetPools/"):
		pool, err := o.gce.GetTargetPool(ctx, resourceScopeValue(r.Target, "regions"), resourceTail(r.Target))
		if err != nil {
			return false, "", err
		}
		if len(pool.Instances) == 0 {
			return true, fmt.Sprintf("target pool %s has no instances", pool.Name), nil
		}
		return false, "", nil

	case strings.Contains(r.Target, "/targetHttpProxies/"), strings.Contains(r.Target, "/targetHttpsProxies/"):
		kind := "targetHttpProxies"
		if strings.Contains(r.Target, "/targetHttpsProxies/") {
			kind = "targetHttpsProxies"
		}
		scope := resourceScopeValue(r.Target, "regions")
		urlMap, err := o.gce.GetProxyURLMap(ctx, scope, kind, resourceTail(r.Target))
		if err != nil {
			return false, "", err
		}
		um, err := o.gce.GetURLMap(ctx, resourceScopeValue(urlMap, "regions"), resourceTail(urlMap))
		if err != nil {
			return false, "", err
		}
		services, elsewhere := urlMapRoutes(um)
		if elsewhere {
			// A bucket or a redirect serves without endpoints.
			return false, "", nil
		}
		if len(services) == 0 {
			return true, fmt.Sprintf("url map %s routes to no backend service", um.Name), nil
		}
		return o.judgeBackendServices(ctx, services)

	default:
		// Unjudgeable target kind: skipped, never guessed.
		return false, "", nil
	}
}

// judgeBackendServices sums endpoint counts across the services'
// judgeable backends: any endpoint → not orphaned; else any
// unjudgeable backend → skipped; else orphaned.
func (o *orphanAPI) judgeBackendServices(ctx context.Context, services []string) (bool, string, error) {
	total := int64(0)
	unjudged := false
	names := make([]string, 0, len(services))
	for _, s := range services {
		bs, err := o.gce.GetBackendService(ctx, s)
		if err != nil {
			return false, "", err
		}
		names = append(names, bs.Name)
		for _, b := range bs.Backends {
			if b == nil || b.Group == "" {
				continue
			}
			collection, ok := backendGroupShape(b.Group)
			if !ok {
				unjudged = true
				continue
			}
			g, err := o.gce.GetBackendGroup(ctx, b.Group)
			if err != nil {
				return false, "", err
			}
			if collection == collectionNEG && !countedEndpointTypes[g.EndpointType] {
				unjudged = true
				continue
			}
			total += g.Size
		}
	}
	if total > 0 || unjudged {
		return false, "", nil
	}
	return true, fmt.Sprintf("backend service %s has 0 endpoints across all groups", strings.Join(names, ",")), nil
}

// urlMapRoutes collects every backend service a URL map can route
// to — the default service and route action plus each path
// matcher's default, path rules and route rules — deduplicated and
// order-stable. elsewhere reports a destination that is not a
// backend service (a backend bucket or a URL redirect), for which
// the verdict has no endpoints to count.
func urlMapRoutes(um *compute.UrlMap) (services []string, elsewhere bool) {
	seen := map[string]bool{}
	add := func(s string) {
		switch {
		case s == "":
		case !strings.Contains(s, "/backendServices/"):
			elsewhere = true // a backendBuckets URL
		case !seen[s]:
			seen[s] = true
			services = append(services, s)
		}
	}
	route := func(service string, redirect *compute.HttpRedirectAction, action *compute.HttpRouteAction) {
		add(service)
		if redirect != nil {
			elsewhere = true
		}
		if action != nil {
			for _, w := range action.WeightedBackendServices {
				if w != nil {
					add(w.BackendService)
				}
			}
		}
	}
	route(um.DefaultService, um.DefaultUrlRedirect, um.DefaultRouteAction)
	for _, pm := range um.PathMatchers {
		if pm == nil {
			continue
		}
		route(pm.DefaultService, pm.DefaultUrlRedirect, pm.DefaultRouteAction)
		for _, pr := range pm.PathRules {
			if pr != nil {
				route(pr.Service, pr.UrlRedirect, pr.RouteAction)
			}
		}
		for _, rr := range pm.RouteRules {
			if rr != nil {
				route(rr.Service, rr.UrlRedirect, rr.RouteAction)
			}
		}
	}
	sort.Strings(services)
	return services, elsewhere
}

// ruleRegion is the rule's region short name, "global" for global
// rules.
func ruleRegion(r *compute.ForwardingRule) string {
	if r.Region == "" {
		return "global"
	}
	return resourceTail(r.Region)
}

// parseGCPTime parses the first non-empty RFC3339 timestamp; zero
// time when none parses (callers report "age unknown", never drop).
func parseGCPTime(candidates ...string) time.Time {
	for _, c := range candidates {
		if c == "" {
			continue
		}
		if t, err := time.Parse(time.RFC3339, c); err == nil {
			return t
		}
	}
	return time.Time{}
}

// gceOrphanClient is the production orphanComputeAPI over the
// Compute REST API.
type gceOrphanClient struct {
	project string
	svc     func(ctx context.Context) (*compute.Service, error)
}

func newGCEOrphanClient(project string) *gceOrphanClient {
	return &gceOrphanClient{
		project: project,
		svc:     lazyClient(func(ctx context.Context) (*compute.Service, error) { return compute.NewService(ctx) }),
	}
}

func (c *gceOrphanClient) ListDisks(ctx context.Context) ([]*compute.Disk, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	var out []*compute.Disk
	err = svc.Disks.AggregatedList(c.project).Pages(ctx, func(page *compute.DiskAggregatedList) error {
		out = append(out, flattenDiskAggregate(page)...)
		return nil
	})
	return out, err
}

func (c *gceOrphanClient) ListForwardingRules(ctx context.Context) ([]*compute.ForwardingRule, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	var out []*compute.ForwardingRule
	err = svc.ForwardingRules.AggregatedList(c.project).Pages(ctx, func(page *compute.ForwardingRuleAggregatedList) error {
		out = append(out, flattenRuleAggregate(page)...)
		return nil
	})
	return out, err
}

func (c *gceOrphanClient) ListAddresses(ctx context.Context) ([]*compute.Address, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	var out []*compute.Address
	err = svc.Addresses.AggregatedList(c.project).Pages(ctx, func(page *compute.AddressAggregatedList) error {
		out = append(out, flattenAddressAggregate(page)...)
		return nil
	})
	return out, err
}

func flattenAddressAggregate(page *compute.AddressAggregatedList) []*compute.Address {
	var out []*compute.Address
	for _, scope := range sortedKeys(page.Items) {
		out = append(out, page.Items[scope].Addresses...)
	}
	return out
}

// flattenDiskAggregate / flattenRuleAggregate turn one aggregated
// page into a flat, scope-key-ordered slice. Shared by the
// production paging above and the fixture tests, so the recorded
// wire shape exercises exactly the flattening production runs.
func flattenDiskAggregate(page *compute.DiskAggregatedList) []*compute.Disk {
	var out []*compute.Disk
	for _, scope := range sortedKeys(page.Items) {
		out = append(out, page.Items[scope].Disks...)
	}
	return out
}

func flattenRuleAggregate(page *compute.ForwardingRuleAggregatedList) []*compute.ForwardingRule {
	var out []*compute.ForwardingRule
	for _, scope := range sortedKeys(page.Items) {
		out = append(out, page.Items[scope].ForwardingRules...)
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (c *gceOrphanClient) GetBackendService(ctx context.Context, service string) (*compute.BackendService, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	project, name := c.projectOf(service), resourceTail(service)
	if region := resourceScopeValue(service, "regions"); region != "" {
		return svc.RegionBackendServices.Get(project, region, name).Context(ctx).Do()
	}
	return svc.BackendServices.Get(project, name).Context(ctx).Do()
}

// projectOf is the project a resource URL names, else the
// provider's own.
func (c *gceOrphanClient) projectOf(u string) string {
	if p := resourceScopeValue(u, "projects"); p != "" {
		return p
	}
	return c.project
}

func (c *gceOrphanClient) GetProxyURLMap(ctx context.Context, scope, kind, name string) (string, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return "", err
	}
	switch {
	case kind == "targetHttpProxies" && scope == "":
		p, err := svc.TargetHttpProxies.Get(c.project, name).Context(ctx).Do()
		if err != nil {
			return "", err
		}
		return p.UrlMap, nil
	case kind == "targetHttpProxies":
		p, err := svc.RegionTargetHttpProxies.Get(c.project, scope, name).Context(ctx).Do()
		if err != nil {
			return "", err
		}
		return p.UrlMap, nil
	case kind == "targetHttpsProxies" && scope == "":
		p, err := svc.TargetHttpsProxies.Get(c.project, name).Context(ctx).Do()
		if err != nil {
			return "", err
		}
		return p.UrlMap, nil
	default:
		p, err := svc.RegionTargetHttpsProxies.Get(c.project, scope, name).Context(ctx).Do()
		if err != nil {
			return "", err
		}
		return p.UrlMap, nil
	}
}

func (c *gceOrphanClient) GetURLMap(ctx context.Context, scope, name string) (*compute.UrlMap, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	if scope == "" {
		return svc.UrlMaps.Get(c.project, name).Context(ctx).Do()
	}
	return svc.RegionUrlMaps.Get(c.project, scope, name).Context(ctx).Do()
}

func (c *gceOrphanClient) GetTargetPool(ctx context.Context, region, name string) (*compute.TargetPool, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	return svc.TargetPools.Get(c.project, region, name).Context(ctx).Do()
}

func (c *gceOrphanClient) GetBackendGroup(ctx context.Context, group string) (backendGroup, error) {
	collection, ok := backendGroupShape(group)
	if !ok {
		// Never reached through judgeBackendServices; a guard so no
		// caller can turn a missing scope into zone="".
		return backendGroup{}, fmt.Errorf("unrecognized backend group: %s", group)
	}
	svc, err := c.svc(ctx)
	if err != nil {
		return backendGroup{}, err
	}
	project, name := c.projectOf(group), resourceTail(group)
	zone, region := resourceScopeValue(group, "zones"), resourceScopeValue(group, "regions")
	if collection == collectionNEG {
		var neg *compute.NetworkEndpointGroup
		switch {
		case zone != "":
			neg, err = svc.NetworkEndpointGroups.Get(project, zone, name).Context(ctx).Do()
		case region != "":
			neg, err = svc.RegionNetworkEndpointGroups.Get(project, region, name).Context(ctx).Do()
		default:
			neg, err = svc.GlobalNetworkEndpointGroups.Get(project, name).Context(ctx).Do()
		}
		if err != nil {
			return backendGroup{}, err
		}
		return backendGroup{EndpointType: neg.NetworkEndpointType, Size: neg.Size}, nil
	}
	var ig *compute.InstanceGroup
	if zone != "" {
		ig, err = svc.InstanceGroups.Get(project, zone, name).Context(ctx).Do()
	} else {
		ig, err = svc.RegionInstanceGroups.Get(project, region, name).Context(ctx).Do()
	}
	if err != nil {
		return backendGroup{}, err
	}
	return backendGroup{Size: ig.Size}, nil
}
