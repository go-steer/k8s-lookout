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
// Load balancers: a forwarding rule is orphaned when every backend
// it routes to resolves to zero endpoints. Resolution follows the
// rule's shape:
//
//   - backendService rules (L4 ILB / L7-ILB via backend service):
//     the backend service's NEG/instance-group sizes;
//   - target-proxy rules (GKE Ingress: HTTP/HTTPS proxy → URL map):
//     every backend service the URL map can route to;
//   - targetPool rules (legacy network LB): the pool's instance list.
//
// The judgment counts endpoint PRESENCE, not health: a rule whose
// NEGs have endpoints is not orphaned even if they are all
// unhealthy — that is an outage (state edges / triage territory),
// not an orphan. Rules with target kinds outside the three shapes
// above (SSL/TCP proxies, VPN/IPsec) are skipped, not guessed at.

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
	// GetBackendService fetches a backend service; scope is a
	// region name or "" for global.
	GetBackendService(ctx context.Context, scope, name string) (*compute.BackendService, error)
	// GetProxyURLMap resolves a target HTTP(S) proxy to its URL map
	// URL; kind is "targetHttpProxies" or "targetHttpsProxies",
	// scope a region or "" for global.
	GetProxyURLMap(ctx context.Context, scope, kind, name string) (string, error)
	// GetURLMap fetches a URL map; scope is a region or "" global.
	GetURLMap(ctx context.Context, scope, name string) (*compute.UrlMap, error)
	// GetTargetPool fetches a legacy target pool.
	GetTargetPool(ctx context.Context, region, name string) (*compute.TargetPool, error)
	// GroupSize returns the endpoint/instance count of a backend
	// group URL (network endpoint group or instance group).
	GroupSize(ctx context.Context, group string) (int64, error)
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
		return nil, fmt.Errorf("listing disks: %w", err)
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

// OrphanLoadBalancers implements cloud.OrphanAPI per the package
// comment's resolution rules.
func (o *orphanAPI) OrphanLoadBalancers(ctx context.Context) ([]cloud.OrphanLoadBalancer, error) {
	rules, err := o.gce.ListForwardingRules(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing forwarding rules: %w", err)
	}
	var out []cloud.OrphanLoadBalancer
	for _, r := range rules {
		if r == nil {
			continue
		}
		orphaned, reason, err := o.judgeRule(ctx, r)
		if err != nil {
			return nil, fmt.Errorf("resolving forwarding rule %q: %w", r.Name, err)
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

// judgeRule resolves one forwarding rule to its endpoint count.
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
		services := urlMapServices(um)
		if len(services) == 0 {
			return true, fmt.Sprintf("url map %s routes to no backend service", um.Name), nil
		}
		return o.judgeBackendServices(ctx, services)

	default:
		// Unjudgeable target kind: skipped, never guessed.
		return false, "", nil
	}
}

// judgeBackendServices sums endpoint counts across services; zero
// across the board is an orphan.
func (o *orphanAPI) judgeBackendServices(ctx context.Context, services []string) (bool, string, error) {
	total := int64(0)
	names := make([]string, 0, len(services))
	for _, s := range services {
		bs, err := o.gce.GetBackendService(ctx, resourceScopeValue(s, "regions"), resourceTail(s))
		if err != nil {
			return false, "", err
		}
		names = append(names, bs.Name)
		for _, b := range bs.Backends {
			if b == nil || b.Group == "" {
				continue
			}
			n, err := o.gce.GroupSize(ctx, b.Group)
			if err != nil {
				return false, "", err
			}
			total += n
		}
	}
	if total > 0 {
		return false, "", nil
	}
	return true, fmt.Sprintf("backend service %s has 0 endpoints across all groups", strings.Join(names, ",")), nil
}

// urlMapServices collects every backend service a URL map can route
// to: the default service plus each path matcher's default and path
// rules, deduplicated, order-stable.
func urlMapServices(um *compute.UrlMap) []string {
	seen := map[string]bool{}
	var out []string
	add := func(s string) {
		if s != "" && strings.Contains(s, "/backendServices/") && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	add(um.DefaultService)
	for _, pm := range um.PathMatchers {
		if pm == nil {
			continue
		}
		add(pm.DefaultService)
		for _, pr := range pm.PathRules {
			if pr != nil {
				add(pr.Service)
			}
		}
	}
	sort.Strings(out)
	return out
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

func (c *gceOrphanClient) GetBackendService(ctx context.Context, scope, name string) (*compute.BackendService, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	if scope == "" {
		return svc.BackendServices.Get(c.project, name).Context(ctx).Do()
	}
	return svc.RegionBackendServices.Get(c.project, scope, name).Context(ctx).Do()
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

func (c *gceOrphanClient) GroupSize(ctx context.Context, group string) (int64, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return 0, err
	}
	name := resourceTail(group)
	switch {
	case strings.Contains(group, "/networkEndpointGroups/"):
		if zone := resourceScopeValue(group, "zones"); zone != "" {
			neg, err := svc.NetworkEndpointGroups.Get(c.project, zone, name).Context(ctx).Do()
			if err != nil {
				return 0, err
			}
			return neg.Size, nil
		}
		if region := resourceScopeValue(group, "regions"); region != "" {
			neg, err := svc.RegionNetworkEndpointGroups.Get(c.project, region, name).Context(ctx).Do()
			if err != nil {
				return 0, err
			}
			return neg.Size, nil
		}
		neg, err := svc.GlobalNetworkEndpointGroups.Get(c.project, name).Context(ctx).Do()
		if err != nil {
			return 0, err
		}
		return neg.Size, nil
	case strings.Contains(group, "/instanceGroups/"):
		ig, err := svc.InstanceGroups.Get(c.project, resourceScopeValue(group, "zones"), name).Context(ctx).Do()
		if err != nil {
			return 0, err
		}
		return ig.Size, nil
	default:
		return 0, fmt.Errorf("unrecognized backend group kind: %s", group)
	}
}
