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

// Scope discovery (cloud.ScopedFleet, docs/mcp-multi-tenant-design.md
// "Many projects"): clusters in other projects, in a GKE fleet, or under
// a folder or organization.
//
// Every scope ends the same way: a set of projects, and the Container
// API's clusters.list in each, because that is the one call that returns
// a cluster's DNS endpoint. The scopes differ only in how they find the
// projects:
//
//   - fleet: the fleet host project's Hub memberships, each GKE member
//     naming its cluster's project, location and name;
//   - folder / organization: Cloud Asset Inventory, one search for every
//     cluster under the parent. When that cannot answer (the API is not
//     enabled, or the identity may not search), a Resource Manager walk
//     of the folder tree instead: slower, needs nothing extra, and
//     reported as a fallback.
//
// All of it runs as the identity in the context (clientOptions), so for
// a caller-token invocation a scope yields only what the caller can see.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"

	cloudasset "google.golang.org/api/cloudasset/v1"
	crm "google.golang.org/api/cloudresourcemanager/v3"
	gkehub "google.golang.org/api/gkehub/v1"
	"google.golang.org/api/googleapi"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// The GKE provider implements the optional cloud.ScopedFleet surface.
var _ cloud.ScopedFleet = (*Provider)(nil)

// How a scope's clusters were found (cloud.Discovery.Via).
const (
	viaContainer       = "container"
	viaHub             = "hub"
	viaAsset           = "asset"
	viaResourceManager = "resourcemanager"
)

// Why part of a scope yielded nothing (cloud.Discovery.Skipped keys).
const (
	skipNotGKE        = "fleet members that are not GKE clusters"
	skipClusterGone   = "fleet members whose cluster no longer exists"
	skipAPIDisabled   = "projects with the Kubernetes Engine API off"
	skipNoAccess      = "projects whose clusters cannot be listed"
	skipListFailed    = "projects whose cluster list failed"
	skipFolderNoRead  = "folders that cannot be listed"
	clusterAssetType  = "container.googleapis.com/Cluster"
	listConcurrency   = 8
	maxFallbackReason = 200
)

// hubLister, assetSearcher and projectWalker are the §13 small client
// interfaces over the calls scope discovery needs; production adapters
// below, tests replay fixtures.
type hubLister interface {
	ListMemberships(ctx context.Context, parent string) ([]*gkehub.Membership, error)
}

type assetSearcher interface {
	// SearchClusters returns the full resource names of the clusters
	// under scope ("folders/N" or "organizations/N").
	SearchClusters(ctx context.Context, scope string) ([]string, error)
}

type projectWalker interface {
	// ListProjects returns the IDs of the active projects directly under
	// parent; ListFolders the names ("folders/N") of its active folders.
	ListProjects(ctx context.Context, parent string) ([]string, error)
	ListFolders(ctx context.Context, parent string) ([]string, error)
}

type scopeDeps struct {
	clusters fleetLister
	hub      hubLister
	asset    assetSearcher
	walker   projectWalker
}

// Discover implements cloud.ScopedFleet.
func (p *Provider) Discover(ctx context.Context, s cloud.Scope) (cloud.Discovery, error) {
	return discoverScope(ctx, s, scopeDeps{
		clusters: newFleetLister(),
		hub:      newHubLister(),
		asset:    newAssetSearcher(),
		walker:   newProjectWalker(),
	})
}

// discoverScope is the testable core of Discover.
func discoverScope(ctx context.Context, s cloud.Scope, d scopeDeps) (cloud.Discovery, error) {
	switch s.Kind {
	case cloud.ScopeProject:
		refs, err := discoverClusters(ctx, s.Project, s.Location, d.clusters)
		return cloud.Discovery{Clusters: refs, Via: viaContainer}, err
	case cloud.ScopeFleet:
		return discoverFleetMembers(ctx, s.Project, d)
	case cloud.ScopeFolder, cloud.ScopeOrganization:
		return discoverUnder(ctx, s, d)
	default:
		return cloud.Discovery{}, fmt.Errorf("unknown discovery scope kind %q", s.Kind)
	}
}

// discoverFleetMembers lists a fleet's GKE members. A membership names
// its cluster by resource link; the clusters' DNS endpoints come from
// clusters.list in each member project, filtered to the members.
func discoverFleetMembers(ctx context.Context, host string, d scopeDeps) (cloud.Discovery, error) {
	disc := cloud.Discovery{Via: viaHub}
	if host == "" {
		return disc, errors.New("fleet discovery: no fleet host project")
	}
	members, err := d.hub.ListMemberships(ctx, "projects/"+host+"/locations/-")
	if err != nil {
		return disc, fmt.Errorf("listing the memberships of fleet %s: %w", host, classifyPermission(err, "gkehub.memberships.list"))
	}
	want := map[string]bool{} // "project/location/name"
	var projects []string
	for _, m := range members {
		if m == nil || m.Endpoint == nil || m.Endpoint.GkeCluster == nil {
			skip(&disc, skipNotGKE)
			continue
		}
		if m.Endpoint.GkeCluster.ClusterMissing {
			skip(&disc, skipClusterGone)
			continue
		}
		project, location, name, ok := parseClusterName(m.Endpoint.GkeCluster.ResourceLink)
		if !ok {
			skip(&disc, skipNotGKE)
			continue
		}
		want[project+"/"+location+"/"+name] = true
		if !slices.Contains(projects, project) {
			projects = append(projects, project)
		}
	}
	refs, err := listInProjects(ctx, projects, d.clusters, &disc)
	disc.Clusters = slices.DeleteFunc(refs, func(r cloud.ClusterRef) bool {
		return !want[r.Project+"/"+r.Location+"/"+r.Name]
	})
	return disc, err
}

// discoverUnder lists every cluster under a folder or organization:
// Cloud Asset Inventory for the projects that hold clusters, else a
// Resource Manager walk for every project, then clusters.list in each.
func discoverUnder(ctx context.Context, s cloud.Scope, d scopeDeps) (cloud.Discovery, error) {
	disc := cloud.Discovery{Via: viaAsset}
	if s.ID == "" {
		return disc, fmt.Errorf("%s discovery: no %s ID", s.Kind, s.Kind)
	}
	parent := s.String()
	var projects []string
	if s.Search == cloud.SearchResourceManager {
		// The operator turned Cloud Asset Inventory off for this endpoint.
		disc.Via = viaResourceManager
		projects, err := walkProjects(ctx, parent, d.walker, &disc)
		if err != nil {
			return disc, fmt.Errorf("listing the projects under %s: %w", parent, classifyPermission(err, "resourcemanager.projects.list"))
		}
		refs, err := listInProjects(ctx, projects, d.clusters, &disc)
		disc.Clusters = refs
		return disc, err
	}
	names, err := d.asset.SearchClusters(ctx, parent)
	switch {
	case err == nil:
		for _, n := range names {
			if project, _, _, ok := parseClusterName(n); ok && !slices.Contains(projects, project) {
				projects = append(projects, project)
			}
		}
	case assetUnavailable(err) && s.Search != cloud.SearchAsset:
		disc.Via = viaResourceManager
		disc.Fallback = "Cloud Asset Inventory could not answer: " + shortReason(err)
		projects, err = walkProjects(ctx, parent, d.walker, &disc)
		if err != nil {
			return disc, fmt.Errorf("listing the projects under %s: %w", parent, classifyPermission(err, "resourcemanager.projects.list"))
		}
	default:
		return disc, fmt.Errorf("searching %s for clusters: %w", parent, err)
	}
	refs, err := listInProjects(ctx, projects, d.clusters, &disc)
	disc.Clusters = refs
	return disc, err
}

// walkProjects lists the active projects under parent at any depth.
// The parent itself must be listable; a subfolder that is not is
// counted and skipped, so one locked-down folder does not cost the
// rest of the tree.
func walkProjects(ctx context.Context, parent string, w projectWalker, disc *cloud.Discovery) ([]string, error) {
	var projects []string
	queue := []string{parent}
	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]
		ids, err := w.ListProjects(ctx, next)
		if err != nil {
			if next == parent || ctx.Err() != nil {
				return nil, err
			}
			skip(disc, skipFolderNoRead)
			continue
		}
		projects = append(projects, ids...)
		folders, err := w.ListFolders(ctx, next)
		if err != nil {
			if next == parent || ctx.Err() != nil {
				return nil, err
			}
			skip(disc, skipFolderNoRead)
			continue
		}
		queue = append(queue, folders...)
	}
	return projects, nil
}

// listInProjects runs clusters.list in each project, a few at a time.
// A project whose API is off, or whose clusters the identity may not
// list, is counted and skipped: under a folder that is most projects.
// Any other failure is counted too, and is the error returned when no
// project yielded anything.
func listInProjects(ctx context.Context, projects []string, lister fleetLister, disc *cloud.Discovery) ([]cloud.ClusterRef, error) {
	var (
		mu       sync.Mutex
		refs     []cloud.ClusterRef
		firstErr error
		wg       sync.WaitGroup
		slots    = make(chan struct{}, listConcurrency)
	)
	for _, project := range projects {
		wg.Add(1)
		slots <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			got, err := discoverClusters(ctx, project, "", lister)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				refs = append(refs, got...)
			case apiDisabled(err):
				skip(disc, skipAPIDisabled)
			case isStatus(err, http.StatusForbidden), isStatus(err, http.StatusNotFound):
				skip(disc, skipNoAccess)
			default:
				skip(disc, skipListFailed)
				if firstErr == nil {
					firstErr = err
				}
			}
		}()
	}
	wg.Wait()
	slices.SortFunc(refs, func(a, b cloud.ClusterRef) int {
		return strings.Compare(a.Project+"/"+a.Location+"/"+a.Name, b.Project+"/"+b.Location+"/"+b.Name)
	})
	if len(refs) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return refs, nil
}

// parseClusterName splits a GKE cluster's full resource name, as Cloud
// Asset Inventory and Hub memberships spell it:
// //container.googleapis.com/projects/P/locations/L/clusters/N (older
// zonal names use zones/ for locations/).
func parseClusterName(full string) (project, location, name string, ok bool) {
	rest, found := strings.CutPrefix(full, "//container.googleapis.com/")
	if !found {
		return "", "", "", false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 6 || parts[0] != "projects" || (parts[2] != "locations" && parts[2] != "zones") || parts[4] != "clusters" {
		return "", "", "", false
	}
	if parts[1] == "" || parts[3] == "" || parts[5] == "" {
		return "", "", "", false
	}
	return parts[1], parts[3], parts[5], true
}

func skip(disc *cloud.Discovery, reason string) {
	if disc.Skipped == nil {
		disc.Skipped = map[string]int{}
	}
	disc.Skipped[reason]++
}

func isStatus(err error, code int) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && gerr.Code == code
}

// apiDisabled reports a 403 that means the API is off in the project,
// not a missing grant.
func apiDisabled(err error) bool {
	var gerr *googleapi.Error
	if !errors.As(err, &gerr) || gerr.Code != http.StatusForbidden {
		return false
	}
	for _, item := range gerr.Errors {
		if item.Reason == "accessNotConfigured" || item.Reason == "SERVICE_DISABLED" {
			return true
		}
	}
	return strings.Contains(gerr.Message, "has not been used") || strings.Contains(gerr.Message, "is disabled")
}

// assetUnavailable reports a Cloud Asset Inventory refusal the
// Resource Manager walk can stand in for: the API off, or no search
// permission. Anything else (a network failure, a bad scope) is not
// papered over.
func assetUnavailable(err error) bool {
	return isStatus(err, http.StatusForbidden)
}

// shortReason is an API error's message, first line, capped: enough to
// say why, short enough for a summary note.
func shortReason(err error) string {
	msg := err.Error()
	var gerr *googleapi.Error
	if errors.As(err, &gerr) && gerr.Message != "" {
		msg = gerr.Message
	}
	msg, _, _ = strings.Cut(msg, "\n")
	if len(msg) > maxFallbackReason {
		msg = msg[:maxFallbackReason] + "…"
	}
	return msg
}

// gkeHubLister is the production hubLister.
type gkeHubLister struct {
	svc func(ctx context.Context) (*gkehub.Service, error)
}

func newHubLister() *gkeHubLister {
	return &gkeHubLister{svc: lazyClient(func(ctx context.Context) (*gkehub.Service, error) {
		return gkehub.NewService(ctx, clientOptions(ctx)...)
	})}
}

func (l *gkeHubLister) ListMemberships(ctx context.Context, parent string) ([]*gkehub.Membership, error) {
	svc, err := l.svc(ctx)
	if err != nil {
		return nil, err
	}
	var out []*gkehub.Membership
	err = svc.Projects.Locations.Memberships.List(parent).Pages(ctx, func(r *gkehub.ListMembershipsResponse) error {
		out = append(out, r.Resources...)
		return nil
	})
	return out, err
}

// caiSearcher is the production assetSearcher.
type caiSearcher struct {
	svc func(ctx context.Context) (*cloudasset.Service, error)
}

func newAssetSearcher() *caiSearcher {
	return &caiSearcher{svc: lazyClient(func(ctx context.Context) (*cloudasset.Service, error) {
		return cloudasset.NewService(ctx, clientOptions(ctx)...)
	})}
}

func (s *caiSearcher) SearchClusters(ctx context.Context, scope string) ([]string, error) {
	svc, err := s.svc(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	err = svc.V1.SearchAllResources(scope).AssetTypes(clusterAssetType).Pages(ctx, func(r *cloudasset.SearchAllResourcesResponse) error {
		for _, res := range r.Results {
			if res != nil {
				out = append(out, res.Name)
			}
		}
		return nil
	})
	return out, err
}

// crmWalker is the production projectWalker.
type crmWalker struct {
	svc func(ctx context.Context) (*crm.Service, error)
}

func newProjectWalker() *crmWalker {
	return &crmWalker{svc: lazyClient(func(ctx context.Context) (*crm.Service, error) {
		return crm.NewService(ctx, clientOptions(ctx)...)
	})}
}

func (w *crmWalker) ListProjects(ctx context.Context, parent string) ([]string, error) {
	svc, err := w.svc(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	err = svc.Projects.List().Parent(parent).Pages(ctx, func(r *crm.ListProjectsResponse) error {
		for _, p := range r.Projects {
			if p != nil && p.State == "ACTIVE" {
				out = append(out, p.ProjectId)
			}
		}
		return nil
	})
	return out, err
}

func (w *crmWalker) ListFolders(ctx context.Context, parent string) ([]string, error) {
	svc, err := w.svc(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	err = svc.Folders.List().Parent(parent).Pages(ctx, func(r *crm.ListFoldersResponse) error {
		for _, f := range r.Folders {
			if f != nil && f.State == "ACTIVE" {
				out = append(out, f.Name)
			}
		}
		return nil
	})
	return out, err
}
