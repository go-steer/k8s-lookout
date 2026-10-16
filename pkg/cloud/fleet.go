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

package cloud

import (
	"context"

	"k8s.io/client-go/rest"
)

// ClusterRef identifies one Kubernetes cluster a Fleet provider can
// connect to without a kubeconfig. Name is the human-readable cluster
// name the sentinel stamps as Signal.Cluster; Endpoint is the
// control-plane host the rest.Config dials (GKE: the *.gke.goog
// DNS endpoint).
//
// Location and Region mirror the Identity pair for a cluster the
// process does NOT run in: Location is the provider's single-string
// spelling (a zone for zonal clusters, a region for regional ones) and
// Region is always the region. A provider must set both; the sentinel
// reads the cluster's zone off the pair without parsing either
// (Location when it differs from Region, empty when it does not).
type ClusterRef struct {
	Name     string
	Project  string
	Location string
	Region   string
	Endpoint string
}

// Fleet is the OPTIONAL provider surface for kubeconfig-free
// multi-cluster bootstrap (docs/multi-cluster-design.md). It mirrors
// the Identity pattern: a surface the sentinel type-asserts on the
// Provider, NOT a capability in the Metrics()/Quota() matrix — this is
// deployment bootstrap, resolved once at startup, not a per-signal
// facet. A provider that cannot mint credentials without a kubeconfig
// simply does not implement it, and the sentinel falls back to
// kube.Options (in-cluster / kubeconfig).
//
// The GKE implementation (pkg/cloud/gke, behind the gke build tag)
// discovers clusters via the Container API and mints an authenticated
// rest.Config from Application Default Credentials over each cluster's
// control-plane DNS endpoint — one identity for every cluster, no CA
// cert to pin.
type Fleet interface {
	// DiscoverClusters lists the clusters in the provider's configured
	// scope (GKE: projects/locations). An empty result is not an error
	// — it means nothing matched — but a missing project identity or a
	// failed API call is (§2 fail-loudly).
	DiscoverClusters(ctx context.Context) ([]ClusterRef, error)

	// RESTConfig mints an authenticated *rest.Config for one cluster.
	// The credential is the provider's own ADC identity; per-cluster
	// authorization is RBAC, bound to that identity in the target
	// cluster (docs/multi-cluster-design.md: authN ≠ authZ).
	RESTConfig(ctx context.Context, ref ClusterRef) (*rest.Config, error)
}

// ScopeKind is what a discovery Scope names.
type ScopeKind string

// The discovery scopes (docs/mcp-multi-tenant-design.md, "Many projects").
const (
	// ScopeProject is one project's clusters, in one location or all.
	ScopeProject ScopeKind = "project"
	// ScopeFleet is a fleet's member clusters, which can span projects
	// (GKE: the Hub memberships of the fleet host project).
	ScopeFleet ScopeKind = "fleet"
	// ScopeFolder is every cluster under a folder, at any depth.
	ScopeFolder ScopeKind = "folder"
	// ScopeOrganization is every cluster under an organization.
	ScopeOrganization ScopeKind = "organization"
)

// Scope is one place to discover clusters in.
type Scope struct {
	Kind ScopeKind
	// Project is the project of a ScopeProject, or the host project of
	// a ScopeFleet.
	Project string
	// Location narrows a ScopeProject to one location; empty is all.
	Location string
	// ID is the folder or organization number.
	ID string
	// Search is how a folder or organization scope is searched: "" or
	// SearchAuto (Cloud Asset Inventory, else a Resource Manager walk),
	// SearchAsset (Cloud Asset Inventory only) or SearchResourceManager
	// (never Cloud Asset Inventory). Ignored for other scopes.
	Search string
}

// The ways a folder or organization scope can be searched.
const (
	SearchAuto            = "auto"
	SearchAsset           = "asset"
	SearchResourceManager = "resourcemanager"
)

// String is the scope as it is spelled on --clusters-from.
func (s Scope) String() string {
	switch s.Kind {
	case ScopeFleet:
		return "fleet:" + s.Project
	case ScopeFolder:
		return "folders/" + s.ID
	case ScopeOrganization:
		return "organizations/" + s.ID
	default:
		if s.Location != "" {
			return s.Project + "/" + s.Location
		}
		return s.Project
	}
}

// Discovery is what one Scope yielded, and how it was found.
type Discovery struct {
	Clusters []ClusterRef
	// Via names the API the clusters were found through, for the
	// operator and the caller to see (GKE: "container", "hub", "asset",
	// "resourcemanager").
	Via string
	// Fallback says why the preferred API was not used, when it was not;
	// empty otherwise. A fallback is never silent.
	Fallback string
	// Skipped counts what the scope held but could not yield, by reason
	// (a fleet's non-GKE member, a project with the cluster API off, a
	// project the identity may not list clusters in).
	Skipped map[string]int
}

// ScopedFleet is the OPTIONAL Fleet surface for discovery beyond the
// provider's own project: other projects, fleets, folders and
// organizations. Discovery and credentials both run as the identity in
// the context, so for an invocation running as its caller
// (kube.WithCallerToken) a scope yields only what that caller can see.
type ScopedFleet interface {
	Fleet
	// Discover lists the clusters in one scope. A scope the identity may
	// not read at all is an error; parts of a scope it may not read are
	// counted in Skipped.
	Discover(ctx context.Context, s Scope) (Discovery, error)
}
