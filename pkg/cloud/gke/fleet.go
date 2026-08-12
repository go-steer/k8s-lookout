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

// Fleet implementation (docs/multi-cluster-design.md): kubeconfig-free
// multi-cluster bootstrap for GKE. DiscoverClusters lists clusters via
// the Container API; RESTConfig mints an authenticated *rest.Config
// from Application Default Credentials over a cluster's control-plane
// DNS endpoint (*.gke.goog).
//
// Why the DNS endpoint (and why we require it): its TLS is a
// publicly-trusted cert, so there is no per-cluster CA to fetch or pin;
// and it authorizes by IAM, not the authorized-networks IP allowlist,
// so one ADC identity reaches every cluster the sentinel is granted.
// Authentication is solved here; per-cluster *authorization* is RBAC,
// bound to this identity in each target cluster.
//
// Same SDK choice as the rest of this package: the
// google.golang.org/api container discovery client (ipspace.go uses it
// for clusters.get), whose JSON wire shapes are the recorded-fixture
// format. The one credential detail unique to this file is the raw
// oauth2 token source (golang.org/x/oauth2/google) the k8s rest.Config
// needs — the google.golang.org/api clients wrap the same ADC source
// internally.

import (
	"context"
	"fmt"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	container "google.golang.org/api/container/v1"
	"k8s.io/client-go/rest"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// cloudPlatformScope is the OAuth scope GKE control planes accept for
// authentication; the same scope gke-gcloud-auth-plugin requests.
const cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

// allLocations is the wildcard Container API location, listing clusters
// across every region/zone in the project.
const allLocations = "-"

// tokenSourceFunc is the ADC token-source constructor, indirected for
// tests (production: google.DefaultTokenSource, which reaches the
// credential chain / metadata server).
type tokenSourceFunc func(ctx context.Context, scope ...string) (oauth2.TokenSource, error)

// fleetLister is the §13 small client interface over the one Container
// API call discovery needs (clusters.list); production adapter below,
// tests replay recorded ListClustersResponse fixtures.
type fleetLister interface {
	ListClusters(ctx context.Context, parent string) ([]*container.Cluster, error)
}

// The GKE provider implements the optional cloud.Fleet surface.
var _ cloud.Fleet = (*Provider)(nil)

// DiscoverClusters implements cloud.Fleet: every cluster in the
// provider's project (all locations when none is pinned).
func (p *Provider) DiscoverClusters(ctx context.Context) ([]cloud.ClusterRef, error) {
	return discoverClusters(ctx, p.project, p.location, newFleetLister())
}

// RESTConfig implements cloud.Fleet: an ADC-authenticated config for
// the cluster's DNS endpoint.
func (p *Provider) RESTConfig(ctx context.Context, ref cloud.ClusterRef) (*rest.Config, error) {
	return restConfig(ctx, ref, google.DefaultTokenSource)
}

// discoverClusters is the testable core of DiscoverClusters. A missing
// project is a §2 fail-loudly error, not an empty result: the operator
// asked for discovery and we cannot name the parent.
func discoverClusters(ctx context.Context, project, location string, lister fleetLister) ([]cloud.ClusterRef, error) {
	if project == "" {
		return nil, fmt.Errorf("fleet discovery: %s", reasonNoProject)
	}
	loc := location
	if loc == "" {
		loc = allLocations
	}
	parent := fmt.Sprintf("projects/%s/locations/%s", project, loc)
	clusters, err := lister.ListClusters(ctx, parent)
	if err != nil {
		return nil, fmt.Errorf("listing clusters under %s: %w", parent, err)
	}
	out := make([]cloud.ClusterRef, 0, len(clusters))
	for _, c := range clusters {
		if c == nil {
			continue
		}
		out = append(out, cloud.ClusterRef{
			Name:     c.Name,
			Project:  project,
			Location: c.Location,
			Endpoint: dnsEndpoint(c),
		})
	}
	return out, nil
}

// dnsEndpoint pulls the control-plane DNS endpoint host from a cluster
// record, or "" when the cluster has no DNS endpoint enabled (RESTConfig
// then fails loudly for that ref).
func dnsEndpoint(c *container.Cluster) string {
	cpe := c.ControlPlaneEndpointsConfig
	if cpe == nil || cpe.DnsEndpointConfig == nil {
		return ""
	}
	return cpe.DnsEndpointConfig.Endpoint
}

// restConfig is the testable core of RESTConfig: an ADC bearer token,
// auto-refreshing, over the cluster's public-cert DNS endpoint. No
// CAData — the *.gke.goog endpoint presents a publicly-trusted
// certificate.
func restConfig(ctx context.Context, ref cloud.ClusterRef, newTS tokenSourceFunc) (*rest.Config, error) {
	if ref.Endpoint == "" {
		return nil, fmt.Errorf("cluster %q has no control-plane DNS endpoint (enable it, or this cluster is unreachable without a kubeconfig)", ref.Name)
	}
	ts, err := newTS(ctx, cloudPlatformScope)
	if err != nil {
		return nil, fmt.Errorf("application default credentials: %w", err)
	}
	cfg := &rest.Config{Host: "https://" + ref.Endpoint}
	cfg.Wrap(func(base http.RoundTripper) http.RoundTripper {
		return &oauth2.Transport{Source: ts, Base: base}
	})
	return cfg, nil
}

// gkeFleetLister is the production fleetLister.
type gkeFleetLister struct {
	svc func(ctx context.Context) (*container.Service, error)
}

func newFleetLister() *gkeFleetLister {
	return &gkeFleetLister{
		svc: lazyClient(func(ctx context.Context) (*container.Service, error) { return container.NewService(ctx) }),
	}
}

func (l *gkeFleetLister) ListClusters(ctx context.Context, parent string) ([]*container.Cluster, error) {
	svc, err := l.svc(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := svc.Projects.Locations.Clusters.List(parent).Context(ctx).Do()
	if err != nil {
		return nil, err
	}
	return resp.Clusters, nil
}
