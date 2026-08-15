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

// ClusterConfigAPI implementation (`audit cluster`, epic #182): the
// security-relevant fields of the GKE clusters.get record, projected
// onto the provider-neutral cloud.ClusterConfig shape.
//
// It is the same one API call `cloud ipspace` already makes — the
// clusters.get response carries the whole cluster, and the posture
// checks read a different corner of it. The projection lives here
// rather than in pkg/checks because that is the AGENTS.md boundary:
// container.Cluster is an SDK type and pkg/checks may not see one.
//
// # Tri-states are preserved, not flattened
//
// Two of the three fields the posture checks read are absent-able, and
// absent means something different from either explicit value:
// workloadMetadataConfig unset resolves to the cluster default, and a
// node pool with no disable-legacy-endpoints metadata key was never
// configured either way. Collapsing those to a bool here would decide
// the check's question inside the SDK adapter, where the reasoning
// cannot be read. They are carried across as the documented
// cloud.MetadataMode* / cloud.LegacyEndpoints* values instead.

import (
	"context"
	"fmt"

	container "google.golang.org/api/container/v1"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// legacyEndpointsKey is the node metadata key GKE writes to turn the
// pre-v1 (/0.1/, /v1beta1/) metadata endpoints off. GKE sets it to
// "true" on pools created since 1.12; anything else leaves them
// serving credentials to any unheaded request from the node.
const legacyEndpointsKey = "disable-legacy-endpoints"

// The GKE workloadMetadataConfig.mode enum values.
const (
	gkeMetadataMode = "GKE_METADATA"
	gceMetadataMode = "GCE_METADATA"
)

// clusterConfigGetter is the §13 small client interface over the one
// GKE API call this capability makes (clusters.get). Production uses
// the same adapter `cloud ipspace` does; tests replay a recorded
// response fixture.
type clusterConfigGetter interface {
	GetCluster(ctx context.Context) (*container.Cluster, error)
}

// clusterConfigAPI implements cloud.ClusterConfigAPI.
type clusterConfigAPI struct {
	location string
	cluster  string
	clusters clusterConfigGetter
}

func newClusterConfigAPI(p *Provider) *clusterConfigAPI {
	return &clusterConfigAPI{
		location: p.location,
		cluster:  p.cluster,
		clusters: newGKEClusterClient(p.project, p.location, p.cluster),
	}
}

// Config implements cloud.ClusterConfigAPI.
func (a *clusterConfigAPI) Config(ctx context.Context) (cloud.ClusterConfig, error) {
	c, err := a.clusters.GetCluster(ctx)
	if err != nil {
		return cloud.ClusterConfig{}, fmt.Errorf("reading cluster record: %w", err)
	}

	out := cloud.ClusterConfig{
		Name:     firstNonEmpty(c.Name, a.cluster),
		Location: firstNonEmpty(c.Location, a.location),
	}
	if c.WorkloadIdentityConfig != nil {
		out.WorkloadIdentityPool = c.WorkloadIdentityConfig.WorkloadPool
	}

	out.PublicEndpoint = publicEndpoint(c)
	out.AuthorizedNetworks = authorizedNetworks(c)

	for _, np := range c.NodePools {
		if np == nil {
			continue
		}
		out.NodePools = append(out.NodePools, cloud.NodePoolConfig{
			Name:               np.Name,
			MetadataServerMode: metadataServerMode(np),
			LegacyEndpoints:    legacyEndpoints(np),
		})
	}
	return out, nil
}

// ipEndpointsConfig returns the cluster's current-surface IP endpoint
// configuration, nil when the record predates it. GKE populates both
// surfaces on reads, and the newer one is where the fields moved, so
// it wins wherever it is present.
func ipEndpointsConfig(c *container.Cluster) *container.IPEndpointsConfig {
	if c.ControlPlaneEndpointsConfig == nil {
		return nil
	}
	return c.ControlPlaneEndpointsConfig.IpEndpointsConfig
}

// publicEndpoint resolves the control plane's internet-facing address,
// "" when there is none.
//
// On the current surface the two booleans are separate questions: IP
// endpoints can be off entirely (a DNS-only control plane), and with
// them on the public one can still be disabled. On the deprecated
// surface a cluster with no privateClusterConfig at all is the fully
// public default, and reports its address in the top-level Endpoint.
func publicEndpoint(c *container.Cluster) string {
	if ip := ipEndpointsConfig(c); ip != nil {
		if !ip.Enabled || !ip.EnablePublicEndpoint {
			return ""
		}
		return firstNonEmpty(ip.PublicEndpoint, c.Endpoint)
	}
	switch {
	case c.PrivateClusterConfig == nil:
		return c.Endpoint
	case c.PrivateClusterConfig.EnablePrivateEndpoint:
		return ""
	default:
		return firstNonEmpty(c.PrivateClusterConfig.PublicEndpoint, c.Endpoint)
	}
}

// authorizedNetworks projects the allow-list in front of the public
// endpoint. The API forbids setting both the top-level
// masterAuthorizedNetworksConfig and the ipEndpointsConfig one, so at
// most one is populated; the newer location wins.
func authorizedNetworks(c *container.Cluster) cloud.AuthorizedNetworks {
	m := c.MasterAuthorizedNetworksConfig
	if ip := ipEndpointsConfig(c); ip != nil && ip.AuthorizedNetworksConfig != nil {
		m = ip.AuthorizedNetworksConfig
	}
	if m == nil {
		return cloud.AuthorizedNetworks{}
	}
	out := cloud.AuthorizedNetworks{
		Enabled:        m.Enabled,
		GCPPublicCIDRs: m.GcpPublicCidrsAccessEnabled,
	}
	for _, b := range m.CidrBlocks {
		if b == nil || b.CidrBlock == "" {
			continue
		}
		out.CIDRs = append(out.CIDRs, b.CidrBlock)
	}
	return out
}

// metadataServerMode projects workloadMetadataConfig.mode. An unset
// config and an unrecognized mode are both reported as unset: the
// check's claim is about what is KNOWN to expose the node identity,
// and guessing a default here would make it fire on a cluster shape
// this code has never seen.
func metadataServerMode(np *container.NodePool) string {
	if np.Config == nil || np.Config.WorkloadMetadataConfig == nil {
		return cloud.MetadataModeUnset
	}
	switch np.Config.WorkloadMetadataConfig.Mode {
	case gkeMetadataMode:
		return cloud.MetadataModeProviderServer
	case gceMetadataMode:
		return cloud.MetadataModeNodeIdentity
	default:
		return cloud.MetadataModeUnset
	}
}

// legacyEndpoints projects the disable-legacy-endpoints node metadata
// key. The value is a string in the API ("true"/"false"), and a pool
// carrying no key at all is its own state.
func legacyEndpoints(np *container.NodePool) string {
	if np.Config == nil {
		return cloud.LegacyEndpointsUnset
	}
	v, ok := np.Config.Metadata[legacyEndpointsKey]
	if !ok {
		return cloud.LegacyEndpointsUnset
	}
	if v == "true" {
		return cloud.LegacyEndpointsDisabled
	}
	return cloud.LegacyEndpointsEnabled
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
