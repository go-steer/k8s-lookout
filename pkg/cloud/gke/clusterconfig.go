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

// ClusterConfigAPI implementation (`audit cluster` #186,
// `audit upgrades` #187, epic #182): the posture-relevant fields of the
// GKE clusters.get record, projected onto the provider-neutral
// cloud.ClusterConfig shape.
//
// It is the same one API call `cloud ipspace` already makes — the
// clusters.get response carries the whole cluster, and the posture
// checks read a different corner of it. The projection lives here
// rather than in pkg/checks because that is the AGENTS.md boundary:
// container.Cluster is an SDK type and pkg/checks may not see one.
//
// UpgradeTargets is the one thing not in that record: what the provider
// would upgrade a cluster TO is a property of the release channel, not
// of the cluster, and comes from getServerConfig for the location.
//
// # Tri-states are preserved, not flattened
//
// Several of the fields the posture checks read are absent-able, and
// absent means something different from either explicit value:
// workloadMetadataConfig unset resolves to the cluster default, a node
// pool with no disable-legacy-endpoints metadata key was never
// configured either way, and a pool with no management block states
// nothing about auto-upgrade. Collapsing those to a bool here would
// decide the check's question inside the SDK adapter, where the
// reasoning cannot be read. They are carried across as the documented
// cloud.MetadataMode* / cloud.Toggle values instead.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

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

// The GKE releaseChannel, maintenance-exclusion and image-type enum
// values this projection recognizes.
const (
	unspecifiedChannel    = "UNSPECIFIED"
	noMinorUpgrades       = "NO_MINOR_UPGRADES"
	noMinorOrNodeUpgrades = "NO_MINOR_OR_NODE_UPGRADES"
	untilEndOfSupport     = "UNTIL_END_OF_SUPPORT"
	containerdImageSuffix = "_CONTAINERD"
)

// clusterConfigGetter is the §13 small client interface over the GKE
// API calls this capability makes: clusters.get for the cluster record
// and getServerConfig for the location's published versions.
// Production uses the same clusters.get adapter `cloud ipspace` does;
// tests replay recorded response fixtures.
type clusterConfigGetter interface {
	GetCluster(ctx context.Context) (*container.Cluster, error)
}

type serverConfigGetter interface {
	GetServerConfig(ctx context.Context) (*container.ServerConfig, error)
}

// clusterConfigAPI implements cloud.ClusterConfigAPI.
type clusterConfigAPI struct {
	location string
	cluster  string
	clusters clusterConfigGetter
	server   serverConfigGetter
}

func newClusterConfigAPI(p *Provider) *clusterConfigAPI {
	return &clusterConfigAPI{
		location: p.location,
		cluster:  p.cluster,
		clusters: newGKEClusterClient(p.project, p.location, p.cluster),
		server:   newGKEServerConfigClient(p.project, p.location),
	}
}

// Config implements cloud.ClusterConfigAPI.
func (a *clusterConfigAPI) Config(ctx context.Context) (cloud.ClusterConfig, error) {
	c, err := a.clusters.GetCluster(ctx)
	if err != nil {
		return cloud.ClusterConfig{}, fmt.Errorf("reading cluster record: %w", err)
	}

	out := cloud.ClusterConfig{
		Name:                 firstNonEmpty(c.Name, a.cluster),
		Location:             firstNonEmpty(c.Location, a.location),
		Version:              c.CurrentMasterVersion,
		ReleaseChannel:       releaseChannel(c),
		Maintenance:          maintenance(c),
		UpgradeNotifications: upgradeNotifications(c),
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
			Version:            np.Version,
			ImageType:          imageType(np),
			NodeRuntime:        nodeRuntime(np),
			MetadataServerMode: metadataServerMode(np),
			LegacyEndpoints:    legacyEndpoints(np),
			AutoUpgrade:        nodeManagement(np, func(m *container.NodeManagement) bool { return m.AutoUpgrade }),
			AutoRepair:         nodeManagement(np, func(m *container.NodeManagement) bool { return m.AutoRepair }),
		})
	}
	return out, nil
}

// UpgradeTargets implements cloud.ClusterConfigAPI. The versions GKE
// publishes are per-location and per-channel, so this is a second call
// (getServerConfig) and not a corner of the cluster record.
//
// A cluster on no channel is compared against the location's default
// cluster version: it is what a new cluster created today would run,
// and the only published "current" GKE offers such a cluster. There is
// no upgrade target for it, because nothing is going to upgrade it.
func (a *clusterConfigAPI) UpgradeTargets(ctx context.Context, channel string) (cloud.UpgradeTargets, error) {
	sc, err := a.server.GetServerConfig(ctx)
	if err != nil {
		return cloud.UpgradeTargets{}, fmt.Errorf("reading published versions: %w", err)
	}
	if channel == "" {
		return cloud.UpgradeTargets{DefaultVersion: sc.DefaultClusterVersion}, nil
	}
	for _, ch := range sc.Channels {
		if ch == nil || releaseChannelName(ch.Channel) != channel {
			continue
		}
		return cloud.UpgradeTargets{
			Channel:              channel,
			DefaultVersion:       ch.DefaultVersion,
			UpgradeTargetVersion: ch.UpgradeTargetVersion,
		}, nil
	}
	// A channel the location publishes nothing for. Reporting zero is
	// the honest answer: the caller has no target and makes no claim.
	return cloud.UpgradeTargets{}, nil
}

// releaseChannel projects the cluster's subscription. UNSPECIFIED is
// GKE's own name for "no channel" and is deprecated in favour of
// omitting the block, so both arrive as empty.
func releaseChannel(c *container.Cluster) string {
	if c.ReleaseChannel == nil {
		return ""
	}
	return releaseChannelName(c.ReleaseChannel.Channel)
}

func releaseChannelName(channel string) string {
	if channel == "" || channel == unspecifiedChannel {
		return ""
	}
	return strings.ToLower(channel)
}

// maintenance projects the maintenance policy: whether a window exists
// at all, and every exclusion the policy carries. Which exclusions are
// in force is not decided here — that needs a clock, and the adapter
// has no business owning the check's "now".
func maintenance(c *container.Cluster) cloud.Maintenance {
	if c.MaintenancePolicy == nil || c.MaintenancePolicy.Window == nil {
		return cloud.Maintenance{}
	}
	w := c.MaintenancePolicy.Window
	out := cloud.Maintenance{
		Scheduled: w.DailyMaintenanceWindow != nil ||
			w.RecurringWindow != nil ||
			w.RecurringMaintenanceWindow != nil,
	}
	for name, tw := range w.MaintenanceExclusions {
		ex := cloud.MaintenanceExclusion{
			Name:  name,
			Start: parseAPITime(tw.StartTime),
			End:   parseAPITime(tw.EndTime),
			Scope: cloud.ExclusionScopeAll,
		}
		if o := tw.MaintenanceExclusionOptions; o != nil {
			ex.Scope = exclusionScope(o.Scope)
			ex.UntilEndOfSupport = o.EndTimeBehavior == untilEndOfSupport
		}
		out.Exclusions = append(out.Exclusions, ex)
	}
	// Map iteration order is random and the record has to be stable.
	sort.Slice(out.Exclusions, func(i, j int) bool {
		return out.Exclusions[i].Name < out.Exclusions[j].Name
	})
	return out
}

// exclusionScope projects the exclusion scope enum. An exclusion that
// names no scope blocks everything — that is the documented API
// default, not a guess this code is making.
func exclusionScope(scope string) string {
	switch scope {
	case noMinorUpgrades:
		return cloud.ExclusionScopeMinor
	case noMinorOrNodeUpgrades:
		return cloud.ExclusionScopeMinorAndNodes
	default:
		return cloud.ExclusionScopeAll
	}
}

// parseAPITime reads an RFC 3339 timestamp, yielding the zero time for
// anything it cannot parse. An exclusion with an unreadable bound is
// left for the caller to notice rather than dropped.
func parseAPITime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// upgradeNotifications projects notificationConfig.pubsub. The block
// being absent and the block being present-but-off are the same claim
// — nobody is told — so unlike the node-pool toggles this one has no
// third state to preserve.
func upgradeNotifications(c *container.Cluster) cloud.Toggle {
	if c.NotificationConfig == nil || c.NotificationConfig.Pubsub == nil {
		return cloud.ToggleDisabled
	}
	if c.NotificationConfig.Pubsub.Enabled {
		return cloud.ToggleEnabled
	}
	return cloud.ToggleDisabled
}

// nodeManagement projects one boolean out of the pool's management
// block, preserving the block's absence as unset.
func nodeManagement(np *container.NodePool, get func(*container.NodeManagement) bool) cloud.Toggle {
	if np.Management == nil {
		return cloud.ToggleUnset
	}
	if get(np.Management) {
		return cloud.ToggleEnabled
	}
	return cloud.ToggleDisabled
}

func imageType(np *container.NodePool) string {
	if np.Config == nil {
		return ""
	}
	return np.Config.ImageType
}

// nodeRuntime classifies the node image. GKE's image types are the
// runtime: everything current carries the _CONTAINERD suffix, and the
// ones that do not are the Docker images whose shim Kubernetes removed
// in 1.24. An unnamed image type is left unset — the pool takes the
// provider's default, which this read cannot see.
func nodeRuntime(np *container.NodePool) string {
	t := imageType(np)
	switch {
	case t == "":
		return cloud.NodeRuntimeUnset
	case strings.HasSuffix(strings.ToUpper(t), containerdImageSuffix):
		return cloud.NodeRuntimeContainerd
	default:
		return cloud.NodeRuntimeDockershim
	}
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
func legacyEndpoints(np *container.NodePool) cloud.Toggle {
	if np.Config == nil {
		return cloud.ToggleUnset
	}
	v, ok := np.Config.Metadata[legacyEndpointsKey]
	if !ok {
		return cloud.ToggleUnset
	}
	if v == "true" {
		return cloud.ToggleDisabled
	}
	return cloud.ToggleEnabled
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// gkeServerConfigClient is the production serverConfigGetter.
type gkeServerConfigClient struct {
	project, location string
	svc               func(ctx context.Context) (*container.Service, error)
}

func newGKEServerConfigClient(project, location string) *gkeServerConfigClient {
	return &gkeServerConfigClient{
		project: project, location: location,
		svc: lazyClient(func(ctx context.Context) (*container.Service, error) { return container.NewService(ctx) }),
	}
}

func (c *gkeServerConfigClient) GetServerConfig(ctx context.Context) (*container.ServerConfig, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("projects/%s/locations/%s", c.project, c.location)
	return svc.Projects.Locations.GetServerConfig(name).Context(ctx).Do()
}
