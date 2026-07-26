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

// IPSpaceAPI implementation (`cloud ipspace`, DESIGN.md §5): the
// cluster's three ranges from the GKE clusters.get record joined
// with the subnet's primary CIDR from compute subnetworks.get.
//
// # What "used" means per range (the semantics that matter)
//
//   - pods: GKE carves the cluster (pod) range into one block per
//     node (/nodeIpv4CidrSize, default /24). The range exhausts when
//     the next NODE cannot get a block — long before every pod IP is
//     assigned — so Used counts the addresses of the blocks already
//     allocated: currentNodeCount × 2^(32-blockSize). This is the
//     number behind the CA's IP_SPACE_EXHAUSTED noScaleUp reason
//     (§10.1).
//   - nodes: node IPs from the subnet's primary range; Used is the
//     node count, Capacity excludes the 4 addresses GCP reserves in
//     every primary range.
//   - services: ClusterIPs are allocated by the Kubernetes API
//     server; no cloud API sees the count. Used = -1 (the documented
//     "not cloud-visible" sentinel) — the command renders that
//     explicitly rather than as a fake 0%.

import (
	"context"
	"fmt"
	"net/netip"

	compute "google.golang.org/api/compute/v1"
	container "google.golang.org/api/container/v1"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// defaultPodBlockSize is the per-node pod CIDR prefix length GKE
// defaults to when the cluster record does not carry one.
const defaultPodBlockSize = 24

// primaryRangeReserved is the number of addresses GCP reserves in
// every subnet primary range (network, gateway, second-to-last,
// broadcast).
const primaryRangeReserved = 4

// ipspaceClusterGetter is the §13 small client interface over the
// one GKE API call (clusters.get); ipspaceSubnetGetter over the one
// Compute call (subnetworks.get). Production adapters below; tests
// replay recorded response fixtures.
type ipspaceClusterGetter interface {
	GetCluster(ctx context.Context) (*container.Cluster, error)
}

type ipspaceSubnetGetter interface {
	GetSubnetwork(ctx context.Context, region, name string) (*compute.Subnetwork, error)
}

// ipspaceAPI implements cloud.IPSpaceAPI.
type ipspaceAPI struct {
	location string
	clusters ipspaceClusterGetter
	subnets  ipspaceSubnetGetter
}

func newIPSpaceAPI(p *Provider) *ipspaceAPI {
	return &ipspaceAPI{
		location: p.location,
		clusters: newGKEClusterClient(p.project, p.location, p.cluster),
		subnets:  newGCESubnetClient(p.project),
	}
}

// SubnetUtilization implements cloud.IPSpaceAPI.
func (a *ipspaceAPI) SubnetUtilization(ctx context.Context) ([]cloud.SubnetUtilization, error) {
	c, err := a.clusters.GetCluster(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading cluster record: %w", err)
	}
	subnetName := resourceTail(c.Subnetwork)
	nodeCount := c.CurrentNodeCount
	blockSize := podBlockSize(c)

	var out []cloud.SubnetUtilization

	if c.ClusterIpv4Cidr != "" {
		capacity, err := rangeSize(c.ClusterIpv4Cidr)
		if err != nil {
			return nil, fmt.Errorf("cluster (pod) range: %w", err)
		}
		out = append(out, cloud.SubnetUtilization{
			Subnet:   subnetName,
			CIDR:     c.ClusterIpv4Cidr,
			Purpose:  "pods",
			Used:     nodeCount << (32 - blockSize),
			Capacity: capacity,
		})
	}

	if c.ServicesIpv4Cidr != "" {
		capacity, err := rangeSize(c.ServicesIpv4Cidr)
		if err != nil {
			return nil, fmt.Errorf("services range: %w", err)
		}
		out = append(out, cloud.SubnetUtilization{
			Subnet:   subnetName,
			CIDR:     c.ServicesIpv4Cidr,
			Purpose:  "services",
			Used:     -1, // k8s-side counter; not cloud-visible (package comment)
			Capacity: capacity,
		})
	}

	if subnetName != "" {
		sn, err := a.subnets.GetSubnetwork(ctx, locationRegion(a.location), subnetName)
		if err != nil {
			return nil, fmt.Errorf("reading subnetwork %q: %w", subnetName, err)
		}
		capacity, err := rangeSize(sn.IpCidrRange)
		if err != nil {
			return nil, fmt.Errorf("subnet primary range: %w", err)
		}
		out = append(out, cloud.SubnetUtilization{
			Subnet:   subnetName,
			CIDR:     sn.IpCidrRange,
			Purpose:  "nodes",
			Used:     nodeCount,
			Capacity: capacity - primaryRangeReserved,
		})
	}
	return out, nil
}

// podBlockSize resolves the per-node pod block prefix length:
// the cluster-level nodeIpv4CidrSize when set, else the LARGEST
// per-pool block (smallest prefix — the conservative bound when
// pools differ), else the GKE default /24.
func podBlockSize(c *container.Cluster) int64 {
	if c.NodeIpv4CidrSize > 0 {
		return c.NodeIpv4CidrSize
	}
	best := int64(0)
	for _, np := range c.NodePools {
		if np == nil || np.PodIpv4CidrSize <= 0 {
			continue
		}
		if best == 0 || np.PodIpv4CidrSize < best {
			best = np.PodIpv4CidrSize
		}
	}
	if best > 0 {
		return best
	}
	return defaultPodBlockSize
}

// rangeSize returns the total address count of an IPv4 CIDR.
func rangeSize(cidr string) (int64, error) {
	p, err := netip.ParsePrefix(cidr)
	if err != nil {
		return 0, err
	}
	if !p.Addr().Is4() {
		return 0, fmt.Errorf("range %s is not IPv4", cidr)
	}
	return 1 << (32 - p.Bits()), nil
}

// gkeClusterClient is the production ipspaceClusterGetter.
type gkeClusterClient struct {
	project, location, cluster string
	svc                        func(ctx context.Context) (*container.Service, error)
}

func newGKEClusterClient(project, location, cluster string) *gkeClusterClient {
	return &gkeClusterClient{
		project: project, location: location, cluster: cluster,
		svc: lazyClient(func(ctx context.Context) (*container.Service, error) { return container.NewService(ctx) }),
	}
}

func (c *gkeClusterClient) GetCluster(ctx context.Context) (*container.Cluster, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", c.project, c.location, c.cluster)
	return svc.Projects.Locations.Clusters.Get(name).Context(ctx).Do()
}

// gceSubnetClient is the production ipspaceSubnetGetter.
type gceSubnetClient struct {
	project string
	svc     func(ctx context.Context) (*compute.Service, error)
}

func newGCESubnetClient(project string) *gceSubnetClient {
	return &gceSubnetClient{
		project: project,
		svc:     lazyClient(func(ctx context.Context) (*compute.Service, error) { return compute.NewService(ctx) }),
	}
}

func (c *gceSubnetClient) GetSubnetwork(ctx context.Context, region, name string) (*compute.Subnetwork, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	return svc.Subnetworks.Get(c.project, region, name).Context(ctx).Do()
}
