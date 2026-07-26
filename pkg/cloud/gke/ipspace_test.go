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
	"testing"

	compute "google.golang.org/api/compute/v1"
	container "google.golang.org/api/container/v1"
)

type fixtureClusterGetter struct{ t *testing.T }

func (f *fixtureClusterGetter) GetCluster(context.Context) (*container.Cluster, error) {
	var c container.Cluster
	loadJSON(f.t, "container-cluster.json", &c)
	return &c, nil
}

type fixtureSubnetGetter struct {
	t      *testing.T
	region string // records what region was asked for
}

func (f *fixtureSubnetGetter) GetSubnetwork(_ context.Context, region, name string) (*compute.Subnetwork, error) {
	f.region = region
	if name != "prod-subnet" {
		f.t.Fatalf("GetSubnetwork name = %q, want prod-subnet", name)
	}
	var sn compute.Subnetwork
	loadJSON(f.t, "compute-subnetwork.json", &sn)
	return &sn, nil
}

func TestSubnetUtilizationFromRecordedCluster(t *testing.T) {
	subnets := &fixtureSubnetGetter{t: t}
	api := &ipspaceAPI{
		location: "us-east1-b", // zonal location: region must be derived
		clusters: &fixtureClusterGetter{t: t},
		subnets:  subnets,
	}
	got, err := api.SubnetUtilization(context.Background())
	if err != nil {
		t.Fatalf("SubnetUtilization: %v", err)
	}
	if subnets.region != "us-east1" {
		t.Errorf("subnetworks.get region = %q, want us-east1 (derived from the zonal location)", subnets.region)
	}
	if len(got) != 3 {
		t.Fatalf("ranges = %+v, want pods/services/nodes", got)
	}

	pods := got[0]
	// 93 nodes × /24 blocks (nodeIpv4CidrSize) = 93×256 = 23808 of
	// the /14's 262144 — allocation-block accounting, not pod IPs.
	if pods.Purpose != "pods" || pods.Subnet != "prod-subnet" || pods.CIDR != "10.8.0.0/14" ||
		pods.Used != 23808 || pods.Capacity != 262144 {
		t.Errorf("pods = %+v, want 23808/262144 of 10.8.0.0/14", pods)
	}

	services := got[1]
	if services.Purpose != "services" || services.CIDR != "10.12.0.0/20" ||
		services.Used != -1 || services.Capacity != 4096 {
		t.Errorf("services = %+v, want the explicit Used=-1 not-cloud-visible sentinel with capacity 4096", services)
	}

	nodes := got[2]
	// /24 primary = 256 minus the 4 GCP-reserved addresses.
	if nodes.Purpose != "nodes" || nodes.CIDR != "10.0.0.0/24" ||
		nodes.Used != 93 || nodes.Capacity != 252 {
		t.Errorf("nodes = %+v, want 93/252 of 10.0.0.0/24", nodes)
	}
}

func TestPodBlockSizeFallbacks(t *testing.T) {
	// Cluster-level size wins.
	c := &container.Cluster{NodeIpv4CidrSize: 26, NodePools: []*container.NodePool{{PodIpv4CidrSize: 24}}}
	if got := podBlockSize(c); got != 26 {
		t.Errorf("podBlockSize = %d, want the cluster-level 26", got)
	}
	// Else the largest per-pool block (smallest prefix).
	c = &container.Cluster{NodePools: []*container.NodePool{{PodIpv4CidrSize: 25}, {PodIpv4CidrSize: 24}}}
	if got := podBlockSize(c); got != 24 {
		t.Errorf("podBlockSize = %d, want the conservative /24 across pools", got)
	}
	// Else the GKE default.
	if got := podBlockSize(&container.Cluster{}); got != defaultPodBlockSize {
		t.Errorf("podBlockSize = %d, want the /%d default", got, defaultPodBlockSize)
	}
}

func TestLocationRegion(t *testing.T) {
	for in, want := range map[string]string{
		"us-east1-b":     "us-east1",
		"us-east1":       "us-east1",
		"europe-west1-d": "europe-west1",
		"":               "",
	} {
		if got := locationRegion(in); got != want {
			t.Errorf("locationRegion(%q) = %q, want %q", in, got, want)
		}
	}
}
