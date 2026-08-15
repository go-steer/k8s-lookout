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
	"reflect"
	"testing"

	container "google.golang.org/api/container/v1"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// fixtureConfigGetter replays a recorded clusters.get response.
type fixtureConfigGetter struct {
	t    *testing.T
	file string
}

func (f *fixtureConfigGetter) GetCluster(context.Context) (*container.Cluster, error) {
	var c container.Cluster
	loadJSON(f.t, f.file, &c)
	return &c, nil
}

func configFrom(t *testing.T, file string) cloud.ClusterConfig {
	t.Helper()
	api := &clusterConfigAPI{
		location: "us-central1",
		cluster:  "exposed",
		clusters: &fixtureConfigGetter{t: t, file: file},
	}
	got, err := api.Config(context.Background())
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	return got
}

func TestClusterConfigFromRecordedCluster(t *testing.T) {
	got := configFrom(t, "container-cluster-posture.json")

	if got.Name != "exposed" || got.Location != "us-central1" {
		t.Errorf("identity = %q/%q, want exposed/us-central1", got.Name, got.Location)
	}
	// The fixture carries no workloadIdentityConfig: the feature is off
	// cluster-wide, which is exactly what an empty pool must mean.
	if got.WorkloadIdentityPool != "" {
		t.Errorf("WorkloadIdentityPool = %q, want empty (no workloadIdentityConfig in the record)", got.WorkloadIdentityPool)
	}
	if got.PublicEndpoint != "203.0.113.10" {
		t.Errorf("PublicEndpoint = %q, want the ipEndpointsConfig public address", got.PublicEndpoint)
	}

	wantNetworks := cloud.AuthorizedNetworks{
		Enabled:        true,
		CIDRs:          []string{"198.51.100.0/24", "0.0.0.0/0"},
		GCPPublicCIDRs: true,
	}
	if !reflect.DeepEqual(got.AuthorizedNetworks, wantNetworks) {
		t.Errorf("AuthorizedNetworks = %+v, want %+v", got.AuthorizedNetworks, wantNetworks)
	}

	wantPools := []cloud.NodePoolConfig{
		{Name: "default-pool", MetadataServerMode: cloud.MetadataModeProviderServer, LegacyEndpoints: cloud.LegacyEndpointsDisabled},
		{Name: "legacy-pool", MetadataServerMode: cloud.MetadataModeNodeIdentity, LegacyEndpoints: cloud.LegacyEndpointsEnabled},
		{Name: "unconfigured-pool", MetadataServerMode: cloud.MetadataModeUnset, LegacyEndpoints: cloud.LegacyEndpointsUnset},
	}
	if !reflect.DeepEqual(got.NodePools, wantPools) {
		t.Errorf("NodePools = %+v, want %+v", got.NodePools, wantPools)
	}
}

// The ipspace fixture is a cluster record with none of the posture
// configuration blocks at all — the shape every tri-state has to
// survive. Nothing may be invented from an absent block, and the
// deprecated-surface fallback has to recognize "no privateClusterConfig"
// as the fully public default.
func TestClusterConfigFromSparseRecord(t *testing.T) {
	got := configFrom(t, "container-cluster.json")

	if got.Name != "prod" || got.Location != "us-east1-b" {
		t.Errorf("identity = %q/%q, want prod/us-east1-b", got.Name, got.Location)
	}
	if got.WorkloadIdentityPool != "" {
		t.Errorf("WorkloadIdentityPool = %q, want empty", got.WorkloadIdentityPool)
	}
	if !reflect.DeepEqual(got.AuthorizedNetworks, cloud.AuthorizedNetworks{}) {
		t.Errorf("AuthorizedNetworks = %+v, want the zero value: no allow-list is configured", got.AuthorizedNetworks)
	}
	for _, np := range got.NodePools {
		if np.MetadataServerMode != cloud.MetadataModeUnset || np.LegacyEndpoints != cloud.LegacyEndpointsUnset {
			t.Errorf("pool %q = %+v, want both tri-states unset — the record configures neither", np.Name, np)
		}
	}
}

// publicEndpoint spans two API surfaces, and GKE populates both. Each
// row is a cluster shape an operator can actually have.
func TestPublicEndpointAcrossBothAPISurfaces(t *testing.T) {
	cases := []struct {
		name string
		in   *container.Cluster
		want string
	}{
		{
			name: "current surface, public endpoint on",
			in: &container.Cluster{
				Endpoint: "203.0.113.10",
				ControlPlaneEndpointsConfig: &container.ControlPlaneEndpointsConfig{
					IpEndpointsConfig: &container.IPEndpointsConfig{
						Enabled: true, EnablePublicEndpoint: true, PublicEndpoint: "203.0.113.10",
					},
				},
			},
			want: "203.0.113.10",
		},
		{
			name: "current surface, public endpoint off",
			in: &container.Cluster{
				Endpoint: "10.0.0.2",
				ControlPlaneEndpointsConfig: &container.ControlPlaneEndpointsConfig{
					IpEndpointsConfig: &container.IPEndpointsConfig{
						Enabled: true, PrivateEndpoint: "10.0.0.2",
					},
				},
			},
			want: "",
		},
		{
			name: "current surface, IP endpoints off entirely (DNS-only control plane)",
			in: &container.Cluster{
				Endpoint: "203.0.113.10",
				ControlPlaneEndpointsConfig: &container.ControlPlaneEndpointsConfig{
					IpEndpointsConfig: &container.IPEndpointsConfig{EnablePublicEndpoint: true},
				},
			},
			want: "",
		},
		{
			name: "deprecated surface, private endpoint only",
			in: &container.Cluster{
				Endpoint:             "10.0.0.2",
				PrivateClusterConfig: &container.PrivateClusterConfig{EnablePrivateEndpoint: true, PrivateEndpoint: "10.0.0.2"},
			},
			want: "",
		},
		{
			name: "deprecated surface, private nodes with a public control plane",
			in: &container.Cluster{
				Endpoint:             "203.0.113.10",
				PrivateClusterConfig: &container.PrivateClusterConfig{EnablePrivateNodes: true, PublicEndpoint: "203.0.113.10"},
			},
			want: "203.0.113.10",
		},
		{
			name: "no endpoint configuration at all: the fully public default",
			in:   &container.Cluster{Endpoint: "203.0.113.10"},
			want: "203.0.113.10",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := publicEndpoint(tc.in); got != tc.want {
				t.Errorf("publicEndpoint = %q, want %q", got, tc.want)
			}
		})
	}
}

// The two authorized-networks locations are mutually exclusive per the
// API, and the newer one wins where present.
func TestAuthorizedNetworksLocationPrecedence(t *testing.T) {
	deprecated := &container.Cluster{
		MasterAuthorizedNetworksConfig: &container.MasterAuthorizedNetworksConfig{
			Enabled:    true,
			CidrBlocks: []*container.CidrBlock{{CidrBlock: "198.51.100.0/24"}},
		},
	}
	if got := authorizedNetworks(deprecated); !got.Enabled || len(got.CIDRs) != 1 || got.CIDRs[0] != "198.51.100.0/24" {
		t.Errorf("deprecated location = %+v, want the one office block", got)
	}

	current := &container.Cluster{
		MasterAuthorizedNetworksConfig: &container.MasterAuthorizedNetworksConfig{Enabled: false},
		ControlPlaneEndpointsConfig: &container.ControlPlaneEndpointsConfig{
			IpEndpointsConfig: &container.IPEndpointsConfig{
				AuthorizedNetworksConfig: &container.MasterAuthorizedNetworksConfig{
					Enabled:    true,
					CidrBlocks: []*container.CidrBlock{{CidrBlock: "192.0.2.0/24"}},
				},
			},
		},
	}
	got := authorizedNetworks(current)
	if !got.Enabled || len(got.CIDRs) != 1 || got.CIDRs[0] != "192.0.2.0/24" {
		t.Errorf("current location = %+v, want the ipEndpointsConfig allow-list to win", got)
	}
}

// An unrecognized workloadMetadataConfig mode reports unset rather
// than guessing: the claim is about what is KNOWN to expose the node
// identity.
func TestMetadataModeUnknownValueIsUnset(t *testing.T) {
	np := &container.NodePool{Config: &container.NodeConfig{
		WorkloadMetadataConfig: &container.WorkloadMetadataConfig{Mode: "MODE_UNSPECIFIED"},
	}}
	if got := metadataServerMode(np); got != cloud.MetadataModeUnset {
		t.Errorf("metadataServerMode = %q, want unset", got)
	}
}

// disable-legacy-endpoints is a metadata STRING, and only the exact
// "true" turns the endpoints off.
func TestLegacyEndpointsValues(t *testing.T) {
	for value, want := range map[string]string{
		"true":  cloud.LegacyEndpointsDisabled,
		"false": cloud.LegacyEndpointsEnabled,
		"TRUE":  cloud.LegacyEndpointsEnabled,
		"":      cloud.LegacyEndpointsEnabled,
	} {
		np := &container.NodePool{Config: &container.NodeConfig{
			Metadata: map[string]string{legacyEndpointsKey: value},
		}}
		if got := legacyEndpoints(np); got != want {
			t.Errorf("legacyEndpoints(%q) = %q, want %q", value, got, want)
		}
	}
	if got := legacyEndpoints(&container.NodePool{Config: &container.NodeConfig{}}); got != cloud.LegacyEndpointsUnset {
		t.Errorf("a pool with no metadata key = %q, want unset", got)
	}
}
