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
	"errors"
	"reflect"
	"testing"
	"time"

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

// fixtureServerConfigGetter replays the recorded getServerConfig
// response, or fails the call.
type fixtureServerConfigGetter struct {
	t   *testing.T
	err error
}

func (f *fixtureServerConfigGetter) GetServerConfig(context.Context) (*container.ServerConfig, error) {
	if f.err != nil {
		return nil, f.err
	}
	var sc container.ServerConfig
	loadJSON(f.t, "container-server-config.json", &sc)
	return &sc, nil
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
		{
			Name: "default-pool", Version: "1.29.7-gke.1104000",
			ImageType: "COS_CONTAINERD", NodeRuntime: cloud.NodeRuntimeContainerd,
			MetadataServerMode: cloud.MetadataModeProviderServer,
			LegacyEndpoints:    cloud.ToggleDisabled,
			AutoUpgrade:        cloud.ToggleEnabled, AutoRepair: cloud.ToggleEnabled,
		},
		{
			Name: "legacy-pool", Version: "1.27.11-gke.1062000",
			ImageType: "COS", NodeRuntime: cloud.NodeRuntimeDockershim,
			MetadataServerMode: cloud.MetadataModeNodeIdentity,
			LegacyEndpoints:    cloud.ToggleEnabled,
			AutoUpgrade:        cloud.ToggleDisabled, AutoRepair: cloud.ToggleDisabled,
		},
		{
			Name: "unconfigured-pool", NodeRuntime: cloud.NodeRuntimeUnset,
			MetadataServerMode: cloud.MetadataModeUnset,
			LegacyEndpoints:    cloud.ToggleUnset,
			AutoUpgrade:        cloud.ToggleUnset, AutoRepair: cloud.ToggleUnset,
		},
	}
	if !reflect.DeepEqual(got.NodePools, wantPools) {
		t.Errorf("NodePools = %+v, want %+v", got.NodePools, wantPools)
	}
}

// The same record, read for upgrade governance: every block is absent,
// and each absence has to arrive as its own documented value rather
// than as a plausible default.
func TestClusterConfigUpgradeFieldsWhenNothingIsConfigured(t *testing.T) {
	got := configFrom(t, "container-cluster-posture.json")

	if got.Version != "1.29.7-gke.1104000" {
		t.Errorf("Version = %q, want the currentMasterVersion", got.Version)
	}
	if got.ReleaseChannel != "" {
		t.Errorf("ReleaseChannel = %q, want empty: the record carries no releaseChannel", got.ReleaseChannel)
	}
	if got.Maintenance.Scheduled || len(got.Maintenance.Exclusions) != 0 {
		t.Errorf("Maintenance = %+v, want unscheduled with no exclusions", got.Maintenance)
	}
	// Absent and present-but-off are the same claim here — nobody is
	// told — so this one toggle has no third state.
	if got.UpgradeNotifications != cloud.ToggleDisabled {
		t.Errorf("UpgradeNotifications = %q, want disabled: there is no notificationConfig", got.UpgradeNotifications)
	}
}

// The counterpart record, with every governance block populated.
func TestClusterConfigUpgradeFieldsFromGovernedCluster(t *testing.T) {
	got := configFrom(t, "container-cluster-upgrades.json")

	if got.Version != "1.30.5-gke.1443001" {
		t.Errorf("Version = %q, want the currentMasterVersion", got.Version)
	}
	if got.ReleaseChannel != "regular" {
		t.Errorf("ReleaseChannel = %q, want the lower-cased channel name", got.ReleaseChannel)
	}
	if got.UpgradeNotifications != cloud.ToggleEnabled {
		t.Errorf("UpgradeNotifications = %q, want enabled", got.UpgradeNotifications)
	}
	if !got.Maintenance.Scheduled {
		t.Error("Maintenance.Scheduled = false, want true: the record carries a recurringWindow")
	}

	// Exclusions come out of a JSON map, so the order has to be imposed
	// rather than inherited, and the scope of one that names none is the
	// API's documented default: everything.
	want := []cloud.MaintenanceExclusion{
		{
			Name:  "migration",
			Start: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
			Scope: cloud.ExclusionScopeMinorAndNodes, UntilEndOfSupport: true,
		},
		{
			Name:  "minor-hold",
			Start: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
			Scope: cloud.ExclusionScopeMinor,
		},
		{
			Name:  "retail-freeze",
			Start: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2027, 1, 5, 0, 0, 0, 0, time.UTC),
			Scope: cloud.ExclusionScopeAll,
		},
		{
			Name:  "unscoped",
			Start: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC),
			Scope: cloud.ExclusionScopeAll,
		},
	}
	if !reflect.DeepEqual(got.Maintenance.Exclusions, want) {
		t.Errorf("Exclusions = %+v, want %+v", got.Maintenance.Exclusions, want)
	}
}

// A maintenance policy whose window holds only exclusions is not a
// schedule: maintenance still runs at any hour outside them.
func TestMaintenanceExclusionsWithoutAWindowAreNotScheduled(t *testing.T) {
	c := &container.Cluster{MaintenancePolicy: &container.MaintenancePolicy{
		Window: &container.MaintenanceWindow{
			MaintenanceExclusions: map[string]container.TimeWindow{
				"freeze": {StartTime: "2026-01-01T00:00:00Z", EndTime: "2026-02-01T00:00:00Z"},
			},
		},
	}}
	got := maintenance(c)
	if got.Scheduled {
		t.Error("Scheduled = true, want false: exclusions say when NOT to act, not when to")
	}
	if len(got.Exclusions) != 1 {
		t.Fatalf("Exclusions = %+v, want the one window", got.Exclusions)
	}
	if !got.Exclusions[0].End.Equal(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("End = %v, want the parsed RFC 3339 bound", got.Exclusions[0].End)
	}
}

// Any of the three window shapes counts as a schedule.
func TestMaintenanceWindowShapes(t *testing.T) {
	cases := map[string]*container.MaintenanceWindow{
		"daily":     {DailyMaintenanceWindow: &container.DailyMaintenanceWindow{StartTime: "02:00"}},
		"recurring": {RecurringWindow: &container.RecurringTimeWindow{Recurrence: "FREQ=WEEKLY"}},
		"renamed":   {RecurringMaintenanceWindow: &container.RecurringMaintenanceWindow{Recurrence: "FREQ=WEEKLY"}},
	}
	for name, w := range cases {
		t.Run(name, func(t *testing.T) {
			c := &container.Cluster{MaintenancePolicy: &container.MaintenancePolicy{Window: w}}
			if !maintenance(c).Scheduled {
				t.Error("Scheduled = false, want true")
			}
		})
	}
}

// GKE's own "no channel" enum value and an absent block are the same
// answer; UNSPECIFIED is deprecated in favour of omitting it.
func TestReleaseChannelUnspecifiedIsNoChannel(t *testing.T) {
	for _, in := range []*container.Cluster{
		{},
		{ReleaseChannel: &container.ReleaseChannel{}},
		{ReleaseChannel: &container.ReleaseChannel{Channel: "UNSPECIFIED"}},
	} {
		if got := releaseChannel(in); got != "" {
			t.Errorf("releaseChannel(%+v) = %q, want empty", in.ReleaseChannel, got)
		}
	}
}

// The image type IS the runtime on GKE, and an unnamed one takes a
// default this read cannot see.
func TestNodeRuntimeFromImageType(t *testing.T) {
	for image, want := range map[string]string{
		"COS_CONTAINERD":    cloud.NodeRuntimeContainerd,
		"UBUNTU_CONTAINERD": cloud.NodeRuntimeContainerd,
		"COS":               cloud.NodeRuntimeDockershim,
		"UBUNTU":            cloud.NodeRuntimeDockershim,
		"WINDOWS_SAC":       cloud.NodeRuntimeDockershim,
		"":                  cloud.NodeRuntimeUnset,
	} {
		np := &container.NodePool{Config: &container.NodeConfig{ImageType: image}}
		if got := nodeRuntime(np); got != want {
			t.Errorf("nodeRuntime(%q) = %q, want %q", image, got, want)
		}
	}
	if got := nodeRuntime(&container.NodePool{}); got != cloud.NodeRuntimeUnset {
		t.Errorf("a pool with no config = %q, want unset", got)
	}
}

// UpgradeTargets is a question about the LOCATION's published versions,
// answered per channel.
func TestUpgradeTargetsPerChannel(t *testing.T) {
	cases := []struct {
		channel          string
		wantDefault      string
		wantUpgradeTo    string
		wantEchoedAsUsed string
	}{
		// Mid-rollout: the channel's default and its upgrade target
		// differ, and the target is what an existing cluster is headed to.
		{channel: "regular", wantDefault: "1.30.5-gke.1443001", wantUpgradeTo: "1.30.6-gke.1125000", wantEchoedAsUsed: "regular"},
		{channel: "rapid", wantDefault: "1.31.1-gke.1146000", wantUpgradeTo: "1.31.1-gke.1146000", wantEchoedAsUsed: "rapid"},
		// STABLE publishes no upgrade target: nothing is rolling.
		{channel: "stable", wantDefault: "1.29.9-gke.1496000", wantEchoedAsUsed: "stable"},
		// No channel: the location's default cluster version, and no
		// upgrade target, because nothing is going to upgrade it.
		{channel: "", wantDefault: "1.30.5-gke.1443001"},
		// A channel this location publishes nothing for. Zero, no error:
		// the caller then has no target and must not invent one.
		{channel: "extended"},
	}
	for _, tc := range cases {
		t.Run("channel="+tc.channel, func(t *testing.T) {
			api := &clusterConfigAPI{server: &fixtureServerConfigGetter{t: t}}
			got, err := api.UpgradeTargets(context.Background(), tc.channel)
			if err != nil {
				t.Fatalf("UpgradeTargets: %v", err)
			}
			want := cloud.UpgradeTargets{
				Channel:              tc.wantEchoedAsUsed,
				DefaultVersion:       tc.wantDefault,
				UpgradeTargetVersion: tc.wantUpgradeTo,
			}
			if got != want {
				t.Errorf("UpgradeTargets = %+v, want %+v", got, want)
			}
		})
	}
}

func TestUpgradeTargetsReadError(t *testing.T) {
	api := &clusterConfigAPI{server: &fixtureServerConfigGetter{err: errors.New("permission denied")}}
	if _, err := api.UpgradeTargets(context.Background(), "regular"); err == nil {
		t.Fatal("UpgradeTargets succeeded, want the read error surfaced")
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
		if np.MetadataServerMode != cloud.MetadataModeUnset || np.LegacyEndpoints != cloud.ToggleUnset {
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
	for value, want := range map[string]cloud.Toggle{
		"true":  cloud.ToggleDisabled,
		"false": cloud.ToggleEnabled,
		"TRUE":  cloud.ToggleEnabled,
		"":      cloud.ToggleEnabled,
	} {
		np := &container.NodePool{Config: &container.NodeConfig{
			Metadata: map[string]string{legacyEndpointsKey: value},
		}}
		if got := legacyEndpoints(np); got != want {
			t.Errorf("legacyEndpoints(%q) = %q, want %q", value, got, want)
		}
	}
	if got := legacyEndpoints(&container.NodePool{Config: &container.NodeConfig{}}); got != cloud.ToggleUnset {
		t.Errorf("a pool with no metadata key = %q, want unset", got)
	}
}
