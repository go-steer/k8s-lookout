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

package audit_test

// §13 conventions for a capability-backed command: a fake provider
// that is cloud.NoProvider plus the one capability under test, a
// canned ClusterConfig standing in for the recorded clusters.get
// fixture (which is exercised on the SDK side, in pkg/cloud/gke), a
// golden, VerifyContract in both formats, and the §2 unavailable path.

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// --- fakes ---

type clusterConfigProvider struct {
	cloud.Provider
	api cloud.ClusterConfigAPI
}

func (p clusterConfigProvider) ClusterConfig() (cloud.ClusterConfigAPI, bool) { return p.api, true }

type fakeClusterConfig struct {
	cfg cloud.ClusterConfig
	err error
}

func (f *fakeClusterConfig) Config(context.Context) (cloud.ClusterConfig, error) {
	return f.cfg, f.err
}

func clusterDeps(cfg cloud.ClusterConfig) audit.Deps {
	p := clusterConfigProvider{Provider: cloud.NoProvider, api: &fakeClusterConfig{cfg: cfg}}
	return audit.Deps{
		Provider: func(context.Context) (cloud.Provider, error) { return p, nil },
	}
}

// exposedCluster is the shared fixture: one node pool per shape the
// command has to tell apart, and a public control plane behind an
// allow-list that is real but widened by the provider bypass.
//
//	default-pool  the provider metadata server, legacy endpoints off — clean
//	legacy-pool   node identity AND legacy endpoints on — both pool claims
//	old-pool      states neither, so only the legacy claim (unset serves them)
func exposedCluster() cloud.ClusterConfig {
	return cloud.ClusterConfig{
		Name:                 "prod-east",
		Location:             "us-east1",
		WorkloadIdentityPool: "acme-prod.svc.id.goog",
		PublicEndpoint:       "203.0.113.10",
		AuthorizedNetworks: cloud.AuthorizedNetworks{
			Enabled:        true,
			CIDRs:          []string{"198.51.100.0/24", "192.0.2.0/24"},
			GCPPublicCIDRs: true,
		},
		NodePools: []cloud.NodePoolConfig{
			{Name: "default-pool", MetadataServerMode: cloud.MetadataModeProviderServer, LegacyEndpoints: cloud.LegacyEndpointsDisabled},
			{Name: "legacy-pool", MetadataServerMode: cloud.MetadataModeNodeIdentity, LegacyEndpoints: cloud.LegacyEndpointsEnabled},
			{Name: "old-pool", MetadataServerMode: cloud.MetadataModeUnset, LegacyEndpoints: cloud.LegacyEndpointsUnset},
		},
	}
}

func TestClusterContract(t *testing.T) {
	checktest.VerifyContract(t, audit.ClusterCommand(clusterDeps(exposedCluster())))
}

func TestClusterGolden(t *testing.T) {
	res := checktest.Run(t, audit.ClusterCommand(clusterDeps(exposedCluster())))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	path := filepath.Join("testdata", "cluster.golden")
	if *update {
		if err := os.WriteFile(path, []byte(res.Stdout), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading golden (run 'go test ./pkg/checks/audit -update'): %v", err)
	}
	if !bytes.Equal([]byte(res.Stdout), want) {
		t.Errorf("output does not match %s:\ngot:\n%s\nwant:\n%s", path, res.Stdout, want)
	}
}

// scanned counts the cluster as well as its pools: two of the three
// claims are about the cluster and about nothing inside it, so a
// scanned that counted only pools would understate what was examined.
func TestClusterScannedCountsTheClusterAndItsPools(t *testing.T) {
	res := checktest.Run(t, audit.ClusterCommand(clusterDeps(exposedCluster())))
	if !strings.Contains(res.Stdout, "\nscanned=4 findings=4 elapsed=100ms node_pools=3\n") {
		t.Errorf("want scanned=4 (1 cluster + 3 node pools) and the node_pools note, got:\n%s", res.Stdout)
	}
}

// With Workload Identity off cluster-wide there is one setting and one
// remedy, so there is one finding — against the cluster. Repeating it
// per pool would multiply a single missing setting by the pool count,
// and every pool is on node identity by definition when it is off.
func TestClusterWorkloadIdentityOffIsOneClusterFinding(t *testing.T) {
	cfg := exposedCluster()
	cfg.WorkloadIdentityPool = ""
	res := checktest.Run(t, audit.ClusterCommand(clusterDeps(cfg)))

	var wi []map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "audit.workload_identity_off" {
			wi = append(wi, r)
		}
	}
	if len(wi) != 1 {
		t.Fatalf("want exactly one workload-identity finding, got %d:\n%s", len(wi), res.Stdout)
	}
	if wi[0]["kind_of_object"] != "Cluster" || wi[0]["name"] != "prod-east" {
		t.Errorf("subject should be the cluster: %v", wi[0])
	}
	if wi[0]["reason"] != "WorkloadIdentityDisabled" {
		t.Errorf("reason = %q, want WorkloadIdentityDisabled", wi[0]["reason"])
	}
}

// With it on, a pool still serving the node's identity is the
// interesting case: the cluster reads as configured and that pool's
// pods bypass it. The pool is the subject — it is the single edit.
func TestClusterNodePoolBypassesWorkloadIdentity(t *testing.T) {
	res := checktest.Run(t, audit.ClusterCommand(clusterDeps(exposedCluster())))

	var wi []map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "audit.workload_identity_off" {
			wi = append(wi, r)
		}
	}
	if len(wi) != 1 {
		t.Fatalf("want one finding, for the one node-identity pool, got %d:\n%s", len(wi), res.Stdout)
	}
	if wi[0]["kind_of_object"] != "NodePool" || wi[0]["name"] != "legacy-pool" {
		t.Errorf("subject should be the offending pool: %v", wi[0])
	}
	if wi[0]["cluster"] != "prod-east" || wi[0]["workload_pool"] != "acme-prod.svc.id.goog" {
		t.Errorf("the pool finding should stand alone (cluster + the pool it bypasses): %v", wi[0])
	}
}

// A pool that states no metadata mode is not judged: the provider
// resolves an unset mode from cluster-level defaults this read cannot
// see, so a claim either way would be a guess.
func TestClusterUnsetMetadataModeMakesNoClaim(t *testing.T) {
	cfg := exposedCluster()
	cfg.NodePools = []cloud.NodePoolConfig{
		{Name: "old-pool", MetadataServerMode: cloud.MetadataModeUnset, LegacyEndpoints: cloud.LegacyEndpointsDisabled},
	}
	res := checktest.Run(t, audit.ClusterCommand(clusterDeps(cfg)))
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "audit.workload_identity_off" {
			t.Errorf("an unset metadata mode must not be judged: %v", r)
		}
	}
}

// Both states that leave the legacy endpoints serving are reported,
// under one reason with the raw setting in a detail: the remedy is the
// same single edit, and "somebody turned them back on" is not a
// different fix from "nobody ever turned them off".
func TestClusterLegacyMetadataStates(t *testing.T) {
	res := checktest.Run(t, audit.ClusterCommand(clusterDeps(exposedCluster())))

	got := map[string]string{}
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "audit.legacy_metadata" {
			got[r["name"]] = r["disable_legacy_endpoints"]
		}
	}
	want := map[string]string{"legacy-pool": "enabled", "old-pool": "unset"}
	if len(got) != len(want) {
		t.Fatalf("legacy-metadata findings = %v, want %v (the disabled pool is silent)", got, want)
	}
	for pool, state := range want {
		if got[pool] != state {
			t.Errorf("pool %q reported %q, want %q", pool, got[pool], state)
		}
	}
}

// The control-plane claim is about what NARROWS the public endpoint,
// not about the endpoint existing: a public control plane behind an
// allow-list that means something is the ordinary GKE deployment and
// must be silent.
func TestClusterControlPlaneExposure(t *testing.T) {
	cases := []struct {
		name       string
		endpoint   string
		networks   cloud.AuthorizedNetworks
		wantReason string
		wantSev    string
	}{
		{
			name:     "private control plane",
			endpoint: "",
			networks: cloud.AuthorizedNetworks{},
		},
		{
			name:       "public with no allow-list",
			endpoint:   "203.0.113.10",
			networks:   cloud.AuthorizedNetworks{},
			wantReason: "PublicEndpointUnrestricted",
			wantSev:    emit.SeverityWarning,
		},
		{
			name:     "public behind a real allow-list",
			endpoint: "203.0.113.10",
			networks: cloud.AuthorizedNetworks{Enabled: true, CIDRs: []string{"198.51.100.0/24"}},
		},
		{
			name:       "allow-list that allows everything",
			endpoint:   "203.0.113.10",
			networks:   cloud.AuthorizedNetworks{Enabled: true, CIDRs: []string{"198.51.100.0/24", "0.0.0.0/0"}},
			wantReason: "AuthorizedNetworksAllowAll",
			wantSev:    emit.SeverityWarning,
		},
		{
			name:       "allow-list widened by the provider's own ranges",
			endpoint:   "203.0.113.10",
			networks:   cloud.AuthorizedNetworks{Enabled: true, CIDRs: []string{"198.51.100.0/24"}, GCPPublicCIDRs: true},
			wantReason: "AuthorizedNetworksAllowProviderCIDRs",
			wantSev:    emit.SeverityInfo,
		},
		{
			// 0.0.0.0/0 already admits every provider range, so the
			// two claims are one finding, not two overlapping ones.
			name:       "both at once reports the wider claim only",
			endpoint:   "203.0.113.10",
			networks:   cloud.AuthorizedNetworks{Enabled: true, CIDRs: []string{"0.0.0.0/0"}, GCPPublicCIDRs: true},
			wantReason: "AuthorizedNetworksAllowAll",
			wantSev:    emit.SeverityWarning,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := exposedCluster()
			cfg.NodePools = nil // isolate the control-plane claim
			cfg.PublicEndpoint = tc.endpoint
			cfg.AuthorizedNetworks = tc.networks

			res := checktest.Run(t, audit.ClusterCommand(clusterDeps(cfg)))
			var got []map[string]string
			for _, r := range findingLines(t, res.Stdout) {
				if r["kind"] == "audit.public_control_plane" {
					got = append(got, r)
				}
			}
			if tc.wantReason == "" {
				if len(got) != 0 {
					t.Fatalf("want silence, got:\n%s", res.Stdout)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("want one finding, got %d:\n%s", len(got), res.Stdout)
			}
			if got[0]["reason"] != tc.wantReason || got[0]["severity"] != tc.wantSev {
				t.Errorf("got reason=%q severity=%q, want %q/%q", got[0]["reason"], got[0]["severity"], tc.wantReason, tc.wantSev)
			}
			if got[0]["endpoint"] != tc.endpoint {
				t.Errorf("endpoint = %q, want %q", got[0]["endpoint"], tc.endpoint)
			}
		})
	}
}

// The §2 degradation path: no provider capability means one explicit
// record and scanned=0, never an empty report that reads as a clean
// cluster.
func TestClusterUnavailableIsExplicit(t *testing.T) {
	deps := audit.Deps{Provider: func(context.Context) (cloud.Provider, error) { return cloud.NoProvider, nil }}
	res := checktest.Run(t, audit.ClusterCommand(deps))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 1 || recs[0]["kind"] != "cloud.unavailable" || recs[0]["reason"] != "CapabilityUnavailable" {
		t.Fatalf("want one cloud.unavailable finding, got:\n%s", res.Stdout)
	}
	if recs[0]["capability"] != "cluster-config" || recs[0]["provider"] != "none" {
		t.Errorf("the record must name what was needed and who was asked: %v", recs[0])
	}
	if !strings.Contains(res.Stdout, `scanned=0 findings=1 `) ||
		!strings.Contains(res.Stdout, `unavailable="no cloud provider configured"`) {
		t.Errorf("want scanned=0 and the §2 summary marker, got:\n%s", res.Stdout)
	}
}

// A provider that has the capability but cannot serve it is an error,
// not an empty report: the difference between "nothing is wrong" and
// "we could not look" is the whole point of the group.
func TestClusterReadErrorFails(t *testing.T) {
	p := clusterConfigProvider{Provider: cloud.NoProvider, api: &fakeClusterConfig{err: errors.New("permission denied")}}
	deps := audit.Deps{Provider: func(context.Context) (cloud.Provider, error) { return p, nil }}
	res := checktest.Run(t, audit.ClusterCommand(deps))
	if res.Code == emit.ExitData {
		t.Fatalf("a failed cloud read must not exit as data:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stderr, "permission denied") {
		t.Errorf("stderr should carry the provider error, got: %s", res.Stderr)
	}
}

// The command reads the provider's record, not objects inside the
// cluster, so the §4.2 scoping flags are a usage error rather than a
// silent no-op.
func TestClusterScopeErrors(t *testing.T) {
	for _, args := range [][]string{
		{"--namespace=prod"},
		{"-A"},
		{"--workload=Deployment/prod/api"},
	} {
		res := checktest.Run(t, audit.ClusterCommand(clusterDeps(exposedCluster())), args...)
		if res.Code != emit.ExitUsage {
			t.Errorf("%v: exit %d, want usage error; stdout:\n%s", args, res.Code, res.Stdout)
		}
	}
}

// Posture fingerprints hash the class, not the instance: the same gap
// on two different clusters rolls up as one class across the fleet,
// which is what a fleet consumer counts.
func TestClusterFingerprintIsClassNotInstance(t *testing.T) {
	fps := map[string]string{}
	for _, name := range []string{"prod-east", "prod-west"} {
		cfg := exposedCluster()
		cfg.Name = name
		cfg.WorkloadIdentityPool = ""
		cfg.NodePools = nil
		cfg.PublicEndpoint = ""
		res := checktest.Run(t, audit.ClusterCommand(clusterDeps(cfg)))
		recs := findingLines(t, res.Stdout)
		if len(recs) != 1 {
			t.Fatalf("%s: want one finding, got:\n%s", name, res.Stdout)
		}
		fps[name] = recs[0]["fingerprint"]
	}
	if fps["prod-east"] == "" || fps["prod-east"] != fps["prod-west"] {
		t.Errorf("same claim on two clusters fingerprinted apart: %v", fps)
	}
}
