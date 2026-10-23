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

// The fakes are shared with cluster_test.go — both commands read the
// same capability — so this file adds only the upgrade-shaped fixture
// and the clock.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// upgradeNow anchors every exclusion window below. Exclusions are the
// one claim in this command that is about a moment.
var upgradeNow = time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

func upgradeDeps(api *fakeClusterConfig) audit.Deps {
	deps := providerFor(api)
	deps.Now = func() time.Time { return upgradeNow }
	return deps
}

// behindCluster is the shared fixture: a cluster on the regular channel
// that is a minor behind what the channel publishes, with one pool per
// shape the command has to tell apart.
//
//	current-pool  on the control plane's version, fully managed — clean
//	stale-pool    two minors back, auto-upgrade and auto-repair off
//	docker-pool   a pre-1.24 node image, and no management block at all
func behindCluster() *fakeClusterConfig {
	return &fakeClusterConfig{
		cfg: cloud.ClusterConfig{
			Name:                 "prod-east",
			Location:             "us-east1",
			Version:              "1.29.7-gke.1104000",
			ReleaseChannel:       "regular",
			UpgradeNotifications: cloud.ToggleEnabled,
			Maintenance: cloud.Maintenance{
				Scheduled: true,
				Exclusions: []cloud.MaintenanceExclusion{{
					Name:  "retail-freeze",
					Start: upgradeNow.Add(-30 * 24 * time.Hour),
					End:   upgradeNow.Add(60 * 24 * time.Hour),
					Scope: cloud.ExclusionScopeAll,
				}},
			},
			NodePools: []cloud.NodePoolConfig{
				{
					Name: "current-pool", Version: "1.29.7-gke.1104000",
					ImageType: "COS_CONTAINERD", NodeRuntime: cloud.NodeRuntimeContainerd,
					AutoUpgrade: cloud.ToggleEnabled, AutoRepair: cloud.ToggleEnabled,
				},
				{
					Name: "stale-pool", Version: "1.27.11-gke.1062000",
					ImageType: "COS_CONTAINERD", NodeRuntime: cloud.NodeRuntimeContainerd,
					AutoUpgrade: cloud.ToggleDisabled, AutoRepair: cloud.ToggleDisabled,
				},
				{
					Name: "docker-pool", Version: "1.29.7-gke.1104000",
					ImageType: "COS", NodeRuntime: cloud.NodeRuntimeDockershim,
				},
			},
		},
		targets: cloud.UpgradeTargets{
			Channel:              "regular",
			DefaultVersion:       "1.30.5-gke.1443001",
			UpgradeTargetVersion: "1.30.6-gke.1125000",
		},
	}
}

func TestUpgradesContract(t *testing.T) {
	checktest.VerifyContract(t, audit.UpgradesCommand(upgradeDeps(behindCluster())))
}

func TestUpgradesGolden(t *testing.T) {
	res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(behindCluster())))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/upgrades.golden", res.Stdout)
}

// The comparison is against THIS cluster's channel. Asking the provider
// for a default channel's versions would report a stable-channel
// cluster as behind for running exactly what stable publishes.
func TestUpgradesAsksForTheClustersOwnChannel(t *testing.T) {
	api := behindCluster()
	api.cfg.ReleaseChannel = "stable"
	checktest.Run(t, audit.UpgradesCommand(upgradeDeps(api)))
	if api.askedChannel != "stable" {
		t.Errorf("asked for channel %q, want the cluster's own", api.askedChannel)
	}
}

// A minor behind and a patch behind are different claims at different
// severities: one is a release line of fixes, the other is the patch
// stream this cluster has not taken.
func TestUpgradesControlPlaneDistance(t *testing.T) {
	cases := []struct {
		name       string
		version    string
		target     cloud.UpgradeTargets
		wantReason string
		wantSev    string
	}{
		{
			name:       "three minors behind the channel's target (unsupported)",
			version:    "1.28.5-gke.1000",
			target:     cloud.UpgradeTargets{DefaultVersion: "1.31.2-gke.1000"},
			wantReason: "MinorUnsupported",
			wantSev:    "critical",
		},
		{
			name:       "a minor behind the channel's target",
			version:    "1.29.7-gke.1104000",
			target:     cloud.UpgradeTargets{DefaultVersion: "1.30.6-gke.1125000"},
			wantReason: "ControlPlaneMinorBehind",
			wantSev:    "warning",
		},
		{
			name:       "same release line, older patch",
			version:    "1.30.5-gke.1443001",
			target:     cloud.UpgradeTargets{DefaultVersion: "1.30.6-gke.1125000"},
			wantReason: "ControlPlanePatchBehind",
			wantSev:    "info",
		},
		{
			// GKE ships most security patches as a build-number bump on
			// an unchanged 1.30.5; ignoring the suffix would report this
			// cluster as current.
			name:       "same patch, older provider build",
			version:    "1.30.5-gke.1000000",
			target:     cloud.UpgradeTargets{DefaultVersion: "1.30.5-gke.1443001"},
			wantReason: "ControlPlanePatchBehind",
			wantSev:    "info",
		},
		{
			name:    "current",
			version: "1.30.6-gke.1125000",
			target:  cloud.UpgradeTargets{DefaultVersion: "1.30.6-gke.1125000"},
		},
		{
			// Ahead of the published default happens on a cluster whose
			// channel was just changed. It is not a finding.
			name:    "ahead of what the channel publishes",
			version: "1.31.1-gke.1146000",
			target:  cloud.UpgradeTargets{DefaultVersion: "1.30.6-gke.1125000"},
		},
		{
			// The provider published nothing for this channel. No target,
			// no claim — a comparison against zero would report every
			// cluster as ahead or behind on nothing.
			name:    "no published target",
			version: "1.29.7-gke.1104000",
			target:  cloud.UpgradeTargets{},
		},
		{
			name:    "a version string this code cannot parse",
			version: "v1.29-custom",
			target:  cloud.UpgradeTargets{DefaultVersion: "1.30.6-gke.1125000"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := behindCluster()
			api.cfg.Version = tc.version
			api.cfg.NodePools = nil
			api.targets = tc.target
			res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(api)))

			var got []map[string]string
			for _, r := range findingLines(t, res.Stdout) {
				if r["kind"] == "audit.version_behind" {
					got = append(got, r)
				}
			}
			if tc.wantReason == "" {
				if len(got) != 0 {
					t.Fatalf("want no version finding, got:\n%s", res.Stdout)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("want one version finding, got %d:\n%s", len(got), res.Stdout)
			}
			if got[0]["reason"] != tc.wantReason || got[0]["severity"] != tc.wantSev {
				t.Errorf("got reason=%q severity=%q, want %q/%q", got[0]["reason"], got[0]["severity"], tc.wantReason, tc.wantSev)
			}
			if got[0]["kind_of_object"] != "Cluster" {
				t.Errorf("subject = %q, want the Cluster", got[0]["kind_of_object"])
			}
		})
	}
}

// The upgrade TARGET wins over the channel default: it is what the
// provider is actually moving existing clusters to, and a cluster
// sitting on the default mid-rollout is still behind.
func TestUpgradesPrefersTheRollingTargetOverTheDefault(t *testing.T) {
	api := behindCluster()
	api.cfg.Version = "1.30.5-gke.1443001"
	api.cfg.NodePools = nil
	api.targets = cloud.UpgradeTargets{
		DefaultVersion:       "1.30.5-gke.1443001",
		UpgradeTargetVersion: "1.30.6-gke.1125000",
	}
	res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(api)))

	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] != "audit.version_behind" {
			continue
		}
		if r["target_version"] != "1.30.6-gke.1125000" {
			t.Errorf("target_version = %q, want the channel's rolling target", r["target_version"])
		}
		return
	}
	t.Errorf("want a version finding against the rolling target:\n%s", res.Stdout)
}

// One minor behind is what a rolling upgrade looks like from the
// outside. Two is the supported limit, and the pool stops being a
// supported configuration the next time the control plane moves.
func TestUpgradesNodePoolSkew(t *testing.T) {
	cases := map[string]struct {
		pool string
		want bool
	}{
		"same version":         {"1.29.7-gke.1104000", false},
		"one minor behind":     {"1.28.9-gke.1000000", false},
		"two minors behind":    {"1.27.11-gke.1062000", true},
		"three minors behind":  {"1.26.15-gke.1000000", true},
		"ahead of the control": {"1.30.1-gke.1000000", false},
		"unparseable":          {"unknown", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			api := behindCluster()
			api.cfg.NodePools = []cloud.NodePoolConfig{{
				Name: "pool", Version: tc.pool,
				NodeRuntime: cloud.NodeRuntimeContainerd,
				AutoUpgrade: cloud.ToggleEnabled, AutoRepair: cloud.ToggleEnabled,
			}}
			res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(api)))

			var got map[string]string
			for _, r := range findingLines(t, res.Stdout) {
				if r["reason"] == "NodePoolVersionSkew" {
					got = r
				}
			}
			if tc.want != (got != nil) {
				t.Fatalf("skew finding = %v, want %v:\n%s", got != nil, tc.want, res.Stdout)
			}
			if got != nil && (got["kind_of_object"] != "NodePool" || got["control_plane_version"] != api.cfg.Version) {
				t.Errorf("the pool finding should name the pool and what it was measured against: %v", got)
			}
		})
	}
}

// An unset toggle is not a disabled one. The provider's default depends
// on how and when the pool was created, so a pool that states nothing
// gets no claim — the same discipline as the metadata mode in #186.
func TestUpgradesUnsetManagementMakesNoClaim(t *testing.T) {
	api := behindCluster()
	api.cfg.NodePools = []cloud.NodePoolConfig{{
		Name: "pool", Version: api.cfg.Version,
		NodeRuntime: cloud.NodeRuntimeContainerd,
		AutoUpgrade: cloud.ToggleUnset, AutoRepair: cloud.ToggleUnset,
	}}
	res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(api)))
	for _, r := range findingLines(t, res.Stdout) {
		if r["reason"] == "NodeAutoUpgradeOff" || r["reason"] == "NodeAutoRepairOff" {
			t.Errorf("an unset management toggle must not be judged: %v", r)
		}
	}
}

// Auto-upgrade and auto-repair are separate settings with separate
// remedies, so a pool with both off is two findings and not one.
func TestUpgradesManagementOffIsTwoClaims(t *testing.T) {
	res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(behindCluster())))

	got := map[string]string{}
	for _, r := range findingLines(t, res.Stdout) {
		switch r["reason"] {
		case "NodeAutoUpgradeOff", "NodeAutoRepairOff":
			got[r["reason"]] = r["name"]
		}
	}
	if got["NodeAutoUpgradeOff"] != "stale-pool" || got["NodeAutoRepairOff"] != "stale-pool" {
		t.Errorf("want both claims against stale-pool, got %v", got)
	}
}

// A cluster on no channel gets the claim AND is still compared against
// the version a new cluster would get today: "nothing upgrades this"
// and "this is old" are different remedies.
func TestUpgradesNoChannelIsItsOwnClaim(t *testing.T) {
	api := behindCluster()
	api.cfg.ReleaseChannel = ""
	api.cfg.NodePools = nil
	api.targets = cloud.UpgradeTargets{DefaultVersion: "1.30.6-gke.1125000"}
	res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(api)))

	reasons := map[string]map[string]string{}
	for _, r := range findingLines(t, res.Stdout) {
		reasons[r["reason"]] = r
	}
	if _, ok := reasons["NoReleaseChannel"]; !ok {
		t.Errorf("want the NoReleaseChannel claim:\n%s", res.Stdout)
	}
	behind, ok := reasons["ControlPlaneMinorBehind"]
	if !ok {
		t.Fatalf("want the version claim too:\n%s", res.Stdout)
	}
	if behind["channel"] != "none" {
		t.Errorf("channel = %q, want an explicit none so the record says what it compared against", behind["channel"])
	}
	if !strings.Contains(res.Stdout, "channel=none") {
		t.Errorf("want the channel note on the summary line:\n%s", res.Stdout)
	}
}

// Only an exclusion in force right now blocks anything. One that has
// not started is a plan and one that has ended is history; reporting
// either would make the finding uncheckable.
func TestUpgradesExclusionWindows(t *testing.T) {
	day := 24 * time.Hour
	cases := []struct {
		name       string
		ex         cloud.MaintenanceExclusion
		wantReason string
		wantSev    string
	}{
		{
			name: "active, blocking everything",
			ex: cloud.MaintenanceExclusion{
				Name: "freeze", Start: upgradeNow.Add(-day), End: upgradeNow.Add(10 * day),
				Scope: cloud.ExclusionScopeAll,
			},
			wantReason: "MaintenanceExclusionBlocksPatches",
			wantSev:    "warning",
		},
		{
			// Patches still flow, so this is posture and not a defect —
			// the same call #186 made for the provider-CIDR bypass.
			name: "active, minor upgrades only",
			ex: cloud.MaintenanceExclusion{
				Name: "hold", Start: upgradeNow.Add(-day), End: upgradeNow.Add(10 * day),
				Scope: cloud.ExclusionScopeMinor,
			},
			wantReason: "MaintenanceExclusionActive",
			wantSev:    "info",
		},
		{
			name: "active with no end date at all",
			ex: cloud.MaintenanceExclusion{
				Name: "migration", Start: upgradeNow.Add(-day),
				Scope: cloud.ExclusionScopeMinorAndNodes, UntilEndOfSupport: true,
			},
			wantReason: "MaintenanceExclusionActive",
			wantSev:    "info",
		},
		{
			name: "not started yet",
			ex: cloud.MaintenanceExclusion{
				Name: "planned", Start: upgradeNow.Add(day), End: upgradeNow.Add(10 * day),
				Scope: cloud.ExclusionScopeAll,
			},
		},
		{
			name: "already over",
			ex: cloud.MaintenanceExclusion{
				Name: "past", Start: upgradeNow.Add(-10 * day), End: upgradeNow.Add(-day),
				Scope: cloud.ExclusionScopeAll,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := behindCluster()
			api.cfg.NodePools = nil
			api.cfg.Maintenance.Exclusions = []cloud.MaintenanceExclusion{tc.ex}
			res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(api)))

			var got map[string]string
			for _, r := range findingLines(t, res.Stdout) {
				if r["kind"] == "audit.upgrade_blocked" {
					got = r
				}
			}
			if tc.wantReason == "" {
				if got != nil {
					t.Fatalf("want no exclusion finding, got: %v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("want an exclusion finding:\n%s", res.Stdout)
			}
			if got["reason"] != tc.wantReason || got["severity"] != tc.wantSev {
				t.Errorf("got reason=%q severity=%q, want %q/%q", got["reason"], got["severity"], tc.wantReason, tc.wantSev)
			}
			if got["exclusion"] != tc.ex.Name || got["scope"] != tc.ex.Scope {
				t.Errorf("the record must name the window and its scope: %v", got)
			}
			if tc.ex.UntilEndOfSupport {
				if got["ends"] != "end-of-support" || got["days_remaining"] != "" {
					t.Errorf("an open-ended exclusion has no end date to count down to: %v", got)
				}
			} else if got["days_remaining"] != "10" {
				t.Errorf("days_remaining = %q, want 10", got["days_remaining"])
			}
		})
	}
}

// A Docker node image is not a version gap that any upgrade setting
// closes: the pool cannot move past the last version carrying the shim
// until the image type changes.
func TestUpgradesStaleNodeImage(t *testing.T) {
	res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(behindCluster())))

	var got []map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		if r["reason"] == "StaleNodeImageType" {
			got = append(got, r)
		}
	}
	if len(got) != 1 {
		t.Fatalf("want one stale-image finding, for docker-pool, got %d:\n%s", len(got), res.Stdout)
	}
	if got[0]["name"] != "docker-pool" || got[0]["image_type"] != "COS" {
		t.Errorf("the finding must name the pool and the image an operator would change: %v", got[0])
	}
}

// An unset image type is a pool taking the provider's default, which
// this read cannot see. No claim.
func TestUpgradesUnsetImageTypeMakesNoClaim(t *testing.T) {
	api := behindCluster()
	api.cfg.NodePools = []cloud.NodePoolConfig{{
		Name: "pool", Version: api.cfg.Version, NodeRuntime: cloud.NodeRuntimeUnset,
		AutoUpgrade: cloud.ToggleEnabled, AutoRepair: cloud.ToggleEnabled,
	}}
	res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(api)))
	for _, r := range findingLines(t, res.Stdout) {
		if r["reason"] == "StaleNodeImageType" {
			t.Errorf("an unnamed image type must not be judged: %v", r)
		}
	}
}

// Both defaults-off settings are reported, at info, and both are about
// the cluster rather than any pool.
func TestUpgradesUnattendedClaims(t *testing.T) {
	api := behindCluster()
	api.cfg.NodePools = nil
	api.cfg.Maintenance = cloud.Maintenance{}
	api.cfg.UpgradeNotifications = cloud.ToggleDisabled
	res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(api)))

	got := map[string]string{}
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "audit.upgrade_unattended" {
			got[r["reason"]] = r["severity"]
		}
	}
	want := map[string]string{"NoMaintenanceWindow": "info", "NoUpgradeNotifications": "info"}
	if len(got) != len(want) {
		t.Fatalf("unattended findings = %v, want %v", got, want)
	}
	for reason, sev := range want {
		if got[reason] != sev {
			t.Errorf("%s severity = %q, want %q", reason, got[reason], sev)
		}
	}
}

// A configured window and published notifications are silent: the
// group reports gaps, not inventory.
func TestUpgradesGovernedClusterIsSilentOnAttention(t *testing.T) {
	api := behindCluster()
	api.cfg.NodePools = nil
	api.cfg.Maintenance.Exclusions = nil
	res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(api)))
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "audit.upgrade_unattended" {
			t.Errorf("a scheduled, subscribed cluster must be silent here: %v", r)
		}
	}
}

// scanned counts the cluster as well as its pools: the control plane's
// version and every governance setting belong to the cluster.
func TestUpgradesScannedCountsTheClusterAndItsPools(t *testing.T) {
	res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(behindCluster())))
	if !strings.Contains(res.Stdout, "\nscanned=4 findings=6 elapsed=100ms channel=regular node_pools=3\n") {
		t.Errorf("want scanned=4 (1 cluster + 3 node pools) with both notes, got:\n%s", res.Stdout)
	}
}

// The §2 degradation path, the same record `audit cluster` emits: no
// capability means one explicit finding and scanned=0.
func TestUpgradesUnavailableIsExplicit(t *testing.T) {
	deps := audit.Deps{Provider: func(context.Context) (cloud.Provider, error) { return cloud.NoProvider, nil }}
	res := checktest.Run(t, audit.UpgradesCommand(deps))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 1 || recs[0]["kind"] != "cloud.unavailable" || recs[0]["capability"] != "cluster-config" {
		t.Fatalf("want one cloud.unavailable finding naming the capability, got:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, `scanned=0 findings=1 `) {
		t.Errorf("want scanned=0, got:\n%s", res.Stdout)
	}
}

// The published-versions read is a second API call, and its failure is
// an error rather than a version comparison quietly dropped.
func TestUpgradesTargetsReadErrorFails(t *testing.T) {
	api := behindCluster()
	api.targetsErr = errors.New("permission denied on serverConfig")
	res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(api)))
	if res.Code == emit.ExitData {
		t.Fatalf("a failed cloud read must not exit as data:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stderr, "permission denied on serverConfig") {
		t.Errorf("stderr should carry the provider error, got: %s", res.Stderr)
	}
}

// The command reads the provider's record, not objects inside the
// cluster, so the §4.2 scoping flags are a usage error.
func TestUpgradesScopeErrors(t *testing.T) {
	for _, args := range [][]string{
		{"--namespace=prod"},
		{"-A"},
		{"--workload=Deployment/prod/api"},
	} {
		res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(behindCluster())), args...)
		if res.Code != emit.ExitUsage {
			t.Errorf("%v: exit %d, want a usage error", args, res.Code)
		}
	}
}

// The posture fingerprint is the incident CLASS: two clusters with the
// same gap collapse to one rolled-up finding, so it must not carry the
// cluster or pool name.
func TestUpgradesFingerprintIsClassNotInstance(t *testing.T) {
	first := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(behindCluster())))

	other := behindCluster()
	other.cfg.Name = "prod-west"
	other.cfg.Location = "us-west1"
	for i := range other.cfg.NodePools {
		other.cfg.NodePools[i].Name += "-b"
	}
	second := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(other)))

	got := map[string]string{}
	for _, r := range findingLines(t, first.Stdout) {
		got[r["reason"]] = r["fingerprint"]
	}
	for _, r := range findingLines(t, second.Stdout) {
		if want := got[r["reason"]]; r["fingerprint"] != want {
			t.Errorf("%s fingerprint %q != %q: the class must not carry the subject", r["reason"], r["fingerprint"], want)
		}
	}
}

func upgradeDepsWithKube(api *fakeClusterConfig, objs ...runtime.Object) audit.Deps {
	deps := upgradeDeps(api)
	client := fake.NewClientset(objs...)
	deps.Client = func(context.Context) (kubernetes.Interface, error) { return client, nil }
	return deps
}

func TestUpgradesTargetVersionFlag(t *testing.T) {
	t.Run("overrides provider target with 2-part major.minor", func(t *testing.T) {
		api := behindCluster()
		api.cfg.Version = "1.29.7-gke.1104000"
		api.cfg.NodePools = nil
		res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(api)), "--target-version=1.32")
		if res.Code != emit.ExitData {
			t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
		}
		var got map[string]string
		for _, r := range findingLines(t, res.Stdout) {
			if r["kind"] == "audit.version_behind" {
				got = r
			}
		}
		if got == nil {
			t.Fatalf("want version_behind finding:\n%s", res.Stdout)
		}
		if got["reason"] != "MinorUnsupported" || got["severity"] != "critical" || got["target_version"] != "1.32" || got["minor_versions_behind"] != "3" {
			t.Errorf("unexpected finding: %v", got)
		}
	})

	t.Run("works without cloud provider when --target-version is set", func(t *testing.T) {
		ing := &networkingv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod",
				Name:      "legacy-ing",
				Annotations: map[string]string{
					"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"networking.k8s.io/v1beta1","kind":"Ingress"}`,
				},
			},
		}
		client := fake.NewClientset(ing)
		deps := audit.Deps{
			Client:   func(context.Context) (kubernetes.Interface, error) { return client, nil },
			Provider: func(context.Context) (cloud.Provider, error) { return cloud.NoProvider, nil },
		}
		res := checktest.Run(t, audit.UpgradesCommand(deps), "--target-version=1.32")
		if res.Code != emit.ExitData {
			t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
		}
		reasons := map[string]bool{}
		for _, r := range findingLines(t, res.Stdout) {
			reasons[r["reason"]] = true
		}
		if !reasons["RemovedInTargetMinor"] || !reasons["RemovedAPIInUse"] {
			t.Errorf("want RemovedInTargetMinor and RemovedAPIInUse, got %v\nstdout:\n%s", reasons, res.Stdout)
		}
	})

	t.Run("rejects invalid --target-version", func(t *testing.T) {
		res := checktest.Run(t, audit.UpgradesCommand(upgradeDeps(behindCluster())), "--target-version=not-a-version")
		if res.Code != emit.ExitUsage {
			t.Errorf("exit %d, want usage error", res.Code)
		}
	})
}

func TestUpgradesDeprecatedAndRemovedAPIs(t *testing.T) {
	objs := []runtime.Object{
		&networkingv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod",
				Name:      "old-ing",
				Annotations: map[string]string{
					"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"networking.k8s.io/v1beta1","kind":"Ingress"}`,
				},
			},
		},
		&policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod",
				Name:      "old-pdb",
				Annotations: map[string]string{
					"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"policy/v1beta1","kind":"PodDisruptionBudget"}`,
				},
			},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod",
				Name:      "flowschema-holder",
				Annotations: map[string]string{
					"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"flowcontrol.apiserver.k8s.io/v1beta3","kind":"FlowSchema"}`,
				},
			},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod",
				Name:      "clean-deploy",
				Annotations: map[string]string{
					"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"apps/v1","kind":"Deployment"}`,
				},
			},
		},
	}

	t.Run("at 1.30 flowcontrol v1beta3 is deprecated while 1.22 and 1.25 APIs are removed", func(t *testing.T) {
		api := behindCluster()
		api.cfg.NodePools = nil
		api.cfg.Maintenance.Exclusions = nil
		res := checktest.Run(t, audit.UpgradesCommand(upgradeDepsWithKube(api, objs...)), "--target-version=1.30")
		if res.Code != emit.ExitData {
			t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
		}

		byNameReason := map[string]map[string]string{}
		for _, r := range findingLines(t, res.Stdout) {
			byNameReason[r["name"]+"/"+r["reason"]] = r
		}

		if r, ok := byNameReason["old-ing/RemovedInTargetMinor"]; !ok || r["severity"] != "critical" || r["removed_in"] != "1.22" || r["replacement"] != "networking.k8s.io/v1" {
			t.Errorf("old-ing RemovedInTargetMinor = %v", r)
		}
		if r, ok := byNameReason["old-ing/RemovedAPIInUse"]; !ok || r["kind"] != "audit.upgrade_blocked" || r["severity"] != "critical" {
			t.Errorf("old-ing RemovedAPIInUse = %v", r)
		}
		if r, ok := byNameReason["old-pdb/RemovedInTargetMinor"]; !ok || r["removed_in"] != "1.25" || r["replacement"] != "policy/v1" {
			t.Errorf("old-pdb RemovedInTargetMinor = %v", r)
		}
		if r, ok := byNameReason["flowschema-holder/DeprecatedInTargetMinor"]; !ok || r["severity"] != "warning" || r["removed_in"] != "1.32" {
			t.Errorf("flowschema-holder DeprecatedInTargetMinor = %v", r)
		}
		if _, ok := byNameReason["flowschema-holder/RemovedAPIInUse"]; ok {
			t.Errorf("flowschema-holder must not be RemovedAPIInUse at 1.30")
		}
	})

	t.Run("at 1.32 flowcontrol v1beta3 becomes RemovedInTargetMinor and RemovedAPIInUse", func(t *testing.T) {
		api := behindCluster()
		api.cfg.NodePools = nil
		api.cfg.Maintenance.Exclusions = nil
		res := checktest.Run(t, audit.UpgradesCommand(upgradeDepsWithKube(api, objs...)), "--target-version=1.32")
		if res.Code != emit.ExitData {
			t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
		}

		byNameReason := map[string]map[string]string{}
		for _, r := range findingLines(t, res.Stdout) {
			byNameReason[r["name"]+"/"+r["reason"]] = r
		}
		if r, ok := byNameReason["flowschema-holder/RemovedInTargetMinor"]; !ok || r["severity"] != "critical" {
			t.Errorf("want flowschema-holder RemovedInTargetMinor at 1.32, got %v", r)
		}
		if r, ok := byNameReason["flowschema-holder/RemovedAPIInUse"]; !ok || r["severity"] != "critical" {
			t.Errorf("want flowschema-holder RemovedAPIInUse at 1.32, got %v", r)
		}
	})
}

func TestUpgradesWebhookPathGroups(t *testing.T) {
	fail := admissionregistrationv1.Fail
	ignore := admissionregistrationv1.Ignore
	shortTimeout := int32(5)
	longTimeout := int32(25)

	groups := []struct {
		name     string
		group    string
		resource string
		want     string
	}{
		{name: "drain-eviction", group: "", resource: "pods/eviction", want: "drain-eviction"},
		{name: "pod-creation", group: "", resource: "pods", want: "pod-creation"},
		{name: "node-lease", group: "coordination.k8s.io", resource: "leases", want: "node-lease"},
		{name: "kubelet-csr", group: "certificates.k8s.io", resource: "certificatesigningrequests", want: "kubelet-csr"},
		{name: "csi-storage", group: "storage.k8s.io", resource: "volumeattachments", want: "csi-storage"},
		{name: "control-plane-rbac", group: "rbac.authorization.k8s.io", resource: "clusterrolebindings", want: "control-plane-rbac"},
	}

	for _, tc := range groups {
		t.Run("dead service on "+tc.name, func(t *testing.T) {
			vwc := &admissionregistrationv1.ValidatingWebhookConfiguration{
				ObjectMeta: metav1.ObjectMeta{Name: "vwc-" + tc.name},
				Webhooks: []admissionregistrationv1.ValidatingWebhook{{
					Name:           "hook.example.com",
					FailurePolicy:  &fail,
					TimeoutSeconds: &shortTimeout,
					ClientConfig: admissionregistrationv1.WebhookClientConfig{
						Service: &admissionregistrationv1.ServiceReference{
							Namespace: "webhook-sys",
							Name:      "missing-svc",
						},
					},
					Rules: []admissionregistrationv1.RuleWithOperations{{
						Rule: admissionregistrationv1.Rule{
							APIGroups:   []string{tc.group},
							APIVersions: []string{"v1"},
							Resources:   []string{tc.resource},
						},
					}},
				}},
			}
			api := behindCluster()
			api.cfg.NodePools = nil
			api.cfg.Maintenance.Exclusions = nil
			res := checktest.Run(t, audit.UpgradesCommand(upgradeDepsWithKube(api, vwc)))
			if res.Code != emit.ExitData {
				t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
			}

			var got map[string]string
			for _, r := range findingLines(t, res.Stdout) {
				if r["reason"] == "WebhookBlocksUpgrade" {
					got = r
				}
			}
			if got == nil {
				t.Fatalf("want WebhookBlocksUpgrade for %s:\n%s", tc.name, res.Stdout)
			}
			if got["severity"] != "critical" || got["webhook_group"] != tc.want || got["service"] != "webhook-sys/missing-svc" {
				t.Errorf("unexpected webhook finding: %v", got)
			}
		})
	}

	t.Run("single-replica backing service and long timeout", func(t *testing.T) {
		one := int32(1)
		sel := map[string]string{"app": "admit"}
		svcObj := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: "webhook-sys", Name: "admit-svc"},
			Spec:       corev1.ServiceSpec{Selector: sel},
		}
		depObj := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "webhook-sys", Name: "admit"},
			Spec: appsv1.DeploymentSpec{
				Replicas: &one,
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: sel}},
			},
		}
		mwc := &admissionregistrationv1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: "mwc-pods"},
			Webhooks: []admissionregistrationv1.MutatingWebhook{
				{
					Name:           "mutate.example.com",
					FailurePolicy:  &fail,
					TimeoutSeconds: &longTimeout,
					ClientConfig: admissionregistrationv1.WebhookClientConfig{
						Service: &admissionregistrationv1.ServiceReference{
							Namespace: "webhook-sys",
							Name:      "admit-svc",
						},
					},
					Rules: []admissionregistrationv1.RuleWithOperations{{
						Rule: admissionregistrationv1.Rule{
							APIGroups:   []string{""},
							APIVersions: []string{"v1"},
							Resources:   []string{"pods"},
						},
					}},
				},
				{
					Name:          "ignored.example.com",
					FailurePolicy: &ignore,
					Rules: []admissionregistrationv1.RuleWithOperations{{
						Rule: admissionregistrationv1.Rule{
							APIGroups:   []string{""},
							APIVersions: []string{"v1"},
							Resources:   []string{"pods/eviction"},
						},
					}},
				},
			},
		}
		api := behindCluster()
		api.cfg.NodePools = nil
		api.cfg.Maintenance.Exclusions = nil
		res := checktest.Run(t, audit.UpgradesCommand(upgradeDepsWithKube(api, svcObj, depObj, mwc)))
		if res.Code != emit.ExitData {
			t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
		}

		reasons := map[string]map[string]string{}
		for _, r := range findingLines(t, res.Stdout) {
			reasons[r["reason"]] = r
		}
		if r, ok := reasons["WebhookTargetsSingleReplica"]; !ok || r["severity"] != "warning" || r["replicas"] != "1" || r["webhook_group"] != "pod-creation" {
			t.Errorf("want WebhookTargetsSingleReplica, got %v", r)
		}
		if r, ok := reasons["ControlPlaneWebhookNoTimeout"]; !ok || r["severity"] != "warning" || r["timeout_seconds"] != "25" {
			t.Errorf("want ControlPlaneWebhookNoTimeout with timeout_seconds=25, got %v", r)
		}
	})
}

func TestUpgradesPDBBlocksNodeDrain(t *testing.T) {
	one := int32(1)
	two := int32(2)
	lblOne := map[string]string{"app": "singleton"}
	lblTwo := map[string]string{"app": "pair"}
	lblHealthy := map[string]string{"app": "healthy-pair"}

	depOne := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "singleton"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &one,
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: lblOne}},
		},
	}
	depTwo := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "pair"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &two,
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: lblTwo}},
		},
	}
	depHealthy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "healthy-pair"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &two,
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: lblHealthy}},
		},
	}

	zero := intstr.FromInt32(0)
	pct50 := intstr.FromString("50%")
	pct80 := intstr.FromString("80%")
	pct100 := intstr.FromString("100%")

	pdbs := []runtime.Object{
		depOne, depTwo, depHealthy,
		// 1. maxUnavailable = 0
		&policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "pdb-max-zero"},
			Spec:       policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &zero},
		},
		// 2. minAvailable = 100%
		&policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "pdb-min-100pct"},
			Spec:       policyv1.PodDisruptionBudgetSpec{MinAvailable: &pct100},
		},
		// 3. minAvailable = 50% on 1 replica -> ceil(0.5) = 1 >= 1 (blocks drain)
		&policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "pdb-50pct-on-1"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &pct50,
				Selector:     &metav1.LabelSelector{MatchLabels: lblOne},
			},
		},
		// 4. minAvailable = 80% on 2 replicas -> ceil(1.6) = 2 >= 2 (blocks drain)
		&policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "pdb-80pct-on-2"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &pct80,
				Selector:     &metav1.LabelSelector{MatchLabels: lblTwo},
			},
		},
		// 5. minAvailable = 50% on 2 replicas -> ceil(1.0) = 1 < 2 (allows 1 disruption -> silent)
		&policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "pdb-50pct-on-2-ok"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &pct50,
				Selector:     &metav1.LabelSelector{MatchLabels: lblHealthy},
			},
			Status: policyv1.PodDisruptionBudgetStatus{CurrentHealthy: 2, DisruptionsAllowed: 1},
		},
		// 6. live status disruptionsAllowed == 0 with currentHealthy > 0
		&policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "pdb-live-zero"},
			Status:     policyv1.PodDisruptionBudgetStatus{CurrentHealthy: 2, DisruptionsAllowed: 0},
		},
	}

	api := behindCluster()
	api.cfg.NodePools = nil
	api.cfg.Maintenance.Exclusions = nil
	res := checktest.Run(t, audit.UpgradesCommand(upgradeDepsWithKube(api, pdbs...)))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}

	blockedPDBs := map[string]map[string]string{}
	for _, r := range findingLines(t, res.Stdout) {
		if r["reason"] == "PDBBlocksNodeDrain" {
			blockedPDBs[r["name"]] = r
		}
	}

	for _, wantName := range []string{"pdb-max-zero", "pdb-min-100pct", "pdb-50pct-on-1", "pdb-80pct-on-2", "pdb-live-zero"} {
		if _, ok := blockedPDBs[wantName]; !ok {
			t.Errorf("missing PDBBlocksNodeDrain for %q; got %v\nstdout:\n%s", wantName, blockedPDBs, res.Stdout)
		}
	}
	if _, ok := blockedPDBs["pdb-50pct-on-2-ok"]; ok {
		t.Errorf("pdb-50pct-on-2-ok allows 1 disruption and must not be flagged: %v", blockedPDBs["pdb-50pct-on-2-ok"])
	}
	if blockedPDBs["pdb-50pct-on-1"]["replicas"] != "1" || blockedPDBs["pdb-80pct-on-2"]["replicas"] != "2" {
		t.Errorf("expected replicas detail on percentage-rounded PDBs: 50pct=%v 80pct=%v",
			blockedPDBs["pdb-50pct-on-1"], blockedPDBs["pdb-80pct-on-2"])
	}
}
