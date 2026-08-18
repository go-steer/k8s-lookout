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

package audit

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// The upgrade-readiness kinds (issue #187), grouped by the question an
// operator is answering rather than by which API field they read:
// how far behind the cluster is, whether anything is going to fix
// that on its own, what is standing in the way, and whether anyone
// will notice when it happens.
const (
	kindVersionBehind     = "audit.version_behind"
	kindUpgradeUnmanaged  = "audit.upgrade_unmanaged"
	kindUpgradeBlocked    = "audit.upgrade_blocked"
	kindUpgradeUnattended = "audit.upgrade_unattended"
)

const (
	reasonControlPlaneMinorBehind = "ControlPlaneMinorBehind"
	reasonControlPlanePatchBehind = "ControlPlanePatchBehind"
	reasonNodePoolVersionSkew     = "NodePoolVersionSkew"
	reasonNoReleaseChannel        = "NoReleaseChannel"
	reasonAutoUpgradeOff          = "NodeAutoUpgradeOff"
	reasonAutoRepairOff           = "NodeAutoRepairOff"
	reasonExclusionBlocksPatches  = "MaintenanceExclusionBlocksPatches"
	reasonExclusionActive         = "MaintenanceExclusionActive"
	reasonStaleNodeImage          = "StaleNodeImageType"
	reasonNoMaintenanceWindow     = "NoMaintenanceWindow"
	reasonNoNotifications         = "NoUpgradeNotifications"
)

// supportedMinorSkew is how many minor versions a node pool may trail
// its control plane before the provider stops supporting the pair. At
// the limit the pool is still running; one more control-plane minor
// and it is not, which is why the claim is made AT it rather than past
// it.
const supportedMinorSkew = 2

// UpgradesCommand builds `lookout audit upgrades`: the patch-readiness
// half of the posture group (#187) — whether this cluster is current,
// whether anything keeps it current, and what would stop it.
//
// # The claim is readiness, not currency
//
// A cluster one patch behind is not a finding worth waking anyone for;
// a cluster that is current today with auto-upgrade off, no release
// channel and a nine-month maintenance exclusion is the one that will
// be a year behind when the next CVE lands. Six of this command's ten
// reasons are about the MECHANISM rather than the version, which is
// the whole point of putting them in the posture group: they are all
// true while nothing is wrong.
//
// # Versions are compared against what the provider publishes
//
// "Behind" needs a target, and the target is not a constant — it is the
// version the provider is currently moving clusters on this channel to,
// which is why the read is a second API call and not a corner of the
// cluster record. A cluster on no channel is compared against the
// version a new cluster would get today, and separately reported as
// having no channel; those are two different remedies.
//
// A version this code cannot parse produces no claim at all. Provider
// version strings are not semver (GKE's build suffix is where most
// security patches actually land, so it is compared, not discarded),
// and inventing an ordering for a format nobody has seen would report
// a cluster as behind on the strength of a string compare.
//
// # Node pools are judged at the supported skew, not at any difference
//
// Every cluster mid-upgrade has pools one minor behind the control
// plane; that is what a rolling upgrade looks like from the outside,
// and reporting it would make the check fire on the fleet doing
// exactly the right thing. Two minors behind is the supported limit —
// the pool works until the control plane advances once more, and then
// it does not.
func UpgradesCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "audit upgrades",
		MCPName: "k8s_audit_upgrades",
		Summary: "Upgrade and patch readiness, read from the cloud provider: how far the control plane and its node pools are behind what the provider publishes, and whether anything is set up to close that gap on its own — release channel, node auto-upgrade and auto-repair, a maintenance window, active maintenance exclusions, node images on the removed Docker runtime, and upgrade notifications. Reads the provider's cluster record, not Kubernetes objects, so it takes no --namespace/-A/--workload; scanned counts the cluster plus its node pools. Without a provider capability it reports an explicit unavailable rather than silence.",
		Kinds: []checks.KindField{
			checks.Kind(kindVersionBehind, "the control plane or a node pool is behind what the provider publishes, or a node pool has skewed from the control plane; info while the gap is still within the supported skew", emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind(kindUpgradeUnmanaged, "nothing will close that gap on its own: no release channel, or node auto-upgrade/auto-repair off", emit.SeverityWarning),
			checks.Kind(kindUpgradeBlocked, "an active maintenance exclusion, or a node image on the removed Docker runtime, will stop the upgrade when it comes", emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind(kindUpgradeUnattended, "upgrades will happen with nobody watching: no maintenance window, or no upgrade notifications", emit.SeverityInfo),
			checks.CloudUnavailableKind(),
		},
		Output: []checks.OutputField{
			{Name: "cluster", Doc: "on a node-pool finding: the cluster the pool belongs to, so the record stands alone"},
			{Name: "version", Doc: "the current version of the finding's subject — the control plane's, or the node pool's"},
			{Name: "target_version", Doc: "the version the provider would move this cluster to: its channel's upgrade target where one is published, otherwise the channel's default"},
			{Name: "control_plane_version", Doc: "on a node-pool skew finding: the control-plane version the pool is measured against"},
			{Name: "minor_versions_behind", Doc: "how many minor releases separate the two versions"},
			{Name: "channel", Doc: "the release channel the cluster is subscribed to, and the one whose published versions the comparison used; `none` when it is subscribed to no channel"},
			{Name: "image_type", Doc: "the provider's name for the node image the pool runs"},
			{Name: "exclusion", Doc: "the operator's name for the maintenance exclusion currently in force"},
			{Name: "scope", Doc: "how much of the upgrade stream that exclusion holds back: all-upgrades, minor-upgrades or minor-and-node-upgrades"},
			{Name: "ends", Doc: "when the exclusion lifts, or `end-of-support` for one that runs until the cluster's version leaves support"},
			{Name: "days_remaining", Doc: "how much longer the exclusion has left to run"},
			{Name: "node_pools", Doc: "summary note: node pools examined — the cluster itself is the other unit `scanned` counts"},
			{Name: "capability", Doc: "cloud.unavailable: the provider capability this command needed (" + string(cloud.CapabilityClusterConfig) + ")"},
			{Name: "provider", Doc: "cloud.unavailable: the provider that was asked"},
			{Name: "unavailable", Doc: "summary-line note (§2 marker): why the cloud read could not be served"},
		},
		Examples: []string{
			"lookout audit upgrades",
			"lookout audit upgrades --format=json",
			"lookout audit upgrades --exemptions=exemptions.yaml",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runUpgrades(ctx, deps, inv)
		},
	}
}

func runUpgrades(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if inv.Scope.Namespace != "" || inv.Scope.AllNamespaces || !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("audit upgrades reads the provider's cluster record, not objects inside the cluster: --namespace/-A/--workload do not apply")
	}

	provider, err := deps.provider(ctx)
	if err != nil {
		return 0, err
	}
	api, ok := provider.ClusterConfig()
	if !ok {
		return emitUnavailable(inv, provider, cloud.CapabilityClusterConfig, "audit upgrades")
	}
	cfg, err := api.Config(ctx)
	if err != nil {
		return 0, fmt.Errorf("cluster configuration: %w", err)
	}
	targets, err := api.UpgradeTargets(ctx, cfg.ReleaseChannel)
	if err != nil {
		return 0, fmt.Errorf("published versions: %w", err)
	}

	findings := append(versionFindings(cfg, targets), upgradeMechanismFindings(cfg)...)
	findings = append(findings, upgradeBlockedFindings(cfg, deps.now())...)
	findings = append(findings, upgradeUnattendedFindings(cfg)...)
	sortFindings(findings)
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	if err := inv.Out.Note("channel", channelName(cfg.ReleaseChannel)); err != nil {
		return 0, err
	}
	if err := inv.Out.Note("node_pools", itoa(len(cfg.NodePools))); err != nil {
		return 0, err
	}
	// The cluster is a scanned subject in its own right: the control
	// plane's version and every governance setting are its, not any
	// pool's.
	return 1 + len(cfg.NodePools), nil
}

// versionFindings compares the control plane against what the provider
// publishes, and the node pools against the control plane. Anything
// unparseable on either side of a comparison makes no claim.
func versionFindings(cfg cloud.ClusterConfig, targets cloud.UpgradeTargets) []emit.Finding {
	cp, cpOK := parseVersion(cfg.Version)
	var out []emit.Finding

	target := targets.UpgradeTargetVersion
	if target == "" {
		target = targets.DefaultVersion
	}
	if tv, ok := parseVersion(target); cpOK && ok {
		details := []emit.Field{
			{Key: "version", Value: cfg.Version},
			{Key: "target_version", Value: target},
			{Key: "channel", Value: channelName(cfg.ReleaseChannel)},
		}
		switch {
		case olderMinor(cp, tv):
			if gap, ok := minorGap(cp, tv); ok {
				details = append(details, emit.Field{Key: "minor_versions_behind", Value: itoa(gap)})
			}
			out = append(out, clusterFinding(cfg, emit.Finding{
				Kind:     kindVersionBehind,
				Severity: emit.SeverityWarning,
				Reason:   reasonControlPlaneMinorBehind,
				Message: fmt.Sprintf("the control plane runs %s and the provider publishes %s for this cluster: a whole release line of fixes has not been applied, and every minor the cluster falls behind is one the provider will eventually stop supporting it on",
					cfg.Version, target),
				Details: details,
			}))
		case older(cp, tv):
			out = append(out, clusterFinding(cfg, emit.Finding{
				Kind:     kindVersionBehind,
				Severity: emit.SeverityInfo,
				Reason:   reasonControlPlanePatchBehind,
				Message: fmt.Sprintf("the control plane runs %s and the provider publishes %s on the same release line: this is where the security patches land, and the gap is the set of them this cluster has not taken",
					cfg.Version, target),
				Details: details,
			}))
		}
	}

	if !cpOK {
		return out
	}
	for _, np := range cfg.NodePools {
		pv, ok := parseVersion(np.Version)
		if !ok || !beyondSkew(pv, cp) {
			continue
		}
		details := []emit.Field{
			{Key: "version", Value: np.Version},
			{Key: "control_plane_version", Value: cfg.Version},
		}
		if gap, ok := minorGap(pv, cp); ok {
			details = append(details, emit.Field{Key: "minor_versions_behind", Value: itoa(gap)})
		}
		out = append(out, nodePoolFinding(cfg, np, emit.Finding{
			Kind:     kindVersionBehind,
			Severity: emit.SeverityWarning,
			Reason:   reasonNodePoolVersionSkew,
			Message: fmt.Sprintf("the node pool runs %s against a %s control plane, at the %d-minor version skew the provider supports: the pool works today and stops being a supported configuration the next time the control plane advances, which on a channel-subscribed cluster is not something anyone here schedules",
				np.Version, cfg.Version, supportedMinorSkew),
			Details: details,
		}))
	}
	return out
}

// upgradeMechanismFindings judges what is supposed to keep the cluster
// current: the channel for the control plane, the pool's own
// management for its nodes.
func upgradeMechanismFindings(cfg cloud.ClusterConfig) []emit.Finding {
	var out []emit.Finding
	if cfg.ReleaseChannel == "" {
		out = append(out, clusterFinding(cfg, emit.Finding{
			Kind:     kindUpgradeUnmanaged,
			Severity: emit.SeverityWarning,
			Reason:   reasonNoReleaseChannel,
			Message:  "the cluster is subscribed to no release channel: the provider will not move the control plane to a patched version on its own, so every CVE in the API server, scheduler and controller manager waits for somebody here to notice it and act",
			Details:  []emit.Field{{Key: "version", Value: cfg.Version}},
		}))
	}
	for _, np := range cfg.NodePools {
		if np.AutoUpgrade == cloud.ToggleDisabled {
			out = append(out, nodePoolFinding(cfg, np, emit.Finding{
				Kind:     kindUpgradeUnmanaged,
				Severity: emit.SeverityWarning,
				Reason:   reasonAutoUpgradeOff,
				Message:  "node auto-upgrade is off for this pool: its nodes stay on the version they were created with until a human recreates them, so the node OS, the kubelet and the container runtime accumulate every fix nobody applies by hand",
				Details:  []emit.Field{{Key: "version", Value: np.Version}},
			}))
		}
		if np.AutoRepair == cloud.ToggleDisabled {
			out = append(out, nodePoolFinding(cfg, np, emit.Finding{
				Kind:     kindUpgradeUnmanaged,
				Severity: emit.SeverityWarning,
				Reason:   reasonAutoRepairOff,
				Message:  "node auto-repair is off for this pool: a node that stops reporting healthy stays in the pool as it is, which is how a half-finished upgrade leaves one broken node behind and nothing takes it out",
			}))
		}
	}
	return out
}

// upgradeBlockedFindings judges what actively stands in the way of an
// upgrade that would otherwise happen: an exclusion in force right now,
// and a node image the current runtime replaced.
func upgradeBlockedFindings(cfg cloud.ClusterConfig, now time.Time) []emit.Finding {
	var out []emit.Finding
	for _, ex := range cfg.Maintenance.Exclusions {
		if !exclusionInForce(ex, now) {
			continue
		}
		details := []emit.Field{
			{Key: "exclusion", Value: ex.Name},
			{Key: "scope", Value: ex.Scope},
			{Key: "ends", Value: exclusionEnds(ex)},
		}
		if !ex.UntilEndOfSupport && !ex.End.IsZero() {
			details = append(details, emit.Field{
				Key:   "days_remaining",
				Value: itoa(int(ex.End.Sub(now).Hours() / 24)),
			})
		}
		f := emit.Finding{
			Kind:     kindUpgradeBlocked,
			Severity: emit.SeverityInfo,
			Reason:   reasonExclusionActive,
			Message: fmt.Sprintf("maintenance exclusion %q is in force and holds back %s until %s: patches still flow, but the cluster does not move release lines while it stands, and an exclusion nobody removes is the ordinary way a cluster ends up years behind",
				ex.Name, ex.Scope, exclusionEnds(ex)),
			Details: details,
		}
		if ex.Scope == cloud.ExclusionScopeAll {
			f.Severity = emit.SeverityWarning
			f.Reason = reasonExclusionBlocksPatches
			f.Message = fmt.Sprintf("maintenance exclusion %q is in force and blocks ALL upgrades until %s, security patches included: for as long as it stands the provider will not fix this cluster even when it has the fix",
				ex.Name, exclusionEnds(ex))
		}
		out = append(out, clusterFinding(cfg, f))
	}
	for _, np := range cfg.NodePools {
		if np.NodeRuntime != cloud.NodeRuntimeDockershim {
			continue
		}
		out = append(out, nodePoolFinding(cfg, np, emit.Finding{
			Kind:     kindUpgradeBlocked,
			Severity: emit.SeverityWarning,
			Reason:   reasonStaleNodeImage,
			Message: fmt.Sprintf("the pool runs the %s node image, which is built around the Docker runtime Kubernetes removed in 1.24: the pool cannot be upgraded past the last version that still carried the shim, so it is not a version gap that any upgrade setting will close — the image type has to change",
				np.ImageType),
			Details: []emit.Field{
				{Key: "image_type", Value: np.ImageType},
				{Key: "version", Value: np.Version},
			},
		}))
	}
	return out
}

// upgradeUnattendedFindings judges the two settings that decide when an
// upgrade happens and who hears about it. Both are provider defaults,
// so both are info: true, worth knowing, and not on their own a defect.
func upgradeUnattendedFindings(cfg cloud.ClusterConfig) []emit.Finding {
	var out []emit.Finding
	if !cfg.Maintenance.Scheduled {
		out = append(out, clusterFinding(cfg, emit.Finding{
			Kind:     kindUpgradeUnattended,
			Severity: emit.SeverityInfo,
			Reason:   reasonNoMaintenanceWindow,
			Message:  "no maintenance window is configured: the provider may start an upgrade at any hour of any day, including the hours this cluster is busiest and the ones nobody is on call for. The upgrade itself is wanted — the absence of a window is what makes its timing somebody else's choice",
		}))
	}
	if cfg.UpgradeNotifications != cloud.ToggleEnabled {
		out = append(out, clusterFinding(cfg, emit.Finding{
			Kind:     kindUpgradeUnattended,
			Severity: emit.SeverityInfo,
			Reason:   reasonNoNotifications,
			Message:  "upgrade notifications are not published anywhere: nothing tells this team that the control plane moved, that a new version is available, or that a security bulletin applies to the cluster, so the first sign of an upgrade is its consequences",
		}))
	}
	return out
}

// exclusionInForce reports whether the exclusion is holding upgrades
// back at now. An exclusion that runs until the cluster's version
// leaves support has no end date to compare against; one that has not
// started yet is a plan, not a blocker.
func exclusionInForce(ex cloud.MaintenanceExclusion, now time.Time) bool {
	if !ex.Start.IsZero() && now.Before(ex.Start) {
		return false
	}
	if ex.UntilEndOfSupport {
		return true
	}
	return !ex.End.IsZero() && now.Before(ex.End)
}

func exclusionEnds(ex cloud.MaintenanceExclusion) string {
	if ex.UntilEndOfSupport {
		return "end-of-support"
	}
	return ex.End.UTC().Format(time.RFC3339)
}

// channelName renders the release channel for output. A cluster on no
// channel gets an explicit "none" rather than an empty value, so the
// record says which comparison was made.
func channelName(channel string) string {
	if channel == "" {
		return "none"
	}
	return channel
}

// version is a provider version string parsed far enough to order two
// of them. GKE's format is Kubernetes' major.minor.patch with a
// provider build suffix ("1.30.5-gke.1443001"), and the suffix is not
// cosmetic: a GKE security patch usually moves only the build number,
// so discarding it would report a cluster months behind as current.
type version struct {
	major, minor, patch, build int
}

// parseVersion reads major.minor.patch with an optional trailing
// provider build number. Anything else fails, and a failed parse makes
// no claim at all rather than falling back to a string comparison.
func parseVersion(s string) (version, bool) {
	core, suffix, _ := strings.Cut(s, "-")
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return version{}, false
	}
	var v version
	for i, into := range []*int{&v.major, &v.minor, &v.patch} {
		n, err := strconv.Atoi(parts[i])
		if err != nil || n < 0 {
			return version{}, false
		}
		*into = n
	}
	if _, build, ok := strings.Cut(suffix, "."); ok {
		// A build number that will not parse leaves the three numbers
		// intact: the release line is still comparable, and only the
		// patch-level tie-break is lost.
		if n, err := strconv.Atoi(build); err == nil {
			v.build = n
		}
	}
	return v, true
}

// older reports whether v precedes target outright.
func older(v, target version) bool {
	if v.major != target.major {
		return v.major < target.major
	}
	if v.minor != target.minor {
		return v.minor < target.minor
	}
	if v.patch != target.patch {
		return v.patch < target.patch
	}
	return v.build < target.build
}

// olderMinor reports whether v is on an earlier release line than
// target — a different claim from being behind on it.
func olderMinor(v, target version) bool {
	return v.major < target.major || (v.major == target.major && v.minor < target.minor)
}

// minorGap counts the release lines between two versions, and reports
// false across a major boundary, where the count would not mean
// anything. The claim survives without it; the detail field does not
// have to be invented.
func minorGap(v, target version) (int, bool) {
	if v.major != target.major {
		return 0, false
	}
	return target.minor - v.minor, true
}

// beyondSkew reports whether a node pool has reached the version skew
// the provider supports against its control plane.
func beyondSkew(pool, controlPlane version) bool {
	if pool.major != controlPlane.major {
		return pool.major < controlPlane.major
	}
	return controlPlane.minor-pool.minor >= supportedMinorSkew
}
