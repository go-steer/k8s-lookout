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

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
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
	reasonMinorUnsupported             = "MinorUnsupported"
	reasonControlPlaneMinorBehind      = "ControlPlaneMinorBehind"
	reasonControlPlanePatchBehind      = "ControlPlanePatchBehind"
	reasonNodePoolVersionSkew          = "NodePoolVersionSkew"
	reasonNoReleaseChannel             = "NoReleaseChannel"
	reasonAutoUpgradeOff               = "NodeAutoUpgradeOff"
	reasonAutoRepairOff                = "NodeAutoRepairOff"
	reasonExclusionBlocksPatches       = "MaintenanceExclusionBlocksPatches"
	reasonExclusionActive              = "MaintenanceExclusionActive"
	reasonStaleNodeImage               = "StaleNodeImageType"
	reasonNoMaintenanceWindow          = "NoMaintenanceWindow"
	reasonNoNotifications              = "NoUpgradeNotifications"
	reasonWebhookBlocksUpgrade         = "WebhookBlocksUpgrade"
	reasonControlPlaneWebhookNoTimeout = "ControlPlaneWebhookNoTimeout"
	reasonWebhookTargetsSingleReplica  = "WebhookTargetsSingleReplica"
	reasonPDBBlocksNodeDrain           = "PDBBlocksNodeDrain"
)

// Upgrade-critical admission webhook path groups (issue #791, L-65).
const (
	webhookGroupDrainEviction    = "drain-eviction"
	webhookGroupPodCreation      = "pod-creation"
	webhookGroupNodeLease        = "node-lease"
	webhookGroupKubeletCSR       = "kubelet-csr"
	webhookGroupCSIStorage       = "csi-storage"
	webhookGroupControlPlaneRBAC = "control-plane-rbac"
)

// supportedMinorSkew is how many minor versions a node pool may trail
// its control plane before the provider stops supporting the pair. At
// the limit the pool is still running; one more control-plane minor
// and it is not, which is why the claim is made AT it rather than past
// it.
const supportedMinorSkew = 2

// unsupportedMinorGap is the control-plane minor-version distance (3 or
// more minors behind target_version) at which the cluster's release line
// is past the standard 3-minor GKE/Kubernetes support window.
const unsupportedMinorGap = 3

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
		Name:        "audit upgrades",
		MCPName:     "k8s_audit_upgrades",
		MCPProfiles: []string{"audit"},
		Summary:     "Upgrade and patch readiness, read from the cloud provider and cluster upgrade-blocker catalogue: how far the control plane and its node pools are behind what the provider publishes (or --target-version), whether anything is set up to close that gap on its own — release channel, node auto-upgrade and auto-repair, a maintenance window, active maintenance exclusions, node images on the removed Docker runtime, and upgrade notifications — plus in-cluster upgrade blockers (deprecated/removed APIs in use, fail-closed admission webhooks on upgrade-critical paths, and PodDisruptionBudgets that allow zero disruptions during node drain). Without a provider capability or --target-version it reports an explicit unavailable rather than silence.",
		Flags: append([]emit.FlagSpec{
			{
				Name: "target-version",
				Type: emit.FlagString,
				Help: "target Kubernetes version (e.g. 1.32 or 1.32.1-gke.1000) to evaluate upgrade readiness and API removals against, overriding the provider's published target",
			},
		}, PolicyFlags()...),
		Kinds: []checks.KindField{
			checks.Kind(kindVersionBehind, "the control plane or a node pool is behind what the provider publishes, or a node pool has skewed from the control plane; info while the gap is still within the supported skew", emit.SeverityCritical, emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind(kindUpgradeUnmanaged, "nothing will close that gap on its own: no release channel, or node auto-upgrade/auto-repair off", emit.SeverityWarning),
			checks.Kind(kindUpgradeBlocked, "an active maintenance exclusion, a node image on the removed Docker runtime, a removed API in use, a fail-closed upgrade-path webhook, or a zero-disruption PDB will stop or stall the upgrade when it comes", emit.SeverityCritical, emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind(kindDeprecatedAPIInUse, "a cluster resource was last applied with a Kubernetes API version that is deprecated or removed in the target minor release", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind(kindUpgradeUnattended, "upgrades will happen with nobody watching: no maintenance window, or no upgrade notifications", emit.SeverityInfo),
			checks.CloudUnavailableKind(),
		},
		Output: []checks.OutputField{
			{Name: "cluster", Doc: "on a node-pool finding: the cluster the pool belongs to, so the record stands alone"},
			{Name: "version", Doc: "the current version of the finding's subject — the control plane's, or the node pool's"},
			{Name: "target_version", Doc: "the version the provider would move this cluster to: its channel's upgrade target where one is published, otherwise the channel's default (or --target-version)"},
			{Name: "control_plane_version", Doc: "on a node-pool skew finding: the control-plane version the pool is measured against"},
			{Name: "minor_versions_behind", Doc: "how many minor releases separate the two versions"},
			{Name: "channel", Doc: "the release channel the cluster is subscribed to, and the one whose published versions the comparison used; `none` when it is subscribed to no channel"},
			{Name: "image_type", Doc: "the provider's name for the node image the pool runs"},
			{Name: "exclusion", Doc: "the operator's name for the maintenance exclusion currently in force"},
			{Name: "scope", Doc: "how much of the upgrade stream that exclusion holds back: all-upgrades, minor-upgrades or minor-and-node-upgrades"},
			{Name: "ends", Doc: "when the exclusion lifts, or `end-of-support` for one that runs until the cluster's version leaves support"},
			{Name: "days_remaining", Doc: "how much longer the exclusion has left to run"},
			{Name: "api_version", Doc: "on a deprecated-API finding: the deprecated or removed group/version in use"},
			{Name: "removed_in", Doc: "on a deprecated-API finding: the Kubernetes major.minor release that removes the API"},
			{Name: "replacement", Doc: "on a deprecated-API finding: the successor group/version to migrate to"},
			{Name: "webhook", Doc: "on a webhook upgrade-blocker finding: the individual webhook entry within the configuration"},
			{Name: "webhook_group", Doc: "on a webhook upgrade-blocker finding: the upgrade-critical operation group intercepted (drain-eviction, pod-creation, node-lease, kubelet-csr, csi-storage, control-plane-rbac)"},
			{Name: "service", Doc: "on a webhook upgrade-blocker finding: the namespace/name of the backing Service"},
			{Name: "timeout_seconds", Doc: "on a webhook timeout finding: the configured webhook timeout in seconds, or default(10) when unset"},
			{Name: "replicas", Doc: "on a single-replica webhook or PDB finding: the replica count of the backing or protected workload"},
			{Name: "min_available", Doc: "on a PDB upgrade-blocker finding: the configured minAvailable threshold"},
			{Name: "max_unavailable", Doc: "on a PDB upgrade-blocker finding: the configured maxUnavailable threshold"},
			{Name: "disruptions_allowed", Doc: "on a PDB upgrade-blocker finding: the live disruptionsAllowed count"},
			{Name: "node_pools", Doc: "summary note: node pools examined — the cluster itself is the other unit `scanned` counts"},
			{Name: "capability", Doc: "cloud.unavailable: the provider capability this command needed (" + string(cloud.CapabilityClusterConfig) + ")"},
			{Name: "provider", Doc: "cloud.unavailable: the provider that was asked"},
			{Name: "unavailable", Doc: "summary-line note (§2 marker): why the cloud read could not be served"},
		},
		Examples: []string{
			"lookout audit upgrades",
			"lookout audit upgrades --target-version=1.32",
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
	if _, err := bindPolicy(inv); err != nil {
		return 0, err
	}

	targetFlag := strings.TrimSpace(inv.Flags.String("target-version"))
	if targetFlag != "" {
		if _, ok := parseTargetVersion(targetFlag); !ok {
			return 0, emit.UsageErrorf("invalid --target-version %q: expected major.minor or major.minor.patch", targetFlag)
		}
	}

	provider, err := deps.provider(ctx)
	if err != nil {
		return 0, err
	}
	api, ok := provider.ClusterConfig()
	if !ok {
		return runUpgradesWithoutCloud(ctx, deps, inv, provider, targetFlag)
	}
	cfg, err := api.Config(ctx)
	if handled, err := checks.DegradeCredentials(ctx, inv.Out, provider.Name(), cloud.CapabilityClusterConfig, "audit upgrades", err); handled {
		return 0, err
	}
	if err != nil {
		return 0, fmt.Errorf("cluster configuration: %w", err)
	}
	targets, err := api.UpgradeTargets(ctx, cfg.ReleaseChannel)
	if handled, err := checks.DegradeCredentials(ctx, inv.Out, provider.Name(), cloud.CapabilityClusterConfig, "audit upgrades", err); handled {
		return 0, err
	}
	if err != nil {
		return 0, fmt.Errorf("published versions: %w", err)
	}
	if targetFlag != "" {
		targets.UpgradeTargetVersion = targetFlag
	}

	findings := append(versionFindings(cfg, targets), upgradeMechanismFindings(cfg)...)
	findings = append(findings, upgradeBlockedFindings(cfg, deps.now())...)
	findings = append(findings, upgradeUnattendedFindings(cfg)...)

	if deps.Client != nil {
		if client, err := deps.client(ctx); err == nil && client != nil {
			snap := snapshotAnyScope(ctx, client)
			effTarget := targets.UpgradeTargetVersion
			if effTarget == "" {
				effTarget = targets.DefaultVersion
			}
			if tv, ok := parseTargetVersion(effTarget); ok {
				findings = append(findings, deprecatedAPIFindings(ctx, snap, effTarget, tv)...)
			}
			findings = append(findings, webhookUpgradeFindings(ctx, snap)...)
			findings = append(findings, pdbUpgradeFindings(ctx, snap)...)
		}
	}

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

func runUpgradesWithoutCloud(ctx context.Context, deps Deps, inv emit.Invocation, provider cloud.Provider, targetFlag string) (int, error) {
	if targetFlag == "" {
		return emitUnavailable(inv, provider, cloud.CapabilityClusterConfig, "audit upgrades")
	}
	client, err := deps.client(ctx)
	if err != nil || client == nil {
		return emitUnavailable(inv, provider, cloud.CapabilityClusterConfig, "audit upgrades")
	}
	snap := snapshotAnyScope(ctx, client)

	var findings []emit.Finding
	cfg := cloud.ClusterConfig{Name: "cluster"}
	if sv, err := client.Discovery().ServerVersion(); err == nil && sv != nil {
		cfg.Version = normalizeServerVersion(sv.GitVersion, sv.Major, sv.Minor)
	}
	targets := cloud.UpgradeTargets{UpgradeTargetVersion: targetFlag}
	findings = append(findings, versionFindings(cfg, targets)...)
	if tv, ok := parseTargetVersion(targetFlag); ok {
		findings = append(findings, deprecatedAPIFindings(ctx, snap, targetFlag, tv)...)
	}
	findings = append(findings, webhookUpgradeFindings(ctx, snap)...)
	findings = append(findings, pdbUpgradeFindings(ctx, snap)...)

	sortFindings(findings)
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	if err := inv.Out.Note("channel", "none"); err != nil {
		return 0, err
	}
	if err := inv.Out.Note("node_pools", "0"); err != nil {
		return 0, err
	}
	return 1, nil
}

func normalizeServerVersion(gitVersion, major, minor string) string {
	trimmed := strings.TrimPrefix(strings.TrimSpace(gitVersion), "v")
	if _, ok := parseVersion(trimmed); ok {
		return trimmed
	}
	maj := strings.TrimSpace(major)
	min := strings.TrimRight(strings.TrimSpace(minor), "+")
	if maj != "" && min != "" {
		candidate := maj + "." + min + ".0"
		if _, ok := parseVersion(candidate); ok {
			return candidate
		}
	}
	return ""
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
	if tv, ok := parseTargetVersion(target); cpOK && ok {
		details := []emit.Field{
			{Key: "version", Value: cfg.Version},
			{Key: "target_version", Value: target},
			{Key: "channel", Value: channelName(cfg.ReleaseChannel)},
		}
		switch {
		case olderMinor(cp, tv):
			gap, hasGap := minorGap(cp, tv)
			if hasGap {
				details = append(details, emit.Field{Key: "minor_versions_behind", Value: itoa(gap)})
			}
			if (hasGap && gap >= unsupportedMinorGap) || cp.major < tv.major {
				out = append(out, clusterFinding(cfg, emit.Finding{
					Kind:     kindVersionBehind,
					Severity: emit.SeverityCritical,
					Reason:   reasonMinorUnsupported,
					Message: fmt.Sprintf("the control plane runs %s and the provider publishes %s for this cluster: at 3 or more minor versions behind, this release line is past the standard support window and no longer receives security fixes",
						cfg.Version, target),
					Details: details,
				}))
			} else {
				out = append(out, clusterFinding(cfg, emit.Finding{
					Kind:     kindVersionBehind,
					Severity: emit.SeverityWarning,
					Reason:   reasonControlPlaneMinorBehind,
					Message: fmt.Sprintf("the control plane runs %s and the provider publishes %s for this cluster: a whole release line of fixes has not been applied, and every minor the cluster falls behind is one the provider will eventually stop supporting it on",
						cfg.Version, target),
					Details: details,
				}))
			}
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

// webhookUpgradeFindings inspects ValidatingWebhookConfigurations and
// MutatingWebhookConfigurations for fail-closed hooks that intercept one
// of the 6 upgrade-critical operation groups (drain-eviction, pod-creation,
// node-lease, kubelet-csr, csi-storage, control-plane-rbac).
func webhookUpgradeFindings(ctx context.Context, snap *Snapshot) []emit.Finding {
	if snap == nil {
		return nil
	}
	var out []emit.Finding

	if vwcs, _, err := snap.ValidatingWebhookConfigurations(ctx); err == nil {
		for _, vwc := range vwcs {
			for _, w := range vwc.Webhooks {
				out = append(out, evaluateUpgradeWebhook(ctx, snap, "ValidatingWebhookConfiguration", vwc.Name,
					w.Name, w.FailurePolicy, w.TimeoutSeconds, w.NamespaceSelector, w.ClientConfig, w.Rules)...)
			}
		}
	}
	if mwcs, _, err := snap.MutatingWebhookConfigurations(ctx); err == nil {
		for _, mwc := range mwcs {
			for _, w := range mwc.Webhooks {
				out = append(out, evaluateUpgradeWebhook(ctx, snap, "MutatingWebhookConfiguration", mwc.Name,
					w.Name, w.FailurePolicy, w.TimeoutSeconds, w.NamespaceSelector, w.ClientConfig, w.Rules)...)
			}
		}
	}
	return out
}

func evaluateUpgradeWebhook(
	ctx context.Context,
	snap *Snapshot,
	configKind, configName, webhookName string,
	failurePolicy *admissionregistrationv1.FailurePolicyType,
	timeoutSeconds *int32,
	nsSelector *metav1.LabelSelector,
	clientConfig admissionregistrationv1.WebhookClientConfig,
	rules []admissionregistrationv1.RuleWithOperations,
) []emit.Finding {
	if failurePolicy != nil && *failurePolicy == admissionregistrationv1.Ignore {
		return nil
	}
	group := classifyWebhookUpgradeGroup(rules, nsSelector)
	if group == "" {
		return nil
	}

	var out []emit.Finding
	if clientConfig.Service != nil {
		svcRef := clientConfig.Service
		svcFullName := svcRef.Namespace + "/" + svcRef.Name
		hasBacking, replicas := resolveWebhookServiceBacking(ctx, snap, svcRef.Namespace, svcRef.Name)
		if !hasBacking {
			out = append(out, emit.Finding{
				Kind:         kindUpgradeBlocked,
				Severity:     emit.SeverityCritical,
				KindOfObject: configKind,
				Name:         configName,
				Reason:       reasonWebhookBlocksUpgrade,
				Message: fmt.Sprintf("fail-closed webhook %q in %s %s intercepts %s and points to Service %s with zero ready backing endpoints: node drain and control-plane operations in this path will fail during an upgrade",
					webhookName, configKind, configName, group, svcFullName),
				Details: []emit.Field{
					{Key: "webhook", Value: webhookName},
					{Key: "webhook_group", Value: group},
					{Key: "service", Value: svcFullName},
				},
				Fingerprint: engine.PostureFingerprint(kindUpgradeBlocked, reasonWebhookBlocksUpgrade, configKind),
			})
		} else if replicas == 1 {
			out = append(out, emit.Finding{
				Kind:         kindUpgradeBlocked,
				Severity:     emit.SeverityWarning,
				KindOfObject: configKind,
				Name:         configName,
				Reason:       reasonWebhookTargetsSingleReplica,
				Message: fmt.Sprintf("fail-closed webhook %q in %s %s intercepts %s and is backed by single-replica Service %s: draining the node hosting that single pod deadlocks the webhook during a rolling node upgrade",
					webhookName, configKind, configName, group, svcFullName),
				Details: []emit.Field{
					{Key: "webhook", Value: webhookName},
					{Key: "webhook_group", Value: group},
					{Key: "service", Value: svcFullName},
					{Key: "replicas", Value: "1"},
				},
				Fingerprint: engine.PostureFingerprint(kindUpgradeBlocked, reasonWebhookTargetsSingleReplica, configKind),
			})
		}
	}

	if (timeoutSeconds != nil && *timeoutSeconds > 10) || (timeoutSeconds == nil && len(out) == 0) {
		toStr := "default(10)"
		if timeoutSeconds != nil {
			toStr = itoa(int(*timeoutSeconds))
		}
		out = append(out, emit.Finding{
			Kind:         kindUpgradeBlocked,
			Severity:     emit.SeverityWarning,
			KindOfObject: configKind,
			Name:         configName,
			Reason:       reasonControlPlaneWebhookNoTimeout,
			Message: fmt.Sprintf("fail-closed webhook %q in %s %s intercepts %s with timeout %s seconds: control-plane admission hooks should set an explicit short timeoutSeconds (<=10s) so a slow webhook does not stall upgrade operations",
				webhookName, configKind, configName, group, toStr),
			Details: []emit.Field{
				{Key: "webhook", Value: webhookName},
				{Key: "webhook_group", Value: group},
				{Key: "timeout_seconds", Value: toStr},
			},
			Fingerprint: engine.PostureFingerprint(kindUpgradeBlocked, reasonControlPlaneWebhookNoTimeout, configKind),
		})
	}

	return out
}

func classifyWebhookUpgradeGroup(rules []admissionregistrationv1.RuleWithOperations, nsSelector *metav1.LabelSelector) string {
	kubeSystemExcluded := excludesKubeSystem(nsSelector)

	// Pass 1: explicit resource matches before wildcard resources.
	for _, r := range rules {
		if ruleHasGroup(r, "") && ruleHasExplicitResource(r, "pods/eviction", "pods/*") {
			return webhookGroupDrainEviction
		}
		if ruleHasGroup(r, "") && ruleHasExplicitResource(r, "pods", "serviceaccounts/token", "serviceaccounts/*") {
			return webhookGroupPodCreation
		}
		if (ruleHasGroup(r, "") && ruleHasExplicitResource(r, "nodes", "nodes/status", "nodes/*")) ||
			(ruleHasGroup(r, "coordination.k8s.io") && ruleHasExplicitResource(r, "leases", "leases/*")) {
			return webhookGroupNodeLease
		}
		if ruleHasGroup(r, "certificates.k8s.io") && ruleHasExplicitResource(r,
			"certificatesigningrequests", "certificatesigningrequests/approval", "certificatesigningrequests/status", "certificatesigningrequests/*") {
			return webhookGroupKubeletCSR
		}
		if (ruleHasGroup(r, "storage.k8s.io") && ruleHasExplicitResource(r, "csinodes", "volumeattachments", "csinodes/*", "volumeattachments/*")) ||
			(ruleHasGroup(r, "") && ruleHasExplicitResource(r, "persistentvolumes", "persistentvolumes/*")) {
			return webhookGroupCSIStorage
		}
		if !kubeSystemExcluded && ruleHasGroup(r, "rbac.authorization.k8s.io") && ruleHasExplicitResource(r,
			"roles", "rolebindings", "clusterroles", "clusterrolebindings") {
			return webhookGroupControlPlaneRBAC
		}
	}

	// Pass 2: wildcard resource ("*") scoped to an upgrade-critical API group.
	for _, r := range rules {
		if !ruleHasExplicitResource(r, "*") {
			continue
		}
		if ruleHasExplicitGroup(r, "coordination.k8s.io") {
			return webhookGroupNodeLease
		}
		if ruleHasExplicitGroup(r, "certificates.k8s.io") {
			return webhookGroupKubeletCSR
		}
		if ruleHasExplicitGroup(r, "storage.k8s.io") {
			return webhookGroupCSIStorage
		}
		if !kubeSystemExcluded && ruleHasExplicitGroup(r, "rbac.authorization.k8s.io") {
			return webhookGroupControlPlaneRBAC
		}
		if ruleHasGroup(r, "") {
			return webhookGroupDrainEviction
		}
	}
	return ""
}

func ruleHasGroup(r admissionregistrationv1.RuleWithOperations, want string) bool {
	for _, g := range r.APIGroups {
		if g == want || g == "*" {
			return true
		}
	}
	return false
}

func ruleHasExplicitGroup(r admissionregistrationv1.RuleWithOperations, want string) bool {
	for _, g := range r.APIGroups {
		if g == want {
			return true
		}
	}
	return false
}

func ruleHasExplicitResource(r admissionregistrationv1.RuleWithOperations, wants ...string) bool {
	for _, res := range r.Resources {
		for _, w := range wants {
			if res == w {
				return true
			}
		}
	}
	return false
}

func excludesKubeSystem(sel *metav1.LabelSelector) bool {
	if sel == nil {
		return false
	}
	for k, v := range sel.MatchLabels {
		if (k == "kubernetes.io/metadata.name" || k == "name") && v != "kube-system" && v != "kube-public" && v != "kube-node-lease" {
			return true
		}
	}
	for _, expr := range sel.MatchExpressions {
		if expr.Operator == metav1.LabelSelectorOpNotIn {
			for _, v := range expr.Values {
				if v == "kube-system" {
					return true
				}
			}
		}
	}
	return false
}

func resolveWebhookServiceBacking(ctx context.Context, snap *Snapshot, namespace, name string) (bool, int) {
	services, _, err := snap.Services(ctx)
	if err != nil {
		return true, 2
	}
	var targetSvc *corev1.Service
	for _, svc := range services {
		if svc.Namespace == namespace && svc.Name == name {
			targetSvc = svc
			break
		}
	}
	if targetSvc == nil {
		return false, 0
	}

	if eps, _, err := snap.Endpoints(ctx); err == nil {
		for _, ep := range eps {
			if ep.Namespace == namespace && ep.Name == name {
				ready := 0
				for _, sub := range ep.Subsets {
					ready += len(sub.Addresses)
				}
				if ready == 0 {
					return false, 0
				}
				return true, ready
			}
		}
	}

	if len(targetSvc.Spec.Selector) == 0 {
		return false, 0
	}
	sel := labels.SelectorFromSet(targetSvc.Spec.Selector)

	workloadReplicas := 0
	if deps, _, err := snap.Deployments(ctx); err == nil {
		for _, d := range deps {
			if d.Namespace != namespace {
				continue
			}
			if sel.Matches(labels.Set(d.Spec.Template.Labels)) {
				r := 1
				if d.Spec.Replicas != nil {
					r = int(*d.Spec.Replicas)
				}
				workloadReplicas += r
			}
		}
	}
	if sts, _, err := snap.StatefulSets(ctx); err == nil {
		for _, st := range sts {
			if st.Namespace != namespace {
				continue
			}
			if sel.Matches(labels.Set(st.Spec.Template.Labels)) {
				r := 1
				if st.Spec.Replicas != nil {
					r = int(*st.Spec.Replicas)
				}
				workloadReplicas += r
			}
		}
	}
	if dss, _, err := snap.DaemonSets(ctx); err == nil {
		for _, ds := range dss {
			if ds.Namespace != namespace {
				continue
			}
			if sel.Matches(labels.Set(ds.Spec.Template.Labels)) {
				r := int(ds.Status.DesiredNumberScheduled)
				if r <= 0 {
					r = 2
				}
				workloadReplicas += r
			}
		}
	}

	podCount := 0
	if pods, _, err := snap.Pods(ctx); err == nil {
		for _, p := range pods {
			if p.Namespace != namespace || p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded {
				continue
			}
			if sel.Matches(labels.Set(p.Labels)) {
				podCount++
			}
		}
	}

	count := workloadReplicas
	if podCount > count {
		count = podCount
	}
	if count <= 0 {
		return false, 0
	}
	return true, count
}

// pdbUpgradeFindings inspects PodDisruptionBudgets for configurations that
// permit zero voluntary evictions during a node drain: maxUnavailable=0/0%,
// minAvailable=100%, percentage minAvailable where ceil(pct*replicas/100) >= replicas,
// integer minAvailable >= replicas, or live disruptionsAllowed==0 with currentHealthy>0.
func pdbUpgradeFindings(ctx context.Context, snap *Snapshot) []emit.Finding {
	if snap == nil {
		return nil
	}
	pdbs, _, err := snap.PDBs(ctx)
	if err != nil || len(pdbs) == 0 {
		return nil
	}
	deployments, _, _ := snap.Deployments(ctx)
	statefulsets, _, _ := snap.StatefulSets(ctx)

	var out []emit.Finding
	for _, pdb := range pdbs {
		if f, blocked := evaluateBlockingPDB(pdb, deployments, statefulsets); blocked {
			out = append(out, f)
		}
	}
	return out
}

func evaluateBlockingPDB(
	pdb policyv1.PodDisruptionBudget,
	deployments []appsv1.Deployment,
	statefulsets []appsv1.StatefulSet,
) (emit.Finding, bool) {
	makeFinding := func(msg string, details ...emit.Field) (emit.Finding, bool) {
		return emit.Finding{
			Kind:         kindUpgradeBlocked,
			Severity:     emit.SeverityCritical,
			KindOfObject: "PodDisruptionBudget",
			Namespace:    pdb.Namespace,
			Name:         pdb.Name,
			Reason:       reasonPDBBlocksNodeDrain,
			Message:      msg,
			Details:      details,
			Fingerprint:  engine.PostureFingerprint(kindUpgradeBlocked, reasonPDBBlocksNodeDrain, "PodDisruptionBudget"),
		}, true
	}

	if mu := pdb.Spec.MaxUnavailable; mu != nil {
		switch mu.Type {
		case intstr.Int:
			if mu.IntVal <= 0 {
				return makeFinding(
					fmt.Sprintf("PodDisruptionBudget %s/%s sets maxUnavailable=0: zero voluntary evictions are permitted, so node drain during an upgrade will block until the drain timeout expires",
						pdb.Namespace, pdb.Name),
					emit.Field{Key: "max_unavailable", Value: itoa(int(mu.IntVal))},
				)
			}
		case intstr.String:
			s := strings.TrimSpace(mu.StrVal)
			if pct, ok := parsePercent(s); ok && pct <= 0 {
				return makeFinding(
					fmt.Sprintf("PodDisruptionBudget %s/%s sets maxUnavailable=%s: zero voluntary evictions are permitted, so node drain during an upgrade will block",
						pdb.Namespace, pdb.Name, s),
					emit.Field{Key: "max_unavailable", Value: s},
				)
			}
		}
	}

	replicas := matchedWorkloadReplicas(pdb, deployments, statefulsets)

	if ma := pdb.Spec.MinAvailable; ma != nil {
		switch ma.Type {
		case intstr.String:
			s := strings.TrimSpace(ma.StrVal)
			if pct, ok := parsePercent(s); ok {
				if pct >= 100 {
					details := []emit.Field{{Key: "min_available", Value: s}}
					if len(replicas) > 0 {
						details = append(details, emit.Field{Key: "replicas", Value: itoa(replicas[0])})
					}
					return makeFinding(
						fmt.Sprintf("PodDisruptionBudget %s/%s sets minAvailable=%s: every pod must remain available, blocking node drain during an upgrade",
							pdb.Namespace, pdb.Name, s),
						details...,
					)
				}
				if pct > 0 {
					for _, rep := range replicas {
						if rep <= 0 {
							continue
						}
						required := (pct*rep + 99) / 100
						if required >= rep {
							return makeFinding(
								fmt.Sprintf("PodDisruptionBudget %s/%s sets minAvailable=%s against a %d-replica workload (ceil(%d%% of %d) = %d): Kubernetes rounds percentage minAvailable up, leaving 0 allowed disruptions during node drain",
									pdb.Namespace, pdb.Name, s, rep, pct, rep, required),
								emit.Field{Key: "min_available", Value: s},
								emit.Field{Key: "replicas", Value: itoa(rep)},
							)
						}
					}
				}
			}
		case intstr.Int:
			if ma.IntVal > 0 {
				for _, rep := range replicas {
					if rep > 0 && int(ma.IntVal) >= rep {
						return makeFinding(
							fmt.Sprintf("PodDisruptionBudget %s/%s sets minAvailable=%d against a %d-replica workload: zero pods can be evicted during node drain",
								pdb.Namespace, pdb.Name, ma.IntVal, rep),
							emit.Field{Key: "min_available", Value: itoa(int(ma.IntVal))},
							emit.Field{Key: "replicas", Value: itoa(rep)},
						)
					}
				}
			}
		}
	}

	if pdb.Status.CurrentHealthy > 0 && pdb.Status.DisruptionsAllowed == 0 {
		return makeFinding(
			fmt.Sprintf("PodDisruptionBudget %s/%s currently has disruptionsAllowed=0 with %d healthy pod(s): node drain will be blocked until additional replicas become ready",
				pdb.Namespace, pdb.Name, pdb.Status.CurrentHealthy),
			emit.Field{Key: "disruptions_allowed", Value: "0"},
		)
	}

	return emit.Finding{}, false
}

func matchedWorkloadReplicas(
	pdb policyv1.PodDisruptionBudget,
	deployments []appsv1.Deployment,
	statefulsets []appsv1.StatefulSet,
) []int {
	if pdb.Spec.Selector == nil {
		return nil
	}
	sel, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	if err != nil {
		return nil
	}
	var out []int
	for _, d := range deployments {
		if d.Namespace != pdb.Namespace {
			continue
		}
		if sel.Matches(labels.Set(d.Spec.Template.Labels)) {
			r := 1
			if d.Spec.Replicas != nil {
				r = int(*d.Spec.Replicas)
			}
			out = append(out, r)
		}
	}
	for _, st := range statefulsets {
		if st.Namespace != pdb.Namespace {
			continue
		}
		if sel.Matches(labels.Set(st.Spec.Template.Labels)) {
			r := 1
			if st.Spec.Replicas != nil {
				r = int(*st.Spec.Replicas)
			}
			out = append(out, r)
		}
	}
	return out
}

func parsePercent(s string) (int, bool) {
	if !strings.HasSuffix(s, "%") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(s, "%"))
	if err != nil {
		return 0, false
	}
	return n, true
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

// parseTargetVersion accepts either a full major.minor.patch(-gke.N)
// string or a two-part major.minor release line (such as "1.32" or "v1.32")
// passed via --target-version.
func parseTargetVersion(s string) (version, bool) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(s), "v")
	if v, ok := parseVersion(trimmed); ok {
		return v, true
	}
	if strings.Contains(trimmed, "-") {
		return version{}, false
	}
	parts := strings.Split(trimmed, ".")
	if len(parts) != 2 {
		return version{}, false
	}
	maj, err1 := strconv.Atoi(parts[0])
	min, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || maj < 0 || min < 0 {
		return version{}, false
	}
	return version{major: maj, minor: min}, true
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
