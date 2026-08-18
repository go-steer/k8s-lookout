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

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// The cluster-config posture kinds (issue #186). Unlike every other
// kind in this group they are read from the cloud provider rather than
// the Kubernetes API: the settings are the cluster's, not any object's
// inside it, and nothing in the API server can see them.
const (
	kindWorkloadIdentityOff = "audit.workload_identity_off"
	kindLegacyMetadata      = "audit.legacy_metadata"
	kindPublicControlPlane  = "audit.public_control_plane"
)

const (
	reasonWorkloadIdentityDisabled = "WorkloadIdentityDisabled"
	reasonNodePoolMetadataServer   = "NodePoolMetadataServerOff"
	reasonLegacyMetadata           = "LegacyMetadataEndpoints"
	reasonPublicEndpointOpen       = "PublicEndpointUnrestricted"
	reasonAuthorizedNetworksAll    = "AuthorizedNetworksAllowAll"
	reasonAuthorizedNetworksGCP    = "AuthorizedNetworksAllowProviderCIDRs"
)

// anyIPv4 is the CIDR that makes an allow-list allow everything.
const anyIPv4 = "0.0.0.0/0"

// ClusterCommand builds `lookout audit cluster`: the cluster-config
// half of the compliance audit (#186) — three claims about how the
// cluster itself is set up, none of which any object inside it records.
//
// # Why this one command reads the cloud and the others do not
//
// A pod template says what a workload asked for; the cluster record
// says what the platform will give it regardless. "Workload Identity is
// off" is not visible from any Kubernetes object — the annotation a
// ServiceAccount carries is a CLAIM, and `state wi` already verifies
// claims — so the only honest source is the provider. The read goes
// through the pkg/cloud capability boundary, which is what keeps this
// package free of cloud SDK imports; on a vanilla build, or a GKE build
// that cannot resolve the cluster's identity, the command emits the
// standard explicit cloud.unavailable record and exits 0 with
// scanned=0. An absent answer is reported, never fabricated and never
// silent.
//
// # Two claims are narrowed against what the cluster actually is
//
// **The per-pool metadata claim fires only when cluster-level Workload
// Identity is ON.** With it off, every pool serves the node's identity
// by definition, and one cluster-level finding says so once; repeating
// it per pool would multiply a single setting by the pool count. With
// it on, a pool still on the node metadata server is the interesting
// case — the cluster looks configured and that pool's pods quietly
// bypass it.
//
// **A restricted public endpoint is silent.** A control plane reachable
// from the internet is the GKE default and, behind an authorized-network
// allow-list that means something, is a deliberate and ordinary
// configuration. What is reported is the endpoint nothing narrows, and
// the allow-list that admits everything anyway — which reads as
// restricted to any inventory that counts configuration objects instead
// of reading them.
//
// # Two limits worth stating
//
// **A node pool that states no metadata mode is not judged.** The
// provider resolves an unset mode from cluster-level defaults this read
// cannot see, so a claim either way would be a guess; the pool is
// counted in scanned and makes no assertion.
//
// **Private nodes are not judged here.** Whether nodes carry public IPs
// is a cohort question — the fleet-consistency stream compares it
// across a cohort — and this command is deliberately the three
// single-cluster security-config claims of #186, not the config
// inventory.
func ClusterCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "audit cluster",
		MCPName: "k8s_audit_cluster",
		Summary: "Cluster-level security configuration posture, read from the cloud provider: Workload Identity off cluster-wide or bypassed by a node pool, node pools still serving the legacy metadata endpoints, and a control-plane endpoint the internet can reach with nothing narrowing it. Reads the provider's cluster record, not Kubernetes objects, so it takes no --namespace/-A/--workload; scanned counts the cluster plus its node pools. Without a provider capability it reports an explicit unavailable rather than silence.",
		Kinds: []checks.KindField{
			checks.Kind(kindWorkloadIdentityOff, "Workload Identity is off cluster-wide, or a node pool bypasses it — pods authenticate to the cloud as the node", emit.SeverityWarning),
			checks.Kind(kindLegacyMetadata, "a node pool still serves the pre-v1 instance-metadata endpoints, which any pod can read", emit.SeverityWarning),
			checks.Kind(kindPublicControlPlane, "the control-plane endpoint is reachable from the internet; info when authorized networks narrow it", emit.SeverityWarning, emit.SeverityInfo),
			checks.CloudUnavailableKind(),
		},
		Output: []checks.OutputField{
			{Name: "cluster", Doc: "on a node-pool finding: the cluster the pool belongs to, so the record stands alone"},
			{Name: "workload_pool", Doc: "the cluster-wide workload identity pool that this node pool's pods bypass"},
			{Name: "metadata_mode", Doc: "how the node pool exposes instance metadata to pods: node-identity means any pod can mint tokens for the node's service account"},
			{Name: "disable_legacy_endpoints", Doc: "the pool's legacy-metadata setting as the provider records it: `enabled` when someone turned the pre-v1 endpoints back on, `unset` when the pool was never configured either way"},
			{Name: "node_pools", Doc: "summary note: node pools examined — the cluster itself is the other unit `scanned` counts"},
			{Name: "endpoint", Doc: "the control plane's internet-facing address"},
			{Name: "authorized_networks", Doc: "how many source ranges the allow-list permits"},
			{Name: "authorized_network_cidrs", Doc: "those ranges, sorted as the provider returned them and capped at 8 with a +N more tail"},
			{Name: "gcp_public_cidrs", Doc: "whether the provider's own public ranges are admitted in addition to the allow-list"},
			{Name: "capability", Doc: "cloud.unavailable: the provider capability this command needed (" + string(cloud.CapabilityClusterConfig) + ")"},
			{Name: "provider", Doc: "cloud.unavailable: the provider that was asked"},
			{Name: "unavailable", Doc: "summary-line note (§2 marker): why the cloud read could not be served"},
		},
		Examples: []string{
			"lookout audit cluster",
			"lookout audit cluster --format=json",
			"lookout audit cluster --exemptions=exemptions.yaml",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runCluster(ctx, deps, inv)
		},
	}
}

func runCluster(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if inv.Scope.Namespace != "" || inv.Scope.AllNamespaces || !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("audit cluster reads the provider's cluster record, not objects inside the cluster: --namespace/-A/--workload do not apply")
	}

	provider, err := deps.provider(ctx)
	if err != nil {
		return 0, err
	}
	api, ok := provider.ClusterConfig()
	if !ok {
		return emitUnavailable(inv, provider, cloud.CapabilityClusterConfig, "audit cluster")
	}
	cfg, err := api.Config(ctx)
	if err != nil {
		return 0, fmt.Errorf("cluster configuration: %w", err)
	}

	findings := append(workloadIdentityFindings(cfg), legacyMetadataFindings(cfg)...)
	findings = append(findings, controlPlaneFindings(cfg)...)
	sortFindings(findings)
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	if err := inv.Out.Note("node_pools", itoa(len(cfg.NodePools))); err != nil {
		return 0, err
	}
	// The cluster is a scanned subject in its own right: two of the
	// three claims are about it and not about any pool.
	return 1 + len(cfg.NodePools), nil
}

// workloadIdentityFindings judges the cluster-level setting and, when
// it is on, the pools that opt out of it.
func workloadIdentityFindings(cfg cloud.ClusterConfig) []emit.Finding {
	if cfg.WorkloadIdentityPool == "" {
		return []emit.Finding{clusterFinding(cfg, emit.Finding{
			Kind:     kindWorkloadIdentityOff,
			Severity: emit.SeverityWarning,
			Reason:   reasonWorkloadIdentityDisabled,
			Message: fmt.Sprintf("Workload Identity is off for the whole cluster: no pod can hold an identity of its own, so anything calling a cloud API uses the identity of the node it happens to land on — shared by every other pod on that node — or a static key mounted as a Secret. %d node %s inherit this",
				len(cfg.NodePools), plural(len(cfg.NodePools), "pool")),
		})}
	}

	var out []emit.Finding
	for _, np := range cfg.NodePools {
		if np.MetadataServerMode != cloud.MetadataModeNodeIdentity {
			continue
		}
		out = append(out, nodePoolFinding(cfg, np, emit.Finding{
			Kind:     kindWorkloadIdentityOff,
			Severity: emit.SeverityWarning,
			Reason:   reasonNodePoolMetadataServer,
			Message: fmt.Sprintf("Workload Identity is enabled on the cluster (%s) and this node pool does not use it: its nodes expose the raw instance metadata, so any pod scheduled here can mint tokens for the node's service account and the cluster's per-workload identities are bypassed for everything that lands on it",
				cfg.WorkloadIdentityPool),
			Details: []emit.Field{
				{Key: "workload_pool", Value: cfg.WorkloadIdentityPool},
				{Key: "metadata_mode", Value: np.MetadataServerMode},
			},
		}))
	}
	return out
}

// legacyMetadataFindings judges the pre-v1 metadata endpoints. Both
// states that leave them serving are one reason: the remedy is the same
// single setting, and the detail field carries whether somebody turned
// them back on or nobody ever turned them off.
func legacyMetadataFindings(cfg cloud.ClusterConfig) []emit.Finding {
	var out []emit.Finding
	for _, np := range cfg.NodePools {
		if np.LegacyEndpoints == cloud.ToggleDisabled {
			continue
		}
		out = append(out, nodePoolFinding(cfg, np, emit.Finding{
			Kind:     kindLegacyMetadata,
			Severity: emit.SeverityWarning,
			Reason:   reasonLegacyMetadata,
			Message:  "the node pool still serves the legacy metadata endpoints: they answer without the header the current endpoint requires, so any request a pod can be tricked into making — a URL in a webhook field, an SSRF in a proxy — reaches the node's credentials",
			Details: []emit.Field{
				{Key: "disable_legacy_endpoints", Value: string(np.LegacyEndpoints)},
			},
		}))
	}
	return out
}

// controlPlaneFindings judges what can reach the API server from
// outside. A private control plane, and a public one behind an
// allow-list that means something, are both silent.
func controlPlaneFindings(cfg cloud.ClusterConfig) []emit.Finding {
	if cfg.PublicEndpoint == "" {
		return nil
	}
	an := cfg.AuthorizedNetworks
	endpoint := emit.Field{Key: "endpoint", Value: cfg.PublicEndpoint}

	if !an.Enabled {
		return []emit.Finding{clusterFinding(cfg, emit.Finding{
			Kind:     kindPublicControlPlane,
			Severity: emit.SeverityWarning,
			Reason:   reasonPublicEndpointOpen,
			Message:  "the control plane answers on a public address with no authorized-network allow-list in front of it: every credential leak, every stale kubeconfig and every exposed token is usable from anywhere on the internet rather than from inside the network",
			Details:  []emit.Field{endpoint},
		})}
	}

	withList := []emit.Field{
		endpoint,
		{Key: "authorized_networks", Value: itoa(len(an.CIDRs))},
		{Key: "authorized_network_cidrs", Value: cappedList(an.CIDRs)},
	}
	for _, c := range an.CIDRs {
		if c != anyIPv4 {
			continue
		}
		// 0.0.0.0/0 subsumes the provider-range bypass below, so this
		// is the whole finding for such a cluster rather than one of
		// two saying overlapping things.
		return []emit.Finding{clusterFinding(cfg, emit.Finding{
			Kind:     kindPublicControlPlane,
			Severity: emit.SeverityWarning,
			Reason:   reasonAuthorizedNetworksAll,
			Message: fmt.Sprintf("the authorized-network allow-list is enabled and includes %s: it permits the entire internet, so the control plane is exactly as reachable as one with no allow-list at all while reading as restricted to anything counting configuration rather than reading it",
				anyIPv4),
			Details: withList,
		})}
	}

	if an.GCPPublicCIDRs {
		// Info, not warning: this is on by default, and the allow-list
		// still excludes everything outside the provider. It is
		// reported because it is a real widening an operator who wrote
		// a two-office allow-list did not write.
		return []emit.Finding{clusterFinding(cfg, emit.Finding{
			Kind:     kindPublicControlPlane,
			Severity: emit.SeverityInfo,
			Reason:   reasonAuthorizedNetworksGCP,
			Message: fmt.Sprintf("the authorized-network allow-list names %d %s and additionally admits the provider's own public ranges: anyone who can rent a VM from the provider is inside the allow-list. On by default, so it is reported as posture rather than as a defect",
				len(an.CIDRs), plural(len(an.CIDRs), "range")),
			Details: append(withList, emit.Field{Key: "gcp_public_cidrs", Value: "true"}),
		})}
	}
	return nil
}

// clusterFinding stamps the cluster as the subject.
func clusterFinding(cfg cloud.ClusterConfig, f emit.Finding) emit.Finding {
	f.KindOfObject = "Cluster"
	f.Name = cfg.Name
	f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, f.KindOfObject)
	return f
}

// nodePoolFinding stamps the node pool as the subject: the pool is what
// an operator edits, and on a cluster with one bad pool out of five the
// cluster is not what is wrong.
func nodePoolFinding(cfg cloud.ClusterConfig, np cloud.NodePoolConfig, f emit.Finding) emit.Finding {
	f.KindOfObject = "NodePool"
	f.Name = np.Name
	f.Details = append([]emit.Field{{Key: "cluster", Value: cfg.Name}}, f.Details...)
	f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, f.KindOfObject)
	return f
}

// emitUnavailable is the §2-mandated degradation path, the same record
// the `cloud` group and `state wi` emit: one explicit cloud.unavailable
// finding, the summary marker, exit 0 with scanned=0 — nothing was
// read, and that is reported rather than implied by an empty report.
func emitUnavailable(inv emit.Invocation, p cloud.Provider, c cloud.Capability, what string) (int, error) {
	u := cloud.Unavailable(p, c)
	if err := inv.Out.Emit(emit.Finding{
		Kind:     "cloud.unavailable",
		Severity: emit.SeverityInfo,
		Reason:   "CapabilityUnavailable",
		Message:  fmt.Sprintf("%s needs the provider %s capability: %s", what, c, u.Reason),
		Details: []emit.Field{
			{Key: "capability", Value: string(u.Capability)},
			{Key: "provider", Value: u.Provider},
		},
	}); err != nil {
		return 0, err
	}
	if err := inv.Out.Note("unavailable", u.Reason); err != nil {
		return 0, err
	}
	return 0, nil
}
