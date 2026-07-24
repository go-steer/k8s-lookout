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

// Package cloud is the provider boundary of DESIGN.md §2: everything
// cloud-touching in lookout (capacity explanations, quota inventory,
// orphan sweeps, metrics queries, IP-space utilization, stockout
// extraction, workload-identity verification) goes through the
// Provider interface defined here. pkg/checks and pkg/sources never
// import cloud SDKs (AGENTS.md hard rule) — they import this package
// and ask for capabilities.
//
// # Build tags
//
// Provider implementations are compiled in by build tag, never by
// default:
//
//   - default build   — vanilla Kubernetes; zero cloud SDK linkage;
//     only this package (and its NoProvider sentinel) links in.
//   - gke             — adds pkg/cloud/gke (//go:build gke || allproviders).
//   - allproviders    — release umbrella tag: every provider
//     implementation; release binaries build with this.
//
// dev/tools/build compiles all three variants; dev/tools/test-unit
// additionally runs the tagged tests under -tags allproviders.
// Implementations self-register from an init() guarded by their tag,
// and cmd/lookout blank-imports each implementation from an equally
// guarded file — so the default build cannot link a provider even by
// accident.
//
// # Absent provider ≠ silence
//
// When no provider is configured (or a provider lacks a capability),
// commands must report it explicitly — an `unavailable reason="..."`
// marker in their summary line / an explicit finding, never silence
// (§2, §11). The Unavailability helper produces that marker.
package cloud

// Capability identifies one optional, capability-scoped facet of a
// Provider. Commands ask the Provider for the facet they need and
// receive (impl, available bool); the string values appear in
// operator-facing output (capability reports, unavailable markers),
// so they are stable identifiers.
type Capability string

const (
	// CapabilityMetrics is the metrics query backend behind
	// `perf probe` packs and `triage top --history` (§5; §15 Q4).
	CapabilityMetrics Capability = "metrics"
	// CapabilityCapacity is structured autoscaler decision records —
	// GKE CA visibility logs, §10.1 source 3.
	CapabilityCapacity Capability = "capacity"
	// CapabilityQuota is quota inventory and usage/limit history
	// (§10.2; `cloud quota`).
	CapabilityQuota Capability = "quota"
	// CapabilityOrphans is the orphaned-cloud-resource sweep
	// (`cloud orphans`).
	CapabilityOrphans Capability = "orphans"
	// CapabilityIPSpace is Pod/Service CIDR utilization per subnet
	// (`cloud ipspace`).
	CapabilityIPSpace Capability = "ipspace"
	// CapabilityStockout is capacity-stockout log extraction
	// (`cloud stockout`).
	CapabilityStockout Capability = "stockout"
	// CapabilityWorkloadIdentity is workload-identity binding
	// verification (`state wi`; GKE KSA↔GSA, EKS IRSA analog).
	CapabilityWorkloadIdentity Capability = "workload-identity"
)

// AllCapabilities returns every Capability the boundary defines, in
// stable order. Used for capability reports and by providers that
// need to enumerate a uniform status (e.g. NoProvider).
func AllCapabilities() []Capability {
	return []Capability{
		CapabilityMetrics,
		CapabilityCapacity,
		CapabilityQuota,
		CapabilityOrphans,
		CapabilityIPSpace,
		CapabilityStockout,
		CapabilityWorkloadIdentity,
	}
}

// CapabilityStatus reports one capability's availability on a
// provider, with the reason when absent. `lookout mcp` and `--help`
// use this to omit or mark cloud-dependent commands (§2).
type CapabilityStatus struct {
	Capability Capability
	Available  bool
	// Reason says why the capability is unavailable; empty when
	// Available. Wording is operator-facing (it lands verbatim in
	// `unavailable reason="..."` markers).
	Reason string
}

// Provider is one cloud environment (GKE first; EKS/AKS are future
// pkg/cloud/<impl> packages — no engine, schema, or skill changes).
//
// Identity is the only unconditional surface. Everything else is a
// capability getter returning (impl, available bool): commands take
// the capability they need, and on !available emit the explicit
// unavailable marker via Unavailable(p, c) — never skip silently.
type Provider interface {
	// Name is the registry name ("gke", …) or NoProviderName.
	Name() string

	// Capabilities reports the provider's stance on every capability
	// it knows about, including unavailability reasons.
	Capabilities() []CapabilityStatus

	// Metrics is the metrics query backend (CapabilityMetrics).
	Metrics() (MetricsBackend, bool)
	// Capacity explains autoscaler decisions (CapabilityCapacity).
	Capacity() (CapacityAPI, bool)
	// Quota is quota inventory and history (CapabilityQuota).
	Quota() (QuotaAPI, bool)
	// Orphans sweeps for orphaned cloud resources (CapabilityOrphans).
	Orphans() (OrphanAPI, bool)
	// IPSpace reports CIDR utilization (CapabilityIPSpace).
	IPSpace() (IPSpaceAPI, bool)
	// Stockouts extracts capacity-stockout records (CapabilityStockout).
	Stockouts() (StockoutAPI, bool)
	// WorkloadIdentity verifies identity bindings
	// (CapabilityWorkloadIdentity).
	WorkloadIdentity() (WorkloadIdentityAPI, bool)
}
