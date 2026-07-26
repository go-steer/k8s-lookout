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

// Package gke is the GKE/GCP implementation of the pkg/cloud provider
// boundary (DESIGN.md §2). It compiles only under the `gke` or
// `allproviders` build tags — the default lookout build has zero GCP
// linkage — and self-registers as "gke" in init().
//
// From M1: identity detection (project / location / cluster from
// config, well-known env vars, or the GCE metadata server). From M4:
// Capacity — the cluster-autoscaler visibility log reader
// (capacity.go / logadmin.go) — and the `cloud` command group's
// capabilities: stockout (Cloud Logging audit entries), orphans +
// quota (Compute), ipspace (GKE + Compute), SDK-backed behind §13
// small client interfaces (stockout.go, orphans.go, ipspace.go,
// quota.go; REST-client choice documented in compute.go). From M5:
// workload identity — IAM-backed KSA↔GSA binding verification behind
// `state wi` (wi.go) — and Metrics, the Cloud-Monitoring-backed query
// engine behind `perf probe` packs and `triage top --history`
// (metrics.go / metricsclient.go).
package gke

import (
	"context"
	"os"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// Name is the registry name of this provider.
const Name = "gke"

// reasonDeferred is the unavailability reason for the capabilities
// whose SDK-backed implementations have not landed yet.
const reasonDeferred = "not implemented until M4/M5"

// reasonNoProject / reasonNoClusterIdentity are the §2 fail-loudly
// reasons when a capability's implementation exists but the resolved
// identity is missing what it needs.
const (
	reasonNoProject = "GCP project undetectable (pin it in provider config or run on GCE)"

	reasonNoClusterIdentity = "GKE cluster identity undetectable (project, location, and cluster name are all required — pin them or run on GKE)"
)

func init() {
	cloud.Register(Name, New)
}

// Provider is the GKE cloud provider. Identity fields are resolved at
// construction; capability backends arrive in M4/M5.
type Provider struct {
	project  string
	location string
	cluster  string
}

// New constructs the GKE provider, resolving identity in precedence
// order: explicit Config pins, then well-known env vars
// (GOOGLE_CLOUD_PROJECT / CLOUDSDK_CORE_PROJECT), then the GCE
// metadata server. Detection is best-effort: off-GCE with nothing
// pinned, fields stay empty and the M4/M5 capability implementations
// fail loudly when they actually need them.
func New(ctx context.Context, cfg cloud.Config) (cloud.Provider, error) {
	p := &Provider{
		project:  cfg.Project,
		location: cfg.Location,
		cluster:  cfg.Cluster,
	}

	if p.project == "" {
		p.project = firstEnv("GOOGLE_CLOUD_PROJECT", "CLOUDSDK_CORE_PROJECT")
	}

	if p.project == "" || p.location == "" || p.cluster == "" {
		md := newMetadataClient()
		if p.project == "" {
			p.project = md.lookup(ctx, "project/project-id")
		}
		if p.location == "" {
			p.location = md.lookup(ctx, "instance/attributes/cluster-location")
		}
		if p.cluster == "" {
			p.cluster = md.lookup(ctx, "instance/attributes/cluster-name")
		}
	}
	return p, nil
}

// The resolved identity doubles as the cloud.Identity surface the
// sentinel stamps §8 zone/project from (precedence: explicit
// --project/--zone flag > this metadata > empty).
var _ cloud.Identity = (*Provider)(nil)

// Name implements cloud.Provider.
func (p *Provider) Name() string { return Name }

// Project is the resolved GCP project ID ("" if undetectable).
func (p *Provider) Project() string { return p.project }

// Location is the resolved cluster location ("" if undetectable).
func (p *Provider) Location() string { return p.location }

// Cluster is the resolved cluster name ("" if undetectable).
func (p *Provider) Cluster() string { return p.cluster }

// Capabilities implements cloud.Provider. Everything is declared —
// this provider will implement the full §2 surface — and each
// capability reports per-capability availability (implemented AND
// identity sufficient) with the §2 explicit reason otherwise.
func (p *Provider) Capabilities() []cloud.CapabilityStatus {
	all := cloud.AllCapabilities()
	statuses := make([]cloud.CapabilityStatus, 0, len(all))
	for _, c := range all {
		statuses = append(statuses, p.capabilityStatus(c))
	}
	return statuses
}

// capabilityStatus is the single availability judgment the
// capability getters below mirror: a getter returns ok exactly when
// this reports Available.
func (p *Provider) capabilityStatus(c cloud.Capability) cloud.CapabilityStatus {
	switch c {
	case cloud.CapabilityMetrics,
		cloud.CapabilityCapacity, cloud.CapabilityQuota, cloud.CapabilityOrphans, cloud.CapabilityStockout:
		// Project-scoped reads (M4; Metrics from M5): need the
		// project identity — Monitoring series live per project.
		if p.project == "" {
			return cloud.CapabilityStatus{Capability: c, Reason: reasonNoProject}
		}
	case cloud.CapabilityWorkloadIdentity:
		// Project-scoped read (M5): the expected IAM member embeds
		// the project's workload identity pool.
		if p.project == "" {
			return cloud.CapabilityStatus{Capability: c, Reason: reasonNoProject}
		}
	case cloud.CapabilityIPSpace:
		// Cluster-scoped read (M4): needs the full GKE identity for
		// clusters.get.
		if p.project == "" || p.location == "" || p.cluster == "" {
			return cloud.CapabilityStatus{Capability: c, Reason: reasonNoClusterIdentity}
		}
	default:
		return cloud.CapabilityStatus{Capability: c, Reason: reasonDeferred}
	}
	return cloud.CapabilityStatus{Capability: c, Available: true}
}

// available is the getter-side check against capabilityStatus.
func (p *Provider) available(c cloud.Capability) bool {
	return p.capabilityStatus(c).Available
}

// Metrics implements cloud.Provider: the Cloud-Monitoring-backed
// query engine (M5, metrics.go). The Monitoring client is dialed
// lazily on first QuerySeries call.
func (p *Provider) Metrics() (cloud.MetricsBackend, bool) {
	if !p.available(cloud.CapabilityMetrics) {
		return nil, false
	}
	return newMetricsBackend(p), true
}

// Capacity implements cloud.Provider: the cluster-autoscaler
// visibility log reader (M4, capacity.go). The Logging client itself
// is dialed lazily on first ScaleDecisions call.
func (p *Provider) Capacity() (cloud.CapacityAPI, bool) {
	if !p.available(cloud.CapabilityCapacity) {
		return nil, false
	}
	return &capacityAPI{
		project:   p.project,
		location:  p.location,
		cluster:   p.cluster,
		newLister: newLogadminLister,
	}, true
}

// Quota implements cloud.Provider (M4, quota.go).
func (p *Provider) Quota() (cloud.QuotaAPI, bool) {
	if !p.available(cloud.CapabilityQuota) {
		return nil, false
	}
	return newQuotaAPI(p), true
}

// Orphans implements cloud.Provider (M4, orphans.go).
func (p *Provider) Orphans() (cloud.OrphanAPI, bool) {
	if !p.available(cloud.CapabilityOrphans) {
		return nil, false
	}
	return newOrphanAPI(p), true
}

// IPSpace implements cloud.Provider (M4, ipspace.go).
func (p *Provider) IPSpace() (cloud.IPSpaceAPI, bool) {
	if !p.available(cloud.CapabilityIPSpace) {
		return nil, false
	}
	return newIPSpaceAPI(p), true
}

// Stockouts implements cloud.Provider (M4, stockout.go).
func (p *Provider) Stockouts() (cloud.StockoutAPI, bool) {
	if !p.available(cloud.CapabilityStockout) {
		return nil, false
	}
	return newStockoutAPI(p), true
}

// WorkloadIdentity implements cloud.Provider (M5, wi.go). The IAM
// client itself is dialed lazily on first VerifyBinding call.
func (p *Provider) WorkloadIdentity() (cloud.WorkloadIdentityAPI, bool) {
	if !p.available(cloud.CapabilityWorkloadIdentity) {
		return nil, false
	}
	return newWIAPI(p), true
}

// firstEnv returns the first non-empty value among the named
// environment variables.
func firstEnv(names ...string) string {
	for _, name := range names {
		if v := os.Getenv(name); v != "" {
			return v
		}
	}
	return ""
}
