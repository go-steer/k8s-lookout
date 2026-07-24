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
// M1 scope is the boundary itself: identity detection (project /
// location / cluster from config, well-known env vars, or the GCE
// metadata server — plain HTTP, no GCP SDKs). Every capability
// reports unavailable until the SDK-backed implementations land in
// M4 (capacity/quota) and M5 (cloud group, state wi, perf probe).
package gke

import (
	"context"
	"os"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// Name is the registry name of this provider.
const Name = "gke"

// reasonDeferred is the uniform unavailability reason while this
// package is a boundary skeleton.
const reasonDeferred = "not implemented until M4/M5"

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

// Name implements cloud.Provider.
func (p *Provider) Name() string { return Name }

// Project is the resolved GCP project ID ("" if undetectable).
func (p *Provider) Project() string { return p.project }

// Location is the resolved cluster location ("" if undetectable).
func (p *Provider) Location() string { return p.location }

// Cluster is the resolved cluster name ("" if undetectable).
func (p *Provider) Cluster() string { return p.cluster }

// Capabilities implements cloud.Provider. Everything is declared —
// this provider will implement the full §2 surface — and everything
// is unavailable until M4/M5.
func (p *Provider) Capabilities() []cloud.CapabilityStatus {
	all := cloud.AllCapabilities()
	statuses := make([]cloud.CapabilityStatus, 0, len(all))
	for _, c := range all {
		statuses = append(statuses, cloud.CapabilityStatus{Capability: c, Reason: reasonDeferred})
	}
	return statuses
}

func (p *Provider) Metrics() (cloud.MetricsBackend, bool)               { return nil, false }
func (p *Provider) Capacity() (cloud.CapacityAPI, bool)                 { return nil, false }
func (p *Provider) Quota() (cloud.QuotaAPI, bool)                       { return nil, false }
func (p *Provider) Orphans() (cloud.OrphanAPI, bool)                    { return nil, false }
func (p *Provider) IPSpace() (cloud.IPSpaceAPI, bool)                   { return nil, false }
func (p *Provider) Stockouts() (cloud.StockoutAPI, bool)                { return nil, false }
func (p *Provider) WorkloadIdentity() (cloud.WorkloadIdentityAPI, bool) { return nil, false }

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
