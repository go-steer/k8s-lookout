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
	"errors"
	"os"
	"strings"
	"sync"

	"k8s.io/client-go/tools/clientcmd"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/kube"
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

	reasonNoClusterIdentity = "GKE cluster identity undetectable (project, location, and cluster name are all required — run on GKE, select a gke_<project>_<location>_<cluster> kubeconfig context, or pin them)"

	reasonNoSubscription = "no notifications subscription configured (--notifications-subscription: a Pub/Sub subscription on the cluster's notificationConfig topic)"
)

func init() {
	cloud.Register(Name, New)
}

// Provider is the GKE cloud provider. Identity fields are resolved at
// construction; capability backends arrive in M4/M5.
type Provider struct {
	project      string
	location     string
	cluster      string
	subscription string

	// mu guards the teardown list. Capability getters and Close can
	// be called from different goroutines, and a client dials on
	// first use rather than here, so registration happens long after
	// construction.
	mu      sync.Mutex
	closers []func() error
	closed  bool
}

// track registers a dialed client's teardown with the provider, to be
// run by Close. Called from inside a lazy dial, so it sees only the
// clients that were actually built.
//
// Registering after Close closes immediately instead: a getter racing
// the teardown must not park a live connection on a list nobody will
// read again.
func (p *Provider) track(closeFn func() error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = closeFn()
		return
	}
	p.closers = append(p.closers, closeFn)
	p.mu.Unlock()
}

// Close implements cloud.Provider: release every client this provider
// dialed. Only the gRPC clients (cloud.google.com/go) are tracked —
// the google.golang.org/api REST services hold an *http.Client whose
// idle connections the transport reaps on its own and which exposes
// no teardown to call.
//
// Every teardown runs even if an earlier one fails; the errors are
// joined so a caller logging one line still sees all of them.
func (p *Provider) Close() error {
	p.mu.Lock()
	closers := p.closers
	p.closers = nil
	p.closed = true
	p.mu.Unlock()

	var errs []error
	for _, closeFn := range closers {
		if err := closeFn(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// New constructs the GKE provider, resolving identity in precedence
// order: explicit Config pins, then well-known env vars
// (GOOGLE_CLOUD_PROJECT / CLOUDSDK_CORE_PROJECT), then the cluster the
// invocation selected, then where lookout runs.
//
// "The cluster the invocation selected" is an explicit --kubeconfig or
// --context (kube.Selection) whose context or cluster entry follows
// GKE's `gke_<project>_<location>_<cluster>` convention (#616). When a
// selection is explicit, the GCE metadata server is not consulted at
// all (#694): it describes the node lookout runs on, so inside a pod
// it would answer for lookout's own cluster while the Kubernetes reads
// went to the selected one, and every cloud-backed answer would be
// about the wrong cluster under the right cluster's name. A selection
// that does not name a GKE cluster leaves the fields empty, so
// cluster-scoped capabilities degrade explicitly (§2) instead.
//
// With no selection, the metadata server comes first and the default
// kubeconfig's current-context is the off-GCE fallback. Off-GCE with
// no GKE kubeconfig context or pins, fields stay empty and
// capabilities degrade explicitly per §2.
func New(ctx context.Context, cfg cloud.Config) (cloud.Provider, error) {
	p := &Provider{
		project:      cfg.Project,
		location:     cfg.Location,
		cluster:      cfg.Cluster,
		subscription: cfg.NotificationsSubscription,
	}

	if p.project == "" {
		p.project = firstEnv("GOOGLE_CLOUD_PROJECT", "CLOUDSDK_CORE_PROJECT")
	}

	sel := kube.SelectionFrom(ctx)
	if t := sel.Target; t != nil {
		// A cluster named per call on a multi-cluster endpoint (#700):
		// its own identity, over the environment's and the metadata
		// server's, for the reason above. A kubeconfig-fleet target
		// knows no project, which leaves cluster-scoped capabilities
		// to degrade explicitly.
		if cfg.Project == "" {
			p.project = t.Project
		}
		if cfg.Location == "" {
			p.location = t.Location
		}
		if cfg.Cluster == "" {
			p.cluster = t.Name
		}
		return p, nil
	}
	if !sel.IsZero() {
		p.fillFromKubeconfig(ctx)
		return p, nil
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
	p.fillFromKubeconfig(ctx)
	return p, nil
}

// fillFromKubeconfig fills whichever identity fields are still empty
// from the selected (or default) kubeconfig context's GKE name.
func (p *Provider) fillFromKubeconfig(ctx context.Context) {
	if p.project != "" && p.location != "" && p.cluster != "" {
		return
	}
	proj, loc, clus := kubeconfigGKEIdentity(ctx)
	if p.project == "" {
		p.project = proj
	}
	if p.location == "" {
		p.location = loc
	}
	if p.cluster == "" {
		p.cluster = clus
	}
}

// kubeconfigGKEIdentity inspects the selected kubeconfig context (or
// default current-context) for GKE's `gke_<project>_<location>_<cluster>`
// naming convention so off-cluster workstation invocations resolve
// cluster-scoped cloud reads (`cloud ipspace`, `cloud orphans
// --only=nodepools`, `audit cluster`, `stab drift --identity`) without
// manual flags (#616).
func kubeconfigGKEIdentity(ctx context.Context) (project, location, cluster string) {
	sel := kube.SelectionFrom(ctx)
	var loader clientcmd.ClientConfigLoader = clientcmd.NewDefaultClientConfigLoadingRules()
	if sel.Kubeconfig != "" {
		loader = &clientcmd.ClientConfigLoadingRules{ExplicitPath: sel.Kubeconfig}
	}
	raw, err := loader.Load()
	if err != nil || raw == nil {
		return "", "", ""
	}
	ctxName := sel.Context
	if ctxName == "" {
		ctxName = raw.CurrentContext
	}
	if ctxName == "" {
		return "", "", ""
	}
	if proj, loc, clus, ok := parseGKEContextName(ctxName); ok {
		return proj, loc, clus
	}
	if kctx, ok := raw.Contexts[ctxName]; ok && kctx != nil {
		if proj, loc, clus, ok := parseGKEContextName(kctx.Cluster); ok {
			return proj, loc, clus
		}
	}
	return "", "", ""
}

// parseGKEContextName parses `gke_<project>_<location>_<cluster>` as
// written by `gcloud container clusters get-credentials`. GCP project
// IDs and locations never contain underscores (`_`), so splitting into
// 4 segments (`gke`, project, location, cluster) is exact.
func parseGKEContextName(name string) (project, location, cluster string, ok bool) {
	parts := strings.SplitN(name, "_", 4)
	if len(parts) != 4 || parts[0] != "gke" || parts[1] == "" || parts[2] == "" || parts[3] == "" {
		return "", "", "", false
	}
	return parts[1], parts[2], parts[3], true
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

// Region is the resolved cluster region ("" if undetectable), for
// zonal and regional clusters alike — the GKE location with a zone
// suffix trimmed off it.
func (p *Provider) Region() string { return locationRegion(p.location) }

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
	case cloud.CapabilityAudit:
		// Cluster-scoped read (post-M5 #128): audit entries are
		// filtered by the k8s_cluster resource labels, so the full
		// GKE identity is required.
		if p.project == "" || p.location == "" || p.cluster == "" {
			return cloud.CapabilityStatus{Capability: c, Reason: reasonNoClusterIdentity}
		}
	case cloud.CapabilityClusterConfig:
		// Cluster-scoped read (#186): clusters.get, the same call
		// ipspace makes — the full GKE identity names the record.
		if p.project == "" || p.location == "" || p.cluster == "" {
			return cloud.CapabilityStatus{Capability: c, Reason: reasonNoClusterIdentity}
		}
	case cloud.CapabilityNotifications:
		// Subscription read (post-M5 #130): needs the explicitly
		// configured subscription first (there is no default — the
		// operator creates it on the notificationConfig topic), then
		// the project to resolve a bare subscription name.
		if p.subscription == "" {
			return cloud.CapabilityStatus{Capability: c, Reason: reasonNoSubscription}
		}
		if p.project == "" {
			return cloud.CapabilityStatus{Capability: c, Reason: reasonNoProject}
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
	return credsMetrics{newMetricsBackend(p)}, true
}

// Capacity implements cloud.Provider: the cluster-autoscaler
// visibility log reader (M4, capacity.go). The Logging client itself
// is dialed lazily on first ScaleDecisions call.
func (p *Provider) Capacity() (cloud.CapacityAPI, bool) {
	if !p.available(cloud.CapabilityCapacity) {
		return nil, false
	}
	return credsCapacity{&capacityAPI{
		project:   p.project,
		location:  p.location,
		cluster:   p.cluster,
		newLister: p.dialLogadmin,
	}}, true
}

// dialLogadmin is capacityAPI's lister factory, wrapped so the client
// it dials is released by Close rather than left to the GC — which
// never collects a live gRPC connection's goroutines (issue #382).
func (p *Provider) dialLogadmin(ctx context.Context, project string) (EntryLister, error) {
	l, err := newLogadminLister(ctx, project)
	if err != nil {
		return nil, err
	}
	p.track(l.Close)
	return l, nil
}

// Quota implements cloud.Provider (M4, quota.go).
func (p *Provider) Quota() (cloud.QuotaAPI, bool) {
	if !p.available(cloud.CapabilityQuota) {
		return nil, false
	}
	return credsQuota{newQuotaAPI(p)}, true
}

// Orphans implements cloud.Provider (M4, orphans.go).
func (p *Provider) Orphans() (cloud.OrphanAPI, bool) {
	if !p.available(cloud.CapabilityOrphans) {
		return nil, false
	}
	return credsOrphans{newOrphanAPI(p)}, true
}

// IPSpace implements cloud.Provider (M4, ipspace.go).
func (p *Provider) IPSpace() (cloud.IPSpaceAPI, bool) {
	if !p.available(cloud.CapabilityIPSpace) {
		return nil, false
	}
	return credsIPSpace{newIPSpaceAPI(p)}, true
}

// Stockouts implements cloud.Provider (M4, stockout.go).
func (p *Provider) Stockouts() (cloud.StockoutAPI, bool) {
	if !p.available(cloud.CapabilityStockout) {
		return nil, false
	}
	return credsStockout{newStockoutAPI(p)}, true
}

// WorkloadIdentity implements cloud.Provider (M5, wi.go). The IAM
// client itself is dialed lazily on first VerifyBinding call.
func (p *Provider) WorkloadIdentity() (cloud.WorkloadIdentityAPI, bool) {
	if !p.available(cloud.CapabilityWorkloadIdentity) {
		return nil, false
	}
	return credsWI{newWIAPI(p)}, true
}

// Audit implements cloud.Provider (post-M5 #128, audit.go). The
// Logging client is dialed lazily on first ObjectWrites call.
func (p *Provider) Audit() (cloud.AuditAPI, bool) {
	if !p.available(cloud.CapabilityAudit) {
		return nil, false
	}
	return credsAudit{newAuditAPI(p)}, true
}

// Notifications implements cloud.Provider (post-M5 #130,
// notifications.go). The Pub/Sub client is dialed lazily on Receive.
func (p *Provider) Notifications() (cloud.NotificationsAPI, bool) {
	if !p.available(cloud.CapabilityNotifications) {
		return nil, false
	}
	return credsNotifications{newNotificationsAPI(p)}, true
}

// ClusterConfig implements cloud.Provider (#186, clusterconfig.go).
// The GKE client is dialed lazily on first Config call.
func (p *Provider) ClusterConfig() (cloud.ClusterConfigAPI, bool) {
	if !p.available(cloud.CapabilityClusterConfig) {
		return nil, false
	}
	return credsClusterConfig{newClusterConfigAPI(p)}, true
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
