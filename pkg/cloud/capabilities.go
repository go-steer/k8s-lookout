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

package cloud

import (
	"context"
	"errors"
	"time"
)

// The capability sub-interfaces and their result types. Result shapes
// are deliberately minimal — the fields every consumer named in the
// design needs today; the first SDK-backed implementations (M4/M5)
// grow them where reality demands.

// TimeWindow bounds a historical query: [Start, End).
type TimeWindow struct {
	Start time.Time
	End   time.Time
}

// Point is one timestamped sample.
type Point struct {
	Time  time.Time
	Value float64
}

// Series is one labeled time series.
type Series struct {
	Metric string
	Labels map[string]string
	Points []Point
}

// SeriesQuery selects time series by metric name and label matchers
// over a window. Deliberately not a query-language string: §15 Q4
// requires the `perf probe` pack format to avoid Cloud-Monitoring-only
// constructs, so packs are expressed in this backend-neutral shape and
// each backend (Cloud Monitoring today, Prometheus when a non-GKE
// consumer materializes) translates it.
type SeriesQuery struct {
	Metric string
	// Matchers are exact-match label constraints.
	Matchers map[string]string
	Window   TimeWindow
	// Step is the desired resolution; the backend may coarsen it.
	Step time.Duration

	// The aggregation fields below stay inside the §15 Q4 envelope:
	// each names both its Cloud Monitoring and its PromQL
	// realization, so no pack can express a Monitoring-only shape.

	// GroupBy aggregates ACROSS series, keeping only these labels.
	// nil means no cross-series aggregation (every stored series
	// comes back raw); a non-nil GroupBy — including the empty list —
	// reduces, an empty list collapsing everything into one series.
	// Monitoring: crossSeriesReducer + groupByFields; PromQL:
	// sum/avg by(...) (sum(...) for the empty list).
	GroupBy []string
	// Percentile selects the quantile of a distribution/histogram
	// metric: 0 = raw values, otherwise 50, 95, or 99. Monitoring:
	// REDUCE_PERCENTILE_NN over ALIGN_DELTA'd distributions; PromQL:
	// histogram_quantile(0.NN, ...).
	Percentile int
	// Rate asks for the per-second rate of a cumulative counter.
	// Monitoring: ALIGN_RATE; PromQL: rate().
	Rate bool
}

// ErrMetricAbsent is returned (wrapped, naming the metric) by a
// MetricsBackend that can POSITIVELY determine the queried metric
// does not exist in the workspace — on GKE, a control-plane metric
// whose collection is not enabled on the cluster. It is distinct
// from "no data in the window" (empty result, no error) and from an
// unknown neutral metric name (a programming/spec error). Consumers
// (`perf probe`) turn it into an explicit pack_unavailable finding —
// never silence (§2, §11).
var ErrMetricAbsent = errors.New("metric absent from the metrics workspace")

// MetricsBackend executes the metrics queries behind `perf probe`
// packs and `triage top --history` (§5).
type MetricsBackend interface {
	QuerySeries(ctx context.Context, q SeriesQuery) ([]Series, error)
}

// ScaleDecision is one structured autoscaler decision record —
// on GKE, a cluster-autoscaler-visibility log entry (§10.1 source 3).
type ScaleDecision struct {
	Time time.Time
	// Decision is the record type: "scaleUp", "noScaleUp",
	// "scaleDown", "noScaleDown".
	Decision string
	// NodeGroup is the rejected/acted-on group (GKE: MIG / node pool).
	NodeGroup string
	// Reason is the machine-matchable cause, e.g. "GCE_STOCKOUT",
	// "GCE_QUOTA_EXCEEDED", "IP_SPACE_EXHAUSTED". The stockout/quota
	// distinction drives disjoint remedies (§10.1).
	Reason  string
	Message string
}

// CapacityAPI exposes the provider-side autoscaler decision records
// that upgrade the capacity source from "scaleup failed" to the
// authoritative why (§10.1). The two upstream-portable CA sub-sources
// (Events, status ConfigMap) live in pkg/sources and do not come
// through here.
type CapacityAPI interface {
	ScaleDecisions(ctx context.Context, w TimeWindow) ([]ScaleDecision, error)
}

// QuotaUsage is one quota's current usage/limit pair (§10.2).
type QuotaUsage struct {
	// Name is the provider's quota metric name, e.g. "CPUS".
	Name string
	// Scope is the region for regional quotas, "global" otherwise.
	Scope string
	Usage float64
	Limit float64
	Unit  string
	// ID is the provider's canonical increase-request identifier
	// when it maps one — on GCP the Cloud Quotas API
	// "<service>/<quotaId>" pair a QuotaPreference names (e.g.
	// "compute.googleapis.com/CPUS-per-project-region"). OPTIONAL
	// best-effort enrichment: empty when the provider cannot (or did
	// not) resolve it, and consumers fall back to Name. Feeds the
	// §10.3 drafted increase request.
	ID string
}

// QuotaHistory carries usage-vs-limit series for one quota, feeding
// the saturation-source slope math: not "at 87%" but "exhausted in
// ~6 days at current slope" (§10.2).
type QuotaHistory struct {
	Name  string
	Scope string
	Usage []Point
	Limit []Point
}

// QuotaAPI is quota inventory and history (§10.2; `cloud quota` and
// the per-project quota source).
type QuotaAPI interface {
	// Quotas returns the current usage/limit inventory.
	Quotas(ctx context.Context) ([]QuotaUsage, error)
	// History returns usage/limit series for one quota.
	History(ctx context.Context, name, scope string, w TimeWindow) (QuotaHistory, error)
}

// OrphanDisk is an unattached, billing-active disk.
type OrphanDisk struct {
	Name   string
	Zone   string
	SizeGB int64
	// Type is the disk type short name (e.g. "pd-ssd") — cost
	// relevance: an idle pd-ssd bills ~4x an idle pd-standard.
	Type string
	// UnusedSince is when the disk stopped being used: the last
	// detach time when the provider records one, else the creation
	// time (never attached). Zero when the provider cannot date it —
	// callers treat that as "age unknown" and must not silently
	// drop the disk.
	UnusedSince time.Time
}

// OrphanLoadBalancer is a load balancer / forwarding rule targeting
// zero pods.
type OrphanLoadBalancer struct {
	Name   string
	Region string
	// Reason says why it is considered orphaned (e.g. "no backends").
	Reason string
}

// OrphanAPI sweeps for orphaned cloud resources (`cloud orphans`).
//
// OrphanDisks returns EVERY unattached billing-active disk with its
// UnusedSince timestamp; age thresholds are the caller's policy
// (`cloud orphans --min-age`), so the command's summary line can
// honestly count what was examined. OrphanLoadBalancers returns only
// the rules the provider already judged orphaned.
type OrphanAPI interface {
	OrphanDisks(ctx context.Context) ([]OrphanDisk, error)
	OrphanLoadBalancers(ctx context.Context) ([]OrphanLoadBalancer, error)
}

// SubnetUtilization is IP usage for one subnet range (`cloud ipspace`).
type SubnetUtilization struct {
	Subnet string
	CIDR   string
	// Purpose is what the range allocates: "pods", "services", "nodes".
	Purpose string
	// Used is the number of allocated addresses. For ranges the
	// provider carves out in per-node blocks (GKE pod ranges), Used
	// counts the ALLOCATED blocks' addresses — that is where the
	// range actually exhausts, not at individual pod IPs. Used < 0
	// means the usage is not visible to the cloud APIs (GKE service
	// ClusterIP allocation is a Kubernetes-side counter); consumers
	// must render that explicitly, never as 0%.
	Used     int64
	Capacity int64
}

// IPSpaceAPI reports Pod/Service CIDR utilization per subnet.
// Point-in-time only; consumption rate lives in the capacity source
// (§10).
type IPSpaceAPI interface {
	SubnetUtilization(ctx context.Context) ([]SubnetUtilization, error)
}

// Stockout is one capacity-stockout record (GKE:
// ZONE_RESOURCE_POOL_EXHAUSTED from Cloud Logging).
type Stockout struct {
	Time        time.Time
	Zone        string
	MachineType string
	Message     string
}

// StockoutAPI extracts stockout records for a window (`cloud
// stockout`; the resident version with history is the capacity
// source, §10).
type StockoutAPI interface {
	Stockouts(ctx context.Context, w TimeWindow) ([]Stockout, error)
}

// WIProblem* are the machine-matchable problem codes a WIBinding
// carries when !Bound. Checks branch on these codes, never on prose
// (§8: no parsing of human wording).
const (
	// WIProblemIdentityMissing: the claimed cloud identity does not
	// exist (GKE: the annotated GSA is deleted or never existed).
	WIProblemIdentityMissing = "cloud-identity-missing"
	// WIProblemNoBinding: the identity exists, but the cluster
	// identity is not authorized to act as it (GKE: no
	// roles/iam.workloadIdentityUser member for the KSA).
	WIProblemNoBinding = "no-workload-identity-binding"
)

// WIBinding is the verification result for one service account's
// workload-identity binding (`state wi`).
type WIBinding struct {
	Namespace      string
	ServiceAccount string
	// CloudIdentity is the cloud principal the cluster identity
	// CLAIMS (GKE: the GSA email from the KSA annotation; EKS
	// analog: the IAM role ARN) — echoed back whether or not the
	// claim verifies.
	CloudIdentity string
	Bound         bool
	// Problems enumerates what is broken when !Bound. Each entry —
	// Problems[0] in particular — leads with one of the WIProblem*
	// codes, optionally followed by ": <detail>" for the operator.
	Problems []string
}

// WorkloadIdentityAPI verifies KSA↔cloud-identity bindings.
type WorkloadIdentityAPI interface {
	// VerifyBinding verifies that the cluster identity
	// namespace/serviceAccount is authorized to act as cloudIdentity
	// (GKE: the GSA email from the KSA's iam.gke.io/gcp-service-account
	// annotation; EKS analog: the IAM role ARN). The caller supplies
	// the claimed identity — the provider verifies the claim, it
	// cannot discover it.
	VerifyBinding(ctx context.Context, namespace, serviceAccount, cloudIdentity string) (WIBinding, error)
}
