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
}

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
type OrphanAPI interface {
	OrphanDisks(ctx context.Context) ([]OrphanDisk, error)
	OrphanLoadBalancers(ctx context.Context) ([]OrphanLoadBalancer, error)
}

// SubnetUtilization is IP usage for one subnet range (`cloud ipspace`).
type SubnetUtilization struct {
	Subnet string
	CIDR   string
	// Purpose is what the range allocates: "pods", "services", "nodes".
	Purpose  string
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

// WIBinding is the verification result for one service account's
// workload-identity binding (`state wi`).
type WIBinding struct {
	Namespace      string
	ServiceAccount string
	// CloudIdentity is the bound cloud principal (GKE: GSA email;
	// EKS analog: IAM role ARN).
	CloudIdentity string
	Bound         bool
	// Problems enumerates what is broken when !Bound (missing
	// annotation, missing IAM binding, …).
	Problems []string
}

// WorkloadIdentityAPI verifies KSA↔cloud-identity bindings.
type WorkloadIdentityAPI interface {
	VerifyBinding(ctx context.Context, namespace, serviceAccount string) (WIBinding, error)
}
