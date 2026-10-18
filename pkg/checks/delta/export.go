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

package delta

// Exported seams for the §5 composition commands (`bundle`,
// `health`): the same abnormality derivations `triage delta` runs,
// callable over objects the caller already listed. No API calls
// happen here — composition commands own their List pass and feed
// every consumer from it.

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/client-go/kubernetes"
)

// Config carries the abnormality thresholds. The zero value means
// the `triage delta` flag defaults.
type Config struct {
	// Restarts flags containers restarted at least this many times
	// (default 5).
	Restarts int
	// PendingAge is the age past which a Pending pod (or a
	// not-ready container in a Running pod) is flagged (default 5m).
	PendingAge time.Duration
	// QuotaWarn is the used/hard percentage that warns (default 90).
	QuotaWarn int
	// CronGrace is how late a CronJob activation may be before it
	// counts as missed (default 5m).
	CronGrace time.Duration
	// HPAGrace is how long an HPA condition must have been False
	// before it is flagged (default 5m).
	HPAGrace time.Duration
	// TolerateTransient degrades transient/API read errors on individual
	// delta parts into unavailable reasons when ScanClusterPartial is used.
	TolerateTransient bool
}

// thresholds applies the flag defaults to zero fields.
func (c Config) thresholds() thresholds {
	th := thresholds{restarts: c.Restarts, pendingAge: c.PendingAge, quotaWarn: c.QuotaWarn, cronGrace: c.CronGrace, hpaGrace: c.HPAGrace}
	if th.restarts == 0 {
		th.restarts = 5
	}
	if th.pendingAge == 0 {
		th.pendingAge = 5 * time.Minute
	}
	if th.quotaWarn == 0 {
		th.quotaWarn = 90
	}
	if th.cronGrace == 0 {
		th.cronGrace = 5 * time.Minute
	}
	if th.hpaGrace == 0 {
		th.hpaGrace = 5 * time.Minute
	}
	return th
}

// Objects are the already-listed inputs to ScanObjects. Nil slices
// simply contribute nothing; callers pass exactly what their scope
// contains.
type Objects struct {
	Pods         []corev1.Pod
	Deployments  []appsv1.Deployment
	StatefulSets []appsv1.StatefulSet
	DaemonSets   []appsv1.DaemonSet
	Jobs         []batchv1.Job
	CronJobs     []batchv1.CronJob
	Nodes        []corev1.Node
	PDBs         []policyv1.PodDisruptionBudget
	Quotas       []corev1.ResourceQuota
	// SystemAddons additionally runs the kube-system add-on checks
	// over Deployments/DaemonSets (addon.degraded); when false those
	// objects get the plain workload treatment. `bundle` scopes to
	// one workload and leaves this off; `health` turns it on.
	SystemAddons bool
}

// ScanCluster runs the full `triage delta` pass — the same paged
// Lists, the same derivations — over ns ("" = all namespaces) for
// the given finding classes (any subset of pods, nodes, pdb, system,
// quota, hpa; empty = all). `health` (§5) delegates its delta-backed
// scorecard categories here. Returns the scanned count for the
// caller's summary line and the findings, sorted critical-first.
func ScanCluster(ctx context.Context, client kubernetes.Interface, ns string, now time.Time, cfg Config, classes ...string) (int, []emit.Finding, error) {
	p, err := scanCluster(ctx, client, ns, now, cfg, false, classes)
	return p.Scanned, p.Findings, err
}

// ScanClusterPartial is ScanCluster for a least-privilege credential
// (#546): a part whose List the API server refuses (Forbidden) is
// dropped instead of failing the pass, and named in
// Partial.Unavailable as part (PartPods, PartWorkloads, PartNodes, …)
// → reason, the shared checks.Refusal wording ("forbidden: list nodes
// — cluster-scoped, not granted by the built-in view role; grant list
// on nodes …"). Under the built-in `view` role, which grants no Nodes,
// the nodes part is the one that drops. Any other error still fails.
func ScanClusterPartial(ctx context.Context, client kubernetes.Interface, ns string, now time.Time, cfg Config, classes ...string) (Partial, error) {
	return scanCluster(ctx, client, ns, now, cfg, true, classes)
}

// Partial is one ScanClusterPartial pass.
type Partial struct {
	// Scanned is the count for the caller's summary line.
	Scanned int
	// Findings are sorted critical-first.
	Findings []emit.Finding
	// Unavailable maps each dropped part to why.
	Unavailable map[string]string
	// view is what the pass listed, for ExplainUnschedulable.
	view clusterView
}

// ExplainUnschedulable is what `triage delta` does with unschedulable
// pods, for a composition that ran the pass itself: an idle Autopilot
// cluster's findings fold into the one KindAutopilotIdle finding that
// explains them (#735); otherwise they gain the
// autoscaler.scaleup_blocked explanation (#733). With no unschedulable
// pod the findings come back unchanged. note, when set, says why the
// provider could not answer (delta's `autoscaler` summary note), and
// the findings are then unchanged too.
func (p Partial) ExplainUnschedulable(ctx context.Context, provider cloud.Provider, now time.Time) (findings []emit.Finding, note string) {
	n := unschedulable(p.Findings)
	if n == 0 {
		return p.Findings, ""
	}
	findings, note = explainUnschedulable(ctx, provider, slices.Clone(p.Findings), n, p.view, now)
	sortFindings(findings)
	return findings, note
}

// Unschedulable counts the pods only a new node can help: when it is
// zero, ExplainUnschedulable has nothing to explain and needs no
// provider.
func (p Partial) Unschedulable() int { return unschedulable(p.Findings) }

func scanCluster(ctx context.Context, client kubernetes.Interface, ns string, now time.Time, cfg Config, tolerate bool, classes []string) (Partial, error) {
	sel := map[string]bool{}
	if len(classes) == 0 {
		classes = allClasses
	}
	known := map[string]bool{}
	for _, c := range allClasses {
		known[c] = true
	}
	for _, c := range classes {
		if !known[c] {
			return Partial{}, fmt.Errorf("unknown delta class %q", c)
		}
		sel[c] = true
	}
	s := &scanner{
		client:            client,
		ns:                ns,
		now:               now,
		th:                cfg.thresholds(),
		classes:           sel,
		tolerate:          tolerate,
		tolerateTransient: tolerate && cfg.TolerateTransient,
	}
	scanned, findings, err := s.scan(ctx)
	if err != nil {
		return Partial{}, err
	}
	sortFindings(findings)
	return Partial{Scanned: scanned, Findings: findings, Unavailable: s.unavailable, view: s.view()}, nil
}

// ScanObjects derives the delta findings from objs at time now,
// sorted critical-first exactly like `triage delta` emits them.
func ScanObjects(now time.Time, cfg Config, objs Objects) []emit.Finding {
	s := &scanner{now: now, th: cfg.thresholds()}
	s.checkPods(objs.Pods)
	s.checkWorkloads(objs.Deployments, objs.StatefulSets, objs.DaemonSets, objs.SystemAddons)
	s.checkJobs(objs.Jobs)
	s.checkCronJobs(objs.CronJobs)
	if objs.SystemAddons {
		s.checkSystem(objs.Deployments, objs.DaemonSets)
	}
	if len(objs.Nodes) > 0 {
		s.checkNodes(objs.Nodes, objs.Pods)
	}
	s.checkPDBs(objs.PDBs)
	s.checkQuotas(objs.Quotas)
	sortFindings(s.findings)
	return s.findings
}
