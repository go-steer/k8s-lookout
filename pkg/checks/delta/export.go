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
	"time"

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
}

// thresholds applies the flag defaults to zero fields.
func (c Config) thresholds() thresholds {
	th := thresholds{restarts: c.Restarts, pendingAge: c.PendingAge, quotaWarn: c.QuotaWarn, cronGrace: c.CronGrace}
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
// quota; empty = all). `health` (§5) delegates its delta-backed
// scorecard categories here. Returns the scanned count for the
// caller's summary line and the findings, sorted critical-first.
func ScanCluster(ctx context.Context, client kubernetes.Interface, ns string, now time.Time, cfg Config, classes ...string) (int, []emit.Finding, error) {
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
			return 0, nil, fmt.Errorf("unknown delta class %q", c)
		}
		sel[c] = true
	}
	s := &scanner{client: client, ns: ns, now: now, th: cfg.thresholds(), classes: sel}
	scanned, findings, err := s.scan(ctx)
	if err != nil {
		return 0, nil, err
	}
	sortFindings(findings)
	return scanned, findings, nil
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
