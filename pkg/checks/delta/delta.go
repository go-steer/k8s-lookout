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

// Package delta implements `lookout triage delta` (DESIGN.md §5):
// one scan of the cluster's current state that reports every
// abnormal object and nothing else. It absorbs v2's workload-delta,
// node-pressure-sifter, disruption-budget-analyzer, kernel-sentry,
// and spot-countdown, plus the two classes the health-check review
// added: degraded kube-system add-ons and namespaces at
// ResourceQuota limits.
//
// The scan is one paged List pass per resource kind against the
// live API server — no informers, no cache; this is a one-shot
// read. Healthy objects emit nothing (§1 principle 5); the
// mandatory summary line makes an all-healthy scan explicit.
package delta

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/kube"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Finding classes, toggled by --only. Each class maps to a
// dot-namespaced prefix family in the emitted finding kinds.
const (
	classPods   = "pods"   // pod.*, workload.*, job.*
	classNodes  = "nodes"  // node.*
	classPDB    = "pdb"    // pdb.*
	classSystem = "system" // addon.*
	classQuota  = "quota"  // quota.*
)

// allClasses is the --only default, in scan order.
var allClasses = []string{classPods, classNodes, classPDB, classSystem, classQuota}

func init() {
	checks.Register(New(kube.DefaultSource()))
}

// New returns the `triage delta` command bound to a client source.
func New(source kube.ClientSource) checks.Command {
	return newCommand(source, time.Now)
}

// newCommand additionally injects the clock; tests pin it so ages
// and durations in findings are golden-testable.
func newCommand(source kube.ClientSource, now func() time.Time) checks.Command {
	d := &delta{source: source, now: now}
	return checks.Command{
		Name:    "triage delta",
		MCPName: "k8s_triage_delta",
		Summary: "Every abnormal object in one scan — the first call for \"anything wrong in this cluster?\": broken/pending pods, stalled rollouts, workloads blocked from creating pods at all, node pressure/NPD/preemption, gridlocked PDBs, degraded kube-system add-ons, quotas at their limits.",
		Flags: []emit.FlagSpec{
			{Name: "only", Type: emit.FlagString, Default: strings.Join(allClasses, ","),
				Help: "comma-separated finding classes to scan: any subset of pods,nodes,pdb,system,quota"},
			{Name: "restarts", Type: emit.FlagInt, Default: "5",
				Help: "flag containers restarted at least this many times"},
			{Name: "pending-age", Type: emit.FlagDuration, Default: "5m",
				Help: "flag Pending pods older than this; also the grace before a not-ready container in a Running pod is flagged"},
			{Name: "quota-warn", Type: emit.FlagInt, Default: "90",
				Help: "warn when a ResourceQuota resource reaches this percent of its hard limit (the hard limit itself is always critical)"},
		},
		Kinds: []checks.KindField{
			checks.Kind("pod.crashloop", "a container is in CrashLoopBackOff", emit.SeverityCritical),
			checks.Kind("pod.imagepull", "a container cannot pull its image", emit.SeverityCritical),
			checks.Kind("pod.waiting", "a container is stuck in an error waiting state (CreateContainerConfigError, InvalidImageName, …)", emit.SeverityWarning),
			checks.Kind("pod.oomkilled", "a container's last termination was an OOM kill", emit.SeverityWarning),
			checks.Kind("pod.restarts", "a container has restarted at least --restarts times", emit.SeverityWarning),
			checks.Kind("pod.notready", "a container in a Running pod has been not-ready past the --pending-age grace", emit.SeverityWarning),
			checks.Kind("pod.failed", "the pod reached phase Failed", emit.SeverityWarning),
			checks.Kind("pod.pending", "the pod has been Pending longer than --pending-age with no container-level diagnosis; critical when the scheduler has declared it Unschedulable, which is a capacity or constraint problem rather than latency", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("workload.replicafailure", "the controller cannot create pods at all (quota, PodSecurity, admission) — no pod exists to diagnose", emit.SeverityCritical),
			checks.Kind("workload.stalled", "a Deployment's Progressing condition is False: the rollout has given up", emit.SeverityCritical),
			checks.Kind("workload.rollout", "replicas are short of desired; critical when nothing is serving at all", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("job.failed", "a Job's Failed condition is set", emit.SeverityWarning),
			checks.Kind("node.notready", "the node's Ready condition is not True", emit.SeverityCritical),
			checks.Kind("node.pressure", "the node reports Memory/Disk/PID pressure", emit.SeverityCritical),
			checks.Kind("node.condition", "a non-standard node condition is True — NPD and its cousins publish problems that way", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("node.cordoned", "the node is unschedulable but still holds pods: a stuck drain or a forgotten maintenance step", emit.SeverityWarning),
			checks.Kind("node.preempt", "a reclaim taint marks the node for termination; severity tracks how imminent", emit.SeverityCritical, emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind("pdb.gridlocked", "the budget permits no disruptions; critical when healthy pods are already below the required minimum", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("addon.degraded", "a kube-system add-on (dns, proxy, cni, csi, metrics, connectivity) is short of replicas; critical when none are available", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("quota.near", "a ResourceQuota resource is at or past --quota-warn percent of its hard limit", emit.SeverityWarning),
			checks.Kind("quota.exhausted", "a ResourceQuota resource is at its hard limit: the next create is rejected", emit.SeverityCritical),
		},
		Output: []checks.OutputField{
			{Name: "container", Doc: "container the finding is about (init containers prefixed init:)"},
			{Name: "image", Doc: "image reference that failed to pull"},
			{Name: "restarts", Doc: "container restart count"},
			{Name: "exit_code", Doc: "exit code of the container's last termination"},
			{Name: "last_state", Doc: "reason of the container's last termination (e.g. OOMKilled)"},
			{Name: "age", Doc: "how long the abnormal state has persisted"},
			{Name: "desired", Doc: "desired replica/scheduled count from spec"},
			{Name: "ready", Doc: "ready count from status"},
			{Name: "updated", Doc: "updated-to-current-revision count from status"},
			{Name: "available", Doc: "available count from status"},
			{Name: "failed", Doc: "failed pod count of a Job"},
			{Name: "condition", Doc: "node condition type that is abnormal"},
			{Name: "taint", Doc: "taint key indicating reclaim/drain"},
			{Name: "pods", Doc: "pods affected (behind a cordoned node or a PDB)"},
			{Name: "healthy", Doc: "currently healthy pods behind a PDB"},
			{Name: "required", Doc: "pods the PDB requires healthy"},
			{Name: "addon", Doc: "system add-on role: dns, proxy, cni, csi, metrics, connectivity"},
			{Name: "resource", Doc: "ResourceQuota resource name at or near its limit"},
			{Name: "used", Doc: "quota usage from status"},
			{Name: "hard", Doc: "quota hard limit from status"},
			{Name: "pct", Doc: "quota usage as percent of the hard limit"},
		},
		Examples: []string{
			"lookout triage delta",
			"lookout triage delta --namespace=prod --only=pods,quota",
			"lookout triage delta --only=nodes --format=json",
		},
		Run: d.run,
	}
}

// delta carries the injected seams of one command instance.
type delta struct {
	source kube.ClientSource
	now    func() time.Time
}

// run is the CheckFunc: parse flags, list once per resource kind,
// collect findings, and emit them critical-first.
func (d *delta) run(ctx context.Context, inv emit.Invocation) (int, error) {
	classes, err := parseOnly(inv.Flags.String("only"))
	if err != nil {
		return 0, err
	}
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("--workload is not supported by triage delta (scan a namespace, or use bundle for one workload)")
	}
	th := thresholds{
		restarts:   inv.Flags.Int("restarts"),
		pendingAge: inv.Flags.Duration("pending-age"),
		quotaWarn:  inv.Flags.Int("quota-warn"),
	}
	if th.restarts < 1 {
		return 0, emit.UsageErrorf("--restarts must be at least 1, got %d", th.restarts)
	}
	if th.pendingAge <= 0 {
		return 0, emit.UsageErrorf("--pending-age must be positive, got %s", th.pendingAge)
	}
	if th.quotaWarn < 1 || th.quotaWarn > 100 {
		return 0, emit.UsageErrorf("--quota-warn must be a percentage in 1..100, got %d", th.quotaWarn)
	}

	client, err := d.source(ctx)
	if err != nil {
		return 0, err
	}

	// --namespace scopes every namespaced list; "" (the default,
	// same as -A) scans the whole cluster — "show me everything
	// abnormal" is the mission, so the widest scope is the default.
	ns := inv.Scope.Namespace
	if inv.Scope.AllNamespaces {
		ns = metav1.NamespaceAll
	}

	s := &scanner{client: client, ns: ns, now: d.now(), th: th, classes: classes}
	scanned, findings, err := s.scan(ctx)
	if err != nil {
		return 0, err
	}

	sortFindings(findings)
	for _, f := range findings {
		// §8 push/pull dedup key (docs/signal-schema-v1.md): every
		// symptom-class finding carries the same fingerprint the
		// sentinel would stamp on a push for this breakage, so fleet rollup and
		// the §9.4 join never double-count a symptom reported by both
		// paths. Findings without an incident-class identity
		// (reason+object) stay fingerprint-free — zero nominal state.
		if f.Reason != "" && f.KindOfObject != "" {
			f.Fingerprint = engine.ScanFingerprint(f.Reason, f.KindOfObject, "")
		}
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	return scanned, nil
}

// thresholds are the tunable abnormality cutoffs.
type thresholds struct {
	restarts   int
	pendingAge time.Duration
	quotaWarn  int
}

// parseOnly validates the --only list against the known classes.
func parseOnly(s string) (map[string]bool, error) {
	known := map[string]bool{}
	for _, c := range allClasses {
		known[c] = true
	}
	out := map[string]bool{}
	for _, tok := range strings.Split(s, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		if !known[tok] {
			return nil, emit.UsageErrorf("--only: unknown class %q (want a subset of %s)", tok, strings.Join(allClasses, ","))
		}
		out[tok] = true
	}
	if len(out) == 0 {
		return nil, emit.UsageErrorf("--only: no classes selected (want a subset of %s)", strings.Join(allClasses, ","))
	}
	return out, nil
}

// severityRank orders findings critical-first; unknown severities
// sink to the bottom rather than panicking.
func severityRank(sev string) int {
	switch sev {
	case emit.SeverityCritical:
		return 0
	case emit.SeverityWarning:
		return 1
	case emit.SeverityInfo:
		return 2
	}
	return 3
}

// sortFindings orders by severity rank, then namespace/name, then
// kind and details for a fully deterministic stream.
func sortFindings(fs []emit.Finding) {
	key := func(f emit.Finding) string {
		var b strings.Builder
		fmt.Fprintf(&b, "%d\x00%s\x00%s\x00%s\x00%s", severityRank(f.Severity), f.Namespace, f.Name, f.Kind, f.KindOfObject)
		for _, d := range f.Details {
			b.WriteString("\x00" + d.Key + "=" + d.Value)
		}
		return b.String()
	}
	sort.Slice(fs, func(i, j int) bool { return key(fs[i]) < key(fs[j]) })
}
