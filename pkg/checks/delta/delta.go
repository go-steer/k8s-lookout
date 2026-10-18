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
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/kube"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Finding classes, toggled by --only. Each class maps to a
// dot-namespaced prefix family in the emitted finding kinds.
const (
	classPods   = "pods"   // pod.*, workload.*, job.*, cron.*
	classNodes  = "nodes"  // node.*
	classPDB    = "pdb"    // pdb.*
	classSystem = "system" // addon.*
	classQuota  = "quota"  // quota.*
	classHPA    = "hpa"    // hpa.*
)

// allClasses is the --only default, in scan order.
var allClasses = []string{classPods, classNodes, classPDB, classSystem, classQuota, classHPA}

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
		Name:        "triage delta",
		MCPName:     "k8s_triage_delta",
		MCPProfiles: []string{"triage"},
		Summary:     "Every abnormal object in one scan — the first call for \"anything wrong in this cluster?\": broken/pending pods, stalled rollouts, workloads blocked from creating pods at all, node pressure/NPD/preemption, gridlocked PDBs, degraded kube-system add-ons, quotas at their limits, autoscalers that cannot scale.",
		Flags: []emit.FlagSpec{
			{Name: "only", Type: emit.FlagString, Default: strings.Join(allClasses, ","),
				Help: "comma-separated finding classes to scan: any subset of pods,nodes,pdb,system,quota,hpa"},
			{Name: "restarts", Type: emit.FlagInt, Default: "5",
				Help: "flag containers restarted at least this many times"},
			{Name: "pending-age", Type: emit.FlagDuration, Default: "5m",
				Help: "flag Pending pods older than this; also the grace before a not-ready container in a Running pod is flagged"},
			{Name: "quota-warn", Type: emit.FlagInt, Default: "90",
				Help: "warn when a ResourceQuota resource reaches this percent of its hard limit (the hard limit itself is always critical)"},
			{Name: "cron-grace", Type: emit.FlagDuration, Default: "5m",
				Help: "how late a CronJob activation may be before it counts as missed; absorbs normal controller scheduling latency"},
			{Name: "hpa-grace", Type: emit.FlagDuration, Default: "5m",
				Help: "how long an HPA's AbleToScale or ScalingActive condition must have been False before it is flagged; absorbs metrics-server blips"},
		},
		Kinds: []checks.KindField{
			// Not "is in CrashLoopBackOff": the kubelet only wears
			// that label for part of each restart cycle, and this
			// finding deliberately covers the whole cycle: waiting,
			// terminated (#403), and restarted-but-briefly-running
			// (#522).
			checks.Kind("pod.crashloop", "a container is crash looping", emit.SeverityCritical),
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
			checks.Kind("cron.missed", "an unsuspended CronJob's schedule said to run more than --cron-grace ago and status says it did not; critical once several activations in a row are gone", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("cron.unparseable", "a CronJob's spec.schedule could not be parsed, so its activations cannot be judged at all", emit.SeverityWarning),
			checks.Kind("node.notready", "the node's Ready condition is not True", emit.SeverityCritical),
			checks.Kind("node.pressure", "the node reports Memory/Disk/PID pressure", emit.SeverityCritical),
			checks.Kind("node.condition", "a non-standard node condition is True — NPD and its cousins publish problems that way", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("node.cordoned", "the node is unschedulable but still holds pods: a stuck drain or a forgotten maintenance step", emit.SeverityWarning),
			checks.Kind("node.preempt", "a reclaim taint marks the node for termination; severity tracks how imminent", emit.SeverityCritical, emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind("pdb.gridlocked", "the budget permits no disruptions; critical when healthy pods are already below the required minimum", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("addon.degraded", "a kube-system add-on (dns, proxy, cni, csi, metrics, connectivity) is short of replicas; critical when none are available", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("quota.near", "a ResourceQuota resource is at or past --quota-warn percent of its hard limit", emit.SeverityWarning),
			checks.Kind("quota.exhausted", "a ResourceQuota resource is at its hard limit: the next create is rejected", emit.SeverityCritical),
			checks.Kind("hpa.scale_failed", "an HPA's AbleToScale condition has been False past --hpa-grace: the controller cannot read or write its target's scale", emit.SeverityWarning),
			checks.Kind("hpa.scaling_inactive", "an HPA's ScalingActive condition has been False past --hpa-grace (a failed metric fetch, an invalid selector; not a deliberate scale-to-zero): it cannot compute a replica count", emit.SeverityWarning),
			checks.Kind(KindAutopilotIdle, "an Autopilot cluster with no workloads of its own, scaled to 0 nodes by design: its GKE system pods wait for the first workload, and this one finding replaces every finding that state causes in GKE-managed namespaces", emit.SeverityInfo),
			checks.Kind(kindScaleUpBlocked, "pods are unschedulable and this says why the cluster autoscaler has not added a node: the node pools that cannot grow (autoscaling off, maximum 0) and the autoscaler's own recent refusals, from the cloud provider; critical when no pool can grow, when the only pools that can grow have no node the pods could use (a compute class's pools, for pods that select no class) and node auto-provisioning is off, or when no node is Ready; warning when a pool is blocked or the autoscaler refused; info when neither. With Nodes and Pods read cluster-wide it also accounts for each node against the pending pods: the taints they do not tolerate (one set on a node alone, with who set it and when), the room left where they could go, the largest requests there, and DaemonSets created in the last 7 days", emit.SeverityCritical, emit.SeverityWarning, emit.SeverityInfo),
			checks.UnreadKind(),
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
			{Name: "schedule", Doc: "a CronJob's spec.schedule"},
			{Name: "expected", Doc: "the activation a CronJob should have run and did not"},
			{Name: "missed_runs", Doc: "activations missed since the anchor; ≥N when the walk was capped"},
			{Name: "anchor", Doc: "what the missed count was measured from: last_schedule or creation"},
			{Name: "time_zone", Doc: "a CronJob's spec.timeZone, when set"},
			{Name: "last_schedule", Doc: "a CronJob's status.lastScheduleTime, or never"},
			{Name: "active_jobs", Doc: "Jobs a CronJob still has running"},
			{Name: "condition", Doc: "node condition type that is abnormal; for an HPA, the False condition (AbleToScale=False or ScalingActive=False)"},
			{Name: "taint", Doc: "taint key indicating reclaim/drain"},
			{Name: "pods", Doc: "pods affected (behind a cordoned node or a PDB)"},
			{Name: "healthy", Doc: "currently healthy pods behind a PDB"},
			{Name: "required", Doc: "pods the PDB requires healthy"},
			{Name: "addon", Doc: "system add-on role: dns, proxy, cni, csi, metrics, connectivity"},
			{Name: "resource", Doc: "ResourceQuota resource name at or near its limit"},
			{Name: "used", Doc: "quota usage from status"},
			{Name: "hard", Doc: "quota hard limit from status"},
			{Name: "pct", Doc: "quota usage as percent of the hard limit"},
			{Name: "scale_target", Doc: "an HPA's scaleTargetRef as Kind/name"},
			{Name: "replicas", Doc: "an HPA's current replica count, where the failure holds it"},
			{Name: "audit_reason", Doc: "the audit.hpa_cannot_scale reason (HPATargetMissing, HPATargetMissingRequests) that is the structural cause of this failure, when the controller's message identifies one"},
			{Name: "nodes", Doc: "on autoscaler.scaleup_blocked and cluster.autopilot_idle: Ready/total Nodes in the cluster, when nodes were read"},
			{Name: "autopilot", Doc: "on autoscaler.scaleup_blocked and cluster.autopilot_idle: true for a GKE Autopilot cluster, whose capacity the provider manages"},
			{Name: "pools", Doc: "on autoscaler.scaleup_blocked: every node pool as name (autoscaling bounds, node count, why it cannot grow)"},
			{Name: "no_scale_up", Doc: "on autoscaler.scaleup_blocked: the autoscaler's noScaleUp refusals in the window, as [pool: ]reason ×count, most frequent first"},
			{Name: "window", Doc: "on autoscaler.scaleup_blocked: how far back autoscaler decisions were read"},
			{Name: "decisions", Doc: "on autoscaler.scaleup_blocked: why the autoscaler's decisions could not be read, when they could not"},
			{Name: "auto_provisioning", Doc: "on autoscaler.scaleup_blocked: enabled|disabled, whether the autoscaler may create new node pools for pods no pool fits (GKE node auto-provisioning). Disabled, pods that select no compute class can only land in the pools that exist; a compute class's pools take only pods that select it"},
			{Name: "taints", Doc: "on autoscaler.scaleup_blocked: the node taints no pending pod tolerates, as key=value:effect on the nodes carrying it, operator-set taints first; a compute class's taint says only pods selecting the class may use those nodes. The message names a taint set on a node alone rather than by its pool, with the managedFields manager and time that last wrote the node's taints"},
			{Name: "full_nodes", Doc: "on autoscaler.scaleup_blocked: nodes the pending pods could otherwise use but lack room on, with CPU and memory free of allocatable by requests, and the smallest pending request"},
			{Name: "largest_requests", Doc: "on autoscaler.scaleup_blocked: the largest CPU requests on those full nodes, a DaemonSet named once (marked when created within the last 7 days)"},
			{Name: "new_daemonsets", Doc: "on autoscaler.scaleup_blocked: DaemonSets created within the last 7 days, with their per-pod request, node count and age — capacity taken from every node"},
			{Name: "folded", Doc: "on cluster.autopilot_idle: how many findings the idle state explains, replaced by this one"},
			{Name: "namespaces", Doc: "on cluster.autopilot_idle: the GKE-managed namespaces those findings were in"},
			{Name: "autoscaler", Doc: "summary note: why unschedulable pods could not be explained (no cloud provider, or no permission to read node pools)"},
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
	// provider yields the cloud provider, consulted only to explain
	// unschedulable pods (scaleup.go); nil means cloud.New.
	provider func(ctx context.Context) (cloud.Provider, error)
}

func (d *delta) cloudProvider(ctx context.Context) (cloud.Provider, error) {
	if d.provider != nil {
		return d.provider(ctx)
	}
	return cloud.New(ctx, cloud.Config{})
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
		cronGrace:  inv.Flags.Duration("cron-grace"),
		hpaGrace:   inv.Flags.Duration("hpa-grace"),
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
	// Zero is allowed and meaningful: judge activations the instant
	// they come due. Negative would report activations still in the
	// future as missed.
	if th.cronGrace < 0 {
		return 0, emit.UsageErrorf("--cron-grace must not be negative, got %s", th.cronGrace)
	}
	if th.hpaGrace < 0 {
		return 0, emit.UsageErrorf("--hpa-grace must not be negative, got %s", th.hpaGrace)
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

	// Tolerant (#546): a part whose List RBAC refuses drops out alone,
	// named by one read.unavailable record leading the answer — under
	// the built-in `view` role that is the nodes class, which `view`
	// never grants. Every other class still answers.
	s := &scanner{client: client, ns: ns, now: d.now(), th: th, classes: classes, tolerate: true}
	scanned, findings, err := s.scan(ctx)
	if err != nil {
		return 0, err
	}
	for _, f := range s.unreadFindings() {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}

	// Pods only a new node can help: say why the autoscaler has not
	// added one (#733). The provider is asked only when there are such
	// pods, and an answer it cannot give is a summary note, never
	// silence.
	if n := unschedulable(findings); n > 0 {
		if p, err := d.cloudProvider(ctx); err != nil {
			if err := inv.Out.Note(noteAutoscaler, "unavailable: "+err.Error()); err != nil {
				return 0, err
			}
		} else {
			var note string
			findings, note = explainUnschedulable(ctx, p, findings, n, s.view(), d.now())
			_ = p.Close()
			if note != "" {
				if err := inv.Out.Note(noteAutoscaler, note); err != nil {
					return 0, err
				}
			}
		}
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
	cronGrace  time.Duration
	hpaGrace   time.Duration
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
