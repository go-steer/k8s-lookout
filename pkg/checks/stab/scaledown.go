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

package stab

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// kindScaledownBlocked is the one condition `stab scaledown` reports.
const kindScaledownBlocked = "scaledown.blocked"

// defaultScaledownUtilization is the --utilization default, in percent
// of allocatable. It is not lookout's number: it is the cluster
// autoscaler's own --scale-down-utilization-threshold default (0.5),
// below which the autoscaler considers a node a removal candidate. A
// cluster that tunes that flag should pass its value here.
const defaultScaledownUtilization = 50

// The cluster-autoscaler API strings this check reads. Each is the
// autoscaler's published contract, not a lookout convention.
const (
	// annotationSafeToEvict on a pod overrides the autoscaler's
	// judgment of it: "false" pins the node, "true" releases a pod the
	// autoscaler would otherwise refuse to move (a bare pod).
	annotationSafeToEvict = "cluster-autoscaler.kubernetes.io/safe-to-evict"
	// annotationScaleDownDisabled on a node takes it out of scale-down
	// on purpose.
	annotationScaleDownDisabled = "cluster-autoscaler.kubernetes.io/scale-down-disabled"
	// taintToBeDeleted is the taint the autoscaler puts on a node it is
	// already removing.
	taintToBeDeleted = "ToBeDeletedByClusterAutoscaler"
	// resourceGPU: on a node that offers GPUs the autoscaler judges
	// utilization by GPU requests alone.
	resourceGPU corev1.ResourceName = "nvidia.com/gpu"
)

// controlPlaneLabels mark nodes outside every autoscaled node group.
var controlPlaneLabels = []string{"node-role.kubernetes.io/control-plane", "node-role.kubernetes.io/master"}

// nodePoolLabels name the node's pool, for the cost rollup; the first
// one present wins.
var nodePoolLabels = []string{"cloud.google.com/gke-nodepool", "eks.amazonaws.com/nodegroup", "kubernetes.azure.com/agentpool"}

// ScaledownCommand builds `lookout stab scaledown` (#231, the
// fleet-audit `scaledown-blocked` slug): the nodes the cluster
// autoscaler would remove for being underused, and cannot.
//
// It is a cost-framed read of the `stab drain` index, not a second
// blocker engine. The drain analysis already classifies every pod on a
// node; this command keeps the classes the autoscaler also refuses to
// move and adds the two facts that make it a cost claim:
//
//   - **Underused**, by the autoscaler's own arithmetic: the sum of the
//     REQUESTS of every non-terminal pod bound to the node (DaemonSet
//     and mirror pods included, as the autoscaler counts them by
//     default) over allocatable, taking the larger of CPU and memory —
//     or GPU alone on a node that offers GPUs. Requests, not usage:
//     that is what the autoscaler measures, so it is what decides
//     whether a node is a candidate. Below --utilization (default 50%,
//     the autoscaler's default threshold) it is a candidate.
//   - **Blocked**, by the classes the autoscaler and a drain agree on:
//     a PodDisruptionBudget allowing zero disruptions over a pod on the
//     node (drain.pdb_gridlock), and a pod with no controller
//     (drain.bare_pod) — unless that pod is annotated safe-to-evict
//     "true", which releases it for the autoscaler. Plus the one class
//     that is the autoscaler's alone: a pod annotated safe-to-evict
//     "false".
//
// Deliberately not counted, so the error runs toward silence:
// emptyDir data (drain.local_storage) blocks the autoscaler only under
// its --skip-nodes-with-local-storage flag, whose value differs between
// autoscaler deployments and is not readable from the API; a
// single-replica pod (drain.singleton) is a drain hazard but not an
// autoscaler blocker; and kube-system pods without a PDB, and pods
// whose scheduling constraints leave nowhere to move them, depend on
// autoscaler flags and a scheduling simulation lookout does not run.
// A node pool already at its minimum size cannot shrink either, but
// that is provider configuration the API does not show — read a
// finding as "pods pin this node", not "the node would otherwise go".
//
// Nodes the autoscaler never removes make no claim and are counted as
// excluded: control-plane nodes, nodes annotated scale-down-disabled
// (a decision someone made), and nodes already tainted for deletion.
//
// The command assumes what its name says — that the cluster runs an
// autoscaler. On a fixed-size cluster the same finding still names a
// node billed at low utilization that pods prevent anyone from
// consolidating, but nothing would have removed it automatically.
func ScaledownCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "stab scaledown",
		MCPName: "k8s_scaledown_blockers",
		Summary: "On a cluster-autoscaled cluster, which underused nodes the autoscaler cannot remove and what pins each one — a cost read: nodes requested below --utilization (the autoscaler's own 50% default) that carry a zero-disruption PDB, a controller-less pod, or a safe-to-evict=false pod; scanned counts nodes examined.",
		Flags: []emit.FlagSpec{
			{Name: "utilization", Type: emit.FlagInt, Default: fmt.Sprint(defaultScaledownUtilization),
				Help: "percent of allocatable (requests, max of CPU and memory; GPU alone on GPU nodes) below which a node counts as underused; default 50 is the cluster autoscaler's own --scale-down-utilization-threshold, so match it if yours is tuned (1-100)"},
		},
		Kinds: []checks.KindField{
			checks.Kind(kindScaledownBlocked, "the node is requested below --utilization, so the autoscaler would remove it, but a pod on it cannot be moved — a zero-disruption PDB, a pod with no controller, or a pod annotated safe-to-evict=false — so it keeps billing at low utilization", emit.SeverityWarning),
			checks.UnreadKind(),
		},
		Output: append([]checks.OutputField{
			{Name: "utilization", Doc: "the node's requested share of allocatable on the deciding resource, whole percent, rounded down"},
			{Name: "basis", Doc: "the deciding resource: cpu or memory (whichever is higher), or gpu on a node that offers GPUs"},
			{Name: "threshold", Doc: "the --utilization the node was judged against, in percent"},
			{Name: "nodepool", Doc: "the node's pool, from the provider's pool label; omitted when the node carries none"},
			{Name: "blockers", Doc: "pods and PDBs pinning the node, counted"},
			{Name: "pdb_gridlock", Doc: "PodDisruptionBudgets allowing zero disruptions over a pod on the node; omitted at 0"},
			{Name: "bare_pods", Doc: "pods with no controller and no safe-to-evict=true annotation; omitted at 0"},
			{Name: "not_safe_to_evict", Doc: "pods annotated cluster-autoscaler.kubernetes.io/safe-to-evict=false; omitted at 0"},
			{Name: "blocked_by", Doc: "the pinning objects as <Kind>/<namespace>/<name>, sorted, capped at 8 with a +N more tail"},
			{Name: "nodes", Doc: "summary note: nodes listed"},
			{Name: "excluded", Doc: "summary note: nodes the autoscaler never removes and so were not judged — control-plane, annotated scale-down-disabled, or already tainted for deletion"},
			{Name: "underused", Doc: "summary note: judged nodes below --utilization, blocked or not"},
		}, checks.UnreadFields()...),
		Examples: []string{
			"lookout stab scaledown",
			"lookout stab scaledown --utilization=65",
			"lookout stab scaledown --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runScaledown(ctx, deps, inv)
		},
	}
}

func runScaledown(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	// Nodes are cluster-scoped and so is the autoscaler's judgment of
	// them: a namespace or workload scope would silently drop the pods
	// that pin a node, and -A would claim a widening that is not one.
	if inv.Scope.Namespace != "" || inv.Scope.AllNamespaces || !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("stab scaledown judges whole nodes against every pod on them, so it takes no --namespace, -A or --workload")
	}
	threshold := inv.Flags.Int("utilization")
	if threshold < 1 || threshold > 100 {
		return 0, emit.UsageErrorf("--utilization is a percent of allocatable between 1 and 100, got %d", threshold)
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	ix, err := listDrainIndex(ctx, client)
	if err != nil {
		return 0, err
	}
	// Utilization is requests over each Node's allocatable: without
	// the node List (the built-in `view` role grants none, #546) there
	// is no node to judge, so the answer is the one record and an
	// empty node set.
	if ix.nodesRefused != nil {
		if err := inv.Out.Emit(checks.RefusedFinding(*ix.nodesRefused,
			"no node was judged: utilization is requests over each Node's allocatable")); err != nil {
			return 0, err
		}
	}

	names := make([]string, 0, len(ix.nodeObjs))
	for n := range ix.nodeObjs {
		names = append(names, n)
	}
	sort.Strings(names)

	excluded, underused := 0, 0
	var findings []emit.Finding
	for _, name := range names {
		n := ix.nodeObjs[name]
		if scaledownExcluded(n) {
			excluded++
			continue
		}
		pct, basis, ok := nodeUtilization(n, ix.requested[name])
		if !ok || pct >= threshold {
			continue
		}
		underused++
		b := ix.scaledownBlockers(name)
		if b.total() == 0 {
			continue // underused and removable: the autoscaler's job, not a finding
		}
		findings = append(findings, b.finding(n, pct, basis, threshold))
	}
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	for _, note := range []struct{ k, v string }{
		{"nodes", itoa(len(names))},
		{"excluded", itoa(excluded)},
		{"underused", itoa(underused)},
	} {
		if err := inv.Out.Note(note.k, note.v); err != nil {
			return 0, err
		}
	}
	return len(names), nil
}

// scaledownExcluded reports nodes the autoscaler never removes.
func scaledownExcluded(n *corev1.Node) bool {
	for _, l := range controlPlaneLabels {
		if _, ok := n.Labels[l]; ok {
			return true
		}
	}
	if n.Annotations[annotationScaleDownDisabled] == "true" {
		return true
	}
	for _, t := range n.Spec.Taints {
		if t.Key == taintToBeDeleted {
			return true
		}
	}
	return false
}

// nodeUtilization is the autoscaler's utilization figure for a node:
// requested over allocatable, the larger of CPU and memory, or GPU
// alone where the node offers GPUs. ok is false when the node reports
// no allocatable for the deciding resources — nothing to divide by,
// so no claim.
func nodeUtilization(n *corev1.Node, req corev1.ResourceList) (pct int, basis string, ok bool) {
	alloc := n.Status.Allocatable
	if gpu, has := alloc[resourceGPU]; has && !gpu.IsZero() {
		r := req[resourceGPU]
		return int(r.MilliValue() * 100 / gpu.MilliValue()), "gpu", true
	}
	cpu, mem := alloc[corev1.ResourceCPU], alloc[corev1.ResourceMemory]
	if cpu.IsZero() || mem.IsZero() {
		return 0, "", false
	}
	rc, rm := req[corev1.ResourceCPU], req[corev1.ResourceMemory]
	cpuPct := int(rc.MilliValue() * 100 / cpu.MilliValue())
	// Memory in bytes: scale down before multiplying so a large node
	// cannot overflow int64.
	memPct := int(rm.Value() / 1024 * 100 / max(mem.Value()/1024, 1))
	if memPct > cpuPct {
		return memPct, "memory", true
	}
	return cpuPct, "cpu", true
}

// scaledownSet is one node's autoscaler blockers.
type scaledownSet struct {
	pdbGridlock, bare, notSafe int
	blockedBy                  []string
}

func (s scaledownSet) total() int { return s.pdbGridlock + s.bare + s.notSafe }

// scaledownBlockers keeps the drain blockers the autoscaler also
// honors and adds safe-to-evict=false. It reads drain's classification
// rather than re-deriving it, so the two commands cannot disagree
// about what a gridlocked PDB or a bare pod is.
func (ix *drainIndex) scaledownBlockers(node string) scaledownSet {
	var s scaledownSet
	pods := map[string]*corev1.Pod{}
	for _, p := range ix.podsByNode[node] {
		pods[p.Namespace+"/"+p.Name] = p
	}
	for _, f := range ix.nodeBlockers(node).findings {
		ref := f.Namespace + "/" + f.Name
		switch f.Kind {
		case "drain.pdb_gridlock":
			s.pdbGridlock++
			s.blockedBy = append(s.blockedBy, "PodDisruptionBudget/"+ref)
		case "drain.bare_pod":
			// An explicit annotation either way is the autoscaler's
			// answer for this pod: "true" releases it, "false" is
			// counted below as its own class.
			if p := pods[ref]; p != nil && p.Annotations[annotationSafeToEvict] != "" {
				continue
			}
			s.bare++
			s.blockedBy = append(s.blockedBy, "Pod/"+ref)
		}
	}
	for _, p := range ix.podsByNode[node] {
		if p.Annotations[annotationSafeToEvict] == "false" {
			s.notSafe++
			s.blockedBy = append(s.blockedBy, "Pod/"+p.Namespace+"/"+p.Name)
		}
	}
	sort.Strings(s.blockedBy)
	return s
}

func (s scaledownSet) finding(n *corev1.Node, pct int, basis string, threshold int) emit.Finding {
	var parts []string
	details := []emit.Field{
		{Key: "utilization", Value: itoa(pct)},
		{Key: "basis", Value: basis},
		{Key: "threshold", Value: itoa(threshold)},
	}
	for _, l := range nodePoolLabels {
		if pool := n.Labels[l]; pool != "" {
			details = append(details, emit.Field{Key: "nodepool", Value: pool})
			break
		}
	}
	details = append(details, emit.Field{Key: "blockers", Value: itoa(s.total())})
	class := func(key string, count int, phrase string) {
		if count == 0 {
			return
		}
		details = append(details, emit.Field{Key: key, Value: itoa(count)})
		parts = append(parts, fmt.Sprintf("%d %s", count, phrase))
	}
	class("pdb_gridlock", s.pdbGridlock, "zero-disruption PDB(s)")
	class("bare_pods", s.bare, "pod(s) with no controller")
	class("not_safe_to_evict", s.notSafe, "pod(s) annotated safe-to-evict=false")
	details = append(details, emit.Field{Key: "blocked_by", Value: cappedList(s.blockedBy)})
	return emit.Finding{
		Kind:         kindScaledownBlocked,
		Severity:     emit.SeverityWarning,
		KindOfObject: "Node",
		Name:         n.Name,
		Reason:       "ScaleDownBlocked",
		Message: fmt.Sprintf("node's %s is %d%% requested, under the %d%% scale-down threshold, but the autoscaler cannot remove it: %s — it keeps billing at low utilization",
			basis, pct, threshold, strings.Join(parts, ", ")),
		Details: details,
	}
}

// podRequests is a pod's effective request — what the scheduler, and
// so the autoscaler, reserves for it: pod-level requests where the pod
// sets them, else the regular containers plus restartable (sidecar)
// init containers, raised to what any one-shot init container needs
// beside the sidecars started before it, plus the runtime overhead.
func podRequests(p *corev1.Pod) corev1.ResourceList {
	out := corev1.ResourceList{}
	if p.Spec.Resources != nil && len(p.Spec.Resources.Requests) > 0 {
		addList(out, p.Spec.Resources.Requests)
		addList(out, p.Spec.Overhead)
		return out
	}
	for _, c := range p.Spec.Containers {
		addList(out, c.Resources.Requests)
	}
	sidecars := corev1.ResourceList{}
	for _, c := range p.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			addList(out, c.Resources.Requests)
			addList(sidecars, c.Resources.Requests)
			continue
		}
		need := corev1.ResourceList{}
		addList(need, sidecars)
		addList(need, c.Resources.Requests)
		for name, q := range need {
			if cur, ok := out[name]; !ok || q.Cmp(cur) > 0 {
				out[name] = q.DeepCopy()
			}
		}
	}
	addList(out, p.Spec.Overhead)
	return out
}

func addList(into, add corev1.ResourceList) {
	for name, q := range add {
		cur := into[name]
		cur.Add(q)
		into[name] = cur
	}
}

func addRequests(by map[string]corev1.ResourceList, node string, req corev1.ResourceList) {
	cur := by[node]
	if cur == nil {
		cur = corev1.ResourceList{}
		by[node] = cur
	}
	addList(cur, req)
}
