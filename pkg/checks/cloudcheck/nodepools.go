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

package cloudcheck

// The `cloud orphans --only=nodepools` class (#557, the fleet-audit
// `idle-nodepool` slug split from #231): a node pool that has nodes and
// nothing scheduled on them.
//
// # What "idle" means, and what it gave up
//
// Every other orphan class turns on a terminal state the provider
// reports (unattached, RESERVED with no users, zero endpoints). Idle
// does not, so the maintainer decision on #557 fixes the threshold:
//
//   - A pool is idle when it has at least one node and no WORKLOAD pod
//     runs on any of them. Every node runs DaemonSet pods (CNI, logging,
//     metadata agents) and some run static pods, so "zero pods" never
//     fires; DaemonSet-controlled pods and mirror pods (the API's
//     reflection of a static pod) are therefore not workload. Everything
//     else is — kube-system Deployments included, because the cluster
//     autoscaler will not drain them either. Pods that have terminated
//     (Succeeded/Failed) hold nothing and are not counted at all.
//   - It is judged on ONE observation. A one-shot read cannot measure an
//     idle duration: pods leave no "last scheduled" trace on a pool once
//     they are gone. The cost of that is a batch pool caught between
//     runs, which is why the kind is info and not warning.
//   - A pool at zero nodes costs nothing and stays silent, whatever its
//     configuration.
//   - Intentional headroom (a balloon Deployment's pause pods count as
//     workload already; a GPU pool kept warm with nothing on it does
//     not) is what the --exemptions file is for: kind=orphan.nodepool
//     with name=<pool> annotates the finding as reviewed.
//
// The remedy depends on the autoscaler, so each finding carries the
// pool's autoscaling state and floor and the reason names the case: a
// pool not autoscaled stays this size until someone resizes it; an
// autoscaled pool with a non-zero minimum is held up by that minimum;
// an autoscaled pool whose minimum is zero should shrink on its own, and
// if it does not, `stab scaledown` is the read that says why.
//
// On an Autopilot cluster the provider owns the pools and bills per pod,
// so the class sweeps nothing and says so in a summary note.
//
// The provider supplies the pools (cluster-config capability — the same
// clusters.get record `audit cluster` reads) and the cluster supplies
// what runs on them: Nodes joined to pools by their
// cloud.google.com/gke-nodepool label, and Pods by spec.nodeName. A
// refused clusters.get degrades the class like any refused sweep
// (cloud.unavailable, reason=PermissionDenied); a refused Node or Pod
// List degrades it with a read.unavailable record naming the resource,
// because guessing without either would call a busy pool idle.

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// kindIdleNodePool is the nodepools class's one claim.
const kindIdleNodePool = "orphan.nodepool"

// The orphan.nodepool reasons: which autoscaler case keeps the idle
// nodes billing, and therefore which remedy applies.
const (
	reasonIdleNotAutoscaled     = "IdleNotAutoscaled"
	reasonIdleMinNodeCount      = "IdleMinNodeCount"
	reasonIdleAwaitingScaleDown = "IdleAwaitingScaleDown"
)

// nodePoolLabel is the label GKE puts on every node naming its pool.
const nodePoolLabel = "cloud.google.com/gke-nodepool"

// mirrorPodAnnotation marks the API's mirror of a static pod.
const mirrorPodAnnotation = "kubernetes.io/config.mirror"

// nodePoolPageLimit is the Node/Pod List page size: a one-shot read
// keeps pages small to bound peak memory, as the state group does.
const nodePoolPageLimit = 500

// podPlacement is the only part of a pod the judgment reads.
type podPlacement struct {
	node     string
	workload bool
}

// poolTally is what the join found on one pool's nodes.
type poolTally struct {
	nodes, workload, excluded int
}

// sweepNodePools runs the nodepools class. It returns how many pools
// it examined and, when a cluster List was refused, the summary-note
// fragment saying so.
func sweepNodePools(ctx context.Context, api cloud.ClusterConfigAPI, deps Deps, inv emit.Invocation) (int, string, error) {
	cfg, err := api.Config(ctx)
	if err != nil {
		return 0, "", err
	}
	if cfg.Autopilot {
		return 0, "", inv.Out.Note("nodepools_skipped", "autopilot: node pools are provider-managed and billed per pod")
	}
	client, err := deps.client(ctx)
	if err != nil {
		return 0, "", err
	}
	nodePools, err := listNodePools(ctx, client)
	if r, forbidden := checks.ForbiddenRefusal(err); forbidden {
		return 0, "nodepools: " + r.Short(), emitNodePoolsUnread(inv, r)
	}
	if err != nil {
		return 0, "", err
	}
	pods, err := listPodPlacements(ctx, client)
	if r, forbidden := checks.ForbiddenRefusal(err); forbidden {
		return 0, "nodepools: " + r.Short(), emitNodePoolsUnread(inv, r)
	}
	if err != nil {
		return 0, "", err
	}
	for _, f := range judgeNodePools(cfg.NodePools, nodePools, pods) {
		if err := inv.Out.Emit(f); err != nil {
			return 0, "", err
		}
	}
	return len(cfg.NodePools), "", nil
}

// emitNodePoolsUnread is the #546 degradation record for a refused
// Node or Pod List: the judgment is skipped, not guessed.
func emitNodePoolsUnread(inv emit.Invocation, r checks.Refusal) error {
	return inv.Out.Emit(checks.RefusedFinding(r,
		"idle node-pool detection skipped: without every "+r.Target()+" a busy pool would be misreported as idle"))
}

// listNodePools pages through the Nodes, mapping each node name to the
// pool its label names. Nodes with no pool label (not GKE-managed) are
// left out: no provider pool can claim them.
func listNodePools(ctx context.Context, client kubernetes.Interface) (map[string]string, error) {
	out := map[string]string{}
	opts := metav1.ListOptions{Limit: nodePoolPageLimit}
	for {
		l, err := client.CoreV1().Nodes().List(ctx, opts)
		if err != nil {
			return nil, fmt.Errorf("listing nodes: %w", err)
		}
		for i := range l.Items {
			if pool := l.Items[i].Labels[nodePoolLabel]; pool != "" {
				out[l.Items[i].Name] = pool
			}
		}
		if l.Continue == "" {
			return out, nil
		}
		opts.Continue = l.Continue
	}
}

// listPodPlacements pages through every pod in the cluster, keeping
// only the scheduled, non-terminated ones and whether each is workload.
func listPodPlacements(ctx context.Context, client kubernetes.Interface) ([]podPlacement, error) {
	var out []podPlacement
	opts := metav1.ListOptions{Limit: nodePoolPageLimit}
	for {
		l, err := client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, opts)
		if err != nil {
			return nil, fmt.Errorf("listing pods: %w", err)
		}
		for i := range l.Items {
			p := &l.Items[i]
			if p.Spec.NodeName == "" || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
				continue
			}
			out = append(out, podPlacement{node: p.Spec.NodeName, workload: isWorkloadPod(p)})
		}
		if l.Continue == "" {
			return out, nil
		}
		opts.Continue = l.Continue
	}
}

// isWorkloadPod reports whether a pod is something a pool exists to
// run: not a mirror pod, and not controlled by a DaemonSet.
func isWorkloadPod(p *corev1.Pod) bool {
	if _, mirror := p.Annotations[mirrorPodAnnotation]; mirror {
		return false
	}
	if c := metav1.GetControllerOf(p); c != nil && c.Kind == "DaemonSet" {
		return false
	}
	return true
}

// judgeNodePools is the pure judgment: pools from the provider, the
// node→pool join, and the scheduled pods. One finding per idle pool,
// in pool-name order.
func judgeNodePools(pools []cloud.NodePoolConfig, nodePool map[string]string, pods []podPlacement) []emit.Finding {
	tally := map[string]*poolTally{}
	get := func(pool string) *poolTally {
		t, ok := tally[pool]
		if !ok {
			t = &poolTally{}
			tally[pool] = t
		}
		return t
	}
	for _, pool := range nodePool {
		get(pool).nodes++
	}
	for _, p := range pods {
		pool, ok := nodePool[p.node]
		if !ok {
			continue
		}
		if p.workload {
			get(pool).workload++
		} else {
			get(pool).excluded++
		}
	}

	var out []emit.Finding
	for _, np := range pools {
		t := tally[np.Name]
		if t == nil || t.nodes == 0 || t.workload > 0 {
			continue
		}
		out = append(out, nodePoolFinding(np, *t))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func nodePoolFinding(np cloud.NodePoolConfig, t poolTally) emit.Finding {
	as := np.Autoscaling
	totals := as.TotalMinNodeCount > 0 || as.TotalMaxNodeCount > 0
	floor, floorName := as.MinNodeCount, "min_node_count"
	if totals {
		floor, floorName = as.TotalMinNodeCount, "total_min_node_count"
	}

	nodes := plural(t.nodes, "node")
	var reason, remedy string
	switch {
	case !as.Enabled:
		reason = reasonIdleNotAutoscaled
		remedy = "autoscaling is off, so nothing will shrink it: resize it to 0, enable autoscaling with a 0 minimum, or delete it"
	case floor > 0:
		reason = reasonIdleMinNodeCount
		remedy = fmt.Sprintf("autoscaling is on but %s=%d keeps nodes up: lower it to 0 so the autoscaler can remove them", floorName, floor)
	default:
		reason = reasonIdleAwaitingScaleDown
		remedy = "autoscaling allows 0 nodes, so the autoscaler should remove them; if it persists, `lookout stab scaledown` shows what blocks it"
		if as.Autoprovisioned {
			remedy = "the pool is auto-provisioned and is deleted once empty; if it persists, `lookout stab scaledown` shows what blocks it"
		}
	}
	what := fmt.Sprintf("%d %s", t.nodes, nodes)
	if np.MachineType != "" {
		what += " (" + np.MachineType + ")"
	}

	f := emit.Finding{
		Kind:         kindIdleNodePool,
		Severity:     emit.SeverityInfo,
		KindOfObject: "NodePool",
		Name:         np.Name,
		Reason:       reason,
		Message:      fmt.Sprintf("node pool runs %s and no workload pod is scheduled on them (DaemonSet and static pods aside) — billed while idle; %s", what, remedy),
		Details: []emit.Field{
			{Key: "node_count", Value: strconv.Itoa(t.nodes)},
			{Key: "excluded_pods", Value: strconv.Itoa(t.excluded)},
		},
	}
	if np.MachineType != "" {
		f.Details = append(f.Details, emit.Field{Key: "machine_type", Value: np.MachineType})
	}
	if !as.Enabled {
		f.Details = append(f.Details, emit.Field{Key: "autoscaling", Value: "disabled"})
		return f
	}
	f.Details = append(f.Details, emit.Field{Key: "autoscaling", Value: "enabled"})
	if totals {
		f.Details = append(f.Details,
			emit.Field{Key: "total_min_node_count", Value: strconv.FormatInt(as.TotalMinNodeCount, 10)},
			emit.Field{Key: "total_max_node_count", Value: strconv.FormatInt(as.TotalMaxNodeCount, 10)})
	} else {
		f.Details = append(f.Details,
			emit.Field{Key: "min_node_count", Value: strconv.FormatInt(as.MinNodeCount, 10)},
			emit.Field{Key: "max_node_count", Value: strconv.FormatInt(as.MaxNodeCount, 10)})
	}
	if as.Autoprovisioned {
		f.Details = append(f.Details, emit.Field{Key: "autoprovisioned", Value: "true"})
	}
	return f
}

func plural(n int, word string) string {
	if n == 1 {
		return word
	}
	return word + "s"
}
