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

// Why the cluster autoscaler did not add a node (#733).
//
// pod.pending says a pod is Unschedulable; the obvious next question is
// why the autoscaler did not add a node for it, and the cluster's own
// evidence often cannot say: a live test met NotTriggerScaleUp events
// whose reason was empty ("Pod didn't trigger scale-up: "), on a cluster
// at 0 nodes for eleven hours, and the agent could only guess. The
// provider holds both halves of the answer, and lookout already reads
// them: each node pool's autoscaling bounds (cloud.ClusterConfigAPI) and
// the autoscaler's own decisions with their reason (cloud.CapacityAPI,
// the GKE cluster-autoscaler visibility log). This joins them to the
// pending pods once per run, in one finding about the cluster.

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

const (
	kindScaleUpBlocked = "autoscaler.scaleup_blocked"
	// scaleUpWindow is how far back autoscaler decisions are read.
	scaleUpWindow = time.Hour
	// gkeNodePoolLabel names a node's pool on GKE.
	gkeNodePoolLabel = "cloud.google.com/gke-nodepool"
	// noteAutoscaler is the summary note saying why the explanation is
	// missing, when it is.
	noteAutoscaler = "autoscaler"
)

// unschedulable counts the pod.pending findings the scheduler declared
// Unschedulable (the critical ones): pods only a new node can help.
func unschedulable(fs []emit.Finding) int {
	n := 0
	for _, f := range fs {
		if f.Kind == "pod.pending" && f.Severity == emit.SeverityCritical {
			n++
		}
	}
	return n
}

// poolView is one node pool as the explanation sees it.
type poolView struct {
	name    string
	nodes   int    // from Node objects; -1 when nodes were not read
	blocked string // why it cannot grow, from its config; "" when it can
	bounds  string
}

// explainScaleUp builds the finding, or returns the note to record when
// it cannot be built (no provider, no permission). ok reports a finding.
func explainScaleUp(ctx context.Context, provider cloud.Provider, pending int, nodes []corev1.Node, nodesRead bool, now time.Time) (f emit.Finding, note string, ok bool) {
	cc, has := provider.ClusterConfig()
	if !has {
		return emit.Finding{}, fmt.Sprintf("unavailable: provider %q cannot read node pool configuration", provider.Name()), false
	}
	cfg, err := cc.Config(ctx)
	if err != nil {
		return emit.Finding{}, "unavailable: node pool configuration: " + err.Error(), false
	}

	byPool := map[string]int{}
	ready := 0
	for _, n := range nodes {
		byPool[n.Labels[gkeNodePoolLabel]]++
		if nodeReady(n) {
			ready++
		}
	}

	var pools []poolView
	allBlocked := len(cfg.NodePools) > 0 && !cfg.Autopilot
	for _, np := range cfg.NodePools {
		p := poolView{name: np.Name, nodes: -1, bounds: boundsText(np.Autoscaling)}
		if nodesRead {
			p.nodes = byPool[np.Name]
		}
		switch a := np.Autoscaling; {
		case cfg.Autopilot:
			// Autopilot manages capacity itself; the pools are its own.
		case !a.Enabled:
			p.blocked = "autoscaling off"
		case a.MaxNodeCount == 0 && a.TotalMaxNodeCount == 0:
			p.blocked = "autoscaling maximum is 0"
		}
		if p.blocked == "" {
			allBlocked = false
		}
		pools = append(pools, p)
	}

	// The autoscaler's own word, when the provider has it.
	reasons := map[string]int{}
	var decisionNote string
	if capAPI, has := provider.Capacity(); has {
		decisions, err := capAPI.ScaleDecisions(ctx, cloud.TimeWindow{Start: now.Add(-scaleUpWindow), End: now})
		if err != nil {
			decisionNote = "autoscaler decisions unavailable: " + err.Error()
		}
		for _, d := range decisions {
			if d.Decision != "noScaleUp" || d.Reason == "" {
				continue
			}
			key := d.Reason
			if pool := poolOf(d.NodeGroup, cfg.NodePools); pool != "" {
				key = pool + ": " + d.Reason
			}
			reasons[key]++
		}
	} else {
		decisionNote = "autoscaler decisions unavailable from this provider"
	}

	severity := emit.SeverityInfo
	switch {
	case nodesRead && ready == 0, allBlocked:
		severity = emit.SeverityCritical
	case len(reasons) > 0 || slices.ContainsFunc(pools, func(p poolView) bool { return p.blocked != "" }):
		severity = emit.SeverityWarning
	}

	details := []emit.Field{{Key: "pods", Value: strconv.Itoa(pending)}}
	if nodesRead {
		details = append(details, emit.Field{Key: "nodes", Value: fmt.Sprintf("%d/%d", ready, len(nodes))})
	}
	if cfg.Autopilot {
		details = append(details, emit.Field{Key: "autopilot", Value: "true"})
	}
	if len(pools) > 0 {
		details = append(details, emit.Field{Key: "pools", Value: poolsText(pools)})
	}
	if len(reasons) > 0 {
		details = append(details, emit.Field{Key: "no_scale_up", Value: reasonsText(reasons)})
	}
	details = append(details, emit.Field{Key: "window", Value: scaleUpWindow.String()})
	if decisionNote != "" {
		details = append(details, emit.Field{Key: "decisions", Value: decisionNote})
	}

	name := cfg.Name
	if name == "" {
		name = "cluster"
	}
	return emit.Finding{
		Kind:         kindScaleUpBlocked,
		Severity:     severity,
		KindOfObject: "Cluster",
		Name:         name,
		Reason:       scaleUpReason(severity, allBlocked, nodesRead && ready == 0),
		Message:      scaleUpMessage(pending, pools, reasons, cfg.Autopilot, nodesRead, ready),
		Details:      details,
	}, "", true
}

func scaleUpReason(severity string, allBlocked, noReadyNodes bool) string {
	switch {
	case allBlocked:
		return "NoPoolCanGrow"
	case noReadyNodes:
		return "NoReadyNodes"
	case severity == emit.SeverityWarning:
		return "ScaleUpRefused"
	}
	return "ScaleUpNotRefused"
}

func scaleUpMessage(pending int, pools []poolView, reasons map[string]int, autopilot, nodesRead bool, ready int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d pod(s) are unschedulable", pending)
	if nodesRead {
		fmt.Fprintf(&b, " with %d Ready node(s)", ready)
	}
	var blocked []string
	for _, p := range pools {
		if p.blocked != "" {
			blocked = append(blocked, p.name+" ("+p.blocked+")")
		}
	}
	switch {
	case autopilot:
		b.WriteString("; Autopilot provisions nodes itself")
	case len(blocked) == len(pools) && len(pools) > 0:
		fmt.Fprintf(&b, " and no node pool can grow: %s", strings.Join(blocked, ", "))
	case len(blocked) > 0:
		fmt.Fprintf(&b, "; pools that cannot grow: %s", strings.Join(blocked, ", "))
	}
	if len(reasons) > 0 {
		fmt.Fprintf(&b, "; the autoscaler refused to scale up in the last %s: %s", scaleUpWindow, reasonsText(reasons))
	} else if len(blocked) < len(pools) || autopilot {
		fmt.Fprintf(&b, "; no autoscaler refusal in the last %s, so the pods' own constraints (requests, selectors, taints, affinity) are the likelier cause", scaleUpWindow)
	}
	return b.String()
}

func boundsText(a cloud.NodePoolAutoscaling) string {
	switch {
	case !a.Enabled:
		return "fixed"
	case a.TotalMaxNodeCount > 0 || a.TotalMinNodeCount > 0:
		return fmt.Sprintf("%d-%d total", a.TotalMinNodeCount, a.TotalMaxNodeCount)
	default:
		return fmt.Sprintf("%d-%d per zone", a.MinNodeCount, a.MaxNodeCount)
	}
}

// poolsText renders every pool: name, node count, autoscaling bounds,
// and why it cannot grow when it cannot.
func poolsText(pools []poolView) string {
	parts := make([]string, 0, len(pools))
	for _, p := range pools {
		s := p.name + " (" + p.bounds
		if p.nodes >= 0 {
			s += fmt.Sprintf(", %d node(s)", p.nodes)
		}
		if p.blocked != "" {
			s += ", " + p.blocked
		}
		parts = append(parts, s+")")
	}
	return strings.Join(parts, "; ")
}

// reasonsText renders the refusals most frequent first.
func reasonsText(reasons map[string]int) string {
	keys := make([]string, 0, len(reasons))
	for k := range reasons {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if reasons[keys[i]] != reasons[keys[j]] {
			return reasons[keys[i]] > reasons[keys[j]]
		}
		return keys[i] < keys[j]
	})
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s ×%d", k, reasons[k]))
	}
	return strings.Join(parts, "; ")
}

// poolOf maps an autoscaler node group (GKE: a managed instance group,
// gke-<cluster>-<pool>-<hash>-grp) to its node pool, longest pool name
// first so "pool-a" does not claim "pool-a-spot"'s groups.
func poolOf(group string, pools []cloud.NodePoolConfig) string {
	names := make([]string, 0, len(pools))
	for _, p := range pools {
		names = append(names, p.Name)
	}
	sort.Slice(names, func(i, j int) bool { return len(names[i]) > len(names[j]) })
	for _, n := range names {
		if strings.Contains(group, "-"+n+"-") {
			return n
		}
	}
	return ""
}

func nodeReady(n corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
