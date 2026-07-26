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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// DrainCommand builds `lookout stab drain` (§5 tool matrix row):
// everything that will block — or be destroyed by — a node drain,
// answered BEFORE anyone runs kubectl drain. A PDB with
// disruptionsAllowed=0 IS a drain blocker (the eviction API refuses
// and the drain hangs); bare pods and emptyDir data are the things a
// drain silently destroys; a single-replica workload's only pod is an
// outage in waiting.
//
// The command is node-scoped: pods are always examined across all
// namespaces (a drain does not respect namespace boundaries), and the
// common -A flag is repurposed to mean "all nodes".
func DrainCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "stab drain",
		MCPName: "k8s_drain_blockers",
		Summary: "Before draining a node, list everything that will block the drain (PDBs at disruptionsAllowed=0) or be destroyed by it (bare pods, emptyDir data, single-replica workloads); --node details one node, -A means all nodes here (pods are always examined across all namespaces); scanned counts pods examined after the standard-drain skips (mirror/DaemonSet/completed pods).",
		Flags: []emit.FlagSpec{
			{Name: "node", Type: emit.FlagString, Default: "",
				Help: "analyze one node in detail: every blocker on it becomes its own finding. Exactly one of --node or -A (all-nodes summary) is required."},
		},
		Output: []checks.OutputField{
			{Name: "node", Doc: "the node the blocker sits on (stamped on every --node-mode finding)"},
			{Name: "pods", Doc: "pods on the node covered by the gridlocked PDB"},
			{Name: "pod_names", Doc: "names of the covered pods, capped at 8 with a +N more tail"},
			{Name: "disruptions_allowed", Doc: "PDB status.disruptionsAllowed (always 0 in a gridlock finding)"},
			{Name: "current_healthy", Doc: "PDB status.currentHealthy"},
			{Name: "desired_healthy", Doc: "PDB status.desiredHealthy"},
			{Name: "volumes", Doc: "emptyDir volume names on the pod; memory-backed ones marked (medium=Memory)"},
			{Name: "workload", Doc: "the single-replica controller as <Kind>/<namespace>/<name>"},
			{Name: "replicas", Doc: "the controller's spec.replicas (always 1 in a singleton finding)"},
			{Name: "blockers", Doc: "total drain blockers on the node (also a --node-mode summary note)"},
			{Name: "pdb_gridlock", Doc: "gridlocked-PDB blocker count on the node (-A per-node finding; zero counts omitted)"},
			{Name: "bare_pods", Doc: "bare-pod blocker count on the node (-A per-node finding; zero counts omitted)"},
			{Name: "local_storage", Doc: "emptyDir blocker count on the node (-A per-node finding; zero counts omitted)"},
			{Name: "singletons", Doc: "single-replica blocker count on the node (-A per-node finding; zero counts omitted)"},
			{Name: "drainable", Doc: "summary note (--node mode): yes when the node has no blockers, else no"},
			{Name: "nodes", Doc: "summary note (-A mode): nodes examined"},
			{Name: "blocked", Doc: "summary note (-A mode): nodes with at least one blocker"},
		},
		Examples: []string{
			"lookout stab drain --node=gke-prod-pool-a-x1z2",
			"lookout stab drain -A",
			"lookout stab drain --node=gke-prod-pool-a-x1z2 --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runDrain(ctx, deps, inv)
		},
	}
}

func runDrain(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	node := inv.Flags.String("node")
	// Drain is node-scoped: the namespace/workload scoping flags have
	// no meaning here and silently accepting them would lie about the
	// blast radius (pods are examined across ALL namespaces).
	if inv.Scope.Namespace != "" {
		return 0, emit.UsageErrorf("--namespace does not apply to stab drain (a drain evicts across all namespaces); use --node=<name> or -A")
	}
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("--workload does not apply to stab drain (a drain is node-scoped); use --node=<name> or -A")
	}
	if (node == "") == !inv.Scope.AllNamespaces {
		return 0, emit.UsageErrorf("exactly one of --node=<name> (one node in detail) or -A (all-nodes summary) is required")
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	ix, err := listDrainIndex(ctx, client)
	if err != nil {
		return 0, err
	}
	if node != "" {
		if !ix.nodes[node] {
			return 0, fmt.Errorf("node %q not found (%d nodes in the cluster)", node, len(ix.nodes))
		}
		blockers := ix.nodeBlockers(node)
		sortFindings(blockers.findings)
		for _, f := range blockers.findings {
			if err := inv.Out.Emit(f); err != nil {
				return 0, err
			}
		}
		drainable := "yes"
		if blockers.total() > 0 {
			drainable = "no"
		}
		if err := inv.Out.Note("drainable", drainable); err != nil {
			return 0, err
		}
		if err := inv.Out.Note("blockers", itoa(blockers.total())); err != nil {
			return 0, err
		}
		return len(ix.podsByNode[node]), nil
	}

	// -A: one summary finding per blocked node; blocker-free nodes
	// emit nothing (zero nominal state).
	names := make([]string, 0, len(ix.nodes))
	for n := range ix.nodes {
		names = append(names, n)
	}
	sort.Strings(names)
	scanned, blocked := 0, 0
	var findings []emit.Finding
	for _, n := range names {
		scanned += len(ix.podsByNode[n])
		b := ix.nodeBlockers(n)
		if b.total() == 0 {
			continue
		}
		blocked++
		findings = append(findings, b.nodeFinding(n))
	}
	sortFindings(findings)
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	if err := inv.Out.Note("nodes", itoa(len(names))); err != nil {
		return 0, err
	}
	if err := inv.Out.Note("blocked", itoa(blocked)); err != nil {
		return 0, err
	}
	return scanned, nil
}

// drainIndex holds the listed objects a drain analysis needs.
type drainIndex struct {
	nodes      map[string]bool
	podsByNode map[string][]*corev1.Pod // examined pods only, name-sorted
	pdbs       []*policyv1.PodDisruptionBudget
	replicaSet map[string]*appsv1.ReplicaSet  // ns/name
	deployment map[string]*appsv1.Deployment  // ns/name
	statefulSt map[string]*appsv1.StatefulSet // ns/name
}

// listDrainIndex lists nodes, pods, PDBs, and the singleton-check
// controller kinds. Pods are listed across all namespaces and
// filtered client-side by spec.nodeName; the standard-drain skips
// (mirror pods, DaemonSet pods — drains pass --ignore-daemonsets —
// and Succeeded/Failed pods) are applied here, so scanned counts only
// pods a drain would actually try to evict.
func listDrainIndex(ctx context.Context, client kubernetes.Interface) (*drainIndex, error) {
	ix := &drainIndex{
		nodes:      map[string]bool{},
		podsByNode: map[string][]*corev1.Pod{},
		replicaSet: map[string]*appsv1.ReplicaSet{},
		deployment: map[string]*appsv1.Deployment{},
		statefulSt: map[string]*appsv1.StatefulSet{},
	}
	all := metav1.NamespaceAll
	steps := []func() error{
		func() error {
			return listPages("nodes", func(o metav1.ListOptions) ([]corev1.Node, string, error) {
				l, err := client.CoreV1().Nodes().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(n *corev1.Node) { ix.nodes[n.Name] = true })
		},
		func() error {
			return listPages("pods", func(o metav1.ListOptions) ([]corev1.Pod, string, error) {
				l, err := client.CoreV1().Pods(all).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(p *corev1.Pod) {
				if drainSkips(p) {
					return
				}
				ix.podsByNode[p.Spec.NodeName] = append(ix.podsByNode[p.Spec.NodeName], p)
			})
		},
		func() error {
			return listPages("poddisruptionbudgets", func(o metav1.ListOptions) ([]policyv1.PodDisruptionBudget, string, error) {
				l, err := client.PolicyV1().PodDisruptionBudgets(all).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(p *policyv1.PodDisruptionBudget) { ix.pdbs = append(ix.pdbs, p) })
		},
		func() error {
			return listPages("replicasets", func(o metav1.ListOptions) ([]appsv1.ReplicaSet, string, error) {
				l, err := client.AppsV1().ReplicaSets(all).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(r *appsv1.ReplicaSet) { ix.replicaSet[r.Namespace+"/"+r.Name] = r })
		},
		func() error {
			return listPages("deployments", func(o metav1.ListOptions) ([]appsv1.Deployment, string, error) {
				l, err := client.AppsV1().Deployments(all).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(d *appsv1.Deployment) { ix.deployment[d.Namespace+"/"+d.Name] = d })
		},
		func() error {
			return listPages("statefulsets", func(o metav1.ListOptions) ([]appsv1.StatefulSet, string, error) {
				l, err := client.AppsV1().StatefulSets(all).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(s *appsv1.StatefulSet) { ix.statefulSt[s.Namespace+"/"+s.Name] = s })
		},
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return nil, err
		}
	}
	for _, pods := range ix.podsByNode {
		sort.Slice(pods, func(i, j int) bool {
			if pods[i].Namespace != pods[j].Namespace {
				return pods[i].Namespace < pods[j].Namespace
			}
			return pods[i].Name < pods[j].Name
		})
	}
	sort.Slice(ix.pdbs, func(i, j int) bool {
		if ix.pdbs[i].Namespace != ix.pdbs[j].Namespace {
			return ix.pdbs[i].Namespace < ix.pdbs[j].Namespace
		}
		return ix.pdbs[i].Name < ix.pdbs[j].Name
	})
	return ix, nil
}

// drainSkips reports whether a standard drain ignores this pod:
// unscheduled, already finished, a mirror/static pod (the kubelet
// owns it; eviction is a no-op), or DaemonSet-owned (drains pass
// --ignore-daemonsets).
func drainSkips(p *corev1.Pod) bool {
	if p.Spec.NodeName == "" {
		return true
	}
	if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
		return true
	}
	if _, mirror := p.Annotations[corev1.MirrorPodAnnotationKey]; mirror {
		return true
	}
	for _, o := range p.OwnerReferences {
		if o.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

// blockerSet is one node's drain analysis: the individual findings
// (--node mode payload) plus per-class counts (-A rollup).
type blockerSet struct {
	findings                               []emit.Finding
	pdbGridlock, bare, localStorage, singl int
}

func (b blockerSet) total() int {
	return b.pdbGridlock + b.bare + b.localStorage + b.singl
}

// nodeBlockers classifies every examined pod on node against the four
// blocker classes. One finding per gridlocked PDB (not per covered
// pod); pod-level classes are one finding per pod.
func (ix *drainIndex) nodeBlockers(node string) blockerSet {
	var b blockerSet
	pods := ix.podsByNode[node]

	// drain.pdb_gridlock: disruptionsAllowed=0 + ≥1 covered pod on
	// this node means the eviction API refuses and the drain hangs.
	for _, pdb := range ix.pdbs {
		if pdb.Status.DisruptionsAllowed != 0 || pdb.Spec.Selector == nil {
			continue
		}
		sel, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			continue
		}
		var covered []string
		for _, p := range pods {
			if p.Namespace == pdb.Namespace && sel.Matches(labels.Set(p.Labels)) {
				covered = append(covered, p.Name)
			}
		}
		if len(covered) == 0 {
			continue
		}
		b.pdbGridlock++
		b.findings = append(b.findings, emit.Finding{
			Kind:         "drain.pdb_gridlock",
			Severity:     emit.SeverityCritical,
			Namespace:    pdb.Namespace,
			KindOfObject: "PodDisruptionBudget",
			Name:         pdb.Name,
			Reason:       "PDBGridlock",
			Message: fmt.Sprintf("PDB allows 0 disruptions and covers %d %s on %s: the eviction API will refuse and the drain hangs",
				len(covered), plural(len(covered), "pod"), node),
			Details: []emit.Field{
				{Key: "node", Value: node},
				{Key: "pods", Value: itoa(len(covered))},
				{Key: "pod_names", Value: cappedList(covered)},
				{Key: "disruptions_allowed", Value: "0"},
				{Key: "current_healthy", Value: itoa32(pdb.Status.CurrentHealthy)},
				{Key: "desired_healthy", Value: itoa32(pdb.Status.DesiredHealthy)},
			},
		})
	}

	for _, p := range pods {
		// drain.bare_pod: nothing recreates an evicted bare pod.
		if len(p.OwnerReferences) == 0 {
			b.bare++
			b.findings = append(b.findings, emit.Finding{
				Kind:         "drain.bare_pod",
				Severity:     emit.SeverityWarning,
				Namespace:    p.Namespace,
				KindOfObject: "Pod",
				Name:         p.Name,
				Reason:       "NoController",
				Message:      "pod has no owner: eviction deletes it permanently and nothing recreates it",
				Details:      []emit.Field{{Key: "node", Value: node}},
			})
		}
		// drain.local_storage: eviction is destructive for emptyDir
		// data (kubectl drain demands --delete-emptydir-data).
		if vols := emptyDirVolumes(p); len(vols) > 0 {
			b.localStorage++
			b.findings = append(b.findings, emit.Finding{
				Kind:         "drain.local_storage",
				Severity:     emit.SeverityWarning,
				Namespace:    p.Namespace,
				KindOfObject: "Pod",
				Name:         p.Name,
				Reason:       "EmptyDirData",
				Message: fmt.Sprintf("pod has %d emptyDir %s: the drain needs --delete-emptydir-data and the data is lost",
					len(vols), plural(len(vols), "volume")),
				Details: []emit.Field{
					{Key: "node", Value: node},
					{Key: "volumes", Value: cappedList(vols)},
				},
			})
		}
		// drain.singleton: evicting the only replica is an outage.
		if ref, ok := ix.singletonController(p); ok {
			b.singl++
			b.findings = append(b.findings, emit.Finding{
				Kind:         "drain.singleton",
				Severity:     emit.SeverityWarning,
				Namespace:    p.Namespace,
				KindOfObject: "Pod",
				Name:         p.Name,
				Reason:       "SingleReplica",
				Message:      fmt.Sprintf("evicting this pod takes down the only replica of %s", ref),
				Details: []emit.Field{
					{Key: "node", Value: node},
					{Key: "workload", Value: ref},
					{Key: "replicas", Value: "1"},
				},
			})
		}
	}
	return b
}

// nodeFinding rolls one blocked node up into the -A summary finding:
// severity is the worst blocker class present, per-class counts
// appear only when non-zero (zero nominal state).
func (b blockerSet) nodeFinding(node string) emit.Finding {
	severity := emit.SeverityWarning
	if b.pdbGridlock > 0 {
		severity = emit.SeverityCritical
	}
	details := []emit.Field{{Key: "blockers", Value: itoa(b.total())}}
	var parts []string
	class := func(key string, n int, phrase string) {
		if n == 0 {
			return
		}
		details = append(details, emit.Field{Key: key, Value: itoa(n)})
		parts = append(parts, fmt.Sprintf("%d %s", n, phrase))
	}
	class("pdb_gridlock", b.pdbGridlock, "gridlocked PDB(s)")
	class("bare_pods", b.bare, "bare pod(s)")
	class("local_storage", b.localStorage, "pod(s) with emptyDir data")
	class("singletons", b.singl, "single-replica workload pod(s)")
	return emit.Finding{
		Kind:         "drain.node",
		Severity:     severity,
		KindOfObject: "Node",
		Name:         node,
		Reason:       "DrainBlocked",
		Message:      "not drainable: " + strings.Join(parts, ", "),
		Details:      details,
	}
}

// emptyDirVolumes returns the pod's emptyDir volume names, sorted,
// memory-backed ones marked — those are lost even faster, but both
// kinds die with the eviction.
func emptyDirVolumes(p *corev1.Pod) []string {
	var out []string
	for _, v := range p.Spec.Volumes {
		if v.EmptyDir == nil {
			continue
		}
		name := v.Name
		if v.EmptyDir.Medium == corev1.StorageMediumMemory {
			name += "(medium=Memory)"
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// singletonController resolves a pod's controller chain and reports
// the workload when it runs exactly one replica: StatefulSet
// directly, Deployment via its ReplicaSet, or a bare ReplicaSet. A
// nil spec.replicas defaults to 1, matching the API server.
func (ix *drainIndex) singletonController(p *corev1.Pod) (string, bool) {
	ctrl := metav1.GetControllerOf(p)
	if ctrl == nil {
		return "", false
	}
	switch ctrl.Kind {
	case "StatefulSet":
		if sts, ok := ix.statefulSt[p.Namespace+"/"+ctrl.Name]; ok && replicasOf(sts.Spec.Replicas) == 1 {
			return "StatefulSet/" + p.Namespace + "/" + sts.Name, true
		}
	case "ReplicaSet":
		rs, ok := ix.replicaSet[p.Namespace+"/"+ctrl.Name]
		if !ok {
			return "", false
		}
		if owner := metav1.GetControllerOf(rs); owner != nil && owner.Kind == "Deployment" {
			if d, ok := ix.deployment[p.Namespace+"/"+owner.Name]; ok {
				if replicasOf(d.Spec.Replicas) == 1 {
					return "Deployment/" + p.Namespace + "/" + d.Name, true
				}
				return "", false
			}
		}
		if replicasOf(rs.Spec.Replicas) == 1 {
			return "ReplicaSet/" + p.Namespace + "/" + rs.Name, true
		}
	}
	return "", false
}

func replicasOf(r *int32) int32 {
	if r == nil {
		return 1
	}
	return *r
}
