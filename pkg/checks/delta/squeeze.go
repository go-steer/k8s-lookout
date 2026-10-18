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

// Why unschedulable pods fit on no node (#748).
//
// The scheduler's own message counts nodes by cause ("2 Insufficient
// cpu, 4 node(s) had untolerated taint(s)") and stops there. A live
// case showed what that leaves out: one of a fixed pool's three nodes
// carried a taint added by hand nine days earlier, which the rest of its
// pool did not, and a DaemonSet created two days earlier requested 130m
// on every node, filling the other two. Both were in objects the pass
// had already read; neither reached the answer, and a model reasoning
// from the scheduler's counts built the wrong story.
//
// This accounts for each Ready, schedulable node against the pending
// pods, from the Nodes, Pods and DaemonSets the pass listed: the taints
// they do not tolerate (and, for a taint some nodes of a pool carry and
// the rest do not, who set it and when, from managedFields), the room
// left on the nodes they could use against what they request, the
// largest requests there, and DaemonSets created recently. No API call
// is added.

import (
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

const (
	// squeezeLookback is how recent a DaemonSet must be to be called
	// out as new: a week covers the "what changed?" a capacity squeeze
	// usually turns on, past `triage changes`' default window.
	squeezeLookback = 7 * 24 * time.Hour
	// computeClassTaint is the taint GKE puts on the nodes of a pool a
	// compute class created; only pods that select the class tolerate it.
	computeClassTaint = "cloud.google.com/compute-class"
	// squeezeTop caps the largest requests named.
	squeezeTop = 3
)

// clusterView is what the pass listed that explaining unschedulable
// pods draws on. nodesRead says the Node list ran and was not refused;
// nil pods or daemonSets mean that list was not issued or was refused.
type clusterView struct {
	nodes      []corev1.Node
	nodesRead  bool
	pods       []corev1.Pod
	daemonSets []appsv1.DaemonSet
}

// squeeze is the account of why the unschedulable pods fit nowhere.
type squeeze struct {
	ready     int      // Ready, schedulable nodes accounted
	tainted   int      // of those, nodes with a taint no pending pod tolerates
	selector  int      // nodes the pods' nodeSelector excludes
	full      int      // nodes the pods could use, without room
	roomy     int      // nodes the pods could use, with room (the scheduler saw more)
	cordoned  int      // nodes marked unschedulable
	nodeLevel []string // node-level taints, rendered
	taints    string   // the taints detail
	fullNodes string   // the full_nodes detail
	largest   string   // the largest_requests detail
	newDS     []newDaemonSet
	// usablePools are the pools with a node the pods could use (no
	// untolerated taint, nodeSelector matches); poolsSeen, the pools
	// with any node. Together they say whether a pool that can grow
	// would help: one whose every node is closed to the pods would not.
	usablePools map[string]bool
	poolsSeen   map[string]bool
}

// poolHelps reports whether growing the pool could place the pods: it
// has a node they could use, or no node to judge by.
func (sq squeeze) poolHelps(pool string) bool {
	return sq.usablePools[pool] || !sq.poolsSeen[pool]
}

type newDaemonSet struct {
	name    string
	cpu     int64 // millicores requested per pod
	mem     int64 // bytes requested per pod
	nodes   int32
	created time.Time
}

// explainSqueeze builds the account, or ok=false when the pass did not
// read what it needs (Nodes and Pods, cluster-wide) or no pending pod is
// found among the listed Pods.
func explainSqueeze(findings []emit.Finding, v clusterView, now time.Time) (squeeze, bool) {
	if !v.nodesRead || v.pods == nil {
		return squeeze{}, false
	}
	nodes := v.nodes
	pending := pendingPods(findings, v.pods)
	if len(pending) == 0 {
		return squeeze{}, false
	}

	sq := squeeze{usablePools: map[string]bool{}, poolsSeen: map[string]bool{}}
	needCPU, needMem := smallestRequest(pending)
	onNode := map[string][]corev1.Pod{}
	for _, p := range v.pods {
		if p.Spec.NodeName != "" && p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed {
			onNode[p.Spec.NodeName] = append(onNode[p.Spec.NodeName], p)
		}
	}
	dsByUID := map[types.UID]appsv1.DaemonSet{}
	for _, ds := range v.daemonSets {
		dsByUID[ds.UID] = ds
	}

	type taintAt struct{ nodes []string }
	taintNodes := map[string]*taintAt{}
	var taintOrder []string
	var fullParts []string
	fullNames := map[string]bool{}
	for i := range nodes {
		n := &nodes[i]
		pool := n.Labels[gkeNodePoolLabel]
		sq.poolsSeen[pool] = true
		if !nodeReady(*n) {
			continue
		}
		if n.Spec.Unschedulable {
			sq.cordoned++
			continue
		}
		sq.ready++
		untol := untoleratedTaints(n.Spec.Taints, pending)
		if len(untol) > 0 {
			sq.tainted++
			for _, t := range untol {
				k := taintString(t)
				if taintNodes[k] == nil {
					taintNodes[k] = &taintAt{}
					taintOrder = append(taintOrder, k)
				}
				taintNodes[k].nodes = append(taintNodes[k].nodes, n.Name)
				if t.Key != computeClassTaint {
					if why := nodeLevelTaint(*n, t, nodes); why != "" {
						sq.nodeLevel = append(sq.nodeLevel, fmt.Sprintf("%s on %s (%s)", k, n.Name, why))
					}
				}
			}
			continue
		}
		if !selectorAdmits(n.Labels, pending) {
			sq.selector++
			continue
		}
		sq.usablePools[pool] = true
		reqCPU, reqMem := requested(onNode[n.Name])
		freeCPU := n.Status.Allocatable.Cpu().MilliValue() - reqCPU
		freeMem := n.Status.Allocatable.Memory().Value() - reqMem
		if freeCPU >= needCPU && freeMem >= needMem {
			sq.roomy++
			continue
		}
		sq.full++
		fullNames[n.Name] = true
		fullParts = append(fullParts, fmt.Sprintf("%s %s CPU, %s memory free of %s, %s",
			n.Name, milli(freeCPU), bytesText(freeMem), milli(n.Status.Allocatable.Cpu().MilliValue()), bytesText(n.Status.Allocatable.Memory().Value())))
	}

	// Taints, grouped, with what each means: the ones an operator set
	// first, then the compute classes', which are by design.
	sort.SliceStable(taintOrder, func(i, j int) bool {
		ci := strings.HasPrefix(taintOrder[i], computeClassTaint+"=")
		cj := strings.HasPrefix(taintOrder[j], computeClassTaint+"=")
		if ci != cj {
			return !ci
		}
		return taintOrder[i] < taintOrder[j]
	})
	parts := make([]string, 0, len(taintOrder))
	for _, k := range taintOrder {
		at := taintNodes[k]
		s := k + " on " + nodesText(at.nodes)
		if key, val, _ := strings.Cut(strings.SplitN(k, ":", 2)[0], "="); key == computeClassTaint {
			s += fmt.Sprintf(" (compute class %s: only pods that select it)", val)
		}
		parts = append(parts, s)
	}
	sq.taints = strings.Join(parts, "; ")
	if len(fullParts) > 0 {
		sq.fullNodes = strings.Join(fullParts, "; ") + fmt.Sprintf(" (smallest pending request: %s CPU, %s memory)", milli(needCPU), bytesText(needMem))
	}

	sq.largest = largestRequests(onNode, fullNames, dsByUID, now)
	for _, ds := range v.daemonSets {
		if now.Sub(ds.CreationTimestamp.Time) > squeezeLookback || ds.Status.DesiredNumberScheduled == 0 {
			continue
		}
		cpu, mem := podRequests(ds.Spec.Template.Spec)
		if cpu == 0 && mem == 0 {
			continue
		}
		sq.newDS = append(sq.newDS, newDaemonSet{name: ds.Namespace + "/" + ds.Name, cpu: cpu, mem: mem, nodes: ds.Status.DesiredNumberScheduled, created: ds.CreationTimestamp.Time})
	}
	sort.Slice(sq.newDS, func(i, j int) bool { return sq.newDS[i].created.After(sq.newDS[j].created) })
	return sq, true
}

// newDaemonSetsText renders the new DaemonSets, newest first.
func (sq squeeze) newDaemonSetsText(now time.Time) string {
	parts := make([]string, 0, len(sq.newDS))
	for _, d := range sq.newDS {
		parts = append(parts, fmt.Sprintf("%s %s CPU, %s memory on each of %d node(s), created %s ago", d.name, milli(d.cpu), bytesText(d.mem), d.nodes, ageText(now.Sub(d.created))))
	}
	return strings.Join(parts, "; ")
}

// summary is the sentence the message gains.
func (sq squeeze) summary(now time.Time) string {
	var b strings.Builder
	fmt.Fprintf(&b, "of %d Ready node(s), ", sq.ready)
	var clauses []string
	if sq.tainted > 0 {
		c := fmt.Sprintf("%d carry a taint the pods do not tolerate", sq.tainted)
		if len(sq.nodeLevel) > 0 {
			c += fmt.Sprintf(", %d of them set on the node alone rather than by its pool (%s)", len(sq.nodeLevel), strings.Join(sq.nodeLevel, "; "))
		}
		clauses = append(clauses, c)
	}
	if sq.selector > 0 {
		clauses = append(clauses, fmt.Sprintf("%d are excluded by the pods' nodeSelector", sq.selector))
	}
	if sq.full > 0 {
		clauses = append(clauses, fmt.Sprintf("%d the pods could use lack room for even the smallest of them", sq.full))
	}
	if sq.roomy > 0 {
		clauses = append(clauses, fmt.Sprintf("%d have room by requests, so affinity, ports or volumes are excluding them", sq.roomy))
	}
	if len(clauses) == 0 {
		clauses = append(clauses, "none is excluded by taints, nodeSelector or requests")
	}
	b.WriteString(strings.Join(clauses, ", "))
	if sq.cordoned > 0 {
		fmt.Fprintf(&b, " (%d more cordoned)", sq.cordoned)
	}
	if len(sq.newDS) > 0 && sq.full > 0 {
		d := sq.newDS[0]
		fmt.Fprintf(&b, "; DaemonSet %s, created %s ago, requests %s CPU on each of %d node(s)", d.name, ageText(now.Sub(d.created)), milli(d.cpu), d.nodes)
	}
	return b.String()
}

// pendingPods are the listed Pods behind the critical pod.pending
// findings: the ones the scheduler declared Unschedulable.
func pendingPods(findings []emit.Finding, pods []corev1.Pod) []corev1.Pod {
	want := map[string]bool{}
	for _, f := range findings {
		if f.Kind == "pod.pending" && f.Severity == emit.SeverityCritical {
			want[f.Namespace+"/"+f.Name] = true
		}
	}
	var out []corev1.Pod
	for _, p := range pods {
		if want[p.Namespace+"/"+p.Name] {
			out = append(out, p)
		}
	}
	return out
}

// untoleratedTaints are the node's scheduling taints that no pending
// pod tolerates. A taint one of them tolerates leaves the node open to
// that pod, so it is not the reason for it.
func untoleratedTaints(taints []corev1.Taint, pending []corev1.Pod) []corev1.Taint {
	var out []corev1.Taint
	for _, t := range taints {
		if t.Effect != corev1.TaintEffectNoSchedule && t.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		tolerated := false
		for _, p := range pending {
			if toleratesTaint(p.Spec.Tolerations, t) {
				tolerated = true
				break
			}
		}
		if !tolerated {
			out = append(out, t)
		}
	}
	return out
}

// toleratesTaint is the scheduler's toleration match: the effect
// matches or is empty; the key matches, or is empty with Exists (which
// tolerates everything); Exists ignores the value, Equal (the default)
// compares it.
func toleratesTaint(tols []corev1.Toleration, t corev1.Taint) bool {
	for _, tol := range tols {
		if tol.Effect != "" && tol.Effect != t.Effect {
			continue
		}
		if tol.Key == "" {
			if tol.Operator == corev1.TolerationOpExists {
				return true
			}
			continue
		}
		if tol.Key != t.Key {
			continue
		}
		if tol.Operator == corev1.TolerationOpExists || tol.Value == t.Value {
			return true
		}
	}
	return false
}

// selectorAdmits reports whether some pending pod's nodeSelector matches
// the node's labels (affinity is left to the scheduler's message).
func selectorAdmits(labels map[string]string, pending []corev1.Pod) bool {
	for _, p := range pending {
		ok := true
		for k, v := range p.Spec.NodeSelector {
			if labels[k] != v {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// nodeLevelTaint says why a taint looks set on the node rather than by
// its pool: some other nodes of the pool lack it. It names the
// managedFields manager that last wrote the node's taints, and when.
// "" when the whole pool carries it (or the node has no pool label).
func nodeLevelTaint(n corev1.Node, t corev1.Taint, nodes []corev1.Node) string {
	pool := n.Labels[gkeNodePoolLabel]
	if pool == "" {
		return ""
	}
	without := 0
	for _, o := range nodes {
		if o.Name == n.Name || o.Labels[gkeNodePoolLabel] != pool {
			continue
		}
		if !hasTaint(o.Spec.Taints, t) {
			without++
		}
	}
	if without == 0 {
		return ""
	}
	why := fmt.Sprintf("not on the other %d node(s) of pool %s", without, pool)
	if mgr, at := taintWriter(n); mgr != "" {
		why += fmt.Sprintf("; taints last set by %s", mgr)
		if !at.IsZero() {
			why += " at " + at.UTC().Format(time.RFC3339)
		}
	}
	return why
}

func hasTaint(taints []corev1.Taint, t corev1.Taint) bool {
	for _, o := range taints {
		if o.Key == t.Key && o.Value == t.Value && o.Effect == t.Effect {
			return true
		}
	}
	return false
}

// taintWriter is the most recent managedFields entry owning
// spec.taints: who set the node's taints last, and when.
func taintWriter(n corev1.Node) (string, time.Time) {
	var mgr string
	var at time.Time
	for _, m := range n.ManagedFields {
		if m.FieldsV1 == nil || !strings.Contains(m.FieldsV1.GetRawString(), `"f:taints"`) {
			continue
		}
		var t time.Time
		if m.Time != nil {
			t = m.Time.Time
		}
		if mgr == "" || t.After(at) {
			mgr, at = m.Manager, t
		}
	}
	return mgr, at
}

// requested sums the CPU (millicores) and memory (bytes) the pods on a
// node request, as the scheduler counts them.
func requested(pods []corev1.Pod) (cpu, mem int64) {
	for _, p := range pods {
		c, m := podRequests(p.Spec)
		cpu += c
		mem += m
	}
	return cpu, mem
}

// podRequests is a pod's effective request as the scheduler counts it:
// its containers plus its sidecars (init containers with restartPolicy
// Always, which keep running), or more if a regular init container,
// with the sidecars started before it, asks for more; plus overhead.
func podRequests(spec corev1.PodSpec) (cpu, mem int64) {
	var sideCPU, sideMem, initCPU, initMem int64
	for _, c := range spec.InitContainers {
		c0, m0 := c.Resources.Requests.Cpu().MilliValue(), c.Resources.Requests.Memory().Value()
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			sideCPU += c0
			sideMem += m0
			continue
		}
		initCPU = max(initCPU, sideCPU+c0)
		initMem = max(initMem, sideMem+m0)
	}
	cpu, mem = sideCPU, sideMem
	for _, c := range spec.Containers {
		cpu += c.Resources.Requests.Cpu().MilliValue()
		mem += c.Resources.Requests.Memory().Value()
	}
	cpu, mem = max(cpu, initCPU), max(mem, initMem)
	if q, ok := spec.Overhead[corev1.ResourceCPU]; ok {
		cpu += q.MilliValue()
	}
	if q, ok := spec.Overhead[corev1.ResourceMemory]; ok {
		mem += q.Value()
	}
	return cpu, mem
}

// smallestRequest is the least CPU and least memory any pending pod
// requests: a node without even that much room fits none of them.
func smallestRequest(pending []corev1.Pod) (cpu, mem int64) {
	for i, p := range pending {
		c, m := podRequests(p.Spec)
		if i == 0 || c < cpu {
			cpu = c
		}
		if i == 0 || m < mem {
			mem = m
		}
	}
	return cpu, mem
}

// largestRequests names the biggest CPU requesters on the full nodes,
// by owner: a DaemonSet once, with its per-pod request and age when it
// is new; anything else by its pod.
func largestRequests(onNode map[string][]corev1.Pod, full map[string]bool, dsByUID map[types.UID]appsv1.DaemonSet, now time.Time) string {
	type req struct {
		name string
		cpu  int64
		note string
	}
	byName := map[string]*req{}
	for node := range full {
		for _, p := range onNode[node] {
			cpu, _ := podRequests(p.Spec)
			if cpu == 0 {
				continue
			}
			name, note := p.Namespace+"/"+p.Name, ""
			for _, o := range p.OwnerReferences {
				if ds, ok := dsByUID[o.UID]; ok && o.Kind == "DaemonSet" {
					name, note = ds.Namespace+"/"+ds.Name, "DaemonSet"
					if age := now.Sub(ds.CreationTimestamp.Time); age <= squeezeLookback {
						note += ", created " + ageText(age) + " ago"
					}
				}
			}
			if r := byName[name]; r == nil || cpu > r.cpu {
				byName[name] = &req{name: name, cpu: cpu, note: note}
			}
		}
	}
	all := make([]*req, 0, len(byName))
	for _, r := range byName {
		all = append(all, r)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].cpu != all[j].cpu {
			return all[i].cpu > all[j].cpu
		}
		return all[i].name < all[j].name
	})
	parts := make([]string, 0, squeezeTop)
	for i := 0; i < len(all) && i < squeezeTop; i++ {
		s := all[i].name + " " + milli(all[i].cpu)
		if all[i].note != "" {
			s += " (" + all[i].note + ")"
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ", ")
}

func taintString(t corev1.Taint) string {
	s := t.Key
	if t.Value != "" {
		s += "=" + t.Value
	}
	return s + ":" + string(t.Effect)
}

// nodesText names up to two nodes, then counts.
func nodesText(names []string) string {
	if len(names) <= 2 {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%d nodes", len(names))
}

func milli(m int64) string { return resource.NewMilliQuantity(m, resource.DecimalSI).String() }

// bytesText renders a byte count for a reader: whole Ki/Mi/Gi when
// exact, else Mi or Gi to one decimal.
func bytesText(b int64) string {
	if b < 0 {
		return "-" + bytesText(-b)
	}
	const mi, gi = 1 << 20, 1 << 30
	switch {
	case b >= gi && b%gi != 0:
		return fmt.Sprintf("%.1fGi", float64(b)/gi)
	case b >= mi && b%mi != 0:
		return fmt.Sprintf("%.1fMi", float64(b)/mi)
	}
	return resource.NewQuantity(b, resource.BinarySI).String()
}

// ageText is a coarse age: days past two days, hours past two hours.
func ageText(d time.Duration) string {
	switch {
	case d >= 48*time.Hour:
		return fmt.Sprintf("%dd", int(d.Hours())/24)
	case d >= 2*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return d.Round(time.Minute).String()
	}
}
