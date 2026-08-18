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

package audit

import (
	"context"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// The posture kinds this command emits (issue #190). The other two,
// audit.hpa_cannot_scale and audit.rigid_scheduling, are declared
// beside their detectors in hpa.go and placement.go.
const (
	kindNoPDB       = "audit.no_pdb"
	kindSingle      = "audit.single_replica"
	kindNoReadiness = "audit.no_readiness_probe"
	kindNoLiveness  = "audit.no_liveness_probe"
	kindNoSpread    = "audit.no_spread"
)

// WorkloadsCommand builds `lookout audit workloads`: the workload
// half of the obtainability audit (#190), asking of a workload that
// is currently HEALTHY whether anything protects it from the next
// node drain, upgrade, or wedged process.
//
// # Why this is not `stab drain`
//
// `stab drain` answers the same underlying facts node-scoped and in
// the present tense: "if I drain gke-a right now, what breaks."
// drain.singleton fires only for a single-replica workload whose pod
// happens to sit on the node being drained; the same workload is
// invisible on every other node, and invisible entirely until someone
// runs the check with a node in hand. The posture question is the
// standing one — "which workloads in this cluster have no HA at all"
// — and it is answered from the workload spec, without reference to
// where the pods currently are. Same facts, different tense, so the
// kinds are different (a consumer that merged them would see a
// workload appear and disappear as nodes were drained).
//
// # One API pass, several claims
//
// Every claim here reads the workload spec against one of three sets
// listed alongside it — the PDBs, the HPAs, the nodes — so they share
// one pass rather than one command per slug. The kinds stay separate:
// a consumer suppressing probe noise must not thereby lose
// disruption-budget coverage.
//
// Two subjects, therefore. Most findings are about a workload; the
// audit.hpa_cannot_scale ones are about an autoscaler, and carry
// kind_of_object=HorizontalPodAutoscaler with the HPA's own name.
func WorkloadsCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:        "audit workloads",
		MCPName:     "k8s_audit_workloads",
		MCPProfiles: []string{"audit"},
		Summary:     "Workload reliability posture for workloads that are healthy right now: no PodDisruptionBudget, only one replica, no readiness/liveness probe, no spread across nodes, placement pinned to too few nodes, and autoscalers that structurally cannot scale. Answers \"what has no safety net\", as against `stab drain`, which answers \"what breaks if I drain THIS node now\". Scope with --namespace, -A, or --workload; scanned counts workloads examined.",
		Kinds: []checks.KindField{
			checks.Kind(kindNoPDB, "the workload has no PodDisruptionBudget: a drain can take every replica at once", emit.SeverityWarning),
			checks.Kind(kindSingle, "the workload runs a single replica, so any disruption is an outage", emit.SeverityWarning),
			checks.Kind(kindNoReadiness, "a container has no readiness probe, so traffic reaches it before it can serve", emit.SeverityWarning),
			checks.Kind(kindNoLiveness, "a container has no liveness probe, so a wedged process is never restarted", emit.SeverityInfo),
			checks.Kind(kindNoSpread, "the workload's replicas are not spread across nodes or zones", emit.SeverityInfo),
			checks.Kind(kindRigidScheduling, "placement constraints pin the workload to too few nodes to survive losing one", emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind(kindHPACannotScale, "the autoscaler structurally cannot scale: min equals max, the target is missing, or a container has no request for its utilization target to divide by", emit.SeverityWarning),
		},
		Output: []checks.OutputField{
			{Name: "replicas", Doc: "the workload's spec.replicas (nil defaults to 1, matching the API server); absent on DaemonSets, whose replica count is the node count"},
			{Name: "namespace_pdbs", Doc: "PodDisruptionBudgets in the workload's namespace — 0 says the namespace has no PDB culture at all, a non-zero value says this workload was missed"},
			{Name: "containers", Doc: "containers implicated by the finding: those missing the probe, or missing the request the autoscaler's utilization target divides by"},
			{Name: "container_names", Doc: "their names, capped at 8 with a +N more tail"},
			{Name: "total_containers", Doc: "containers in the pod template, so `containers` reads as a fraction"},
			{Name: "min_replicas", Doc: "the HPA's spec.minReplicas (nil defaults to 1, matching the API server)"},
			{Name: "max_replicas", Doc: "the HPA's spec.maxReplicas"},
			{Name: "metric", Doc: "the utilization metric the HPA cannot compute, comma-separated if more than one"},
			{Name: "scale_target", Doc: "the HPA's scaleTargetRef as Kind/name"},
			{Name: "eligible_nodes", Doc: "nodes satisfying the workload's REQUIRED placement constraint; an upper bound, since taints and cordons are not subtracted"},
			{Name: "cluster_nodes", Doc: "nodes in the cluster, so `eligible_nodes` reads as a fraction"},
			{Name: "constraint", Doc: "the label and field keys that narrow placement, sorted and capped at 8"},
			{Name: "pdbs", Doc: "summary note: PodDisruptionBudgets seen in scope"},
			{Name: "hpas", Doc: "summary note: HorizontalPodAutoscalers seen in scope"},
			{Name: "nodes", Doc: "summary note: nodes in the cluster — the denominator every placement claim is resolved against"},
			{Name: "workloads", Doc: "summary note: workloads examined, broken down as deployments/statefulsets/daemonsets"},
		},
		Examples: []string{
			"lookout audit workloads -A",
			"lookout audit workloads --namespace=prod",
			"lookout audit workloads --workload=Deployment/prod/checkout",
			"lookout audit workloads -A --exemptions=exemptions.yaml --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runWorkloads(ctx, deps, inv)
		},
	}
}

// canonicalWorkloadKinds maps a --workload kind, case-insensitively,
// onto the three kinds this command reasons about.
var canonicalWorkloadKinds = map[string]string{
	"deployment": "Deployment", "deployments": "Deployment", "deploy": "Deployment",
	"statefulset": "StatefulSet", "statefulsets": "StatefulSet", "sts": "StatefulSet",
	"daemonset": "DaemonSet", "daemonsets": "DaemonSet", "ds": "DaemonSet",
}

func runWorkloads(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	wl := inv.Scope.Workload
	switch {
	case wl.IsZero() && inv.Scope.Namespace == "" && !inv.Scope.AllNamespaces:
		return 0, emit.UsageErrorf("no scope: pass --namespace=<ns>, -A, or --workload=<Kind>/<ns>/<name>")
	case !wl.IsZero() && inv.Scope.AllNamespaces:
		return 0, emit.UsageErrorf("-A does not combine with --workload: a workload lives in one namespace (%s)", wl.Namespace)
	case !wl.IsZero() && inv.Scope.Namespace != "" && inv.Scope.Namespace != wl.Namespace:
		return 0, emit.UsageErrorf("--namespace=%s contradicts --workload namespace %s", inv.Scope.Namespace, wl.Namespace)
	}
	if !wl.IsZero() {
		canonical, ok := canonicalWorkloadKinds[strings.ToLower(wl.Kind)]
		if !ok {
			return 0, emit.UsageErrorf("unsupported workload kind %q (want Deployment|StatefulSet|DaemonSet — the kinds that own a pod template and a replica count)", wl.Kind)
		}
		wl.Kind = canonical
	}

	listNS := inv.Scope.Namespace
	if inv.Scope.AllNamespaces {
		listNS = metav1.NamespaceAll
	}
	if !wl.IsZero() {
		listNS = wl.Namespace
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	ix, err := listWorkloadIndex(ctx, client, listNS)
	if err != nil {
		return 0, err
	}

	var findings []emit.Finding
	scanned := 0
	var counts [3]int // Deployment, StatefulSet, DaemonSet
	for _, w := range ix.workloads {
		if !wl.IsZero() && (w.kind != wl.Kind || w.name != wl.Name) {
			continue
		}
		scanned++
		switch w.kind {
		case "Deployment":
			counts[0]++
		case "StatefulSet":
			counts[1]++
		default:
			counts[2]++
		}
		findings = append(findings, ix.judge(w)...)
	}
	if !wl.IsZero() && scanned == 0 {
		return 0, fmt.Errorf("workload %s not found", wl)
	}

	// The autoscaler findings are indexed by their own subject, so they
	// are gathered separately; under --workload scope the ones worth
	// showing are those attached to the workload asked about.
	for _, h := range ix.hpas {
		if !wl.IsZero() && !hpaTargets(h, wl.Kind, wl.Namespace, wl.Name) {
			continue
		}
		findings = append(findings, ix.hpaFindings(h)...)
	}

	sortFindings(findings)
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	notes := [][2]string{
		{"pdbs", itoa(len(ix.pdbs))},
		{"hpas", itoa(len(ix.hpas))},
		{"nodes", itoa(len(ix.nodes))},
		{"workloads", fmt.Sprintf("%d/%d/%d", counts[0], counts[1], counts[2])},
	}
	for _, n := range notes {
		if err := inv.Out.Note(n[0], n[1]); err != nil {
			return 0, err
		}
	}
	return scanned, nil
}

// workload is one pod-template owner, flattened so the judgments do
// not each have to switch on the concrete apps/v1 type.
type workload struct {
	kind      string
	namespace string
	name      string
	// replicas is nil for a DaemonSet, whose replica count is the
	// node count and is therefore not a spec property to judge.
	replicas *int32
	template corev1.PodTemplateSpec
}

// wantReplicas resolves spec.replicas the way the API server does: a
// nil value means 1.
func (w workload) wantReplicas() int32 {
	if w.replicas == nil {
		return 1
	}
	return *w.replicas
}

// workloadIndex holds the listed objects a posture pass needs: the
// pod-template owners, the PDBs that may or may not cover them, the
// HPAs that may or may not be able to scale them, and the nodes their
// placement constraints resolve against.
type workloadIndex struct {
	workloads []workload
	pdbs      []*policyv1.PodDisruptionBudget
	hpas      []*autoscalingv2.HorizontalPodAutoscaler
	// nodes are cluster-scoped, so this is the whole inventory even
	// under --namespace: a placement constraint does not stop at a
	// namespace boundary.
	nodes []*corev1.Node
	// pdbsByNS counts PDBs per namespace, so a no-PDB finding can say
	// whether the namespace uses PDBs at all.
	pdbsByNS map[string]int
}

func listWorkloadIndex(ctx context.Context, client kubernetes.Interface, ns string) (*workloadIndex, error) {
	ix := &workloadIndex{pdbsByNS: map[string]int{}}
	steps := []func() error{
		func() error {
			return listPages("deployments", func(o metav1.ListOptions) ([]appsv1.Deployment, string, error) {
				l, err := client.AppsV1().Deployments(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(d *appsv1.Deployment) {
				ix.workloads = append(ix.workloads, workload{
					kind: "Deployment", namespace: d.Namespace, name: d.Name,
					replicas: d.Spec.Replicas, template: d.Spec.Template,
				})
			})
		},
		func() error {
			return listPages("statefulsets", func(o metav1.ListOptions) ([]appsv1.StatefulSet, string, error) {
				l, err := client.AppsV1().StatefulSets(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(s *appsv1.StatefulSet) {
				ix.workloads = append(ix.workloads, workload{
					kind: "StatefulSet", namespace: s.Namespace, name: s.Name,
					replicas: s.Spec.Replicas, template: s.Spec.Template,
				})
			})
		},
		func() error {
			return listPages("daemonsets", func(o metav1.ListOptions) ([]appsv1.DaemonSet, string, error) {
				l, err := client.AppsV1().DaemonSets(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(d *appsv1.DaemonSet) {
				ix.workloads = append(ix.workloads, workload{
					kind: "DaemonSet", namespace: d.Namespace, name: d.Name,
					template: d.Spec.Template,
				})
			})
		},
		func() error {
			return listPages("poddisruptionbudgets", func(o metav1.ListOptions) ([]policyv1.PodDisruptionBudget, string, error) {
				l, err := client.PolicyV1().PodDisruptionBudgets(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(p *policyv1.PodDisruptionBudget) {
				ix.pdbs = append(ix.pdbs, p)
				ix.pdbsByNS[p.Namespace]++
			})
		},
		func() error {
			return listPages("horizontalpodautoscalers", func(o metav1.ListOptions) ([]autoscalingv2.HorizontalPodAutoscaler, string, error) {
				l, err := client.AutoscalingV2().HorizontalPodAutoscalers(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(h *autoscalingv2.HorizontalPodAutoscaler) {
				ix.hpas = append(ix.hpas, h)
			})
		},
		func() error {
			return listPages("nodes", func(o metav1.ListOptions) ([]corev1.Node, string, error) {
				l, err := client.CoreV1().Nodes().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(n *corev1.Node) {
				ix.nodes = append(ix.nodes, n)
			})
		},
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return nil, err
		}
	}
	sort.Slice(ix.workloads, func(i, j int) bool {
		a, b := ix.workloads[i], ix.workloads[j]
		if a.namespace != b.namespace {
			return a.namespace < b.namespace
		}
		if a.kind != b.kind {
			return a.kind < b.kind
		}
		return a.name < b.name
	})
	sort.Slice(ix.hpas, func(i, j int) bool {
		a, b := ix.hpas[i], ix.hpas[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	return ix, nil
}

// judge applies every workload-subject posture claim to one workload.
// The autoscaler claims are not here: their subject is the HPA.
func (ix *workloadIndex) judge(w workload) []emit.Finding {
	out := append(ix.availability(w), probeFindings(w)...)
	out = append(out, ix.placement(w)...)
	for i := range out {
		out[i].Namespace = w.namespace
		out[i].KindOfObject = w.kind
		out[i].Name = w.name
		out[i].Fingerprint = engine.PostureFingerprint(out[i].Kind, out[i].Reason, w.kind)
	}
	return out
}

// availability judges the disruption-survival claims: replica count,
// PDB coverage, and spread.
//
// A DaemonSet is exempt from all three by construction — its replica
// count is the node count, a drain skips its pods (kubectl drain
// passes --ignore-daemonsets), and one-pod-per-node IS the spread.
//
// A workload scaled to zero is exempt too: it has no availability to
// protect, and "add a PDB to something you deliberately turned off"
// is advice nobody should act on. Its probes are still judged — the
// template is what runs when it is scaled back up.
func (ix *workloadIndex) availability(w workload) []emit.Finding {
	if w.kind == "DaemonSet" {
		return nil
	}
	want := w.wantReplicas()
	if want == 0 {
		return nil
	}

	// One replica: the remedy is a second replica, not a budget for
	// disrupting the one. Emitting no_pdb here as well would double-
	// count the same outage and point at the wrong fix — a PDB over a
	// single replica is a drain gridlock, which is a DIFFERENT
	// finding (stab drain → drain.pdb_gridlock).
	if want == 1 {
		return []emit.Finding{{
			Kind:     kindSingle,
			Severity: emit.SeverityWarning,
			Reason:   "SingleReplica",
			Message:  "spec.replicas=1: a node drain, upgrade, or eviction takes the workload fully down, and no PodDisruptionBudget can prevent that",
			Details:  []emit.Field{{Key: "replicas", Value: itoa(int(want))}},
		}}
	}

	var out []emit.Finding
	if !ix.covered(w) {
		out = append(out, emit.Finding{
			Kind:     kindNoPDB,
			Severity: emit.SeverityWarning,
			Reason:   "NoPodDisruptionBudget",
			Message: fmt.Sprintf("%d replicas and no PodDisruptionBudget selecting them: the eviction API will let a drain or upgrade take all %d at once",
				want, want),
			Details: []emit.Field{
				{Key: "replicas", Value: itoa(int(want))},
				{Key: "namespace_pdbs", Value: itoa(ix.pdbsByNS[w.namespace])},
			},
		})
	}
	if !spreadConstrained(w.template.Spec) {
		out = append(out, emit.Finding{
			Kind:     kindNoSpread,
			Severity: emit.SeverityInfo,
			Reason:   "NoTopologySpread",
			Message: fmt.Sprintf("%d replicas with no topologySpreadConstraints and no pod anti-affinity: nothing in the spec stops the scheduler putting them all on one node, and the cluster-level default spread is best-effort (whenUnsatisfiable=ScheduleAnyway)",
				want),
			Details: []emit.Field{{Key: "replicas", Value: itoa(int(want))}},
		})
	}
	return out
}

// spreadConstrained reports whether the pod template asks for its
// replicas to be placed apart, by either mechanism. Anti-affinity is
// accepted in its preferred form as well as its required one: a
// preference is a weaker guarantee, but it is a deliberate statement
// about placement, which is what this check is looking for.
func spreadConstrained(spec corev1.PodSpec) bool {
	if len(spec.TopologySpreadConstraints) > 0 {
		return true
	}
	if spec.Affinity == nil || spec.Affinity.PodAntiAffinity == nil {
		return false
	}
	aa := spec.Affinity.PodAntiAffinity
	return len(aa.RequiredDuringSchedulingIgnoredDuringExecution) > 0 ||
		len(aa.PreferredDuringSchedulingIgnoredDuringExecution) > 0
}

// covered reports whether any PDB in the workload's namespace selects
// its pod template. Matching the TEMPLATE labels rather than live
// pods is deliberate: the claim is about the spec, so it holds while
// the workload is scaled down, mid-rollout, or momentarily podless,
// and it costs no pod List.
func (ix *workloadIndex) covered(w workload) bool {
	tpl := labels.Set(w.template.Labels)
	for _, pdb := range ix.pdbs {
		if pdb.Namespace != w.namespace {
			continue
		}
		// LabelSelectorAsSelector encodes the policy/v1 distinction
		// this check would otherwise get wrong: a nil selector matches
		// NOTHING, an empty one matches everything in the namespace.
		sel, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			continue
		}
		if sel.Matches(tpl) {
			return true
		}
	}
	return false
}

// probeFindings judges the two probes over the template's containers.
//
// Only spec.containers are judged. An init container runs to
// completion before the pod is ready, so neither probe applies to the
// classic form; the sidecar form (restartPolicy: Always) does support
// them, and is left for a later pass rather than guessed at here.
func probeFindings(w workload) []emit.Finding {
	cs := w.template.Spec.Containers
	if len(cs) == 0 {
		return nil
	}
	var noReadiness, noLiveness []string
	for _, c := range cs {
		if c.ReadinessProbe == nil {
			noReadiness = append(noReadiness, c.Name)
		}
		if c.LivenessProbe == nil {
			noLiveness = append(noLiveness, c.Name)
		}
	}
	var out []emit.Finding
	if len(noReadiness) > 0 {
		out = append(out, probeFinding(kindNoReadiness, emit.SeverityWarning, "NoReadinessProbe",
			"the kubelet marks the container ready as soon as it starts, so the Service sends traffic to a process that may not be listening yet and a rollout can report success into a black hole",
			noReadiness, len(cs)))
	}
	if len(noLiveness) > 0 {
		// Info, not warning: a missing liveness probe costs an
		// automatic restart of a wedged process, but a badly written
		// one causes restart storms. The absence is worth reporting
		// and is not, on its own, a defect.
		out = append(out, probeFinding(kindNoLiveness, emit.SeverityInfo, "NoLivenessProbe",
			"a process that wedges without exiting is never restarted; it stays in the Service until something else notices",
			noLiveness, len(cs)))
	}
	return out
}

func probeFinding(kind, severity, reason, consequence string, missing []string, total int) emit.Finding {
	return emit.Finding{
		Kind:     kind,
		Severity: severity,
		Reason:   reason,
		Message: fmt.Sprintf("%d of %d %s missing the probe: %s",
			len(missing), total, plural(total, "container"), consequence),
		Details: []emit.Field{
			{Key: "containers", Value: itoa(len(missing))},
			{Key: "container_names", Value: cappedList(missing)},
			{Key: "total_containers", Value: itoa(total)},
		},
	}
}

// severityRank orders findings critical-first, matching the other
// command groups.
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

// sortFindings orders by namespace/name first, then severity: a
// posture report is read workload by workload, unlike an incident
// report, where the worst thing in the cluster leads.
//
// The reason is the last key rather than an afterthought: one subject
// can carry several findings of the same kind at the same severity —
// an HPA that is both pinned and unmeasurable, a pod template in all
// three host namespaces — and without it those tie, leaving their
// order to the sort's internals and the goldens to luck.
func sortFindings(fs []emit.Finding) {
	key := func(f emit.Finding) string {
		var b strings.Builder
		fmt.Fprintf(&b, "%s\x00%s\x00%s\x00%d\x00%s\x00%s",
			f.Namespace, f.KindOfObject, f.Name, severityRank(f.Severity), f.Kind, f.Reason)
		return b.String()
	}
	sort.Slice(fs, func(i, j int) bool { return key(fs[i]) < key(fs[j]) })
}

// pageLimit is the paged-List page size (§6.3).
const pageLimit = 500

// listPages drives one paged List to exhaustion.
func listPages[T any](what string, list func(metav1.ListOptions) ([]T, string, error), each func(*T)) error {
	opts := metav1.ListOptions{Limit: pageLimit}
	for {
		items, cont, err := list(opts)
		if err != nil {
			return fmt.Errorf("listing %s: %w", what, err)
		}
		for i := range items {
			each(&items[i])
		}
		if cont == "" {
			return nil
		}
		opts.Continue = cont
	}
}
