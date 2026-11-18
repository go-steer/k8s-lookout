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

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// scanner holds one scan's inputs and accumulates findings.
type scanner struct {
	ctx     context.Context
	client  kubernetes.Interface
	ns      string // "" = all namespaces
	now     time.Time
	th      thresholds
	classes map[string]bool

	// tolerate turns a Forbidden List into a dropped part recorded in
	// unavailable (part → reason) instead of a failed pass.
	tolerate          bool
	tolerateTransient bool
	unavailable       map[string]string
	// refusals is the same per part, as the structured Refusal the
	// standalone command renders into read.unavailable records.
	refusals map[string]checks.Refusal

	findings []emit.Finding

	// nodes are the listed Nodes, kept for the scale-up explanation's
	// per-pool counts; nodesRead says the list ran and was not refused.
	nodes     []corev1.Node
	nodesRead bool
	// pendingCandidates, scheduledPods, and daemonSets are retained for
	// the capacity-squeeze account of unschedulable pods (#748): what
	// each node's requests add up to, and which DaemonSets are new,
	// without keeping full corev1.Pod structs across large clusters.
	podsRead          bool
	pendingCandidates []corev1.Pod
	scheduledPods     []scheduledPodReq
	daemonSets        []appsv1.DaemonSet
}

// view is what the pass listed, for explaining unschedulable pods.
func (s *scanner) view() clusterView {
	return clusterView{
		nodes:             s.nodes,
		nodesRead:         s.nodesRead,
		podsRead:          s.podsRead,
		pendingCandidates: s.pendingCandidates,
		scheduledPods:     s.scheduledPods,
		daemonSets:        s.daemonSets,
	}
}

// scan runs one paged List pass per resource kind needed by the
// enabled classes and derives all findings from those lists.
//
// scanned counts the objects of the enabled classes: pods, apps
// workloads, Jobs and CronJobs for the pods class; Nodes for nodes; PDBs for
// pdb; kube-system Deployments/DaemonSets for system (unless the
// pods class already counted them); ResourceQuotas for quota; HPAs
// for hpa. A
// list fetched only as auxiliary input (pods when just the nodes
// class needs per-node occupancy) is not counted — the summary
// reflects what was assessed, not what was downloaded.
func (s *scanner) scan(ctx context.Context) (int, []emit.Finding, error) {
	s.client = kube.WrapClient(ctx, s.client)
	s.ctx = ctx
	scanned := 0

	// Cluster-scoped classes follow the namespace scope rather
	// than ignoring it: the system class only makes sense where
	// the scope can see kube-system, and a --namespace scan does
	// not report Nodes (they belong to no namespace; scoping to
	// `prod` and getting unrelated node findings would make the
	// namespace filter lie).
	systemOn := s.classes[classSystem] && (s.ns == metav1.NamespaceAll || s.ns == metav1.NamespaceSystem)
	nodesOn := s.classes[classNodes] && s.ns == metav1.NamespaceAll

	// A refused List is fatal unless the caller asked for a tolerant
	// pass (ScanClusterPartial), in which case only the parts that
	// needed it are dropped, each named with the reason (#546).
	var (
		podsRefused       bool
		podScanned        int
		pendingCandidates []corev1.Pod
		scheduledPods     []scheduledPodReq
		occupancy         map[string]int
	)
	if nodesOn {
		occupancy = make(map[string]int)
	}
	var (
		prefetchWG sync.WaitGroup

		wlDeps []appsv1.Deployment
		wlStss []appsv1.StatefulSet
		wlDss  []appsv1.DaemonSet
		wlJobs []batchv1.Job
		wlCjs  []batchv1.CronJob
		wlErr  error

		prefetchedNodes []corev1.Node
		nodesErr        error
	)
	if s.classes[classPods] {
		prefetchWG.Add(1)
		go func() {
			defer prefetchWG.Done()
			wlErr = firstErr(
				func() (err error) { wlDeps, err = listDeployments(ctx, s.client, s.ns); return },
				func() (err error) { wlStss, err = listStatefulSets(ctx, s.client, s.ns); return },
				func() (err error) { wlDss, err = listDaemonSets(ctx, s.client, s.ns); return },
				func() (err error) { wlJobs, err = listJobs(ctx, s.client, s.ns); return },
				func() (err error) { wlCjs, err = listCronJobs(ctx, s.client, s.ns); return },
			)
		}()
	}
	if nodesOn {
		prefetchWG.Add(1)
		go func() {
			defer prefetchWG.Done()
			prefetchedNodes, nodesErr = listNodes(ctx, s.client)
		}()
	}

	if s.classes[classPods] || nodesOn {
		findingsBase := len(s.findings)
		err := streamPods(ctx, s.client, s.ns, func() {
			podScanned = 0
			s.findings = s.findings[:findingsBase]
			pendingCandidates = pendingCandidates[:0]
			scheduledPods = scheduledPods[:0]
			clear(occupancy)
		}, func(p *corev1.Pod) {
			podScanned++
			if s.classes[classPods] {
				prevLen := len(s.findings)
				s.checkPod(p)
				if len(s.findings) > prevLen &&
					s.findings[len(s.findings)-1].Kind == "pod.pending" &&
					s.findings[len(s.findings)-1].Severity == emit.SeverityCritical {
					kube.TrimReadPod(p)
					pendingCandidates = append(pendingCandidates, *p)
				}
				if req, ok := makeScheduledPodReq(p); ok {
					scheduledPods = append(scheduledPods, req)
				}
			}
			if nodesOn {
				recordPodOccupancy(occupancy, p)
			}
		})
		if err != nil {
			s.findings = s.findings[:findingsBase]
			var parts []string
			if s.classes[classPods] {
				parts = append(parts, PartPods)
			}
			if nodesOn {
				parts = append(parts, PartNodes)
			}
			if !s.refuse(err, parts...) {
				prefetchWG.Wait()
				return 0, nil, err
			}
			podsRefused = true
		}
	}
	prefetchWG.Wait()

	if s.classes[classPods] {
		if !podsRefused {
			scanned += podScanned
			s.podsRead = true
			s.pendingCandidates = pendingCandidates
			s.scheduledPods = scheduledPods
		}

		deps, stss, dss, jobs, cjs, err := wlDeps, wlStss, wlDss, wlJobs, wlCjs, wlErr
		switch {
		case err == nil:
			scanned += len(deps) + len(stss) + len(dss) + len(jobs) + len(cjs)
			s.daemonSets = dss
			s.checkWorkloads(deps, stss, dss, systemOn)
			s.checkJobs(jobs)
			s.checkCronJobs(cjs)
			if systemOn {
				// checkSystem skips objects outside kube-system.
				s.checkSystem(deps, dss)
			}
		case systemOn:
			if !s.refuse(err, PartWorkloads, PartSystem) {
				return 0, nil, err
			}
		default:
			if !s.refuse(err, PartWorkloads) {
				return 0, nil, err
			}
		}
	} else if systemOn {
		// One list per kind still holds: without the pods class
		// these are the only Deployment/DaemonSet lists issued,
		// and they are scoped to kube-system.
		var (
			deps []appsv1.Deployment
			dss  []appsv1.DaemonSet
		)
		err := firstErr(
			func() (err error) { deps, err = listDeployments(ctx, s.client, metav1.NamespaceSystem); return },
			func() (err error) { dss, err = listDaemonSets(ctx, s.client, metav1.NamespaceSystem); return },
		)
		if err != nil {
			if !s.refuse(err, PartSystem) {
				return 0, nil, err
			}
		} else {
			scanned += len(deps) + len(dss)
			s.checkSystem(deps, dss)
		}
	}

	if nodesOn && !podsRefused {
		nodes, err := prefetchedNodes, nodesErr
		if err != nil {
			if !s.refuse(err, PartNodes) {
				return 0, nil, err
			}
		} else {
			scanned += len(nodes)
			s.checkNodesWithOccupancy(nodes, occupancy)
			s.nodes, s.nodesRead = nodes, true
		}
	}

	if s.classes[classPDB] {
		pdbs, err := listPDBs(ctx, s.client, s.ns)
		if err != nil {
			if !s.refuse(err, PartPDB) {
				return 0, nil, err
			}
		} else {
			scanned += len(pdbs)
			s.checkPDBs(pdbs)
		}
	}

	if s.classes[classQuota] {
		quotas, err := listQuotas(ctx, s.client, s.ns)
		if err != nil {
			if !s.refuse(err, PartQuota) {
				return 0, nil, err
			}
		} else {
			scanned += len(quotas)
			s.checkQuotas(quotas)
		}
	}

	if s.classes[classHPA] {
		hpas, err := listHPAs(ctx, s.client, s.ns)
		if err != nil {
			if !s.refuse(err, PartHPA) {
				return 0, nil, err
			}
		} else {
			scanned += len(hpas)
			s.checkHPAs(hpas)
		}
	}

	return scanned, s.findings, nil
}

// The parts of a delta pass a tolerant scan can lose independently.
// They are the classes, except that the pods class splits in two: its
// pod derivations need only Pods, its workload, Job and CronJob
// derivations need the apps and batch Lists.
const (
	PartPods      = "pods"      // pod.* findings
	PartWorkloads = "workloads" // workload.*, job.*, cron.* findings
	PartNodes     = "nodes"
	PartSystem    = "system"
	PartPDB       = "pdb"
	PartQuota     = "quota"
	PartHPA       = "hpa"
)

// partOrder and partCost name, per part, what a refused List cost the
// answer — the tail of its read.unavailable record.
var (
	partOrder = []string{PartPods, PartWorkloads, PartNodes, PartSystem, PartPDB, PartQuota, PartHPA}
	partCost  = map[string]string{
		PartPods:      "pod.* findings not checked",
		PartWorkloads: "workload.*, job.* and cron.* findings not checked",
		PartNodes:     "node.* findings not checked",
		PartSystem:    "addon.* findings not checked",
		PartPDB:       "pdb.* findings not checked",
		PartQuota:     "quota.* findings not checked",
		PartHPA:       "hpa.* findings not checked",
	}
)

// unreadFindings renders one read.unavailable record per refused
// resource, naming every part it cost, in part order.
func (s *scanner) unreadFindings() []emit.Finding {
	var (
		order []string
		byRes = map[string]checks.Refusal{}
		costs = map[string][]string{}
	)
	for _, p := range partOrder {
		r, ok := s.refusals[p]
		if !ok {
			continue
		}
		t := r.Target()
		if _, seen := byRes[t]; !seen {
			order = append(order, t)
			byRes[t] = r
		}
		costs[t] = append(costs[t], partCost[p])
	}
	out := make([]emit.Finding, 0, len(order))
	for _, t := range order {
		out = append(out, checks.RefusedFinding(byRes[t], strings.Join(costs[t], "; ")))
	}
	return out
}

// refuse records err against parts when the pass is tolerant and err
// is an authorization refusal (or a non-context API error when
// tolerateTransient is set), and reports whether it did.
func (s *scanner) refuse(err error, parts ...string) bool {
	if !s.tolerate {
		return false
	}
	if s.unavailable == nil {
		s.unavailable = map[string]string{}
		s.refusals = map[string]checks.Refusal{}
	}
	if r, ok := checks.ForbiddenRefusal(err); ok {
		for _, p := range parts {
			if _, seen := s.unavailable[p]; !seen {
				s.unavailable[p] = r.String()
				s.refusals[p] = r
			}
		}
		return true
	}
	if s.tolerateTransient && (s.ctx == nil || s.ctx.Err() == nil) {
		for _, p := range parts {
			if _, seen := s.unavailable[p]; !seen {
				s.unavailable[p] = err.Error()
			}
		}
		return true
	}
	return false
}

// firstErr runs fns in order and returns the first error, skipping the
// rest — the same one-at-a-time List order the strict pass always had.
func firstErr(fns ...func() error) error {
	for _, fn := range fns {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

// add appends one finding, truncating the message for token density.
func (s *scanner) add(f emit.Finding) {
	f.Message = truncate(f.Message, 200)
	s.findings = append(s.findings, f)
}

// truncate caps free-text fields; API-server condition messages can
// run to paragraphs and the tail is rarely the signal.
func truncate(msg string, n int) string {
	if len(msg) <= n {
		return msg
	}
	return msg[:n-1] + "…"
}

// age renders how long ago t was, truncated to the second — ages are
// findings data, so they must be deterministic under a pinned clock.
func (s *scanner) age(t time.Time) string {
	return s.now.Sub(t).Truncate(time.Second).String()
}

func itoa(n int) string     { return strconv.Itoa(n) }
func itoa32(n int32) string { return strconv.FormatInt(int64(n), 10) }

// paged drains one List call chain via kube.ListAllPages (recovering
// transparently from HTTP 410 Gone continue-token expiry).
func paged[T any](ctx context.Context, list func(context.Context, metav1.ListOptions) ([]T, string, error)) ([]T, error) {
	return kube.ListAllPages(ctx, list)
}

func streamPods(ctx context.Context, c kubernetes.Interface, ns string, reset func(), each func(*corev1.Pod)) error {
	if err := kube.StreamClusterPods(ctx, c, ns, nil, reset, each); err != nil {
		return fmt.Errorf("listing pods: %w", err)
	}
	return nil
}

func listNodes(ctx context.Context, c kubernetes.Interface) ([]corev1.Node, error) {
	return kube.ListAllPagesTrim(ctx, func(ctx context.Context, opts metav1.ListOptions) ([]corev1.Node, string, error) {
		l, err := c.CoreV1().Nodes().List(ctx, opts)
		if err != nil {
			return nil, "", fmt.Errorf("listing nodes: %w", err)
		}
		return l.Items, l.Continue, nil
	}, kube.TrimReadNode)
}

func listDeployments(ctx context.Context, c kubernetes.Interface, ns string) ([]appsv1.Deployment, error) {
	return paged(ctx, func(ctx context.Context, opts metav1.ListOptions) ([]appsv1.Deployment, string, error) {
		l, err := c.AppsV1().Deployments(ns).List(ctx, opts)
		if err != nil {
			return nil, "", fmt.Errorf("listing deployments: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func listStatefulSets(ctx context.Context, c kubernetes.Interface, ns string) ([]appsv1.StatefulSet, error) {
	return paged(ctx, func(ctx context.Context, opts metav1.ListOptions) ([]appsv1.StatefulSet, string, error) {
		l, err := c.AppsV1().StatefulSets(ns).List(ctx, opts)
		if err != nil {
			return nil, "", fmt.Errorf("listing statefulsets: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func listDaemonSets(ctx context.Context, c kubernetes.Interface, ns string) ([]appsv1.DaemonSet, error) {
	return paged(ctx, func(ctx context.Context, opts metav1.ListOptions) ([]appsv1.DaemonSet, string, error) {
		l, err := c.AppsV1().DaemonSets(ns).List(ctx, opts)
		if err != nil {
			return nil, "", fmt.Errorf("listing daemonsets: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func listJobs(ctx context.Context, c kubernetes.Interface, ns string) ([]batchv1.Job, error) {
	return paged(ctx, func(ctx context.Context, opts metav1.ListOptions) ([]batchv1.Job, string, error) {
		l, err := c.BatchV1().Jobs(ns).List(ctx, opts)
		if err != nil {
			return nil, "", fmt.Errorf("listing jobs: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func listCronJobs(ctx context.Context, c kubernetes.Interface, ns string) ([]batchv1.CronJob, error) {
	return paged(ctx, func(ctx context.Context, opts metav1.ListOptions) ([]batchv1.CronJob, string, error) {
		l, err := c.BatchV1().CronJobs(ns).List(ctx, opts)
		if err != nil {
			return nil, "", fmt.Errorf("listing cronjobs: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func listPDBs(ctx context.Context, c kubernetes.Interface, ns string) ([]policyv1.PodDisruptionBudget, error) {
	return paged(ctx, func(ctx context.Context, opts metav1.ListOptions) ([]policyv1.PodDisruptionBudget, string, error) {
		l, err := c.PolicyV1().PodDisruptionBudgets(ns).List(ctx, opts)
		if err != nil {
			return nil, "", fmt.Errorf("listing poddisruptionbudgets: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func listQuotas(ctx context.Context, c kubernetes.Interface, ns string) ([]corev1.ResourceQuota, error) {
	return paged(ctx, func(ctx context.Context, opts metav1.ListOptions) ([]corev1.ResourceQuota, string, error) {
		l, err := c.CoreV1().ResourceQuotas(ns).List(ctx, opts)
		if err != nil {
			return nil, "", fmt.Errorf("listing resourcequotas: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func listHPAs(ctx context.Context, c kubernetes.Interface, ns string) ([]autoscalingv2.HorizontalPodAutoscaler, error) {
	return paged(ctx, func(ctx context.Context, opts metav1.ListOptions) ([]autoscalingv2.HorizontalPodAutoscaler, string, error) {
		l, err := c.AutoscalingV2().HorizontalPodAutoscalers(ns).List(ctx, opts)
		if err != nil {
			return nil, "", fmt.Errorf("listing horizontalpodautoscalers: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}
