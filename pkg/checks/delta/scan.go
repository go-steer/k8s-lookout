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
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"

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
	client  kubernetes.Interface
	ns      string // "" = all namespaces
	now     time.Time
	th      thresholds
	classes map[string]bool

	// tolerate turns a Forbidden List into a dropped part recorded in
	// unavailable (part → reason) instead of a failed pass.
	tolerate    bool
	unavailable map[string]string

	findings []emit.Finding
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
	var podsRefused bool
	var pods []corev1.Pod
	if s.classes[classPods] || nodesOn {
		var err error
		pods, err = listPods(ctx, s.client, s.ns)
		if err != nil {
			var parts []string
			if s.classes[classPods] {
				parts = append(parts, PartPods)
			}
			if nodesOn {
				parts = append(parts, PartNodes)
			}
			if !s.refuse(err, parts...) {
				return 0, nil, err
			}
			podsRefused = true
		}
	}

	if s.classes[classPods] {
		if !podsRefused {
			scanned += len(pods)
			s.checkPods(pods)
		}

		var (
			deps []appsv1.Deployment
			stss []appsv1.StatefulSet
			dss  []appsv1.DaemonSet
			jobs []batchv1.Job
			cjs  []batchv1.CronJob
		)
		err := firstErr(
			func() (err error) { deps, err = listDeployments(ctx, s.client, s.ns); return },
			func() (err error) { stss, err = listStatefulSets(ctx, s.client, s.ns); return },
			func() (err error) { dss, err = listDaemonSets(ctx, s.client, s.ns); return },
			func() (err error) { jobs, err = listJobs(ctx, s.client, s.ns); return },
			func() (err error) { cjs, err = listCronJobs(ctx, s.client, s.ns); return },
		)
		switch {
		case err == nil:
			scanned += len(deps) + len(stss) + len(dss) + len(jobs) + len(cjs)
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
		nodes, err := listNodes(ctx, s.client)
		if err != nil {
			if !s.refuse(err, PartNodes) {
				return 0, nil, err
			}
		} else {
			scanned += len(nodes)
			s.checkNodes(nodes, pods)
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

// refuse records err against parts when the pass is tolerant and err
// is an authorization refusal, and reports whether it did. Anything
// else stays fatal.
func (s *scanner) refuse(err error, parts ...string) bool {
	if !s.tolerate {
		return false
	}
	reason, ok := checks.ListForbidden(err)
	if !ok {
		return false
	}
	if s.unavailable == nil {
		s.unavailable = map[string]string{}
	}
	for _, p := range parts {
		if _, seen := s.unavailable[p]; !seen {
			s.unavailable[p] = reason
		}
	}
	return true
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

// pageSize keeps single List responses bounded on large clusters;
// the loop follows Continue tokens until the pass is complete.
const pageSize = 500

// paged drains one List call chain.
func paged[T any](ctx context.Context, list func(context.Context, metav1.ListOptions) ([]T, string, error)) ([]T, error) {
	var out []T
	opts := metav1.ListOptions{Limit: pageSize}
	for {
		items, cont, err := list(ctx, opts)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
		if cont == "" {
			return out, nil
		}
		opts.Continue = cont
	}
}

func listPods(ctx context.Context, c kubernetes.Interface, ns string) ([]corev1.Pod, error) {
	return paged(ctx, func(ctx context.Context, opts metav1.ListOptions) ([]corev1.Pod, string, error) {
		l, err := c.CoreV1().Pods(ns).List(ctx, opts)
		if err != nil {
			return nil, "", fmt.Errorf("listing pods: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func listNodes(ctx context.Context, c kubernetes.Interface) ([]corev1.Node, error) {
	return paged(ctx, func(ctx context.Context, opts metav1.ListOptions) ([]corev1.Node, string, error) {
		l, err := c.CoreV1().Nodes().List(ctx, opts)
		if err != nil {
			return nil, "", fmt.Errorf("listing nodes: %w", err)
		}
		return l.Items, l.Continue, nil
	})
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
