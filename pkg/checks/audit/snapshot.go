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
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
)

// Snapshot is the audit group's shared, lazy, per-invocation read of
// the cluster. Each getter lists its kind at most once for the life of
// the snapshot, so detectors that run together (`audit workloads` and a
// rules evaluation in the same process, or `audit all`) read shared
// resources once instead of once each. A detector that never asks never
// pays.
//
// The snapshot rides the context (WithSnapshot / snapshotFor) rather
// than Deps, because Deps is per command and the snapshot is per
// invocation: the same Deps value serves every call, the snapshot
// serves one scan of one scope.
type Snapshot struct {
	client kubernetes.Interface
	ns     string

	deployments         snapshotEntry[appsv1.Deployment]
	statefulsets        snapshotEntry[appsv1.StatefulSet]
	daemonsets          snapshotEntry[appsv1.DaemonSet]
	jobs                snapshotEntry[batchv1.Job]
	cronjobs            snapshotEntry[batchv1.CronJob]
	pods                snapshotEntry[corev1.Pod]
	namespaces          snapshotEntry[corev1.Namespace]
	nodes               snapshotEntry[corev1.Node]
	serviceAccounts     snapshotEntry[corev1.ServiceAccount]
	pdbs                snapshotEntry[policyv1.PodDisruptionBudget]
	hpas                snapshotEntry[autoscalingv2.HorizontalPodAutoscaler]
	networkPolicies     snapshotEntry[networkingv1.NetworkPolicy]
	roles               snapshotEntry[rbacv1.Role]
	roleBindings        snapshotEntry[rbacv1.RoleBinding]
	clusterRoles        snapshotEntry[rbacv1.ClusterRole]
	clusterRoleBindings snapshotEntry[rbacv1.ClusterRoleBinding]

	servicesOnce    sync.Once
	services        []*corev1.Service
	servicesRefused *checks.Refusal
	servicesErr     error

	skippedMu sync.Mutex
	skippedNS map[string]bool
}

func (s *Snapshot) recordSkippedNS(ns string) {
	if s == nil || ns == "" {
		return
	}
	s.skippedMu.Lock()
	if s.skippedNS == nil {
		s.skippedNS = map[string]bool{}
	}
	s.skippedNS[ns] = true
	s.skippedMu.Unlock()
}

func (s *Snapshot) skippedNSCount() int {
	if s == nil {
		return 0
	}
	s.skippedMu.Lock()
	defer s.skippedMu.Unlock()
	return len(s.skippedNS)
}

type snapshotEntry[T any] struct {
	once    sync.Once
	items   []T
	refused *checks.Refusal
	err     error
}

type snapshotKey struct{}

// NewSnapshot constructs a lazy snapshot scoped to ns (empty string means
// metav1.NamespaceAll). Nothing is fetched until a getter is called.
func NewSnapshot(client kubernetes.Interface, ns string) *Snapshot {
	return &Snapshot{client: client, ns: ns}
}

// WithSnapshot attaches a Snapshot to ctx so the audit commands it calls
// share one read per resource kind.
func WithSnapshot(ctx context.Context, s *Snapshot) context.Context {
	return context.WithValue(ctx, snapshotKey{}, s)
}

// snapshotFor returns the Snapshot on ctx if one is attached for the
// same client and namespace, or a fresh one scoped to THIS call. The
// identity check protects a caller that attaches a cluster-wide
// snapshot and then runs a single-namespace check underneath it.
func snapshotFor(ctx context.Context, client kubernetes.Interface, ns string) *Snapshot {
	if s, ok := ctx.Value(snapshotKey{}).(*Snapshot); ok && s != nil && s.client == client && s.ns == ns {
		return s
	}
	return NewSnapshot(client, ns)
}

func ensureSnapshot(ctx context.Context, client kubernetes.Interface, ns string) (context.Context, *Snapshot) {
	if s, ok := ctx.Value(snapshotKey{}).(*Snapshot); ok && s != nil && s.client == client && s.ns == ns {
		return ctx, s
	}
	s := NewSnapshot(client, ns)
	return WithSnapshot(ctx, s), s
}

// fatalRefusal turns a cached Refusal into the RefusalError a caller
// returns when the refused list is one the command cannot answer without.
func fatalRefusal(r *checks.Refusal, context string) error {
	return &checks.RefusalError{
		Refusal: *r,
		What:    "the command cannot answer without it",
		Context: context,
	}
}

func loadSlice[T any](
	e *snapshotEntry[T],
	what string,
	fetch func(metav1.ListOptions) ([]T, string, error),
	nsOf func(*T) string,
	nameOf func(*T) string,
) ([]T, *checks.Refusal, error) {
	e.once.Do(func() {
		err := listPages(what, fetch, func(item *T) {
			e.items = append(e.items, *item)
		})
		if err != nil {
			if r, ok := checks.ForbiddenRefusal(err); ok {
				e.refused = &r
				return
			}
			e.err = err
			return
		}
		sort.Slice(e.items, func(i, j int) bool {
			a, b := &e.items[i], &e.items[j]
			if nsOf != nil {
				na, nb := nsOf(a), nsOf(b)
				if na != nb {
					return na < nb
				}
			}
			return nameOf(a) < nameOf(b)
		})
	})
	return e.items, e.refused, e.err
}

// Deployments lists the Deployments in scope, once, sorted by namespace/name.
func (s *Snapshot) Deployments(ctx context.Context) ([]appsv1.Deployment, *checks.Refusal, error) {
	return loadSlice(&s.deployments, "deployments", func(o metav1.ListOptions) ([]appsv1.Deployment, string, error) {
		l, err := s.client.AppsV1().Deployments(s.ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(d *appsv1.Deployment) string { return d.Namespace }, func(d *appsv1.Deployment) string { return d.Name })
}

// StatefulSets lists the StatefulSets in scope, once, sorted by namespace/name.
func (s *Snapshot) StatefulSets(ctx context.Context) ([]appsv1.StatefulSet, *checks.Refusal, error) {
	return loadSlice(&s.statefulsets, "statefulsets", func(o metav1.ListOptions) ([]appsv1.StatefulSet, string, error) {
		l, err := s.client.AppsV1().StatefulSets(s.ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(st *appsv1.StatefulSet) string { return st.Namespace }, func(st *appsv1.StatefulSet) string { return st.Name })
}

// DaemonSets lists the DaemonSets in scope, once, sorted by namespace/name.
func (s *Snapshot) DaemonSets(ctx context.Context) ([]appsv1.DaemonSet, *checks.Refusal, error) {
	return loadSlice(&s.daemonsets, "daemonsets", func(o metav1.ListOptions) ([]appsv1.DaemonSet, string, error) {
		l, err := s.client.AppsV1().DaemonSets(s.ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(ds *appsv1.DaemonSet) string { return ds.Namespace }, func(ds *appsv1.DaemonSet) string { return ds.Name })
}

// Jobs lists the Jobs in scope, once, sorted by namespace/name.
func (s *Snapshot) Jobs(ctx context.Context) ([]batchv1.Job, *checks.Refusal, error) {
	return loadSlice(&s.jobs, "jobs", func(o metav1.ListOptions) ([]batchv1.Job, string, error) {
		l, err := s.client.BatchV1().Jobs(s.ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(j *batchv1.Job) string { return j.Namespace }, func(j *batchv1.Job) string { return j.Name })
}

// CronJobs lists the CronJobs in scope, once, sorted by namespace/name.
func (s *Snapshot) CronJobs(ctx context.Context) ([]batchv1.CronJob, *checks.Refusal, error) {
	return loadSlice(&s.cronjobs, "cronjobs", func(o metav1.ListOptions) ([]batchv1.CronJob, string, error) {
		l, err := s.client.BatchV1().CronJobs(s.ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(cj *batchv1.CronJob) string { return cj.Namespace }, func(cj *batchv1.CronJob) string { return cj.Name })
}

// Pods lists the Pods in scope, once, sorted by namespace/name.
func (s *Snapshot) Pods(ctx context.Context) ([]corev1.Pod, *checks.Refusal, error) {
	return loadSlice(&s.pods, "pods", func(o metav1.ListOptions) ([]corev1.Pod, string, error) {
		l, err := s.client.CoreV1().Pods(s.ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(p *corev1.Pod) string { return p.Namespace }, func(p *corev1.Pod) string { return p.Name })
}

// Namespaces returns the Namespaces in scope, once, sorted by name.
// When the snapshot is scoped to a single namespace, it fetches that
// Namespace via Get rather than listing all namespaces cluster-wide.
func (s *Snapshot) Namespaces(ctx context.Context) ([]corev1.Namespace, *checks.Refusal, error) {
	if s.ns == metav1.NamespaceAll {
		return loadSlice(&s.namespaces, "namespaces", func(o metav1.ListOptions) ([]corev1.Namespace, string, error) {
			l, err := s.client.CoreV1().Namespaces().List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, nil, func(n *corev1.Namespace) string { return n.Name })
	}
	s.namespaces.once.Do(func() {
		got, err := s.client.CoreV1().Namespaces().Get(ctx, s.ns, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			s.namespaces.err = fmt.Errorf("namespace %q not found", s.ns)
			return
		}
		if err != nil {
			if r, ok := checks.ForbiddenRefusal(err); ok {
				s.namespaces.refused = &r
				return
			}
			s.namespaces.err = fmt.Errorf("getting namespace %s: %w", s.ns, err)
			return
		}
		s.namespaces.items = []corev1.Namespace{*got}
	})
	return s.namespaces.items, s.namespaces.refused, s.namespaces.err
}

// Nodes lists the cluster's Nodes, once, sorted by name.
func (s *Snapshot) Nodes(ctx context.Context) ([]corev1.Node, *checks.Refusal, error) {
	return loadSlice(&s.nodes, "nodes", func(o metav1.ListOptions) ([]corev1.Node, string, error) {
		l, err := s.client.CoreV1().Nodes().List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, nil, func(n *corev1.Node) string { return n.Name })
}

// ServiceAccounts lists the ServiceAccounts in scope, once, sorted by namespace/name.
func (s *Snapshot) ServiceAccounts(ctx context.Context) ([]corev1.ServiceAccount, *checks.Refusal, error) {
	return loadSlice(&s.serviceAccounts, "serviceaccounts", func(o metav1.ListOptions) ([]corev1.ServiceAccount, string, error) {
		l, err := s.client.CoreV1().ServiceAccounts(s.ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(sa *corev1.ServiceAccount) string { return sa.Namespace }, func(sa *corev1.ServiceAccount) string { return sa.Name })
}

// Services lists the Services in scope, once. An RBAC refusal is not an
// error: it is returned as a Refusal so the caller can skip the claims
// that need Services and say so, rather than fail the whole command.
func (s *Snapshot) Services(ctx context.Context) ([]*corev1.Service, *checks.Refusal, error) {
	s.servicesOnce.Do(func() {
		err := listPages("services", func(o metav1.ListOptions) ([]corev1.Service, string, error) {
			l, err := s.client.CoreV1().Services(s.ns).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(svc *corev1.Service) {
			s.services = append(s.services, svc)
		})
		if err != nil {
			if r, ok := checks.ForbiddenRefusal(err); ok {
				s.servicesRefused = &r
				return
			}
			s.servicesErr = err
			return
		}
		sort.Slice(s.services, func(i, j int) bool {
			a, b := s.services[i], s.services[j]
			if a.Namespace != b.Namespace {
				return a.Namespace < b.Namespace
			}
			return a.Name < b.Name
		})
	})
	return s.services, s.servicesRefused, s.servicesErr
}

// PDBs lists the PodDisruptionBudgets in scope, once, sorted by namespace/name.
func (s *Snapshot) PDBs(ctx context.Context) ([]policyv1.PodDisruptionBudget, *checks.Refusal, error) {
	return loadSlice(&s.pdbs, "poddisruptionbudgets", func(o metav1.ListOptions) ([]policyv1.PodDisruptionBudget, string, error) {
		l, err := s.client.PolicyV1().PodDisruptionBudgets(s.ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(p *policyv1.PodDisruptionBudget) string { return p.Namespace }, func(p *policyv1.PodDisruptionBudget) string { return p.Name })
}

// HPAs lists the HorizontalPodAutoscalers in scope, once, sorted by namespace/name.
func (s *Snapshot) HPAs(ctx context.Context) ([]autoscalingv2.HorizontalPodAutoscaler, *checks.Refusal, error) {
	return loadSlice(&s.hpas, "horizontalpodautoscalers", func(o metav1.ListOptions) ([]autoscalingv2.HorizontalPodAutoscaler, string, error) {
		l, err := s.client.AutoscalingV2().HorizontalPodAutoscalers(s.ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(h *autoscalingv2.HorizontalPodAutoscaler) string { return h.Namespace }, func(h *autoscalingv2.HorizontalPodAutoscaler) string { return h.Name })
}

// NetworkPolicies lists the NetworkPolicies in scope, once, sorted by namespace/name.
func (s *Snapshot) NetworkPolicies(ctx context.Context) ([]networkingv1.NetworkPolicy, *checks.Refusal, error) {
	return loadSlice(&s.networkPolicies, "networkpolicies", func(o metav1.ListOptions) ([]networkingv1.NetworkPolicy, string, error) {
		l, err := s.client.NetworkingV1().NetworkPolicies(s.ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(np *networkingv1.NetworkPolicy) string { return np.Namespace }, func(np *networkingv1.NetworkPolicy) string { return np.Name })
}

// Roles lists the Roles in scope, once, sorted by namespace/name.
func (s *Snapshot) Roles(ctx context.Context) ([]rbacv1.Role, *checks.Refusal, error) {
	return loadSlice(&s.roles, "roles", func(o metav1.ListOptions) ([]rbacv1.Role, string, error) {
		l, err := s.client.RbacV1().Roles(s.ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(r *rbacv1.Role) string { return r.Namespace }, func(r *rbacv1.Role) string { return r.Name })
}

// RoleBindings lists the RoleBindings in scope, once, sorted by namespace/name.
func (s *Snapshot) RoleBindings(ctx context.Context) ([]rbacv1.RoleBinding, *checks.Refusal, error) {
	return loadSlice(&s.roleBindings, "rolebindings", func(o metav1.ListOptions) ([]rbacv1.RoleBinding, string, error) {
		l, err := s.client.RbacV1().RoleBindings(s.ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(rb *rbacv1.RoleBinding) string { return rb.Namespace }, func(rb *rbacv1.RoleBinding) string { return rb.Name })
}

// ClusterRoles lists the ClusterRoles in the cluster, once, sorted by name.
func (s *Snapshot) ClusterRoles(ctx context.Context) ([]rbacv1.ClusterRole, *checks.Refusal, error) {
	return loadSlice(&s.clusterRoles, "clusterroles", func(o metav1.ListOptions) ([]rbacv1.ClusterRole, string, error) {
		l, err := s.client.RbacV1().ClusterRoles().List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, nil, func(cr *rbacv1.ClusterRole) string { return cr.Name })
}

// ClusterRoleBindings lists the ClusterRoleBindings in the cluster, once, sorted by name.
func (s *Snapshot) ClusterRoleBindings(ctx context.Context) ([]rbacv1.ClusterRoleBinding, *checks.Refusal, error) {
	return loadSlice(&s.clusterRoleBindings, "clusterrolebindings", func(o metav1.ListOptions) ([]rbacv1.ClusterRoleBinding, string, error) {
		l, err := s.client.RbacV1().ClusterRoleBindings().List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, nil, func(crb *rbacv1.ClusterRoleBinding) string { return crb.Name })
}

// selectingServices returns the Services in a namespace whose selector
// matches a pod template's labels. ExternalName Services and Services
// with no selector route to nothing in the cluster and are skipped —
// the same rule collect.py's `_selecting_services` applies, so the two
// tools agree on what "Service-backed" means.
func selectingServices(services []*corev1.Service, namespace string, podLabels map[string]string) []*corev1.Service {
	var out []*corev1.Service
	for _, svc := range services {
		if svc.Namespace != namespace || svc.Spec.Type == corev1.ServiceTypeExternalName || len(svc.Spec.Selector) == 0 {
			continue
		}
		if labels.SelectorFromSet(svc.Spec.Selector).Matches(labels.Set(podLabels)) {
			out = append(out, svc)
		}
	}
	return out
}
