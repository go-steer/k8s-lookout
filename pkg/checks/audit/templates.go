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

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// podTemplate is one pod template with the object an operator would
// edit to change it. Flattened so a judgment does not have to switch on
// the concrete apps/v1 or batch/v1 type it came from.
type podTemplate struct {
	kind      string
	namespace string
	name      string
	// labels are the TEMPLATE's labels, not the owner's. They are what a
	// selector written against these pods — a NetworkPolicy's, a PDB's —
	// actually matches, and the two sets routinely differ.
	labels map[string]string
	spec   corev1.PodSpec
}

// listPodTemplates enumerates every pod template in scope: the workload
// kinds that carry one, plus Jobs and Pods that are nobody's copy.
//
// This is the subject population shared by the posture detectors that
// judge "what does this thing run like" rather than "is this thing
// available" — `audit hardening` (#183) and `audit netpol` (#185). It
// is wider than `audit workloads`' three kinds on purpose: a privileged
// CronJob is as privileged as a privileged Deployment, and an
// unselected hand-rolled Pod is as reachable as an unselected one.
//
// # Judged once, at the owner
//
// A Job created by a CronJob and a Pod created by anything both carry
// an ownerReference and are skipped. Their template was already judged
// at the object an operator would edit, and reporting the copies would
// turn one defect into as many findings as the controller happens to
// have created.
//
// The result is sorted by namespace, kind, then name, which is what
// makes a caller's per-namespace aggregates deterministic before
// sortFindings ever sees them.
func listPodTemplates(ctx context.Context, client kubernetes.Interface, ns string) ([]podTemplate, error) {
	var out []podTemplate
	add := func(kind, namespace, name string, labels map[string]string, spec corev1.PodSpec) {
		out = append(out, podTemplate{
			kind: kind, namespace: namespace, name: name, labels: labels, spec: spec,
		})
	}
	steps := []func() error{
		func() error {
			return listPages("deployments", func(o metav1.ListOptions) ([]appsv1.Deployment, string, error) {
				l, err := client.AppsV1().Deployments(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(d *appsv1.Deployment) {
				add("Deployment", d.Namespace, d.Name, d.Spec.Template.Labels, d.Spec.Template.Spec)
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
				add("StatefulSet", s.Namespace, s.Name, s.Spec.Template.Labels, s.Spec.Template.Spec)
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
				add("DaemonSet", d.Namespace, d.Name, d.Spec.Template.Labels, d.Spec.Template.Spec)
			})
		},
		func() error {
			return listPages("cronjobs", func(o metav1.ListOptions) ([]batchv1.CronJob, string, error) {
				l, err := client.BatchV1().CronJobs(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(c *batchv1.CronJob) {
				t := c.Spec.JobTemplate.Spec.Template
				add("CronJob", c.Namespace, c.Name, t.Labels, t.Spec)
			})
		},
		func() error {
			return listPages("jobs", func(o metav1.ListOptions) ([]batchv1.Job, string, error) {
				l, err := client.BatchV1().Jobs(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(j *batchv1.Job) {
				// A Job spawned by a CronJob repeats its owner's template;
				// the CronJob is the object an operator edits.
				if len(j.OwnerReferences) > 0 {
					return
				}
				add("Job", j.Namespace, j.Name, j.Spec.Template.Labels, j.Spec.Template.Spec)
			})
		},
		func() error {
			return listPages("pods", func(o metav1.ListOptions) ([]corev1.Pod, string, error) {
				l, err := client.CoreV1().Pods(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(p *corev1.Pod) {
				// Every pod with an owner is a copy of a template judged
				// above; only hand-rolled pods are their own subject.
				if len(p.OwnerReferences) > 0 {
					return
				}
				add("Pod", p.Namespace, p.Name, p.Labels, p.Spec)
			})
		},
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return nil, err
		}
	}

	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.namespace != b.namespace {
			return a.namespace < b.namespace
		}
		if a.kind != b.kind {
			return a.kind < b.kind
		}
		return a.name < b.name
	})
	return out, nil
}

// listNamespacesInScope resolves the namespace population that a
// namespace-subject claim is made about: every namespace under -A,
// exactly the one asked for otherwise. A --namespace that does not
// exist is an error, not an empty clean scan.
func listNamespacesInScope(ctx context.Context, client kubernetes.Interface, ns string) ([]*corev1.Namespace, error) {
	if ns != metav1.NamespaceAll {
		got, err := client.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("namespace %q not found", ns)
			}
			return nil, fmt.Errorf("getting namespace %s: %w", ns, err)
		}
		return []*corev1.Namespace{got}, nil
	}
	var out []*corev1.Namespace
	err := listPages("namespaces", func(o metav1.ListOptions) ([]corev1.Namespace, string, error) {
		l, err := client.CoreV1().Namespaces().List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(n *corev1.Namespace) {
		out = append(out, n)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
