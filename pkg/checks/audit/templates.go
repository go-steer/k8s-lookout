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
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
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
//
// With tolerate, a kind whose List RBAC refuses is skipped and named in
// the returned refusals (#584) — for a caller whose claims are each
// about one template, so a missing kind costs only its own templates.
// Without it any error is fatal: `audit netpol` judges policies against
// the whole population, where a missing kind would make a live policy
// read as selecting nothing.
func listPodTemplates(ctx context.Context, client kubernetes.Interface, ns string, tolerate bool) ([]podTemplate, []kindRefusal, error) {
	snap := snapshotFor(ctx, client, ns)
	var out []podTemplate
	add := func(kind, namespace, name string, labels map[string]string, spec corev1.PodSpec) {
		out = append(out, podTemplate{
			kind: kind, namespace: namespace, name: name, labels: labels, spec: spec,
		})
	}
	steps := []struct {
		what string
		kind string
		run  func() (*checks.Refusal, error)
	}{
		{
			what: "deployments",
			kind: "Deployment",
			run: func() (*checks.Refusal, error) {
				items, r, err := snap.Deployments(ctx)
				if err != nil || r != nil {
					return r, err
				}
				for i := range items {
					d := &items[i]
					add("Deployment", d.Namespace, d.Name, d.Spec.Template.Labels, d.Spec.Template.Spec)
				}
				return nil, nil
			},
		},
		{
			what: "statefulsets",
			kind: "StatefulSet",
			run: func() (*checks.Refusal, error) {
				items, r, err := snap.StatefulSets(ctx)
				if err != nil || r != nil {
					return r, err
				}
				for i := range items {
					s := &items[i]
					add("StatefulSet", s.Namespace, s.Name, s.Spec.Template.Labels, s.Spec.Template.Spec)
				}
				return nil, nil
			},
		},
		{
			what: "daemonsets",
			kind: "DaemonSet",
			run: func() (*checks.Refusal, error) {
				items, r, err := snap.DaemonSets(ctx)
				if err != nil || r != nil {
					return r, err
				}
				for i := range items {
					d := &items[i]
					add("DaemonSet", d.Namespace, d.Name, d.Spec.Template.Labels, d.Spec.Template.Spec)
				}
				return nil, nil
			},
		},
		{
			what: "cronjobs",
			kind: "CronJob",
			run: func() (*checks.Refusal, error) {
				items, r, err := snap.CronJobs(ctx)
				if err != nil || r != nil {
					return r, err
				}
				for i := range items {
					c := &items[i]
					t := c.Spec.JobTemplate.Spec.Template
					add("CronJob", c.Namespace, c.Name, t.Labels, t.Spec)
				}
				return nil, nil
			},
		},
		{
			what: "jobs",
			kind: "unowned Job",
			run: func() (*checks.Refusal, error) {
				items, r, err := snap.Jobs(ctx)
				if err != nil || r != nil {
					return r, err
				}
				for i := range items {
					j := &items[i]
					// A Job spawned by a CronJob repeats its owner's template;
					// the CronJob is the object an operator edits.
					if len(j.OwnerReferences) > 0 {
						continue
					}
					add("Job", j.Namespace, j.Name, j.Spec.Template.Labels, j.Spec.Template.Spec)
				}
				return nil, nil
			},
		},
		{
			what: "pods",
			kind: "unowned Pod",
			run: func() (*checks.Refusal, error) {
				items, r, err := snap.Pods(ctx)
				if err != nil || r != nil {
					return r, err
				}
				for i := range items {
					p := &items[i]
					// Every pod with an owner is a copy of a template judged
					// above; only hand-rolled pods are their own subject.
					if len(p.OwnerReferences) > 0 {
						continue
					}
					add("Pod", p.Namespace, p.Name, p.Labels, p.Spec)
				}
				return nil, nil
			},
		},
	}
	var refused []kindRefusal
	for _, step := range steps {
		r, err := step.run()
		if err != nil {
			return nil, nil, err
		}
		if r != nil {
			if tolerate {
				refused = append(refused, kindRefusal{Refusal: *r, kind: step.kind})
				continue
			}
			return nil, nil, fatalRefusal(r, "listing "+step.what)
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
	return out, refused, nil
}

// kindRefusal is one pod-template kind whose List RBAC refused.
type kindRefusal struct {
	checks.Refusal
	kind string
}

// listNamespacesInScope resolves the namespace population that a
// namespace-subject claim is made about: every namespace under -A,
// exactly the one asked for otherwise. A --namespace that does not
// exist is an error, not an empty clean scan.
func listNamespacesInScope(ctx context.Context, client kubernetes.Interface, ns string) ([]*corev1.Namespace, error) {
	items, r, err := snapshotFor(ctx, client, ns).Namespaces(ctx)
	if err != nil {
		return nil, err
	}
	if r != nil {
		ctxWhat := "listing namespaces"
		if ns != metav1.NamespaceAll {
			ctxWhat = "getting namespace " + ns
		}
		return nil, fatalRefusal(r, ctxWhat)
	}
	out := make([]*corev1.Namespace, len(items))
	for i := range items {
		out[i] = &items[i]
	}
	return out, nil
}
