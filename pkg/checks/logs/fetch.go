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

package logs

// Log fetching: scope resolution (workload → pods via the workload's
// own label selector, namespace, or a single pod) and the log-stream
// seam. The stream itself sits behind PodLogGetter because the fake
// clientset's GetLogs returns a canned constant — tests inject
// fixture streams here while still using fake.Clientset for pod and
// workload discovery (§13).

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// PodLogGetter streams one container's logs. The production
// implementation wraps the clientset's GetLogs subresource.
type PodLogGetter interface {
	Stream(ctx context.Context, namespace, pod string, opts *corev1.PodLogOptions) (io.ReadCloser, error)
}

// clientLogGetter is the production PodLogGetter.
type clientLogGetter struct{ cs kubernetes.Interface }

func (g clientLogGetter) Stream(ctx context.Context, namespace, pod string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	return g.cs.CoreV1().Pods(namespace).GetLogs(pod, opts).Stream(ctx)
}

// target is one (pod, container) stream to fetch.
type target struct {
	namespace string
	pod       string
	container string
}

// resolveTargets expands the invocation scope into concrete
// (pod, container) streams:
//
//   - --pod (+ --namespace): that single pod;
//   - --workload=<Kind>/<ns>/<name>: pods matched by the workload's
//     own label selector (Deployment, StatefulSet, DaemonSet,
//     ReplicaSet, Job);
//   - --namespace / -A: every pod in scope.
//
// Containers default to all of the pod's init + regular + ephemeral
// containers; --container restricts to pods that have that container.
func resolveTargets(ctx context.Context, cs kubernetes.Interface, scope emit.Scope, podName, container string) ([]target, error) {
	pods, err := resolvePods(ctx, cs, scope, podName)
	if err != nil {
		return nil, err
	}
	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Namespace != pods[j].Namespace {
			return pods[i].Namespace < pods[j].Namespace
		}
		return pods[i].Name < pods[j].Name
	})
	var out []target
	for i := range pods {
		for _, c := range podContainers(&pods[i], container) {
			out = append(out, target{namespace: pods[i].Namespace, pod: pods[i].Name, container: c})
		}
	}
	return out, nil
}

func resolvePods(ctx context.Context, cs kubernetes.Interface, scope emit.Scope, podName string) ([]corev1.Pod, error) {
	switch {
	case podName != "":
		if scope.Namespace == "" {
			return nil, emit.UsageErrorf("--pod requires --namespace")
		}
		p, err := cs.CoreV1().Pods(scope.Namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("pod %s/%s: %w", scope.Namespace, podName, err)
		}
		return []corev1.Pod{*p}, nil

	case !scope.Workload.IsZero():
		return workloadPods(ctx, cs, scope.Workload)

	case scope.Namespace != "" || scope.AllNamespaces:
		list, err := cs.CoreV1().Pods(scope.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("listing pods: %w", err)
		}
		return list.Items, nil
	}
	return nil, emit.UsageErrorf("scope required: one of --workload, --pod (with --namespace), --namespace, or -A")
}

// workloadPods resolves a workload's pods through the label selector
// declared on the workload object itself — the same selector its
// controller uses to claim pods, so the owner chain and this listing
// agree.
func workloadPods(ctx context.Context, cs kubernetes.Interface, w emit.WorkloadRef) ([]corev1.Pod, error) {
	var (
		sel *metav1.LabelSelector
		err error
	)
	ns, name := w.Namespace, w.Name
	switch strings.ToLower(w.Kind) {
	case "deployment", "deploy":
		if d, e := cs.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{}); e != nil {
			err = e
		} else {
			sel = d.Spec.Selector
		}
	case "statefulset", "sts":
		if s, e := cs.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{}); e != nil {
			err = e
		} else {
			sel = s.Spec.Selector
		}
	case "daemonset", "ds":
		if d, e := cs.AppsV1().DaemonSets(ns).Get(ctx, name, metav1.GetOptions{}); e != nil {
			err = e
		} else {
			sel = d.Spec.Selector
		}
	case "replicaset", "rs":
		if r, e := cs.AppsV1().ReplicaSets(ns).Get(ctx, name, metav1.GetOptions{}); e != nil {
			err = e
		} else {
			sel = r.Spec.Selector
		}
	case "job":
		if j, e := cs.BatchV1().Jobs(ns).Get(ctx, name, metav1.GetOptions{}); e != nil {
			err = e
		} else {
			sel = j.Spec.Selector
		}
	default:
		return nil, emit.UsageErrorf("unsupported workload kind %q (want Deployment, StatefulSet, DaemonSet, ReplicaSet, or Job)", w.Kind)
	}
	if err != nil {
		return nil, fmt.Errorf("workload %s: %w", w, err)
	}
	selector, err := metav1.LabelSelectorAsSelector(sel)
	if err != nil {
		return nil, fmt.Errorf("workload %s: selector: %w", w, err)
	}
	list, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, fmt.Errorf("workload %s: listing pods: %w", w, err)
	}
	return list.Items, nil
}

// podContainers lists the pod's container names in spec order (init,
// regular, ephemeral). With only set, it returns just that container
// — or nothing if the pod does not have it.
func podContainers(pod *corev1.Pod, only string) []string {
	var names []string
	for _, c := range pod.Spec.InitContainers {
		names = append(names, c.Name)
	}
	for _, c := range pod.Spec.Containers {
		names = append(names, c.Name)
	}
	for _, c := range pod.Spec.EphemeralContainers {
		names = append(names, c.Name)
	}
	if only == "" {
		return names
	}
	for _, n := range names {
		if n == only {
			return []string{only}
		}
	}
	return nil
}
