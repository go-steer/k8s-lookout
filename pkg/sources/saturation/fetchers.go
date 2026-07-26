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

package saturation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
)

// metricsPodFetcher is the real PodUsageFetcher: container usage from
// metrics.k8s.io joined with limits from the pod specs. For the
// sentinel both listings are cluster-wide per cycle — one resident
// process per cluster (§7.2) and the metrics API is itself a cache,
// so this costs two LISTs per --saturation-interval, not per pod.
// namespace (metav1.NamespaceAll for the sentinel) narrows both
// LISTs for one-shot consumers.
type metricsPodFetcher struct {
	metrics   metricsv.Interface
	core      kubernetes.Interface
	namespace string
}

// NewMetricsPodFetcher returns the cluster-wide metrics.k8s.io-backed
// fetcher (the sentinel's wiring).
func NewMetricsPodFetcher(metrics metricsv.Interface, core kubernetes.Interface) PodUsageFetcher {
	return &metricsPodFetcher{metrics: metrics, core: core, namespace: metav1.NamespaceAll}
}

// NewScopedMetricsPodFetcher is NewMetricsPodFetcher restricted to
// one namespace (metav1.NamespaceAll for the whole cluster) — the
// seam `triage top` (§5) shares with this source: the same
// usage-vs-limits join, scoped to what the one-shot command was
// asked about, so the metrics-client code exists exactly once.
func NewScopedMetricsPodFetcher(metrics metricsv.Interface, core kubernetes.Interface, namespace string) PodUsageFetcher {
	return &metricsPodFetcher{metrics: metrics, core: core, namespace: namespace}
}

func (f *metricsPodFetcher) FetchPodUsage(ctx context.Context) ([]ContainerSample, error) {
	pmList, err := f.metrics.MetricsV1beta1().PodMetricses(f.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list pod metrics: %w", err)
	}
	podList, err := f.core.CoreV1().Pods(f.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	type limits struct {
		cpu, mem float64
	}
	type podRef struct {
		uid, node string
		byName    map[string]limits
	}
	pods := make(map[string]podRef, len(podList.Items))
	for i := range podList.Items {
		p := &podList.Items[i]
		byName := make(map[string]limits)
		for _, c := range append(append([]corev1.Container{}, p.Spec.InitContainers...), p.Spec.Containers...) {
			var l limits
			if q, ok := c.Resources.Limits[corev1.ResourceCPU]; ok {
				l.cpu = float64(q.MilliValue())
			}
			if q, ok := c.Resources.Limits[corev1.ResourceMemory]; ok {
				l.mem = float64(q.Value())
			}
			byName[c.Name] = l
		}
		pods[p.Namespace+"/"+p.Name] = podRef{uid: string(p.UID), node: p.Spec.NodeName, byName: byName}
	}

	var out []ContainerSample
	for i := range pmList.Items {
		pm := &pmList.Items[i]
		ref, ok := pods[pm.Namespace+"/"+pm.Name]
		if !ok {
			continue // pod deleted between the two lists
		}
		for _, c := range pm.Containers {
			l := ref.byName[c.Name]
			base := ContainerSample{
				Namespace: pm.Namespace,
				Pod:       pm.Name,
				PodUID:    ref.uid,
				Container: c.Name,
				Node:      ref.node,
			}
			cpu := base
			cpu.Resource, cpu.Used, cpu.Limit = ResourceCPU, float64(c.Usage.Cpu().MilliValue()), l.cpu
			mem := base
			mem.Resource, mem.Used, mem.Limit = ResourceMemory, float64(c.Usage.Memory().Value()), l.mem
			out = append(out, cpu, mem)
		}
	}
	return out, nil
}

// kubeletVolumeFetcher is the real VolumeUsageFetcher: each node's
// kubelet stats summary (/api/v1/nodes/<node>/proxy/stats/summary)
// carries per-pod volume stats with PVC references — the standard
// kubelet endpoint, no cloud dependency (§2). Both hooks are
// overridable so tests exercise parsing and partial-failure
// aggregation without a REST transport.
type kubeletVolumeFetcher struct {
	listNodes func(ctx context.Context) ([]string, error)
	proxyGet  func(ctx context.Context, node string) ([]byte, error)
}

// NewKubeletVolumeFetcher returns the nodes/proxy-backed fetcher.
func NewKubeletVolumeFetcher(client kubernetes.Interface) VolumeUsageFetcher {
	return &kubeletVolumeFetcher{
		listNodes: func(ctx context.Context) ([]string, error) {
			nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, err
			}
			names := make([]string, 0, len(nodes.Items))
			for _, n := range nodes.Items {
				names = append(names, n.Name)
			}
			return names, nil
		},
		proxyGet: func(ctx context.Context, node string) ([]byte, error) {
			return client.CoreV1().RESTClient().Get().
				Resource("nodes").Name(node).SubResource("proxy").
				Suffix("stats/summary").DoRaw(ctx)
		},
	}
}

// statsSummary is the minimal slice of the kubelet Summary API this
// fetcher reads (a private mirror — importing k8s.io/kubelet for two
// fields would drag a whole extra module in).
type statsSummary struct {
	Pods []struct {
		Volume []struct {
			PVCRef *struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"pvcRef"`
			UsedBytes     *uint64 `json:"usedBytes"`
			CapacityBytes *uint64 `json:"capacityBytes"`
		} `json:"volume"`
	} `json:"pods"`
}

// FetchVolumeUsage aggregates PVC usage across nodes. Per-node
// failures are tolerated (nodes cordon, restart, and get replaced);
// only a total blank — every node failing — is an error, which the
// source turns into the one-time "PVC dimension skipped" log.
func (f *kubeletVolumeFetcher) FetchVolumeUsage(ctx context.Context) ([]VolumeSample, error) {
	nodes, err := f.listNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	var out []VolumeSample
	var errs []error
	for _, node := range nodes {
		raw, err := f.proxyGet(ctx, node)
		if err != nil {
			errs = append(errs, fmt.Errorf("node %s: %w", node, err))
			continue
		}
		var sum statsSummary
		if err := json.Unmarshal(raw, &sum); err != nil {
			errs = append(errs, fmt.Errorf("node %s: parse stats summary: %w", node, err))
			continue
		}
		for _, pod := range sum.Pods {
			for _, vol := range pod.Volume {
				if vol.PVCRef == nil || vol.UsedBytes == nil || vol.CapacityBytes == nil || *vol.CapacityBytes == 0 {
					continue
				}
				out = append(out, VolumeSample{
					Namespace:     vol.PVCRef.Namespace,
					ClaimName:     vol.PVCRef.Name,
					UsedBytes:     float64(*vol.UsedBytes),
					CapacityBytes: float64(*vol.CapacityBytes),
				})
			}
		}
	}
	if len(out) == 0 && len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return out, nil
}
