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
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"
)

// newMetricsFake seeds a metrics fake clientset. The objects go in
// through Tracker().Create with the EXPLICIT pods GVR: the fake's
// generic Add guesses the resource from the kind ("PodMetrics" →
// "podmetricses"), which is not the "pods" resource the client lists
// — the long-standing metrics fake quirk.
func newMetricsFake(t *testing.T, objs ...*metricsv1beta1.PodMetrics) *metricsfake.Clientset {
	t.Helper()
	cs := metricsfake.NewSimpleClientset()
	gvr := schema.GroupVersionResource{Group: "metrics.k8s.io", Version: "v1beta1", Resource: "pods"}
	for _, o := range objs {
		if err := cs.Tracker().Create(gvr, o, o.Namespace); err != nil {
			t.Fatalf("seed pod metrics: %v", err)
		}
	}
	return cs
}

func TestMetricsPodFetcher_JoinsUsageWithLimits(t *testing.T) {
	t.Parallel()
	metrics := newMetricsFake(t,
		&metricsv1beta1.PodMetrics{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web-1"},
			Containers: []metricsv1beta1.ContainerMetrics{{
				Name: "app",
				Usage: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("250m"),
					corev1.ResourceMemory: resource.MustParse("512Mi"),
				},
			}},
		},
		&metricsv1beta1.PodMetrics{
			// No matching pod (deleted between lists) → skipped.
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "gone"},
			Containers: []metricsv1beta1.ContainerMetrics{{Name: "app", Usage: corev1.ResourceList{}}},
		},
	)
	core := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web-1", UID: "u1"},
		Spec: corev1.PodSpec{
			NodeName: "n1",
			Containers: []corev1.Container{{
				Name: "app",
				Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				}},
			}},
		},
	})
	got, err := NewMetricsPodFetcher(metrics, core).FetchPodUsage(context.Background())
	if err != nil {
		t.Fatalf("FetchPodUsage: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("samples = %d (%+v), want cpu+memory for the one matched container", len(got), got)
	}
	byResource := map[string]ContainerSample{}
	for _, s := range got {
		byResource[s.Resource] = s
	}
	cpu := byResource[ResourceCPU]
	if cpu.Used != 250 || cpu.Limit != 500 {
		t.Errorf("cpu = used %v limit %v, want 250/500 millicores", cpu.Used, cpu.Limit)
	}
	mem := byResource[ResourceMemory]
	if mem.Used != 512*mib || mem.Limit != 1024*mib {
		t.Errorf("memory = used %v limit %v, want %v/%v bytes", mem.Used, mem.Limit, 512*mib, 1024*mib)
	}
	if cpu.PodUID != "u1" || cpu.Node != "n1" || cpu.Container != "app" || cpu.Namespace != "prod" || cpu.Pod != "web-1" {
		t.Errorf("identity wrong: %+v", cpu)
	}
}

func TestMetricsPodFetcher_NoLimitIsZero(t *testing.T) {
	t.Parallel()
	metrics := newMetricsFake(t, &metricsv1beta1.PodMetrics{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web-1"},
		Containers: []metricsv1beta1.ContainerMetrics{{
			Name:  "app",
			Usage: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("100Mi")},
		}},
	})
	core := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web-1", UID: "u1"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
	})
	got, err := NewMetricsPodFetcher(metrics, core).FetchPodUsage(context.Background())
	if err != nil {
		t.Fatalf("FetchPodUsage: %v", err)
	}
	for _, s := range got {
		if s.Limit != 0 {
			t.Errorf("%s limit = %v, want 0 (no limit configured → no forecast)", s.Resource, s.Limit)
		}
	}
}

const summaryFixture = `{
  "node": {"nodeName": "n1"},
  "pods": [
    {
      "podRef": {"name": "db-0", "namespace": "prod"},
      "volume": [
        {"name": "data", "usedBytes": 100, "capacityBytes": 1000,
         "pvcRef": {"name": "data-db-0", "namespace": "prod"}},
        {"name": "scratch-emptydir", "usedBytes": 5, "capacityBytes": 10},
        {"name": "zero-cap", "usedBytes": 0, "capacityBytes": 0,
         "pvcRef": {"name": "weird", "namespace": "prod"}}
      ]
    }
  ]
}`

func TestKubeletVolumeFetcher_ParsesPVCRefsAndToleratesPartialFailure(t *testing.T) {
	t.Parallel()
	f := &kubeletVolumeFetcher{
		listNodes: func(context.Context) ([]string, error) { return []string{"n1", "n2"}, nil },
		proxyGet: func(_ context.Context, node string) ([]byte, error) {
			if node == "n2" {
				return nil, errors.New("node n2 unreachable")
			}
			return []byte(summaryFixture), nil
		},
	}
	got, err := f.FetchVolumeUsage(context.Background())
	if err != nil {
		t.Fatalf("partial node failure must not fail the fetch: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("samples = %+v, want exactly the one PVC-backed volume (emptyDir and zero-capacity skipped)", got)
	}
	s := got[0]
	if s.Namespace != "prod" || s.ClaimName != "data-db-0" || s.UsedBytes != 100 || s.CapacityBytes != 1000 {
		t.Errorf("sample = %+v, want prod/data-db-0 100/1000", s)
	}
}

func TestKubeletVolumeFetcher_AllNodesFailing_IsAnError(t *testing.T) {
	t.Parallel()
	f := &kubeletVolumeFetcher{
		listNodes: func(context.Context) ([]string, error) { return []string{"n1", "n2"}, nil },
		proxyGet: func(_ context.Context, node string) ([]byte, error) {
			return nil, errors.New(node + ": proxy forbidden")
		},
	}
	if _, err := f.FetchVolumeUsage(context.Background()); err == nil {
		t.Fatal("every node failing must surface as an error (feeds the one-time loud log)")
	}
}
