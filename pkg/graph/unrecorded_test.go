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

package graph

import (
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// fullCluster is one object of every ingested kind, wired so each
// declares every edge derive.go can derive from it: the richest input
// a graph of any watched set could be fed.
func fullCluster() map[NodeKind]any {
	meta := func(name string, owner ...metav1.OwnerReference) metav1.ObjectMeta {
		return metav1.ObjectMeta{Namespace: "prod", Name: name, Labels: map[string]string{"app": "web"}, OwnerReferences: owner}
	}
	own := func(kind, name string) metav1.OwnerReference { return metav1.OwnerReference{Kind: kind, Name: name} }
	return map[NodeKind]any{
		KindNode: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1",
			Labels: map[string]string{corev1.LabelTopologyZone: "us-central1-a"}}},
		KindPod: &corev1.Pod{
			ObjectMeta: meta("web-1", own("ReplicaSet", "web-7b9d"), own("StatefulSet", "db"),
				own("DaemonSet", "agent"), own("Job", "nightly-1")),
			Spec: corev1.PodSpec{
				NodeName: "n1",
				Containers: []corev1.Container{{Name: "app", EnvFrom: []corev1.EnvFromSource{{
					SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "creds"}}}}}},
				Volumes: []corev1.Volume{
					{Name: "cfg", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "app-config"}}}},
					{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data"}}},
				},
			},
		},
		KindService: &corev1.Service{ObjectMeta: meta("web"), Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "web"}}},
		KindEndpointSlice: &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web-abc12",
				Labels: map[string]string{discoveryv1.LabelServiceName: "web"}},
			Endpoints: []discoveryv1.Endpoint{{TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: "web-1"}}},
		},
		KindIngress: &netv1.Ingress{ObjectMeta: meta("web"), Spec: netv1.IngressSpec{
			DefaultBackend: &netv1.IngressBackend{Service: &netv1.IngressServiceBackend{Name: "web"}}}},
		KindNetworkPolicy: &netv1.NetworkPolicy{ObjectMeta: meta("deny"), Spec: netv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}}},
		KindConfigMap:             &corev1.ConfigMap{ObjectMeta: meta("app-config")},
		KindSecret:                &corev1.Secret{ObjectMeta: meta("creds")},
		KindPersistentVolumeClaim: &corev1.PersistentVolumeClaim{ObjectMeta: meta("data")},
		KindDeployment:            &appsv1.Deployment{ObjectMeta: meta("web")},
		KindReplicaSet:            &appsv1.ReplicaSet{ObjectMeta: meta("web-7b9d", own("Deployment", "web"))},
		KindStatefulSet:           &appsv1.StatefulSet{ObjectMeta: meta("db")},
		KindDaemonSet:             &appsv1.DaemonSet{ObjectMeta: meta("agent")},
		KindJob:                   &batchv1.Job{ObjectMeta: meta("nightly-1", own("CronJob", "nightly"))},
		KindCronJob:               &batchv1.CronJob{ObjectMeta: meta("nightly")},
	}
}

// TestSnapshot_Unrecorded_IsExact feeds each watched set the richest
// input its informers could deliver — every object of a watched kind,
// nothing else — and holds Unrecorded to the result both ways: no kind
// it lists may appear (the claim "cannot contain" is sound), and every
// kind it omits must (the claim is not padded, so a reader is never
// told to distrust a kind history does hold).
func TestSnapshot_Unrecorded_IsExact(t *testing.T) {
	t.Parallel()
	objs := fullCluster()
	sets := map[string][]NodeKind{
		"sentinel graph feed": {KindPod, KindNode, KindReplicaSet},
		"routing layer only":  {KindService, KindEndpointSlice, KindIngress},
		"jobs without pods":   {KindJob},
		"nodes only":          {KindNode},
	}
	for name, watched := range sets {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g := New(Options{SwapInterval: -1, WatchedKinds: watched})
			var fed []any
			for _, k := range watched {
				fed = append(fed, objs[k])
			}
			if err := g.Writer().FromObjects(slices.Values(fed)); err != nil {
				t.Fatal(err)
			}
			s, err := g.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			unrecorded := s.Unrecorded()
			for kind := range declaredBy {
				n := s.CountKind(kind)
				switch listed := slices.Contains(unrecorded, kind); {
				case listed && n > 0:
					t.Errorf("%v listed unrecorded, but the snapshot holds %d", kind, n)
				case !listed && n == 0:
					t.Errorf("%v not listed unrecorded, but the richest input put none in the snapshot", kind)
				}
			}
		})
	}
}

// TestSnapshot_Unrecorded_SentinelFeed pins the answer #396 is about,
// through the history encoding the --at path restores from: the
// sentinel's snapshots cannot hold the routing layer, and say so.
func TestSnapshot_Unrecorded_SentinelFeed(t *testing.T) {
	t.Parallel()
	g := New(Options{SwapInterval: -1, WatchedKinds: []NodeKind{KindPod, KindNode, KindReplicaSet}})
	if err := g.Writer().FromObjects(slices.Values([]any{fullCluster()[KindPod]})); err != nil {
		t.Fatal(err)
	}
	s, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	data, err := s.Encode()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := Restore(data)
	if err != nil {
		t.Fatal(err)
	}
	want := []NodeKind{KindCronJob, KindEndpointSlice, KindIngress, KindNetworkPolicy, KindService}
	if got := restored.Unrecorded(); !slices.Equal(got, want) {
		t.Errorf("Unrecorded() = %v, want %v", got, want)
	}
}

// TestSnapshot_Unrecorded_WatchEverything: a one-shot full-List graph
// can contain every kind, so it names none.
func TestSnapshot_Unrecorded_WatchEverything(t *testing.T) {
	t.Parallel()
	g := New(Options{SwapInterval: -1})
	if err := g.Writer().FromObjects(slices.Values([]any{fullCluster()[KindPod]})); err != nil {
		t.Fatal(err)
	}
	s, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Unrecorded(); len(got) != 0 {
		t.Errorf("Unrecorded() = %v, want none", got)
	}
}

// TestDeclaredBy_CoversEveryKind: a kind added to types.go without a
// declaredBy row would silently never be reported unrecorded.
func TestDeclaredBy_CoversEveryKind(t *testing.T) {
	t.Parallel()
	for k := KindUnknown + 1; k < numNodeKinds; k++ {
		if _, ok := declaredBy[k]; !ok && k != KindNamespace {
			t.Errorf("declaredBy has no row for %v", k)
		}
	}
}
