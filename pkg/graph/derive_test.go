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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func assertEdges(t *testing.T, s *Snapshot, want []string) {
	t.Helper()
	got := dumpEdges(s)
	for _, e := range want {
		if !got[e] {
			t.Errorf("missing edge: %s", e)
		}
	}
	if len(got) != len(want) {
		for e := range got {
			found := false
			for _, w := range want {
				if e == w {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("unexpected edge: %s", e)
			}
		}
	}
	checkInvariants(t, s)
}

// TestPodEdgeDerivation covers every Mounts position (env, envFrom,
// volume, projected, PVC), RunsOn, Contains (init + regular
// containers), and the ownerReferences Owns edge — against exact
// expected edge sets.
func TestPodEdgeDerivation(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "ns",
			Name:            "web-1",
			Labels:          map[string]string{"app": "web"},
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-rs"}},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			InitContainers: []corev1.Container{{
				Name: "init",
				EnvFrom: []corev1.EnvFromSource{{
					ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "cm-envfrom"},
					},
				}},
			}},
			Containers: []corev1.Container{{
				Name: "app",
				Env: []corev1.EnvVar{
					{
						Name: "TOKEN",
						ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "sec-env"},
							Key:                  "token",
						}},
					},
					{
						Name: "MODE",
						ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "cm-env"},
							Key:                  "mode",
						}},
					},
				},
				EnvFrom: []corev1.EnvFromSource{{
					SecretRef: &corev1.SecretEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "sec-envfrom"},
					},
				}},
			}},
			Volumes: []corev1.Volume{
				{
					Name: "cfg",
					VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "cm-vol"},
					}},
				},
				{
					Name: "creds",
					VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
						SecretName: "sec-vol",
					}},
				},
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "pvc-data",
					}},
				},
				{
					Name: "proj",
					VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
						Sources: []corev1.VolumeProjection{
							{ConfigMap: &corev1.ConfigMapProjection{
								LocalObjectReference: corev1.LocalObjectReference{Name: "cm-proj"},
							}},
							{Secret: &corev1.SecretProjection{
								LocalObjectReference: corev1.LocalObjectReference{Name: "sec-proj"},
							}},
						},
					}},
				},
				// Duplicate reference: same ConfigMap via a second
				// volume must fold into one edge.
				{
					Name: "cfg-again",
					VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "cm-vol"},
					}},
				},
			},
		},
	}
	_, s := buildGraph(t, []any{pod})
	assertEdges(t, s, []string{
		"!ReplicaSet/ns/web-rs -Owns-> Pod/ns/web-1",
		"Pod/ns/web-1 -RunsOn-> !Node//node-1",
		"Pod/ns/web-1 -Contains-> Container/ns/web-1/init",
		"Pod/ns/web-1 -Contains-> Container/ns/web-1/app",
		"Pod/ns/web-1 -Mounts-> !ConfigMap/ns/cm-envfrom",
		"Pod/ns/web-1 -Mounts-> !ConfigMap/ns/cm-env",
		"Pod/ns/web-1 -Mounts-> !ConfigMap/ns/cm-vol",
		"Pod/ns/web-1 -Mounts-> !ConfigMap/ns/cm-proj",
		"Pod/ns/web-1 -Mounts-> !Secret/ns/sec-env",
		"Pod/ns/web-1 -Mounts-> !Secret/ns/sec-envfrom",
		"Pod/ns/web-1 -Mounts-> !Secret/ns/sec-vol",
		"Pod/ns/web-1 -Mounts-> !Secret/ns/sec-proj",
		"Pod/ns/web-1 -Mounts-> !PersistentVolumeClaim/ns/pvc-data",
	})
}

func TestServiceSelectorLifecycle(t *testing.T) {
	mkpod := func(name string, app string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns", Name: name, Labels: map[string]string{"app": app},
		}}
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "web"},
		Spec:       corev1.ServiceSpec{Selector: map[string]string{"app": "web"}},
	}
	otherNS := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "other", Name: "p1", Labels: map[string]string{"app": "web"},
	}}

	// Order matters for the incremental paths: service first, then
	// pods (service must pick up later-arriving pods).
	g, s := buildGraph(t, []any{svc, mkpod("p1", "web"), otherNS})
	w := g.Writer()
	apply := func(op Op, obj any) *Snapshot {
		t.Helper()
		if err := w.Apply(Delta{Op: op, Object: obj}); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		s, err := g.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	assertEdges(t, s, []string{"Service/ns/web -Selects-> Pod/ns/p1"}) // not the other-namespace pod

	// A pod added later in the same namespace gets selected.
	s = apply(OpAdd, mkpod("p2", "web"))
	assertEdges(t, s, []string{
		"Service/ns/web -Selects-> Pod/ns/p1",
		"Service/ns/web -Selects-> Pod/ns/p2",
	})

	// Label change deselects.
	s = apply(OpUpdate, mkpod("p2", "api"))
	assertEdges(t, s, []string{"Service/ns/web -Selects-> Pod/ns/p1"})

	// Label change back reselects.
	s = apply(OpUpdate, mkpod("p2", "web"))
	if !dumpEdges(s)["Service/ns/web -Selects-> Pod/ns/p2"] {
		t.Fatal("relabeled pod not reselected")
	}

	// Service selector update rewrites the edge set.
	svc2 := svc.DeepCopy()
	svc2.Spec.Selector = map[string]string{"app": "api"}
	s = apply(OpUpdate, svc2)
	if edges := dumpEdges(s); edges["Service/ns/web -Selects-> Pod/ns/p1"] || edges["Service/ns/web -Selects-> Pod/ns/p2"] {
		t.Fatal("stale Selects edges after selector change")
	}

	// Service delete removes it and its edges entirely.
	s = apply(OpDelete, svc2)
	if _, ok := s.Lookup(KindService, "ns", "web"); ok {
		t.Fatal("deleted service still present")
	}
	// Pod churn after service deletion must not resurrect edges
	// (selector must be unregistered).
	s = apply(OpUpdate, mkpod("p1", "api"))
	for e := range dumpEdges(s) {
		if e != "" && (len(e) >= 7 && e[:7] == "Service") {
			t.Fatalf("edge from deleted service: %s", e)
		}
	}
	checkInvariants(t, s)
}

func TestNetworkPolicyGoverns(t *testing.T) {
	// Empty podSelector = every pod in the namespace (k8s semantics).
	np := &netv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "deny-all"}}
	scoped := &netv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "web-only"},
		Spec: netv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
	}
	web := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "web-1", Labels: map[string]string{"app": "web"}}}
	api := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "api-1", Labels: map[string]string{"app": "api"}}}
	_, s := buildGraph(t, []any{np, scoped, web, api})
	assertEdges(t, s, []string{
		"NetworkPolicy/ns/deny-all -Governs-> Pod/ns/web-1",
		"NetworkPolicy/ns/deny-all -Governs-> Pod/ns/api-1",
		"NetworkPolicy/ns/web-only -Governs-> Pod/ns/web-1",
	})
}

func TestTrafficChainDerivation(t *testing.T) {
	// Ingress → Service → EndpointSlice → Pod, plus the Owns edge
	// the slice's ownerReference contributes.
	ing := &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "edge"},
		Spec: netv1.IngressSpec{
			DefaultBackend: &netv1.IngressBackend{Service: &netv1.IngressServiceBackend{Name: "web"}},
			Rules: []netv1.IngressRule{{
				IngressRuleValue: netv1.IngressRuleValue{HTTP: &netv1.HTTPIngressRuleValue{
					Paths: []netv1.HTTPIngressPath{{Backend: netv1.IngressBackend{
						Service: &netv1.IngressServiceBackend{Name: "web"},
					}}},
				}},
			}},
		},
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "web"},
		Spec:       corev1.ServiceSpec{Selector: map[string]string{"app": "web"}},
	}
	eps := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "ns",
			Name:            "web-x7k2p",
			Labels:          map[string]string{discoveryv1.LabelServiceName: "web"},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Service", Name: "web"}},
		},
		Endpoints: []discoveryv1.Endpoint{
			{TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: "web-1"}},
			{TargetRef: &corev1.ObjectReference{Kind: "Node", Name: "nope"}}, // non-pod targetRef ignored
			{TargetRef: nil},
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "web-1", Labels: map[string]string{"app": "web"}}}
	_, s := buildGraph(t, []any{ing, svc, eps, pod})
	assertEdges(t, s, []string{
		"Ingress/ns/edge -RoutesTo-> Service/ns/web",
		"Service/ns/web -RoutesTo-> EndpointSlice/ns/web-x7k2p",
		"Service/ns/web -Owns-> EndpointSlice/ns/web-x7k2p",
		"Service/ns/web -Selects-> Pod/ns/web-1",
		"EndpointSlice/ns/web-x7k2p -RoutesTo-> Pod/ns/web-1",
	})
}

func TestOwnerChainAndNodeZoneDerivation(t *testing.T) {
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "web"}}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: "ns", Name: "web-7f9c",
		OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web"}},
	}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns", Name: "web-7f9c-1",
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "ReplicaSet", Name: "web-7f9c"},
				{Kind: "SomeCRD", Name: "custom"}, // unknown owner kind skipped in v1
			},
		},
		Spec: corev1.PodSpec{NodeName: "node-1"},
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "node-1",
		Labels: map[string]string{corev1.LabelFailureDomainBetaZone: "zone-a"}, // legacy label honored
	}}
	_, s := buildGraph(t, []any{dep, rs, pod, node})
	assertEdges(t, s, []string{
		"Deployment/ns/web -Owns-> ReplicaSet/ns/web-7f9c",
		"ReplicaSet/ns/web-7f9c -Owns-> Pod/ns/web-7f9c-1",
		"Pod/ns/web-7f9c-1 -RunsOn-> Node//node-1",
		"Node//node-1 -RunsOn-> Zone//zone-a",
	})
}
