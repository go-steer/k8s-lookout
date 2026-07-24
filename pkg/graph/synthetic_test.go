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

// Synthetic-cluster generator (DESIGN.md §13): a deterministic,
// seeded cluster of roughly nPods pods with the full pod-nexus
// around them — nodes/zones, owner chains (Deployment→ReplicaSet,
// StatefulSet, DaemonSet, CronJob→Job), services + endpoint slices +
// ingresses, per-workload and shared config/secret mounts, PVCs, and
// a namespace-wide NetworkPolicy. Used by the property tests and the
// §15 Q5 gate benchmarks.

import (
	"fmt"
	"math/rand/v2"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// synthCluster generates the object set. Same seed + nPods ⇒
// byte-identical objects in identical order.
func synthCluster(seed uint64, nPods int) []any {
	rng := rand.New(rand.NewPCG(seed, seed+1))
	var objs []any

	zones := []string{"zone-a", "zone-b", "zone-c"}
	nNodes := max(3, nPods/25)
	nodeNames := make([]string, nNodes)
	for i := range nNodes {
		nodeNames[i] = fmt.Sprintf("node-%04d", i)
		objs = append(objs, &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   nodeNames[i],
				Labels: map[string]string{corev1.LabelTopologyZone: zones[i%len(zones)]},
			},
		})
	}

	nNS := max(2, nPods/400)
	nsNames := make([]string, nNS)
	for i := range nNS {
		nsNames[i] = fmt.Sprintf("ns-%03d", i)
		objs = append(objs, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsNames[i]}})
		// Shared per-namespace ConfigMap (lateral blast radius) and
		// a namespace-wide NetworkPolicy (empty selector = all pods).
		objs = append(objs, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Namespace: nsNames[i], Name: "cm-shared",
		}})
		objs = append(objs, &netv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{
			Namespace: nsNames[i], Name: "default-deny",
		}})
	}

	randNode := func() string { return nodeNames[rng.IntN(nNodes)] }

	mkPod := func(ns, name, app, node string, owner metav1.OwnerReference, secret string, cms []string, pvc string) *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:       ns,
				Name:            name,
				Labels:          map[string]string{"app": app},
				OwnerReferences: []metav1.OwnerReference{owner},
			},
			Spec: corev1.PodSpec{NodeName: node},
		}
		app0 := corev1.Container{Name: "app"}
		if secret != "" {
			app0.Env = []corev1.EnvVar{{
				Name: "CREDS",
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secret},
					Key:                  "token",
				}},
			}}
		}
		pod.Spec.Containers = []corev1.Container{app0}
		if rng.IntN(3) == 0 {
			pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "sidecar"})
		}
		for i, cm := range cms {
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
				Name: fmt.Sprintf("vol-%d", i),
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cm},
				}},
			})
		}
		if pvc != "" {
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
				Name: "data",
				VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc,
				}},
			})
		}
		return pod
	}

	mkService := func(ns, app string) {
		objs = append(objs, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: app},
			Spec:       corev1.ServiceSpec{Selector: map[string]string{"app": app}},
		})
	}
	mkSlice := func(ns, app string, podNames []string) {
		eps := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      app + "-abc12",
				Labels:    map[string]string{discoveryv1.LabelServiceName: app},
				OwnerReferences: []metav1.OwnerReference{
					{Kind: "Service", Name: app},
				},
			},
		}
		for _, p := range podNames {
			eps.Endpoints = append(eps.Endpoints, discoveryv1.Endpoint{
				TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: ns, Name: p},
			})
		}
		objs = append(objs, eps)
	}

	budget := nPods
	nDepPods := nPods * 70 / 100
	nSTSPods := nPods * 10 / 100
	nDSPods := nPods * 10 / 100

	// Deployments: 5 pods each, Deployment→ReplicaSet→Pod chain,
	// per-deployment ConfigMap + Secret, a Service + EndpointSlice,
	// an Ingress per 5 services.
	var svcBacklog []struct{ ns, app string }
	for i := 0; nDepPods > 0 && budget > 0; i++ {
		ns := nsNames[i%nNS]
		app := fmt.Sprintf("dep-%04d", i)
		rs := app + "-7f9c"
		objs = append(objs, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: app}})
		objs = append(objs, &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: rs,
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: app}},
		}})
		objs = append(objs, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "cm-" + app}})
		objs = append(objs, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "sec-" + app},
			Data:       map[string][]byte{"token": []byte(secretCanary)},
		})
		var podNames []string
		for j := 0; j < 5 && nDepPods > 0 && budget > 0; j++ {
			name := fmt.Sprintf("%s-%05d", rs, j)
			podNames = append(podNames, name)
			objs = append(objs, mkPod(ns, name, app, randNode(),
				metav1.OwnerReference{Kind: "ReplicaSet", Name: rs},
				"sec-"+app, []string{"cm-" + app, "cm-shared"}, ""))
			nDepPods--
			budget--
		}
		mkService(ns, app)
		mkSlice(ns, app, podNames)
		svcBacklog = append(svcBacklog, struct{ ns, app string }{ns, app})
		if len(svcBacklog) == 5 {
			ing := &netv1.Ingress{ObjectMeta: metav1.ObjectMeta{
				Namespace: svcBacklog[0].ns, Name: "ing-" + svcBacklog[0].app,
			}}
			for _, s := range svcBacklog {
				if s.ns != ing.Namespace {
					continue // ingress backends are same-namespace
				}
				ing.Spec.Rules = append(ing.Spec.Rules, netv1.IngressRule{
					IngressRuleValue: netv1.IngressRuleValue{HTTP: &netv1.HTTPIngressRuleValue{
						Paths: []netv1.HTTPIngressPath{{Backend: netv1.IngressBackend{
							Service: &netv1.IngressServiceBackend{Name: s.app},
						}}},
					}},
				})
			}
			objs = append(objs, ing)
			svcBacklog = svcBacklog[:0]
		}
	}

	// StatefulSets: 3 pods each, one PVC per pod, a headless-style
	// Service + slice.
	for i := 0; nSTSPods > 0 && budget > 0; i++ {
		ns := nsNames[i%nNS]
		app := fmt.Sprintf("sts-%04d", i)
		objs = append(objs, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: app}})
		var podNames []string
		for j := 0; j < 3 && nSTSPods > 0 && budget > 0; j++ {
			name := fmt.Sprintf("%s-%d", app, j)
			pvc := "data-" + name
			objs = append(objs, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: pvc}})
			podNames = append(podNames, name)
			objs = append(objs, mkPod(ns, name, app, randNode(),
				metav1.OwnerReference{Kind: "StatefulSet", Name: app},
				"", []string{"cm-shared"}, pvc))
			nSTSPods--
			budget--
		}
		mkService(ns, app)
		mkSlice(ns, app, podNames)
	}

	// DaemonSets: one per namespace, pods spread over distinct nodes.
	for i := 0; nDSPods > 0 && budget > 0; i++ {
		ns := nsNames[i%nNS]
		app := fmt.Sprintf("ds-%04d", i)
		objs = append(objs, &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: app}})
		for j := 0; j < nNodes && nDSPods > 0 && budget > 0; j++ {
			name := fmt.Sprintf("%s-%05d", app, j)
			objs = append(objs, mkPod(ns, name, app, nodeNames[j],
				metav1.OwnerReference{Kind: "DaemonSet", Name: app},
				"", nil, ""))
			nDSPods--
			budget--
		}
	}

	// Jobs for the remainder; a third of them owned by a CronJob.
	for i := 0; budget > 0; i++ {
		ns := nsNames[i%nNS]
		app := fmt.Sprintf("job-%04d", i)
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: app}}
		if i%3 == 0 {
			cj := "cron-" + app
			objs = append(objs, &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: cj}})
			job.OwnerReferences = []metav1.OwnerReference{{Kind: "CronJob", Name: cj}}
		}
		objs = append(objs, job)
		objs = append(objs, mkPod(ns, app+"-1", app, randNode(),
			metav1.OwnerReference{Kind: "Job", Name: app},
			"", nil, ""))
		budget--
	}

	return objs
}

// secretCanary is planted in every synthetic Secret's data; a test
// asserts it can never be found anywhere in graph storage (§6.5:
// names only, never values).
const secretCanary = "SUPER-SECRET-VALUE-do-not-store"
