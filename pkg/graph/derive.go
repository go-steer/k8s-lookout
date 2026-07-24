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

// Edge derivation from typed Kubernetes objects (§6.1 pod-nexus
// model). Every edge is "declared" by exactly one object — the one
// whose spec/metadata implies it — so an update or delete of that
// object reconciles precisely its own edges:
//
//	Pod        → RunsOn(Node), Contains(Container), Mounts(ConfigMap/
//	             Secret/PVC via env, envFrom, volumes, projected)
//	Service    → Selects(Pod) via label selector
//	EndpointSlice → RoutesTo(Pod) via targetRef; and declares
//	             Service→RoutesTo(EndpointSlice) via its
//	             kubernetes.io/service-name label
//	Ingress    → RoutesTo(Service) via rules + default backend
//	NetworkPolicy → Governs(Pod) via podSelector
//	Node       → RunsOn(Zone) via topology labels
//	any object → Owns edges (owner→object) from its ownerReferences
//
// Selector-derived edges (Selects, Governs) are additionally
// re-evaluated on pod churn via the writer's namespace-scoped
// selector index.

import (
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// ownerNodeKinds maps ownerReference.Kind strings to graph kinds.
// Owner kinds outside this set (arbitrary CRDs — Argo Rollouts, etc.)
// are skipped in v1; widening this is part of the §15 Q6 coverage
// question.
var ownerNodeKinds = map[string]NodeKind{
	"Deployment":  KindDeployment,
	"ReplicaSet":  KindReplicaSet,
	"StatefulSet": KindStatefulSet,
	"DaemonSet":   KindDaemonSet,
	"Job":         KindJob,
	"CronJob":     KindCronJob,
	"Service":     KindService,
	"Node":        KindNode, // static/mirror pods are owned by their Node
	"Pod":         KindPod,
}

// ownerEdges derives owner→self Owns edges from ownerReferences.
// Owners live in the same namespace as the owned object, except
// cluster-scoped Node owners.
func (w *Writer) ownerEdges(b *batch, refs []metav1.OwnerReference, namespace string, self NodeID) []halfEdge {
	var decl []halfEdge
	for i := range refs {
		kind, ok := ownerNodeKinds[refs[i].Kind]
		if !ok {
			continue
		}
		ownerNS := namespace
		if kind == KindNode {
			ownerNS = ""
		}
		owner := w.node(b, kind, ownerNS, refs[i].Name)
		decl = append(decl, halfEdge{From: owner, To: self, Kind: EdgeOwns})
	}
	return decl
}

// applyPlain handles kinds that carry no spec-derived edges of their
// own: workload owners, ConfigMap/Secret/PVC (edge *targets*, whose
// Mounts edges are declared by pods), and Namespace.
func (w *Writer) applyPlain(b *batch, op Op, kind NodeKind, namespace, name string, refs []metav1.OwnerReference) {
	id := w.node(b, kind, namespace, name)
	if op == OpDelete {
		w.setDeclared(b, id, nil)
		w.markDeleted(b, id)
		return
	}
	w.observe(b, id, kind)
	w.setDeclared(b, id, w.ownerEdges(b, refs, namespace, id))
}

func (w *Writer) applyPod(b *batch, op Op, pod *corev1.Pod) {
	ns := pod.Namespace
	id := w.node(b, KindPod, ns, pod.Name)
	if op == OpDelete {
		// Drop the pod's own edges (containers GC with their
		// Contains edge), detach every selector source, and forget
		// its labels. Edges *targeting* the pod that are declared by
		// other objects (e.g. a stale EndpointSlice) survive and
		// keep the node alive as Observed=false.
		w.setDeclared(b, id, nil)
		for src, ent := range w.selectors[ns] {
			w.setSelectorEdge(b, src, id, ent.kind, false)
		}
		if pods := w.podLabels[ns]; pods != nil {
			delete(pods, id)
			if len(pods) == 0 {
				delete(w.podLabels, ns)
			}
		}
		w.markDeleted(b, id)
		return
	}

	w.observe(b, id, KindPod)
	decl := w.ownerEdges(b, pod.OwnerReferences, ns, id)
	if pod.Spec.NodeName != "" {
		decl = append(decl, halfEdge{From: id, To: w.node(b, KindNode, "", pod.Spec.NodeName), Kind: EdgeRunsOn})
	}
	containers := make([]corev1.Container, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers))
	containers = append(containers, pod.Spec.InitContainers...)
	containers = append(containers, pod.Spec.Containers...)
	for i := range containers {
		c := &containers[i]
		cid := w.node(b, KindContainer, ns, pod.Name+"/"+c.Name)
		w.observe(b, cid, KindContainer)
		decl = append(decl, halfEdge{From: id, To: cid, Kind: EdgeContains})
		decl = append(decl, w.containerMounts(b, ns, id, c)...)
	}
	decl = append(decl, w.volumeMounts(b, ns, id, pod.Spec.Volumes)...)
	w.setDeclared(b, id, decl)

	// Label index + selector re-evaluation: every Service /
	// NetworkPolicy selector in the namespace is re-checked against
	// the pod's current labels.
	set := labels.Set(pod.Labels)
	pods := w.podLabels[ns]
	if pods == nil {
		pods = make(map[NodeID]labels.Set)
		w.podLabels[ns] = pods
	}
	pods[id] = set
	for src, ent := range w.selectors[ns] {
		w.setSelectorEdge(b, src, id, ent.kind, ent.sel.Matches(set))
	}
}

// containerMounts derives Pod→ConfigMap/Secret Mounts edges from a
// container's env and envFrom. Only reference *names* are read —
// never values.
func (w *Writer) containerMounts(b *batch, ns string, pod NodeID, c *corev1.Container) []halfEdge {
	var decl []halfEdge
	mount := func(kind NodeKind, name string) {
		decl = append(decl, halfEdge{From: pod, To: w.node(b, kind, ns, name), Kind: EdgeMounts})
	}
	for i := range c.Env {
		if vf := c.Env[i].ValueFrom; vf != nil {
			if vf.ConfigMapKeyRef != nil {
				mount(KindConfigMap, vf.ConfigMapKeyRef.Name)
			}
			if vf.SecretKeyRef != nil {
				mount(KindSecret, vf.SecretKeyRef.Name)
			}
		}
	}
	for i := range c.EnvFrom {
		if ref := c.EnvFrom[i].ConfigMapRef; ref != nil {
			mount(KindConfigMap, ref.Name)
		}
		if ref := c.EnvFrom[i].SecretRef; ref != nil {
			mount(KindSecret, ref.Name)
		}
	}
	return decl
}

// volumeMounts derives Pod→ConfigMap/Secret/PVC Mounts edges from
// pod volumes, including projected volume sources.
func (w *Writer) volumeMounts(b *batch, ns string, pod NodeID, volumes []corev1.Volume) []halfEdge {
	var decl []halfEdge
	mount := func(kind NodeKind, name string) {
		decl = append(decl, halfEdge{From: pod, To: w.node(b, kind, ns, name), Kind: EdgeMounts})
	}
	for i := range volumes {
		switch v := &volumes[i]; {
		case v.ConfigMap != nil:
			mount(KindConfigMap, v.ConfigMap.Name)
		case v.Secret != nil:
			mount(KindSecret, v.Secret.SecretName)
		case v.PersistentVolumeClaim != nil:
			mount(KindPersistentVolumeClaim, v.PersistentVolumeClaim.ClaimName)
		case v.Projected != nil:
			for j := range v.Projected.Sources {
				s := &v.Projected.Sources[j]
				if s.ConfigMap != nil {
					mount(KindConfigMap, s.ConfigMap.Name)
				}
				if s.Secret != nil {
					mount(KindSecret, s.Secret.Name)
				}
			}
		}
	}
	return decl
}

func (w *Writer) applyService(b *batch, op Op, svc *corev1.Service) {
	ns := svc.Namespace
	id := w.node(b, KindService, ns, svc.Name)
	if op == OpDelete {
		w.dropSelector(ns, id)
		w.setDeclared(b, id, nil)
		w.markDeleted(b, id)
		return
	}
	w.observe(b, id, KindService)
	decl := w.ownerEdges(b, svc.OwnerReferences, ns, id)
	if len(svc.Spec.Selector) > 0 {
		sel := labels.SelectorFromSet(labels.Set(svc.Spec.Selector))
		w.setSelector(ns, id, selEntry{sel: sel, kind: EdgeSelects})
		for pod, pl := range w.podLabels[ns] {
			if sel.Matches(pl) {
				decl = append(decl, halfEdge{From: id, To: pod, Kind: EdgeSelects})
			}
		}
	} else {
		// Selector-less service (external / manual endpoints): no
		// Selects edges; traffic reachability still shows up via its
		// EndpointSlices.
		w.dropSelector(ns, id)
	}
	w.setDeclared(b, id, decl)
}

func (w *Writer) applyNetworkPolicy(b *batch, op Op, np *netv1.NetworkPolicy) {
	ns := np.Namespace
	id := w.node(b, KindNetworkPolicy, ns, np.Name)
	if op == OpDelete {
		w.dropSelector(ns, id)
		w.setDeclared(b, id, nil)
		w.markDeleted(b, id)
		return
	}
	w.observe(b, id, KindNetworkPolicy)
	decl := w.ownerEdges(b, np.OwnerReferences, ns, id)
	// An empty podSelector selects every pod in the namespace —
	// that is the k8s semantic and exactly the blast radius a
	// misconfigured deny-all policy has.
	sel, err := metav1.LabelSelectorAsSelector(&np.Spec.PodSelector)
	if err == nil {
		w.setSelector(ns, id, selEntry{sel: sel, kind: EdgeGoverns})
		for pod, pl := range w.podLabels[ns] {
			if sel.Matches(pl) {
				decl = append(decl, halfEdge{From: id, To: pod, Kind: EdgeGoverns})
			}
		}
	} else {
		// Unparseable selector (the API server rejects these; only
		// reachable with hand-built objects): no Governs edges.
		w.dropSelector(ns, id)
	}
	w.setDeclared(b, id, decl)
}

func (w *Writer) setSelector(ns string, src NodeID, ent selEntry) {
	m := w.selectors[ns]
	if m == nil {
		m = make(map[NodeID]selEntry)
		w.selectors[ns] = m
	}
	m[src] = ent
}

func (w *Writer) dropSelector(ns string, src NodeID) {
	m := w.selectors[ns]
	if m == nil {
		return
	}
	delete(m, src)
	if len(m) == 0 {
		delete(w.selectors, ns)
	}
}

func (w *Writer) applyEndpointSlice(b *batch, op Op, eps *discoveryv1.EndpointSlice) {
	ns := eps.Namespace
	id := w.node(b, KindEndpointSlice, ns, eps.Name)
	if op == OpDelete {
		w.setDeclared(b, id, nil)
		w.markDeleted(b, id)
		return
	}
	w.observe(b, id, KindEndpointSlice)
	decl := w.ownerEdges(b, eps.OwnerReferences, ns, id)
	if svcName := eps.Labels[discoveryv1.LabelServiceName]; svcName != "" {
		svc := w.node(b, KindService, ns, svcName)
		decl = append(decl, halfEdge{From: svc, To: id, Kind: EdgeRoutesTo})
	}
	for i := range eps.Endpoints {
		ref := eps.Endpoints[i].TargetRef
		if ref == nil || ref.Kind != "Pod" || ref.Name == "" {
			continue
		}
		pod := w.node(b, KindPod, ns, ref.Name)
		decl = append(decl, halfEdge{From: id, To: pod, Kind: EdgeRoutesTo})
	}
	w.setDeclared(b, id, decl)
}

func (w *Writer) applyIngress(b *batch, op Op, ing *netv1.Ingress) {
	ns := ing.Namespace
	id := w.node(b, KindIngress, ns, ing.Name)
	if op == OpDelete {
		w.setDeclared(b, id, nil)
		w.markDeleted(b, id)
		return
	}
	w.observe(b, id, KindIngress)
	decl := w.ownerEdges(b, ing.OwnerReferences, ns, id)
	route := func(backend *netv1.IngressBackend) {
		if backend == nil || backend.Service == nil || backend.Service.Name == "" {
			return
		}
		svc := w.node(b, KindService, ns, backend.Service.Name)
		decl = append(decl, halfEdge{From: id, To: svc, Kind: EdgeRoutesTo})
	}
	route(ing.Spec.DefaultBackend)
	for i := range ing.Spec.Rules {
		if http := ing.Spec.Rules[i].HTTP; http != nil {
			for j := range http.Paths {
				route(&http.Paths[j].Backend)
			}
		}
	}
	w.setDeclared(b, id, decl)
}

func (w *Writer) applyNode(b *batch, op Op, node *corev1.Node) {
	id := w.node(b, KindNode, "", node.Name)
	if op == OpDelete {
		w.setDeclared(b, id, nil)
		w.markDeleted(b, id)
		return
	}
	w.observe(b, id, KindNode)
	decl := w.ownerEdges(b, node.OwnerReferences, "", id)
	zone := node.Labels[corev1.LabelTopologyZone]
	if zone == "" {
		zone = node.Labels[corev1.LabelFailureDomainBetaZone]
	}
	if zone != "" {
		zid := w.node(b, KindZone, "", zone)
		w.observe(b, zid, KindZone) // derived kind: observed while referenced
		decl = append(decl, halfEdge{From: id, To: zid, Kind: EdgeRunsOn})
	}
	w.setDeclared(b, id, decl)
}
