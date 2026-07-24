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
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// queryFixture is a small two-deployment cluster exercising every
// query direction:
//
//	ingress edge → svc web → slice web-s → pods web-1..2
//	web: Deployment→ReplicaSet→{web-1@node-1, web-2@node-2}
//	api: Deployment→ReplicaSet→{api-1@node-1}
//	web pods mount cm-web + cm-shared; api-1 mounts cm-shared
//	netpol deny-all governs all pods; nodes in zone-a
func queryFixture(t *testing.T) *Snapshot {
	t.Helper()
	mknode := func(name string) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name: name, Labels: map[string]string{corev1.LabelTopologyZone: "zone-a"},
		}}
	}
	mkdep := func(app string) []any {
		return []any{
			&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: app}},
			&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns", Name: app + "-rs",
				OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: app}},
			}},
		}
	}
	mkpod := func(app, name, node string, cms ...string) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns", Name: name,
				Labels:          map[string]string{"app": app},
				OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: app + "-rs"}},
			},
			Spec: corev1.PodSpec{
				NodeName:   node,
				Containers: []corev1.Container{{Name: "app"}},
			},
		}
		for i, cm := range cms {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
				Name: "v" + string(rune('a'+i)),
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cm},
				}},
			})
		}
		return p
	}
	objs := []any{
		mknode("node-1"), mknode("node-2"),
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cm-web"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cm-shared"}},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "web"},
			Spec:       corev1.ServiceSpec{Selector: map[string]string{"app": "web"}},
		},
		&discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns", Name: "web-s",
				Labels:          map[string]string{discoveryv1.LabelServiceName: "web"},
				OwnerReferences: []metav1.OwnerReference{{Kind: "Service", Name: "web"}},
			},
			Endpoints: []discoveryv1.Endpoint{
				{TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: "web-1"}},
				{TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: "web-2"}},
			},
		},
		&netv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "edge"},
			Spec: netv1.IngressSpec{
				DefaultBackend: &netv1.IngressBackend{Service: &netv1.IngressServiceBackend{Name: "web"}},
			},
		},
		&netv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "deny-all"}},
	}
	objs = append(objs, mkdep("web")...)
	objs = append(objs, mkdep("api")...)
	objs = append(objs,
		mkpod("web", "web-1", "node-1", "cm-web", "cm-shared"),
		mkpod("web", "web-2", "node-2", "cm-web", "cm-shared"),
		mkpod("api", "api-1", "node-1", "cm-shared"),
	)
	_, s := buildGraph(t, objs)
	checkInvariants(t, s)
	return s
}

func mustLookup(t *testing.T, s *Snapshot, kind NodeKind, ns, name string) NodeID {
	t.Helper()
	id, ok := s.Lookup(kind, ns, name)
	if !ok {
		t.Fatalf("lookup %s/%s/%s failed", kind, ns, name)
	}
	return id
}

func hitStrings(s *Snapshot, hits []Hit) []string {
	out := make([]string, len(hits))
	for i, h := range hits {
		out[i] = refString(s, h.ID)
	}
	slices.Sort(out)
	return out
}

func TestRadiusDirections(t *testing.T) {
	s := queryFixture(t)
	origin := mustLookup(t, s, KindPod, "ns", "web-1")
	r := s.Radius(origin, 3)

	wantUp := []string{
		"EndpointSlice/ns/web-s",
		"Deployment/ns/web",
		"Ingress/ns/edge",
		"NetworkPolicy/ns/deny-all",
		"ReplicaSet/ns/web-rs",
		"Service/ns/web",
	}
	slices.Sort(wantUp)
	if got := hitStrings(s, r.Up); !slices.Equal(got, wantUp) {
		t.Errorf("Up = %v, want %v", got, wantUp)
	}

	wantDown := []string{
		"ConfigMap/ns/cm-shared",
		"ConfigMap/ns/cm-web",
		"Container/ns/web-1/app",
		"Node//node-1",
		"Zone//zone-a",
	}
	slices.Sort(wantDown)
	if got := hitStrings(s, r.Down); !slices.Equal(got, wantDown) {
		t.Errorf("Down = %v, want %v", got, wantDown)
	}

	// Lateral: api-1 shares node-1 and cm-shared; web-2 shares
	// cm-web/cm-shared; node-2 shares zone-a.
	wantLateral := []string{
		"Node//node-2",
		"Pod/ns/api-1",
		"Pod/ns/web-2",
	}
	slices.Sort(wantLateral)
	if got := hitStrings(s, r.Lateral); !slices.Equal(got, wantLateral) {
		t.Errorf("Lateral = %v, want %v", got, wantLateral)
	}

	// Determinism: same snapshot, same answer, byte for byte.
	r2 := s.Radius(origin, 3)
	if !slices.Equal(r.Up, r2.Up) || !slices.Equal(r.Down, r2.Down) || !slices.Equal(r.Lateral, r2.Lateral) {
		t.Error("Radius is not deterministic on a fixed snapshot")
	}
}

func TestRadiusDepthLimit(t *testing.T) {
	s := queryFixture(t)
	origin := mustLookup(t, s, KindPod, "ns", "web-1")

	r1 := s.Radius(origin, 1)
	for _, h := range append(append([]Hit{}, r1.Up...), r1.Down...) {
		if h.Depth != 1 {
			t.Errorf("depth-1 radius contains depth-%d hit %s", h.Depth, refString(s, h.ID))
		}
	}
	// Zone is 2 hops down (pod→node→zone): absent at depth 1.
	if slices.Contains(hitStrings(s, r1.Down), "Zone//zone-a") {
		t.Error("depth-1 radius leaked a depth-2 node")
	}
	// Ingress is 2 hops up (pod←svc←ingress): absent at depth 1.
	if slices.Contains(hitStrings(s, r1.Up), "Ingress/ns/edge") {
		t.Error("depth-1 radius leaked a depth-2 up node")
	}

	// Unknown origin and non-positive depth are empty, not panics.
	if r := s.Radius(NodeID(999999), 3); len(r.Up)+len(r.Down)+len(r.Lateral) != 0 {
		t.Error("unknown origin must yield empty radius")
	}
	if r := s.Radius(origin, 0); len(r.Up)+len(r.Down)+len(r.Lateral) != 0 {
		t.Error("zero depth must yield empty radius")
	}
}

func TestOwnerChain(t *testing.T) {
	s := queryFixture(t)
	pod := mustLookup(t, s, KindPod, "ns", "web-1")
	var got []string
	for _, id := range s.OwnerChain(pod) {
		got = append(got, refString(s, id))
	}
	want := []string{"ReplicaSet/ns/web-rs", "Deployment/ns/web"}
	if !slices.Equal(got, want) {
		t.Fatalf("OwnerChain = %v, want %v", got, want)
	}
	if chain := s.OwnerChain(mustLookup(t, s, KindDeployment, "ns", "web")); len(chain) != 0 {
		t.Fatalf("root object has owner chain %v", chain)
	}
}

func TestCommonAncestors(t *testing.T) {
	s := queryFixture(t)
	web1 := mustLookup(t, s, KindPod, "ns", "web-1")
	web2 := mustLookup(t, s, KindPod, "ns", "web-2")
	api1 := mustLookup(t, s, KindPod, "ns", "api-1")

	toSet := func(ids []NodeID) map[string]bool {
		m := make(map[string]bool, len(ids))
		for _, id := range ids {
			m[refString(s, id)] = true
		}
		return m
	}

	// Same deployment, different nodes: owner chain + shared config
	// correlate; the nodes must not.
	got := toSet(s.CommonAncestors(web1, web2))
	for _, want := range []string{"ReplicaSet/ns/web-rs", "Deployment/ns/web", "ConfigMap/ns/cm-web", "Namespace//ns", "Zone//zone-a"} {
		if !got[want] {
			t.Errorf("CommonAncestors(web-1, web-2) missing %s (got %v)", want, got)
		}
	}
	if got["Node//node-1"] || got["Node//node-2"] {
		t.Error("pods on different nodes must not share a node ancestor")
	}

	// Different deployments, same node: the node is the shared
	// ancestor (the classic "30 pods on one broken node" storm key),
	// and it ranks before the zone.
	ordered := s.CommonAncestors(web1, api1)
	got = toSet(ordered)
	if !got["Node//node-1"] || !got["ConfigMap/ns/cm-shared"] || !got["Namespace//ns"] {
		t.Errorf("CommonAncestors(web-1, api-1) = %v, want node-1 + cm-shared + namespace", got)
	}
	if got["ReplicaSet/ns/web-rs"] || got["Deployment/ns/web"] {
		t.Error("unrelated workloads must not share an owner ancestor")
	}
	nodeIdx := slices.IndexFunc(ordered, func(id NodeID) bool { return refString(s, id) == "Node//node-1" })
	zoneIdx := slices.IndexFunc(ordered, func(id NodeID) bool { return refString(s, id) == "Zone//zone-a" })
	if nodeIdx == -1 || zoneIdx == -1 || nodeIdx > zoneIdx {
		t.Errorf("nearest-first ordering violated: node at %d, zone at %d in %v", nodeIdx, zoneIdx, ordered)
	}

	// Nothing shared: pods in different namespaces on different
	// nodes.
	other := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "elsewhere", Name: "p"}}
	g2, s2 := buildGraph(t, []any{other,
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "q"}}})
	_ = g2
	a := mustLookup(t, s2, KindPod, "elsewhere", "p")
	q := mustLookup(t, s2, KindPod, "ns", "q")
	if anc := s2.CommonAncestors(a, q); len(anc) != 0 {
		t.Errorf("unrelated pods share ancestors: %v", anc)
	}
	if anc := s.CommonAncestors(); anc != nil {
		t.Errorf("no inputs must yield nil, got %v", anc)
	}
}

func TestWorkloadEdges(t *testing.T) {
	s := queryFixture(t)
	dep := mustLookup(t, s, KindDeployment, "ns", "web")
	var got []string
	for _, e := range s.WorkloadEdges(dep) {
		got = append(got, refString(s, e.From)+" -"+e.Kind.String()+"-> "+refString(s, e.To))
	}
	slices.Sort(got)
	want := []string{
		// Pod dependencies lifted to the workload, deduplicated.
		"Deployment/ns/web -Mounts-> ConfigMap/ns/cm-shared",
		"Deployment/ns/web -Mounts-> ConfigMap/ns/cm-web",
		"Deployment/ns/web -RunsOn-> Node//node-1",
		"Deployment/ns/web -RunsOn-> Node//node-2",
		// Traffic/policy sources reported against the workload.
		"EndpointSlice/ns/web-s -RoutesTo-> Deployment/ns/web",
		"NetworkPolicy/ns/deny-all -Governs-> Deployment/ns/web",
		"Service/ns/web -Selects-> Deployment/ns/web",
		// One northbound hop: the rest of the traffic chain.
		"Ingress/ns/edge -RoutesTo-> Service/ns/web",
		"Service/ns/web -RoutesTo-> EndpointSlice/ns/web-s",
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("WorkloadEdges =\n  %v\nwant\n  %v", got, want)
	}

	// PodsUnder resolves the owner chain down.
	pods := hitStringsIDs(s, s.PodsUnder(dep))
	if !slices.Equal(pods, []string{"Pod/ns/web-1", "Pod/ns/web-2"}) {
		t.Fatalf("PodsUnder = %v", pods)
	}
	// A pod input aggregates over itself.
	pod := mustLookup(t, s, KindPod, "ns", "api-1")
	for _, e := range s.WorkloadEdges(pod) {
		if e.Kind == EdgeContains {
			t.Fatal("WorkloadEdges must not surface Contains edges")
		}
	}
}

func hitStringsIDs(s *Snapshot, ids []NodeID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = refString(s, id)
	}
	slices.Sort(out)
	return out
}

// TestCycleSafety builds degenerate cycles (mutual ownerReferences —
// illegal in k8s, but the graph must not assume validity) and checks
// every traversal terminates.
func TestCycleSafety(t *testing.T) {
	a := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: "ns", Name: "a",
		OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "b"}},
	}}
	bb := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: "ns", Name: "b",
		OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "a"}},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "ns", Name: "p",
		OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "a"}},
	}}
	_, s := buildGraph(t, []any{a, bb, pod})
	checkInvariants(t, s)

	podID := mustLookup(t, s, KindPod, "ns", "p")
	aID := mustLookup(t, s, KindReplicaSet, "ns", "a")

	chain := s.OwnerChain(podID)
	if len(chain) > ownerChainMax {
		t.Fatalf("owner chain did not terminate: %d entries", len(chain))
	}
	r := s.Radius(podID, 10)
	if len(r.Up) == 0 {
		t.Fatal("radius up empty despite owners")
	}
	if anc := s.CommonAncestors(podID, aID); len(anc) == 0 {
		t.Fatal("cycle members share no ancestor?")
	}
	_ = s.PodsUnder(aID) // must terminate
}
