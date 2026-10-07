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

package computeclass

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// classObject renders a ComputeClass the way the dynamic informer delivers
// one: an Unstructured whose spec is generic JSON, never a typed struct.
// There is no Go type for this CRD and deliberately no generated one — §7.7
// decodes the handful of spec fields it understands and reports the rest as
// unsupported.
func classObject(t *testing.T, name, raw string) *unstructured.Unstructured {
	t.Helper()
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cloud.google.com/v1",
		"kind":       "ComputeClass",
		"metadata":   map[string]any{"name": name},
		"spec":       spec(t, raw),
	}}
}

// servedClient is a typed fake whose discovery answers "yes, ComputeClasses".
func servedClient(objs ...runtime.Object) *fake.Clientset {
	c := fake.NewSimpleClientset(objs...)
	c.Resources = []*metav1.APIResourceList{{
		GroupVersion: "cloud.google.com/v1",
		APIResources: []metav1.APIResource{{Name: "computeclasses", Namespaced: false, Kind: "ComputeClass"}},
	}}
	return c
}

func dynClient(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		scheme,
		map[schema.GroupVersionResource]string{computeClassGVR: "ComputeClassList"},
		objs...,
	)
}

// runSource starts Run and returns once the caches have synced, so a test can
// read state without racing the initial LIST. Cancellation and the goroutine's
// return are both waited on, which is what makes the deferred closeMetrics
// deterministic — two of these in one package with a live meter would
// otherwise collide on a duplicate registration.
func runSource(t *testing.T, s *Source) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, func(sources.Signal) {}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return within 5s of cancellation")
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for !s.HasSynced() {
		if time.Now().After(deadline) {
			t.Fatal("caches never synced")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// awaitRank polls until the named node resolves to want, which is how an
// informer-driven test asserts a value: the handlers run on the shared
// processor's goroutine, so a read straight after the sync barrier is a race
// even when the object was in the initial LIST.
func awaitRank(t *testing.T, s *Source, node string, want leeway.Rank) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got leeway.Rank
	for time.Now().Before(deadline) {
		s.mu.Lock()
		ns := s.nodes[node]
		if ns != nil {
			got = ns.place.Rank
		}
		s.mu.Unlock()
		if ns != nil && got == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("node %q settled at rank %v, want %v", node, got, want)
}

// newRunnableSource wires a source over the two fakes with its own informer
// factories — the standalone shape, which is also the shape the wiring uses
// before it hands over the shared factory.
func newRunnableSource(t *testing.T, client *fake.Clientset, dyn *dynamicfake.FakeDynamicClient) *Source {
	t.Helper()
	cfg := DefaultConfig()
	// Short enough that a test can watch a flush happen without sleeping for
	// half a minute; the tracker is exact at flush time either way.
	cfg.FlushInterval = 10 * time.Millisecond
	s, err := New(client, dyn, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.logf = func(string, ...any) {}
	return s
}

// TestRunJoinsThreeStreams is the end-to-end shape of the source: the rank of
// a pod is a property of the node it landed on, and the rank of a node is a
// property of the class it belongs to. Nothing here can be read off one
// stream — the whole source is that join.
func TestRunJoinsThreeStreams(t *testing.T) {
	client := servedClient(
		node("gke-n4-1", "n4-preferred", "n4", "0"),
		node("gke-c3-1", "n4-preferred", "c3", "1"),
		pod("api-a", "gke-n4-1", "n4-preferred", corev1.PodRunning),
		pod("api-b", "gke-c3-1", "n4-preferred", corev1.PodRunning),
	)
	dyn := dynClient(classObject(t, "n4-preferred", n4PreferredSpec))
	s := newRunnableSource(t, client, dyn)
	runSource(t, s)

	awaitRank(t, s, "gke-n4-1", 0)
	awaitRank(t, s, "gke-c3-1", 1)

	s.mu.Lock()
	defer s.mu.Unlock()
	for ref, ps := range s.pods {
		if !ps.counted {
			t.Errorf("pod %v is not accruing time", ref)
		}
	}
	if len(s.pods) != 2 {
		t.Errorf("tracked %d pods, want 2", len(s.pods))
	}
}

// TestRunChargesOnlyPodsThatAskedForTheClass is the 2026-10-02 drill, where
// the leeway-fallback-probe axis read pods=10 for one workload pod. A class
// node runs the workload that asked for it plus every DaemonSet GKE schedules
// on all nodes (kube-proxy, fluentbit, netd, gke-metadata-server, pdcsi, …)
// and any static pod. None of those asked for the class; they are on the node
// because it is a node. Charging them weights each node by its DaemonSet count
// and turns §7.7.3's rank shares into a node census.
//
// The workload pod is created after the barrier on purpose: one informer
// delivers in order, so once it is charged every system pod in the initial
// LIST has been handled too, and the count below is final rather than early.
func TestRunChargesOnlyPodsThatAskedForTheClass(t *testing.T) {
	const class = "leeway-fallback-probe"
	objs := []runtime.Object{node("gke-probe-1", class, "n2", "1")}
	for _, ds := range []string{"kube-proxy", "fluentbit-gke", "netd", "gke-metadata-server", "pdcsi-node", "gke-metrics-agent", "image-package-extractor", "konnectivity-agent", "node-local-dns"} {
		p := pod(ds+"-x7k2p", "gke-probe-1", "", corev1.PodRunning)
		p.Namespace = "kube-system"
		p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: ds}}
		// The blanket toleration DaemonSets carry: it admits them to the
		// class taint without naming the class, so it is not a request for it.
		p.Spec.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
		objs = append(objs, p)
	}
	mirror := pod("kube-proxy-gke-probe-1", "gke-probe-1", "", corev1.PodRunning)
	mirror.Namespace = "kube-system"
	mirror.Annotations = map[string]string{corev1.MirrorPodAnnotationKey: "static"}
	mirror.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: "gke-probe-1"}}
	objs = append(objs, mirror)

	client := servedClient(objs...)
	s := newRunnableSource(t, client, dynClient(classObject(t, class, n4PreferredSpec)))
	runSource(t, s)
	awaitRank(t, s, "gke-probe-1", 1)

	workload := pod("probe-6d4f9b-abcde", "gke-probe-1", class, corev1.PodRunning)
	workload.Spec.Tolerations = []corev1.Toleration{{Key: "cloud.google.com/compute-class", Operator: corev1.TolerationOpEqual, Value: class, Effect: corev1.TaintEffectNoSchedule}}
	if _, err := client.CoreV1().Pods("default").Create(context.Background(), workload, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	ref := podRef{"default", workload.Name}
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		ps := s.pods[ref]
		charged := ps != nil && ps.counted
		s.mu.Unlock()
		if charged {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the workload pod was never charged")
		}
		time.Sleep(2 * time.Millisecond)
	}

	key := leeway.AxisKey{Provider: leeway.ProviderGKEComputeClass, Name: class}
	pods := 0
	for _, r := range s.tracker.Snapshot().Ranks {
		if r.Axis == key {
			pods += r.Pods
		}
	}
	if pods != 1 {
		t.Errorf("pods on %s = %d, want 1 — only the workload asked for the class", class, pods)
	}
}

// TestRunReconcilesAClassThatSyncedLast. The three informers sync
// independently and nothing orders them, so on any given start a node may be
// handled before the class that ranks it. A stable node has no next event, so
// without the post-barrier reconcile pass a quiet fleet would stay unranked
// until somebody touched it — which for a node that is working fine is never.
//
// The class is created AFTER the informers start here, which is the same
// situation from the state machine's point of view and the only one a test can
// arrange deterministically.
func TestRunReconcilesALateClass(t *testing.T) {
	client := servedClient(
		node("gke-n4-1", "n4-preferred", "n4", "0"),
		pod("api-a", "gke-n4-1", "n4-preferred", corev1.PodRunning),
	)
	dyn := dynClient()
	s := newRunnableSource(t, client, dyn)
	runSource(t, s)

	// Before the class exists the node carries one, so it is not off-axis —
	// it is a node whose axis has not arrived.
	awaitRank(t, s, "gke-n4-1", leeway.RankUnknown)

	if _, err := dyn.Resource(computeClassGVR).Create(context.Background(),
		classObject(t, "n4-preferred", n4PreferredSpec), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create ComputeClass: %v", err)
	}
	awaitRank(t, s, "gke-n4-1", 0)
}

// TestRunFollowsANodeDownTheLadder is the condition the source exists for,
// driven through the informer rather than the state machine: capacity for the
// preferred shape runs out, GKE provisions the next rule down, and nothing
// about the pod or the Deployment changes.
func TestRunFollowsANodeDownTheLadder(t *testing.T) {
	client := servedClient(pod("api-a", "gke-fallback", "n4-preferred", corev1.PodRunning))
	dyn := dynClient(classObject(t, "n4-preferred", n4PreferredSpec))
	s := newRunnableSource(t, client, dyn)
	runSource(t, s)

	first := node("gke-fallback", "n4-preferred", "n4", "0")
	if _, err := client.CoreV1().Nodes().Create(context.Background(), first, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create node: %v", err)
	}
	awaitRank(t, s, "gke-fallback", 0)

	// GKE re-stamps the annotation on the node it actually made. (A real
	// fallback provisions a new node; re-annotating one is the compressed
	// version, and it is the node-level transition the source counts.)
	moved := node("gke-fallback", "n4-preferred", "n2", "2")
	if _, err := client.CoreV1().Nodes().Update(context.Background(), moved, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update node: %v", err)
	}
	awaitRank(t, s, "gke-fallback", 2)

	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for key, n := range s.transitions {
		if key.Lateral {
			t.Errorf("0 → 2 counted as lateral: %+v", key)
		}
		total += n
	}
	if total != 1 {
		t.Errorf("counted %d transitions, want exactly 1", total)
	}
}

// TestRunUnchargesADeletedNode. Deletion arrives through the informer as the
// object or as a tombstone, and both have to stop the clock — a node that is
// gone is not still accruing pod-seconds at rank 2.
func TestRunUnchargesADeletedNode(t *testing.T) {
	client := servedClient(
		node("gke-n4-1", "n4-preferred", "n4", "0"),
		pod("api-a", "gke-n4-1", "n4-preferred", corev1.PodRunning),
	)
	dyn := dynClient(classObject(t, "n4-preferred", n4PreferredSpec))
	s := newRunnableSource(t, client, dyn)
	runSource(t, s)
	awaitRank(t, s, "gke-n4-1", 0)

	if err := client.CoreV1().Nodes().Delete(context.Background(), "gke-n4-1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete node: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		_, still := s.nodes["gke-n4-1"]
		s.mu.Unlock()
		if !still {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("deleted node never left the map")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := underflows(s); got != 0 {
		t.Errorf("tracker underflows = %d after a node deletion, want 0", got)
	}
}

// TestRunRefusesWithoutTheCRD. The grant is inert on a cluster without
// ComputeClasses (RBAC naming an absent group always allows), so an operator
// who named this source explicitly on a non-GKE cluster would otherwise get a
// source that starts, watches nothing and reports nothing — which reads
// exactly like a healthy estate at rank 0.
func TestRunRefusesWithoutTheCRD(t *testing.T) {
	s := newRunnableSource(t, fake.NewSimpleClientset(), dynClient())
	err := s.Run(context.Background(), func(sources.Signal) {})
	if err == nil {
		t.Fatal("Run succeeded on a cluster with no ComputeClass CRD")
	}
	// The message has to name the flag, because the fix is a flag edit and
	// the operator is reading a crash loop.
	if !strings.Contains(err.Error(), "--sources") {
		t.Errorf("refusal does not say how to fix it: %v", err)
	}
	if s.HasSynced() {
		t.Error("HasSynced is true after a refused start")
	}
}

// TestComputeClassesServed is the auto-resolution gate in isolation.
func TestComputeClassesServed(t *testing.T) {
	if !ComputeClassesServed(servedClient()) {
		t.Error("ComputeClassesServed = false with the CRD present")
	}
	if ComputeClassesServed(fake.NewSimpleClientset()) {
		t.Error("ComputeClassesServed = true with no cloud.google.com group")
	}
	// The group served but not this resource: GKE serves cloud.google.com for
	// other things, so the check has to look for the resource and not settle
	// for the group.
	partial := fake.NewSimpleClientset()
	partial.Resources = []*metav1.APIResourceList{{
		GroupVersion: "cloud.google.com/v1",
		APIResources: []metav1.APIResource{{Name: "backendconfigs"}},
	}}
	if ComputeClassesServed(partial) {
		t.Error("ComputeClassesServed = true with the group but not the resource")
	}
}

// TestTombstonesAreUnwrapped. A watch that falls behind delivers
// DeletedFinalStateUnknown instead of the object, and a handler that ignored
// it would leak the node — and go on charging its pods forever.
func TestTombstonesAreUnwrapped(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	s.onNode(node("gke-n4-1", "n4-preferred", "n4", "0"))
	s.onPod(pod("api-a", "gke-n4-1", "n4-preferred", corev1.PodRunning))
	s.onClass(classObject(t, "n4-preferred", n4PreferredSpec))
	if len(s.nodes) != 1 || len(s.pods) != 1 || len(s.classes) != 1 {
		t.Fatalf("handlers did not land: %d nodes, %d pods, %d classes", len(s.nodes), len(s.pods), len(s.classes))
	}

	s.onPodDelete(cache.DeletedFinalStateUnknown{Key: "default/api-a", Obj: pod("api-a", "gke-n4-1", "n4-preferred", corev1.PodRunning)})
	s.onNodeDelete(cache.DeletedFinalStateUnknown{Key: "gke-n4-1", Obj: node("gke-n4-1", "n4-preferred", "n4", "0")})
	s.onClassDelete(cache.DeletedFinalStateUnknown{Key: "n4-preferred", Obj: classObject(t, "n4-preferred", n4PreferredSpec)})
	if len(s.nodes) != 0 || len(s.pods) != 0 || len(s.classes) != 0 {
		t.Errorf("tombstones ignored: %d nodes, %d pods, %d classes left", len(s.nodes), len(s.pods), len(s.classes))
	}
	if got := underflows(s); got != 0 {
		t.Errorf("tracker underflows = %d, want 0", got)
	}
}

// TestHandlersIgnoreForeignObjects. The typed casts fail closed: a handler
// that panicked on an unexpected object would take the whole sentinel down
// over a cache oddity, and one that guessed would corrupt the counts.
func TestHandlersIgnoreForeignObjects(t *testing.T) {
	s := newTestSource(t)
	junk := []any{nil, "a string", &corev1.Service{}, cache.DeletedFinalStateUnknown{Key: "x", Obj: nil}}
	for _, obj := range junk {
		s.onPod(obj)
		s.onPodDelete(obj)
		s.onNode(obj)
		s.onNodeDelete(obj)
		s.onClass(obj)
		s.onClassDelete(obj)
	}
	if len(s.nodes) != 0 || len(s.pods) != 0 || len(s.classes) != 0 {
		t.Errorf("a foreign object entered the state: %d nodes, %d pods, %d classes", len(s.nodes), len(s.pods), len(s.classes))
	}
}

// TestSourceContract pins the three declarations the registry reads: the name
// operators type, the tier the §11 probe enforces, and the grant it probes.
func TestSourceContract(t *testing.T) {
	s := newTestSource(t)
	if s.Name() != Name || Name != "compute-class" {
		t.Errorf("Name = %q, want compute-class", s.Name())
	}
	if s.Scope() != sources.ScopeCluster {
		t.Errorf("Scope = %v, want cluster — a rank share over the fraction of the estate a namespace Role can see is not a rank share", s.Scope())
	}
	seen := map[string]bool{}
	for _, r := range s.RequiredAccess() {
		seen[r.Group+"/"+r.Resource+"/"+r.Verb] = true
	}
	for _, want := range []string{
		"cloud.google.com/computeclasses/list",
		"cloud.google.com/computeclasses/watch",
		// Every informer in Run's sync barrier is declared, so a grant
		// missing one fails the probe instead of hanging Run (#407).
		"/pods/list", "/pods/watch",
		"/nodes/list", "/nodes/watch",
	} {
		if !seen[want] {
			t.Errorf("missing required-access declaration %q (have %v)", want, seen)
		}
	}
	// Events are deliberately NOT declared: they sit outside the barrier,
	// and a process that cannot read them degrades the wedged rule only.
	if seen["/events/list"] || seen["/events/watch"] {
		t.Error("events declared; they are outside Run's sync barrier and must not gate startup")
	}
	if len(s.RequiredAccess()) != 6 {
		t.Errorf("RequiredAccess declares %d requirements, want the two CRD verbs plus pods and nodes list/watch", len(s.RequiredAccess()))
	}
}
