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

package watch

import (
	"context"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
	"github.com/go-steer/k8s-lookout/pkg/sources/k8sevents"
)

// The sibling fold with --storm=off (issue #583): no topology graph,
// so the owner the fold keys on is the one the k8s-events source puts
// in ControllerRef from the pod and ReplicaSet caches. These tests
// resolve it with the REAL source over a fake clientset, then drive
// the dispatcher with the shipped storm-off wiring.

func controllerRef(apiVersion, kind, name string) []metav1.OwnerReference {
	yes := true
	return []metav1.OwnerReference{{
		APIVersion: apiVersion, Kind: kind, Name: name,
		UID: types.UID(kind + "-" + name), Controller: &yes,
	}}
}

// ownedRS is a ReplicaSet controlled by Deployment deploy.
func ownedRS(name, deploy string) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: "triage-demo", Name: name, UID: types.UID("ReplicaSet-" + name),
		OwnerReferences: controllerRef("apps/v1", "Deployment", deploy),
	}}
}

// ownedPod is pod name (UID uid) controlled by ReplicaSet rs.
func ownedPod(name, uid, rs string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "triage-demo", Name: name, UID: types.UID(uid),
		OwnerReferences: controllerRef("apps/v1", "ReplicaSet", rs),
	}}
}

// sourceControllerRefs runs the k8s-events source with owner
// resolution on a shared factory over objs, fires one BackOff event
// per pod (name → UID), and returns the ControllerRef each event's
// signal carried.
func sourceControllerRefs(t *testing.T, objs []runtime.Object, pods map[string]string) map[string]string {
	t.Helper()
	client := fake.NewClientset(objs...)
	factory := informers.NewSharedInformerFactory(client, 0)
	src := k8sevents.New(client, 0)
	src.WithFactory(factory)
	src.WithOwnerResolution(true)

	var mu sync.Mutex
	got := map[string]string{}
	emit := func(sig sources.Signal) {
		mu.Lock()
		got[sig.Name] = sig.ControllerRef
		mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- src.Run(ctx, emit) }()
	for !src.HasSynced() {
		select {
		case <-ctx.Done():
			t.Fatal("k8s-events source never armed")
		case <-time.After(time.Millisecond):
		}
	}
	for typ, ok := range factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			t.Fatalf("cache %v never synced", typ)
		}
	}
	for name, uid := range pods {
		ev := &corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Namespace: "triage-demo", Name: name + ".backoff"},
			Reason:         "BackOff",
			Type:           corev1.EventTypeWarning,
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "triage-demo", Name: name, UID: types.UID(uid)},
			Count:          5,
		}
		if _, err := client.CoreV1().Events("triage-demo").Create(ctx, ev, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create event: %v", err)
		}
	}
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == len(pods) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("signals for %d of %d pods", n, len(pods))
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	<-done
	return got
}

// ownedCrashLoop is crashLoopPod carrying the source-resolved owner.
func ownedCrashLoop(refs map[string]string, uid, name string, sec int) engine.Signal {
	sig := crashLoopPod(uid, name, sec)
	sig.ControllerRef = refs[name]
	return sig
}

// TestSiblingFold_StormOffRolloutIsOneIncident is the issue's done
// line: a rollout across two ReplicaSets, both pods crash-looping,
// --storm=off — one incident, the second pod a family.member keyed on
// the Deployment the source resolved.
func TestSiblingFold_StormOffRolloutIsOneIncident(t *testing.T) {
	t.Parallel()
	refs := sourceControllerRefs(t, []runtime.Object{
		ownedRS("checkout-784cfb56df", "checkout"),
		ownedRS("checkout-5d9f8c7b6", "checkout"),
		ownedPod(podOld, "pod-a", "checkout-784cfb56df"),
		ownedPod(podNew, "pod-b", "checkout-5d9f8c7b6"),
	}, map[string]string{podOld: "pod-a", podNew: "pod-b"})
	for _, name := range []string{podOld, podNew} {
		if refs[name] != "Deployment/checkout" {
			t.Fatalf("source ControllerRef for %s = %q, want Deployment/checkout", name, refs[name])
		}
	}

	base, injects := newRoutingFakeDaemon(t)
	d := newRolloutDispatcher(t, base, false)
	if d.resolver != nil || d.storm != nil {
		t.Fatal("storm-off dispatcher has a graph — the test would prove nothing")
	}
	ctx := context.Background()
	d.DispatchSignal(ctx, ownedCrashLoop(refs, "pod-a", podOld, 0))
	d.DispatchSignal(ctx, ownedCrashLoop(refs, "pod-b", podNew, 20))
	later := ownedCrashLoop(refs, "pod-b", podNew, 50)
	later.Count = 9
	d.DispatchSignal(ctx, later)
	d.board.FlushNow(ctx)

	if got := sessionCreates(d); got != 1 {
		t.Fatalf("session creates = %v, want 1 — one fault on one Deployment is one incident with --storm=off too: %+v", got, *injects)
	}
	fm := familyMembers(t, *injects)
	if len(fm) != 1 {
		t.Fatalf("family.member injects = %d, want 1: %+v", len(fm), *injects)
	}
	wantFamily := engine.Ancestor{Kind: "Deployment", Namespace: "triage-demo", Name: "checkout"}.Key()
	if fm[0].SessionID != "sess-1" || fm[0].Name != podNew || fm[0].Family != wantFamily {
		t.Errorf("family.member = (session %q, name %q, family %q), want (sess-1, %s, %s)",
			fm[0].SessionID, fm[0].Name, fm[0].Family, podNew, wantFamily)
	}
	if sid, ok := d.dedup.LookupSession(crashLoopPod("pod-b", podNew, 0).CanonicalKey()); !ok || sid != "sess-1" {
		t.Errorf("new pod binding = (%q, %v), want (sess-1, true)", sid, ok)
	}
	if len(*injects) != 2 {
		t.Errorf("injects = %d, want 2 (the open + one followup; the new pod's later event dedups)", len(*injects))
	}
}

// TestSiblingFold_StormOffDifferentDeploymentsStayTwo: two Deployments
// failing the same way are two incidents — their owners differ.
func TestSiblingFold_StormOffDifferentDeploymentsStayTwo(t *testing.T) {
	t.Parallel()
	const payPod = "payments-6c8d9f7b5-zt4mk"
	refs := sourceControllerRefs(t, []runtime.Object{
		ownedRS("checkout-784cfb56df", "checkout"),
		ownedRS("payments-6c8d9f7b5", "payments"),
		ownedPod(podOld, "pod-a", "checkout-784cfb56df"),
		ownedPod(payPod, "pod-p", "payments-6c8d9f7b5"),
	}, map[string]string{podOld: "pod-a", payPod: "pod-p"})
	if refs[podOld] != "Deployment/checkout" || refs[payPod] != "Deployment/payments" {
		t.Fatalf("source ControllerRefs = %v", refs)
	}

	base, injects := newRoutingFakeDaemon(t)
	d := newRolloutDispatcher(t, base, false)
	ctx := context.Background()
	d.DispatchSignal(ctx, ownedCrashLoop(refs, "pod-a", podOld, 0))
	d.DispatchSignal(ctx, ownedCrashLoop(refs, "pod-p", payPod, 20))

	if got := sessionCreates(d); got != 2 {
		t.Errorf("session creates = %v, want 2 — different Deployments are different workloads", got)
	}
	if n := len(familyMembers(t, *injects)); n != 0 {
		t.Errorf("family.member injects = %d, want 0", n)
	}
}

// TestSiblingFold_StormOffUncachedReplicaSetFallsBack: the old
// ReplicaSet is not in the cache (already gone, or not yet synced), so
// the source can only prove the ReplicaSet as the old pod's owner. The
// two pods then share no owner key — two incidents, never a fold on a
// guessed Deployment. A pod missing from the pod cache carries no
// owner at all and also stays its own incident.
func TestSiblingFold_StormOffUncachedReplicaSetFallsBack(t *testing.T) {
	t.Parallel()
	const ghost = "checkout-5d9f8c7b6-gone1"
	refs := sourceControllerRefs(t, []runtime.Object{
		ownedRS("checkout-5d9f8c7b6", "checkout"),
		ownedPod(podOld, "pod-a", "checkout-784cfb56df"), // its ReplicaSet is not cached
		ownedPod(podNew, "pod-b", "checkout-5d9f8c7b6"),
	}, map[string]string{podOld: "pod-a", podNew: "pod-b", ghost: "pod-g"})
	if refs[podOld] != "ReplicaSet/checkout-784cfb56df" || refs[podNew] != "Deployment/checkout" || refs[ghost] != "" {
		t.Fatalf("source ControllerRefs = %v, want old=ReplicaSet/checkout-784cfb56df new=Deployment/checkout ghost=empty", refs)
	}

	base, injects := newRoutingFakeDaemon(t)
	d := newRolloutDispatcher(t, base, false)
	ctx := context.Background()
	d.DispatchSignal(ctx, ownedCrashLoop(refs, "pod-a", podOld, 0))
	d.DispatchSignal(ctx, ownedCrashLoop(refs, "pod-b", podNew, 20))
	d.DispatchSignal(ctx, ownedCrashLoop(refs, "pod-g", ghost, 30))

	if got := sessionCreates(d); got != 3 {
		t.Errorf("session creates = %v, want 3 — an unproven owner must not fold", got)
	}
	if n := len(familyMembers(t, *injects)); n != 0 {
		t.Errorf("family.member injects = %d, want 0", n)
	}
}

// TestSiblingFold_StormOnWithControllerRefFoldsOnce: with the graph
// AND a source-resolved owner, both name the same Deployment key — the
// fold happens once, keyed on it, exactly as with the graph alone.
func TestSiblingFold_StormOnWithControllerRefFoldsOnce(t *testing.T) {
	t.Parallel()
	refs := map[string]string{podOld: "Deployment/checkout", podNew: "Deployment/checkout"}
	base, injects := newRoutingFakeDaemon(t)
	d := newRolloutDispatcher(t, base, true, rolloutTopology()...)
	ctx := context.Background()

	d.DispatchSignal(ctx, ownedCrashLoop(refs, "pod-a", podOld, 0))
	d.DispatchSignal(ctx, ownedCrashLoop(refs, "pod-b", podNew, 20))
	d.board.FlushNow(ctx)

	if got := sessionCreates(d); got != 1 {
		t.Fatalf("session creates = %v, want 1: %+v", got, *injects)
	}
	fm := familyMembers(t, *injects)
	wantFamily := engine.Ancestor{Kind: "Deployment", Namespace: "triage-demo", Name: "checkout"}.Key()
	if len(fm) != 1 || fm[0].Family != wantFamily {
		t.Errorf("family.member injects = %+v, want one under %s", fm, wantFamily)
	}
}
