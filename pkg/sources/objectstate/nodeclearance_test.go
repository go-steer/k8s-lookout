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

package objectstate

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// clearNode builds a Node with a Ready condition carrying a last
// transition time — the instant NodeClearance vouches stability from.
func clearNode(uid, name string, ready corev1.ConditionStatus, transition time.Time) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Name: name},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: ready, LastTransitionTime: metav1.Time{Time: transition}},
		}},
	}
}

func nodeIncident(uid, name string) engine.Incident {
	return engine.Incident{
		Key:       engine.EventKey{UID: uid, Reason: "node_notready"},
		SessionID: "sess-storm",
		FirstSeen: time.Date(2026, 7, 25, 0, 40, 0, 0, time.UTC),
		Ref: engine.IncidentRef{
			KindOfObject: "Node",
			Name:         name,
			Fingerprint:  "sha256:node",
			Cluster:      "kl-m2",
		},
	}
}

func syncedNodeClearance() *NodeClearance {
	nc := NewNodeClearance()
	nc.SetSynced(func() bool { return true })
	return nc
}

func TestNodeClearance_DeclinesNonNodeAndUnsynced(t *testing.T) {
	t.Parallel()
	nc := NewNodeClearance()
	// Not synced: cannot judge even a Node incident.
	if _, ok := nc.Clearance(nodeIncident("n1", "worker-1")); ok {
		t.Error("unsynced observer must decline to judge")
	}
	nc.SetSynced(func() bool { return true })
	// Pod-scoped incidents are not this observer's to judge.
	pod := nodeIncident("p1", "pay-1")
	pod.Ref.KindOfObject = "Pod"
	if _, ok := nc.Clearance(pod); ok {
		t.Error("node observer must not judge pod-scoped incidents")
	}
	// Node-scoped it judges, even with no state (gone-node path).
	if _, ok := nc.Clearance(nodeIncident("n1", "worker-1")); !ok {
		t.Error("synced observer must judge Node incidents")
	}
}

// TestNodeClearance_NotReadyThenReady is the M2 drill A epilogue in
// miniature: the node goes NotReady (incident opens), comes back
// Ready — the clearance flips with StableSince at the Ready
// transition, so the tracker's stability window measures real
// readiness, not observation time.
func TestNodeClearance_NotReadyThenReady(t *testing.T) {
	t.Parallel()
	nc := syncedNodeClearance()
	down := time.Date(2026, 7, 25, 0, 40, 0, 0, time.UTC)
	up := down.Add(3 * time.Minute)

	nc.Upsert(clearNode("n1", "worker-1", corev1.ConditionFalse, down))
	v, ok := nc.Clearance(nodeIncident("n1", "worker-1"))
	if !ok || v.Cleared {
		t.Fatalf("NotReady node must judge as not cleared (ok=%v, %+v)", ok, v)
	}

	// Unknown counts as NOT ready (node controller lost contact).
	nc.Upsert(clearNode("n1", "worker-1", corev1.ConditionUnknown, down))
	if v, _ := nc.Clearance(nodeIncident("n1", "worker-1")); v.Cleared {
		t.Fatal("Ready=Unknown must judge as not cleared")
	}

	nc.Upsert(clearNode("n1", "worker-1", corev1.ConditionTrue, up))
	v, ok = nc.Clearance(nodeIncident("n1", "worker-1"))
	if !ok || !v.Cleared {
		t.Fatalf("Ready node must judge as cleared (ok=%v, %+v)", ok, v)
	}
	if v.Resolution != engine.ResolutionRecovered {
		t.Errorf("Resolution = %q, want recovered", v.Resolution)
	}
	if !v.StableSince.Equal(up) {
		t.Errorf("StableSince = %v, want the Ready transition %v", v.StableSince, up)
	}
}

func TestNodeClearance_DeletedNodeObjectDeleted(t *testing.T) {
	t.Parallel()
	nc := syncedNodeClearance()
	deletedAt := time.Date(2026, 7, 25, 0, 50, 0, 0, time.UTC)
	nc.now = func() time.Time { return deletedAt }

	n := clearNode("n1", "worker-1", corev1.ConditionFalse, deletedAt.Add(-time.Minute))
	nc.Upsert(n)
	nc.Delete(n)

	v, ok := nc.Clearance(nodeIncident("n1", "worker-1"))
	if !ok || !v.Cleared {
		t.Fatalf("gone node must judge as cleared (ok=%v, %+v)", ok, v)
	}
	if v.Resolution != engine.ResolutionObjectDeleted {
		t.Errorf("Resolution = %q, want object_deleted (a removed node is not a fix)", v.Resolution)
	}
	if !v.StableSince.Equal(deletedAt) {
		t.Errorf("StableSince = %v, want deletion instant %v", v.StableSince, deletedAt)
	}
}

// TestNodeClearance_SameNameReplacement: a node recreated under the
// same name (fresh UID) is the capacity the incident was about — the
// incident clears as recovered against the replacement, and a
// NotReady replacement keeps the symptom alive.
func TestNodeClearance_SameNameReplacement(t *testing.T) {
	t.Parallel()
	nc := syncedNodeClearance()
	old := clearNode("n1", "worker-1", corev1.ConditionFalse, time.Date(2026, 7, 25, 0, 40, 0, 0, time.UTC))
	nc.Upsert(old)
	nc.Delete(old)

	up := time.Date(2026, 7, 25, 0, 55, 0, 0, time.UTC)
	nc.Upsert(clearNode("n2", "worker-1", corev1.ConditionFalse, up))
	if v, _ := nc.Clearance(nodeIncident("n1", "worker-1")); v.Cleared {
		t.Fatal("NotReady replacement must keep the incident symptomatic")
	}
	nc.Upsert(clearNode("n2", "worker-1", corev1.ConditionTrue, up))
	v, ok := nc.Clearance(nodeIncident("n1", "worker-1"))
	if !ok || !v.Cleared || v.Resolution != engine.ResolutionRecovered {
		t.Fatalf("Ready same-name replacement must clear as recovered (ok=%v, %+v)", ok, v)
	}
	if !v.StableSince.Equal(up) {
		t.Errorf("StableSince = %v, want replacement Ready transition %v", v.StableSince, up)
	}
}

// pressureClearNode builds a Node that is Ready (transition at
// readyAt) with a MemoryPressure condition of the given status
// (transition at pressureAt).
func pressureClearNode(uid, name string, mem corev1.ConditionStatus, readyAt, pressureAt time.Time) *corev1.Node {
	n := clearNode(uid, name, corev1.ConditionTrue, readyAt)
	n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{
		Type: corev1.NodeMemoryPressure, Status: mem, LastTransitionTime: metav1.Time{Time: pressureAt},
	})
	return n
}

// TestNodeClearance_PressureAware pins the reason branch: a
// node_pressure incident on a READY node is judged by the pressure
// conditions, not Ready-ness — the §7.4 interaction that would
// otherwise clear the incident the moment it opened.
func TestNodeClearance_PressureAware(t *testing.T) {
	t.Parallel()
	nc := syncedNodeClearance()
	readyAt := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	onset := readyAt.Add(40 * time.Minute)

	inc := nodeIncident("n1", "worker-1")
	inc.Key.Reason = "node_pressure"

	// Ready node under memory pressure: still symptomatic.
	nc.Upsert(pressureClearNode("n1", "worker-1", corev1.ConditionTrue, readyAt, onset))
	v, ok := nc.Clearance(inc)
	if !ok || v.Cleared {
		t.Fatalf("Ready node under pressure must NOT clear a node_pressure incident (ok=%v, %+v)", ok, v)
	}

	// The Ready-based family on the same node still clears by
	// Ready-ness, untouched by the pressure branch.
	ready := nodeIncident("n1", "worker-1") // reason node_notready
	if v, ok := nc.Clearance(ready); !ok || !v.Cleared || !v.StableSince.Equal(readyAt) {
		t.Errorf("node_notready on the same node must clear by Ready-ness (ok=%v, %+v)", ok, v)
	}

	// Pressure lifts: cleared, vouched-stable from the pressure
	// condition's transition time.
	lifted := onset.Add(10 * time.Minute)
	nc.Upsert(pressureClearNode("n1", "worker-1", corev1.ConditionFalse, readyAt, lifted))
	v, ok = nc.Clearance(inc)
	if !ok || !v.Cleared || v.Resolution != engine.ResolutionRecovered {
		t.Fatalf("pressure-free node must clear as recovered (ok=%v, %+v)", ok, v)
	}
	if !v.StableSince.Equal(lifted) {
		t.Errorf("StableSince = %v, want the pressure transition %v", v.StableSince, lifted)
	}

	// Same-name replacement path takes the branch too.
	old := pressureClearNode("n1", "worker-1", corev1.ConditionFalse, readyAt, lifted)
	nc.Delete(old)
	nc.Upsert(pressureClearNode("n2", "worker-1", corev1.ConditionTrue, readyAt, lifted))
	if v, _ := nc.Clearance(inc); v.Cleared {
		t.Error("pressured same-name replacement must keep node_pressure symptomatic")
	}
}

// TestNodeClearance_ResolvesThroughTracker drives the full §7.4 state
// machine: fake NotReady→Ready sequence, tracker ticks across the
// stability window, kind=resolved comes out — the drill A gap
// (node incident never resolves) closed.
func TestNodeClearance_ResolvesThroughTracker(t *testing.T) {
	t.Parallel()
	nc := syncedNodeClearance()
	var emitted []engine.Signal
	tracker := engine.NewRecoveryTracker(time.Minute, func(sig engine.Signal) { emitted = append(emitted, sig) })
	tracker.AddObserver(nc)

	inc := nodeIncident("n1", "worker-2")
	tracker.Track(inc)

	down := inc.FirstSeen
	nc.Upsert(clearNode("n1", "worker-2", corev1.ConditionFalse, down))
	tracker.Tick()
	if len(emitted) != 0 {
		t.Fatalf("emitted while NotReady: %+v", emitted)
	}

	nc.Upsert(clearNode("n1", "worker-2", corev1.ConditionTrue, time.Now().Add(-2*time.Minute)))
	tracker.Tick() // clearing: StableSince is already 2m ago > 1m window
	tracker.Tick()
	if len(emitted) != 1 {
		t.Fatalf("want 1 resolved emission, got %d", len(emitted))
	}
	sig := emitted[0]
	if sig.Kind != engine.KindResolved {
		t.Errorf("Kind = %q, want resolved", sig.Kind)
	}
	if sig.Recovery == nil || sig.Recovery.Resolution != engine.ResolutionRecovered {
		t.Errorf("Recovery = %+v, want resolution recovered", sig.Recovery)
	}
	if sig.KindOfObject != "Node" || sig.Name != "worker-2" || sig.Fingerprint != "sha256:node" {
		t.Errorf("identity not carried: %+v", sig.TriageEvent)
	}
}

// TestSourceClearanceObserver_JudgesNodesAndPods pins the composed
// surface: the source's single ClearanceObserver() judges node-scoped
// incidents via the node informer's state and pod-scoped ones via the
// pod informer's — the wiring setupRecovery relies on.
func TestSourceClearanceObserver_JudgesNodesAndPods(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	s.pc.SetSynced(func() bool { return true })
	s.nc.SetSynced(func() bool { return true })
	obs := s.ClearanceObserver()

	up := time.Date(2026, 7, 25, 1, 0, 0, 0, time.UTC)
	s.onNode(clearNode("n1", "worker-1", corev1.ConditionTrue, up))
	v, ok := obs.Clearance(nodeIncident("n1", "worker-1"))
	if !ok || !v.Cleared || !v.StableSince.Equal(up) {
		t.Errorf("composed observer must judge Node incidents from the node informer (ok=%v, %+v)", ok, v)
	}

	pod := nodeIncident("p1", "pay-1")
	pod.Ref.KindOfObject = "Pod"
	pod.Ref.Namespace = "shop"
	if _, ok := obs.Clearance(pod); !ok {
		t.Error("composed observer must still judge Pod incidents (PodClearance path)")
	}
}
