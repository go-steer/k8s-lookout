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
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// One crash-looping Deployment rolled out mid-failure (PR #569's live
// run): the old ReplicaSet's pod is still backing off when the new
// ReplicaSet's pod starts backing off too. One fault, one Deployment.

// crashLoopPod is a kubelet BackOff (crash-loop) event on pod name,
// shaped as the k8s-events source emits it.
func crashLoopPod(uid, name string, sec int) engine.Signal {
	ts := time.Date(2026, 10, 8, 9, 0, sec, 0, time.UTC)
	return engine.Signal{
		Kind:     engine.KindK8sEvent,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: uid, Reason: "BackOff"},
			Namespace:    "triage-demo",
			KindOfObject: "Pod",
			Name:         name,
			Container:    "spec.containers{checkout}",
			Message:      "Back-off restarting failed container checkout in pod " + name,
			Count:        5,
			FirstSeen:    ts,
			LastSeen:     ts,
		},
	}
}

// newRolloutDispatcher wires the per-incident pipeline the way
// wiring.go does with shipped defaults: severity routing, watchboard,
// and — when storm is true — the storm correlator, the reattachment
// stage and the ancestor resolver over a REAL topology graph (pods
// owned by ReplicaSets owned by Deployments).
func newRolloutDispatcher(t *testing.T, base string, storm bool, objs ...any) *dispatcher {
	t.Helper()
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
	d.filter = engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 0, 0))
	if !storm {
		return d
	}
	feed := buildFeedGraph(t, objs...)
	correlator, err := engine.NewStormCorrelator(engine.DefaultStormWindow, engine.DefaultStormMin, feed)
	if err != nil {
		t.Fatalf("NewStormCorrelator: %v", err)
	}
	d.storm = correlator
	d.resolver = feed
	d.board.reattach = d.reattachWatchboardEntry
	return d
}

// rolloutTopology is one Deployment, two ReplicaSets (old + new), one
// pod in each, on different nodes.
func rolloutTopology() []any {
	return []any{
		testNode("gke-a"), testNode("gke-b"),
		testRS("triage-demo", "checkout-784cfb56df", "checkout"),
		testRS("triage-demo", "checkout-5d9f8c7b6", "checkout"),
		testPod("triage-demo", "checkout-784cfb56df-krzkz", "gke-a", "checkout-784cfb56df", ""),
		testPod("triage-demo", "checkout-5d9f8c7b6-q2x7p", "gke-b", "checkout-5d9f8c7b6", ""),
	}
}

var (
	podOld = "checkout-784cfb56df-krzkz" // old ReplicaSet
	podNew = "checkout-5d9f8c7b6-q2x7p"  // new ReplicaSet
)

func sessionCreates(d *dispatcher) float64 {
	return testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok"))
}

// TestSiblingFold_RolloutAcrossReplicaSetsIsOneIncident is the live
// observation: with storm on (the shipped auto default wherever the
// graph grants exist), the new ReplicaSet's crash-looping pod folds
// into the incident the old one opened, as a family.member followup
// keyed on the Deployment — the ReplicaSets differ, so the shared key
// has to be the Deployment. Its later events are plain duplicates.
func TestSiblingFold_RolloutAcrossReplicaSetsIsOneIncident(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRolloutDispatcher(t, base, true, rolloutTopology()...)
	ctx := context.Background()

	d.DispatchSignal(ctx, crashLoopPod("pod-a", podOld, 0))
	d.DispatchSignal(ctx, crashLoopPod("pod-b", podNew, 20))
	later := crashLoopPod("pod-b", podNew, 50)
	later.Count = 9
	d.DispatchSignal(ctx, later)
	d.board.FlushNow(ctx)

	if got := sessionCreates(d); got != 1 {
		t.Fatalf("session creates = %v, want 1 — one fault on one Deployment is one incident: %+v", got, *injects)
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
	if fm[0].MemberKind != engine.KindK8sEvent || fm[0].DesignRef != inject.FamilyMemberDesignRef {
		t.Errorf("family.member kind/design_ref = %q/%q", fm[0].MemberKind, fm[0].DesignRef)
	}
	if sid, ok := d.dedup.LookupSession(crashLoopPod("pod-b", podNew, 0).CanonicalKey()); !ok || sid != "sess-1" {
		t.Errorf("new pod binding = (%q, %v), want (sess-1, true) — its outcome must close into the shared session", sid, ok)
	}
	if len(*injects) != 2 {
		t.Errorf("injects = %d, want 2 (the open + one followup; the new pod's later event dedups)", len(*injects))
	}
}

// TestSiblingFold_StormOffKeepsTwoIncidents: the fold reads the owner
// chain from the topology graph, which is built only under --storm
// (the #220 precondition — k8s-events signals carry no owner). With
// storm off, the pair stays two incidents, as before.
func TestSiblingFold_StormOffKeepsTwoIncidents(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRolloutDispatcher(t, base, false)
	ctx := context.Background()

	d.DispatchSignal(ctx, crashLoopPod("pod-a", podOld, 0))
	d.DispatchSignal(ctx, crashLoopPod("pod-b", podNew, 20))

	if got := sessionCreates(d); got != 2 {
		t.Errorf("session creates = %v, want 2 (no topology graph, nothing to fold on)", got)
	}
	if n := len(familyMembers(t, *injects)); n != 0 {
		t.Errorf("family.member injects = %d, want 0", n)
	}
}

// TestSiblingFold_DifferentDeploymentsStayTwo: two workloads failing
// the same way on the SAME node are two incidents — placement is not
// an owner, and a pair is below --storm-min.
func TestSiblingFold_DifferentDeploymentsStayTwo(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRolloutDispatcher(t, base, true,
		testNode("gke-a"),
		testRS("triage-demo", "checkout-784cfb56df", "checkout"),
		testRS("triage-demo", "payments-6c8d9f7b5", "payments"),
		testPod("triage-demo", podOld, "gke-a", "checkout-784cfb56df", "shared-config"),
		testPod("triage-demo", "payments-6c8d9f7b5-zt4mk", "gke-a", "payments-6c8d9f7b5", "shared-config"),
	)
	ctx := context.Background()

	d.DispatchSignal(ctx, crashLoopPod("pod-a", podOld, 0))
	d.DispatchSignal(ctx, crashLoopPod("pod-p", "payments-6c8d9f7b5-zt4mk", 20))

	if got := sessionCreates(d); got != 2 {
		t.Errorf("session creates = %v, want 2 — a shared node or ConfigMap is not a shared owner", got)
	}
	if n := len(familyMembers(t, *injects)); n != 0 {
		t.Errorf("family.member injects = %d, want 0", n)
	}
}

// TestSiblingFold_DifferentClassStaysTwo: the fold is same-class
// only. A pod of the same Deployment failing differently (a mount
// failure beside a crash loop) is its own incident.
func TestSiblingFold_DifferentClassStaysTwo(t *testing.T) {
	t.Parallel()
	base, _ := newRoutingFakeDaemon(t)
	d := newRolloutDispatcher(t, base, true, rolloutTopology()...)
	ctx := context.Background()

	d.DispatchSignal(ctx, crashLoopPod("pod-a", podOld, 0))
	mount := crashLoopPod("pod-b", podNew, 20)
	mount.Key.Reason = "FailedMount"
	mount.Message = `MountVolume.SetUp failed for volume "cfg" : configmap "checkout-config" not found`
	d.DispatchSignal(ctx, mount)

	if got := sessionCreates(d); got != 2 {
		t.Errorf("session creates = %v, want 2 (different failure class)", got)
	}
}

// TestSiblingFold_StormStillFormsAndSupersedesOnce: the fold runs
// after the storm stage, so a third same-ancestor incident still
// forms the §7.5 storm exactly as before — and the folded pod, having
// no session of its own, adds no second supersede pointer.
func TestSiblingFold_StormStillFormsAndSupersedesOnce(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	objs := append(rolloutTopology(),
		testPod("triage-demo", "checkout-5d9f8c7b6-m8w2d", "gke-b", "checkout-5d9f8c7b6", ""))
	d := newRolloutDispatcher(t, base, true, objs...)
	ctx := context.Background()

	d.DispatchSignal(ctx, crashLoopPod("pod-a", podOld, 0))
	d.DispatchSignal(ctx, crashLoopPod("pod-b", podNew, 20))
	d.DispatchSignal(ctx, crashLoopPod("pod-c", "checkout-5d9f8c7b6-m8w2d", 30))

	if got := testutil.ToFloat64(d.metrics.stormsFormed); got != 1 {
		t.Fatalf("storms formed = %v, want 1", got)
	}
	if got := sessionCreates(d); got != 2 {
		t.Errorf("session creates = %v, want 2 (the first incident + the storm)", got)
	}
	superseded := 0
	for _, in := range *injects {
		var p struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal([]byte(messageOf(t, in.Body)), &p) == nil && p.Kind == inject.KindStormMemberSuperseded {
			superseded++
			if in.SessionID != "sess-1" {
				t.Errorf("supersede pointer into %q, want sess-1", in.SessionID)
			}
		}
	}
	if superseded != 1 {
		t.Errorf("supersede pointers = %d, want 1 — one pre-storm session, pointed at once", superseded)
	}
	for _, uid := range []string{"pod-a", "pod-b", "pod-c"} {
		if sid, _ := d.dedup.LookupSession(crashLoopPod(uid, "x", 0).CanonicalKey()); sid != "sess-2" {
			t.Errorf("%s bound to %q, want the storm session sess-2", uid, sid)
		}
	}
}
