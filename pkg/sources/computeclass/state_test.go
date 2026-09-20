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
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

var t0 = time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return t0.Add(d) }

// n4PreferredSpec is §7.7.2's verified class: three unscored priorities, so
// preference is list position and rank equals index.
const n4PreferredSpec = `{
  "priorities": [
    {"machineFamily": "n4"},
    {"machineFamily": "c3"},
    {"machineFamily": "n2", "spot": true}
  ],
  "whenUnsatisfiable": "ScaleUpAnyway",
  "activeMigration": {"optimizeRulePriority": true}
}`

// s2ScoredSpec is spike S2's class, and the reason rank and index are two
// different numbers. Its LEAST preferred rule sits at list position 0, and
// positions 1 and 2 are peers at the best tier.
const s2ScoredSpec = `{
  "priorities": [
    {"machineFamily": "n2", "priorityScore": 10},
    {"machineFamily": "n4", "priorityScore": 50},
    {"machineFamily": "c3", "priorityScore": 50}
  ],
  "whenUnsatisfiable": "DoNotScaleUp"
}`

func spec(t *testing.T, raw string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	return out
}

func newTestSource(t *testing.T) *Source {
	t.Helper()
	s, err := New(fake.NewSimpleClientset(), nil, DefaultConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.now = func() time.Time { return t0 }
	s.logf = func(string, ...any) {}
	return s
}

// node builds a node carrying a compute class. family is stamped as the
// machine-family label so inference has something to match; annotation is the
// raw ccc_priority_index value, and "" means absent.
func node(name, class, family, annotation string) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"cloud.google.com/compute-class":   class,
				"cloud.google.com/machine-family":  family,
				"node.kubernetes.io/instance-type": family + "-standard-4",
			},
		},
		Status: corev1.NodeStatus{Capacity: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("4"),
			corev1.ResourceMemory: resource.MustParse("16Gi"),
		}},
	}
	if class == "" {
		delete(n.Labels, "cloud.google.com/compute-class")
	}
	if annotation != "" {
		n.Annotations = map[string]string{"ccc_priority_index": annotation}
	}
	return n
}

func pod(name, nodeName string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
		Spec:       corev1.PodSpec{NodeName: nodeName},
		Status:     corev1.PodStatus{Phase: phase},
	}
}

// wedgedPod is an unscheduled pod that asked for a compute class by name: no
// node, Pending, and the class label in its nodeSelector. That is the exact
// shape wedgedOn looks for, and the only symptom a wedged class has.
func wedgedPod(name, class string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
		Spec:       corev1.PodSpec{NodeSelector: map[string]string{DefaultConfig().ClassLabel: class}},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}
}

// podSeconds reads one bucket out of a snapshot, flushing to `when` first so
// the reading is exact as of that instant — which is what a scrape does.
func podSeconds(t *testing.T, s *Source, axis string, rank leeway.Rank, when time.Time) float64 {
	t.Helper()
	s.tracker.Flush(when)
	key := leeway.AxisKey{Provider: leeway.ProviderGKEComputeClass, Name: axis}
	for _, r := range s.tracker.Snapshot().Ranks {
		if r.Axis == key && r.Rank == rank {
			return r.PodSeconds
		}
	}
	return 0
}

func underflows(s *Source) int64 { return s.tracker.Snapshot().Underflows }

// TestPodAccruesTimeAtItsNodesRank is the whole source in one case: a class, a
// node GKE put at priority 1, a pod on it, and a hundred seconds.
func TestPodAccruesTimeAtItsNodesRank(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("n1", "n4-preferred", "c3", "1"), t0)
	s.UpsertPod(pod("p1", "n1", corev1.PodRunning), t0)

	if got := podSeconds(t, s, "n4-preferred", 1, at(100*time.Second)); got != 100 {
		t.Errorf("rank-1 pod-seconds = %v, want 100", got)
	}
	if got := podSeconds(t, s, "n4-preferred", 0, at(100*time.Second)); got != 0 {
		t.Errorf("rank-0 pod-seconds = %v, want 0 — nothing was ever at rank 0", got)
	}
	if n := underflows(s); n != 0 {
		t.Errorf("underflows = %d, want 0", n)
	}
}

// TestRankIsTierNotIndex is the trap of §7.7.1, end to end. On S2's class a
// node stamped index 2 is at the BEST tier, and a node stamped index 0 is at
// the worst.
func TestRankIsTierNotIndex(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("s2", spec(t, s2ScoredSpec), t0)
	s.UpsertNode(node("best", "s2", "c3", "2"), t0)
	s.UpsertNode(node("worst", "s2", "n2", "0"), t0)
	s.UpsertPod(pod("pb", "best", corev1.PodRunning), t0)
	s.UpsertPod(pod("pw", "worst", corev1.PodRunning), t0)

	end := at(10 * time.Second)
	if got := podSeconds(t, s, "s2", 0, end); got != 10 {
		t.Errorf("rank-0 pod-seconds = %v, want 10 — index 2 is the best tier here", got)
	}
	if got := podSeconds(t, s, "s2", 1, end); got != 10 {
		t.Errorf("rank-1 pod-seconds = %v, want 10 — index 0 is the worst tier here", got)
	}
}

// TestPeerFallbackIsLateralAndCostsNoRank is S2's other measurement: a stockout
// on n4 fell through to c3, the index rose from 1 to 2, and the preference
// tier did not move at all.
func TestPeerFallbackIsLateralAndCostsNoRank(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("s2", spec(t, s2ScoredSpec), t0)
	s.UpsertNode(node("n1", "s2", "n4", "1"), t0)
	s.UpsertPod(pod("p1", "n1", corev1.PodRunning), t0)
	s.UpsertNode(node("n1", "s2", "c3", "2"), at(30*time.Second))

	// Sixty seconds, all of it at rank 0, spanning a move GKE's own index says
	// was a demotion.
	if got := podSeconds(t, s, "s2", 0, at(60*time.Second)); got != 60 {
		t.Errorf("rank-0 pod-seconds = %v, want 60 — a peer move does not interrupt the tier", got)
	}

	axis := leeway.AxisKey{Provider: leeway.ProviderGKEComputeClass, Name: "s2"}
	want := transitionKey{Axis: axis, From: 0, To: 0, Lateral: true}
	if n := s.transitions[want]; n != 1 {
		t.Errorf("lateral transitions = %d, want 1 (have %v)", n, s.transitions)
	}
	if n := underflows(s); n != 0 {
		t.Errorf("underflows = %d, want 0 — a lateral move must not unbalance the buckets", n)
	}
}

// TestRealFallbackMovesTheRank is the condition the source exists to see: the
// same class, a real demotion, and the pod-seconds splitting across two tiers.
func TestRealFallbackMovesTheRank(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("n1", "n4-preferred", "n4", "0"), t0)
	s.UpsertPod(pod("p1", "n1", corev1.PodRunning), t0)
	s.UpsertNode(node("n1", "n4-preferred", "c3", "1"), at(40*time.Second))

	end := at(100 * time.Second)
	if got := podSeconds(t, s, "n4-preferred", 0, end); got != 40 {
		t.Errorf("rank-0 pod-seconds = %v, want 40", got)
	}
	if got := podSeconds(t, s, "n4-preferred", 1, end); got != 60 {
		t.Errorf("rank-1 pod-seconds = %v, want 60", got)
	}

	axis := leeway.AxisKey{Provider: leeway.ProviderGKEComputeClass, Name: "n4-preferred"}
	if n := s.transitions[transitionKey{Axis: axis, From: 0, To: 1}]; n != 1 {
		t.Errorf("0→1 transitions = %d, want 1 (have %v)", n, s.transitions)
	}
}

// TestLateClassIsReconciled covers the ordering the three informers give no
// guarantee about. A node seen before its class resolves against nothing, and
// nothing will touch it again — the node is stable — so the post-sync
// reconcile is the only thing that ever ranks it.
func TestLateClassIsReconciled(t *testing.T) {
	s := newTestSource(t)
	s.UpsertNode(node("n1", "n4-preferred", "n4", "0"), t0)
	s.UpsertPod(pod("p1", "n1", corev1.PodRunning), t0)

	if got := podSeconds(t, s, "n4-preferred", 0, at(10*time.Second)); got != 0 {
		t.Fatalf("accrued %v pod-seconds against a class that had not arrived", got)
	}
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), at(10*time.Second))
	s.reconcile(at(10 * time.Second))

	if got := podSeconds(t, s, "n4-preferred", 0, at(70*time.Second)); got != 60 {
		t.Errorf("rank-0 pod-seconds = %v, want 60 — the pod should start accruing when the class lands", got)
	}
}

// TestReconcileIsIdempotent guards the pass that runs after cache sync: it
// re-resolves every node, and a re-resolution that produced the same answer
// must not re-enter pods that are already counted.
func TestReconcileIsIdempotent(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("n1", "n4-preferred", "n4", "0"), t0)
	s.UpsertPod(pod("p1", "n1", corev1.PodRunning), t0)
	for i := 0; i < 5; i++ {
		s.reconcile(at(time.Duration(i) * time.Second))
	}
	if got := podSeconds(t, s, "n4-preferred", 0, at(10*time.Second)); got != 10 {
		t.Errorf("rank-0 pod-seconds = %v, want 10 — five reconciles must not charge five pods", got)
	}
	if len(s.transitions) != 0 {
		t.Errorf("transitions = %v, want none — a node that did not move did not transition", s.transitions)
	}
}

// TestRescoringResetsTheAxis is the case Forget exists for: rank 1 meant one
// thing before the edit and another after, so the accumulated seconds are
// discarded rather than averaged across two definitions.
func TestRescoringResetsTheAxis(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("c", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("n1", "c", "n2", "2"), t0)
	s.UpsertPod(pod("p1", "n1", corev1.PodRunning), t0)

	if got := podSeconds(t, s, "c", 2, at(50*time.Second)); got != 50 {
		t.Fatalf("pre-edit rank-2 pod-seconds = %v, want 50", got)
	}

	// Re-score so that index 2 is now the best tier.
	s.UpsertClass("c", spec(t, s2ScoredSpec), at(50*time.Second))

	if got := podSeconds(t, s, "c", 2, at(60*time.Second)); got != 0 {
		t.Errorf("rank-2 pod-seconds after the edit = %v, want 0 — the old tiers are gone", got)
	}
	if got := podSeconds(t, s, "c", 0, at(60*time.Second)); got != 10 {
		t.Errorf("rank-0 pod-seconds after the edit = %v, want 10 — the pod re-enters at its new tier", got)
	}
	if n := underflows(s); n != 0 {
		t.Errorf("underflows = %d, want 0 — a reset must not leave the pods to be left twice", n)
	}
}

// TestEditThatDoesNotChangeTheSpecKeepsTheTime. A ComputeClass is rewritten by
// controllers and resynced by the informer constantly; only a spec change is a
// re-tiering, and resetting on every update would make the primary counter
// permanently near zero.
func TestEditThatDoesNotChangeTheSpecKeepsTheTime(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("c", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("n1", "c", "n4", "0"), t0)
	s.UpsertPod(pod("p1", "n1", corev1.PodRunning), t0)
	s.UpsertClass("c", spec(t, n4PreferredSpec), at(30*time.Second))

	if got := podSeconds(t, s, "c", 0, at(60*time.Second)); got != 60 {
		t.Errorf("rank-0 pod-seconds = %v, want 60 — an unchanged spec is not an edit", got)
	}
}

// TestUndecodableClassUnranksItsNodes. A half-read axis is worse than no axis,
// so the refusal has to reach the accounting and not just the log.
func TestUndecodableClassUnranksItsNodes(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("c", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("n1", "c", "n4", "0"), t0)
	s.UpsertPod(pod("p1", "n1", corev1.PodRunning), t0)

	s.UpsertClass("c", map[string]any{"priorities": "not a list"}, at(20*time.Second))

	if got := podSeconds(t, s, "c", 0, at(60*time.Second)); got != 0 {
		t.Errorf("rank-0 pod-seconds = %v, want 0 — the axis was discarded with the spec", got)
	}
	if n := s.decodeFails["c"]; n != 1 {
		t.Errorf("decode failures = %d, want 1", n)
	}
	if ns := s.nodes["n1"]; ns.known {
		t.Error("node still ranked against a class that did not decode")
	}
	if n := underflows(s); n != 0 {
		t.Errorf("underflows = %d, want 0", n)
	}
}

// TestUndecodableClassOnFirstSightIsNotAReset covers the path where the very
// first version of a class is broken — there is no previous axis to discard,
// and the error bookkeeping still has to happen.
func TestUndecodableClassOnFirstSightIsNotAReset(t *testing.T) {
	s := newTestSource(t)
	s.UpsertNode(node("n1", "c", "n4", "0"), t0)
	s.UpsertClass("c", map[string]any{"priorities": []any{"not an object"}}, t0)

	if n := s.decodeFails["c"]; n != 1 {
		t.Errorf("decode failures = %d, want 1", n)
	}
	if _, ok := s.classes["c"]; ok {
		t.Error("a class that did not decode was kept")
	}
}

// TestClassDeletionUnchargesEverything.
func TestClassDeletionUnchargesEverything(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("c", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("n1", "c", "n4", "0"), t0)
	s.UpsertPod(pod("p1", "n1", corev1.PodRunning), t0)
	s.DeleteClass("c", at(30*time.Second))

	if got := podSeconds(t, s, "c", 0, at(90*time.Second)); got != 0 {
		t.Errorf("rank-0 pod-seconds = %v, want 0 after the class was deleted", got)
	}
	if n := underflows(s); n != 0 {
		t.Errorf("underflows = %d, want 0", n)
	}
	// Deleting a class we never had is a no-op, not a panic.
	s.DeleteClass("never-existed", at(40*time.Second))
}

// TestNodeDeletionUnchargesItsPods. The pods' own deletes may lag by minutes,
// and a pod on a node that is gone is not occupying a rank.
func TestNodeDeletionUnchargesItsPods(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("c", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("n1", "c", "n4", "0"), t0)
	s.UpsertPod(pod("p1", "n1", corev1.PodRunning), t0)
	s.DeleteNode("n1", at(20*time.Second))

	if got := podSeconds(t, s, "c", 0, at(80*time.Second)); got != 20 {
		t.Errorf("rank-0 pod-seconds = %v, want 20 — accrual stops when the node goes", got)
	}
	if n := underflows(s); n != 0 {
		t.Errorf("underflows = %d, want 0", n)
	}
	s.DeleteNode("never-existed", at(30*time.Second))
}

// TestOnlyOccupyingPodsAreCounted pins the occupancy predicate. A Pending
// unscheduled pod got nothing and must not land in the same bucket as one that
// got its first choice; a Succeeded pod stopped occupying.
func TestOnlyOccupyingPodsAreCounted(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("c", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("n1", "c", "n4", "0"), t0)

	s.UpsertPod(pod("unscheduled", "", corev1.PodPending), t0)
	s.UpsertPod(pod("running", "n1", corev1.PodRunning), t0)
	s.UpsertPod(pod("binding", "n1", corev1.PodPending), t0)

	if got := podSeconds(t, s, "c", 0, at(10*time.Second)); got != 20 {
		t.Errorf("rank-0 pod-seconds = %v, want 20 — the bound Pending pod occupies, the unbound one does not", got)
	}

	// Completion stops the clock without a delete event.
	s.UpsertPod(pod("running", "n1", corev1.PodSucceeded), at(10*time.Second))
	if got := podSeconds(t, s, "c", 0, at(20*time.Second)); got != 30 {
		t.Errorf("rank-0 pod-seconds = %v, want 30 — only the still-Pending pod accrues after the other completed", got)
	}
	if n := underflows(s); n != 0 {
		t.Errorf("underflows = %d, want 0", n)
	}
}

// TestPodDeletionStopsTheClock, including the repeated-delete case an informer
// resync produces.
func TestPodDeletionStopsTheClock(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("c", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("n1", "c", "n4", "0"), t0)
	s.UpsertPod(pod("p1", "n1", corev1.PodRunning), t0)
	s.DeletePod(pod("p1", "n1", corev1.PodRunning), at(15*time.Second))
	s.DeletePod(pod("p1", "n1", corev1.PodRunning), at(16*time.Second))

	if got := podSeconds(t, s, "c", 0, at(60*time.Second)); got != 15 {
		t.Errorf("rank-0 pod-seconds = %v, want 15", got)
	}
	if n := underflows(s); n != 0 {
		t.Errorf("underflows = %d, want 0 — a repeated delete is not an imbalance", n)
	}
}

// TestPodRebindingIsTreatedAsANewPod. A pod is bound once and never rebound,
// so a name that reappears on another node is a slot reused after a delete we
// missed — and charging it to both nodes would double-count.
func TestPodRebindingIsTreatedAsANewPod(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("c", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("n1", "c", "n4", "0"), t0)
	s.UpsertNode(node("n2", "c", "c3", "1"), t0)
	s.UpsertPod(pod("p1", "n1", corev1.PodRunning), t0)
	s.UpsertPod(pod("p1", "n2", corev1.PodRunning), at(10*time.Second))

	end := at(30 * time.Second)
	if got := podSeconds(t, s, "c", 0, end); got != 10 {
		t.Errorf("rank-0 pod-seconds = %v, want 10", got)
	}
	if got := podSeconds(t, s, "c", 1, end); got != 20 {
		t.Errorf("rank-1 pod-seconds = %v, want 20", got)
	}
	if n := underflows(s); n != 0 {
		t.Errorf("underflows = %d, want 0", n)
	}
}

// TestNodeOutsideEveryClassContributesNothing. Most nodes in a GKE cluster
// carry no compute class at all, and they must not become an axis.
func TestNodeOutsideEveryClassContributesNothing(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("c", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("plain", "", "n4", ""), t0)
	s.UpsertPod(pod("p1", "plain", corev1.PodRunning), t0)
	s.tracker.Flush(at(60 * time.Second))
	if got := s.tracker.Snapshot().Ranks; len(got) != 0 {
		t.Errorf("buckets = %v, want none", got)
	}
}

// TestSentinelsReadAsRanksNotErrors. GKE's two non-integer annotation values
// are findings, and their time is accumulated like any other rank so that "we
// spent a week off-axis" is answerable.
func TestSentinelsReadAsRanksNotErrors(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("c", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("unfit", "c", "e2", "ccc_no_rule_matching"), t0)
	s.UpsertNode(node("outside", "c", "e2", "ccc_scale_up_anyway"), t0)
	s.UpsertPod(pod("pu", "unfit", corev1.PodRunning), t0)
	s.UpsertPod(pod("po", "outside", corev1.PodRunning), t0)

	end := at(30 * time.Second)
	if got := podSeconds(t, s, "c", leeway.RankUnsatisfiable, end); got != 30 {
		t.Errorf("unsatisfiable pod-seconds = %v, want 30", got)
	}
	if got := podSeconds(t, s, "c", leeway.RankOffAxis, end); got != 30 {
		t.Errorf("off-axis pod-seconds = %v, want 30", got)
	}
	// Neither counts towards mean achieved rank: a mean over rank -2 is not a
	// mean of anything.
	for _, a := range s.tracker.Snapshot().Axes {
		if a.TierPodSeconds != 0 {
			t.Errorf("tier pod-seconds = %v, want 0 — both nodes are off the tiers", a.TierPodSeconds)
		}
	}
}

// TestAbsentAnnotationIsPendingNotRankZero is S1's first 44 seconds, and the
// single most dangerous default in the whole subsystem: a node whose
// annotation has not landed is unknown, never rank 0.
func TestAbsentAnnotationIsPendingNotRankZero(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("c", spec(t, n4PreferredSpec), t0)
	// A machine family no rule names, so inference has no opinion either.
	s.UpsertNode(node("fresh", "c", "e2", ""), t0)
	s.UpsertPod(pod("p1", "fresh", corev1.PodRunning), t0)

	end := at(40 * time.Second)
	if got := podSeconds(t, s, "c", 0, end); got != 0 {
		t.Errorf("rank-0 pod-seconds = %v, want 0", got)
	}
	if got := podSeconds(t, s, "c", leeway.RankUnknown, end); got != 40 {
		t.Errorf("unknown pod-seconds = %v, want 40", got)
	}
	if got := s.nodes["fresh"].place.Outcome; got != leeway.OutcomePending {
		t.Errorf("outcome = %v, want pending", got)
	}
}

// TestInferenceAnswersWhenTheAnnotationHasNot. The annotation is primary, but
// during the first minute there is none — and the node's own labels already
// say which rule admitted it.
func TestInferenceAnswersWhenTheAnnotationHasNot(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("c", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("fresh", "c", "c3", ""), t0)

	got := s.nodes["fresh"].place
	if got.Rank != 1 || got.Source != leeway.SourceInferred {
		t.Errorf("placement = rank %v from %v, want rank 1 inferred", got.Rank, got.Source)
	}
}

// TestDisagreementIsRecordedAndTheAnnotationStillWins.
func TestDisagreementIsRecordedAndTheAnnotationStillWins(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("c", spec(t, n4PreferredSpec), t0)
	// Labels say c3 (rule 1); GKE says index 0.
	s.UpsertNode(node("n1", "c", "c3", "0"), t0)

	got := s.nodes["n1"].place
	if got.Rank != 0 || got.Source != leeway.SourceNodeAnnotation {
		t.Errorf("placement = rank %v from %v, want rank 0 from the annotation", got.Rank, got.Source)
	}
	if !got.Disagreed {
		t.Error("disagreement not recorded — the cross-check is the whole point of inferring at all")
	}
}

// TestInferenceOffStopsTheCrossCheck is the escape hatch: with Infer off the
// annotation path is unchanged and nothing claims a disagreement.
func TestInferenceOffStopsTheCrossCheck(t *testing.T) {
	off := false
	cfg := DefaultConfig()
	cfg.Infer = &off
	s, err := New(fake.NewSimpleClientset(), nil, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.logf = func(string, ...any) {}
	s.UpsertClass("c", spec(t, n4PreferredSpec), t0)
	s.UpsertNode(node("n1", "c", "c3", "0"), t0)

	got := s.nodes["n1"].place
	if got.Rank != 0 {
		t.Errorf("rank = %v, want 0", got.Rank)
	}
	if got.Disagreed || got.Unmatched {
		t.Errorf("placement = %+v, want no cross-check flags with inference off", got)
	}
}

// TestChangingClassIsNotATransition. A node relabelled onto a different class
// did not move within a preference order — it left one — and counting that as
// a fallback would put a cross-axis move in the degradation numbers.
func TestChangingClassIsNotATransition(t *testing.T) {
	s := newTestSource(t)
	s.UpsertClass("a", spec(t, n4PreferredSpec), t0)
	s.UpsertClass("b", spec(t, s2ScoredSpec), t0)
	s.UpsertNode(node("n1", "a", "n2", "2"), t0)
	s.UpsertNode(node("n1", "b", "n2", "0"), at(10*time.Second))

	if len(s.transitions) != 0 {
		t.Errorf("transitions = %v, want none across a class change", s.transitions)
	}
}

// TestNewRejectsABadProfileConfig. The extractor compiles a regex, and a
// source that silently ran with a broken one would infer nothing and report
// every node unmatched.
func TestNewRejectsABadProfileConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Profile.MachineFamilyPattern = "^([a-z"
	if _, err := New(fake.NewSimpleClientset(), nil, cfg); err == nil {
		t.Fatal("New accepted an uncompilable machine-family pattern")
	}
}

func TestConfigNormalizeFillsTheDefaults(t *testing.T) {
	got := Config{}.normalize()
	want := DefaultConfig()
	if got.ClassLabel != want.ClassLabel {
		t.Errorf("ClassLabel = %q, want %q", got.ClassLabel, want.ClassLabel)
	}
	if got.PriorityIndexAnnotation != want.PriorityIndexAnnotation {
		t.Errorf("PriorityIndexAnnotation = %q, want %q", got.PriorityIndexAnnotation, want.PriorityIndexAnnotation)
	}
	if got.FlushInterval != DefaultFlushInterval {
		t.Errorf("FlushInterval = %v, want %v", got.FlushInterval, DefaultFlushInterval)
	}
	if got.Profile.InstanceTypeLabel == "" {
		t.Error("Profile left empty")
	}
	if !got.infer() {
		t.Error("inference defaulted off")
	}

	// A caller who set anything keeps everything they set.
	custom := Config{ClassLabel: "x", PriorityIndexAnnotation: "y", FlushInterval: time.Minute}.normalize()
	if custom.ClassLabel != "x" || custom.PriorityIndexAnnotation != "y" || custom.FlushInterval != time.Minute {
		t.Errorf("normalize overwrote explicit config: %+v", custom)
	}
}
