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

package topologydrift

import (
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// disruptedAtTaint is Karpenter's shape: a disruption taint with the API
// server's TimeAdded and a reason for a value.
func disruptedAtTaint(at time.Time) nodeOpt {
	return func(n *corev1.Node) {
		stamp := metav1.NewTime(at)
		n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{
			Key:       "karpenter.sh/disrupted",
			Value:     "underutilized",
			Effect:    corev1.TaintEffectNoSchedule,
			TimeAdded: &stamp,
		})
	}
}

// caDeletionTaint is cluster-autoscaler's shape: no TimeAdded, and the
// deletion time written into the value as unix seconds.
func caDeletionTaint(at time.Time) nodeOpt {
	return func(n *corev1.Node) {
		n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{
			Key:    "ToBeDeletedByClusterAutoscaler",
			Value:  strconv.FormatInt(at.Unix(), 10),
			Effect: corev1.TaintEffectNoSchedule,
		})
	}
}

// taint applies an arbitrary NoSchedule taint.
func taint(key, value string) nodeOpt {
	return func(n *corev1.Node) {
		n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{
			Key: key, Value: value, Effect: corev1.TaintEffectNoSchedule,
		})
	}
}

const (
	zoneA = leeway.Domain("us-central1-a")
	zoneB = leeway.Domain("us-central1-b")
)

// The whole mechanism in one pass: a node the autoscaler claims and then
// removes leaves a stamp on its domain, and only on its domain.
func TestInventory_ARemovedClaimedNodeIsAConsolidation(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)

	claimedAt := t0.Add(-6 * time.Minute)
	inv.Upsert(node("n-a1", string(zoneA)))
	inv.Upsert(node("n-a1", string(zoneA), disruptedAtTaint(claimedAt)))
	inv.Remove("n-a1")

	got := inv.ConsolidationTimes(zoneKey, []leeway.Domain{zoneA, zoneB})
	if len(got) != 1 {
		t.Fatalf("ConsolidationTimes = %v, want only the zone that lost a node", got)
	}
	if !got[zoneA].Equal(claimedAt) {
		t.Errorf("zone a = %v, want the taint's stamp %v", got[zoneA], claimedAt)
	}
}

// The distinction the rung exists for. A node an operator drained and then
// deleted is not a consolidation — it is a drain, and §8.5 has a different
// rung and a different remedy for that.
func TestInventory_ACordonedNodeIsNotAConsolidation(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)

	inv.Upsert(node("n-a1", string(zoneA)))
	inv.Upsert(node("n-a1", string(zoneA), cordonedAtTaint(t0.Add(-time.Minute))))
	inv.Remove("n-a1")

	if got := inv.ConsolidationTimes(zoneKey, []leeway.Domain{zoneA}); len(got) != 0 {
		t.Errorf("ConsolidationTimes = %v after a plain drain, want nothing", got)
	}
}

// A node that simply vanished — a preemption, a repair, an operator with a
// cluster-level tool — is a capacity shortfall, not a consolidation. The
// ready-count fall is what reports it, and that rung ranks below this one.
func TestInventory_AnUnclaimedNodeVanishingIsNotAConsolidation(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)

	inv.Upsert(node("n-a1", string(zoneA)))
	inv.Remove("n-a1")

	if got := inv.ConsolidationTimes(zoneKey, []leeway.Domain{zoneA}); len(got) != 0 {
		t.Errorf("ConsolidationTimes = %v for a node nobody claimed, want nothing", got)
	}
}

// Candidacy is not a decision. Cluster-autoscaler marks and unmarks deletion
// candidates as utilisation moves, and counting one would attribute drift to a
// consolidation that never happened.
func TestInventory_ADeletionCandidateIsNotAConsolidation(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)

	inv.Upsert(node("n-a1", string(zoneA)))
	inv.Upsert(node("n-a1", string(zoneA), taint("DeletionCandidateOfClusterAutoscaler", "1758500000")))
	inv.Remove("n-a1")

	if got := inv.ConsolidationTimes(zoneKey, []leeway.Domain{zoneA}); len(got) != 0 {
		t.Errorf("ConsolidationTimes = %v for a mere candidate, want nothing", got)
	}
}

// An autoscaler that changes its mind un-taints the node, and a node that
// outlived the claim on it was not consolidated. This is where the stamp
// differs from cordonedAt, which deliberately survives an uncordon.
func TestInventory_AWithdrawnClaimLeavesNoStamp(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)

	inv.Upsert(node("n-a1", string(zoneA)))
	inv.Upsert(node("n-a1", string(zoneA), disruptedAtTaint(t0.Add(-5*time.Minute))))
	inv.Upsert(node("n-a1", string(zoneA))) // claim withdrawn
	inv.Remove("n-a1")

	if got := inv.ConsolidationTimes(zoneKey, []leeway.Domain{zoneA}); len(got) != 0 {
		t.Errorf("ConsolidationTimes = %v after a withdrawn claim, want nothing", got)
	}
}

// Cluster-autoscaler writes no TimeAdded, so the value is the only date there
// is — and because it is on the object, it survives a restart of this process
// where a watched flip would not.
func TestInventory_ClusterAutoscalerDatesItselfInTheTaintValue(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)

	deletedAt := t0.Add(-11 * time.Minute).Truncate(time.Second)
	// First seen already claimed: the restart case, and the one an undated
	// taint cannot answer.
	inv.Upsert(node("n-a1", string(zoneA), caDeletionTaint(deletedAt)))
	inv.Remove("n-a1")

	got := inv.ConsolidationTimes(zoneKey, []leeway.Domain{zoneA})
	if !got[zoneA].Equal(deletedAt) {
		t.Errorf("zone a = %v, want the value's stamp %v", got[zoneA], deletedAt)
	}
}

// A node first seen already claimed and undated gets no stamp, for the reason
// cordonTime gives: a restart must not make every node the autoscaler happens
// to be working on look freshly consolidated.
func TestInventory_AnUndatedClaimSeenFirstAtStartupIsNotDated(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)

	inv.Upsert(node("n-a1", string(zoneA), taint("karpenter.sh/disrupted", "underutilized")))
	inv.Remove("n-a1")

	if got := inv.ConsolidationTimes(zoneKey, []leeway.Domain{zoneA}); len(got) != 0 {
		t.Errorf("ConsolidationTimes = %v at startup, want nothing guessed", got)
	}
}

// An undated claim we watched arrive is dated at the moment we watched it, and
// a later update to the same node does not re-date it: the clock started when
// the autoscaler decided, not when we last heard about the node.
func TestInventory_AnUndatedClaimIsDatedOnceAtTheFlip(t *testing.T) {
	now := t0.Add(-8 * time.Minute)
	inv := fixedInventory(&now, zoneKey)

	inv.Upsert(node("n-a1", string(zoneA)))
	inv.Upsert(node("n-a1", string(zoneA), taint("karpenter.sh/disrupted", "underutilized")))
	flippedAt := now

	now = t0
	inv.Upsert(node("n-a1", string(zoneA), taint("karpenter.sh/disrupted", "underutilized"), notReady()))
	inv.Remove("n-a1")

	got := inv.ConsolidationTimes(zoneKey, []leeway.Domain{zoneA})
	if !got[zoneA].Equal(flippedAt) {
		t.Errorf("zone a = %v, want the flip %v rather than the last update", got[zoneA], flippedAt)
	}
}

// An autoscaler packing forty nodes out of a zone is one answer, not forty.
// The latch keeps the most recent, which is also what bounds the memory by the
// cluster's domain count rather than by its churn.
func TestInventory_ManyRemovalsInADomainKeepTheMostRecent(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)

	first := t0.Add(-40 * time.Minute)
	last := t0.Add(-3 * time.Minute)
	for i, at := range []time.Time{first, last, t0.Add(-20 * time.Minute)} {
		name := "n-a" + strconv.Itoa(i)
		inv.Upsert(node(name, string(zoneA)))
		inv.Upsert(node(name, string(zoneA), disruptedAtTaint(at)))
		inv.Remove(name)
	}

	got := inv.ConsolidationTimes(zoneKey, []leeway.Domain{zoneA})
	if !got[zoneA].Equal(last) {
		t.Errorf("zone a = %v, want the most recent consolidation %v", got[zoneA], last)
	}
}

// The latch has no live object behind it, so something has to forget it, and
// the window it is kept for is the one rule that reads it.
func TestInventory_PruneConsolidationsForgetsOnlyWhatAgedOut(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)

	old := t0.Add(-3 * time.Hour)
	recent := t0.Add(-30 * time.Minute)
	inv.Upsert(node("n-a1", string(zoneA), disruptedAtTaint(old)))
	inv.Remove("n-a1")
	inv.Upsert(node("n-b1", string(zoneB), disruptedAtTaint(recent)))
	inv.Remove("n-b1")

	inv.PruneConsolidations(t0, 2*time.Hour)

	got := inv.ConsolidationTimes(zoneKey, []leeway.Domain{zoneA, zoneB})
	if _, still := got[zoneA]; still {
		t.Errorf("zone a = %v, want a three-hour-old consolidation forgotten", got[zoneA])
	}
	if !got[zoneB].Equal(recent) {
		t.Errorf("zone b = %v, want the consolidation still inside the window", got[zoneB])
	}
}

// A retention of zero means "no opinion", not "forget everything" — the same
// reading SampleReady gives it. A caller that has not configured a window must
// not silently erase the evidence.
func TestInventory_PruneConsolidationsIgnoresAZeroRetention(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)

	at := t0.Add(-time.Minute)
	inv.Upsert(node("n-a1", string(zoneA), disruptedAtTaint(at)))
	inv.Remove("n-a1")

	inv.PruneConsolidations(t0, 0)

	if got := inv.ConsolidationTimes(zoneKey, []leeway.Domain{zoneA}); !got[zoneA].Equal(at) {
		t.Errorf("ConsolidationTimes = %v after a zero-retention prune, want the stamp kept", got)
	}
}

// The same refusals DrainTimes makes, because §8.5 calls the two side by side
// and an axis the inventory does not track is a caller error either way.
func TestInventory_ConsolidationTimesRefusesAnUnknownAxisOrAnEmptySet(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey)

	inv.Upsert(node("n-a1", string(zoneA), disruptedAtTaint(t0)))
	inv.Remove("n-a1")

	if got := inv.ConsolidationTimes("nope/key", []leeway.Domain{zoneA}); got != nil {
		t.Errorf("ConsolidationTimes on an untracked axis = %v, want nil", got)
	}
	if got := inv.ConsolidationTimes(zoneKey, nil); got != nil {
		t.Errorf("ConsolidationTimes over no domains = %v, want nil", got)
	}
}

// One removal stamps every axis the node was on. A cluster watching zone and
// region should not have to lose the region answer because the finding that
// asked first was a zonal one.
func TestInventory_AConsolidationStampsEveryAxis(t *testing.T) {
	now := t0
	inv := fixedInventory(&now, zoneKey, regionKey)

	at := t0.Add(-2 * time.Minute)
	n := node("n-a1", string(zoneA), disruptedAtTaint(at))
	n.Labels[corev1.LabelTopologyRegion] = "us-central1"
	inv.Upsert(n)
	inv.Remove("n-a1")

	if got := inv.ConsolidationTimes(zoneKey, []leeway.Domain{zoneA}); !got[zoneA].Equal(at) {
		t.Errorf("zone axis = %v, want %v", got, at)
	}
	if got := inv.ConsolidationTimes(regionKey, []leeway.Domain{"us-central1"}); !got["us-central1"].Equal(at) {
		t.Errorf("region axis = %v, want %v", got, at)
	}
}
