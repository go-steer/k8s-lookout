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
	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// VolumeLookup resolves one of a pod's claims to the PersistentVolume it is
// bound to, reporting false when the claim is unknown to the cache or is not
// bound yet.
//
// A function rather than the two listers it is satisfied from, for the reason
// OwnerLookup is: the counting and pinning logic is then exercisable without a
// clientset, and the wiring decides where the objects come from.
type VolumeLookup func(namespace, claim string) (*corev1.PersistentVolume, bool)

// VolumePins builds the FR-8 pin predicate: a pod is pinned when a volume it is
// bound to cannot follow it to another domain.
//
// This is the difference between drift a human can act on and drift they
// cannot. A StatefulSet whose replicas each hold a zonal disk is *supposed* to
// look uneven when a zone fills up — the pods cannot be rebalanced without
// destroying data, and paging someone about a distribution they are not allowed
// to change is the kind of finding that teaches a team to ignore the tool.
//
// keys are the axes the inventory scores, and they decide the question asked of
// each volume: not "is this volume constrained" but "is it constrained on an
// axis we are about to report drift on". A PV whose node affinity says
// `kubernetes.io/os=linux` constrains placement and pins nothing, because every
// domain on every scored axis still contains a linux node.
func VolumePins(lookup VolumeLookup, keys []leeway.TopologyKey) PinPredicate {
	if lookup == nil {
		return nil
	}
	pinning := make(map[string]struct{}, len(keys)+1)
	for _, k := range keys {
		pinning[string(k)] = struct{}{}
	}
	// The hostname is always a pinning key, whether or not it is scored. A
	// volume that admits exactly one node admits exactly one domain on every
	// axis there is, so a local or hostPath PV pins the zone axis transitively
	// — and those are the volumes that pin hardest.
	pinning[corev1.LabelHostname] = struct{}{}

	return func(pod *corev1.Pod) bool {
		return volumePinned(pod, lookup, pinning)
	}
}

// volumePinned is VolumePins' body, separated so the tests can state the key
// set directly instead of going through an inventory.
func volumePinned(pod *corev1.Pod, lookup VolumeLookup, pinning map[string]struct{}) bool {
	if pod == nil {
		return false
	}
	// The early exit is the affordability argument. This predicate runs inside
	// resolvePlacement, which runs on *every* pod event including the status
	// churn §6.3's early return exists to absorb — and it runs before that
	// return, because Pinned is part of the placement being compared. The
	// overwhelming majority of pods mount no claim at all, and for them the
	// whole predicate is a walk over a short slice with no map lookup and no
	// cache read.
	for i := range pod.Spec.Volumes {
		claim := claimOf(pod, &pod.Spec.Volumes[i])
		if claim == "" {
			continue
		}
		pv, ok := lookup(pod.Namespace, claim)
		if !ok {
			// Unknown or unbound. Reported as unpinned, which is the reading
			// that holds by the time it matters: a pod whose claim is still
			// unbound has not been scheduled, so it occupies no domain and
			// contributes to no distribution. By the time it is Running the
			// binding exists and the next pod event resolves it.
			continue
		}
		if pvPins(pv, pinning) {
			return true
		}
	}
	return false
}

// claimOf returns the PersistentVolumeClaim a pod volume refers to, or "" for
// one that refers to none.
func claimOf(pod *corev1.Pod, v *corev1.Volume) string {
	switch {
	case v.PersistentVolumeClaim != nil:
		return v.PersistentVolumeClaim.ClaimName
	case v.Ephemeral != nil:
		// A generic ephemeral volume gets a PVC created for it under a name
		// the API fixes as <pod>-<volume>. Counting it is not pedantry: its
		// template can name a zonal StorageClass, which pins the pod exactly
		// as hard as a StatefulSet's own claim does.
		return pod.Name + "-" + v.Name
	default:
		return ""
	}
}

// pvPins reports whether a bound volume's node affinity confines the pod on an
// axis worth scoring.
//
// Two imprecisions are deliberate, and both are recorded here because they fail
// in opposite directions:
//
// A PV constrained on a key that is neither scored nor the hostname — a rack
// label, say — reads as unpinned, although it may confine the pod to one zone
// in practice. That under-reports pinning, which shows up as a finding a human
// can dismiss rather than as a finding they never see.
//
// A PV whose affinity enumerates *every* domain on a scored axis reads as
// pinned although it confines nothing. That over-reports, which is the silent
// direction, and it is tolerated only because no provisioner emits such a
// volume: a zonal disk names its one zone and a regional disk names its two.
func pvPins(pv *corev1.PersistentVolume, pinning map[string]struct{}) bool {
	if pv == nil || pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return false
	}
	terms := pv.Spec.NodeAffinity.Required.NodeSelectorTerms
	if len(terms) == 0 {
		return false
	}
	// Terms are ORed, so the pod is pinned only if there is no term that would
	// let it out. One unconstrained term — or one constrained on a key nobody
	// scores — is an escape route, and a volume with an escape route is a
	// volume the pod can rebalance around.
	for i := range terms {
		if !termPins(&terms[i], pinning) {
			return false
		}
	}
	return true
}

// termPins reports whether one ORed term of a node selector confines placement.
func termPins(term *corev1.NodeSelectorTerm, pinning map[string]struct{}) bool {
	// matchFields on a node selector is metadata.name and nothing else, so a
	// term carrying one admits a single node by definition.
	if len(term.MatchFields) > 0 {
		return true
	}
	for _, req := range term.MatchExpressions {
		if _, ok := pinning[req.Key]; !ok {
			continue
		}
		// Exists is the one operator that names a pinning key without
		// narrowing anything: every node in every domain on that axis carries
		// the label, which is what makes it an axis.
		if req.Operator == corev1.NodeSelectorOpExists {
			continue
		}
		return true
	}
	return false
}
