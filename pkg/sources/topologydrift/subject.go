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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// OwnerLookup resolves the controlling owner reference of a namespaced object,
// reporting false when the object is not found.
//
// It is a function rather than a lister so that this package's counting logic
// can be exercised without a clientset, and so the wiring can satisfy it from
// whichever of pkg/graph's OwnerChain or a plain lister is available — the
// graph only exists under --storm, so leeway cannot depend on it (§6.2).
//
// Only ReplicaSet is ever looked up today; see resolveSubject.
type OwnerLookup func(namespace, kind, name string) (*metav1.OwnerReference, bool)

// resolveSubject walks a pod's owner chain to the thing whose distribution we
// track, reporting false for a pod that has no such subject.
//
// The walk is at most one hop past the pod, and deliberately so. A pod owned by
// a ReplicaSet is really a replica of the Deployment above it — scoring the
// ReplicaSet would split one workload's distribution across the old and new
// ReplicaSets mid-rollout and report drift that is just a deployment in
// progress. Every other controller is its own subject: a Job's pods belong to
// the Job and not to a CronJob above it, because each run is scheduled
// independently and the CronJob has no replicas of its own to spread.
//
// A pod whose chain does not end in a supported kind is not counted at all —
// a bare pod, or one owned by a CRD. That is the honest answer rather than a
// gap: leeway scores a distribution against an *intent*, and a pod nobody
// declared has no intent to compare against. Tracking it would mean inventing
// a subject and then reporting drift no one can act on.
func resolveSubject(pod *corev1.Pod, lookup OwnerLookup) (leeway.SubjectRef, bool) {
	ctrl := controllerOf(pod.OwnerReferences)
	if ctrl == nil {
		return leeway.SubjectRef{}, false
	}

	switch ctrl.Kind {
	case "StatefulSet", "DaemonSet", "Job":
		return leeway.SubjectRef{
			Kind:      leeway.SubjectKind(ctrl.Kind),
			Namespace: pod.Namespace,
			Name:      ctrl.Name,
		}, true

	case "ReplicaSet":
		if lookup == nil {
			return leeway.SubjectRef{}, false
		}
		owner, ok := lookup(pod.Namespace, "ReplicaSet", ctrl.Name)
		if !ok || owner == nil || owner.Kind != "Deployment" {
			// A hand-rolled ReplicaSet with no Deployment above it. Not a
			// subject, for the same reason a bare pod is not: there is no
			// declared shape to compare a distribution against.
			return leeway.SubjectRef{}, false
		}
		return leeway.SubjectRef{
			Kind:      leeway.SubjectDeployment,
			Namespace: pod.Namespace,
			Name:      owner.Name,
		}, true
	}

	return leeway.SubjectRef{}, false
}

// controllerOf returns the owner reference with Controller set, or nil.
func controllerOf(refs []metav1.OwnerReference) *metav1.OwnerReference {
	for i := range refs {
		if refs[i].Controller != nil && *refs[i].Controller {
			return &refs[i]
		}
	}
	return nil
}
