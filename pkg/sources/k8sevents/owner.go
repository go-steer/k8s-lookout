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

package k8sevents

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
)

// Owner resolution (issue #583): a pod event's ControllerRef is the
// pod's workload, read from the pod and ReplicaSet informer caches the
// sentinel already keeps — never guessed from the pod's name.
//
// The value is the TOP workload owner the caches can prove, as
// "Kind/name" in the pod's namespace: a ReplicaSet-owned pod whose
// ReplicaSet is in turn controlled by a Deployment reports the
// Deployment, because that is the identity a rollout does not change —
// the old and the new ReplicaSet's pods name one owner, which is what
// lets the dispatcher's sibling fold (§7.7 amendment 2026-10-08) join
// them without the storm topology graph. It is also the convention the
// expiry source set for ACME stalls (#542: the Certificate, not the
// Order in between). StatefulSet, DaemonSet and Job pods report their
// controller directly.
//
// Every miss degrades toward LESS, never toward a guess:
//   - pod not in the cache (already deleted, or the cache has not
//     synced yet), or a same-name pod with a different UID: empty;
//   - pod with no controller ownerReference (a bare pod): empty;
//   - ReplicaSet not in the cache, a different UID under that name, or
//     no Deployment above it: the ReplicaSet itself — a true owner, just
//     a narrower one, so pods of different ReplicaSets simply do not
//     fold.
type ownerResolver struct {
	pods corev1listers.PodLister
	// replicaSets is nil when the deployment cannot list ReplicaSets;
	// ReplicaSet-owned pods then report the ReplicaSet.
	replicaSets appsv1listers.ReplicaSetLister
}

// controllerRef returns ev's involved pod's workload owner as
// "Kind/name", or "" when it cannot be proven. Nil-safe: a source
// without owner resolution reports "" for every event, as before.
func (o *ownerResolver) controllerRef(ev *corev1.Event) string {
	obj := ev.InvolvedObject
	if o == nil || o.pods == nil || obj.Kind != "Pod" || obj.Namespace == "" || obj.Name == "" {
		return ""
	}
	pod, err := o.pods.Pods(obj.Namespace).Get(obj.Name)
	if err != nil || (obj.UID != "" && pod.UID != obj.UID) {
		// A same-name pod with another UID is a different pod (a
		// StatefulSet replacement): its owner is not evidence about
		// the one the event names.
		return ""
	}
	ref := metav1.GetControllerOf(pod)
	if ref == nil || ref.Kind == "" || ref.Name == "" {
		return ""
	}
	if ref.Kind == "ReplicaSet" && isApps(ref.APIVersion) && o.replicaSets != nil {
		rs, err := o.replicaSets.ReplicaSets(pod.Namespace).Get(ref.Name)
		if err == nil && rs.UID == ref.UID {
			if up := metav1.GetControllerOf(rs); up != nil && up.Kind == "Deployment" && isApps(up.APIVersion) && up.Name != "" {
				return "Deployment/" + up.Name
			}
		}
	}
	return ref.Kind + "/" + ref.Name
}

// isApps reports whether apiVersion is in the apps group, so a CRD
// that happens to call itself ReplicaSet or Deployment is not walked
// as one.
func isApps(apiVersion string) bool {
	gv, err := schema.ParseGroupVersion(apiVersion)
	return err == nil && gv.Group == "apps"
}
