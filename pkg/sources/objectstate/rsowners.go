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
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	appsv1informers "k8s.io/client-go/informers/apps/v1"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
)

// NewReplicaSetOwners backs ReplicaSetOwners with a ReplicaSet informer
// — on the shared factory, the one the rollout source, topology-drift
// and the storm graph already run. Asking for it registers the informer;
// the factory's next Start runs it.
//
// deployments, when non-nil, is the Deployment informer (object-state
// and rollout run it on the same factory): a ReplicaSet then belongs to
// Deployment X only if its controller ownerReference carries the LIVE
// X's UID, so the ReplicaSets of a deleted-and-recreated X's old
// incarnation, still waiting for garbage collection, are not X's
// (#601). nil (no deployments grant) keeps the name match.
func NewReplicaSetOwners(rs appsv1informers.ReplicaSetInformer, deployments appsv1informers.DeploymentInformer) ReplicaSetOwners {
	l := listerRSOwners{synced: []func() bool{rs.Informer().HasSynced}, lister: rs.Lister()}
	if deployments != nil {
		l.synced = append(l.synced, deployments.Informer().HasSynced)
		l.deployments = deployments.Lister()
	}
	return l
}

type listerRSOwners struct {
	synced      []func() bool
	lister      appsv1listers.ReplicaSetLister
	deployments appsv1listers.DeploymentLister // nil: match by name
}

// HasSynced reports whether every backing cache has listed: an
// unsynced Deployment cache would make every ReplicaSet look orphaned.
func (l listerRSOwners) HasSynced() bool {
	for _, s := range l.synced {
		if !s() {
			return false
		}
	}
	return true
}

// DeploymentOf reads the ReplicaSet's controller ownerReference; only
// an apps/Deployment counts, and — with the Deployment cache — only
// the live incarnation of it.
func (l listerRSOwners) DeploymentOf(namespace, name string) (string, bool) {
	rs, err := l.lister.ReplicaSets(namespace).Get(name)
	if err != nil {
		return "", false
	}
	return l.controller(rs)
}

// controller is rs's controlling Deployment's name; ok=false when it
// has none or, with the Deployment cache, the Deployment it names is
// gone or is another incarnation (UID mismatch).
func (l listerRSOwners) controller(rs *appsv1.ReplicaSet) (string, bool) {
	name, uid, ok := deploymentRefOf(rs)
	if !ok {
		return "", false
	}
	if l.deployments == nil {
		return name, true
	}
	d, err := l.deployments.Deployments(rs.Namespace).Get(name)
	if err != nil || d.UID != uid {
		return "", false
	}
	return name, true
}

// revisionAnnotation is the Deployment controller's revision stamp on
// each ReplicaSet it owns; the highest is the current template.
const revisionAnnotation = "deployment.kubernetes.io/revision"

// CurrentReplicaSet picks the Deployment's current ReplicaSet among the
// ones it controls: the unique highest revision annotation; when
// revisions are unusable — any missing or unparseable, or a tie at the
// top — the unique newest creationTimestamp instead (#600: the
// Deployment controller creates a new ReplicaSet for every new
// template, so the newest is the current one when nothing better says
// so). Unknown only when that ties too: a guess would let the wrong
// ReplicaSet's Ready pods vouch.
func (l listerRSOwners) CurrentReplicaSet(namespace, deployment string) (string, bool) {
	rss, err := l.lister.ReplicaSets(namespace).List(labels.Everything())
	if err != nil {
		return "", false
	}
	var owned []*appsv1.ReplicaSet
	for _, rs := range rss {
		if dep, ok := l.controller(rs); ok && dep == deployment {
			owned = append(owned, rs)
		}
	}
	if name, ok := byRevision(owned); ok {
		return name, true
	}
	return byCreation(owned)
}

// byRevision is the unique highest revision; ok=false when any
// revision is missing or unparseable, or the top ties.
func byRevision(rss []*appsv1.ReplicaSet) (string, bool) {
	best, bestRev, tie := "", int64(-1), false
	for _, rs := range rss {
		rev, err := strconv.ParseInt(rs.Annotations[revisionAnnotation], 10, 64)
		if err != nil || rev < 0 {
			return "", false
		}
		switch {
		case rev > bestRev:
			best, bestRev, tie = rs.Name, rev, false
		case rev == bestRev:
			tie = true
		}
	}
	return best, best != "" && !tie
}

// byCreation is the unique newest creationTimestamp; ok=false when the
// newest ties (the timestamp has one-second resolution).
func byCreation(rss []*appsv1.ReplicaSet) (string, bool) {
	var best *appsv1.ReplicaSet
	tie := false
	for _, rs := range rss {
		switch {
		case best == nil || best.CreationTimestamp.Before(&rs.CreationTimestamp):
			best, tie = rs, false
		case rs.CreationTimestamp.Equal(&best.CreationTimestamp):
			tie = true
		}
	}
	if best == nil || tie {
		return "", false
	}
	return best.Name, true
}

// deploymentRefOf reads rs's controller ownerReference; only an
// apps/Deployment counts.
func deploymentRefOf(rs *appsv1.ReplicaSet) (string, types.UID, bool) {
	ref := metav1.GetControllerOf(rs)
	if ref == nil || ref.Kind != "Deployment" || ref.Name == "" {
		return "", "", false
	}
	if gv, err := schema.ParseGroupVersion(ref.APIVersion); err != nil || gv.Group != "apps" {
		return "", "", false
	}
	return ref.Name, ref.UID, true
}
