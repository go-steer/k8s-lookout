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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	appsv1informers "k8s.io/client-go/informers/apps/v1"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
)

// NewReplicaSetOwners backs ReplicaSetOwners with a ReplicaSet informer
// — on the shared factory, the one the rollout source, topology-drift
// and the storm graph already run. Asking for it registers the informer;
// the factory's next Start runs it.
func NewReplicaSetOwners(inf appsv1informers.ReplicaSetInformer) ReplicaSetOwners {
	return listerRSOwners{synced: inf.Informer().HasSynced, lister: inf.Lister()}
}

type listerRSOwners struct {
	synced func() bool
	lister appsv1listers.ReplicaSetLister
}

func (l listerRSOwners) HasSynced() bool { return l.synced() }

// DeploymentOf reads the ReplicaSet's controller ownerReference; only
// an apps/Deployment counts.
func (l listerRSOwners) DeploymentOf(namespace, name string) (string, bool) {
	rs, err := l.lister.ReplicaSets(namespace).Get(name)
	if err != nil {
		return "", false
	}
	ref := metav1.GetControllerOf(rs)
	if ref == nil || ref.Kind != "Deployment" || ref.Name == "" {
		return "", false
	}
	if gv, err := schema.ParseGroupVersion(ref.APIVersion); err != nil || gv.Group != "apps" {
		return "", false
	}
	return ref.Name, true
}
