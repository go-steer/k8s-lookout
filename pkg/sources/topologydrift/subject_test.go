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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// ownedBy sets a controller owner reference.
func ownedBy(kind, name string) podOpt {
	return func(p *corev1.Pod) {
		yes := true
		p.OwnerReferences = append(p.OwnerReferences, metav1.OwnerReference{
			Kind:       kind,
			Name:       name,
			Controller: &yes,
		})
	}
}

// staticOwners is an OwnerLookup over a fixed namespace/kind/name → owner-kind,
// owner-name table.
func staticOwners(table map[string][2]string) OwnerLookup {
	return func(namespace, kind, name string) (*metav1.OwnerReference, bool) {
		owner, ok := table[namespace+"/"+kind+"/"+name]
		if !ok {
			return nil, false
		}
		return &metav1.OwnerReference{Kind: owner[0], Name: owner[1]}, true
	}
}

func TestResolveSubject(t *testing.T) {
	owners := staticOwners(map[string][2]string{
		"prod/ReplicaSet/web-7c9f":   {"Deployment", "web"},
		"prod/ReplicaSet/orphan-rs":  {"", ""},
		"prod/ReplicaSet/crd-owned":  {"FooSet", "foo"},
		"prod/StatefulSet/db-owner":  {"Deployment", "nonsense"},
		"prod/ReplicaSet/no-owner-k": {"", "x"},
	})

	tests := []struct {
		name string
		opts []podOpt
		want leeway.SubjectRef
		ok   bool
	}{
		{
			// The one hop that exists: a pod is a replica of the Deployment,
			// not of whichever ReplicaSet happens to hold it this minute.
			"pod under a ReplicaSet resolves to its Deployment",
			[]podOpt{ownedBy("ReplicaSet", "web-7c9f")},
			leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "web"},
			true,
		},
		{
			"StatefulSet is its own subject",
			[]podOpt{ownedBy("StatefulSet", "db")},
			leeway.SubjectRef{Kind: leeway.SubjectStatefulSet, Namespace: "prod", Name: "db"},
			true,
		},
		{
			"DaemonSet is its own subject",
			[]podOpt{ownedBy("DaemonSet", "fluentd")},
			leeway.SubjectRef{Kind: leeway.SubjectDaemonSet, Namespace: "prod", Name: "fluentd"},
			true,
		},
		{
			// Stops at the Job rather than climbing to a CronJob: each run is
			// scheduled independently, and the CronJob has no replicas of its
			// own to spread.
			"Job is its own subject and the walk stops there",
			[]podOpt{ownedBy("Job", "nightly-28471")},
			leeway.SubjectRef{Kind: leeway.SubjectJob, Namespace: "prod", Name: "nightly-28471"},
			true,
		},
		{
			"a bare pod has no subject",
			nil,
			leeway.SubjectRef{},
			false,
		},
		{
			"a pod owned by a CRD has no subject",
			[]podOpt{ownedBy("FooSet", "foo-1")},
			leeway.SubjectRef{},
			false,
		},
		{
			"a hand-rolled ReplicaSet with no Deployment has no subject",
			[]podOpt{ownedBy("ReplicaSet", "orphan-rs")},
			leeway.SubjectRef{},
			false,
		},
		{
			"a ReplicaSet owned by something that is not a Deployment has no subject",
			[]podOpt{ownedBy("ReplicaSet", "crd-owned")},
			leeway.SubjectRef{},
			false,
		},
		{
			// The ReplicaSet is not in the lookup's cache yet. Reporting no
			// subject means the pod goes uncounted and is retried on its next
			// event, which is right: inventing a subject now would need
			// unpicking later.
			"an unknown ReplicaSet has no subject yet",
			[]podOpt{ownedBy("ReplicaSet", "not-in-cache")},
			leeway.SubjectRef{},
			false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := resolveSubject(pod("p", "prod", "", tc.opts...), owners)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Errorf("subject = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestResolveSubject_NonControllerOwnerIsIgnored(t *testing.T) {
	// A non-controller ownerRef is a reference, not a parent — an arbitrary
	// object can add one. Following it would attribute a workload's pods to
	// whatever happened to point at them.
	p := pod("p", "prod", "")
	p.OwnerReferences = []metav1.OwnerReference{{Kind: "DaemonSet", Name: "not-the-controller"}}

	if _, ok := resolveSubject(p, nil); ok {
		t.Error("a non-controller owner reference was followed")
	}
}

func TestResolveSubject_NilLookup(t *testing.T) {
	// Without a lookup the Deployment hop cannot be made, so the pod is
	// uncounted rather than attributed to its ReplicaSet.
	if _, ok := resolveSubject(pod("p", "prod", "", ownedBy("ReplicaSet", "web-7c9f")), nil); ok {
		t.Error("a ReplicaSet resolved to a subject with no owner lookup")
	}
	// Controllers that need no hop still resolve.
	if _, ok := resolveSubject(pod("p", "prod", "", ownedBy("DaemonSet", "fluentd")), nil); !ok {
		t.Error("a DaemonSet needed an owner lookup")
	}
}
