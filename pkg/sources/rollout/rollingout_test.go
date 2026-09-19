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

package rollout

import (
	"slices"
	"testing"
)

// rollingOutNames is RollingOut's answer as sorted "Kind/namespace/name"
// strings, so a test can state the whole expected set in one line.
func rollingOutNames(s *Source) []string {
	var out []string
	for _, w := range s.RollingOut() {
		out = append(out, w.Kind+"/"+w.Namespace+"/"+w.Name)
	}
	slices.Sort(out)
	return out
}

// settled is a Deployment that finished: the controller has caught up, every
// replica is on the new template, and every one of them is available.
func settled(uid, name string) func(*Source) {
	return func(s *Source) {
		s.onReplicaSet(replicaSet("rs-"+uid, "prod", name+"-a1", uid, "1", 3, 3))
		s.onDeployment(deployment(uid, "prod", name, 3, 3, 3, 3))
	}
}

func TestRollingOut_ASettledDeploymentIsNotRollingOut(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	settled("d1", "web")(s)

	if got := rollingOutNames(s); len(got) != 0 {
		t.Errorf("RollingOut() = %v, want nothing", got)
	}
}

// The distinction from deploymentComplete, and the reason RollingOut does not
// reuse it. A Deployment whose pod crash-loops forever is never *complete* —
// AvailableReplicas stays below spec.replicas indefinitely — but nothing is
// moving, so leeway must not relax its placement thresholds for the rest of
// time on the strength of it.
func TestRollingOut_APermanentlyUnavailableDeploymentIsNotRollingOut(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	s.onReplicaSet(replicaSet("rs-1", "prod", "web-a1", "d1", "1", 3, 0))
	// Updated and total say the rollout finished; available says the pods are
	// broken. deploymentComplete is false here; this predicate is not.
	s.onDeployment(deployment("d1", "prod", "web", 3, 3, 0, 3))

	if got := rollingOutNames(s); len(got) != 0 {
		t.Errorf("RollingOut() = %v, want nothing — a crash-looping Deployment is broken, not rolling", got)
	}
	if deploymentComplete(s.deployments["d1"].obj) {
		t.Error("fixture is wrong: this Deployment should be incomplete, which is the whole point")
	}
}

func TestRollingOut_TheThreeDeploymentClauses(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		setup func(*Source)
	}{
		{"the controller has not seen the spec", func(s *Source) {
			s.onReplicaSet(replicaSet("rs-1", "prod", "web-a1", "d1", "1", 3, 3))
			d := deployment("d1", "prod", "web", 3, 3, 3, 3)
			d.Generation = 7 // status.observedGeneration is still 2
			s.onDeployment(d)
		}},
		{"two ReplicaSets still hold replicas", func(s *Source) {
			s.onReplicaSet(replicaSet("rs-old", "prod", "web-a1", "d1", "1", 1, 1))
			s.onReplicaSet(replicaSet("rs-new", "prod", "web-b2", "d1", "2", 3, 3))
			s.onDeployment(deployment("d1", "prod", "web", 3, 3, 3, 4))
		}},
		{"some replicas are still on the old template", func(s *Source) {
			s.onReplicaSet(replicaSet("rs-1", "prod", "web-a1", "d1", "1", 3, 3))
			s.onDeployment(deployment("d1", "prod", "web", 3, 1, 3, 3))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, _, _ := newTestSource(t, Config{})
			tc.setup(s)
			if got := rollingOutNames(s); !slices.Equal(got, []string{"Deployment/prod/web"}) {
				t.Errorf("RollingOut() = %v, want the Deployment", got)
			}
		})
	}
}

// A paused Deployment can sit half-rolled for weeks. That is a steady state
// somebody chose, and relaxing its thresholds for the duration would mean the
// one workload most likely to be wrongly placed is the one nothing judges.
func TestRollingOut_APausedDeploymentIsASteadyStateNotATransient(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	s.onReplicaSet(replicaSet("rs-old", "prod", "web-a1", "d1", "1", 1, 1))
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-b2", "d1", "2", 2, 2))
	d := deployment("d1", "prod", "web", 3, 2, 3, 3)
	d.Spec.Paused = true
	s.onDeployment(d)

	if got := rollingOutNames(s); len(got) != 0 {
		t.Errorf("RollingOut() = %v, want nothing for a paused Deployment", got)
	}
}

// A scaled-down old ReplicaSet is the *normal* resting state of every
// Deployment that has ever been updated — the controller keeps ten of them by
// default. Counting those would report every such Deployment as permanently
// mid-rollout.
func TestRollingOut_EmptyOldReplicaSetsDoNotCount(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	s.onReplicaSet(replicaSet("rs-old", "prod", "web-a1", "d1", "1", 0, 0))
	s.onReplicaSet(replicaSet("rs-older", "prod", "web-z9", "d1", "0", 0, 0))
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-b2", "d1", "2", 3, 3))
	s.onDeployment(deployment("d1", "prod", "web", 3, 3, 3, 3))

	if got := rollingOutNames(s); len(got) != 0 {
		t.Errorf("RollingOut() = %v, want nothing — history is not a rollout", got)
	}
}

// Someone else's ReplicaSets must not be counted against this Deployment, and
// a non-controller owner reference is not ownership.
func TestRollingOut_OnlyOwnedControllerReplicaSetsCount(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	s.onReplicaSet(replicaSet("rs-1", "prod", "web-a1", "d1", "1", 3, 3))
	s.onReplicaSet(replicaSet("rs-other", "prod", "api-c3", "d2", "1", 3, 3))
	adopted := replicaSet("rs-adopted", "prod", "web-q7", "d1", "3", 3, 3)
	adopted.OwnerReferences[0].Controller = boolPtr(false)
	s.onReplicaSet(adopted)
	s.onDeployment(deployment("d1", "prod", "web", 3, 3, 3, 3))

	if got := rollingOutNames(s); len(got) != 0 {
		t.Errorf("RollingOut() = %v, want nothing", got)
	}
}

func TestRollingOut_AStatefulSetMidRevisionChange(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	sts := statefulSet("s1", "prod", "db", 3, 3, "db-1", "db-2")
	sts.Status.UpdatedReplicas = 1
	s.onStatefulSet(sts)

	if got := rollingOutNames(s); !slices.Equal(got, []string{"StatefulSet/prod/db"}) {
		t.Errorf("RollingOut() = %v, want the StatefulSet", got)
	}
}

func TestRollingOut_ASettledStatefulSetIsNotRollingOut(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	sts := statefulSet("s1", "prod", "db", 3, 3, "db-2", "db-2")
	sts.Status.UpdatedReplicas = 3
	s.onStatefulSet(sts)

	if got := rollingOutNames(s); len(got) != 0 {
		t.Errorf("RollingOut() = %v, want nothing", got)
	}
}

func TestRollingOut_AStatefulSetTheControllerHasNotSeen(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	sts := statefulSet("s1", "prod", "db", 3, 3, "db-2", "db-2")
	sts.Status.UpdatedReplicas = 3
	sts.Generation = 9 // status.observedGeneration is still 2
	s.onStatefulSet(sts)

	if got := rollingOutNames(s); !slices.Equal(got, []string{"StatefulSet/prod/db"}) {
		t.Errorf("RollingOut() = %v, want the StatefulSet", got)
	}
}

// An unset spec.replicas is one, not zero. Read as zero, a default-sized
// StatefulSet with its one ordinal updated would report `1 < 0` false and a
// *scaling* one would report backwards; the API's own default is the only
// reading that makes both come out right.
func TestRollingOut_AnUnsetReplicaCountReadsAsOne(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	sts := statefulSet("s1", "prod", "db", 1, 1, "db-2", "db-2")
	sts.Spec.Replicas = nil
	sts.Status.UpdatedReplicas = 0
	s.onStatefulSet(sts)

	if got := rollingOutNames(s); !slices.Equal(got, []string{"StatefulSet/prod/db"}) {
		t.Errorf("RollingOut() = %v, want the StatefulSet — 0 updated of a defaulted 1", got)
	}
}

// Both kinds at once, which is what the caller actually asks for: one call
// covering the whole cluster rather than one per subject.
func TestRollingOut_ReportsEveryKindInOneCall(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	settled("d-quiet", "quiet")(s)
	s.onReplicaSet(replicaSet("rs-1", "prod", "web-a1", "d-busy", "1", 3, 3))
	s.onDeployment(deployment("d-busy", "prod", "busy", 3, 1, 3, 3))
	sts := statefulSet("s1", "prod", "db", 3, 3, "db-1", "db-2")
	sts.Status.UpdatedReplicas = 1
	s.onStatefulSet(sts)

	want := []string{"Deployment/prod/busy", "StatefulSet/prod/db"}
	if got := rollingOutNames(s); !slices.Equal(got, want) {
		t.Errorf("RollingOut() = %v, want %v", got, want)
	}
}

func TestRollingOut_AnEmptySourceAnswersEmpty(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	if got := s.RollingOut(); got != nil {
		t.Errorf("RollingOut() = %v, want nil", got)
	}
}
