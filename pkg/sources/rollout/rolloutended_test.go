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
	"testing"
	"time"
)

// rollingDeployment is a Deployment mid-revision-change: two ReplicaSets still
// hold replicas and only one of three is on the new template.
func rollingDeployment(s *Source) {
	s.onReplicaSet(replicaSet("rs-old", "prod", "web-5f6", "d1", "1", 2, 2))
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 1, 1))
	s.onDeployment(deployment("d1", "prod", "web", 3, 1, 3, 3))
}

// settledDeployment is the same Deployment once the controller finished: the
// old ReplicaSet is scaled to zero and every replica is on the new template.
func settledDeployment(s *Source) {
	s.onReplicaSet(replicaSet("rs-old", "prod", "web-5f6", "d1", "1", 0, 0))
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 3, 3))
	s.onDeployment(deployment("d1", "prod", "web", 3, 3, 3, 3))
}

const webRef = "Deployment/prod/web"

func endedNames(s *Source) map[string]time.Time {
	out := make(map[string]time.Time)
	for w, at := range s.RolloutEnded() {
		out[w.Kind+"/"+w.Namespace+"/"+w.Name] = at
	}
	return out
}

// The edge, which is the whole feature: the stamp is when the rollout STOPPED,
// and a workload that has not been watched through one is absent rather than
// present at the zero time — §8.5 compares this against a settle window, and a
// zero would read as a rollout that ended in 1970.
func TestRolloutEnded_StampsTheFallingEdge(t *testing.T) {
	t.Parallel()
	s, _, clock := newTestSource(t, Config{})

	rollingDeployment(s)
	s.sweep(*clock)
	if got := endedNames(s); len(got) != 0 {
		t.Fatalf("RolloutEnded() = %v while the rollout is still running", got)
	}

	*clock = clock.Add(2 * time.Minute)
	settledDeployment(s)
	ended := *clock
	s.sweep(ended)

	got := endedNames(s)
	if !got[webRef].Equal(ended) {
		t.Fatalf("RolloutEnded()[%s] = %v, want the sweep that saw it stop, %v", webRef, got[webRef], ended)
	}

	// And it stays put. A later sweep that still sees it settled must not
	// re-stamp, or the settle window never elapses and rollout_bias can never
	// be attributed to anything.
	*clock = clock.Add(time.Hour)
	s.sweep(*clock)
	if got := endedNames(s); !got[webRef].Equal(ended) {
		t.Errorf("RolloutEnded()[%s] = %v after a quiet hour, want it unchanged at %v", webRef, got[webRef], ended)
	}
}

// A Deployment that was already settled when this source met it has no edge to
// observe, and inventing one at startup would make every subject in the
// cluster look freshly rolled out for the length of the attribution window.
func TestRolloutEnded_ASettledWorkloadSeenOnceIsNeverStamped(t *testing.T) {
	t.Parallel()
	s, _, clock := newTestSource(t, Config{})

	settledDeployment(s)
	s.sweep(*clock)
	*clock = clock.Add(time.Hour)
	s.sweep(*clock)

	if got := endedNames(s); len(got) != 0 {
		t.Errorf("RolloutEnded() = %v, want nothing — no rollout was watched to its end", got)
	}
}

// The distinction from completedAt, which is what makes this a separate field
// rather than a reuse. A Deployment whose new pods crash-loop forever is never
// deploymentComplete, but the controller stopped moving pods the moment the
// last old replica went away — and that is when §8.5's settle window should
// start, on exactly the workload most likely to have left drift behind.
func TestRolloutEnded_ACrashLoopingDeploymentStillEndsItsRollout(t *testing.T) {
	t.Parallel()
	s, _, clock := newTestSource(t, Config{})

	rollingDeployment(s)
	s.sweep(*clock)

	*clock = clock.Add(time.Minute)
	s.onReplicaSet(replicaSet("rs-old", "prod", "web-5f6", "d1", "1", 0, 0))
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 3, 0))
	// Updated and total say the controller finished; available says the pods
	// are broken.
	s.onDeployment(deployment("d1", "prod", "web", 3, 3, 0, 3))
	ended := *clock
	s.sweep(ended)

	if deploymentComplete(s.deployments["d1"].obj) {
		t.Fatal("fixture is wrong: this Deployment must be incomplete, which is the whole point")
	}
	if got := endedNames(s); !got[webRef].Equal(ended) {
		t.Errorf("RolloutEnded()[%s] = %v, want %v — the pods stopped moving even though they never got healthy",
			webRef, got[webRef], ended)
	}
}

// A second rollout re-arms the edge, so the stamp always dates the most recent
// one. §8.5 asks whether the drift outlived the last rollout, not the first.
func TestRolloutEnded_ASecondRolloutRestampsIt(t *testing.T) {
	t.Parallel()
	s, _, clock := newTestSource(t, Config{})

	rollingDeployment(s)
	s.sweep(*clock)
	*clock = clock.Add(time.Minute)
	settledDeployment(s)
	first := *clock
	s.sweep(first)

	*clock = clock.Add(time.Hour)
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 2, 2))
	s.onReplicaSet(replicaSet("rs-3rd", "prod", "web-9c2", "d1", "3", 1, 1))
	s.onDeployment(deployment("d1", "prod", "web", 3, 1, 3, 3))
	s.sweep(*clock)
	if got := endedNames(s); !got[webRef].Equal(first) {
		t.Fatalf("RolloutEnded()[%s] = %v mid-rollout, want the previous stamp %v", webRef, got[webRef], first)
	}

	*clock = clock.Add(time.Minute)
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 0, 0))
	s.onReplicaSet(replicaSet("rs-3rd", "prod", "web-9c2", "d1", "3", 3, 3))
	s.onDeployment(deployment("d1", "prod", "web", 3, 3, 3, 3))
	second := *clock
	s.sweep(second)
	if got := endedNames(s); !got[webRef].Equal(second) {
		t.Errorf("RolloutEnded()[%s] = %v, want the second rollout's stamp %v", webRef, got[webRef], second)
	}
}

// StatefulSets take the same path through the other predicate.
func TestRolloutEnded_AStatefulSetRevisionChange(t *testing.T) {
	t.Parallel()
	s, _, clock := newTestSource(t, Config{})

	s.onStatefulSet(statefulSet("s1", "prod", "db", 3, 2, "rev-1", "rev-2"))
	s.sweep(*clock)
	if got := endedNames(s); len(got) != 0 {
		t.Fatalf("RolloutEnded() = %v mid-revision-change", got)
	}

	*clock = clock.Add(3 * time.Minute)
	sts := statefulSet("s1", "prod", "db", 3, 3, "rev-2", "rev-2")
	sts.Status.UpdatedReplicas = 3
	s.onStatefulSet(sts)
	ended := *clock
	s.sweep(ended)

	if got := endedNames(s); !got["StatefulSet/prod/db"].Equal(ended) {
		t.Errorf("RolloutEnded()[StatefulSet/prod/db] = %v, want %v", got["StatefulSet/prod/db"], ended)
	}
}

// A workload that goes away takes its stamp with it. The tracks map is pruned
// on the same TTL as the objects, and a stamp outliving its Deployment would
// be attributed to whatever is next created with the same name.
func TestRolloutEnded_ADeletedWorkloadDropsOutOfTheAnswer(t *testing.T) {
	t.Parallel()
	s, _, clock := newTestSource(t, Config{})

	rollingDeployment(s)
	s.sweep(*clock)
	*clock = clock.Add(time.Minute)
	settledDeployment(s)
	s.sweep(*clock)
	if got := endedNames(s); len(got) != 1 {
		t.Fatalf("RolloutEnded() = %v, want the one stamp", got)
	}

	s.onDeploymentDelete(deployment("d1", "prod", "web", 3, 3, 3, 3))
	if got := endedNames(s); len(got) != 0 {
		t.Errorf("RolloutEnded() = %v after the Deployment was deleted, want nothing", got)
	}
}

func TestRolloutEnded_AnEmptySourceAnswersEmpty(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	if got := s.RolloutEnded(); len(got) != 0 {
		t.Errorf("RolloutEnded() = %v, want nothing", got)
	}
}
