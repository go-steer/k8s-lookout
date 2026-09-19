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

package watch

import (
	"slices"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources/rollout"
)

type fakeRollouts []rollout.WorkloadRef

func (f fakeRollouts) RollingOut() []rollout.WorkloadRef { return f }

// The seam docs/leeway-design.md §7.6 asks for: leeway relaxes its placement
// thresholds while a workload is mid-rollout, and it takes that answer from
// the rollout source rather than recomputing one. This is the adapter that
// carries it across, and what is worth testing is that a subject survives the
// trip with its identity intact — one that arrives under a kind leeway does
// not key on is a relaxation that silently never happens.
func TestRolloutOracle_CarriesBothKindsAcrossTheSeam(t *testing.T) {
	t.Parallel()
	got := rolloutOracle(fakeRollouts{
		{Kind: "Deployment", Namespace: "prod", Name: "web"},
		{Kind: "StatefulSet", Namespace: "prod", Name: "db"},
	})()

	want := []leeway.SubjectRef{
		{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "web"},
		{Kind: leeway.SubjectStatefulSet, Namespace: "prod", Name: "db"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("oracle() = %v, want %v", got, want)
	}
}

// A kind leeway does not key subjects on cannot be looked up, so passing it
// through would only grow the map. The rollout source watches exactly the two
// above today; this is the guard for the day one of the two lists moves.
func TestRolloutOracle_DropsAKindLeewayDoesNotTrack(t *testing.T) {
	t.Parallel()
	got := rolloutOracle(fakeRollouts{
		{Kind: "DaemonSet", Namespace: "kube-system", Name: "agent"},
		{Kind: "Deployment", Namespace: "prod", Name: "web"},
	})()

	want := []leeway.SubjectRef{{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "web"}}
	if !slices.Equal(got, want) {
		t.Errorf("oracle() = %v, want just the Deployment", got)
	}
}

func TestRolloutOracle_AQuietClusterAnswersEmpty(t *testing.T) {
	t.Parallel()
	if got := rolloutOracle(fakeRollouts(nil))(); len(got) != 0 {
		t.Errorf("oracle() = %v, want nothing", got)
	}
}
