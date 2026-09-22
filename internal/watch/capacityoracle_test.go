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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/go-steer/k8s-lookout/pkg/sources/capacity"
	"github.com/go-steer/k8s-lookout/pkg/sources/topologydrift"
)

type fakeUnschedulables map[types.UID]capacity.Unschedulable

func (f fakeUnschedulables) Unschedulables() map[types.UID]capacity.Unschedulable { return f }

// The seam docs/leeway-design.md §8.5 asks for: attribution consumes the
// capacity source's judgement about why a pod did not land instead of
// re-reading FailedScheduling. What is worth testing is that the judgement
// survives the trip unchanged — an adapter that dropped the verdict would turn
// every refusal into a capacity shortfall, and one that dropped the UID would
// attribute another workload's pending pods to whichever subject asked first.
func TestCapacityOracle_CarriesTheJudgementAcrossTheSeam(t *testing.T) {
	t.Parallel()
	since := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	got := capacityOracle(fakeUnschedulables{
		"uid-room": {
			Namespace: "prod", Name: "web-1", Since: since,
			Message: "0/3 nodes are available: 3 Insufficient cpu.", InsufficientResource: true,
		},
		"uid-taint": {
			Namespace: "prod", Name: "web-2", Since: since.Add(-time.Hour),
			Message: "0/3 nodes are available: 3 node(s) had untolerated taint {a: b}.",
		},
	})()

	if len(got) != 2 {
		t.Fatalf("oracle() has %d entries, want both refused pods", len(got))
	}
	want := topologydrift.PendingFact{
		Since:                since,
		Message:              "0/3 nodes are available: 3 Insufficient cpu.",
		InsufficientResource: true,
	}
	if got["uid-room"] != want {
		t.Errorf("uid-room = %+v, want %+v", got["uid-room"], want)
	}
	if got["uid-taint"].InsufficientResource {
		t.Error("a pod refused for a taint crossed the seam as a capacity refusal")
	}
}

func TestCapacityOracle_AClusterWithNothingRefusedAnswersEmpty(t *testing.T) {
	t.Parallel()
	if got := capacityOracle(fakeUnschedulables(nil))(); len(got) != 0 {
		t.Errorf("oracle() = %v, want nothing", got)
	}
}
