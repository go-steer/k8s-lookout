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

package capacity

import (
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// TestInsufficientResource is the whole of this source's judgement about *why*
// a pod did not land, and the negative cases are the point: a consumer that
// treats every refusal as a capacity shortfall blames the cluster for a taint.
func TestInsufficientResource(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		msg  string
		want bool
	}{
		{"cpu", "0/3 nodes are available: 3 Insufficient cpu.", true},
		{"memory", "0/3 nodes are available: 3 Insufficient memory.", true},
		{
			// Extended resources and device plugins use the same phrasing,
			// which an enumeration of cpu and memory would miss.
			"extended resource",
			"0/8 nodes are available: 8 Insufficient nvidia.com/gpu.",
			true,
		},
		{
			// The scheduler concatenates per-predicate counts, so one message
			// can cite several reasons. Capacity being one of them is enough.
			"mixed with a taint",
			"0/12 nodes are available: 3 Insufficient cpu, 7 node(s) had untolerated taint {dedicated: gpu}.",
			true,
		},
		{
			"taint only",
			"0/12 nodes are available: 12 node(s) had untolerated taint {dedicated: gpu}.",
			false,
		},
		{
			"affinity only",
			"0/5 nodes are available: 5 node(s) didn't match Pod's node affinity/selector.",
			false,
		},
		{
			"volume node conflict",
			"0/5 nodes are available: 5 node(s) had volume node affinity conflict.",
			false,
		},
		{"empty", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := insufficientResource(tc.msg); got != tc.want {
				t.Errorf("insufficientResource(%q) = %v, want %v", tc.msg, got, tc.want)
			}
		})
	}
}

// TestUnschedulables_SnapshotsTheJudgement: the exported view carries the
// scheduler's own words and this source's verdict, and retires a pod the
// moment it schedules — a consumer that saw a stale entry would attribute a
// drift to a pod that has since landed.
func TestUnschedulables_SnapshotsTheJudgement(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})

	t0 := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	s.trackPod(unschedulablePod("uid-1", "web-1", t0))

	taintedPod := unschedulablePod("uid-2", "web-2", t0.Add(-time.Hour))
	taintedPod.Status.Conditions[0].Message = "0/3 nodes are available: 3 node(s) had untolerated taint {a: b}."
	s.trackPod(taintedPod)

	got := s.Unschedulables()
	if len(got) != 2 {
		t.Fatalf("snapshot has %d entries, want both refused pods", len(got))
	}
	one := got["uid-1"]
	if one.Namespace != "shop" || one.Name != "web-1" {
		t.Errorf("identity = %s/%s, want shop/web-1", one.Namespace, one.Name)
	}
	if !one.Since.Equal(t0) {
		t.Errorf("Since = %v, want the condition's transition %v", one.Since, t0)
	}
	if !one.InsufficientResource {
		t.Error("a pod refused for Insufficient cpu is not marked as a capacity refusal")
	}
	if one.Message != "0/3 nodes are available: 3 Insufficient cpu." {
		t.Errorf("Message = %q, want the scheduler's verbatim text", one.Message)
	}
	if got["uid-2"].InsufficientResource {
		t.Error("a pod refused only for a taint is marked as a capacity refusal")
	}

	// The snapshot is a copy: a consumer holding it cannot reach the table.
	got["uid-1"] = Unschedulable{Name: "tampered"}
	if s.Unschedulables()["uid-1"].Name != "web-1" {
		t.Error("mutating the snapshot reached the source's own table")
	}

	// And it retires with the pod.
	scheduled := unschedulablePod("uid-1", "web-1", t0)
	scheduled.Status.Conditions = nil
	s.trackPod(scheduled)
	if _, still := s.Unschedulables()["uid-1"]; still {
		t.Error("a pod that scheduled is still in the snapshot")
	}
}
