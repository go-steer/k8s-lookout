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

package leeway

import "testing"

func TestPlacement_Equal(t *testing.T) {
	base := Placement{
		NodeName: "node-1",
		Domains:  []Domain{"us-central1-a", "pool-1"},
		State:    StateRunning,
		Pinned:   false,
	}

	tests := []struct {
		name  string
		other Placement
		want  bool
	}{
		{"identical", base, true},
		{
			"same values, different slice backing",
			Placement{NodeName: "node-1", Domains: []Domain{"us-central1-a", "pool-1"}, State: StateRunning},
			true,
		},
		{
			"different node",
			Placement{NodeName: "node-2", Domains: []Domain{"us-central1-a", "pool-1"}, State: StateRunning},
			false,
		},
		{
			// The move that matters: same node name would be a no-op, but a
			// relabelled node changes the distribution without the pod
			// changing at all.
			"different domain on one axis",
			Placement{NodeName: "node-1", Domains: []Domain{"us-central1-b", "pool-1"}, State: StateRunning},
			false,
		},
		{
			"different state",
			Placement{NodeName: "node-1", Domains: []Domain{"us-central1-a", "pool-1"}, State: StateTerminating},
			false,
		},
		{
			"different pinning",
			Placement{NodeName: "node-1", Domains: []Domain{"us-central1-a", "pool-1"}, State: StateRunning, Pinned: true},
			false,
		},
		{
			// A key-set change mid-flight. Not equal is the safe answer: it
			// forces a re-apply rather than leaving a placement counted under
			// an axis that no longer exists.
			"different number of axes",
			Placement{NodeName: "node-1", Domains: []Domain{"us-central1-a"}, State: StateRunning},
			false,
		},
		{
			"nil domains against empty domains",
			Placement{NodeName: "node-1", Domains: []Domain{}, State: StateRunning},
			false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := base.Equal(tc.other); got != tc.want {
				t.Errorf("Equal = %v, want %v", got, tc.want)
			}
			// Symmetry is not decoration here: OnPodUpdate compares in one
			// direction and the verifier in the other.
			if got := tc.other.Equal(base); got != tc.want {
				t.Errorf("reversed Equal = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPlacement_EqualZeroValues(t *testing.T) {
	var a, b Placement
	if !a.Equal(b) {
		t.Error("two zero-value placements are not equal")
	}
	// A pod that has never been placed and one placed on a node with no
	// topology labels are different facts, and the second has a domain.
	unlabelled := Placement{Domains: []Domain{DomainUnknown}}
	if a.Equal(unlabelled) {
		t.Error("an unplaced pod compares equal to one on an unlabelled node")
	}
}

func TestPlacement_Domain(t *testing.T) {
	p := Placement{Domains: []Domain{"zone-a", "pool-1"}}

	if got := p.Domain(0); got != "zone-a" {
		t.Errorf("Domain(0) = %q, want zone-a", got)
	}
	if got := p.Domain(1); got != "pool-1" {
		t.Errorf("Domain(1) = %q, want pool-1", got)
	}

	// Out of range in both directions returns the sentinel rather than
	// panicking, because the caller is an informer handler.
	for _, ordinal := range []int{-1, 2, 99} {
		if got := p.Domain(ordinal); got != DomainUnknown {
			t.Errorf("Domain(%d) = %q, want %q", ordinal, got, DomainUnknown)
		}
	}
	var empty Placement
	if got := empty.Domain(0); got != DomainUnknown {
		t.Errorf("zero-value Domain(0) = %q, want %q", got, DomainUnknown)
	}
}
