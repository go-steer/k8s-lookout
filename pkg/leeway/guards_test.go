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

import (
	"reflect"
	"testing"
)

// The guards below are unreachable through the exported API today. They are
// tested directly rather than deleted, and directly rather than left uncovered.
//
// Deleting them would be wrong: each sits on an internal helper whose caller
// enforces the precondition, and "the caller enforces it" is exactly the
// property a later refactor breaks. Leaving them uncovered would be worse —
// an untested branch that only ever runs the day something else goes wrong is
// the one most likely to turn a small bug into a panic.
//
// Each case records *why* it is currently unreachable, so that a future reader
// who does reach it knows a precondition changed.

// hamilton's caller never passes a non-positive budget.
//
// Proof that budget > 0 at every call: a domain is frozen only when its share
// strictly exceeds its cap, so Σcaps over the frozen set is strictly less than
// Σshares over that set, which is at most n. budget = n − Σfrozen caps is
// therefore strictly positive whenever any domain was frozen, and equals n on
// the first pass.
func TestHamilton_NonPositiveBudget(t *testing.T) {
	for _, n := range []int64{0, -1, -1000} {
		got := hamilton(n, EqualWeights(3))
		if want := []int64{0, 0, 0}; !reflect.DeepEqual(got, want) {
			t.Errorf("hamilton(%d, …) = %v, want %v", n, got, want)
		}
	}
}

// chiSquare's caller always passes equal-length, non-empty slices, because
// Score builds both from the same domain list.
func TestChiSquare_MalformedInput(t *testing.T) {
	tests := []struct {
		name             string
		actual, expected []int64
	}{
		{"both empty", nil, nil},
		{"actual shorter", []int64{1}, []int64{1, 2}},
		{"expected shorter", []int64{1, 2}, []int64{1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			x2, valid := chiSquare(tc.actual, tc.expected)
			if valid {
				t.Errorf("chiSquare(%v, %v) reported valid", tc.actual, tc.expected)
			}
			if x2 != 0 {
				t.Errorf("χ² = %v, want 0 when the input is unusable", x2)
			}
		})
	}
}

// domainAgg.reason is only consulted for a domain with zero eligible nodes, and
// such a domain has at least one node that produced a rejection reason — an
// agg is not created for a domain no node is in.
func TestDomainAgg_ReasonWithNoReasons(t *testing.T) {
	var a domainAgg
	if got, want := a.reason(), "no nodes in this domain"; got != want {
		t.Errorf("reason() = %q, want %q", got, want)
	}
}

// note folds duplicates, which is what keeps a 50-node cordoned zone from
// producing a 50-clause exclusion string.
func TestDomainAgg_NoteFoldsDuplicates(t *testing.T) {
	var a domainAgg
	a.note("cordoned")
	a.note("cordoned")
	a.note("NotReady")
	a.note("cordoned")

	if want := []string{"cordoned", "NotReady"}; !reflect.DeepEqual(a.reasons, want) {
		t.Errorf("reasons = %v, want %v", a.reasons, want)
	}
	if got, want := a.reason(), "no usable nodes: cordoned, NotReady"; got != want {
		t.Errorf("reason() = %q, want %q", got, want)
	}
}
