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
	"math"
	"math/rand/v2"
	"reflect"
	"testing"
)

// propSeed fixes the property-test stream. A property test that reseeds from
// the clock reports a failure the next run cannot reproduce, which is the
// worst possible property to have in a test whose whole job is finding rare
// inputs.
const propSeed = 0x1eeba11

func propRand() *rand.Rand { return rand.New(rand.NewPCG(propSeed, 0x5eed)) }

func TestApportion_Examples(t *testing.T) {
	tests := []struct {
		name    string
		n       int64
		weights []float64
		caps    []int64
		want    []int64
	}{
		{"even split", 9, EqualWeights(3), nil, []int64{3, 3, 3}},
		{"largest remainder breaks by index", 10, EqualWeights(4), nil, []int64{3, 3, 2, 2}},
		{"three over two", 3, EqualWeights(2), nil, []int64{2, 1}},
		{"single domain takes all", 7, EqualWeights(1), nil, []int64{7}},
		{"weighted 2:1:1", 8, []float64{2, 1, 1}, nil, []int64{4, 2, 2}},
		{"zero weight gets nothing", 6, []float64{1, 0, 1}, nil, []int64{3, 0, 3}},
		{"all weights zero falls back to equal", 6, []float64{0, 0, 0}, nil, []int64{2, 2, 2}},
		{"negative weight treated as zero", 6, []float64{-5, 1, 1}, nil, []int64{0, 3, 3}},
		{"nan weight treated as zero", 6, []float64{math.NaN(), 1, 1}, nil, []int64{0, 3, 3}},
		{"inf weight does not swallow everything", 6, []float64{math.Inf(1), 1, 1}, nil, []int64{0, 3, 3}},
		{"zero replicas", 0, EqualWeights(3), nil, []int64{0, 0, 0}},

		// Water-filling. The first round wants [4,4,4]; domain 0 is capped at
		// 1, so its 3 surplus units are re-apportioned over the rest.
		{"cap forces overflow elsewhere", 12, EqualWeights(3), []int64{1, Uncapped, Uncapped}, []int64{1, 6, 5}},
		{"cap above share is inert", 9, EqualWeights(3), []int64{100, 100, 100}, []int64{3, 3, 3}},
		{"cascading caps", 12, EqualWeights(3), []int64{1, 2, Uncapped}, []int64{1, 2, 9}},
		{"total capacity short", 10, EqualWeights(2), []int64{2, 3}, []int64{2, 3}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Apportion(tc.n, tc.weights, tc.caps)
			if !reflect.DeepEqual(got.Expected, tc.want) {
				t.Errorf("Apportion(%d, %v, %v).Expected = %v, want %v", tc.n, tc.weights, tc.caps, got.Expected, tc.want)
			}
		})
	}
}

func TestApportion_ShortCapacityIsReportedNotAbsorbed(t *testing.T) {
	// 10 pods, room for 5. Silently apportioning [2,3] and scoring against it
	// would report drift for a workload that simply cannot fit, blaming
	// placement for a capacity shortfall.
	got := Apportion(10, EqualWeights(2), []int64{2, 3})
	if got.Placed != 5 {
		t.Errorf("Placed = %d, want 5", got.Placed)
	}
	if got.Unplaceable != 5 {
		t.Errorf("Unplaceable = %d, want 5", got.Unplaceable)
	}
	for i, sat := range got.Saturated {
		if !sat {
			t.Errorf("domain %d should be saturated", i)
		}
	}
}

func TestApportion_NoEligibleDomains(t *testing.T) {
	got := Apportion(5, nil, nil)
	if got.Placed != 0 || got.Unplaceable != 5 {
		t.Errorf("Placed=%d Unplaceable=%d, want 0 and 5", got.Placed, got.Unplaceable)
	}
}

// TestApportion_SumInvariant is the property everything downstream rests on:
// the expectation must account for every object, or R and ρ are measured
// against a total that does not match the one they are divided by.
func TestApportion_SumInvariant(t *testing.T) {
	r := propRand()
	for i := 0; i < 3000; i++ {
		n := int64(r.IntN(2000))
		m := 1 + r.IntN(8)
		w := randomWeights(r, m)
		caps := maybeCaps(r, m, n)

		got := Apportion(n, w, caps)

		var sum int64
		for _, e := range got.Expected {
			if e < 0 {
				t.Fatalf("negative expectation %d for n=%d w=%v caps=%v", e, n, w, caps)
			}
			sum += e
		}
		if sum != got.Placed {
			t.Fatalf("Σexpected=%d != Placed=%d for n=%d w=%v caps=%v", sum, got.Placed, n, w, caps)
		}
		if got.Placed+got.Unplaceable != n {
			t.Fatalf("Placed+Unplaceable=%d != n=%d (w=%v caps=%v)", got.Placed+got.Unplaceable, n, w, caps)
		}
		if caps == nil && got.Placed != n {
			t.Fatalf("uncapped apportionment placed %d of %d (w=%v)", got.Placed, n, w)
		}
	}
}

// TestApportion_RespectsCaps checks the water-filling actually clamps, and
// that Placed matches the theoretical maximum min(n, Σcaps).
func TestApportion_RespectsCaps(t *testing.T) {
	r := propRand()
	for i := 0; i < 3000; i++ {
		n := int64(r.IntN(500))
		m := 1 + r.IntN(6)
		w := randomWeights(r, m)
		caps := make([]int64, m)
		var totalCap int64
		for j := range caps {
			caps[j] = int64(r.IntN(100))
			totalCap += caps[j]
		}

		got := Apportion(n, w, caps)
		for j, e := range got.Expected {
			if e > caps[j] {
				t.Fatalf("domain %d got %d over cap %d (n=%d w=%v caps=%v)", j, e, caps[j], n, w, caps)
			}
		}
		want := n
		if totalCap < want {
			want = totalCap
		}
		// Placed can fall short of min(n, Σcaps) only where a domain has zero
		// weight and therefore never receives a unit to clamp. Allow that,
		// but require the shortfall to be explained by zero-weight capacity.
		if got.Placed > want {
			t.Fatalf("Placed=%d exceeds min(n,Σcaps)=%d", got.Placed, want)
		}
		if got.Placed < want {
			var zeroWeightCap int64
			for j, wt := range w {
				if wt <= 0 || math.IsNaN(wt) {
					zeroWeightCap += caps[j]
				}
			}
			if zeroWeightCap == 0 {
				t.Fatalf("Placed=%d < min(n,Σcaps)=%d with no zero-weight domain (n=%d w=%v caps=%v)", got.Placed, want, n, w, caps)
			}
		}
	}
}

// TestApportion_SatisfiesQuota pins the defining property of the largest
// remainder method: every domain gets either the floor or the ceiling of its
// exact fractional entitlement, never further away than that.
//
// This is what makes the expectation defensible to a user. "Your zone should
// hold 4 pods" has to be one of the two whole numbers adjacent to its true
// share, or the expectation itself is the thing that is wrong.
func TestApportion_SatisfiesQuota(t *testing.T) {
	r := propRand()
	for i := 0; i < 3000; i++ {
		n := int64(1 + r.IntN(1000))
		m := 1 + r.IntN(8)
		w := randomPositiveWeights(r, m)

		got := Apportion(n, w, nil)

		var sumW float64
		for _, x := range w {
			sumW += x
		}
		for j := range w {
			exact := float64(n) * w[j] / sumW
			lo := int64(math.Floor(exact))
			hi := int64(math.Ceil(exact))
			if got.Expected[j] < lo || got.Expected[j] > hi {
				t.Fatalf("quota violated: domain %d got %d, exact %.6f (n=%d w=%v)", j, got.Expected[j], exact, n, w)
			}
		}
	}
}

// TestApportion_EqualWeightsAreBalanced: with equal weights and no caps, no
// two domains may differ by more than one. This is the case a human eyeballs,
// and getting it wrong would make leeway report drift on a perfectly balanced
// workload.
func TestApportion_EqualWeightsAreBalanced(t *testing.T) {
	r := propRand()
	for i := 0; i < 2000; i++ {
		n := int64(r.IntN(5000))
		m := 1 + r.IntN(12)

		got := Apportion(n, EqualWeights(m), nil)

		lo, hi := got.Expected[0], got.Expected[0]
		for _, e := range got.Expected {
			lo = min(lo, e)
			hi = max(hi, e)
		}
		if hi-lo > 1 {
			t.Fatalf("equal weights gave skew %d for n=%d m=%d: %v", hi-lo, n, m, got.Expected)
		}
		// And the design's S* claim: 0 when n divides evenly, else 1.
		want := int64(1)
		if n%int64(m) == 0 {
			want = 0
		}
		if hi-lo != want {
			t.Fatalf("S* = %d, want %d for n=%d m=%d", hi-lo, want, n, m)
		}
	}
}

// TestApportion_IsDeterministic guards the reason SortDomains exists. A
// tie-break that depended on map order would make findings flap between
// evaluations with nothing in the cluster having changed.
func TestApportion_IsDeterministic(t *testing.T) {
	r := propRand()
	for i := 0; i < 500; i++ {
		n := int64(r.IntN(1000))
		m := 1 + r.IntN(8)
		w := randomWeights(r, m)
		caps := maybeCaps(r, m, n)

		first := Apportion(n, w, caps)
		for k := 0; k < 5; k++ {
			again := Apportion(n, w, caps)
			if !reflect.DeepEqual(first, again) {
				t.Fatalf("non-deterministic for n=%d w=%v caps=%v:\n %v\n %v", n, w, caps, first, again)
			}
		}
	}
}

func TestNeedsCapacityWeighting(t *testing.T) {
	tests := []struct {
		name string
		caps []float64
		want bool
	}{
		{"identical", []float64{100, 100, 100}, false},
		{"just under trigger", []float64{100, 120}, false},
		{"just over trigger", []float64{100, 126}, true},
		{"one empty domain", []float64{100, 0}, true},
		{"single domain", []float64{100}, false},
		{"none", nil, false},
		{"all unusable", []float64{math.NaN(), math.Inf(1)}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := NeedsCapacityWeighting(tc.caps); got != tc.want {
				t.Errorf("NeedsCapacityWeighting(%v) = %v, want %v", tc.caps, got, tc.want)
			}
		})
	}
}

func randomWeights(r *rand.Rand, m int) []float64 {
	w := make([]float64, m)
	for i := range w {
		switch r.IntN(10) {
		case 0:
			w[i] = 0
		case 1:
			w[i] = -r.Float64() * 10
		case 2:
			w[i] = math.NaN()
		default:
			w[i] = r.Float64() * 100
		}
	}
	return w
}

func randomPositiveWeights(r *rand.Rand, m int) []float64 {
	w := make([]float64, m)
	for i := range w {
		w[i] = 0.01 + r.Float64()*100
	}
	return w
}

func maybeCaps(r *rand.Rand, m int, n int64) []int64 {
	if r.IntN(2) == 0 {
		return nil
	}
	caps := make([]int64, m)
	for i := range caps {
		if r.IntN(3) == 0 {
			caps[i] = Uncapped
			continue
		}
		caps[i] = int64(r.IntN(int(n) + 2))
	}
	return caps
}
