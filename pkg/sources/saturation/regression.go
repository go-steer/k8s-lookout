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

package saturation

import "time"

// LeastSquaresSlope is the exported seam over this source's
// regression internals (DESIGN.md §10.2: "the saturation-source
// slope math applies directly" to quota usage series — the quota
// source reuses THIS fit rather than growing a second one). It fits
// value = a + b·t over the paired (times, values) samples and
// returns b in units per second; time is centered on the first
// sample for numeric stability. Fewer than two samples, mismatched
// lengths, or a degenerate time spread return 0 — callers treat a
// non-positive slope as "no projection", same as this source does.
func LeastSquaresSlope(times []time.Time, values []float64) float64 {
	if len(times) < 2 || len(times) != len(values) {
		return 0
	}
	n := float64(len(times))
	t0 := times[0]
	var sumT, sumV, sumTT, sumTV float64
	for i, ts := range times {
		t := ts.Sub(t0).Seconds()
		v := values[i]
		sumT += t
		sumV += v
		sumTT += t * t
		sumTV += t * v
	}
	den := n*sumTT - sumT*sumT
	if den == 0 {
		return 0
	}
	return (n*sumTV - sumT*sumV) / den
}
