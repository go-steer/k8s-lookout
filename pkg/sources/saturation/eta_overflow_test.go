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

import (
	"context"
	"testing"
	"time"
)

const gib = float64(1 << 30)

// Regression tests for issue #80: eta = time.Duration(headroom /
// slope * float64(time.Second)) overflows int64 when the projected
// ETA exceeds ~292 years. On amd64 the out-of-range float→int
// conversion yields math.MinInt64 — a huge NEGATIVE duration that
// skips the eta > clearETA() recede gate and satisfies eta < CritETA,
// emitting a false CRITICAL saturation.forecast. A projection that
// far out must read as "no projection" (the same outcome as a
// non-positive slope): no signal at any severity.

// TestNoForecast_TinySlopeHugeHeadroom_ETAOverflow is the direct
// overflow case: an exact epsilon-positive slope (1e-6 B/s) against
// ~10GiB of headroom projects ETA ≈ 1.07e16s (~340M years) — 1.07e25
// ns, far past int64.
func TestNoForecast_TinySlopeHugeHeadroom_ETAOverflow(t *testing.T) {
	t.Parallel()
	pods := &scriptedPods{}
	s, _ := testSource(smallCfg(), pods, nil)
	ctx := context.Background()
	// 3e-5 B per 30s sample = slope exactly 1e-6 B/s; both count and
	// span gates open at i=10 (11 samples over 5m = window/2).
	for i := 0; i < 14; i++ {
		pods.samples = []ContainerSample{memSample(100+float64(i)*3e-5, 10*gib)}
		if got := s.sampleOnce(ctx, t0.Add(time.Duration(i)*30*time.Second)); len(got) != 0 {
			t.Fatalf("i=%d: emitted %d signal(s) (kind=%s severity=%s) for a ~340M-year ETA — an overflow-range projection must read as no projection, like slope<=0 (issue #80)",
				i, len(got), got[0].Kind, got[0].Severity)
		}
	}
}

// TestNoForecast_FlatSeriesEpsilonSlope_NoFalseCritical is the
// realistic end-to-end trigger from issue #80: an EXACTLY FLAT series
// (idle pod, constant memory) sampled at realistically jittered
// instants. The least-squares fit accumulates floating-point residue
// and returns a tiny positive slope (~2e-8 B/s for these timestamps)
// instead of exactly 0 — which then overflows the ETA conversion and
// fires a false critical. The timestamps below were found by driving
// constant values through the real LeastSquaresSlope; the residue is
// deterministic for a given (times, values) input.
func TestNoForecast_FlatSeriesEpsilonSlope_NoFalseCritical(t *testing.T) {
	t.Parallel()
	pods := &scriptedPods{}
	s, _ := testSource(smallCfg(), pods, nil)
	ctx := context.Background()
	// ~30s cadence with millisecond jitter; span 331s ≥ window/2 at
	// the last sample, where the 12-point fit residue is positive.
	offsetsMs := []int64{1786, 30786, 61692, 91040, 120104, 150854, 180730, 212964, 240511, 271176, 301310, 332859}
	const used = 3.3 * float64(1<<30) // constant — the pod is idle
	for i, off := range offsetsMs {
		pods.samples = []ContainerSample{memSample(used, 10*gib)}
		if got := s.sampleOnce(ctx, t0.Add(time.Duration(off)*time.Millisecond)); len(got) != 0 {
			t.Fatalf("i=%d: emitted %d signal(s) (kind=%s severity=%s) on an exactly flat series — least-squares residue slope must not forecast (issue #80)",
				i, len(got), got[0].Kind, got[0].Severity)
		}
	}
}
