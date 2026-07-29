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

package tokenburn

import (
	"testing"
	"time"
)

// Regression tests for issue #80: eta = time.Duration((BudgetUSD -
// spent) / costRate * float64(time.Second)) overflows int64 when the
// projected exhaustion exceeds ~292 years. On amd64 the out-of-range
// float→int conversion yields math.MinInt64 — a huge NEGATIVE
// duration that satisfies eta < BurnETA, flipping budgetHot and
// emitting a false critical token.burn page. A projection that far
// out must read as "no projection" (the same outcome as costRate <=
// 0): no signal, and the calm/clearance path undisturbed.

// TestBudgetTrigger_TinyCostRateHugeBudget_ETAOverflowNoCritical is
// the direct overflow case: a $1e-9-per-poll drip (~1.7e-11 USD/s)
// against ~$10 of remaining budget projects exhaustion in ~6e11s
// (~19k years) — 6e20 ns, past int64.
func TestBudgetTrigger_TinyCostRateHugeBudget_ETAOverflowNoCritical(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.BudgetUSD = 10
	h := newHarness(t, cfg, "solo")
	h.fc.usage[UID("core-agent", "solo")] = Usage{TotalTokens: 1000, CostUSD: 0.01}
	drip := map[string]Usage{"solo": {CostUSD: 1e-9}}
	for i := 1; i <= 4; i++ {
		if sigs := h.poll(drip); len(sigs) != 0 {
			t.Fatalf("poll %d emitted %d signal(s) (kind=%s severity=%s) for a ~19k-year budget ETA — an overflow-range projection must read as no projection, like costRate<=0 (issue #80)",
				i, len(sigs), sigs[0].Kind, sigs[0].Severity)
		}
	}
}

// TestBudgetTrigger_FlatSpendEpsilonSlope_NoFalseCritical is the
// realistic end-to-end trigger from issue #80: a session whose
// cumulative spend is EXACTLY FLAT (idle session, constant CostUSD)
// polled at realistically jittered instants. The least-squares fit
// accumulates floating-point residue and returns a tiny positive
// cost rate (~4e-17 USD/s for these timestamps) instead of exactly 0
// — which then overflows the ETA conversion and fires a false
// critical budget page. The timestamps below were found by driving
// constant values through the real saturation.LeastSquaresSlope; the
// residue is deterministic for a given (times, values) input.
func TestBudgetTrigger_FlatSpendEpsilonSlope_NoFalseCritical(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.BudgetUSD = 100
	h := newHarness(t, cfg, "solo")
	// Pre-existing spend, then fully idle: every poll observes the
	// same cumulative cost.
	h.fc.usage[UID("core-agent", "solo")] = Usage{TotalTokens: 50000, CostUSD: 12.3456789}
	// ~60s cadence with millisecond jitter.
	offsetsMs := []int64{3081, 62887, 121847, 184059, 242081, 301318}
	start := h.now
	for i, off := range offsetsMs {
		h.now = start.Add(time.Duration(off) * time.Millisecond)
		if sigs := h.poll(nil); len(sigs) != 0 {
			t.Fatalf("poll %d emitted %d signal(s) (kind=%s severity=%s) on an exactly flat spend series — least-squares residue slope must not project exhaustion (issue #80)",
				i+1, len(sigs), sigs[0].Kind, sigs[0].Severity)
		}
	}
}
