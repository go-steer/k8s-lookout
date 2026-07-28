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

package quota

import (
	"context"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// Regression tests for issue #80: eta = time.Duration(headroom /
// slope * float64(time.Second)) overflows int64 when the projected
// ETA exceeds ~292 years. On amd64 the out-of-range float→int
// conversion yields math.MinInt64 — a huge NEGATIVE duration that
// satisfies eta < CritETA, emitting a false critical quota.forecast
// PLUS a garbage quota-increase draft. A projection that far out must
// read as "no projection" (hasETA=false, same as a non-positive
// slope): with usage far below the ratio thresholds, no signal at
// all.

// TestPoll_TinySlopeHugeHeadroom_ETAOverflowNoForecast is the direct
// overflow case: slope 1e-6 units/day (~1.16e-11/s) against ~1e10
// headroom projects ETA ≈ 8.6e20s — far past int64 nanoseconds.
// Usage is at 0.000001% of the limit, so no ratio threshold can fire;
// any signal here is the overflow.
func TestPoll_TinySlopeHugeHeadroom_ETAOverflowNoForecast(t *testing.T) {
	t.Parallel()
	api := &scriptedQuotaAPI{
		inventory: []cloud.QuotaUsage{{Name: "CPUS", Scope: "us-east1", Usage: 100, Limit: 1e10}},
		histories: map[string]cloud.QuotaHistory{
			// 8 daily points creeping up 1e-6/day: epsilon-positive
			// slope, passes the minPoints and span gates.
			"CPUS/us-east1": {Name: "CPUS", Scope: "us-east1", Usage: dailySeries(testNow, 8, 100, 1e-6)},
		},
	}
	s := newTestSource(t, api, Config{})
	sigs, err := s.poll(context.Background(), testNow)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(sigs) != 0 {
		t.Fatalf("poll emitted %d signal(s) (severity=%s, draft attached=%v) for a ~2.7e13-year ETA at 0.000001%% usage — an overflow-range projection must read as no projection (issue #80)",
			len(sigs), sigs[0].Severity, sigs[0].QuotaDraft != nil)
	}
}

// TestPoll_FlatSeriesEpsilonSlope_NoFalseCritical is the realistic
// end-to-end trigger from issue #80: an EXACTLY FLAT usage history
// (idle quota) at realistically jittered daily timestamps. The
// least-squares fit accumulates floating-point residue and returns a
// tiny positive slope (~1.6e-15/s for these timestamps) instead of
// exactly 0 — which then overflows the ETA conversion into a false
// critical with a garbage draft. The timestamps below were found by
// driving constant values through the real
// saturation.LeastSquaresSlope; the residue is deterministic for a
// given (times, values) input.
func TestPoll_FlatSeriesEpsilonSlope_NoFalseCritical(t *testing.T) {
	t.Parallel()
	// ~daily cadence with up-to-1h jitter, spanning ~7d.
	offsetsMs := []int64{2498081, 86527887, 174331847, 259584059, 346702081, 434541318, 518954425, 606122540}
	const flat = 1234567.89 // constant usage, ~0.12% of the limit
	base := testNow.Add(-7 * 24 * time.Hour)
	pts := make([]cloud.Point, len(offsetsMs))
	for i, off := range offsetsMs {
		pts[i] = cloud.Point{Time: base.Add(time.Duration(off) * time.Millisecond), Value: flat}
	}
	api := &scriptedQuotaAPI{
		inventory: []cloud.QuotaUsage{{Name: "CPUS", Scope: "us-east1", Usage: flat, Limit: 1e9}},
		histories: map[string]cloud.QuotaHistory{
			"CPUS/us-east1": {Name: "CPUS", Scope: "us-east1", Usage: pts},
		},
	}
	s := newTestSource(t, api, Config{})
	sigs, err := s.poll(context.Background(), testNow)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(sigs) != 0 {
		t.Fatalf("poll emitted %d signal(s) (severity=%s, draft attached=%v) on an exactly flat history — least-squares residue slope must not forecast (issue #80)",
			len(sigs), sigs[0].Severity, sigs[0].QuotaDraft != nil)
	}
}
