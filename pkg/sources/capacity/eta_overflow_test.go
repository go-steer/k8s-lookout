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

	corev1 "k8s.io/api/core/v1"
)

// Mirror of the issue #80 regression tests (quota/eta_overflow_test.go,
// tokenburn/eta_overflow_test.go) for the cluster-forecast dimension:
// eta = time.Duration(headroom/slope * 1e9) overflows int64 when the
// projection exceeds ~292 years — on amd64 the out-of-range float→int
// conversion yields math.MinInt64, a huge NEGATIVE duration that
// satisfies eta <= CritETA and would page critical on an effectively
// idle cluster. saturation.ETAFromSeconds's clamp must make such a
// projection read as no projection at all.

// TestClusterForecast_TinySlopeHugeHeadroom_ETAOverflowNoForecast is
// the direct overflow case: requests creeping up 1 byte per 30s
// against a 1 TiB allocatable — ratio slope ~3e-14/s, projected
// time-to-full ~3e13 s, far past int64 nanoseconds. Any signal here
// is the overflow.
func TestClusterForecast_TinySlopeHugeHeadroom_ETAOverflowNoForecast(t *testing.T) {
	t.Parallel()
	s := newForecastSource(smallForecastCfg())
	nodes := []*corev1.Node{forecastNode("n1", nil, 0, 1<<40)}
	for i := 0; i < 20; i++ {
		pods := []*corev1.Pod{forecastPod("web", "n1", 0, 100+int64(i))}
		if got := s.sampleCluster(pods, nodes, ft0.Add(time.Duration(i)*30*time.Second)); len(got) != 0 {
			t.Fatalf("emitted %d signal(s) (severity=%s) for a ~1M-year ETA at ~0%% usage — an overflow-range projection must read as no projection (issue #80)",
				len(got), got[0].Severity)
		}
	}
}

// TestClusterForecast_FlatSeriesJitteredTimestamps_NoFalseCritical is
// the realistic trigger: an EXACTLY FLAT ratio (steady cluster) at
// 95% of allocatable, sampled at jittered timestamps. The
// least-squares fit can return a tiny floating-point-residue slope
// instead of exactly 0; whether the residue lands positive (overflow
// clamp) or non-positive (slope gate), the flat series must never
// fire — there is no ratio-threshold in this source, only the trend.
func TestClusterForecast_FlatSeriesJitteredTimestamps_NoFalseCritical(t *testing.T) {
	t.Parallel()
	s := newForecastSource(smallForecastCfg())
	nodes := []*corev1.Node{forecastNode("n1", nil, 0, 1000)}
	// ~30s cadence with irregular millisecond jitter, spanning ~9m.
	offsetsMs := []int64{
		2498, 31527, 62331, 95584, 124702, 158541, 187954, 216122,
		247811, 279333, 308990, 341207, 371868, 404511, 433276, 466099,
		495732, 528444,
	}
	for _, off := range offsetsMs {
		pods := []*corev1.Pod{forecastPod("web", "n1", 0, 950)}
		if got := s.sampleCluster(pods, nodes, ft0.Add(time.Duration(off)*time.Millisecond)); len(got) != 0 {
			t.Fatalf("emitted %d signal(s) (severity=%s) on an exactly flat 95%% series — least-squares residue slope must not forecast (issue #80)",
				len(got), got[0].Severity)
		}
	}
}
