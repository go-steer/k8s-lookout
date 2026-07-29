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
	"math"
	"time"
)

// maxETASeconds bounds the projections representable as a
// time.Duration: math.MaxInt64/2 nanoseconds (~146 years) as float
// seconds. Beyond it the float→int64 conversion in
// time.Duration(seconds * 1e9) is out of range — on amd64 it yields
// math.MinInt64, a negative ETA that reads as "already breached"
// (issue #80). 146 years sits comfortably past every configurable
// ETA threshold, so the clamp never suppresses an actionable
// forecast.
const maxETASeconds = float64(math.MaxInt64/2) / float64(time.Second)

// ETAFromSeconds converts a projected time-to-limit (headroom/slope,
// in seconds) into a time.Duration. Like LeastSquaresSlope this is
// the shared seam: the quota and tokenburn sources reuse THIS
// conversion rather than growing per-package copies of the overflow
// clamp. ok=false — for NaN, negative, or beyond-maxETASeconds
// input — means the projection is not representable; callers must
// treat it as no projection at all, the same outcome as a
// non-positive slope.
func ETAFromSeconds(seconds float64) (eta time.Duration, ok bool) {
	if !(seconds >= 0 && seconds <= maxETASeconds) {
		return 0, false
	}
	return time.Duration(seconds * float64(time.Second)), true
}
