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

package engine

import (
	"testing"
	"time"
)

// stormUpdateHarness forms a 3-member storm and returns the
// correlator, its settable clock, and an attach helper that reports
// the SizeUpdate of the i-th member's arrival.
func stormUpdateHarness(t *testing.T) (*StormCorrelator, *time.Time, func(i int) *StormSizeUpdate) {
	t.Helper()
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	c, now := newTestCorrelator(t, time.Minute, 3, res)
	attach := func(i int) *StormSizeUpdate {
		sig := podSignal(i, "shop")
		res.byObject[sig.ref()] = []Ancestor{ancNode}
		v := c.Observe(sig)
		if i < 3 && v.Kind != StormNone {
			t.Fatalf("member %d: verdict %v before threshold", i, v.Kind)
		}
		if i == 3 && v.Kind != StormFormed {
			t.Fatalf("member 3: verdict %v, want formed", v.Kind)
		}
		if i > 3 && v.Kind != StormAttached {
			t.Fatalf("member %d: verdict %v, want attached", i, v.Kind)
		}
		return v.SizeUpdate
	}
	for i := 1; i <= 3; i++ {
		attach(i)
	}
	return c, now, attach
}

// TestStormSizeUpdate_DoublingFiresAfterInterval: the formation
// payload is the first size report (count 3); the next update is due
// when membership doubles, and never inside the 1-minute rate limit.
func TestStormSizeUpdate_DoublingFiresAfterInterval(t *testing.T) {
	t.Parallel()
	_, now, attach := stormUpdateHarness(t)

	// Members 4-5: below the doubling threshold — no update even
	// with the interval elapsed.
	*now = now.Add(61 * time.Second)
	for i := 4; i <= 5; i++ {
		if upd := attach(i); upd != nil {
			t.Fatalf("member %d (below threshold) produced update %+v", i, upd)
		}
	}
	// Member 6 doubles the reported 3 → update with current totals.
	upd := attach(6)
	if upd == nil {
		t.Fatal("member 6 (doubling) must produce a size update")
	}
	if upd.AffectedCount != 6 || upd.NamespaceCount != 1 || upd.NewSinceLast != 3 {
		t.Errorf("update = %+v, want {6 1 3}", upd)
	}
	// Immediately after, growth resets: member 7 is below the new
	// thresholds.
	if upd := attach(7); upd != nil {
		t.Errorf("member 7 right after a report produced update %+v", upd)
	}
}

// TestStormSizeUpdate_RateLimitHoldsThenFiresCumulative: crossing the
// threshold inside the 1-minute window emits nothing, but the report
// cursor is held — the first attach past the window fires ONE update
// carrying the cumulative growth.
func TestStormSizeUpdate_RateLimitHoldsThenFiresCumulative(t *testing.T) {
	t.Parallel()
	_, now, attach := stormUpdateHarness(t)

	// Doubling reached 10s after formation: rate-limited, no update.
	*now = now.Add(10 * time.Second)
	for i := 4; i <= 7; i++ {
		if upd := attach(i); upd != nil {
			t.Fatalf("member %d inside the rate-limit window produced update %+v", i, upd)
		}
	}
	// Past the window, the next attach reports everything since
	// formation in one update.
	*now = now.Add(51 * time.Second)
	upd := attach(8)
	if upd == nil {
		t.Fatal("first attach past the rate limit must fire the held update")
	}
	if upd.AffectedCount != 8 || upd.NewSinceLast != 5 {
		t.Errorf("update = %+v, want cumulative {8 _ 5}", upd)
	}
}

// TestStormSizeUpdate_AbsoluteGrowthBeforeDoubling: once the reported
// count is large, +10 members fires before the next doubling — the
// drill's 3→33 growth would have reported at 6, 12, and 22.
func TestStormSizeUpdate_AbsoluteGrowthBeforeDoubling(t *testing.T) {
	t.Parallel()
	_, now, attach := stormUpdateHarness(t)

	grow := func(from, to, wantAt int) *StormSizeUpdate {
		t.Helper()
		var got *StormSizeUpdate
		for i := from; i <= to; i++ {
			*now = now.Add(61 * time.Second)
			upd := attach(i)
			if upd != nil {
				if i != wantAt {
					t.Fatalf("update fired at member %d, want %d (%+v)", i, wantAt, upd)
				}
				got = upd
			}
		}
		return got
	}
	if upd := grow(4, 6, 6); upd == nil || upd.AffectedCount != 6 {
		t.Fatalf("first update = %+v, want at count 6", upd)
	}
	if upd := grow(7, 12, 12); upd == nil || upd.AffectedCount != 12 || upd.NewSinceLast != 6 {
		t.Fatalf("second update = %+v, want doubling at 12", upd)
	}
	// From 12, +10 (22) precedes the doubling (24).
	if upd := grow(13, 23, 22); upd == nil || upd.AffectedCount != 22 || upd.NewSinceLast != 10 {
		t.Fatalf("third update = %+v, want +10 rule at 22", upd)
	}
}
