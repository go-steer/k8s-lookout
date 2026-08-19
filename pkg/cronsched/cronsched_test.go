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

package cronsched

import (
	"testing"
	"time"
)

func mustParse(t *testing.T, schedule, tz string) Schedule {
	t.Helper()
	s, err := Parse(schedule, tz)
	if err != nil {
		t.Fatalf("Parse(%q, %q): %v", schedule, tz, err)
	}
	return s
}

func ts(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("bad timestamp %q: %v", s, err)
	}
	return v
}

func TestParse_AcceptsWhatTheAPIServerAccepts(t *testing.T) {
	t.Parallel()
	// Every form kube's validation allows on spec.schedule.
	for _, sched := range []string{
		"*/5 * * * *",
		"0 3 * * *",
		"0 0 1 * *",
		"15,45 * * * *",
		"0 9-17 * * MON-FRI",
		"0 0 * * 0",
		"@hourly", "@daily", "@midnight", "@weekly", "@monthly", "@yearly", "@annually",
		"@every 1h30m",
	} {
		if _, err := Parse(sched, ""); err != nil {
			t.Errorf("Parse(%q) = %v, want ok", sched, err)
		}
	}
}

func TestParse_RejectsMalformed(t *testing.T) {
	t.Parallel()
	for _, sched := range []string{"", "not a schedule", "* * * *", "99 * * * *"} {
		if _, err := Parse(sched, ""); err == nil {
			t.Errorf("Parse(%q) = nil error, want a parse failure", sched)
		}
	}
}

// The zone must actually move the activation, which is the whole point
// of spec.timeZone — and it must resolve without /usr/share/zoneinfo,
// which is what the time/tzdata import buys.
func TestParse_TimeZoneShiftsTheActivation(t *testing.T) {
	t.Parallel()
	anchor := ts(t, "2026-03-10T00:00:00Z")

	utc := mustParse(t, "0 3 * * *", "").Next(anchor)
	if want := ts(t, "2026-03-10T03:00:00Z"); !utc.Equal(want) {
		t.Errorf("UTC next = %s, want %s", utc, want)
	}

	// 03:00 in Tokyo (UTC+9) is 18:00 UTC the previous day, so the
	// next one after midnight UTC on the 10th is 18:00 on the 10th.
	tokyo := mustParse(t, "0 3 * * *", "Asia/Tokyo").Next(anchor)
	if want := ts(t, "2026-03-10T18:00:00Z"); !tokyo.Equal(want.UTC()) {
		t.Errorf("Asia/Tokyo next = %s, want %s", tokyo.UTC(), want)
	}
}

func TestParse_UnknownTimeZoneIsAnError(t *testing.T) {
	t.Parallel()
	if _, err := Parse("0 3 * * *", "Mars/Olympus_Mons"); err == nil {
		t.Error("Parse with an unknown zone = nil error, want a failure")
	}
}

func TestSchedule_Spec_CarriesTheZonePrefix(t *testing.T) {
	t.Parallel()
	if got, want := mustParse(t, "0 3 * * *", "Asia/Tokyo").Spec(), "CRON_TZ=Asia/Tokyo 0 3 * * *"; got != want {
		t.Errorf("Spec() = %q, want %q", got, want)
	}
	if got, want := mustParse(t, "0 3 * * *", "").Spec(), "0 3 * * *"; got != want {
		t.Errorf("Spec() = %q, want %q", got, want)
	}
}

// The day-of-month / day-of-week OR rule: when BOTH fields are
// restricted, Vixie cron fires if EITHER matches. This is the rule a
// hand-rolled parser is most likely to get wrong, and getting it wrong
// means calling a healthy CronJob overdue.
func TestSchedule_Next_DomDowOrSemantics(t *testing.T) {
	t.Parallel()
	// "1st of the month, OR any Monday". 2026-08-01 is a Saturday.
	s := mustParse(t, "0 0 1 * MON", "")
	got := s.Next(ts(t, "2026-08-01T12:00:00Z"))
	// Not the 8th (next 1st-of-month is September) — the next Monday,
	// the 3rd.
	if want := ts(t, "2026-08-03T00:00:00Z"); !got.Equal(want) {
		t.Errorf("Next = %s, want %s (dom OR dow)", got, want)
	}
}

func TestSchedule_MissedSince(t *testing.T) {
	t.Parallel()
	hourly := mustParse(t, "@hourly", "")
	anchor := ts(t, "2026-08-19T00:00:00Z")

	tests := []struct {
		name   string
		now    time.Time
		want   int
		capped bool
	}{
		{"nothing elapsed", anchor, 0, false},
		{"before the first activation", ts(t, "2026-08-19T00:30:00Z"), 0, false},
		{"exactly on an activation counts it", ts(t, "2026-08-19T01:00:00Z"), 1, false},
		{"three hours", ts(t, "2026-08-19T03:30:00Z"), 3, false},
		{"a full day", ts(t, "2026-08-20T00:00:00Z"), 24, false},
		{"now before anchor", ts(t, "2026-08-18T00:00:00Z"), 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n, capped := hourly.MissedSince(anchor, tc.now)
			if n != tc.want || capped != tc.capped {
				t.Errorf("MissedSince = (%d, %v), want (%d, %v)", n, capped, tc.want, tc.capped)
			}
		})
	}
}

func TestSchedule_MissedSince_ZeroAnchorIsNotMaximallyOverdue(t *testing.T) {
	t.Parallel()
	// A CronJob that has never run has a nil lastScheduleTime. Walking
	// from the zero time would report every activation since year 1.
	n, capped := mustParse(t, "@hourly", "").MissedSince(time.Time{}, ts(t, "2026-08-19T00:00:00Z"))
	if n != 0 || capped {
		t.Errorf("MissedSince(zero anchor) = (%d, %v), want (0, false)", n, capped)
	}
}

func TestSchedule_MissedSince_CapsTheWalk(t *testing.T) {
	t.Parallel()
	// Every minute, anchored a year back: far more than maxWalk.
	n, capped := mustParse(t, "* * * * *", "").MissedSince(
		ts(t, "2025-08-19T00:00:00Z"), ts(t, "2026-08-19T00:00:00Z"))
	if !capped {
		t.Error("capped = false, want true for a year of minutely activations")
	}
	if n != maxWalk {
		t.Errorf("n = %d, want the %d cap as a floor", n, maxWalk)
	}
}

func TestSchedule_Overdue(t *testing.T) {
	t.Parallel()
	hourly := mustParse(t, "@hourly", "")
	anchor := ts(t, "2026-08-19T00:00:00Z")
	const grace = 5 * time.Minute

	tests := []struct {
		name string
		now  time.Time
		want bool
	}{
		{"before the activation", ts(t, "2026-08-19T00:59:00Z"), false},
		{"inside the grace window", ts(t, "2026-08-19T01:03:00Z"), false},
		{"exactly at the grace boundary", ts(t, "2026-08-19T01:05:00Z"), true},
		{"past the grace window", ts(t, "2026-08-19T01:30:00Z"), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			expected, overdue := hourly.Overdue(anchor, tc.now, grace)
			if overdue != tc.want {
				t.Errorf("Overdue = %v, want %v", overdue, tc.want)
			}
			if want := ts(t, "2026-08-19T01:00:00Z"); !expected.Equal(want) {
				t.Errorf("expected activation = %s, want %s", expected, want)
			}
		})
	}
}

func TestSchedule_Overdue_ZeroAnchorNeverFires(t *testing.T) {
	t.Parallel()
	_, overdue := mustParse(t, "@hourly", "").Overdue(time.Time{}, ts(t, "2026-08-19T12:00:00Z"), 0)
	if overdue {
		t.Error("Overdue(zero anchor) = true, want false")
	}
}

// A zero-value Schedule must not panic — callers that ignore the Parse
// error would otherwise take the process down.
func TestSchedule_ZeroValueIsInert(t *testing.T) {
	t.Parallel()
	var s Schedule
	now := ts(t, "2026-08-19T00:00:00Z")
	if got := s.Next(now); !got.IsZero() {
		t.Errorf("zero Schedule Next = %s, want zero", got)
	}
	if n, capped := s.MissedSince(now.Add(-time.Hour), now); n != 0 || capped {
		t.Errorf("zero Schedule MissedSince = (%d, %v), want (0, false)", n, capped)
	}
	if _, overdue := s.Overdue(now.Add(-time.Hour), now, 0); overdue {
		t.Error("zero Schedule Overdue = true, want false")
	}
}
