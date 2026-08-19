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

// Package cronsched resolves a CronJob's spec.schedule into activation
// times.
//
// One parser, shared by every surface that reasons about CronJob
// schedules: the `workload` sentinel source (workload.cron_missed),
// `triage delta` (cron.missed) and `audit workloads` (the suspended
// claim). It wraps github.com/robfig/cron/v3 — the SAME parser the
// upstream CronJob controller uses — deliberately: a claim that a
// controller missed a window is only sound if "window" means what the
// controller means by it. Hand-rolling a second grammar would let us
// disagree with kube-controller-manager on the day-of-month /
// day-of-week OR rule or a DST boundary and call a healthy cluster
// broken.
//
// spec.timeZone is honored via the CRON_TZ= prefix, exactly as
// upstream does. Zone names resolve through time.LoadLocation, which
// needs a tzdata source: the blank time/tzdata import below embeds one
// so resolution does not depend on /usr/share/zoneinfo existing in
// whatever base image we ship (distroless carries it today, but that
// is a property of the image, not of this binary).
package cronsched

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	// Embeds the IANA database so spec.timeZone resolves in a
	// scratch/distroless image. ~450KB, paid once.
	_ "time/tzdata"
)

// Schedule is a parsed CronJob schedule.
type Schedule struct {
	inner cron.Schedule
	spec  string
}

// Spec returns the schedule string as handed to the parser, CRON_TZ
// prefix included. Useful in log lines and finding details.
func (s Schedule) Spec() string { return s.spec }

// Parse resolves schedule (spec.schedule) in timeZone (spec.timeZone,
// "" for the controller's local time) into a Schedule.
//
// The API server rejects a malformed spec.schedule at admission, so a
// parse error here means the API server and this parser disagree —
// near-impossible, but callers must handle it rather than assume.
func Parse(schedule, timeZone string) (Schedule, error) {
	spec := schedule
	if timeZone != "" {
		spec = "CRON_TZ=" + timeZone + " " + schedule
	}
	inner, err := cron.ParseStandard(spec)
	if err != nil {
		return Schedule{}, fmt.Errorf("parse schedule %q: %w", spec, err)
	}
	return Schedule{inner: inner, spec: spec}, nil
}

// Next returns the first activation strictly after t, or the zero time
// if the schedule has none (a February 30th, say).
func (s Schedule) Next(t time.Time) time.Time {
	if s.inner == nil {
		return time.Time{}
	}
	return s.inner.Next(t)
}

// maxWalk caps the activation walk in MissedSince. A schedule as tight
// as * * * * * produces 1440 activations a day, so an anchor a year
// stale would otherwise spin through half a million steps to answer a
// question whose answer is already "very many". Callers get the cap
// back as a floor, flagged by the capped return.
const maxWalk = 10000

// MissedSince counts the activations that fell in (anchor, now] — how
// many times the schedule said to run since anchor.
//
// capped reports that the walk hit its internal ceiling, so n is a
// floor rather than an exact count. Callers phrase such findings as
// "at least n".
//
// A zero anchor, or one at/after now, yields 0: with no baseline there
// is nothing to have missed, and inventing one from the epoch would
// make every CronJob maximally overdue.
func (s Schedule) MissedSince(anchor, now time.Time) (n int, capped bool) {
	if s.inner == nil || anchor.IsZero() || !anchor.Before(now) {
		return 0, false
	}
	t := anchor
	for n < maxWalk {
		t = s.Next(t)
		if t.IsZero() || t.After(now) {
			return n, false
		}
		n++
	}
	return n, true
}

// Overdue reports whether the schedule's first activation after anchor
// is more than grace in the past — the shared "should have fired by
// now and did not" predicate.
//
// expected is that activation, so callers can name the window they are
// complaining about. A zero anchor is never overdue, for the reason
// MissedSince gives.
func (s Schedule) Overdue(anchor, now time.Time, grace time.Duration) (expected time.Time, overdue bool) {
	if s.inner == nil || anchor.IsZero() {
		return time.Time{}, false
	}
	expected = s.Next(anchor)
	if expected.IsZero() {
		return time.Time{}, false
	}
	// >=, not >: an activation whose grace window closes exactly now
	// is overdue. Matches the sentinel's original predicate.
	return expected, !now.Before(expected.Add(grace))
}
