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

package delta

import (
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"

	"github.com/go-steer/k8s-lookout/pkg/cronsched"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// controllerMissedLimit is the CronJob controller's own give-up
// threshold: with spec.startingDeadlineSeconds unset it looks back at
// most 100 missed start times, and past that refuses to schedule at
// all ("Cannot determine if job needs to be started. Too many missed
// start time (> 100)"). A schedule over that line does not recover on
// its own — someone has to unwedge it — so the finding says so.
const controllerMissedLimit = 100

// criticalMissedRuns is where one late activation stops reading as
// latency and starts reading as a stopped schedule. Matches the
// `workload` sentinel's CriticalMisses so the two paths agree on
// severity for the same CronJob.
const criticalMissedRuns = 3

// checkCronJobs flags unsuspended CronJobs whose schedule says they
// should have fired and whose status says they did not.
//
// This is the scan-path twin of the `workload` sentinel's
// workload.cron_missed signal, and deliberately NOT the same
// judgement. The sentinel is transition-based and arms after sync: it
// folds armedAt into its anchor so it never backfills across sentinel
// downtime, which leaves pre-arm history to exactly this command. A
// scan has no arming concept — it anchors on what the cluster itself
// records (status.lastScheduleTime, or creationTimestamp for a
// CronJob that has never run) and judges the schedule as it stands
// right now.
//
// Suspended CronJobs are skipped: `suspend: true` is a deliberate
// standing state that never self-clears, so it is posture (`audit
// workloads`), not an incident (DESIGN.md §1 principle 5).
func (s *scanner) checkCronJobs(cjs []batchv1.CronJob) {
	for i := range cjs {
		cj := &cjs[i]
		if cj.Spec.Suspend != nil && *cj.Spec.Suspend {
			continue
		}
		sched, err := cronsched.Parse(cj.Spec.Schedule, timeZoneOf(cj))
		if err != nil {
			// The API server validates spec.schedule at admission, so
			// this means our parser and the controller's disagree.
			// §2 forbids silence: say so rather than skipping quietly.
			s.add(emit.Finding{
				Kind:         "cron.unparseable",
				Severity:     emit.SeverityWarning,
				Namespace:    cj.Namespace,
				KindOfObject: "CronJob",
				Name:         cj.Name,
				Reason:       "ScheduleUnparseable",
				Message: fmt.Sprintf("schedule %q could not be parsed (%v) — cron.missed cannot be judged for this CronJob",
					cj.Spec.Schedule, err),
				Details: []emit.Field{{Key: "schedule", Value: cj.Spec.Schedule}},
			})
			continue
		}

		anchor, anchorKind := cronAnchor(cj)
		expected, overdue := sched.Overdue(anchor, s.now, s.th.cronGrace)
		if !overdue {
			continue
		}
		// lastScheduleTime having reached the activation means it ran;
		// only a stale anchor is a miss.
		if last := cj.Status.LastScheduleTime; last != nil && !last.Time.Before(expected) {
			continue
		}

		missed, capped := sched.MissedSince(anchor, s.now)
		severity := emit.SeverityWarning
		if missed >= criticalMissedRuns {
			severity = emit.SeverityCritical
		}

		reason := "ScheduleMissed"
		consequence := "the schedule is not producing runs (controller wedged, startingDeadlineSeconds starvation, or concurrencyPolicy=Forbid behind a stuck run)"
		if capped || missed > controllerMissedLimit {
			// Past 100 the controller stops trying permanently.
			reason = "ScheduleAbandoned"
			consequence = fmt.Sprintf("more than %d activations have been missed: with startingDeadlineSeconds unset the controller stops scheduling entirely and will not recover without intervention", controllerMissedLimit)
		}

		missedValue := itoa(missed)
		if capped {
			missedValue = "≥" + missedValue
		}
		details := []emit.Field{
			{Key: "schedule", Value: cj.Spec.Schedule},
			{Key: "expected", Value: expected.UTC().Format(time.RFC3339)},
			{Key: "missed_runs", Value: missedValue},
			{Key: "anchor", Value: anchorKind},
		}
		if tz := timeZoneOf(cj); tz != "" {
			details = append(details, emit.Field{Key: "time_zone", Value: tz})
		}
		if last := cj.Status.LastScheduleTime; last != nil {
			details = append(details,
				emit.Field{Key: "last_schedule", Value: last.UTC().Format(time.RFC3339)},
				emit.Field{Key: "age", Value: s.age(last.Time)})
		} else {
			details = append(details, emit.Field{Key: "last_schedule", Value: "never"})
		}
		if n := len(cj.Status.Active); n > 0 {
			// A Forbid-policy CronJob with a run still active is the
			// textbook cause; naming it saves a lookup.
			details = append(details, emit.Field{Key: "active_jobs", Value: itoa(n)})
		}

		s.add(emit.Finding{
			Kind:         "cron.missed",
			Severity:     severity,
			Namespace:    cj.Namespace,
			KindOfObject: "CronJob",
			Name:         cj.Name,
			Reason:       reason,
			Message: fmt.Sprintf("CronJob should have run at %s (schedule %q) and did not — %s",
				expected.UTC().Format(time.RFC3339), cj.Spec.Schedule, consequence),
			Details: details,
		})
	}
}

// cronAnchor picks the point to judge the schedule forward from, and
// names it for the finding.
//
// status.lastScheduleTime is the truth when the CronJob has ever
// fired. When it has not, creationTimestamp is the only baseline the
// cluster offers — and it is the right one: a CronJob created an hour
// ago that has never run its @hourly schedule IS overdue. There is
// deliberately no third fallback; a CronJob always has a creation
// timestamp, and inventing an anchor from the zero time would report
// every activation since year 1.
func cronAnchor(cj *batchv1.CronJob) (time.Time, string) {
	if last := cj.Status.LastScheduleTime; last != nil && !last.Time.IsZero() {
		return last.Time, "last_schedule"
	}
	return cj.CreationTimestamp.Time, "creation"
}

// timeZoneOf reads spec.timeZone ("" = the controller's local time,
// which is what the field's absence means).
func timeZoneOf(cj *batchv1.CronJob) string {
	if cj.Spec.TimeZone == nil {
		return ""
	}
	return *cj.Spec.TimeZone
}
