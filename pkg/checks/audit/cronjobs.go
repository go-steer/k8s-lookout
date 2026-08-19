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

package audit

import (
	"encoding/json"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/cronsched"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// kindSuspendedCron is the posture claim of issue #293.
const kindSuspendedCron = "audit.suspended_cronjob"

// defaultCronSuspended is the wall-clock floor below which a suspended
// CronJob is assumed to be mid-maintenance rather than forgotten.
const defaultCronSuspended = 7 * 24 * time.Hour

// suspendedCronJob judges one CronJob's spec.suspend as posture.
//
// # Why this is posture and not an incident
//
// `suspend: true` is a deliberate setting that never self-clears. An
// incident scan reports things that stop being true when someone fixes
// them (DESIGN.md §1 principle 5); this one stops being true only when
// someone changes the spec, so reporting it from `triage delta` would
// pile a flat, permanent backlog into the `findings diff` transition
// stream. `triage delta` therefore skips suspended CronJobs outright,
// and the standing question — "which schedules has this cluster
// quietly turned off and forgotten" — is answered here.
//
// # Why the threshold is not a plain duration
//
// A suspension is normal for as long as the maintenance that motivated
// it. What makes one a finding is that it outlived its reason, and how
// long that takes depends on the schedule: an @hourly job dark for a
// week is forgotten, while a @monthly job dark for a week has not even
// skipped a run yet. So two conditions must both hold:
//
//   - the suspension is older than `suspended` (--cron-suspended,
//     default 7d) — the wall-clock floor that keeps a deploy window
//     from being a finding, and
//   - at least one activation has actually been skipped — the
//     structural rule that scales the claim to the schedule. This is
//     not a tunable number: a suspension that has cost nothing yet has
//     nothing to report.
//
// A CronJob whose spec.schedule does not parse is judged on age alone
// and says so, rather than escaping both this claim and `triage
// delta`'s (which skips it for being suspended).
func suspendedCronJob(cj *batchv1.CronJob, now time.Time, suspended time.Duration) *emit.Finding {
	if cj.Spec.Suspend == nil || !*cj.Spec.Suspend {
		return nil
	}
	since, anchor := suspendedSince(cj)
	age := now.Sub(since)
	if age < suspended {
		return nil
	}

	tz := ""
	if cj.Spec.TimeZone != nil {
		tz = *cj.Spec.TimeZone
	}
	missedValue := "unknown"
	skipped := "the schedule could not be parsed, so the skipped activations could not be counted"
	if sched, err := cronsched.Parse(cj.Spec.Schedule, tz); err == nil {
		missed, capped := sched.MissedSince(since, now)
		if missed == 0 {
			// Nothing has been skipped yet: on this schedule the
			// suspension has not cost a run, whatever the calendar says.
			return nil
		}
		missedValue = itoa(missed)
		if capped {
			missedValue = "≥" + missedValue
		}
		skipped = fmt.Sprintf("%s %s %s been skipped", missedValue, plural(missed, "activation"), pluralHave(missed))
	}

	details := []emit.Field{
		{Key: "schedule", Value: cj.Spec.Schedule},
		{Key: "suspended_for", Value: roundDays(age)},
		{Key: "suspended_since", Value: since.UTC().Format(time.RFC3339)},
		{Key: "anchor", Value: anchor},
		{Key: "missed_runs", Value: missedValue},
	}
	if tz != "" {
		details = append(details, emit.Field{Key: "time_zone", Value: tz})
	}

	f := emit.Finding{
		Kind:         kindSuspendedCron,
		Severity:     emit.SeverityWarning,
		Namespace:    cj.Namespace,
		KindOfObject: "CronJob",
		Name:         cj.Name,
		Reason:       "SuspendedCronJob",
		Message: fmt.Sprintf("spec.suspend has been true for %s (schedule %q) and %s: whatever this job does — backup, reconcile, expiry sweep — has not run since, and nothing else reports that",
			roundDays(age), cj.Spec.Schedule, skipped),
		Details: details,
	}
	f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, "CronJob")
	return &f
}

// pluralHave agrees the verb with the count roundDays cannot.
func pluralHave(n int) string {
	if n == 1 {
		return "has"
	}
	return "have"
}

// suspendedSince estimates when the CronJob was suspended, and names
// the evidence it used.
//
// The best available signal is the managedFields entry that owns
// spec.suspend: the API server stamps every entry with the time its
// manager last wrote its own fields, which is the same forensic
// technique that root-caused #320. It is an ESTIMATE, and errs one way
// on purpose. ManagedFieldsEntry.Time moves whenever that manager
// touches ANY field it owns, not specifically when suspend flipped, so
// the true suspension time is always at or before what we read — the
// reported age is a floor, and the failure mode is staying quiet about
// a CronJob whose owner rewrites it regularly, not inventing one.
//
// Without such an entry (a cluster old enough to predate the field, or
// an object written by a client that strips it) it falls back to the
// last time the schedule did fire, and then to creation. Both are
// weaker — lastScheduleTime is nil outright for a CronJob suspended
// before it ever ran — but they are what the object carries.
func suspendedSince(cj *batchv1.CronJob) (time.Time, string) {
	var best time.Time
	for _, e := range cj.ManagedFields {
		// Subresource entries record status writes; spec.suspend is not
		// among them.
		if e.Subresource != "" || e.Time == nil || !ownsSuspend(e) {
			continue
		}
		if e.Time.After(best) {
			best = e.Time.Time
		}
	}
	if !best.IsZero() {
		return best, "managed_field"
	}
	if last := cj.Status.LastScheduleTime; last != nil && !last.Time.IsZero() {
		return last.Time, "last_schedule"
	}
	return cj.CreationTimestamp.Time, "creation"
}

// ownsSuspend reports whether a managedFields entry claims
// spec.suspend. FieldsV1 is an opaque JSON set of "f:<field>" keys, so
// this walks the two levels it needs rather than pulling in the
// fieldpath machinery for one lookup. Unparseable JSON means "cannot
// tell", which is not-owned: the fallbacks below it are still honest.
func ownsSuspend(e metav1.ManagedFieldsEntry) bool {
	raw := e.FieldsV1.GetRawBytes() // nil-safe on a nil FieldsV1
	if len(raw) == 0 {
		return false
	}
	var set map[string]json.RawMessage
	if err := json.Unmarshal(raw, &set); err != nil {
		return false
	}
	spec, ok := set["f:spec"]
	if !ok {
		return false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(spec, &fields); err != nil {
		return false
	}
	_, ok = fields["f:suspend"]
	return ok
}
