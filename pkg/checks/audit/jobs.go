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
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// kindSuspendedJob is the posture claim of issue #265.
const kindSuspendedJob = "audit.suspended_job"

// defaultJobSuspended matches defaultCronSuspended: the same "longer
// than a maintenance window" floor, for the same reason.
const defaultJobSuspended = 7 * 24 * time.Hour

// kueueQueueName is the label (and, on older Kueue, annotation) that
// hands a Job to Kueue's admission queue.
const kueueQueueName = "kueue.x-k8s.io/queue-name"

// jobControllerName is the spec.managedBy value naming the built-in
// Job controller; any other value hands the Job to someone else.
const jobControllerName = "kubernetes.io/job-controller"

// suspendedJob judges one Job's spec.suspend as posture.
//
// # Why posture, following the CronJob precedent
//
// A suspended Job is the same shape as a suspended CronJob
// (cronjobs.go): a deliberate setting that never self-clears, so it
// belongs here and not in `triage delta`, which treats suspended Jobs
// as nominal. What makes one a finding is the same, too: it outlived
// the maintenance that motivated it, and the one-shot task it carries
// — a migration, a backfill — is quietly not happening.
//
// # Why most suspended Jobs are not judged
//
// Suspension is also a queueing mechanism, and there it is the normal
// state of a Job waiting its turn, not a decision anyone forgot. So the
// claim is limited to Jobs a person suspended:
//
//   - a Job with a controller ownerReference is its owner's business: a
//     CronJob's own suspension is judged above, and a JobSet, workflow
//     engine or batch operator suspends and resumes its children itself;
//   - a Job carrying Kueue's queue-name label (or annotation) is queued
//     for admission, which Kueue signals by suspending it;
//   - a Job whose spec.managedBy names a controller other than the
//     built-in one (MultiKueue, for instance) is that controller's;
//   - a Job that has already completed or failed has nothing left to
//     suspend.
//
// There is no schedule to scale the floor by, so the wall-clock floor
// (--job-suspended) is the whole threshold.
func suspendedJob(j *batchv1.Job, now time.Time, suspended time.Duration) *emit.Finding {
	if j.Spec.Suspend == nil || !*j.Spec.Suspend {
		return nil
	}
	if metav1.GetControllerOf(j) != nil || j.Labels[kueueQueueName] != "" || j.Annotations[kueueQueueName] != "" {
		return nil
	}
	if m := j.Spec.ManagedBy; m != nil && *m != "" && *m != jobControllerName {
		return nil
	}
	if jobCondition(j, batchv1.JobComplete) || jobCondition(j, batchv1.JobFailed) {
		return nil
	}
	since, anchor := jobSuspendedSince(j)
	age := now.Sub(since)
	if age < suspended {
		return nil
	}

	f := emit.Finding{
		Kind:         kindSuspendedJob,
		Severity:     emit.SeverityInfo,
		Namespace:    j.Namespace,
		KindOfObject: "Job",
		Name:         j.Name,
		Reason:       "SuspendedJob",
		Message: fmt.Sprintf("spec.suspend has been true for %s and the Job has not finished: it runs no pods and nothing will resume it, so whatever it does — a migration, a backfill — is not happening, and nothing else reports that",
			roundDays(age)),
		Details: []emit.Field{
			{Key: "suspended_for", Value: roundDays(age)},
			{Key: "suspended_since", Value: since.UTC().Format(time.RFC3339)},
			{Key: "anchor", Value: anchor},
		},
	}
	f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, "Job")
	return &f
}

// jobSuspendedSince estimates when the Job was suspended, and names
// the evidence it used. Unlike a CronJob, a Job says so itself: the
// Job controller sets a Suspended condition whose lastTransitionTime
// is the moment it took effect. The managedFields estimate
// (suspendedSince's reasoning) and creation are the fallbacks for a
// Job the controller has not reconciled.
func jobSuspendedSince(j *batchv1.Job) (time.Time, string) {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobSuspended && c.Status == corev1.ConditionTrue && !c.LastTransitionTime.IsZero() {
			return c.LastTransitionTime.Time, "condition"
		}
	}
	var best time.Time
	for _, e := range j.ManagedFields {
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
	return j.CreationTimestamp.Time, "creation"
}

func jobCondition(j *batchv1.Job, t batchv1.JobConditionType) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == t && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
