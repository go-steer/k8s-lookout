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

package audit_test

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// cronNow is the pinned clock for the suspension claims; a suspension
// age is findings data, so it must be deterministic.
var cronNow = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func cronAgo(d time.Duration) metav1.Time { return metav1.Time{Time: cronNow.Add(-d)} }

func cronDeps(objs ...runtime.Object) audit.Deps {
	client := fake.NewClientset(objs...)
	return audit.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return client, nil },
		Now:    func() time.Time { return cronNow },
	}
}

// cron builds an @daily CronJob created 90 days before the pinned
// clock. suspend and the suspension evidence are set by the modifiers.
func cron(ns, name string, suspend bool, mods ...func(*batchv1.CronJob)) *batchv1.CronJob {
	cj := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name,
			CreationTimestamp: cronAgo(90 * 24 * time.Hour),
		},
		Spec: batchv1.CronJobSpec{Schedule: "@daily", Suspend: ptr(suspend)},
	}
	for _, m := range mods {
		m(cj)
	}
	return cj
}

// suspendedAt stamps the managedFields entry that owns spec.suspend —
// the evidence the claim prefers over every fallback.
func suspendedAt(d time.Duration) func(*batchv1.CronJob) {
	return func(cj *batchv1.CronJob) {
		cj.ManagedFields = append(cj.ManagedFields, metav1.ManagedFieldsEntry{
			Manager:    "kubectl-edit",
			Operation:  metav1.ManagedFieldsOperationUpdate,
			APIVersion: "batch/v1",
			Time:       ptr(cronAgo(d)),
			FieldsType: "FieldsV1",
			FieldsV1:   metav1.NewFieldsV1(`{"f:spec":{"f:suspend":{},"f:schedule":{}}}`),
		})
	}
}

func lastScheduled(d time.Duration) func(*batchv1.CronJob) {
	return func(cj *batchv1.CronJob) { cj.Status.LastScheduleTime = ptr(cronAgo(d)) }
}

func schedule(s string) func(*batchv1.CronJob) {
	return func(cj *batchv1.CronJob) { cj.Spec.Schedule = s }
}

func runCron(t *testing.T, objs []runtime.Object, args ...string) []map[string]string {
	t.Helper()
	res := checktest.Run(t, audit.WorkloadsCommand(cronDeps(objs...)), args...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	return findingLines(t, res.Stdout)
}

// An unsuspended CronJob is not this command's business at all — a
// schedule that should have fired and did not is an incident, and
// `triage delta` owns it (cron.missed).
func TestSuspendedCronJobIgnoresRunningOnes(t *testing.T) {
	recs := runCron(t, []runtime.Object{
		cron("prod", "backup", false, lastScheduled(90*24*time.Hour)),
	}, "-A")
	if len(recs) != 0 {
		t.Errorf("an unsuspended CronJob must produce nothing here, got %v", recs)
	}
}

// The wall-clock floor: a suspension inside a maintenance window is
// not a finding, however many activations it covers.
func TestSuspendedCronJobRespectsTheAgeFloor(t *testing.T) {
	objs := []runtime.Object{cron("prod", "backup", true, suspendedAt(2*24*time.Hour))}

	if recs := runCron(t, objs, "-A"); len(recs) != 0 {
		t.Errorf("2 days into the 7d default: want silence, got %v", recs)
	}
	recs := runCron(t, objs, "-A", "--cron-suspended=24h")
	if len(recs) != 1 || recs[0]["kind"] != "audit.suspended_cronjob" {
		t.Fatalf("past a 24h floor: want one suspended_cronjob, got %v", recs)
	}
	if recs[0]["severity"] != "warning" {
		t.Errorf("severity = %q, want warning", recs[0]["severity"])
	}
}

// The structural rule that makes the claim schedule-relative: a
// @monthly job dark for ten days has not skipped anything yet, so
// there is nothing to report, while an @hourly one dark for the same
// ten days has skipped 240 runs.
func TestSuspendedCronJobScalesToTheSchedule(t *testing.T) {
	const dark = 10 * 24 * time.Hour

	// Suspended on 19 February, runs on the 5th: the next activation is
	// 5 March, still ahead of the pinned clock, so the suspension has
	// cost nothing yet.
	monthly := cron("prod", "monthly", true, schedule("0 0 5 * *"), suspendedAt(dark))
	if recs := runCron(t, []runtime.Object{monthly}, "-A"); len(recs) != 0 {
		t.Errorf("a monthly job that has skipped nothing: want silence, got %v", recs)
	}

	hourly := cron("prod", "hourly", true, schedule("@hourly"), suspendedAt(dark))
	recs := runCron(t, []runtime.Object{hourly}, "-A")
	if len(recs) != 1 {
		t.Fatalf("an hourly job dark for 10 days: want one finding, got %v", recs)
	}
	if recs[0]["missed_runs"] != "240" {
		t.Errorf("missed_runs = %q, want 240", recs[0]["missed_runs"])
	}
}

func TestSuspendedCronJobDetails(t *testing.T) {
	cj := cron("prod", "backup", true, suspendedAt(30*24*time.Hour))
	cj.Spec.TimeZone = ptr("UTC")

	recs := runCron(t, []runtime.Object{cj}, "-A")
	if len(recs) != 1 {
		t.Fatalf("want one finding, got %v", recs)
	}
	for key, want := range map[string]string{
		"kind":            "audit.suspended_cronjob",
		"kind_of_object":  "CronJob",
		"namespace":       "prod",
		"name":            "backup",
		"reason":          "SuspendedCronJob",
		"schedule":        "@daily",
		"suspended_for":   "30d",
		"suspended_since": "2026-01-30T12:00:00Z",
		"anchor":          "managed_field",
		"missed_runs":     "30",
		"time_zone":       "UTC",
	} {
		if recs[0][key] != want {
			t.Errorf("%s = %q, want %q", key, recs[0][key], want)
		}
	}
	if !strings.HasPrefix(recs[0]["fingerprint"], "sha256:") {
		t.Errorf("no posture fingerprint: %v", recs[0])
	}
}

// The anchor ladder: managedFields first, then the last time the
// schedule fired, then creation. Which one was used changes what the
// age means, so the finding names it.
func TestSuspendedCronJobAnchorLadder(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mods       []func(*batchv1.CronJob)
		wantAnchor string
		wantFor    string
	}{
		{
			"managed field wins over lastScheduleTime",
			[]func(*batchv1.CronJob){suspendedAt(20 * 24 * time.Hour), lastScheduled(60 * 24 * time.Hour)},
			"managed_field", "20d",
		},
		{
			"lastScheduleTime when no entry owns suspend",
			[]func(*batchv1.CronJob){lastScheduled(60 * 24 * time.Hour)},
			"last_schedule", "60d",
		},
		{
			"creation for a CronJob suspended before it ever ran",
			nil,
			"creation", "90d",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recs := runCron(t, []runtime.Object{cron("prod", "backup", true, tc.mods...)}, "-A")
			if len(recs) != 1 {
				t.Fatalf("want one finding, got %v", recs)
			}
			if recs[0]["anchor"] != tc.wantAnchor {
				t.Errorf("anchor = %q, want %q", recs[0]["anchor"], tc.wantAnchor)
			}
			if recs[0]["suspended_for"] != tc.wantFor {
				t.Errorf("suspended_for = %q, want %q", recs[0]["suspended_for"], tc.wantFor)
			}
		})
	}
}

// A managedFields entry that does not own spec.suspend must not be
// mistaken for one that does — otherwise any controller touching the
// object resets the age.
func TestSuspendedCronJobIgnoresUnrelatedManagedFields(t *testing.T) {
	cj := cron("prod", "backup", true, lastScheduled(60*24*time.Hour))
	cj.ManagedFields = []metav1.ManagedFieldsEntry{
		{ // owns other spec fields, but not suspend
			Manager: "argocd", Time: ptr(cronAgo(time.Hour)), FieldsType: "FieldsV1",
			FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:schedule":{}}}`),
		},
		{ // owns suspend, but on the status subresource — impossible in
			// practice, and a decoy for a naive scan of the raw JSON
			Manager: "kube-controller-manager", Subresource: "status",
			Time: ptr(cronAgo(time.Hour)), FieldsType: "FieldsV1",
			FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:suspend":{}}}`),
		},
	}

	recs := runCron(t, []runtime.Object{cj}, "-A")
	if len(recs) != 1 {
		t.Fatalf("want one finding, got %v", recs)
	}
	if recs[0]["anchor"] != "last_schedule" {
		t.Errorf("anchor = %q, want last_schedule (neither entry owns spec.suspend)", recs[0]["anchor"])
	}
}

// A single skipped activation reads as one, not as "1 activations
// have been skipped".
func TestSuspendedCronJobSingleMissedRunReadsAsOne(t *testing.T) {
	// Suspended 30 January, runs on the 5th: February's activation is
	// the only one gone; March's is still ahead.
	recs := runCron(t, []runtime.Object{
		cron("prod", "monthly", true, schedule("0 0 5 * *"), suspendedAt(30*24*time.Hour)),
	}, "-A")
	if len(recs) != 1 {
		t.Fatalf("want one finding, got %v", recs)
	}
	if recs[0]["missed_runs"] != "1" {
		t.Errorf("missed_runs = %q, want 1", recs[0]["missed_runs"])
	}
	if !strings.Contains(recs[0]["message"], "1 activation has been skipped") {
		t.Errorf("message = %q, want singular agreement", recs[0]["message"])
	}
}

// FieldsV1 is opaque JSON. Anything that is not recognisably
// spec.suspend must fall through to the weaker anchors rather than
// being read as evidence.
func TestSuspendedCronJobFieldsV1Shapes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry metav1.ManagedFieldsEntry
		want  string
	}{
		{"no FieldsV1 at all", metav1.ManagedFieldsEntry{
			Manager: "x", Time: ptr(cronAgo(time.Hour)),
		}, "creation"},
		{"empty raw", metav1.ManagedFieldsEntry{
			Manager: "x", Time: ptr(cronAgo(time.Hour)), FieldsV1: metav1.NewFieldsV1(""),
		}, "creation"},
		{"malformed json", metav1.ManagedFieldsEntry{
			Manager: "x", Time: ptr(cronAgo(time.Hour)),
			FieldsV1: metav1.NewFieldsV1(`{"f:spec":`),
		}, "creation"},
		{"f:spec is not an object", metav1.ManagedFieldsEntry{
			Manager: "x", Time: ptr(cronAgo(time.Hour)),
			FieldsV1: metav1.NewFieldsV1(`{"f:spec":"suspend"}`),
		}, "creation"},
		{"no f:spec", metav1.ManagedFieldsEntry{
			Manager: "x", Time: ptr(cronAgo(time.Hour)),
			FieldsV1: metav1.NewFieldsV1(`{"f:metadata":{"f:labels":{}}}`),
		}, "creation"},
		{"no timestamp", metav1.ManagedFieldsEntry{
			Manager:  "x",
			FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:suspend":{}}}`),
		}, "creation"},
		{"owns spec.suspend", metav1.ManagedFieldsEntry{
			Manager: "x", Time: ptr(cronAgo(30 * 24 * time.Hour)),
			FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:suspend":{}}}`),
		}, "managed_field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cj := cron("prod", "backup", true)
			cj.ManagedFields = []metav1.ManagedFieldsEntry{tc.entry}
			recs := runCron(t, []runtime.Object{cj}, "-A", "--cron-suspended=30m")
			if len(recs) != 1 {
				t.Fatalf("want one finding, got %v", recs)
			}
			if recs[0]["anchor"] != tc.want {
				t.Errorf("anchor = %q, want %q", recs[0]["anchor"], tc.want)
			}
		})
	}
}

// A suspended CronJob with an unparseable schedule escapes `triage
// delta` (which skips it for being suspended); §2 forbids it escaping
// here too, so it is judged on age alone and says the count is
// unknown rather than inventing one.
func TestSuspendedCronJobUnparseableScheduleIsStillReported(t *testing.T) {
	recs := runCron(t, []runtime.Object{
		cron("prod", "bogus", true, schedule("not a schedule"), suspendedAt(30*24*time.Hour)),
	}, "-A")
	if len(recs) != 1 {
		t.Fatalf("want one finding, got %v", recs)
	}
	if recs[0]["missed_runs"] != "unknown" {
		t.Errorf("missed_runs = %q, want unknown", recs[0]["missed_runs"])
	}
	if !strings.Contains(recs[0]["message"], "could not be parsed") {
		t.Errorf("message = %q, want it to say why the count is missing", recs[0]["message"])
	}
}

// CronJobs count toward scanned and get their own slot in the
// workloads note, so a reader can tell an empty result from an
// unexamined one.
func TestSuspendedCronJobCountsInTheSummary(t *testing.T) {
	res := checktest.Run(t, audit.WorkloadsCommand(cronDeps(
		cron("prod", "a", true, suspendedAt(30*24*time.Hour)),
		cron("prod", "b", false),
	)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	sum := parseLine(t, lines[len(lines)-1])
	if sum["scanned"] != "2" {
		t.Errorf("scanned = %q, want 2", sum["scanned"])
	}
	if sum["workloads"] != "0/0/0/2" {
		t.Errorf("workloads note = %q, want 0/0/0/2", sum["workloads"])
	}
}

// --workload=CronJob/ns/name scopes to the one CronJob, and nothing
// else in the command's vocabulary applies to it.
func TestSuspendedCronJobWorkloadScope(t *testing.T) {
	objs := []runtime.Object{
		cron("prod", "backup", true, suspendedAt(30*24*time.Hour)),
		cron("prod", "other", true, suspendedAt(30*24*time.Hour)),
		deploy("prod", "checkout", 1, template(map[string]string{"app": "checkout"},
			container("app", false, false))),
	}
	recs := runCron(t, objs, "--workload=cj/prod/backup")
	if len(recs) != 1 || recs[0]["name"] != "backup" {
		t.Fatalf("want the one CronJob asked for, got %v", recs)
	}
}

func TestSuspendedCronJobWorkloadScopeNotFound(t *testing.T) {
	res := checktest.Run(t, audit.WorkloadsCommand(cronDeps(cron("prod", "backup", true))),
		"--workload=CronJob/prod/missing")
	if res.Code == emit.ExitData {
		t.Fatalf("a missing CronJob must not exit as data:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stderr, "not found") {
		t.Errorf("stderr = %q, want a not-found error", res.Stderr)
	}
}
