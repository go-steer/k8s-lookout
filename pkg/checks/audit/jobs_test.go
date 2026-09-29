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
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// batchJob builds a Job created 90 days before the pinned clock (cronNow).
// suspend and the suspension evidence are set by the modifiers.
func batchJob(ns, name string, suspend bool, mods ...func(*batchv1.Job)) *batchv1.Job {
	j := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name,
			CreationTimestamp: cronAgo(90 * 24 * time.Hour),
		},
		Spec: batchv1.JobSpec{Suspend: ptr(suspend)},
	}
	for _, m := range mods {
		m(j)
	}
	return j
}

func jobCond(t batchv1.JobConditionType, ago time.Duration) func(*batchv1.Job) {
	return func(j *batchv1.Job) {
		j.Status.Conditions = append(j.Status.Conditions, batchv1.JobCondition{
			Type: t, Status: corev1.ConditionTrue, LastTransitionTime: cronAgo(ago),
		})
	}
}

func jobSuspendedAt(d time.Duration) func(*batchv1.Job) {
	return func(j *batchv1.Job) {
		j.ManagedFields = append(j.ManagedFields, metav1.ManagedFieldsEntry{
			Manager:    "kubectl-patch",
			Operation:  metav1.ManagedFieldsOperationUpdate,
			APIVersion: "batch/v1",
			Time:       ptr(cronAgo(d)),
			FieldsType: "FieldsV1",
			FieldsV1:   metav1.NewFieldsV1(`{"f:spec":{"f:suspend":{}}}`),
		})
	}
}

func TestSuspendedJobIsReported(t *testing.T) {
	recs := runCron(t, []runtime.Object{
		batchJob("prod", "migrate-v42", true, jobCond(batchv1.JobSuspended, 30*24*time.Hour)),
	}, "-A")
	if len(recs) != 1 {
		t.Fatalf("want one finding, got %v", recs)
	}
	want := map[string]string{
		"kind":            "audit.suspended_job",
		"severity":        "info",
		"namespace":       "prod",
		"kind_of_object":  "Job",
		"name":            "migrate-v42",
		"reason":          "SuspendedJob",
		"suspended_for":   "30d",
		"suspended_since": "2026-01-30T12:00:00Z",
		"anchor":          "condition",
	}
	for k, v := range want {
		if recs[0][k] != v {
			t.Errorf("%s = %q, want %q", k, recs[0][k], v)
		}
	}
	if !strings.Contains(recs[0]["message"], "is not happening") {
		t.Errorf("message should say what is lost: %q", recs[0]["message"])
	}
}

// Running, queued, owned, externally managed and finished Jobs are
// all out of scope: only a Job a person suspended is judged.
func TestSuspendedJobSilentCases(t *testing.T) {
	old := jobCond(batchv1.JobSuspended, 30*24*time.Hour)
	recs := runCron(t, []runtime.Object{
		batchJob("prod", "running", false),
		batchJob("prod", "recent", true, jobCond(batchv1.JobSuspended, 2*24*time.Hour)),
		batchJob("prod", "from-cron", true, old, func(j *batchv1.Job) {
			j.OwnerReferences = []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "CronJob", Name: "nightly", UID: "u1", Controller: ptr(true)}}
		}),
		batchJob("prod", "queued", true, old, func(j *batchv1.Job) {
			j.Labels = map[string]string{"kueue.x-k8s.io/queue-name": "team-a"}
		}),
		batchJob("prod", "queued-old-kueue", true, old, func(j *batchv1.Job) {
			j.Annotations = map[string]string{"kueue.x-k8s.io/queue-name": "team-a"}
		}),
		batchJob("prod", "multikueue", true, old, func(j *batchv1.Job) {
			j.Spec.ManagedBy = ptr("kueue.x-k8s.io/multikueue")
		}),
		batchJob("prod", "done", true, old, jobCond(batchv1.JobComplete, 40*24*time.Hour)),
		batchJob("prod", "failed", true, old, jobCond(batchv1.JobFailed, 40*24*time.Hour)),
	}, "-A")
	if len(recs) != 0 {
		t.Errorf("want silence, got %v", recs)
	}
}

// A non-controller ownerReference (garbage-collection only) and the
// built-in controller's own managedBy value do not exempt a Job.
func TestSuspendedJobStillJudgedWhenOnlyNominallyOwned(t *testing.T) {
	old := jobCond(batchv1.JobSuspended, 30*24*time.Hour)
	recs := runCron(t, []runtime.Object{
		batchJob("prod", "gc-owned", true, old, func(j *batchv1.Job) {
			j.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "anchor", UID: "u2"}}
		}),
		batchJob("prod", "builtin", true, old, func(j *batchv1.Job) {
			j.Spec.ManagedBy = ptr("kubernetes.io/job-controller")
		}),
	}, "-A")
	if len(recs) != 2 {
		t.Errorf("want both judged, got %v", recs)
	}
}

func TestSuspendedJobAnchorLadder(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mods   []func(*batchv1.Job)
		anchor string
		since  string
	}{
		{"condition wins", []func(*batchv1.Job){jobSuspendedAt(10 * 24 * time.Hour), jobCond(batchv1.JobSuspended, 20*24*time.Hour)}, "condition", "2026-02-09T12:00:00Z"},
		{"managed field", []func(*batchv1.Job){jobSuspendedAt(10 * 24 * time.Hour)}, "managed_field", "2026-02-19T12:00:00Z"},
		{"creation", nil, "creation", "2025-12-01T12:00:00Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recs := runCron(t, []runtime.Object{batchJob("prod", "j", true, tc.mods...)}, "-A")
			if len(recs) != 1 {
				t.Fatalf("want one finding, got %v", recs)
			}
			if recs[0]["anchor"] != tc.anchor || recs[0]["suspended_since"] != tc.since {
				t.Errorf("anchor=%s since=%s, want %s %s", recs[0]["anchor"], recs[0]["suspended_since"], tc.anchor, tc.since)
			}
		})
	}
}

func TestSuspendedJobFloorFlag(t *testing.T) {
	objs := []runtime.Object{batchJob("prod", "j", true, jobCond(batchv1.JobSuspended, 2*24*time.Hour))}
	if recs := runCron(t, objs, "-A", "--job-suspended=24h"); len(recs) != 1 {
		t.Errorf("a lowered floor should report it, got %v", recs)
	}
	res := checktest.Run(t, audit.WorkloadsCommand(cronDeps(objs...)), "-A", "--job-suspended=-1h")
	if res.Code != emit.ExitUsage {
		t.Errorf("negative floor: exit %d, want usage", res.Code)
	}
}

func TestSuspendedJobCountsInTheSummary(t *testing.T) {
	res := checktest.Run(t, audit.WorkloadsCommand(cronDeps(
		batchJob("prod", "a", true),
		batchJob("prod", "b", false),
		cron("prod", "c", false),
	)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	sum := parseLine(t, lines[len(lines)-1])
	if sum["scanned"] != "3" || sum["jobs"] != "2" || sum["workloads"] != "0/0/0/1" {
		t.Errorf("summary = %v, want scanned=3 jobs=2 workloads=0/0/0/1", sum)
	}
}

func TestSuspendedJobWorkloadScope(t *testing.T) {
	objs := []runtime.Object{
		batchJob("prod", "migrate", true),
		batchJob("prod", "other", true),
		cron("prod", "migrate", true, suspendedAt(30*24*time.Hour)),
	}
	recs := runCron(t, objs, "--workload=job/prod/migrate")
	if len(recs) != 1 || recs[0]["kind_of_object"] != "Job" || recs[0]["name"] != "migrate" {
		t.Fatalf("want the one Job asked for, got %v", recs)
	}
}
