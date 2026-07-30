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

package workload

// §13 conventions mirrored from the rollout source's suite: the
// source is driven directly through its handlers (no informers) with
// a settable fake clock; Run/informer plumbing is covered by the
// end-to-end path everywhere else.

import (
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

func boolPtr(b bool) *bool    { return &b }
func strPtr(s string) *string { return &s }

type collector struct {
	mu   sync.Mutex
	sigs []engine.Signal
}

func (c *collector) emit(sig engine.Signal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sigs = append(c.sigs, sig)
}

func (c *collector) all() []engine.Signal {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]engine.Signal, len(c.sigs))
	copy(out, c.sigs)
	return out
}

var testStart = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

// newTestSource returns an armed source driven directly through its
// handlers with a settable fake clock.
func newTestSource(t *testing.T, cfg Config) (*Source, *collector, *time.Time) {
	t.Helper()
	s := New(fake.NewSimpleClientset(), cfg)
	col := &collector{}
	now := testStart
	clock := &now
	s.now = func() time.Time { return *clock }
	s.emit = col.emit
	s.armed = true
	s.armedAt = testStart
	return s, col, clock
}

// job builds a Job fixture; conditions are appended per the args.
func job(uid, ns, name string, owner *batchv1.CronJob, conds ...batchv1.JobCondition) *batchv1.Job {
	j := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Namespace: ns, Name: name},
		Status:     batchv1.JobStatus{Conditions: conds},
	}
	if owner != nil {
		j.OwnerReferences = []metav1.OwnerReference{{
			Kind: "CronJob", Name: owner.Name, UID: owner.UID, Controller: boolPtr(true),
		}}
	}
	return j
}

func failedCond(reason string) batchv1.JobCondition {
	return batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: reason}
}

func completeCond() batchv1.JobCondition {
	return batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}
}

// cronJob builds a CronJob fixture. lastSchedule nil = never ran.
func cronJob(uid, ns, name, schedule string, lastSchedule *time.Time) *batchv1.CronJob {
	c := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID(uid), Namespace: ns, Name: name,
			CreationTimestamp: metav1.Time{Time: testStart.Add(-24 * time.Hour)},
		},
		Spec: batchv1.CronJobSpec{Schedule: schedule},
	}
	if lastSchedule != nil {
		c.Status.LastScheduleTime = &metav1.Time{Time: *lastSchedule}
	}
	return c
}

// ---- job_failed ----

func TestJobFailedTransition(t *testing.T) {
	s, col, _ := newTestSource(t, Config{})
	// Running job first, then the failure transition.
	s.onJob(job("j1", "prod", "migrate", nil))
	s.onJob(job("j1", "prod", "migrate", nil, failedCond("BackoffLimitExceeded")))

	sigs := col.all()
	if len(sigs) != 1 {
		t.Fatalf("got %d signals, want 1: %+v", len(sigs), sigs)
	}
	sig := sigs[0]
	if sig.Kind != KindJobFailed || sig.Key.Reason != ReasonJobFailed || sig.Key.UID != "j1" {
		t.Errorf("signal identity = %s/%s/%s", sig.Kind, sig.Key.Reason, sig.Key.UID)
	}
	if sig.Severity != engine.SeverityWarning {
		t.Errorf("severity = %s", sig.Severity)
	}
	if !strings.Contains(sig.Message, "BackoffLimitExceeded") || !strings.Contains(sig.Message, "standalone Job") {
		t.Errorf("message = %q", sig.Message)
	}

	// Re-observing the same failed job never re-fires.
	s.onJob(job("j1", "prod", "migrate", nil, failedCond("BackoffLimitExceeded")))
	if len(col.all()) != 1 {
		t.Error("re-observation re-fired")
	}
}

func TestJobFailedPreExistingIsHistory(t *testing.T) {
	s, col, _ := newTestSource(t, Config{})
	s.armed = false
	// The initial LIST delivers an already-failed Job un-armed.
	s.onJob(job("j1", "prod", "migrate", nil, failedCond("DeadlineExceeded")))
	s.armed = true
	// Post-arm re-observation of the same state: still history.
	s.onJob(job("j1", "prod", "migrate", nil, failedCond("DeadlineExceeded")))
	if n := len(col.all()); n != 0 {
		t.Fatalf("pre-existing failure emitted %d signals, want 0", n)
	}
}

func TestJobFailedCronOwnedNamesOwner(t *testing.T) {
	s, col, _ := newTestSource(t, Config{})
	cj := cronJob("c1", "prod", "nightly", "0 3 * * *", nil)
	s.onJob(job("j1", "prod", "nightly-29100240", cj))
	s.onJob(job("j1", "prod", "nightly-29100240", cj, failedCond("BackoffLimitExceeded")))
	sigs := col.all()
	if len(sigs) != 1 {
		t.Fatalf("got %d signals, want 1", len(sigs))
	}
	if !strings.Contains(sigs[0].Message, "CronJob nightly") {
		t.Errorf("message does not name the owner: %q", sigs[0].Message)
	}
}

// ---- job_failed clearance ----

func jobIncident(uid string) engine.Incident {
	return engine.Incident{Key: engine.EventKey{UID: uid, Reason: ReasonJobFailed}}
}

func TestJobClearanceStandaloneTerminal(t *testing.T) {
	s, _, _ := newTestSource(t, Config{})
	s.onJob(job("j1", "prod", "migrate", nil))
	s.onJob(job("j1", "prod", "migrate", nil, failedCond("BackoffLimitExceeded")))

	cl, ok := s.Clearance(jobIncident("j1"))
	if !ok || cl.Cleared {
		t.Fatalf("standalone failed job: clearance = %+v ok=%v, want held open", cl, ok)
	}
	// Deletion is the only exit.
	s.onJobDelete(job("j1", "prod", "migrate", nil))
	cl, ok = s.Clearance(jobIncident("j1"))
	if !ok || !cl.Cleared || cl.Resolution != engine.ResolutionObjectDeleted {
		t.Fatalf("deleted job: clearance = %+v ok=%v", cl, ok)
	}
}

func TestJobClearanceSiblingSuccess(t *testing.T) {
	s, _, clock := newTestSource(t, Config{})
	cj := cronJob("c1", "prod", "nightly", "0 3 * * *", nil)
	s.onJob(job("j1", "prod", "nightly-a", cj))
	s.onJob(job("j1", "prod", "nightly-a", cj, failedCond("BackoffLimitExceeded")))

	cl, ok := s.Clearance(jobIncident("j1"))
	if !ok || cl.Cleared {
		t.Fatalf("no sibling success yet: clearance = %+v ok=%v", cl, ok)
	}

	// The next run succeeds 30m later.
	*clock = clock.Add(30 * time.Minute)
	successAt := *clock
	s.onJob(job("j2", "prod", "nightly-b", cj, completeCond()))

	cl, ok = s.Clearance(jobIncident("j1"))
	if !ok || !cl.Cleared || cl.Resolution != engine.ResolutionRecovered {
		t.Fatalf("sibling success: clearance = %+v ok=%v", cl, ok)
	}
	if !cl.StableSince.Equal(successAt) {
		t.Errorf("StableSince = %v, want the success OBSERVATION %v", cl.StableSince, successAt)
	}
}

func TestJobClearanceSuccessBeforeFailureDoesNotClear(t *testing.T) {
	s, _, clock := newTestSource(t, Config{})
	cj := cronJob("c1", "prod", "nightly", "0 3 * * *", nil)
	// A success observed BEFORE the failure must not clear it.
	s.onJob(job("j0", "prod", "nightly-old", cj, completeCond()))
	*clock = clock.Add(10 * time.Minute)
	s.onJob(job("j1", "prod", "nightly-a", cj))
	s.onJob(job("j1", "prod", "nightly-a", cj, failedCond("BackoffLimitExceeded")))

	cl, ok := s.Clearance(jobIncident("j1"))
	if !ok || cl.Cleared {
		t.Fatalf("stale success cleared a newer failure: %+v ok=%v", cl, ok)
	}
}

// ---- cron_missed ----

func cronIncident(uid string) engine.Incident {
	return engine.Incident{Key: engine.EventKey{UID: uid, Reason: ReasonCronMissed}}
}

func TestCronMissedFiresAfterGrace(t *testing.T) {
	s, col, clock := newTestSource(t, Config{Grace: 5 * time.Minute})
	// Hourly schedule; last ran at 11:00, so 12:00 is the next
	// expected activation (armedAt is 12:00 — anchor is max of both;
	// lastSchedule 11:00 < armedAt, so anchor = armedAt = 12:00,
	// expected = 13:00).
	last := testStart.Add(-time.Hour)
	s.onCronJob(cronJob("c1", "prod", "hourly", "0 * * * *", &last))

	// At 13:04 (inside grace) nothing fires.
	*clock = testStart.Add(time.Hour + 4*time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 0 {
		t.Fatalf("fired inside grace: %+v", sigs)
	}
	// At 13:06 the 13:00 activation is missed.
	*clock = testStart.Add(time.Hour + 6*time.Minute)
	sigs := s.sweep(*clock)
	if len(sigs) != 1 {
		t.Fatalf("got %d signals, want 1: %+v", len(sigs), sigs)
	}
	sig := sigs[0]
	if sig.Kind != KindCronMissed || sig.Key.UID != "c1" || sig.Key.Reason != ReasonCronMissed {
		t.Errorf("signal identity = %s/%s/%s", sig.Kind, sig.Key.Reason, sig.Key.UID)
	}
	if sig.Severity != engine.SeverityWarning {
		t.Errorf("severity = %s", sig.Severity)
	}
	if !strings.Contains(sig.Message, `schedule "0 * * * *"`) {
		t.Errorf("message = %q", sig.Message)
	}
	// The same activation never re-fires; sweeps stay quiet until the
	// NEXT activation's grace elapses.
	if sigs := s.sweep(clock.Add(time.Minute)); len(sigs) != 0 {
		t.Errorf("same activation re-fired: %+v", sigs)
	}
	_ = col
}

func TestCronMissedEscalatesToCritical(t *testing.T) {
	s, _, clock := newTestSource(t, Config{Grace: 5 * time.Minute})
	s.onCronJob(cronJob("c1", "prod", "hourly", "0 * * * *", nil))

	var last engine.Signal
	fired := 0
	// Walk the clock across CriticalMisses activations.
	for h := 1; fired < CriticalMisses; h++ {
		*clock = testStart.Add(time.Duration(h)*time.Hour + 6*time.Minute)
		for _, sig := range s.sweep(*clock) {
			last, fired = sig, fired+1
		}
	}
	if last.Severity != engine.SeverityCritical {
		t.Errorf("severity after %d consecutive misses = %s, want critical", CriticalMisses, last.Severity)
	}
	if !strings.Contains(last.Message, "consecutive_missed=3") {
		t.Errorf("message = %q", last.Message)
	}
}

func TestCronMissedNoBackfillAcrossArming(t *testing.T) {
	s, _, clock := newTestSource(t, Config{Grace: 5 * time.Minute})
	// The CronJob missed runs for a DAY before the sentinel started
	// (lastSchedule 24h ago). Misses are judged from armedAt forward:
	// the first reportable activation is 13:00, not 24 stale ones.
	last := testStart.Add(-24 * time.Hour)
	s.onCronJob(cronJob("c1", "prod", "hourly", "0 * * * *", &last))
	*clock = testStart.Add(time.Hour + 6*time.Minute)
	sigs := s.sweep(*clock)
	if len(sigs) != 1 {
		t.Fatalf("got %d signals, want exactly 1 (no backfill): %+v", len(sigs), sigs)
	}
}

func TestCronMissedSuspendedNeverFires(t *testing.T) {
	s, _, clock := newTestSource(t, Config{Grace: 5 * time.Minute})
	cj := cronJob("c1", "prod", "hourly", "0 * * * *", nil)
	cj.Spec.Suspend = boolPtr(true)
	s.onCronJob(cj)
	*clock = testStart.Add(2*time.Hour + 6*time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 0 {
		t.Fatalf("suspended cron fired: %+v", sigs)
	}
}

func TestCronMissedTimeZone(t *testing.T) {
	s, _, clock := newTestSource(t, Config{Grace: 5 * time.Minute})
	cj := cronJob("c1", "prod", "daily", "0 9 * * *", nil)
	cj.Spec.TimeZone = strPtr("America/New_York")
	s.onCronJob(cj)
	// 09:00 America/New_York on 2026-07-25 is 13:00 UTC. At 12:30
	// UTC nothing is due; at 13:06 UTC the activation is missed.
	*clock = testStart.Add(30 * time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 0 {
		t.Fatalf("fired before the zoned activation: %+v", sigs)
	}
	*clock = testStart.Add(time.Hour + 6*time.Minute)
	sigs := s.sweep(*clock)
	if len(sigs) != 1 {
		t.Fatalf("zoned activation: got %d signals, want 1", len(sigs))
	}
}

// ---- cron_missed clearance ----

func TestCronClearanceScheduleResumes(t *testing.T) {
	s, _, clock := newTestSource(t, Config{Grace: 5 * time.Minute})
	s.onCronJob(cronJob("c1", "prod", "hourly", "0 * * * *", nil))
	*clock = testStart.Add(time.Hour + 6*time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 1 {
		t.Fatalf("setup: expected the miss to fire")
	}

	cl, ok := s.Clearance(cronIncident("c1"))
	if !ok || cl.Cleared {
		t.Fatalf("still missing: clearance = %+v ok=%v", cl, ok)
	}

	// The 14:00 activation actually runs; the informer reports the
	// advanced lastScheduleTime at 14:01.
	*clock = testStart.Add(2*time.Hour + time.Minute)
	resumedObserved := *clock
	ran := testStart.Add(2 * time.Hour)
	s.onCronJob(cronJob("c1", "prod", "hourly", "0 * * * *", &ran))

	cl, ok = s.Clearance(cronIncident("c1"))
	if !ok || !cl.Cleared || cl.Resolution != engine.ResolutionRecovered {
		t.Fatalf("resumed: clearance = %+v ok=%v", cl, ok)
	}
	if !cl.StableSince.Equal(resumedObserved) {
		t.Errorf("StableSince = %v, want the resume OBSERVATION %v", cl.StableSince, resumedObserved)
	}
}

func TestCronClearanceSuspend(t *testing.T) {
	s, _, clock := newTestSource(t, Config{Grace: 5 * time.Minute})
	s.onCronJob(cronJob("c1", "prod", "hourly", "0 * * * *", nil))
	*clock = testStart.Add(time.Hour + 6*time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 1 {
		t.Fatalf("setup: expected the miss to fire")
	}

	*clock = clock.Add(10 * time.Minute)
	suspendObserved := *clock
	cj := cronJob("c1", "prod", "hourly", "0 * * * *", nil)
	cj.Spec.Suspend = boolPtr(true)
	s.onCronJob(cj)

	cl, ok := s.Clearance(cronIncident("c1"))
	if !ok || !cl.Cleared || cl.Resolution != engine.ResolutionRecovered {
		t.Fatalf("suspended: clearance = %+v ok=%v", cl, ok)
	}
	if !cl.StableSince.Equal(suspendObserved) {
		t.Errorf("StableSince = %v, want the suspend OBSERVATION %v", cl.StableSince, suspendObserved)
	}
}

func TestCronClearanceRestoredIncident(t *testing.T) {
	// Restart posture: the incident predates this process, so there
	// is no miss memory (tracks empty of lastMissed). A run observed
	// since arming proves the schedule recovered; a pre-arm
	// lastSchedule proves nothing.
	s, _, clock := newTestSource(t, Config{})
	preArm := testStart.Add(-2 * time.Hour)
	s.onCronJob(cronJob("c1", "prod", "hourly", "0 * * * *", &preArm))

	cl, ok := s.Clearance(cronIncident("c1"))
	if !ok || cl.Cleared {
		t.Fatalf("pre-arm lastSchedule cleared a restored incident: %+v ok=%v", cl, ok)
	}

	*clock = testStart.Add(time.Hour + time.Minute)
	resumeObserved := *clock
	ran := testStart.Add(time.Hour)
	s.onCronJob(cronJob("c1", "prod", "hourly", "0 * * * *", &ran))

	cl, ok = s.Clearance(cronIncident("c1"))
	if !ok || !cl.Cleared || cl.Resolution != engine.ResolutionRecovered {
		t.Fatalf("post-arm run must clear a restored incident: %+v ok=%v", cl, ok)
	}
	if !cl.StableSince.Equal(resumeObserved) {
		t.Errorf("StableSince = %v, want the run OBSERVATION %v", cl.StableSince, resumeObserved)
	}
}

func TestQuietObjectsSurviveLongUptime(t *testing.T) {
	// Quiet live objects must never age out of the mirror: a
	// terminally failed Job and a dead schedule produce no informer
	// updates, and a last-seen prune would falsely resolve their
	// incidents as object_deleted (the review finding on the removed
	// StateTTL prune).
	s, _, clock := newTestSource(t, Config{Grace: 5 * time.Minute})
	s.onJob(job("j1", "prod", "migrate", nil))
	s.onJob(job("j1", "prod", "migrate", nil, failedCond("BackoffLimitExceeded")))
	s.onCronJob(cronJob("c1", "prod", "hourly", "0 * * * *", nil))
	*clock = testStart.Add(time.Hour + 6*time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 1 {
		t.Fatalf("setup: expected the miss to fire")
	}

	// Three days later, without a single informer touch:
	*clock = testStart.Add(72 * time.Hour)
	s.sweep(*clock)

	cl, ok := s.Clearance(jobIncident("j1"))
	if !ok || cl.Cleared {
		t.Fatalf("quiet failed job aged into %+v ok=%v, want held open", cl, ok)
	}
	cl, ok = s.Clearance(cronIncident("c1"))
	if !ok || cl.Cleared {
		t.Fatalf("quiet dead schedule aged into %+v ok=%v, want held open", cl, ok)
	}
}

func TestCronClearanceDeleted(t *testing.T) {
	s, _, _ := newTestSource(t, Config{})
	s.onCronJob(cronJob("c1", "prod", "hourly", "0 * * * *", nil))
	s.onCronJobDelete(cronJob("c1", "prod", "hourly", "0 * * * *", nil))
	cl, ok := s.Clearance(cronIncident("c1"))
	if !ok || !cl.Cleared || cl.Resolution != engine.ResolutionObjectDeleted {
		t.Fatalf("deleted: clearance = %+v ok=%v", cl, ok)
	}
}

// ---- boundaries ----

func TestClearanceForeignReason(t *testing.T) {
	s, _, _ := newTestSource(t, Config{})
	if _, ok := s.Clearance(engine.Incident{Key: engine.EventKey{UID: "x", Reason: "rollout_stall"}}); ok {
		t.Error("claimed a foreign reason")
	}
}

func TestClearanceUnarmed(t *testing.T) {
	s, _, _ := newTestSource(t, Config{})
	s.armed = false
	if _, ok := s.Clearance(jobIncident("j1")); ok {
		t.Error("judged against an unsynced mirror")
	}
	if _, ok := s.Clearance(cronIncident("c1")); ok {
		t.Error("judged against an unsynced mirror")
	}
}

func TestRequiredAccess(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{})
	reqs := s.RequiredAccess()
	want := map[string]bool{
		"batch/jobs/list": true, "batch/jobs/watch": true,
		"batch/cronjobs/list": true, "batch/cronjobs/watch": true,
	}
	for _, r := range reqs {
		key := r.Group + "/" + r.Resource + "/" + r.Verb
		if !want[key] {
			t.Errorf("unexpected requirement %s", key)
		}
		delete(want, key)
	}
	for key := range want {
		t.Errorf("missing requirement %s", key)
	}
}
