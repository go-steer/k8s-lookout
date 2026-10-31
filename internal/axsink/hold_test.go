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

package axsink

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/internal/axapi"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// fakeClock is a settable clock for the hold logic.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// outcomes collects UseReminderCounter calls.
type outcomes struct {
	mu sync.Mutex
	n  map[string]int
}

func (o *outcomes) count(outcome string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.n[outcome]++
}

func (o *outcomes) get(outcome string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.n[outcome]
}

type holdRig struct {
	sink   *Sink
	ax     *fakeAX
	router *fakeRouter
	clock  *fakeClock
	counts *outcomes
}

func newHoldRig(t *testing.T, scope Scope, tmpl *axapi.Task) *holdRig {
	t.Helper()
	client, ax := newFakeAXClient(t)
	router := &fakeRouter{}
	hs := httptest.NewServer(router)
	t.Cleanup(hs.Close)
	clock := &fakeClock{t: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}
	if tmpl == nil {
		tmpl = testTemplate()
	}
	sink, err := New(Config{
		Client:       client,
		Template:     tmpl,
		RouterURL:    hs.URL,
		BearerToken:  "tok",
		Scope:        scope,
		StartTimeout: 10 * time.Second,
		Now:          clock.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	counts := &outcomes{n: map[string]int{}}
	sink.UseReminderCounter("prod", counts.count)
	return &holdRig{sink: sink, ax: ax, router: router, clock: clock, counts: counts}
}

func crashPayload(repeat int) inject.Payload {
	return inject.Payload{Kind: "k8s-event-followup", Cluster: "prod", Reason: "CrashLoopBackOff", UID: "u1", RepeatCount: repeat}
}

// calls snapshots what reached AX and the router.
func (r *holdRig) calls() (resumes, gets, requests, injects int) {
	r.ax.mu.Lock()
	resumes, gets = len(r.ax.resumed), r.ax.gets
	r.ax.mu.Unlock()
	r.router.mu.Lock()
	requests, injects = r.router.requests, len(r.router.order)
	r.router.mu.Unlock()
	return
}

func (r *holdRig) order() []string {
	r.router.mu.Lock()
	defer r.router.mu.Unlock()
	return append([]string(nil), r.router.order...)
}

func (r *holdRig) open(t *testing.T, key inject.IncidentKey, p inject.Payload) string {
	t.Helper()
	id, err := r.sink.OpenIncidentKeyed(context.Background(), key, p)
	if err != nil {
		t.Fatalf("OpenIncidentKeyed: %v", err)
	}
	return id
}

func (r *holdRig) appendOK(t *testing.T, id string, p any) {
	t.Helper()
	if err := r.sink.Append(context.Background(), id, p); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

// A reminder for an idle task is held: no ResumeTask and nothing through the
// router, so nothing can wake the task. The one AX call is a GetTask, a read
// of AX's record that never wakes it.
func TestHold_ReminderToIdleTaskIsHeld(t *testing.T) {
	r := newHoldRig(t, ScopeIncident, nil)
	id := r.open(t, crashKey, crashPayload(0))
	r.clock.advance(defaultIdleAfter)

	resumes, gets, requests, _ := r.calls()
	r.appendOK(t, id, crashPayload(1))
	resumes2, gets2, requests2, _ := r.calls()
	if resumes2 != resumes || requests2 != requests {
		t.Errorf("a held reminder could wake the task: resumes %d->%d, router requests %d->%d",
			resumes, resumes2, requests, requests2)
	}
	if gets2 > gets+1 {
		t.Errorf("GetTask calls %d->%d, want at most one read", gets, gets2)
	}
	if got := r.sink.heldFor(r.sink.taskFor(crashKey)); len(got) != 1 || got[0] != id {
		t.Errorf("held = %v, want [%s]", got, id)
	}
	if r.counts.get(ReminderHeld) != 1 {
		t.Errorf("held count = %d, want 1", r.counts.get(ReminderHeld))
	}
}

// Within the idle window after a real inject, the task is busy: the reminder
// goes straight through.
func TestHold_ReminderToBusyTaskGoesThrough(t *testing.T) {
	r := newHoldRig(t, ScopeIncident, nil)
	id := r.open(t, crashKey, crashPayload(0))
	r.clock.advance(time.Minute)

	r.appendOK(t, id, crashPayload(1))
	order := r.order()
	if len(order) != 2 || !strings.Contains(order[1], `"repeat_count":1`) {
		t.Fatalf("reminder not delivered at once: %v", order)
	}
	if len(r.sink.heldFor(r.sink.taskFor(crashKey))) != 0 {
		t.Error("a busy task's reminder was held")
	}
	if r.counts.get(ReminderDirect) != 1 {
		t.Errorf("direct count = %d, want 1", r.counts.get(ReminderDirect))
	}
}

// A recent real inject is not enough if AX records the task as suspended
// (on completion, or by hand): the reminder is held.
func TestHold_SuspendedPhaseHolds(t *testing.T) {
	r := newHoldRig(t, ScopeIncident, nil)
	id := r.open(t, crashKey, crashPayload(0))
	r.clock.advance(time.Minute)
	r.ax.mu.Lock()
	r.ax.phase = "Suspended"
	r.ax.mu.Unlock()

	resumes, _, _, injects := r.calls()
	r.appendOK(t, id, crashPayload(1))
	resumes2, _, _, injects2 := r.calls()
	if resumes2 != resumes || injects2 != injects {
		t.Errorf("reminder to a suspended task was delivered (resumes %d->%d, injects %d->%d)", resumes, resumes2, injects, injects2)
	}
	if len(r.sink.heldFor(r.sink.taskFor(crashKey))) != 1 {
		t.Error("reminder to a suspended task was not held")
	}
}

// Phase Unknown (AX could not reach Substrate) says nothing about the task,
// so it is treated like a failed GetTask: the sink's record decides. It is
// never "gone": the incident stays bound and nothing answers 404.
func TestHold_UnknownPhaseFallsBackToTheRecord(t *testing.T) {
	cases := []struct {
		name    string
		since   time.Duration // since the open, the last real inject
		wantOut string
	}{
		{"recent real inject: sent", time.Minute, ReminderDirect},
		{"idle: held", defaultIdleAfter, ReminderHeld},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := newHoldRig(t, ScopeIncident, nil)
			id := r.open(t, crashKey, crashPayload(0))
			r.clock.advance(c.since)
			r.ax.mu.Lock()
			r.ax.phase = phaseUnknown
			r.ax.mu.Unlock()

			resumes, _, _, _ := r.calls()
			if err := r.sink.Append(context.Background(), id, crashPayload(1)); err != nil {
				t.Fatalf("Append with phase Unknown = %v, want nil (never a gone 404)", err)
			}
			if r.counts.get(c.wantOut) != 1 {
				t.Errorf("outcome %s not counted (held=%d direct=%d)", c.wantOut, r.counts.get(ReminderHeld), r.counts.get(ReminderDirect))
			}
			if resumes2, _, _, _ := r.calls(); resumes2 != resumes {
				t.Errorf("a reminder resumed the task (resumes %d->%d)", resumes, resumes2)
			}
			if r.counts.get(ReminderDropped) != 0 {
				t.Errorf("dropped = %d, want 0: Unknown is not gone", r.counts.get(ReminderDropped))
			}
		})
	}
}

// Held reminders fold to the latest per session, and it is delivered ahead
// of the next real inject (here a material change).
func TestHold_LatestOnlyDeliveredAheadOfTheNextRealInject(t *testing.T) {
	r := newHoldRig(t, ScopeIncident, nil)
	id := r.open(t, crashKey, crashPayload(0))
	r.clock.advance(defaultIdleAfter)
	for n := 1; n <= 3; n++ {
		r.appendOK(t, id, crashPayload(n))
		r.clock.advance(5 * time.Minute)
	}
	if got := len(r.order()); got != 1 {
		t.Fatalf("held reminders were delivered early: %d injects", got)
	}

	change := crashPayload(0)
	change.Message = "a different message"
	r.appendOK(t, id, change)
	order := r.order()
	if len(order) != 3 {
		t.Fatalf("want open + latest reminder + change, got %v", order)
	}
	if !strings.Contains(order[1], `"repeat_count":3`) {
		t.Errorf("delivered reminder is not the latest: %s", order[1])
	}
	if strings.Contains(order[2], "repeat_count") || !strings.Contains(order[2], "a different message") {
		t.Errorf("the change was not delivered after the reminder: %s", order[2])
	}
	if len(r.sink.heldFor(r.sink.taskFor(crashKey))) != 0 {
		t.Error("the reminder is still held after delivery")
	}
	if r.counts.get(ReminderHeld) != 3 || r.counts.get(ReminderSuperseded) != 2 || r.counts.get(ReminderDelivered) != 1 {
		t.Errorf("counts held=%d superseded=%d delivered=%d, want 3, 2, 1",
			r.counts.get(ReminderHeld), r.counts.get(ReminderSuperseded), r.counts.get(ReminderDelivered))
	}

	// The change woke the task: the next reminder within the window is busy.
	r.clock.advance(time.Minute)
	r.appendOK(t, id, crashPayload(1))
	if got := len(r.order()); got != 4 {
		t.Errorf("reminder after a real inject was not delivered at once (%d injects)", got)
	}
}

// A resolve delivers the held reminder first, then the resolution, so the
// agent reads the incident's history in order.
func TestHold_ResolveDeliversTheHeldReminderFirst(t *testing.T) {
	r := newHoldRig(t, ScopeIncident, nil)
	id := r.open(t, crashKey, crashPayload(0))
	r.clock.advance(defaultIdleAfter)
	r.appendOK(t, id, crashPayload(2))

	r.appendOK(t, id, inject.ResolvedPayload{Kind: "resolved"})
	order := r.order()
	if len(order) != 3 || !strings.Contains(order[1], `"repeat_count":2`) || !strings.Contains(order[2], `"kind":"resolved"`) {
		t.Fatalf("want open, reminder, resolved; got %v", order)
	}
}

// In cluster scope a new incident's open wakes the shared task, so another
// incident's held reminder goes ahead of it.
func TestHold_ClusterScopeNewIncidentDeliversHeld(t *testing.T) {
	r := newHoldRig(t, ScopeCluster, nil)
	crashID := r.open(t, crashKey, crashPayload(0))
	r.clock.advance(defaultIdleAfter)
	r.appendOK(t, crashID, crashPayload(1))
	if got := len(r.order()); got != 1 {
		t.Fatalf("reminder delivered to an idle task: %d injects", got)
	}

	oomID := r.open(t, oomKey, inject.Payload{Kind: "k8s-event", Cluster: "prod", Reason: "OOMKilled", UID: "u2"})
	order := r.order()
	if len(order) != 3 {
		t.Fatalf("want open, held reminder, new open; got %v", order)
	}
	_, crashSID, _ := splitID(crashID)
	_, oomSID, _ := splitID(oomID)
	if !strings.HasPrefix(order[1], "/sessions/"+crashSID+"/inject") || !strings.Contains(order[1], `"repeat_count":1`) {
		t.Errorf("held reminder not delivered to its own session first: %s", order[1])
	}
	if !strings.HasPrefix(order[2], "/sessions/"+oomSID+"/inject") {
		t.Errorf("new incident not opened after the reminder: %s", order[2])
	}
}

// The idle window is the template's spec.idle.suspendAfter when it has one.
func TestHold_IdleWindowFromTheTemplate(t *testing.T) {
	tmpl := testTemplate()
	tmpl.Spec.Idle = &axapi.TaskIdle{SuspendAfter: "2m"}
	r := newHoldRig(t, ScopeIncident, tmpl)
	if r.sink.idleAfter != 2*time.Minute {
		t.Fatalf("idleAfter = %s, want 2m", r.sink.idleAfter)
	}
	id := r.open(t, crashKey, crashPayload(0))
	r.clock.advance(90 * time.Second)
	r.appendOK(t, id, crashPayload(1)) // busy: delivered
	r.clock.advance(3 * time.Minute)
	r.appendOK(t, id, crashPayload(2)) // idle: held
	if got := len(r.order()); got != 2 {
		t.Errorf("injects = %d, want open + one direct reminder", got)
	}
	if len(r.sink.heldFor(r.sink.taskFor(crashKey))) != 1 {
		t.Error("reminder past the template's idle window was not held")
	}
}

// A held reminder nothing comes for in heldTTL is dropped, not delivered
// hours later.
func TestHold_StaleHeldReminderIsDropped(t *testing.T) {
	r := newHoldRig(t, ScopeIncident, nil)
	id := r.open(t, crashKey, crashPayload(0))
	r.clock.advance(defaultIdleAfter)
	r.appendOK(t, id, crashPayload(1))
	r.clock.advance(heldTTL)

	r.appendOK(t, id, inject.ResolvedPayload{Kind: "resolved"})
	order := r.order()
	if len(order) != 2 || strings.Contains(order[1], "repeat_count") {
		t.Errorf("a stale reminder was delivered: %v", order)
	}
	if r.counts.get(ReminderDropped) != 1 {
		t.Errorf("dropped count = %d, want 1", r.counts.get(ReminderDropped))
	}
}

// Held reminders are bounded: past maxHeld the oldest goes.
func TestHold_BoundedAcrossSessions(t *testing.T) {
	r := newHoldRig(t, ScopeIncident, nil)
	for i := 0; i <= maxHeld; i++ {
		r.sink.holdReminder(fmt.Sprintf("lookout-t/attach-%d", i), "prod", crashPayload(1))
		r.clock.advance(time.Millisecond)
	}
	held := r.sink.heldFor("lookout-t")
	if len(held) != maxHeld {
		t.Fatalf("held %d reminders, want %d", len(held), maxHeld)
	}
	for _, id := range held {
		if id == "lookout-t/attach-0" {
			t.Error("the oldest reminder survived the bound")
		}
	}
	if r.counts.get(ReminderDropped) != 1 {
		t.Errorf("dropped count = %d, want 1", r.counts.get(ReminderDropped))
	}
}

// After a restart the sink has no record of real injects, so a reminder is
// held until the next real inject (accepted: it only repeats what the agent
// already knows).
func TestHold_NoRecordMeansIdle(t *testing.T) {
	r := newHoldRig(t, ScopeIncident, nil)
	task := r.sink.taskFor(crashKey)
	r.ax.mu.Lock()
	r.ax.created[task] = testTemplate() // the task outlived the restart
	r.ax.mu.Unlock()
	r.appendOK(t, task+"/attach-1", crashPayload(4))
	if len(r.order()) != 0 || len(r.sink.heldFor(task)) != 1 {
		t.Error("a reminder with no record of the task was not held")
	}
}

// A hung AX API can't stall the dispatcher: the GetTask is bounded by
// getTaskTimeout, and past it the sink's record decides — sent if a real
// inject was recent, held otherwise. Append is called with a context that
// has no deadline, as the dispatcher's is.
func TestHold_HungGetTaskIsBounded(t *testing.T) {
	cases := []struct {
		name    string
		since   time.Duration // since the open, the last real inject
		wantOut string
	}{
		{"recent real inject: sent", time.Minute, ReminderDirect},
		{"idle: held", defaultIdleAfter, ReminderHeld},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := newHoldRig(t, ScopeIncident, nil)
			id := r.open(t, crashKey, crashPayload(0))
			r.clock.advance(c.since)
			r.ax.mu.Lock()
			r.ax.hang = true
			r.ax.mu.Unlock()

			start := time.Now()
			r.appendOK(t, id, crashPayload(1))
			if took := time.Since(start); took > getTaskTimeout+2*time.Second {
				t.Errorf("Append took %s against a hung AX API, want about %s", took, getTaskTimeout)
			}
			if r.counts.get(c.wantOut) != 1 {
				t.Errorf("outcome %s not counted (held=%d direct=%d)", c.wantOut, r.counts.get(ReminderHeld), r.counts.get(ReminderDirect))
			}
		})
	}
}

// A reminder after an open whose payload never arrived may be the agent's
// first word on the incident: it is never held, however idle the task.
func TestHold_NeverHoldsTheFirstDelivery(t *testing.T) {
	r := newHoldRig(t, ScopeIncident, nil)
	r.router.mu.Lock()
	r.router.failWith = 500
	r.router.mu.Unlock()
	id, err := r.sink.OpenIncidentKeyed(context.Background(), crashKey, crashPayload(0))
	if err == nil || id == "" {
		t.Fatalf("want a partial open (session, failed inject), got id=%q err=%v", id, err)
	}
	r.router.mu.Lock()
	r.router.failWith = 0
	r.router.mu.Unlock()
	r.clock.advance(defaultIdleAfter)

	r.appendOK(t, id, crashPayload(1))
	order := r.order()
	if len(order) != 1 || !strings.Contains(order[0], `"repeat_count":1`) {
		t.Fatalf("the first delivery was held: %v", order)
	}
	// Delivered: the incident is now known to the agent, so the next
	// reminder to the idle task is held as usual.
	r.clock.advance(defaultIdleAfter)
	r.appendOK(t, id, crashPayload(2))
	if len(r.order()) != 1 || len(r.sink.heldFor(r.sink.taskFor(crashKey))) != 1 {
		t.Error("the reminder after a delivered one was not held")
	}
}

// A reminder for a task AX no longer has is not held for a wake that will
// never come: earlier held reminders are dropped and Append answers 404, the
// same as for a lost session, so the dispatcher can reopen the incident.
func TestHold_GoneTaskDropsAndAnswers404(t *testing.T) {
	r := newHoldRig(t, ScopeIncident, nil)
	id := r.open(t, crashKey, crashPayload(0))
	r.clock.advance(defaultIdleAfter)
	r.appendOK(t, id, crashPayload(1)) // held
	r.ax.mu.Lock()
	delete(r.ax.created, r.sink.taskFor(crashKey))
	r.ax.mu.Unlock()

	err := r.sink.Append(context.Background(), id, crashPayload(2))
	var se *inject.StatusError
	if !errors.As(err, &se) || se.Code != http.StatusNotFound {
		t.Fatalf("Append for a gone task = %v, want a 404 StatusError", err)
	}
	if len(r.sink.heldFor(r.sink.taskFor(crashKey))) != 0 {
		t.Error("a reminder for a gone task is still held")
	}
	if r.counts.get(ReminderDropped) != 1 {
		t.Errorf("dropped count = %d, want 1", r.counts.get(ReminderDropped))
	}
}

// A task whose actor was removed outside AX (phase Failed, Ready reason
// ActorNotFound) is gone as far as its sessions go: ResumeTask would
// recreate the actor, but as a fresh agent. A reminder drops what was held
// and answers 404, as for a task AX no longer has.
func TestHold_ActorNotFoundIsGone(t *testing.T) {
	r := newHoldRig(t, ScopeIncident, nil)
	id := r.open(t, crashKey, crashPayload(0))
	r.clock.advance(defaultIdleAfter)
	r.appendOK(t, id, crashPayload(1)) // held
	r.ax.mu.Lock()
	r.ax.phase, r.ax.reason = phaseFailed, reasonActorNotFound
	r.ax.mu.Unlock()

	err := r.sink.Append(context.Background(), id, crashPayload(2))
	var se *inject.StatusError
	if !errors.As(err, &se) || se.Code != http.StatusNotFound {
		t.Fatalf("reminder for a task whose actor is gone = %v, want a 404 StatusError", err)
	}
	if len(r.sink.heldFor(r.sink.taskFor(crashKey))) != 0 {
		t.Error("a reminder for a task whose actor is gone is still held")
	}
}

// Failed for any other reason is not proof the sessions are gone: the
// record decides, as for Unknown.
func TestHold_FailedForAnotherReasonIsNotGone(t *testing.T) {
	r := newHoldRig(t, ScopeIncident, nil)
	id := r.open(t, crashKey, crashPayload(0))
	r.clock.advance(defaultIdleAfter)
	r.ax.mu.Lock()
	r.ax.phase, r.ax.reason = phaseFailed, "ActorResumeFailed"
	r.ax.mu.Unlock()
	r.appendOK(t, id, crashPayload(1))
	if len(r.sink.heldFor(r.sink.taskFor(crashKey))) != 1 {
		t.Error("a reminder for a Failed (not ActorNotFound) idle task was not held")
	}
}

// A real inject that fails with something other than 404 or 410 asks AX
// whether the actor is gone. If it is, the failure becomes the gone 404 so
// the dispatcher re-opens the incident instead of retrying a dead session;
// if not, the original error stands.
func TestAppend_FailureAgainstAGoneActorBecomes404(t *testing.T) {
	for _, c := range []struct {
		name     string
		reason   string
		wantCode int
	}{
		{"actor removed", reasonActorNotFound, http.StatusNotFound},
		{"actor fine", "", http.StatusServiceUnavailable},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := newHoldRig(t, ScopeIncident, nil)
			id := r.open(t, crashKey, crashPayload(0))
			r.router.mu.Lock()
			r.router.failWith = http.StatusServiceUnavailable
			r.router.mu.Unlock()
			if c.reason != "" {
				r.ax.mu.Lock()
				r.ax.phase, r.ax.reason = phaseFailed, c.reason
				r.ax.mu.Unlock()
			}
			err := r.sink.Append(context.Background(), id, inject.Payload{Kind: "k8s-event-followup", Cluster: "prod", UID: "u1", Message: "changed"})
			var se *inject.StatusError
			if !errors.As(err, &se) || se.Code != c.wantCode {
				t.Fatalf("Append = %v, want a %d StatusError", err, c.wantCode)
			}
		})
	}
}

// On the fast path (task known awake), a recalled session whose actor turns
// out to be gone starts over: ResumeTask (which recreates the actor) and a
// new session, not an error bound to the dead one.
func TestOpen_FastPathRecoversFromAGoneActor(t *testing.T) {
	r := newHoldRig(t, ScopeIncident, nil)
	first := r.open(t, crashKey, crashPayload(0))
	r.router.mu.Lock()
	r.router.failWith = http.StatusServiceUnavailable
	r.router.mu.Unlock()
	r.ax.mu.Lock()
	r.ax.phase, r.ax.reason = phaseFailed, reasonActorNotFound
	r.ax.mu.Unlock()

	// ResumeTask recreates the actor, and AX reads it as running again.
	r.ax.mu.Lock()
	r.ax.onResume = func(f *fakeAX) {
		f.phase, f.reason = "", ""
		r.router.mu.Lock()
		r.router.failWith = 0
		r.router.mu.Unlock()
	}
	r.ax.mu.Unlock()
	id, err := r.sink.OpenIncidentKeyed(context.Background(), crashKey, crashPayload(0))
	if err != nil {
		t.Fatalf("reopen after the actor was removed: %v", err)
	}
	if id == first {
		t.Errorf("reopen stayed bound to the dead session %s", first)
	}
	r.ax.mu.Lock()
	defer r.ax.mu.Unlock()
	if len(r.ax.resumed) != 2 {
		t.Errorf("resumes = %d, want 2 (the open, then the recovery)", len(r.ax.resumed))
	}
}
