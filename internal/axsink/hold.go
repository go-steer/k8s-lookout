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
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/go-steer/k8s-lookout/internal/axapi"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// Holding reminders (issue #599, docs/ax-sink-design.md "Holding
// reminders"). A pure reminder (repeat_count > 0) says "still failing,
// nothing changed"; waking a suspended task, or keeping an idle one awake,
// to say that is the expensive part. So a reminder for an idle task is
// held, latest only per session, and delivered ahead of the next real
// inject that reaches the task anyway.

// defaultIdleAfter is how long after its last real inject a task counts as
// idle when its template sets no spec.idle.suspendAfter: AX's suggested
// idle-suspend setting, ten minutes.
const defaultIdleAfter = 10 * time.Minute

// maxHeld bounds the held reminders across every task. Past it, the oldest
// is dropped. Each session holds at most one (the latest).
const maxHeld = 4096

// heldTTL drops a held reminder no real inject has come for in this long.
// A still-failing incident refreshes its held reminder at least hourly (the
// reminder schedule's cap), so one this old means lookout stopped
// reminding: the incident went quiet or lost its binding.
const heldTTL = 3 * time.Hour

// Reminder outcomes, the label values of lookout_ax_reminders_total.
const (
	ReminderHeld       = "held"       // held: the task was idle
	ReminderSuperseded = "superseded" // a held reminder replaced by a newer one for the same session
	ReminderDelivered  = "delivered"  // a held reminder delivered ahead of a real inject
	ReminderDirect     = "direct"     // sent at once: the task was busy
	ReminderDropped    = "dropped"    // a held reminder expired, evicted, or failed delivery
)

// heldReminder is the latest reminder held for one incident id.
type heldReminder struct {
	payload any
	cluster string
	at      time.Time
}

// holder is the sink's reminder state: when each task last got a real
// inject, and the reminders held for idle tasks. Lost on restart, by
// design: a held reminder only repeats what the agent already knows.
type holder struct {
	mu       sync.Mutex
	lastReal map[string]time.Time     // task -> last real inject delivered
	held     map[string]*heldReminder // "<task>/<session>" -> latest reminder
	owed     map[string]bool          // "<task>/<session>" whose last real inject failed
	counters sync.Map                 // cluster -> func(outcome string)
}

func newHolder() *holder {
	return &holder{lastReal: map[string]time.Time{}, held: map[string]*heldReminder{}, owed: map[string]bool{}}
}

// UseReminderCounter has the sink count cluster's reminders with count,
// called with one of the Reminder* outcomes. Like UseSessionStore, each
// cluster runner hands the sink its own; nil removes it.
func (s *Sink) UseReminderCounter(cluster string, count func(outcome string)) {
	if count == nil {
		s.hold.counters.Delete(cluster)
		return
	}
	s.hold.counters.Store(cluster, count)
}

func (s *Sink) countReminder(cluster, outcome string) {
	if c, ok := s.hold.counters.Load(cluster); ok {
		c.(func(string))(outcome)
	}
}

// reminder reports whether payload is a pure reminder, and its cluster.
func reminder(payload any) (cluster string, ok bool) {
	switch p := payload.(type) {
	case inject.Payload:
		return p.Cluster, p.RepeatCount > 0
	case *inject.Payload:
		if p != nil {
			return p.Cluster, p.RepeatCount > 0
		}
	}
	return "", false
}

// idleAfterFor is the task template's spec.idle.suspendAfter, else
// defaultIdleAfter.
func idleAfterFor(t *axapi.Task) time.Duration {
	if d, err := time.ParseDuration(t.GetSpec().GetIdle().GetSuspendAfter()); err == nil && d > 0 {
		return d
	}
	return defaultIdleAfter
}

// taskState is what a reminder finds its task to be.
type taskState int

const (
	taskAwake taskState = iota // send the reminder
	taskIdle                   // hold it
	taskGone                   // AX has no such task: the session can't come back
)

// stateOf decides whether a reminder may go to task now without waking it
// or holding it awake past its idle window. Awake means a real inject was
// delivered within the idle window (so AX cannot have suspended it for
// idleness, and the agent is plausibly still on the incident) and AX does
// not record it as Suspended (on completion, or by hand). Both come from a
// GetTask, a read of AX's own record that never wakes the task, which also
// says whether the task still exists. If GetTask fails, the record alone
// decides: a busy task gets the reminder as before, an idle one holds it.
func (s *Sink) stateOf(ctx context.Context, task string) taskState {
	s.hold.mu.Lock()
	last, ok := s.hold.lastReal[task]
	s.hold.mu.Unlock()
	recent := ok && s.now().Sub(last) < s.idleAfter
	t, err := s.cfg.Client.GetTask(ctx, &axapi.GetTaskRequest{Atespace: s.atespace, Name: task})
	switch {
	case status.Code(err) == codes.NotFound:
		return taskGone
	case err != nil && recent:
		return taskAwake
	case err != nil, !recent, t.GetStatus().GetPhase() == "Suspended":
		return taskIdle
	}
	return taskAwake
}

// goneError is what Append returns for a reminder whose task AX no longer
// has: the same 404 the agent gives for a session it no longer has, so the
// dispatcher un-binds the incident and reopens it on the next occurrence
// instead of holding a reminder for a wake that will never come.
func (s *Sink) goneError(task string) error {
	return &inject.StatusError{
		Op:   "ax sink: Append",
		Code: http.StatusNotFound,
		Body: fmt.Sprintf("task %s/%s not found", s.atespace, task),
	}
}

// owe and paid track incident ids whose last real inject failed: an open
// whose payload never arrived, or a change the agent never got. A reminder
// for such an id may be the agent's first word on it, so it is never held.
func (s *Sink) owe(id string) {
	s.hold.mu.Lock()
	defer s.hold.mu.Unlock()
	if !s.hold.owed[id] && len(s.hold.owed) >= maxRemembered {
		for old := range s.hold.owed {
			delete(s.hold.owed, old)
			break
		}
	}
	s.hold.owed[id] = true
}

func (s *Sink) paid(id string) {
	s.hold.mu.Lock()
	defer s.hold.mu.Unlock()
	delete(s.hold.owed, id)
}

func (s *Sink) owes(id string) bool {
	s.hold.mu.Lock()
	defer s.hold.mu.Unlock()
	return s.hold.owed[id]
}

// dropHeldIn drops every reminder held for task: AX no longer has it.
func (s *Sink) dropHeldIn(task string) {
	var dropped []string
	s.hold.mu.Lock()
	for id, h := range s.hold.held {
		if t, _, _ := splitID(id); t == task {
			delete(s.hold.held, id)
			dropped = append(dropped, h.cluster)
		}
	}
	s.hold.mu.Unlock()
	for _, c := range dropped {
		s.countReminder(c, ReminderDropped)
	}
}

// noteReal records a real inject delivered to task: it is awake, and its
// idle clock restarted.
func (s *Sink) noteReal(task string) {
	s.hold.mu.Lock()
	defer s.hold.mu.Unlock()
	if _, ok := s.hold.lastReal[task]; !ok && len(s.hold.lastReal) >= maxRemembered {
		for old := range s.hold.lastReal {
			delete(s.hold.lastReal, old) // forgotten = idle: reminders are held, never lost
			break
		}
	}
	s.hold.lastReal[task] = s.now()
}

// holdReminder keeps payload as id's latest reminder, replacing an older
// one, and drops expired reminders and, past maxHeld, the oldest.
func (s *Sink) holdReminder(id, cluster string, payload any) {
	now := s.now()
	var dropped, superseded []string
	s.hold.mu.Lock()
	if old, ok := s.hold.held[id]; ok {
		superseded = append(superseded, old.cluster)
	}
	s.hold.held[id] = &heldReminder{payload: payload, cluster: cluster, at: now}
	for k, h := range s.hold.held {
		if now.Sub(h.at) >= heldTTL {
			delete(s.hold.held, k)
			dropped = append(dropped, h.cluster)
		}
	}
	for len(s.hold.held) > maxHeld {
		oldest := ""
		for k, h := range s.hold.held {
			if oldest == "" || h.at.Before(s.hold.held[oldest].at) {
				oldest = k
			}
		}
		dropped = append(dropped, s.hold.held[oldest].cluster)
		delete(s.hold.held, oldest)
	}
	s.hold.mu.Unlock()
	s.countReminder(cluster, ReminderHeld)
	for _, c := range superseded {
		s.countReminder(c, ReminderSuperseded)
	}
	for _, c := range dropped {
		s.countReminder(c, ReminderDropped)
	}
}

// forgetHeld drops id's held reminder because a newer one is going straight
// through.
func (s *Sink) forgetHeld(id string) {
	s.hold.mu.Lock()
	old, ok := s.hold.held[id]
	delete(s.hold.held, id)
	s.hold.mu.Unlock()
	if ok {
		s.countReminder(old.cluster, ReminderSuperseded)
	}
}

// flushHeld delivers every reminder held for task, oldest first, ahead of
// a real inject that is waking the task anyway. In cluster scope that is
// every incident in the cluster with a held reminder. A failed delivery
// is logged and counted as dropped; it never fails the real inject.
func (s *Sink) flushHeld(ctx context.Context, task string) {
	type due struct {
		id string
		h  *heldReminder
	}
	var batch []due
	now := s.now()
	s.hold.mu.Lock()
	for id, h := range s.hold.held {
		if t, _, _ := splitID(id); t == task {
			batch = append(batch, due{id, h})
			delete(s.hold.held, id)
		}
	}
	s.hold.mu.Unlock()
	sort.Slice(batch, func(i, j int) bool { return batch[i].h.at.Before(batch[j].h.at) })
	inj := s.injectorFor(task)
	for _, d := range batch {
		if now.Sub(d.h.at) >= heldTTL {
			s.countReminder(d.h.cluster, ReminderDropped)
			continue
		}
		_, sid, _ := splitID(d.id)
		if err := inj.Append(ctx, sid, d.h.payload); err != nil {
			log.Printf("ax sink: delivering the held reminder for %s: %v (dropped)", d.id, err)
			s.countReminder(d.h.cluster, ReminderDropped)
			continue
		}
		s.countReminder(d.h.cluster, ReminderDelivered)
	}
}

// heldFor returns the ids with a held reminder in task, sorted (tests).
func (s *Sink) heldFor(task string) []string {
	s.hold.mu.Lock()
	defer s.hold.mu.Unlock()
	var ids []string
	for id := range s.hold.held {
		if strings.HasPrefix(id, task+"/") {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
