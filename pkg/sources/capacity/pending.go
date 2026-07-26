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

package capacity

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// Sub-source 4: pending-pod aging — the resident TRENDING version of
// `triage delta`'s point-in-time pending-pod scan. The pod informer
// tracks Pending pods whose PodScheduled condition is False with
// reason Unschedulable; the poll-tick sweep fires
// capacity.pending-aged at warning once a pod has been stuck past
// Config.PendingAge and escalates to critical past
// Config.criticalPendingAge(). Countdown semantics (package comment):
// pods already stuck at startup fire after arming; the engine's
// persisted dedup absorbs restart repeats.

// pendingEntry is the per-pod aging memory.
type pendingEntry struct {
	namespace string
	name      string
	// since is when the pod became unschedulable: the PodScheduled
	// condition's LastTransitionTime, falling back to pod creation.
	since time.Time
	// scheduleMsg is the scheduler's own explanation (condition
	// message), carried as evidence.
	scheduleMsg string
	fired       level
}

// trackPod records or retires a pod in the aging table. Runs from
// informer handlers including the initial LIST (recording is not
// emission — the sweep emits, post-arm by construction).
func (s *Source) trackPod(p *corev1.Pod) {
	cond := unschedulableCond(p)
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.Status.Phase != corev1.PodPending || cond == nil {
		// Scheduled, running, or gone-from-Pending: the aging
		// episode (if any) is over.
		delete(s.pending, string(p.UID))
		return
	}
	e, ok := s.pending[string(p.UID)]
	if !ok {
		since := cond.LastTransitionTime.Time
		if since.IsZero() {
			since = p.CreationTimestamp.Time
		}
		e = &pendingEntry{since: since}
		s.pending[string(p.UID)] = e
	}
	e.namespace = p.Namespace
	e.name = p.Name
	e.scheduleMsg = cond.Message
}

// forgetPod drops a deleted pod from the aging table.
func (s *Source) forgetPod(p *corev1.Pod) {
	s.mu.Lock()
	delete(s.pending, string(p.UID))
	s.mu.Unlock()
}

// unschedulableCond returns the pod's PodScheduled=False condition
// when its reason is Unschedulable, else nil. This is the scheduler's
// own verdict — a pod merely Pending (image pulling, volume binding)
// is not a capacity signal.
func unschedulableCond(p *corev1.Pod) *corev1.PodCondition {
	for i := range p.Status.Conditions {
		c := &p.Status.Conditions[i]
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse && c.Reason == corev1.PodReasonUnschedulable {
			return c
		}
	}
	return nil
}

// sweepPending fires the aged crossings. Level latch per pod: one
// warning per episode, one critical escalation per episode.
func (s *Source) sweepPending(now time.Time) {
	critical := s.cfg.criticalPendingAge()
	var out []engine.Signal
	s.mu.Lock()
	for uid, e := range s.pending {
		age := now.Sub(e.since)
		lvl := levelNone
		switch {
		case age >= critical:
			lvl = levelCritical
		case age >= s.cfg.PendingAge:
			lvl = levelWarn
		}
		if lvl == levelNone || lvl <= e.fired {
			continue
		}
		e.fired = lvl
		out = append(out, pendingAgedSignal(uid, e, age, now, lvl))
	}
	s.mu.Unlock()
	for _, sig := range out {
		s.send(sig)
	}
}

func pendingAgedSignal(uid string, e *pendingEntry, age time.Duration, now time.Time, lvl level) engine.Signal {
	msg := fmt.Sprintf("pod Pending and Unschedulable for %s", age.Truncate(time.Second))
	if e.scheduleMsg != "" {
		msg += "; scheduler: " + e.scheduleMsg
	}
	return engine.Signal{
		Kind:     KindPendingAged,
		Source:   engine.SourceSentinel,
		Severity: lvl.severity(),
		TriageEvent: engine.TriageEvent{
			Key: engine.EventKey{
				UID:    uid,
				Reason: strings.TrimPrefix(KindPendingAged, kindPrefix),
			},
			Namespace:    e.namespace,
			KindOfObject: "Pod",
			Name:         e.name,
			Message:      truncate(msg),
			FirstSeen:    e.since,
			LastSeen:     now,
			Count:        1,
		},
	}
}
