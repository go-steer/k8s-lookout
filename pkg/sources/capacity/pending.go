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
	"k8s.io/apimachinery/pkg/types"

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
	// insufficient records that the refusal cites insufficient
	// resources rather than some other reason the pod will not fit.
	// This is the judgement other subsystems consume through
	// Unschedulables — see insufficientResource.
	insufficient bool
	fired        level
}

// Unschedulable is one pod the scheduler has refused, as this source
// judges it. It is the shape Unschedulables hands to consumers.
type Unschedulable struct {
	Namespace string
	Name      string
	// Since is when the pod became unschedulable, not when it was
	// created: a pod that scheduled, was evicted and could not get back
	// is a different episode from one that never fit.
	Since time.Time
	// Message is the scheduler's own words, verbatim and unparsed. A
	// consumer that re-derives numbers from it invents a second answer
	// to a question the scheduler already answered.
	Message string
	// InsufficientResource is whether the refusal is about capacity. A
	// pod refused for an untolerated taint or an unsatisfiable affinity
	// is unschedulable too, and calling that a capacity shortfall is how
	// an attribution ladder blames the wrong thing.
	InsufficientResource bool
}

// Unschedulables snapshots the pending table, keyed by pod UID.
//
// This exists so that sibling subsystems can consume this source's
// judgement rather than re-reading pod conditions themselves — the
// `topology-drift` attribution ladder (leeway-design §8.5) is the first
// consumer. The whole table is returned rather than a per-subject query
// because it is small by construction (only refused pods are in it) and
// because the caller's index, not this one's, knows what a subject is.
//
// Safe to call from any goroutine; the returned map is a copy.
func (s *Source) Unschedulables() map[types.UID]Unschedulable {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[types.UID]Unschedulable, len(s.pending))
	for uid, e := range s.pending {
		out[types.UID(uid)] = Unschedulable{
			Namespace:            e.namespace,
			Name:                 e.name,
			Since:                e.since,
			Message:              e.scheduleMsg,
			InsufficientResource: e.insufficient,
		}
	}
	return out
}

// insufficientResource reports whether a FailedScheduling message is
// about capacity.
//
// The scheduler composes these by concatenating per-predicate counts:
//
//	0/12 nodes are available: 3 Insufficient cpu, 2 Insufficient memory,
//	7 node(s) had untolerated taint {dedicated: gpu}.
//
// so a message can cite capacity alongside other reasons, and any
// mention of it is enough to say capacity is part of why the pod did not
// land. Matching the "Insufficient <resource>" phrasing covers extended
// resources and device plugins as well as cpu and memory, which an
// enumeration of resource names would not.
//
// The negative cases matter as much: a pod refused only for a taint, an
// affinity or a volume-node conflict is unschedulable without the
// cluster being short of anything, and reporting that as a capacity
// shortfall points the reader at the wrong remedy.
func insufficientResource(msg string) bool {
	return strings.Contains(msg, "Insufficient ")
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
	e.insufficient = insufficientResource(cond.Message)
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
