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

package findings

import (
	"sort"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// Transition classifies one subject's change between two runs. The
// value set is a WIRE CONTRACT with the consumer (go-steer/mast reads
// these strings to build its digest) — append-only, never rename.
type Transition string

const (
	// TransitionNew: the subject was not in the previous state. The
	// only class that unambiguously warrants waking someone.
	TransitionNew Transition = "new"
	// TransitionOngoing: present in both runs at the same or lower
	// severity. The class that makes unattended operation tolerable —
	// a digest can collapse these to a count.
	TransitionOngoing Transition = "ongoing"
	// TransitionEscalated: present in both runs, but the severity
	// increased (warning → critical). Distinct from ongoing because
	// "it got worse" is the one repeat worth interrupting for.
	TransitionEscalated Transition = "escalated"
	// TransitionResolved: in the previous state, absent from this run.
	TransitionResolved Transition = "resolved"
	// TransitionSuppressed: present, but inside an operator ack
	// window. Distinct from resolved on purpose — the finding is still
	// there, someone has just taken it.
	TransitionSuppressed Transition = "suppressed"
)

// transitionOrder ranks transitions for output: what CHANGED comes
// before what didn't, so a truncated digest keeps the useful half.
var transitionOrder = map[Transition]int{
	TransitionEscalated:  0,
	TransitionNew:        1,
	TransitionResolved:   2,
	TransitionOngoing:    3,
	TransitionSuppressed: 4,
}

// severityRank orders the §7.7 severity classes. An unknown or empty
// severity ranks lowest, so a malformed report can never manufacture
// an escalation.
func severityRank(s string) int {
	switch s {
	case emit.SeverityCritical:
		return 3
	case emit.SeverityWarning:
		return 2
	case emit.SeverityInfo:
		return 1
	default:
		return 0
	}
}

// Observation is one finding from the CURRENT run, reduced to the
// fields the diff keys on and reports back. Built from an emit.Finding
// by ObservationOf.
type Observation struct {
	SubjectKey   string
	Fingerprint  string
	Cluster      string
	Namespace    string
	KindOfObject string
	Name         string
	// Reason is the CANONICAL reason (see SubjectKey) — the caller
	// canonicalizes before constructing, so the key and the reported
	// reason can never disagree.
	Reason   string
	Severity string
	Message  string
}

// State is the durable per-subject row carried between runs — one row
// per open subject, in the §9.1 store.
type State struct {
	SubjectKey   string
	Fingerprint  string
	Cluster      string
	Namespace    string
	KindOfObject string
	Name         string
	Reason       string
	// Severity is the severity at the LAST observation; the next run
	// compares against it to detect escalation.
	Severity string
	// FirstSeen is when this subject first appeared and is preserved
	// across runs — it is the "broken for 3 days" number, and
	// recomputing it from the current run would reset it every time.
	FirstSeen time.Time
	LastSeen  time.Time
	// AckUntil is the operator ack expiry; zero means not acked. An
	// ack is TIME-BOXED and asserts no diagnosis — that is what makes
	// it different from §9.4's severity_override, which is a standing
	// routing judgment.
	AckUntil time.Time
	AckBy    string
}

// Acked reports whether the ack window is still open at now.
func (s State) Acked(now time.Time) bool {
	return !s.AckUntil.IsZero() && now.Before(s.AckUntil)
}

// Change is one classified transition, carrying enough context that a
// consumer never has to join back against the report.
type Change struct {
	Transition Transition
	Observation
	// PrevSeverity is the severity recorded at the previous run, empty
	// for `new`. Set on every other class — including plain `ongoing`,
	// which is where a DE-escalation (critical → warning) shows up:
	// the transition stays `ongoing` because the finding did not
	// change state, and the severity pair tells the fuller story
	// without adding a sixth wire value.
	PrevSeverity string
	FirstSeen    time.Time
	LastSeen     time.Time
	// AckUntil is the open ack window on a `suppressed` change; zero
	// on every other class.
	AckUntil time.Time
	AckBy    string
}

// Result is a diff run: the transitions to report, and the state to
// persist for the next run.
type Result struct {
	Changes []Change
	// Next is the complete replacement state set. Resolved subjects
	// are ABSENT — dropping the row is what makes a later recurrence
	// read as genuinely `new` rather than as a permanently-ongoing
	// zombie. It also drops any ack that rode on the row, which is
	// correct: the ack was taken against an occurrence that is now
	// over.
	Next []State
}

// Diff classifies the current run against the previous state.
//
// Rules, in the order they are applied per subject:
//
//  1. Present + ack window open at now → suppressed. The ack outranks
//     escalation deliberately: an operator who says "I have this for
//     four hours" has taken the whole subject, and a severity bump
//     inside their own window is usually their own remediation
//     churning (a rollback restarting pods) rather than news. It
//     expires on its own; nothing is lost permanently.
//  2. Present + not in previous state → new.
//  3. Present + severity rank increased → escalated.
//  4. Present otherwise → ongoing.
//  5. In previous state + absent from this run → resolved, and the
//     row is dropped from Next.
//
// Output is deterministic: transitions ranked by transitionOrder
// (changed first), then by subject key. Callers can therefore golden-
// test the surface, and a digest that truncates keeps the useful half.
//
// now is passed in rather than read from the clock so the ack boundary
// is testable and a diff is a pure function of its inputs.
func Diff(prev []State, cur []Observation, now time.Time) Result {
	prevByKey := make(map[string]State, len(prev))
	for _, s := range prev {
		prevByKey[s.SubjectKey] = s
	}

	var res Result
	seen := make(map[string]struct{}, len(cur))
	for _, obs := range cur {
		if _, dup := seen[obs.SubjectKey]; dup {
			// Two findings collapsed onto one subject (the normalizer
			// doing its job across a rescheduled pair, or two checks
			// reporting the same symptom). First wins; the state row
			// is per subject by construction.
			continue
		}
		seen[obs.SubjectKey] = struct{}{}

		old, known := prevByKey[obs.SubjectKey]
		next := State{
			SubjectKey:   obs.SubjectKey,
			Fingerprint:  obs.Fingerprint,
			Cluster:      obs.Cluster,
			Namespace:    obs.Namespace,
			KindOfObject: obs.KindOfObject,
			Name:         obs.Name,
			Reason:       obs.Reason,
			Severity:     obs.Severity,
			FirstSeen:    now,
			LastSeen:     now,
		}
		if known {
			next.FirstSeen = old.FirstSeen
			next.AckUntil = old.AckUntil
			next.AckBy = old.AckBy
		}

		ch := Change{
			Observation: obs,
			FirstSeen:   next.FirstSeen,
			LastSeen:    now,
		}
		switch {
		case known && old.Acked(now):
			ch.Transition = TransitionSuppressed
			ch.PrevSeverity = old.Severity
			ch.AckUntil = old.AckUntil
			ch.AckBy = old.AckBy
			// A suppressed subject's recorded severity is NOT advanced:
			// the ack window is a pause, and letting severity creep up
			// silently inside it would mean the escalation that should
			// fire the moment the ack expires never does.
			next.Severity = old.Severity
		case !known:
			ch.Transition = TransitionNew
		case severityRank(obs.Severity) > severityRank(old.Severity):
			ch.Transition = TransitionEscalated
			ch.PrevSeverity = old.Severity
		default:
			ch.Transition = TransitionOngoing
			ch.PrevSeverity = old.Severity
		}
		res.Changes = append(res.Changes, ch)
		res.Next = append(res.Next, next)
	}

	for _, old := range prev {
		if _, still := seen[old.SubjectKey]; still {
			continue
		}
		res.Changes = append(res.Changes, Change{
			Transition: TransitionResolved,
			Observation: Observation{
				SubjectKey:   old.SubjectKey,
				Fingerprint:  old.Fingerprint,
				Cluster:      old.Cluster,
				Namespace:    old.Namespace,
				KindOfObject: old.KindOfObject,
				Name:         old.Name,
				Reason:       old.Reason,
				Severity:     old.Severity,
			},
			PrevSeverity: old.Severity,
			FirstSeen:    old.FirstSeen,
			LastSeen:     old.LastSeen,
		})
	}

	sort.SliceStable(res.Changes, func(i, j int) bool {
		a, b := res.Changes[i], res.Changes[j]
		if oa, ob := transitionOrder[a.Transition], transitionOrder[b.Transition]; oa != ob {
			return oa < ob
		}
		return a.SubjectKey < b.SubjectKey
	})
	sort.Slice(res.Next, func(i, j int) bool {
		return res.Next[i].SubjectKey < res.Next[j].SubjectKey
	})
	return res
}
