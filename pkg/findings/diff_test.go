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
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

var (
	t0 = time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	t1 = t0.Add(15 * time.Minute)
)

// obs builds an Observation for a Pod subject in the default cluster.
func obs(name, reason, severity string) Observation {
	return Observation{
		SubjectKey:   SubjectKey("", "prod", "Pod", name, reason),
		Fingerprint:  "sha256:" + reason,
		Namespace:    "prod",
		KindOfObject: "Pod",
		Name:         name,
		Reason:       reason,
		Severity:     severity,
	}
}

// stateOf turns an observation into the state row a previous run would
// have persisted for it.
func stateOf(o Observation, first, last time.Time) State {
	return State{
		SubjectKey:   o.SubjectKey,
		Fingerprint:  o.Fingerprint,
		Namespace:    o.Namespace,
		KindOfObject: o.KindOfObject,
		Name:         o.Name,
		Reason:       o.Reason,
		Severity:     o.Severity,
		FirstSeen:    first,
		LastSeen:     last,
	}
}

func transitionsBySubject(res Result) map[string]Transition {
	out := make(map[string]Transition, len(res.Changes))
	for _, c := range res.Changes {
		out[c.SubjectKey] = c.Transition
	}
	return out
}

func TestDiffClassifiesTransitions(t *testing.T) {
	crash := obs("payment-backend-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityWarning)
	pull := obs("checkout-2bv4d-m6n8p", "ImagePullBackOff", emit.SeverityWarning)
	gone := obs("legacy-worker-4fk2n-b7c3v", "OOMKilled", emit.SeverityCritical)

	prev := []State{
		stateOf(crash, t0, t0),
		stateOf(gone, t0, t0),
	}
	// This run: crash escalates, pull is brand new, gone is absent.
	cur := []Observation{
		obs("payment-backend-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityCritical),
		pull,
	}

	res := Diff(prev, cur, t1)
	got := transitionsBySubject(res)

	want := map[string]Transition{
		crash.SubjectKey: TransitionEscalated,
		pull.SubjectKey:  TransitionNew,
		gone.SubjectKey:  TransitionResolved,
	}
	for key, wantT := range want {
		if got[key] != wantT {
			t.Errorf("subject %q: transition = %q, want %q", key, got[key], wantT)
		}
	}
	if len(res.Changes) != 3 {
		t.Fatalf("got %d changes, want 3: %+v", len(res.Changes), res.Changes)
	}

	// Resolved subjects must NOT survive into the next state, or a
	// later recurrence would read as a permanently-ongoing zombie
	// instead of a new failure.
	if len(res.Next) != 2 {
		t.Fatalf("Next has %d rows, want 2 (resolved must be dropped): %+v", len(res.Next), res.Next)
	}
	for _, s := range res.Next {
		if s.SubjectKey == gone.SubjectKey {
			t.Errorf("resolved subject %q survived into Next", s.SubjectKey)
		}
	}
}

// TestDiffRescheduledPodIsOngoing is the case the whole package exists
// for: a crash-looping pod replaced by a differently-suffixed pod is
// ONE ongoing finding, not resolved + new.
func TestDiffRescheduledPodIsOngoing(t *testing.T) {
	before := obs("payment-backend-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityWarning)
	after := obs("payment-backend-7d9f8-q4m7p", "CrashLoopBackOff", emit.SeverityWarning)

	res := Diff([]State{stateOf(before, t0, t0)}, []Observation{after}, t1)

	if len(res.Changes) != 1 {
		t.Fatalf("got %d changes, want exactly 1 (a reschedule is not resolved+new): %+v",
			len(res.Changes), res.Changes)
	}
	if got := res.Changes[0].Transition; got != TransitionOngoing {
		t.Errorf("transition = %q, want %q", got, TransitionOngoing)
	}
	// FirstSeen is the "broken for how long" number and must survive
	// the reschedule; recomputing it every run would reset it forever.
	if got := res.Changes[0].FirstSeen; !got.Equal(t0) {
		t.Errorf("FirstSeen = %v, want the original %v — a reschedule must not reset the clock", got, t0)
	}
	// The reported name is the CURRENT pod, even though the key is the
	// normalized one: an operator needs the pod that exists now.
	if got := res.Changes[0].Name; got != after.Name {
		t.Errorf("Name = %q, want the current pod %q", got, after.Name)
	}
}

// TestDiffDistinctReasonsAreDistinctSubjects: a pod that stops
// ImagePullBackOff-ing and starts CrashLoopBackOff-ing has not
// "continued" — one failure ended and another began.
func TestDiffDistinctReasonsAreDistinctSubjects(t *testing.T) {
	was := obs("api-7d9f8-x9k2l", "ImagePullBackOff", emit.SeverityWarning)
	now := obs("api-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityWarning)

	res := Diff([]State{stateOf(was, t0, t0)}, []Observation{now}, t1)
	got := transitionsBySubject(res)

	if got[was.SubjectKey] != TransitionResolved {
		t.Errorf("old reason: transition = %q, want %q", got[was.SubjectKey], TransitionResolved)
	}
	if got[now.SubjectKey] != TransitionNew {
		t.Errorf("new reason: transition = %q, want %q", got[now.SubjectKey], TransitionNew)
	}
}

func TestDiffAckWindow(t *testing.T) {
	o := obs("payment-backend-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityWarning)

	acked := stateOf(o, t0, t0)
	acked.AckUntil = t0.Add(4 * time.Hour)
	acked.AckBy = "ops@example.com"

	t.Run("inside the window suppresses", func(t *testing.T) {
		res := Diff([]State{acked}, []Observation{o}, t1)
		if got := res.Changes[0].Transition; got != TransitionSuppressed {
			t.Errorf("transition = %q, want %q", got, TransitionSuppressed)
		}
		if got := res.Changes[0].AckBy; got != "ops@example.com" {
			t.Errorf("AckBy = %q, want it carried onto the change", got)
		}
		// The ack must survive into the next state or it would last
		// exactly one run.
		if len(res.Next) != 1 || !res.Next[0].AckUntil.Equal(acked.AckUntil) {
			t.Errorf("ack did not survive into Next: %+v", res.Next)
		}
	})

	t.Run("after expiry it resurfaces as ongoing", func(t *testing.T) {
		after := t0.Add(5 * time.Hour)
		res := Diff([]State{acked}, []Observation{o}, after)
		// Ongoing, not new: we knew about it the whole time, and
		// calling it new would be a lie an operator would act on.
		if got := res.Changes[0].Transition; got != TransitionOngoing {
			t.Errorf("transition = %q, want %q", got, TransitionOngoing)
		}
	})

	t.Run("ack outranks escalation", func(t *testing.T) {
		worse := obs("payment-backend-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityCritical)
		res := Diff([]State{acked}, []Observation{worse}, t1)
		if got := res.Changes[0].Transition; got != TransitionSuppressed {
			t.Errorf("transition = %q, want %q — an ack takes the whole subject", got, TransitionSuppressed)
		}
		// Severity must NOT advance inside the window, or the
		// escalation that should fire the moment the ack expires never
		// would: the next run would compare critical against critical.
		if got := res.Next[0].Severity; got != emit.SeverityWarning {
			t.Errorf("Next severity = %q, want the pre-ack %q", got, emit.SeverityWarning)
		}
	})

	t.Run("escalation fires once the ack expires", func(t *testing.T) {
		worse := obs("payment-backend-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityCritical)
		// Run 1, inside the window: suppressed, severity pinned.
		first := Diff([]State{acked}, []Observation{worse}, t1)
		// Run 2, after expiry, carrying run 1's state forward.
		second := Diff(first.Next, []Observation{worse}, t0.Add(5*time.Hour))
		if got := second.Changes[0].Transition; got != TransitionEscalated {
			t.Errorf("transition = %q, want %q — the escalation must survive the ack window", got, TransitionEscalated)
		}
	})

	t.Run("resolving inside the window still reports resolved", func(t *testing.T) {
		res := Diff([]State{acked}, nil, t1)
		if got := res.Changes[0].Transition; got != TransitionResolved {
			t.Errorf("transition = %q, want %q", got, TransitionResolved)
		}
		if len(res.Next) != 0 {
			t.Errorf("Next = %+v, want empty — the ack dies with the occurrence it was taken against", res.Next)
		}
	})
}

// TestDiffDeescalationStaysOngoing pins the decision not to add a
// sixth wire value: the severity pair carries the story.
func TestDiffDeescalationStaysOngoing(t *testing.T) {
	was := obs("api-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityCritical)
	now := obs("api-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityWarning)

	res := Diff([]State{stateOf(was, t0, t0)}, []Observation{now}, t1)
	c := res.Changes[0]
	if c.Transition != TransitionOngoing {
		t.Errorf("transition = %q, want %q", c.Transition, TransitionOngoing)
	}
	if c.PrevSeverity != emit.SeverityCritical || c.Severity != emit.SeverityWarning {
		t.Errorf("severity pair = (%q → %q), want (critical → warning)", c.PrevSeverity, c.Severity)
	}
}

// TestDiffUnknownSeverityCannotEscalate: a malformed report must not
// manufacture an interrupt.
func TestDiffUnknownSeverityCannotEscalate(t *testing.T) {
	was := obs("api-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityWarning)
	now := obs("api-7d9f8-x9k2l", "CrashLoopBackOff", "catastrophic")

	res := Diff([]State{stateOf(was, t0, t0)}, []Observation{now}, t1)
	if got := res.Changes[0].Transition; got != TransitionOngoing {
		t.Errorf("transition = %q, want %q — an unrecognized severity ranks lowest", got, TransitionOngoing)
	}
}

func TestDiffEmptyReportResolvesEverything(t *testing.T) {
	prev := []State{
		stateOf(obs("a-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityWarning), t0, t0),
		stateOf(obs("b-7d9f8-x9k2l", "OOMKilled", emit.SeverityCritical), t0, t0),
	}
	res := Diff(prev, nil, t1)
	if len(res.Changes) != 2 {
		t.Fatalf("got %d changes, want 2", len(res.Changes))
	}
	for _, c := range res.Changes {
		if c.Transition != TransitionResolved {
			t.Errorf("subject %q: transition = %q, want %q", c.SubjectKey, c.Transition, TransitionResolved)
		}
	}
	if len(res.Next) != 0 {
		t.Errorf("Next = %+v, want empty", res.Next)
	}
}

func TestDiffFirstRunIsAllNew(t *testing.T) {
	cur := []Observation{
		obs("a-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityWarning),
		obs("b-7d9f8-x9k2l", "OOMKilled", emit.SeverityCritical),
	}
	res := Diff(nil, cur, t0)
	for _, c := range res.Changes {
		if c.Transition != TransitionNew {
			t.Errorf("subject %q: transition = %q, want %q", c.SubjectKey, c.Transition, TransitionNew)
		}
		if c.PrevSeverity != "" {
			t.Errorf("subject %q: PrevSeverity = %q, want empty on a new finding", c.SubjectKey, c.PrevSeverity)
		}
		if !c.FirstSeen.Equal(t0) {
			t.Errorf("subject %q: FirstSeen = %v, want %v", c.SubjectKey, c.FirstSeen, t0)
		}
	}
}

// TestDiffCollapsesDuplicateSubjects: two pods of the same Deployment
// failing the same way normalize onto one subject, and the state table
// is one row per subject by construction.
func TestDiffCollapsesDuplicateSubjects(t *testing.T) {
	cur := []Observation{
		obs("payment-backend-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityWarning),
		obs("payment-backend-7d9f8-q4m7p", "CrashLoopBackOff", emit.SeverityWarning),
	}
	res := Diff(nil, cur, t0)
	if len(res.Changes) != 1 {
		t.Fatalf("got %d changes, want 1 — same subject twice: %+v", len(res.Changes), res.Changes)
	}
	if len(res.Next) != 1 {
		t.Fatalf("Next has %d rows, want 1", len(res.Next))
	}
}

// TestDiffOutputIsDeterministic pins the ordering contract: changed
// first (escalated, new, resolved), then unchanged, each group by
// subject key. A digest that truncates keeps the useful half.
func TestDiffOutputIsDeterministic(t *testing.T) {
	prev := []State{
		stateOf(obs("zeta-7d9f8-x9k2l", "OOMKilled", emit.SeverityWarning), t0, t0),
		stateOf(obs("alpha-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityWarning), t0, t0),
		stateOf(obs("gamma-7d9f8-x9k2l", "FailedMount", emit.SeverityWarning), t0, t0),
	}
	cur := []Observation{
		obs("zeta-7d9f8-x9k2l", "OOMKilled", emit.SeverityCritical),        // escalated
		obs("alpha-7d9f8-x9k2l", "CrashLoopBackOff", emit.SeverityWarning), // ongoing
		obs("beta-7d9f8-x9k2l", "FailedScheduling", emit.SeverityWarning),  // new
		// gamma absent → resolved
	}

	want := []Transition{
		TransitionEscalated, // zeta
		TransitionNew,       // beta
		TransitionResolved,  // gamma
		TransitionOngoing,   // alpha
	}
	// Run twice with the inputs in a different order; the output must
	// be identical both times.
	for i, in := range [][]Observation{cur, {cur[2], cur[0], cur[1]}} {
		res := Diff(prev, in, t1)
		if len(res.Changes) != len(want) {
			t.Fatalf("run %d: got %d changes, want %d", i, len(res.Changes), len(want))
		}
		for j, c := range res.Changes {
			if c.Transition != want[j] {
				t.Errorf("run %d, change %d: transition = %q (%s), want %q",
					i, j, c.Transition, c.Name, want[j])
			}
		}
	}
}
