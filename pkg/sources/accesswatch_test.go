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

package sources

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// steppedReviewer answers from a per-call script so a test can walk a
// grant through allowed → denied → allowed one sweep at a time.
type steppedReviewer struct {
	calls int
	step  func(n int, req Requirement) (Decision, error)
}

func (s *steppedReviewer) Allowed(_ context.Context, req Requirement) (Decision, error) {
	s.calls++
	return s.step(s.calls, req)
}

// oneRequirementSource declares exactly one requirement, so a sweep
// and a reviewer call are the same thing.
func oneRequirementSource(req Requirement) *fakeSource {
	return &fakeSource{name: "k8s-events", scope: ScopeCluster, reqs: []Requirement{req}}
}

// recorder captures every handler call so a test can assert on the
// sequence rather than on a single flag.
type recorder struct {
	revs     []Revocation
	restored []string
	errs     []error
}

func (r *recorder) events() AccessEvents {
	return AccessEvents{
		Revoked:  func(rev Revocation) { r.revs = append(r.revs, rev) },
		Restored: func(src string, req Requirement) { r.restored = append(r.restored, src+" "+req.String()) },
		Error:    func(err error) { r.errs = append(r.errs, err) },
	}
}

// TestAccessWatch_ConfirmsBeforeReporting is the anti-flap rule: one
// denying sweep is a sample, not a verdict. An authorizer mid-IAM
// propagation can say no for a second, and acting on that would stop a
// watch over a blip.
func TestAccessWatch_ConfirmsBeforeReporting(t *testing.T) {
	t.Parallel()
	req := Requirement{Resource: "events", Verb: "watch"}
	rv := &steppedReviewer{step: func(int, Requirement) (Decision, error) {
		return Decision{Allowed: false, Reason: "no binding"}, nil
	}}
	w := NewAccessWatch(rv, []Source{oneRequirementSource(req)})
	var rec recorder
	ev := rec.events()

	w.sweep(context.Background(), ev)
	if len(rec.revs) != 0 {
		t.Fatalf("reported after ONE denying sweep: %v — the confirm threshold is what keeps a blip from stopping a watch", rec.revs)
	}
	w.sweep(context.Background(), ev)
	if len(rec.revs) != 1 {
		t.Fatalf("got %d revocations after the confirming sweep, want 1", len(rec.revs))
	}
	got := rec.revs[0]
	if got.Source != "k8s-events" || got.Requirement != req || got.Scope != ScopeCluster {
		t.Errorf("revocation = %+v, want source k8s-events / %v / cluster scope", got, req)
	}
	if !strings.Contains(got.Error(), "no binding") {
		t.Errorf("revocation message %q drops the authorizer's own reason (#145)", got.Error())
	}
	// A standing denial is announced once, not every tick: an
	// operator who has been told does not need telling again at 2m
	// intervals until the grant comes back.
	w.sweep(context.Background(), ev)
	w.sweep(context.Background(), ev)
	if len(rec.revs) != 1 {
		t.Errorf("a standing denial reported %d times, want 1", len(rec.revs))
	}
}

// TestAccessWatch_AReviewerErrorIsNotADenial holds the same line the
// supervisor's exit classification holds (#383): "could not verify" is
// not "denied". An unreachable apiserver must neither manufacture a
// revocation nor wipe out a real one that is halfway confirmed.
func TestAccessWatch_AReviewerErrorIsNotADenial(t *testing.T) {
	t.Parallel()
	req := Requirement{Resource: "events", Verb: "watch"}
	rv := &steppedReviewer{step: func(n int, _ Requirement) (Decision, error) {
		switch n {
		case 1, 2, 3:
			return Decision{}, errors.New("connection refused")
		case 4:
			return Decision{Allowed: false}, nil // first real miss
		case 5:
			return Decision{}, errors.New("connection refused")
		default:
			return Decision{Allowed: false}, nil // second real miss
		}
	}}
	w := NewAccessWatch(rv, []Source{oneRequirementSource(req)})
	var rec recorder
	ev := rec.events()

	for range 5 {
		w.sweep(context.Background(), ev)
	}
	if len(rec.revs) != 0 {
		t.Fatalf("an unreachable reviewer produced %d revocations, want 0", len(rec.revs))
	}
	if len(rec.errs) != 4 {
		t.Fatalf("got %d reviewer errors surfaced, want 4 — a watchdog that cannot check must say so", len(rec.errs))
	}
	if !strings.Contains(rec.errs[0].Error(), "k8s-events") {
		t.Errorf("reviewer error %q does not name the source it was checking", rec.errs[0])
	}
	// Sweep 6 is the second genuine denial. The error at sweep 5 left
	// the count alone rather than resetting it, so this confirms.
	w.sweep(context.Background(), ev)
	if len(rec.revs) != 1 {
		t.Errorf("got %d revocations, want 1 — a reviewer error between two denials reset the count", len(rec.revs))
	}
}

// TestAccessWatch_AGrantComingBackClearsTheState covers the half the
// upstream fix cannot do at all: an SSAR sweep is the only thing that
// can observe a re-granted permission, because the refused call is no
// longer being made.
func TestAccessWatch_AGrantComingBackClearsTheState(t *testing.T) {
	t.Parallel()
	req := Requirement{Resource: "events", Verb: "watch"}
	allow := true
	rv := &steppedReviewer{step: func(int, Requirement) (Decision, error) {
		return Decision{Allowed: allow}, nil
	}}
	w := NewAccessWatch(rv, []Source{oneRequirementSource(req)})
	var rec recorder
	ev := rec.events()

	// One miss, then the grant returns before the second sweep can
	// confirm: nothing is reported, and the count is back to zero.
	allow = false
	w.sweep(context.Background(), ev)
	allow = true
	w.sweep(context.Background(), ev)
	allow = false
	w.sweep(context.Background(), ev)
	if len(rec.revs) != 0 {
		t.Fatalf("got %d revocations, want 0 — an intervening allow did not reset the miss count", len(rec.revs))
	}
	w.sweep(context.Background(), ev)
	if len(rec.revs) != 1 {
		t.Fatalf("got %d revocations, want 1", len(rec.revs))
	}
	// Reported once, then re-granted, then revoked again: the second
	// revocation is a new fact and must be announced.
	allow = true
	w.sweep(context.Background(), ev)
	allow = false
	w.sweep(context.Background(), ev)
	w.sweep(context.Background(), ev)
	if len(rec.revs) != 2 {
		t.Errorf("got %d revocations, want 2 — a revocation after a re-grant was swallowed by the report-once guard", len(rec.revs))
	}
	// Restored is the counterpart of Revoked and nothing else: exactly
	// one per re-grant of a REPORTED requirement, so the caller's
	// denial gauge can go back to 0 — and never on the plain allowed
	// sweeps, which would retract a denial that was never published.
	want := []string{"k8s-events " + req.String()}
	if len(rec.restored) != 1 || rec.restored[0] != want[0] {
		t.Errorf("restored = %v, want %v", rec.restored, want)
	}
}

// TestAccessWatch_ReportsOptionalRequirementsToo: AccessWatch reports,
// the caller decides. An optional denial degrades one dimension rather
// than ending the runner, but that is internal/watch's call to make —
// the watchdog must still hand it over, with Optional intact.
func TestAccessWatch_ReportsOptionalRequirementsToo(t *testing.T) {
	t.Parallel()
	req := Requirement{Resource: "nodes", Subresource: "proxy", Verb: "get", Optional: true}
	rv := &steppedReviewer{step: func(int, Requirement) (Decision, error) {
		return Decision{Allowed: false, Reason: "GKE Warden"}, nil
	}}
	w := NewAccessWatch(rv, []Source{oneRequirementSource(req)}).WithConfirm(1)
	var rec recorder
	ev := rec.events()

	w.sweep(context.Background(), ev)
	if len(rec.revs) != 1 {
		t.Fatalf("got %d revocations, want 1", len(rec.revs))
	}
	if !rec.revs[0].Requirement.Optional {
		t.Error("the revocation lost Requirement.Optional — the caller cannot tell a degraded dimension from a blind sentinel")
	}
}

// TestRevocation_JoinsTheDeniedClassification: a refusal found an hour
// in is terminal for the same reason one found at startup is (#383),
// so it answers the same errors.Is question.
func TestRevocation_JoinsTheDeniedClassification(t *testing.T) {
	t.Parallel()
	rev := Revocation{
		Source:      "k8s-events",
		Requirement: Requirement{Resource: "events", Verb: "watch"},
		Scope:       ScopeCluster,
	}
	if !errors.Is(rev, ErrAccessDenied) {
		t.Error("a Revocation does not satisfy errors.Is(err, ErrAccessDenied) — the supervisor would retry it forever")
	}
	wrapped := errors.Join(errors.New("runner"), rev)
	if !errors.Is(wrapped, ErrAccessDenied) {
		t.Error("the classification does not survive wrapping")
	}
}

// TestAccessWatch_SkipsSourcesThatDeclareNothing mirrors Probe: a
// source with no declared access has nothing to re-check, and a
// watchdog over zero requirements says so rather than pretending to
// watch.
func TestAccessWatch_SkipsSourcesThatDeclareNothing(t *testing.T) {
	t.Parallel()
	rv := &steppedReviewer{step: func(int, Requirement) (Decision, error) {
		t.Error("reviewed a source that declares no access")
		return Decision{}, nil
	}}
	w := NewAccessWatch(rv, []Source{bareSource{}})
	if w.Watching() {
		t.Error("Watching() is true with no declaring sources")
	}
	var rec recorder
	ev := rec.events()
	w.sweep(context.Background(), ev)
	if len(rec.revs) != 0 {
		t.Errorf("got %d revocations from a non-declaring source", len(rec.revs))
	}
}

// TestAccessWatch_RunStopsOnlyOnCancel: Run returns nil on shutdown
// and never otherwise. A watchdog that gave up because the apiserver
// hiccuped would be worse than none, since its silence reads as "all
// grants present".
func TestAccessWatch_RunStopsOnlyOnCancel(t *testing.T) {
	t.Parallel()
	req := Requirement{Resource: "events", Verb: "watch"}
	rv := &steppedReviewer{step: func(int, Requirement) (Decision, error) {
		return Decision{}, errors.New("apiserver unreachable")
	}}
	w := NewAccessWatch(rv, []Source{oneRequirementSource(req)})

	for _, tc := range []struct {
		name     string
		interval time.Duration
	}{
		{"sweeping", time.Millisecond},
		{"disabled", 0}, // --access-recheck=0
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- w.Run(ctx, tc.interval, AccessEvents{}) }()
			select {
			case err := <-done:
				t.Fatalf("Run returned %v before cancellation", err)
			case <-time.After(20 * time.Millisecond):
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run on cancel = %v, want nil", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Run did not return after cancellation")
			}
		})
	}
}

// TestAccessWatch_ConfirmFloor: a caller asking for zero confirming
// sweeps means "report immediately", not "never report".
func TestAccessWatch_ConfirmFloor(t *testing.T) {
	t.Parallel()
	w := NewAccessWatch(&steppedReviewer{}, nil).WithConfirm(0)
	if w.confirm != 1 {
		t.Errorf("confirm = %d, want 1", w.confirm)
	}
}
