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
	"fmt"
	"time"
)

// AccessWatch keeps the §11 charter — "never a silently empty watch" —
// true for the whole life of the process, not just its first second
// (issue #385).
//
// Probe is a strong preflight: it SSARs every source's declared
// requirements before anything watches, so the canonical
// "informer retries a 403 forever" failure cannot happen to us at
// startup. Nothing re-checked afterwards. A grant revoked at runtime
// degraded into exactly the state the charter exists to prevent: the
// reflector retries the refused LIST/WATCH on client-go's backoff
// forever, HasSynced stays true because it latched at arm time and
// never un-latches, and the source emits nothing — which reads
// downstream as "cluster healthy". A sentinel reporting no incidents
// because it lost permission to look is the failure mode §11 was
// written against; we had moved it in time rather than eliminated it.
//
// # Why an interval and not a 403 handler
//
// The obvious trigger is the forbidden watch error itself: hook every
// informer's WatchErrorHandler and re-probe on apierrors.IsForbidden.
// The interval sweep is better here for three reasons.
//
// It covers the poll-driven sources. expiry, quota, saturation,
// notifications and token-burn have no informer and so no watch error
// to hook; a revoked grant under any of them would never fire a
// handler, and those are precisely the sources whose silence is
// hardest to notice.
//
// It answers the question the 403 cannot. A forbidden watch says the
// call was refused, not whether the grant is gone — an authorizer
// mid-IAM-propagation and a deleted ClusterRoleBinding look identical
// from there. SSAR distinguishes them in one round trip, which is the
// seam we have and the upstream fix (gke-labs/kube-agents#1351, a flat
// 10-minute hold) does not.
//
// It is the only thing that can re-arm. Once an informer is held off,
// the refused call is not being made, so nothing can observe that the
// grant came back except an SSAR sweep.
//
// The one case it does not catch is an authorizer that answers SSAR
// "allowed" and then denies the real call — a webhook authorizer, or
// GKE Autopilot's Warden (#145). A re-probe there returns allowed, so
// there is nothing for this watchdog to act on either way.
//
// # What it does not decide
//
// AccessWatch reports; the caller acts. It has no opinion on whether a
// revocation should end the runner, degrade a cluster or open an
// incident — that is the sentinel's policy (internal/watch), and
// keeping it out of here is what lets the whole thing be tested
// against a scripted reviewer with no cluster in sight.
type AccessWatch struct {
	reviewer AccessReviewer
	srcs     []Source
	// confirm is how many consecutive sweeps must agree before a
	// denial is reported. A single sweep is not enough: an authorizer
	// can flap mid-IAM-propagation, and acting on one sample would
	// turn a two-second blip into a stopped watch.
	confirm int

	// misses counts consecutive denials per requirement key, reset by
	// any sweep that finds the requirement allowed again. Only Run
	// touches it, so it needs no lock.
	misses map[string]int
	// reported remembers what has already been handed to the caller,
	// so a standing denial is announced once rather than every tick.
	reported map[string]bool
}

// Revocation is one requirement that was allowed at startup and is
// denied now.
type Revocation struct {
	// Source is the Name() of the source that declared the
	// requirement.
	Source string
	// Requirement is the permission that went away. Its Optional
	// field is the whole difference between "this source is degraded"
	// and "this sentinel is blind": Probe's startup posture, applied
	// to the same requirement at runtime.
	Requirement Requirement
	// Scope is the declaring source's tier (§11).
	Scope Scope
	// Decision is the authorizer's verdict verbatim, reason included
	// (#145: it may name a platform policy no grant can satisfy).
	Decision Decision
}

// Error renders the revocation as the operator-facing sentence, in
// deliberate parallel to DeniedError's: the same fact, found later.
func (r Revocation) Error() string {
	return fmt.Sprintf("source %q lost permission to %q (scope: %s) while running: %s",
		r.Source, r.Requirement, r.Scope, DenialRemedy(r.Decision))
}

// Unwrap joins the ErrAccessDenied classification, so a revocation
// that ends a runner is terminal for exactly the same reason a startup
// DeniedError is (issue #383): the authorizer was asked and said no.
// One question — errors.Is(err, ErrAccessDenied) — covers a refusal
// found at second zero and one found an hour in.
func (r Revocation) Unwrap() error { return ErrAccessDenied }

// KindAccessRevoked is the signal a confirmed runtime revocation puts
// on the wire (§8 v1-additive; the ledger entry is in
// pkg/inject/schema). APPEND-ONLY.
//
// It is a signal and not merely a log line because it is a cluster
// fact of exactly the sort the sentinel exists to report: from this
// moment the sentinel's coverage of that source is gone, and every
// later silence from it means less than it did. A log line reaches
// whoever is tailing logs; a signal reaches the same session an
// operator is already reading incidents in.
//
// Namespaced `sentinel.` rather than under a source name because the
// subject is the sentinel's own capability, not the workload the
// denied source watches — the denied source is the signal's `name`.
const KindAccessRevoked = "sentinel.access_revoked"

// ReasonAccessRevoked is the wire reason on a KindAccessRevoked
// signal, and half of its dedup identity.
const ReasonAccessRevoked = "access_revoked"

// DefaultAccessRecheck is the sweep interval when the caller does not
// pick one. An SSAR is a single cheap API call and the sentinel
// declares a few dozen requirements, so the load is negligible; the
// interval is set by how long a coverage gap may go unnoticed, not by
// cost. With DefaultAccessConfirm, a revocation is reported within
// roughly two intervals.
const DefaultAccessRecheck = 2 * time.Minute

// DefaultAccessConfirm is how many consecutive sweeps must agree.
const DefaultAccessConfirm = 2

// NewAccessWatch builds a watchdog over every source that declares
// its access. Sources that do not implement AccessDeclarer are
// skipped, exactly as Probe skips them.
func NewAccessWatch(reviewer AccessReviewer, srcs []Source) *AccessWatch {
	declaring := make([]Source, 0, len(srcs))
	for _, s := range srcs {
		if _, ok := s.(AccessDeclarer); ok {
			declaring = append(declaring, s)
		}
	}
	return &AccessWatch{
		reviewer: reviewer,
		srcs:     declaring,
		confirm:  DefaultAccessConfirm,
		misses:   make(map[string]int),
		reported: make(map[string]bool),
	}
}

// WithConfirm overrides how many consecutive denying sweeps are needed
// before a revocation is reported. Values below 1 are treated as 1.
func (w *AccessWatch) WithConfirm(n int) *AccessWatch {
	if n < 1 {
		n = 1
	}
	w.confirm = n
	return w
}

// Watching reports whether there is anything to sweep — false when no
// resolved source declares access, in which case the caller can skip
// starting the loop and say so.
func (w *AccessWatch) Watching() bool { return len(w.srcs) > 0 }

// AccessEvents is what a sweep can tell the caller. Every field is
// optional; a nil handler is simply not called.
type AccessEvents struct {
	// Revoked fires once per requirement that has been denied for
	// confirm consecutive sweeps — not once per sweep, because an
	// operator who has been told does not need telling again every
	// two minutes until the grant comes back.
	Revoked func(Revocation)
	// Restored fires when a requirement that was Revoked is allowed
	// again, so a caller can retract whatever it published — the
	// denial gauge, most of all. Only ever the counterpart of a
	// Revoked call: an ordinary allowed sweep says nothing.
	Restored func(source string, req Requirement)
	// Error fires when the reviewer itself could not answer. "Could
	// not verify" is not "denied" (the same rule the supervisor's
	// exit classification follows), so this never accompanies a
	// Revoked — but a watchdog that has silently stopped being able
	// to check must still say so, since its silence would otherwise
	// read as "all grants present".
	Error func(error)
}

// Run sweeps every declared requirement every interval until ctx is
// cancelled.
//
// Returns nil on ctx cancellation and never on anything else: a
// watchdog that gave up because the apiserver hiccuped is worse than
// no watchdog at all.
//
// Handlers are called from Run's goroutine, so a caller that blocks in
// one delays the next sweep — which is the right coupling: the
// sentinel's response to a revocation (report it, stop watching) is
// more urgent than the next tick.
func (w *AccessWatch) Run(ctx context.Context, interval time.Duration, ev AccessEvents) error {
	if interval <= 0 || len(w.srcs) == 0 {
		<-ctx.Done()
		return nil
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		w.sweep(ctx, ev)
	}
}

// sweep runs one pass. Split out so tests drive passes directly
// instead of sleeping through a ticker.
func (w *AccessWatch) sweep(ctx context.Context, ev AccessEvents) {
	for _, s := range w.srcs {
		decl, ok := s.(AccessDeclarer)
		if !ok {
			continue
		}
		for _, req := range decl.RequiredAccess() {
			if ctx.Err() != nil {
				return
			}
			key := s.Name() + "\x00" + req.String()
			d, err := w.reviewer.Allowed(ctx, req)
			if err != nil {
				// Not evidence either way. Leave the miss count
				// where it is so a flapping apiserver neither
				// accumulates toward a false revocation nor resets
				// a real one that is halfway confirmed.
				if ev.Error != nil && ctx.Err() == nil {
					ev.Error(fmt.Errorf("access recheck: source %q: %q: %w", s.Name(), req, err))
				}
				continue
			}
			if d.Allowed {
				// The grant is back (or never left). Clearing
				// `reported` is what makes a later revocation of the
				// same requirement announceable again.
				delete(w.misses, key)
				if w.reported[key] {
					delete(w.reported, key)
					if ev.Restored != nil {
						ev.Restored(s.Name(), req)
					}
				}
				continue
			}
			w.misses[key]++
			if w.misses[key] < w.confirm || w.reported[key] {
				continue
			}
			w.reported[key] = true
			if ev.Revoked != nil {
				ev.Revoked(Revocation{
					Source:      s.Name(),
					Requirement: req,
					Scope:       s.Scope(),
					Decision:    d,
				})
			}
		}
	}
}
