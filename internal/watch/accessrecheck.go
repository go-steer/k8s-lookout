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

package watch

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// accessRecheck is the runner's policy over sources.AccessWatch
// (issue #385): the library finds a permission that was granted at
// startup and is refused now; this decides what the sentinel does
// about it.
//
// The split matters because the two halves answer different
// questions. Whether a grant is gone is a fact about the cluster and
// belongs where the SSAR is. What losing it means — one degraded
// dimension, or a runner that must stop — is a fact about this
// deployment, and it has to match the posture Probe already takes at
// startup, which lives here.
//
// Three things happen on a confirmed revocation, in this order:
//
//  1. lookout_source_denied goes to 1 for that (source, resource).
//     It goes back to 0 if the grant returns, so the series answers
//     "is coverage missing right now", not "was it ever".
//  2. A kind=sentinel.access_revoked signal is dispatched. This is
//     the piece a log line cannot do: from this moment the source
//     sees nothing, and everyone downstream reading its silence as
//     health needs to be told once, in the channel they already read.
//  3. If the requirement was REQUIRED, the runner is cancelled with
//     an error that wraps sources.ErrAccessDenied — so #383's exit
//     classification treats it as terminal and does not restart a
//     cluster whose authorizer has settled on no.
//
// Step 3 is deliberately NOT the issue's original sketch, which was
// to un-arm the source's readiness probe. That would hold /readyz at
// 503 for the whole process because one cluster in a fleet lost one
// grant — exactly the defect #383 had just fixed. Routing it through
// the terminal path instead gets the honest answer with none of that:
// the cluster is dropped from the readiness expectation and shown as
// degraded on /readyz?verbose, its siblings are untouched, and at N=1
// the process exits non-zero so the kubelet restarts it into the loud
// §11 startup refusal.
//
// An OPTIONAL requirement never ends the runner. It is the same
// degrade-and-say-so posture Probe takes for the canonical case
// (saturation's nodes/proxy read, platform-denied on GKE Autopilot —
// issue #145): the source runs with one dimension dark.
type accessRecheck struct {
	mu  sync.Mutex
	err error
}

// fatal records the first terminal revocation. First wins, like the
// feed-failure path: later ones are consequences of the same lost
// grant and the first is the one worth reporting.
func (a *accessRecheck) fatal(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err == nil {
		a.err = err
	}
}

// failure returns the terminal revocation error, or nil.
func (a *accessRecheck) failure() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}

// start launches this runner's capability re-check and returns the
// handle whose failure() the caller surfaces after RunAll — the same
// shape the graph feed's fatal error takes, for the same reason: the
// goroutine cancels the runner, and the error has to outlive it to be
// classified.
//
// Returns nil when there is nothing to watch (--access-recheck=0, or
// no enabled source declares access), and says so: a watchdog that is
// off must not be mistaken for one that is quiet.
func (a *accessRecheck) start(ctx context.Context, cancel context.CancelFunc, interval time.Duration, reviewer sources.AccessReviewer, srcs []sources.Source, m *metrics, emit func(engine.Signal)) {
	if interval <= 0 {
		log.Printf("access recheck: disabled (--access-recheck=0) — a grant revoked after startup will NOT be noticed; the §11 probe is a point-in-time answer")
		return
	}
	w := sources.NewAccessWatch(reviewer, srcs)
	if !w.Watching() {
		log.Printf("access recheck: no enabled source declares RBAC — nothing to re-check")
		return
	}
	log.Printf("access recheck: enabled (every %s, %d consecutive denials to confirm) — a permission revoked after startup fires kind=%s and, when the permission is required, stops this cluster's runner",
		interval, sources.DefaultAccessConfirm, sources.KindAccessRevoked)

	go func() {
		_ = w.Run(ctx, interval, sources.AccessEvents{
			Revoked: func(rev sources.Revocation) {
				required := !rev.Requirement.Optional
				m.sourceDenied.WithLabelValues(rev.Source, requirementResource(rev.Requirement), strconv.FormatBool(required)).Set(1)
				log.Printf("access recheck: %v", rev)
				// Dispatched BEFORE the cancel below: the runner's
				// ctx is what carries the inject to the sink, so
				// cancelling first would drop the one report that
				// tells anyone the coverage is gone.
				emit(accessRevokedSignal(rev, time.Now()))
				if !required {
					log.Printf("access recheck: source %q keeps running with that dimension disabled — an optional requirement degrades one facet, it does not blind the source", rev.Source)
					return
				}
				a.fatal(fmt.Errorf("§11: a permission this sentinel held at startup is gone: %w", rev))
				cancel()
			},
			Restored: func(source string, req sources.Requirement) {
				m.sourceDenied.WithLabelValues(source, requirementResource(req), strconv.FormatBool(!req.Optional)).Set(0)
				log.Printf("access recheck: source %q regained permission to %q — coverage restored", source, req)
			},
			Error: func(err error) {
				// Logged, never acted on: "could not verify" is not
				// "denied" (#383). Worth a line all the same, because
				// a watchdog that has stopped being able to check is
				// silent in exactly the way a healthy one is.
				log.Printf("access recheck: could not verify: %v", err)
			},
		})
	}()
}

// requirementResource is the metric's resource label: the RBAC rule's
// resource[/subresource], without the verb or the scope. Those two
// vary per requirement within one source and would multiply the
// series for no operator benefit — the signal and the log line carry
// the full requirement.
func requirementResource(req sources.Requirement) string {
	res := req.Resource
	if req.Subresource != "" {
		res += "/" + req.Subresource
	}
	if req.Group != "" {
		res += "." + req.Group
	}
	return res
}

// accessRevokedSignal renders a confirmed revocation as a signal.
//
// The subject is the sentinel's own capability, so the object
// identity is synthetic in the same way the quota source's is: kind
// "Grant", named for the source that lost the permission, with a UID
// stable across restarts and across sweeps so the dedup window folds
// a re-report instead of opening a second incident. Deployment
// identity (cluster/project/region/zone) is stamped by the pipeline,
// as for every other signal.
//
// Severity follows the same line the runner does: losing a REQUIRED
// permission is critical — the source is blind and this cluster is
// about to stop being watched — while an optional one is a warning,
// because the source keeps running with one dimension dark.
func accessRevokedSignal(rev sources.Revocation, now time.Time) engine.Signal {
	sev := engine.SeverityCritical
	scope := "this source can no longer run"
	if rev.Requirement.Optional {
		sev = engine.SeverityWarning
		scope = "this source keeps running with that dimension disabled"
	}
	return engine.Signal{
		Kind:     sources.KindAccessRevoked,
		Source:   engine.SourceSentinel,
		Severity: sev,
		TriageEvent: engine.TriageEvent{
			Key: engine.EventKey{
				UID:    "access/" + rev.Source + "/" + rev.Requirement.String(),
				Reason: sources.ReasonAccessRevoked,
			},
			KindOfObject: "Grant",
			Name:         rev.Source,
			Message: fmt.Sprintf("%s — %s. The sentinel held this permission when it started, so the source's silence since the revocation does NOT mean the cluster is healthy.",
				rev.Error(), scope),
			FirstSeen: now,
			LastSeen:  now,
			Count:     1,
		},
	}
}
