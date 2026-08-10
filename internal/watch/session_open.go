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

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// The four open→bind→track→record paths — the fresh per-incident open
// (dispatch), the deferred per-incident retry after a failed create
// (dispatch, issue #84), storm formation, and the session-less storm
// retry (issue #81) — used to hand-roll identical sink-open bookkeeping
// and, for the two storm paths, an identical member rebind loop. The
// copies drifted (issue #110): a fix to the session-create metric or
// the member-ref resolution in one path silently missed the others.
// The two helpers below are the single home for the shared mechanics;
// each caller keeps only its path-specific parts (log subject, §9.1
// store record, dedup fallback, and when it counts the partial-open
// inject error).

// openSession opens a new sink session for payload and does the metric
// bookkeeping every open path shares: sessionCreates{ok|error} and, on
// a hard failure, injectErrors{session_create,reason}. reason is the
// (unbounded) Event.reason; it is bounded here via metrics.boundReason
// (issue #109) exactly as the call sites did inline.
//
// It returns (sid, err, ok):
//   - ok == false: the open hard-failed (sid==""). The two failure
//     metrics are already counted; the caller must log the failure with
//     its own subject, do its path-specific §9.1 record + dedup
//     fallback, and stop.
//   - ok == true: sid is the opened session and sessionCreates{ok} is
//     counted. err may still be non-nil — a partial open, where the
//     container opened but the initial delivery failed (see Sink docs).
//     The caller binds/tracks/records as normal and counts the inject
//     error itself, because the paths differ on WHEN they do so (storm
//     formation defers it past the member rebind loop).
func (d *dispatcher) openSession(ctx context.Context, payload any, reason string) (sid string, err error, ok bool) {
	// Fit the payload under the sink's per-inject wire ceiling before the
	// open: an oversized enrichment bundle otherwise makes the daemon
	// 400 the initial inject, leaving a bound-but-empty session (#198).
	// Every open shape (incident + storm) routes through here.
	payload = d.fitInject(payload)
	sid, err = d.injector.OpenIncident(ctx, payload)
	if sid == "" {
		d.metrics.sessionCreates.WithLabelValues("error").Inc()
		d.metrics.injectErrors.WithLabelValues(d.metrics.boundReason(reason), "session_create").Inc()
		return "", err, false
	}
	d.metrics.sessionCreates.WithLabelValues("ok").Inc()
	return sid, err, true
}

// rebindStormMembers rebinds and retracks every storm member to the
// storm session sid: each member's dedup entry is attached to the storm
// (so its followups and §7.4 outcomes route there) and, when a recovery
// tracker is present, the incident is retracked against sid. For each
// member it resolves the richest available ref — the trigger's own
// IncidentRef when the member IS the triggering signal, else the
// member's existing dedup binding (which carries container/controller
// detail), else the coarse ref built from the member fields.
//
// countMembers controls the stormMembers{suppressed|superseded} split:
// formation counts it (a member that fired per-incident before the
// storm was superseded; one that never did was suppressed), while the
// session-less retry path (issue #81) deliberately does not — its
// post-failure attaches were already counted "attached" and the
// founding split was lost with the failed formation, so under-counting
// beats double-counting.
func (d *dispatcher) rebindStormMembers(members []engine.StormMember, sid, fingerprint string, triggerKey engine.EventKey, triggerRef engine.IncidentRef, countMembers bool) {
	for _, m := range members {
		ref := engine.IncidentRef{
			Namespace:    m.Namespace,
			KindOfObject: m.KindOfObject,
			Name:         m.Name,
			Fingerprint:  m.Fingerprint,
			Cluster:      d.cluster,
		}
		if m.Key == triggerKey {
			ref = triggerRef // trigger/attacher: full identity in hand
		} else if bound, ok := d.dedup.LookupBinding(m.Key); ok {
			ref = bound.Ref // richer (container, controller_ref)
		}
		d.dedup.AttachToStorm(m.Key, sid, fingerprint, ref)
		if d.tracker != nil {
			d.tracker.Track(engine.Incident{Key: m.Key, SessionID: sid, FirstSeen: m.FirstSeen, Ref: ref})
		}
		if countMembers {
			if m.SessionID == "" {
				d.metrics.stormMembers.WithLabelValues("suppressed").Inc()
			} else {
				d.metrics.stormMembers.WithLabelValues("superseded").Inc()
			}
		}
	}
}
