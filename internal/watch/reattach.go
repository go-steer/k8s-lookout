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
	"encoding/json"
	"fmt"
	"log"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// Ancestor reattachment (§7.7 + issue #220): one root cause observed
// from two altitudes must not become two sessions.
//
// A pod that cannot mount its secret fires a critical k8s-event and
// opens an enriched per-incident session. Its Deployment, unable to
// progress for the same reason, fires a warning — which §7.7 routes
// to the watchboard, where it becomes a digest entry in a SECOND
// session with no pointer back to the diagnosis. Storm correlation
// (§7.5) does not catch this: the two incidents share the Deployment
// ancestor, but a pair never reaches --storm-min, and a storm session
// is the wrong artifact anyway (aggregate blast radius, not one
// incident seen twice).
//
// So: at flush time, before a buffered warning becomes a digest
// entry, ask whether its blast-radius ancestor already owns a live
// per-incident session. If it does, the warning goes there as a
// kind=family.member followup (§10.3, the same shape a cross-source
// dedup join uses) instead of into the digest.
//
// FLUSH time, not route time, is load-bearing. In the trace that
// motivated this, the warning was buffered 60ms BEFORE the critical
// event opened its session; a check at board.Add would have found
// nothing. The batching delay the watchboard already imposes is
// exactly what makes the correlation possible.

// reattachAncestorKinds are the ancestor classes a warning may
// reattach through — §7.5 classes 0-2 (graphfeed.ancestorClass):
// placement, the owner chain, and shared config/PVC.
//
// Namespace (class 3) is deliberately EXCLUDED. Every incident in a
// namespace shares it, so admitting it would reattach unrelated
// warnings to whichever critical incident happens to be open there —
// worse than the two-session split this fixes. The synthetic Registry
// key (§7.5, issue #213) is excluded for the same reason at fleet
// scale: it spans workloads by design.
var reattachAncestorKinds = map[string]bool{
	"Node":                  true,
	"Deployment":            true,
	"ReplicaSet":            true,
	"StatefulSet":           true,
	"DaemonSet":             true,
	"Job":                   true,
	"CronJob":               true,
	"ConfigMap":             true,
	"Secret":                true,
	"PersistentVolumeClaim": true,
}

// ancestorKeysFor resolves sig's object to its reattachment-eligible
// blast-radius keys, best-priority first (the resolver's own order).
// Empty when the resolver is absent, the topology index has not
// synced, or every candidate was a namespace-class key.
func (d *dispatcher) ancestorKeysFor(sig engine.Signal) []string {
	if d.resolver == nil {
		return nil
	}
	cands := d.resolver.Ancestors(engine.ObjectRef{
		Kind:      sig.KindOfObject,
		Namespace: sig.Namespace,
		Name:      sig.Name,
	})
	keys := make([]string, 0, len(cands))
	for _, a := range cands {
		if reattachAncestorKinds[a.Kind] {
			keys = append(keys, a.Key())
		}
	}
	return keys
}

// noteAncestors indexes a freshly bound incident by its blast-radius
// keys so a later watchboard warning under the same ancestor can find
// its session. Nil-safe and inert without a resolver.
func (d *dispatcher) noteAncestors(key engine.EventKey, sig engine.Signal) {
	if keys := d.ancestorKeysFor(sig); len(keys) > 0 {
		d.dedup.NoteAncestors(key, keys)
	}
}

// reattachWatchboardEntry is the watchboard's flush-time callback: it
// reports whether sig was delivered into an existing per-incident
// session instead of the digest. A true return means the entry is
// consumed — the caller must drop it from the buffer.
//
// Called with the watchboard's lock held, same contract as bind: it
// must not call back into the watchboard.
func (d *dispatcher) reattachWatchboardEntry(ctx context.Context, sig engine.Signal, count int) bool {
	if d.resolver == nil {
		return false
	}
	key := sig.CanonicalKey()
	// Already bound (a storm claimed it while it sat in the buffer, or
	// a duplicate opened it): the incident has a home and bindings are
	// per-incident. Leave it to bindWatchboardIncident's existing
	// precedence rule.
	if _, bound := d.dedup.LookupSession(key); bound {
		return false
	}
	cands := d.ancestorKeysFor(sig)
	sid, matched, ok := d.dedup.SessionForAncestors(key, cands, engine.SourceFamily(sig.Kind))
	if !ok {
		return false
	}
	if !d.injectReattachFollowup(ctx, sig, count, sid, matched) {
		return false
	}
	// Bind so the §7.4 outcome for this warning closes into the same
	// session its evidence landed in, and the recovery tracker knows
	// where to send it.
	d.dedup.BindIncident(key, sid, sig.IncidentRef())
	if d.tracker != nil {
		d.tracker.Track(engine.Incident{
			Key:       key,
			SessionID: sid,
			FirstSeen: sig.FirstSeen,
			Ref:       sig.IncidentRef(),
		})
	}
	d.metrics.watchboardReattached.WithLabelValues(sig.Kind).Inc()
	log.Printf("reattach %s %s/%s → sid=%s (ancestor=%s: a live incident already owns this blast radius — followup instead of a digest entry)",
		sig.Kind, sig.Namespace, sig.Name, sid, matched)
	return true
}

// injectReattachFollowup delivers the §10.3 kind=family.member
// payload into the ancestor's session. Returns false when the inject
// failed, so the caller leaves the entry in the digest rather than
// dropping it — a failed reattachment must degrade to the pre-#220
// behavior, never to silence.
//
// NOT store-recorded: the §9.1 occurrence row for this signal was
// already written at route time (route=watchboard, dispatch.go),
// because that is when severity routing made its decision and when
// the occurrence actually happened. Writing a second row here would
// double-count the occurrence and corrupt the lookback rates §9.2
// reads; rewriting the first would mean moving every watchboard row's
// emitted_at to flush time. The reattachment is observable through
// lookout_watchboard_reattached_total and the log line above.
func (d *dispatcher) injectReattachFollowup(ctx context.Context, sig engine.Signal, count int, sid, ancestor string) bool {
	payload := inject.FamilyMemberPayload{
		Kind:         inject.KindFamilyMember,
		MemberKind:   sig.Kind,
		Reason:       sig.Key.Reason,
		Severity:     string(sig.Severity),
		Namespace:    sig.Namespace,
		KindOfObject: sig.KindOfObject,
		Name:         sig.Name,
		UID:          sig.Key.UID,
		Fingerprint:  sig.Fingerprint,
		Family:       ancestor,
		OpenedBy:     engine.SourceFamily(sig.Kind),
		Cluster:      sig.Cluster,
		SessionID:    sid,
		Message: fmt.Sprintf(
			"blast-radius join: %s (%s, severity=%s, count=%d) shares the ancestor %s with this session's incident — the same failure seen from a different altitude, reattached here instead of opening a watchboard digest entry; at most one per source family per incident per window",
			sig.Kind, sig.Key.Reason, sig.Severity, count, ancestor),
		DesignRef: inject.FamilyMemberDesignRef,
	}
	if d.dryRun {
		out, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", sid, string(out))
		return true
	}
	if err := d.injector.Append(ctx, sid, payload); err != nil {
		d.metrics.injectErrors.WithLabelValues("watchboard", "inject").Inc()
		log.Printf("reattach %s %s/%s into sid=%s failed (%v) — falling back to the digest",
			sig.Kind, sig.Namespace, sig.Name, sid, err)
		return false
	}
	return true
}
