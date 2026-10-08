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
	"strings"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/store"
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

// declaredAncestorKinds are owner kinds the topology graph does not
// index but a source can name on the signal itself (issue #542): a
// cert-manager Certificate is the owner of the ACME Challenge/Order
// stalls the expiry source reports, and no graph vertex models it.
//
// The key reads the SIGNAL, in two shapes that meet on one value:
//   - a signal ABOUT a Certificate (expiry.warning, or a k8s-event on
//     it) keys on itself — the incident a stall should join;
//   - a signal whose ControllerRef names a Certificate
//     ("Certificate/<name>", same namespace — expiry.challenge_stuck,
//     expiry.order_failed) keys on that owner.
//
// Kept to an explicit allow-list for the same reason the graph kinds
// above are: a declared key joins incidents, so each kind admitted
// here must be a real one-root-cause owner, never a shared grouping.
var declaredAncestorKinds = map[string]bool{
	"Certificate": true,
}

// declaredAncestorKeys returns sig's signal-declared ancestor keys:
// its ControllerRef owner first (nearest), then itself.
func declaredAncestorKeys(sig engine.Signal) []string {
	var keys []string
	if kind, name, ok := strings.Cut(sig.ControllerRef, "/"); ok && declaredAncestorKinds[kind] && name != "" {
		keys = append(keys, engine.Ancestor{Kind: kind, Namespace: sig.Namespace, Name: name}.Key())
	}
	if declaredAncestorKinds[sig.KindOfObject] && sig.Name != "" {
		keys = append(keys, engine.Ancestor{Kind: sig.KindOfObject, Namespace: sig.Namespace, Name: sig.Name}.Key())
	}
	return keys
}

// ancestorKeysFor resolves sig's object to its reattachment-eligible
// blast-radius keys, best-priority first: signal-declared owners
// (declaredAncestorKinds), then the resolver's own order. Empty when
// the resolver is absent (the stage is inert without it — see the
// wiring), or when the signal declares nothing, the topology index
// has not synced, and every candidate was a namespace-class key.
func (d *dispatcher) ancestorKeysFor(sig engine.Signal) []string {
	if d.resolver == nil {
		return nil
	}
	keys := declaredAncestorKeys(sig)
	cands := d.resolver.Ancestors(engine.ObjectRef{
		Kind:      sig.KindOfObject,
		Namespace: sig.Namespace,
		Name:      sig.Name,
	})
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

// siblingOwnerKinds are the ancestor classes a NEW incident may fold
// into a sibling's session through (DESIGN.md §7.7 amendment
// 2026-10-08): the owner chain only — §7.5 class 1. Placement and
// shared config are excluded on purpose: two Deployments on one node,
// or mounting one ConfigMap, failing the same way are two workloads
// and stay two incidents unless the storm stage groups them. The
// Deployment is the key that matters: across a rollout the pods'
// ReplicaSets differ, their Deployment does not.
var siblingOwnerKinds = map[string]bool{
	"Deployment":  true,
	"ReplicaSet":  true,
	"StatefulSet": true,
	"DaemonSet":   true,
	"Job":         true,
	"CronJob":     true,
}

// foldSibling is the sibling fold (§7.7 amendment 2026-10-08): a new
// critical incident whose workload owner already has a live incident
// of the same class — same kind, same canonical reason — goes into
// that incident's session as a kind=family.member followup instead
// of opening a second one. It reports whether sig was consumed.
//
// The case it exists for: a crash-looping Deployment is rolled out,
// the old ReplicaSet's pod is still backing off and the new one's
// starts. Two pods are two dedup keys (uid, reason), a pair never
// reaches --storm-min, and a storm is the wrong artifact anyway
// (aggregate blast radius, not one fault seen twice — #220's
// reasoning). The fingerprint is untouched: it already hashes the
// class, not the pod.
//
// Runs AFTER the storm stage, so a burst big enough to storm still
// storms exactly as before. Inert without a resolver (the topology
// graph is built only under --storm, like #220's reattachment). A
// failed inject degrades to the incident's own session, never to
// silence.
func (d *dispatcher) foldSibling(ctx context.Context, sig engine.Signal, key engine.EventKey, count int) bool {
	if d.resolver == nil {
		return false
	}
	var cands []string
	for _, a := range d.resolver.Ancestors(engine.ObjectRef{
		Kind:      sig.KindOfObject,
		Namespace: sig.Namespace,
		Name:      sig.Name,
	}) {
		if siblingOwnerKinds[a.Kind] {
			cands = append(cands, a.Key())
		}
	}
	sid, matched, ok := d.dedup.SessionForSibling(key, sig.Kind, cands)
	if !ok {
		return false
	}
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
		Family:       matched,
		OpenedBy:     engine.SourceFamily(sig.Kind),
		Cluster:      sig.Cluster,
		SessionID:    sid,
		Message: fmt.Sprintf(
			"same-workload join: %s %s (%s, count=%d) shows the same failure (%s) as this session's incident under %s — another pod of the same workload (a rollout's new ReplicaSet, or another replica), folded here instead of opening a second incident",
			sig.KindOfObject, sig.Name, sig.Key.Reason, count, key.Reason, matched),
		DesignRef: inject.FamilyMemberDesignRef,
	}
	if err := d.injector.Append(ctx, sid, payload); err != nil {
		d.metrics.injectErrors.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), "inject").Inc()
		log.Printf("sibling fold %s %s/%s into sid=%s failed (%v) — opening its own session",
			sig.Key.Reason, sig.Namespace, sig.Name, sid, err)
		return false
	}
	// Bind so this pod's later events are plain duplicates of the
	// shared session and its §7.4 outcome closes there. Deliberately
	// NOT indexed by ancestors (noteAncestors) and NOT noted as a storm
	// member session: the original incident stays the one target, and
	// a later storm supersedes that session once, not twice.
	d.dedup.BindIncident(key, sid, sig.IncidentRef())
	if d.tracker != nil {
		d.tracker.Track(engine.Incident{
			Key:       key,
			SessionID: sid,
			FirstSeen: sig.FirstSeen,
			Ref:       sig.IncidentRef(),
		})
	}
	d.store.Record(sig, store.Outcome{Route: store.RouteFollowup, SessionID: sid})
	log.Printf("sibling fold %s %s/%s → sid=%s (ancestor=%s: a live incident of the same class already owns this workload — followup instead of a second incident)",
		sig.Key.Reason, sig.Namespace, sig.Name, sid, matched)
	return true
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
