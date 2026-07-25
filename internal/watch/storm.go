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
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// Dispatcher side of storm correlation (DESIGN.md §7.5): the
// StormCorrelator (pkg/engine) decides WHAT correlates; this file owns
// the session mechanics — one storm session, member supersede/attach
// followups, dedup rebinding so followups and §7.4 outcomes route to
// the storm session, and the storm's own resolved record when every
// member has cleared.

// stormFormed handles a StormFormed verdict: opens the ONE storm
// session, injects the kind=storm payload, rebinds every member's
// dedup entry (and recovery tracking) to the storm session, and
// leaves a kind=storm.member_superseded pointer in each member
// session that fired per-incident before the storm formed (§7.5:
// the first incidents of a burst inherently may). The triggering
// incident itself never opens a session — its suppression is the
// storm forming.
func (d *dispatcher) stormFormed(ctx context.Context, sig engine.Signal, v engine.StormVerdict) {
	sid := d.targetSid
	if d.mode == "per-incident" && !d.dryRun {
		newSid, err := d.injector.CreateSession(ctx)
		if err != nil {
			log.Printf("storm: create storm session for %s: %v", v.Storm.Ancestor.Display(), err)
			d.metrics.sessionCreates.WithLabelValues("error").Inc()
			d.metrics.injectErrors.WithLabelValues(sig.Key.Reason, "session_create").Inc()
			return
		}
		sid = newSid
		d.metrics.sessionCreates.WithLabelValues("ok").Inc()
	}
	d.storm.BindStormSession(v.Storm.ID, sid)
	info := v.Storm
	info.SessionID = sid
	// §9.1: the triggering signal's outcome is "my arrival formed the
	// storm" — earlier members were already recorded when they routed
	// (injected or watchboard) before the storm claimed them.
	d.store.Record(sig, store.Outcome{Route: store.RouteStorm, SessionID: sid, StormFingerprint: info.Fingerprint})

	// Rebind + retrack every member to the storm session: dedup
	// followups and recovery outcomes now route there (the extended
	// binding model — the entry records the storm fingerprint too).
	for _, m := range v.Members {
		ref := engine.IncidentRef{
			Namespace:    m.Namespace,
			KindOfObject: m.KindOfObject,
			Name:         m.Name,
			Fingerprint:  m.Fingerprint,
			Cluster:      d.cluster,
		}
		if m.Key == sig.Key {
			ref = sig.IncidentRef() // trigger: full identity in hand
		} else if bound, ok := d.dedup.LookupBinding(m.Key); ok {
			ref = bound.Ref // richer (container, controller_ref)
		}
		d.dedup.AttachToStorm(m.Key, sid, info.Fingerprint, ref)
		if d.tracker != nil {
			d.tracker.Track(engine.Incident{Key: m.Key, SessionID: sid, FirstSeen: m.FirstSeen, Ref: ref})
		}
		if m.SessionID == "" {
			d.metrics.stormMembers.WithLabelValues("suppressed").Inc()
		} else {
			d.metrics.stormMembers.WithLabelValues("superseded").Inc()
		}
	}
	d.metrics.stormsFormed.Inc()
	d.metrics.stormsActive.Set(float64(d.storm.ActiveStorms()))

	payload := stormPayload(info, d.cluster)
	// Enrichment (§7.6), storm flavor: the ancestor's blast radius
	// from the live graph, radius-only by design — a storm exists to
	// collapse N incidents into one session, so its enrichment must
	// not fan back out into N log fetches. Severity-gated like the
	// per-incident stage; errors ride inside the bundle.
	if d.enrich != nil && d.mode == "per-incident" && d.enrich.enabledFor(info.Severity) {
		if bundleStr := d.enrich.Storm(ctx, info); bundleStr != "" {
			payload.Enrichment = &inject.PayloadEnrichment{Bundle: bundleStr}
		}
	}
	if d.dryRun {
		out, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", sid, string(out))
	} else if err := d.injector.InjectStorm(ctx, sid, payload); err != nil {
		log.Printf("storm: inject storm for %s (sid=%s): %v", info.Ancestor.Display(), sid, err)
		d.metrics.injectErrors.WithLabelValues(sig.Key.Reason, "inject").Inc()
	}
	log.Printf("storm formed on %s: %d incidents across %d namespace(s) → sid=%s (mode=%s)",
		info.Ancestor.Display(), info.AffectedCount, info.NamespaceCount, sid, d.mode)

	// Supersede pointers into the members' pre-storm sessions.
	for _, m := range v.Members {
		if m.SessionID == "" || m.SessionID == sid {
			continue
		}
		mp := stormMemberPayload(inject.KindStormMemberSuperseded, m, info, d.cluster)
		if d.dryRun {
			out, _ := json.MarshalIndent(mp, "", "  ")
			fmt.Printf("--- dry-run payload for session %q ---\n%s\n", m.SessionID, string(out))
			continue
		}
		if err := d.injector.InjectStormMember(ctx, m.SessionID, mp); err != nil {
			log.Printf("storm: supersede member %s/%s (sid=%s): %v", m.Namespace, m.Name, m.SessionID, err)
			d.metrics.injectErrors.WithLabelValues(m.Reason, "inject").Inc()
		}
	}
}

// stormAttached handles a late arrival (§7.5 window semantics): while
// the storm is unresolved, an incident sharing its key attaches to
// the storm session as a kind=storm.member followup instead of
// opening a session.
func (d *dispatcher) stormAttached(ctx context.Context, sig engine.Signal, v engine.StormVerdict) {
	sid := v.Storm.SessionID
	if d.mode == "shared" {
		sid = d.targetSid
	}
	ref := sig.IncidentRef()
	// §9.1: a late arrival's outcome is membership, not a session of
	// its own.
	d.store.Record(sig, store.Outcome{Route: store.RouteStormMember, SessionID: sid, StormFingerprint: v.Storm.Fingerprint})
	d.dedup.AttachToStorm(sig.Key, sid, v.Storm.Fingerprint, ref)
	if d.tracker != nil {
		d.tracker.Track(engine.Incident{Key: sig.Key, SessionID: sid, FirstSeen: v.Member.FirstSeen, Ref: ref})
	}
	d.metrics.stormMembers.WithLabelValues("attached").Inc()

	mp := stormMemberPayload(inject.KindStormMember, v.Member, v.Storm, d.cluster)
	mp.StormSessionID = sid
	if d.dryRun {
		out, _ := json.MarshalIndent(mp, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", sid, string(out))
	} else if err := d.injector.InjectStormMember(ctx, sid, mp); err != nil {
		log.Printf("storm: attach member %s/%s to %s (sid=%s): %v", sig.Namespace, sig.Name, v.Storm.Ancestor.Display(), sid, err)
		d.metrics.injectErrors.WithLabelValues(sig.Key.Reason, "inject").Inc()
	}
	log.Printf("storm attach %s %s/%s → %s (sid=%s, members=%d)",
		sig.Key.Reason, sig.Namespace, sig.Name, v.Storm.Ancestor.Display(), sid, v.Storm.AffectedCount)
}

// stormResolved injects the storm's own §9.3 outcome record when its
// LAST member cleared (sig is that member's kind=resolved signal —
// the storm's stability evidence rides on it). Reuses the
// schema-stable ResolvedPayload: reason "storm", the ancestor as the
// object, the storm fingerprint, and a synthetic uid
// ("storm:<ancestor key>") since a blast-radius key has no k8s UID.
func (d *dispatcher) stormResolved(ctx context.Context, sig engine.Signal, info engine.StormInfo) {
	sid := info.SessionID
	if d.mode == "shared" {
		sid = d.targetSid
	}
	d.metrics.stormsResolved.Inc()
	d.metrics.stormsActive.Set(float64(d.storm.ActiveStorms()))
	rec := sig.Recovery
	payload := inject.ResolvedPayload{
		Kind:              inject.KindResolved,
		Reason:            "storm",
		Namespace:         info.Ancestor.Namespace,
		KindOfObject:      info.Ancestor.Kind,
		Name:              info.Ancestor.Name,
		UID:               "storm:" + info.ID,
		Fingerprint:       info.Fingerprint,
		Cluster:           d.cluster,
		FirstSeen:         info.FirstSeen,
		ResolvedAt:        rec.ResolvedAt,
		ClearedAfter:      rec.ResolvedAt.Sub(info.FirstSeen).String(),
		ObservedStableFor: rec.ObservedStableFor.String(),
		Resolution:        string(rec.Resolution),
	}
	if sid == "" {
		log.Printf("storm: %s resolved (all %d members cleared) but no storm session bound — outcome dropped", info.Ancestor.Display(), info.AffectedCount)
		return
	}
	if d.dryRun {
		out, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", sid, string(out))
	} else if err := d.injector.InjectResolved(ctx, sid, payload); err != nil {
		log.Printf("storm: inject storm resolved for %s (sid=%s): %v", info.Ancestor.Display(), sid, err)
		d.metrics.injectErrors.WithLabelValues("storm", "inject").Inc()
		return
	}
	log.Printf("storm resolved %s: all %d member(s) cleared → sid=%s", info.Ancestor.Display(), info.AffectedCount, sid)
}

// stormPayload composes the schema-stable kind=storm wire body from a
// storm snapshot. Pinned byte-exact by TestStormFormed_ExactWireShape.
func stormPayload(info engine.StormInfo, cluster string) inject.StormPayload {
	p := inject.StormPayload{
		Kind:              inject.KindStorm,
		Fingerprint:       info.Fingerprint,
		Severity:          string(info.Severity),
		Cluster:           cluster,
		AncestorKind:      info.Ancestor.Kind,
		AncestorNamespace: info.Ancestor.Namespace,
		AncestorName:      info.Ancestor.Name,
		Reason:            info.Reason,
		Message: fmt.Sprintf("%s: %d incidents across %d namespace(s) share this blast-radius key; %d representative incident(s) attached; member sessions are suppressed and route here",
			info.Ancestor.Display(), info.AffectedCount, info.NamespaceCount, len(info.Representatives)),
		AffectedCount:      info.AffectedCount,
		NamespacesCount:    info.NamespaceCount,
		FirstSeen:          info.FirstSeen,
		LastSeen:           info.LastSeen,
		Representatives:    stormRefs(info.Representatives),
		MemberFingerprints: info.MemberFingerprints,
	}
	if info.Ancestor.Kind == "Node" {
		p.Context.Node = info.Ancestor.Name
	}
	return p
}

// stormMemberPayload composes the kind=storm.member /
// storm.member_superseded wire body for one member.
func stormMemberPayload(kind string, m engine.StormMember, info engine.StormInfo, cluster string) inject.StormMemberPayload {
	var msg string
	if kind == inject.KindStormMemberSuperseded {
		msg = fmt.Sprintf("this incident was folded into the %s storm (%d incidents); further followups and the outcome record route to the storm session",
			info.Ancestor.Display(), info.AffectedCount)
	} else {
		msg = fmt.Sprintf("late-arriving incident attached to the %s storm (now %d incidents)",
			info.Ancestor.Display(), info.AffectedCount)
	}
	return inject.StormMemberPayload{
		Kind:              kind,
		StormFingerprint:  info.Fingerprint,
		StormSessionID:    info.SessionID,
		AncestorKind:      info.Ancestor.Kind,
		AncestorNamespace: info.Ancestor.Namespace,
		AncestorName:      info.Ancestor.Name,
		Cluster:           cluster,
		Message:           msg,
		Incident:          stormRef(m),
	}
}

func stormRefs(members []engine.StormMember) []inject.StormIncidentRef {
	out := make([]inject.StormIncidentRef, 0, len(members))
	for _, m := range members {
		out = append(out, stormRef(m))
	}
	return out
}

func stormRef(m engine.StormMember) inject.StormIncidentRef {
	return inject.StormIncidentRef{
		Fingerprint:  m.Fingerprint,
		Reason:       m.Reason,
		Namespace:    m.Namespace,
		KindOfObject: m.KindOfObject,
		Name:         m.Name,
		UID:          m.Key.UID,
		SessionID:    m.SessionID,
	}
}
