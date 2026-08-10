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
	"sync"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// Dispatch policy (§7.1): DispatchSignal's filter → dedup →
// correlate → route → inject pipeline, the dispatcher it runs on,
// and the per-incident payload composition (§8 wire kinds).

// dispatcher is the pipeline that ties filter → dedup → sink +
// metrics for one signal. Sources (pkg/sources) feed it through
// DispatchSignal; storm correlation, severity routing, and
// enrichment slot in here as they land (§7.1).
type dispatcher struct {
	filter *engine.Filter
	dedup  *engine.DedupCache
	// injector is the agent sink (docs/agent-sink-design.md) every
	// inject routes through: the core-agent daemon client by default
	// (--sink=core-agent — the field keeps its historical name), or
	// the generic webhook sink (--sink=webhook). Two verbs only:
	// OpenIncident for new incidents, Append for everything bound.
	injector inject.Sink
	metrics  *metrics
	cluster  string
	// project / zone are the resolved §8 deployment identity (see
	// resolveIdentity: explicit flag > provider metadata > empty),
	// stamped onto every signal whose source left them blank. Zone
	// participates in the fingerprint hash; empty values reproduce
	// the pre-wiring zone-less fingerprints exactly.
	project   string
	zone      string
	mode      string // "per-incident" or "shared"
	targetSid string // for shared mode
	dryRun    bool
	// injectMaxBytes is the per-inject wire-body ceiling
	// (--inject-max-bytes, default the daemon's inject.MaxInjectBytes):
	// a new incident's payload is fit under it before the open so an
	// oversized enrichment bundle degrades gracefully instead of the
	// daemon 400ing the whole inject and leaving an empty session
	// (issue #198). Zero in tests that predate the guard disables it —
	// fitInject treats <= 0 as "no ceiling".
	injectMaxBytes int
	// tracker, when non-nil, is the §7.4 recovery tracker: every new
	// incident this dispatcher binds is handed to it for clearance
	// watching, and the resolved signals it emits come back through
	// DispatchSignal. Nil when recovery is disabled
	// (--recovery-stable-for=0 or missing pods RBAC). Dry-run keeps
	// the tracker — outcome records print to stdout like every other
	// dry-run payload.
	tracker *engine.RecoveryTracker
	// routing, when non-nil, is the §7.7 severity-routing policy:
	// per-kind severity defaults come stamped by the sources; config
	// overrides them via --severity. Nil in unit tests that predate
	// severity routing — a nil policy skips the routing stage
	// entirely, preserving the pre-§7.7 pipeline byte-for-byte.
	routing *engine.RoutingPolicy
	// board, when non-nil, is the §7.7 shared watchboard the warning
	// class batches into. Only ever set in per-incident mode: in
	// --mode=shared ALL severities keep routing to --target-session
	// (the frozen shared-mode contract) and the watchboard machinery
	// is disabled.
	board *watchboard
	// triage, when non-nil, is the §9.4 severity-routing consumer:
	// after Classify, open triage-status records may override a
	// signal's class (agent downgrade honored, escalated pins
	// critical). Consulted in per-incident mode only — shared mode
	// routes ALL severities to --target-session (frozen contract).
	// The resolve flip (§9.4 automatic lifecycle) runs in every
	// mode. Nil when --store is unset.
	triage *triageOverrides
	// storm, when non-nil, is the §7.5 correlation stage sitting
	// between dedup and session creation: new incidents pass through
	// it and may be folded into a kind=storm session instead of
	// opening their own. Nil when storm correlation is disabled
	// (--storm absent — the default).
	storm *engine.StormCorrelator
	// store, when non-nil, is the §9.1 raw-occurrence store: every
	// signal that survives the filter is recorded post-dedup with the
	// routing outcome it received. Nil when --store is unset — all
	// Store methods are nil-safe no-ops, so no call site branches.
	// The store is telemetry, never control flow: nothing in this
	// dispatcher reads it back.
	store *store.Store
	// enrich, when non-nil, is the §7.6 enrichment stage: new
	// per-incident (and storm) sessions get the in-process bundle
	// attached to their initial inject. Nil when disabled
	// (--enrich=off, --mode=shared, dry-run, or unit tests predating
	// §7.6 — a nil enricher keeps every payload byte-identical).
	enrich *enricher
	// injectLock serializes per-(app, sid) session creation +
	// injects so two rapid-fire events for the same key don't
	// both call CreateSession. Coarse-grained; a per-key map of
	// mutexes would let concurrent keys parallelize but this
	// path is nowhere near a bottleneck.
	injectLock sync.Mutex
}

// Dispatch is the TriageEvent-shaped entry point, kept from M0: it
// wraps the event in the kind=k8s-event Signal the k8s-events source
// would emit and forwards to DispatchSignal. The M0 contract tests
// (wire shape, dispatcher logs) pin this path; keep it until every
// caller speaks Signal.
func (d *dispatcher) Dispatch(ctx context.Context, ev engine.TriageEvent) {
	d.DispatchSignal(ctx, engine.Signal{
		Kind:        engine.KindK8sEvent,
		Source:      engine.SourceSentinel,
		Severity:    engine.SeverityCritical,
		TriageEvent: ev,
	})
}

// DispatchSignal is the pipeline entry point for signals emitted by
// sources (§7.1: filter → dedup → inject; the correlation / routing /
// enrichment stages land in later M2 changes).
//
// For Kind=k8s-event the inject payload is the frozen
// pkg/inject.Payload, byte-identical to the M0 watcher's — the
// Signal-only fields (severity, fingerprint, source, zone) are
// carried in-process but not serialized for that kind (§8: existing
// fields keep their exact names; new fields ship with new kinds).
func (d *dispatcher) DispatchSignal(ctx context.Context, sig engine.Signal) {
	// Stamp deployment identity + derived fields sources leave
	// blank: sources don't know which cluster they run in (§7.2),
	// and the fingerprint (§8) needs the canonicalized reason-class
	// + zone, so it is computed here, once, for every source.
	if sig.Cluster == "" {
		sig.Cluster = d.cluster
	}
	if sig.Project == "" {
		sig.Project = d.project
	}
	if sig.Zone == "" {
		sig.Zone = d.zone
	}
	if sig.Source == "" {
		sig.Source = engine.SourceSentinel
	}
	// Resolved / resolved.reverted (§7.4) are outcome records for an
	// EXISTING incident: they bypass filter + dedup (they are not new
	// incidents) and route as followups into the incident's bound
	// session. Their fingerprint is the original incident's — never
	// re-stamped here.
	if sig.Kind == engine.KindResolved || sig.Kind == engine.KindResolvedReverted {
		d.dispatchResolved(ctx, sig)
		return
	}
	// The canonical pipeline key, computed ONCE per signal with the
	// message in hand (engine.CanonicalReasonForEvent): kubelet's
	// generic BackOff/Failed reasons need the message to land in the
	// right family (pull vs crash-loop). Every key-shaped stage below
	// — fingerprint, dedup, cross-source joins, triage regression
	// state, storm member notes, recovery tracking — uses THIS key,
	// so they all agree on the incident's identity. The wire payload
	// keeps sig.Key.Reason (the original event reason) untouched.
	key := sig.CanonicalKey()
	if sig.Fingerprint == "" {
		sig.Fingerprint = engine.Fingerprint(sig.Kind, key.Reason, sig.KindOfObject, sig.Zone)
	}
	// Effective severity (§7.7): the source-stamped per-kind default,
	// unless config overrides the kind via --severity. Stamped before
	// the storm stage so a storm's max-member severity honors the
	// override too. A nil policy (pre-§7.7 unit tests) leaves the
	// source's stamp untouched.
	if d.routing != nil {
		sig.Severity = d.routing.Classify(sig)
	}
	// §9.4: triage-status records refine the class AFTER config —
	// the agent that diagnosed the incident outranks the per-kind
	// default. Downgraded incidents stop re-paging (they route to
	// the watchboard/store); escalated pins critical and thereby
	// bypasses the watchboard. Per-incident mode only, like the
	// routing stages below. A returned record means THIS signal was
	// downgraded — the input to the regression evidence check in the
	// duplicate branch (M4 observation 3).
	var downgraded *memory.TriageStatusRecord
	if d.triage != nil && d.mode == "per-incident" {
		sig.Severity, downgraded = d.triage.Apply(ctx, sig)
	}
	d.metrics.eventsSeen.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), sig.Namespace).Inc()
	if !d.filter.Accept(sig) {
		return
	}
	result := d.dedup.Observe(key, sig.LastSeen)
	d.metrics.activeIncidents.Set(float64(d.dedup.Len()))
	if result.Kind == engine.DedupDuplicate {
		d.metrics.eventsDedupSuppress.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), sig.Namespace).Inc()
		// Info-level log: the operator asked "is the watcher seeing
		// events?" and today the answer was "yes when things break,
		// silent when things work" — this line makes suppressed
		// duplicates visible so the operator can distinguish
		// "watcher missed the event" from "watcher saw it and
		// correctly deduped". Bound is the dedup window (set via
		// --dedup-window, default 5m); result.Count is the running
		// hit count for this key within the current window.
		log.Printf("dedup %s pod=%s/%s (count=%d, window active)",
			sig.Key.Reason, sig.Namespace, sig.Name, result.Count)
		// §9.4 regression evidence (M4 observation 3): a steady
		// symptom stream never exits the dedup window, so a
		// downgraded incident could regress hard without any visible
		// routing change until the loop pauses. When the window count
		// reaches --triage-regress-factor × the count at downgrade
		// time, ONE schema-stable kind=triage.regressed followup goes
		// into the bound session. Evidence only — no re-page, no
		// record rewrite: the agent/human decides
		// (docs/triage-status-write-design.md §out-of-scope).
		if downgraded != nil && result.SessionID != "" {
			if baseline, due := d.triage.noteRegression(key, result.Count); due {
				d.injectTriageRegressed(ctx, sig, *downgraded, baseline, result)
			}
		}
		// Cross-source join visibility (M4 observation 4): when the
		// duplicate comes from a DIFFERENT source family than the
		// signal that opened the incident (leading↔reactive — e.g.
		// capacity's quota_blocked folding into the quota source's
		// forecast session), the join must be audible inside the
		// session, not only store-visible: route it as a schema-stable
		// kind=family.member followup instead of suppressing it.
		// Bounded to one per source family per incident per window by
		// CrossSourceJoin. Per-incident mode only, like every routing
		// stage.
		//
		// STORM-CLAIMED entries are excluded (result.Storm != "": the
		// bound session is the STORM's session, §7.5): a storm exists
		// to collapse member-level chatter into one session, so a
		// joined member of a storm must not fan out family.member
		// injects there. The occurrence is still store-recorded below
		// (route=suppressed, carrying the storm's session), just never
		// injected.
		if d.mode == "per-incident" && result.SessionID != "" && result.Storm == "" {
			if openedBy, join := d.dedup.CrossSourceJoin(key, sig.Kind); join {
				d.injectJoinFollowup(ctx, sig, result, key, openedBy)
				return
			}
		}
		// Unbound entry on a session-class duplicate (issue #84): the
		// residue of an OpenIncident that failed with sid=="" — no
		// BindIncident ever ran. The case-2 retry safety net cannot
		// repair it, because case 3 advances LastSeen on every
		// sub-window event and a steady symptom stream never exits
		// the dedup window (see above). Retry the open on THIS
		// duplicate instead — the #81 storm-path convention (retry on
		// the next event, which has the payload in hand). Guarded to
		// the severity class that opens sessions: info- and
		// watchboard-routed entries are legitimately unbound, not
		// residue. Never fires an Append at sid=="".
		//
		// A STORM-CLAIMED entry (result.Storm != "") is excluded (issue
		// #104): a session-less storm member is not #84 open residue —
		// the storm claimed it, and its own retry-on-attach (#94) owns
		// recovering the storm session. Opening a fresh per-incident
		// (kind=k8s-event) session here would overwrite the storm binding,
		// the §7.5 N-session fan-out. It is suppressed as a storm member
		// below instead; the storm's next fresh attach reopens the class.
		if d.mode == "per-incident" && !d.dryRun && result.SessionID == "" && result.Storm == "" &&
			(d.routing == nil || engine.RouteFor(sig.Severity) != engine.RouteStore) &&
			(d.board == nil || engine.RouteFor(sig.Severity) != engine.RouteWatchboard) {
			d.retryIncidentOpen(ctx, sig, result, key)
			return
		}
		// Storm-claimed but session-less duplicate (issue #104): the
		// storm's formation/attach open failed and has not recovered yet.
		// Its outcome is membership, not a session of its own — record it
		// against the storm (route=storm-member, empty session until the
		// storm reopens) rather than the plain route=suppressed row below,
		// and never open a competing per-incident session.
		if result.SessionID == "" && result.Storm != "" {
			d.store.Record(sig, store.Outcome{Route: store.RouteStormMember, StormFingerprint: result.Storm})
			return
		}
		// §9.1: suppressed duplicates are still emitted signals — the
		// store keeps them (with the session the incident is bound to)
		// so lookback sees the true occurrence rate, not the deduped one.
		d.store.Record(sig, store.Outcome{Route: store.RouteSuppressed, SessionID: result.SessionID})
		return
	}
	// A fresh incident window: stamp which source family opened it
	// (the reference point for cross-source join followups above) and
	// reset any regression baseline from the previous window.
	d.dedup.NoteIncidentKind(key, sig.Kind)
	if d.triage != nil {
		d.triage.windowRolled(key)
	}
	// Severity routing, info class (§7.7): stored only per §9.1 —
	// with --store set the signal is persisted (route=info-stored) and
	// surfaced by read-path queries and digests; without it, counted +
	// logged and dropped, never silently ignored. The info_dropped
	// metric counts the class in BOTH cases (frozen name; it is the
	// "did not inject" count, store or no store). Placed after dedup
	// (so it counts incident records, not raw event volume) and before
	// storm correlation (info neither opens nor joins sessions, storms
	// included). Shared mode skips routing entirely: ALL severities go
	// to --target-session.
	if d.mode == "per-incident" && d.routing != nil && engine.RouteFor(sig.Severity) == engine.RouteStore {
		d.metrics.infoDropped.WithLabelValues(sig.Kind).Inc()
		if d.store != nil {
			d.store.Record(sig, store.Outcome{Route: store.RouteInfoStored})
			log.Printf("info-store %s %s/%s (severity=info: stored-only class; persisted to --store)",
				sig.Kind, sig.Namespace, sig.Name)
		} else {
			log.Printf("info-drop %s %s/%s (severity=info: stored-only class; set --store to persist — counted and dropped)",
				sig.Kind, sig.Namespace, sig.Name)
		}
		return
	}
	// New incident: correlate, then create or reuse a session and
	// inject. The lock serializes storm formation with session
	// creation so two racing incidents cannot both open the storm.
	d.injectLock.Lock()
	defer d.injectLock.Unlock()
	// Storm correlation (§7.5, pipeline position: after dedup,
	// before severity routing): a new incident may form a storm,
	// attach to an open one, or fall through per-incident.
	if d.storm != nil {
		switch v := d.storm.Observe(sig); v.Kind {
		case engine.StormFormed:
			d.stormFormed(ctx, sig, v)
			return
		case engine.StormAttached:
			d.stormAttached(ctx, sig, v)
			return
		}
	}
	// Severity routing, warning class (§7.7): batch into the shared
	// watchboard's rolling digest instead of opening a per-incident
	// session. AFTER the storm stage on purpose: a correlated burst
	// always opens a storm session regardless of member severity —
	// §7.5's whole point is ONE aggregate incident an agent works,
	// which a digest entry is not — so storm formation/attachment
	// bypasses warning routing (the returns above). Only in
	// per-incident mode: shared mode routes ALL severities to
	// --target-session unchanged.
	if d.mode == "per-incident" && d.board != nil && engine.RouteFor(sig.Severity) == engine.RouteWatchboard {
		// Recorded without a session on purpose: the watchboard's
		// session is created lazily at flush time and rotates — the
		// digest inject, not this occurrence row, is the session-bound
		// artifact.
		d.store.Record(sig, store.Outcome{Route: store.RouteWatchboard})
		d.board.Add(ctx, sig, result.Count)
		return
	}
	if d.mode == "per-incident" && !d.dryRun {
		// Enrichment (§7.6): pre-warm the session by attaching the
		// in-process bundle to the INITIAL inject — composed BEFORE
		// the open, because the sink delivers the initial payload
		// with it. Severity-gated by --enrich; hard-capped by
		// --enrich-timeout. Runs under injectLock deliberately —
		// critical incidents are rare, the budget is seconds, and
		// enriching after unlock would let a followup for the same
		// key race ahead of the session's first message. Errors never
		// block the inject: they ride inside the bundle as
		// enrichment_error trailers.
		if d.enrich != nil && d.enrich.enabledFor(sig.Severity) {
			if bundleStr := d.enrich.Incident(ctx, sig); bundleStr != "" {
				sig.Enrichment = &engine.Enrichment{Bundle: bundleStr}
			}
		}
		payload := incidentPayload(sig, result)
		// New incident → the sink's open verb: the core-agent sink
		// POSTs /sessions then the frozen inject envelope (the exact
		// pre-Sink byte sequence); the webhook sink POSTs /incidents
		// with the payload as the body. A non-empty id alongside an
		// error means the container opened but the initial delivery
		// failed — bind anyway (followups and §7.4 outcomes still
		// have a home) and count the inject error, exactly the
		// pre-Sink behavior.
		sid, err, ok := d.openSession(ctx, payload, sig.Key.Reason)
		if !ok {
			log.Printf("dispatcher: create session for %s/%s: %v", sig.Namespace, sig.Name, err)
			return
		}
		// BindIncident = BindSession + the identity the recovery
		// tracker needs to survive a restart (rides on dedup-persist).
		d.dedup.BindIncident(key, sid, sig.IncidentRef())
		if d.storm != nil {
			// Remember the session for a possible later supersede:
			// if this incident becomes a founding storm member, the
			// storm inject points its session at the storm's.
			d.storm.NoteMemberSession(key, sid)
		}
		// Hand the bound incident to the recovery tracker (§7.4) so
		// the fix-verify loop closes into this session.
		if d.tracker != nil {
			d.tracker.Track(engine.Incident{
				Key:       key,
				SessionID: sid,
				FirstSeen: sig.FirstSeen,
				Ref:       sig.IncidentRef(),
			})
		}
		// §9.1: record the routing DECISION (route=injected, with the
		// session it targets), not delivery success — delivery errors
		// stay the sink's telemetry (inject_errors_total). The
		// recorded signal carries no enrichment (the store strips it).
		d.store.Record(sig, store.Outcome{Route: store.RouteInjected, SessionID: sid})
		if err != nil {
			log.Printf("dispatcher: inject for %s/%s (sid=%s): %v", sig.Namespace, sig.Name, sid, err)
			d.metrics.injectErrors.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), "inject").Inc()
			return
		}
		d.metrics.eventsInjected.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), sig.Namespace).Inc()
		// Info-level log: the successful-inject case was silent before
		// #212 — operators had to correlate client-go informer warnings
		// with daemon session-list dumps to infer whether the watcher
		// was firing at all. Making success visible turns "is the
		// sidecar working?" into a grep. sid is traceable in the daemon's
		// own logs / /sessions API so cross-container reconstruction of
		// an incident is a single traceID-style filter.
		log.Printf("fire %s pod=%s/%s → sid=%s (mode=%s)",
			sig.Key.Reason, sig.Namespace, sig.Name, sid, d.mode)
		return
	}
	// Shared mode + dry-run: no incident open — the payload appends to
	// the pre-configured target session (or prints).
	sid := d.targetSid
	// Hand the bound incident to the recovery tracker (§7.4). Shared
	// mode tracks too — the outcome routes to the shared session.
	if d.tracker != nil {
		d.tracker.Track(engine.Incident{
			Key:       key,
			SessionID: sid,
			FirstSeen: sig.FirstSeen,
			Ref:       sig.IncidentRef(),
		})
	}
	// Enrichment stays per-incident-mode-only (§7.6): shared mode has
	// no session creation to warm and keeps its frozen contract.
	if d.enrich != nil && d.mode == "per-incident" && d.enrich.enabledFor(sig.Severity) {
		if bundleStr := d.enrich.Incident(ctx, sig); bundleStr != "" {
			sig.Enrichment = &engine.Enrichment{Bundle: bundleStr}
		}
	}
	payload := incidentPayload(sig, result)
	// Fit under the sink's per-inject ceiling like the open path does
	// (#198). This shared/dry-run branch reaches Append directly (not
	// openSession), so it carries its own guard; fitting before the
	// dry-run print keeps the printed payload the true wire.
	if d.injectMaxBytes > 0 {
		if shed := payload.FitTo(d.injectMaxBytes); len(shed) > 0 {
			d.noteInjectShrunk(sig.Namespace, sig.Name, shed)
		}
	}
	// §9.1: record the routing DECISION — see the per-incident branch.
	d.store.Record(sig, store.Outcome{Route: store.RouteInjected, SessionID: sid})
	if d.dryRun {
		out, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", sid, string(out))
		d.metrics.eventsInjected.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), sig.Namespace).Inc()
		log.Printf("would-fire %s pod=%s/%s (sid=%s, mode=%s, dry-run)",
			sig.Key.Reason, sig.Namespace, sig.Name, sid, d.mode)
		return
	}
	if err := d.injector.Append(ctx, sid, payload); err != nil {
		log.Printf("dispatcher: inject for %s/%s (sid=%s): %v", sig.Namespace, sig.Name, sid, err)
		d.metrics.injectErrors.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), "inject").Inc()
		return
	}
	d.metrics.eventsInjected.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), sig.Namespace).Inc()
	log.Printf("fire %s pod=%s/%s → sid=%s (mode=%s)",
		sig.Key.Reason, sig.Namespace, sig.Name, sid, d.mode)
}

// incidentPayload composes the wire payload for a NEW incident from
// the signal + its dedup result: the frozen k8s-event shape for the
// M0 kinds, the §8 source-namespaced shape (stampIdentity) for the
// rest, with the additive enrichment/forecast/quota-draft attachments
// when present. Split from DispatchSignal so both the per-incident
// open path and the shared/dry-run append path build the identical
// bytes.
func incidentPayload(sig engine.Signal, result engine.DedupResult) inject.Payload {
	payload := inject.Payload{
		Kind:         sig.Kind,
		Reason:       sig.Key.Reason,
		Namespace:    sig.Namespace,
		KindOfObject: sig.KindOfObject,
		Name:         sig.Name,
		Container:    sig.Container,
		UID:          sig.Key.UID,
		Message:      maskString(sig.Message), // §6.5 inject surface (issue #82)
		Count:        result.Count,
		FirstSeen:    sig.FirstSeen,
		LastSeen:     sig.LastSeen,
		Cluster:      sig.Cluster,
		Context: inject.PayloadContext{
			ControllerRef: sig.ControllerRef,
			Node:          sig.Node,
			Labels:        maskLabels(sig.Labels),
		},
		Type: sig.Type,
	}
	stampIdentity(&payload, sig)
	if sig.Enrichment != nil {
		payload.Enrichment = &inject.PayloadEnrichment{Bundle: sig.Enrichment.Bundle}
	}
	// §8 forecast: trend/countdown sources only; omitempty keeps every
	// reactive payload byte-identical (frozen wire pins unchanged).
	if sig.Forecast != nil {
		payload.Forecast = &inject.PayloadForecast{ETA: sig.Forecast.ETA, ConfidenceBasis: sig.Forecast.ConfidenceBasis}
	}
	// §10.3 drafted increase request: quota.forecast only; additive
	// via omitempty like Forecast. The agent files it through
	// core-agent's permission gate — the sentinel only attaches.
	if sig.QuotaDraft != nil {
		payload.QuotaIncreaseDraft = &inject.PayloadQuotaDraft{
			QuotaID:        sig.QuotaDraft.QuotaID,
			Region:         sig.QuotaDraft.Region,
			Unit:           sig.QuotaDraft.Unit,
			CurrentUsage:   sig.QuotaDraft.CurrentUsage,
			CurrentLimit:   sig.QuotaDraft.CurrentLimit,
			SuggestedLimit: sig.QuotaDraft.SuggestedLimit,
			SlopePerDay:    sig.QuotaDraft.SlopePerDay,
			Justification:  sig.QuotaDraft.Justification,
		}
	}
	return payload
}

// fitInject shrinks payload to the --inject-max-bytes wire ceiling
// before it goes on the wire, logging and counting anything it sheds
// (issue #198). It handles both open shapes — incident (inject.Payload)
// and storm (inject.StormPayload) — so the single call in openSession
// covers every enrichment-bearing open path; other, always-small
// payloads pass through untouched. A ceiling of <= 0 (tests predating
// the guard) is a no-op that returns payload unchanged.
func (d *dispatcher) fitInject(payload any) any {
	if d.injectMaxBytes <= 0 {
		return payload
	}
	switch p := payload.(type) {
	case inject.Payload:
		if shed := p.FitTo(d.injectMaxBytes); len(shed) > 0 {
			d.noteInjectShrunk(p.Namespace, p.Name, shed)
		}
		return p
	case inject.StormPayload:
		if shed := p.FitTo(d.injectMaxBytes); len(shed) > 0 {
			d.noteInjectShrunk(p.AncestorNamespace, p.AncestorName, shed)
		}
		return p
	default:
		return payload
	}
}

// noteInjectShrunk logs and counts a payload the fit guard shrank to
// clear the sink's per-inject ceiling (issue #198). Loud on purpose: a
// shrunk incident lost context (its enrichment bundle, or the tail of
// its message), and a steady stream of them means --enrich-cap is set
// too high for the sink's inject limit.
func (d *dispatcher) noteInjectShrunk(ns, name string, shed []string) {
	log.Printf("dispatcher: inject payload for %s/%s exceeded --inject-max-bytes=%d; shed %s to fit",
		ns, name, d.injectMaxBytes, strings.Join(shed, ", "))
	for _, what := range shed {
		d.metrics.injectShrinks.WithLabelValues(what).Inc()
	}
}

// retryIncidentOpen re-attempts the per-incident open for a duplicate
// whose dedup entry is unbound (issue #84). Semantics mirror the
// fresh-incident open path exactly — enrichment, standard
// incidentPayload composition (result.Count carries the occurrences
// seen so far), bind-on-partial-open, storm member note, tracker
// handoff, §9.1 record, metrics — because this IS that open, deferred
// to the first event after the daemon recovered. While the daemon
// stays down, each duplicate costs one POST /sessions
// (sessionCreates{error}, like the original failure) and the key
// stays suppressed; no Append is ever attempted without a session.
func (d *dispatcher) retryIncidentOpen(ctx context.Context, sig engine.Signal, result engine.DedupResult, key engine.EventKey) {
	d.injectLock.Lock()
	defer d.injectLock.Unlock()
	// Re-check under the lock: a racing duplicate (or a storm claim)
	// may have just bound the key.
	if sid, ok := d.dedup.LookupSession(key); ok && sid != "" {
		d.store.Record(sig, store.Outcome{Route: store.RouteSuppressed, SessionID: sid})
		return
	}
	if d.enrich != nil && d.enrich.enabledFor(sig.Severity) {
		if bundleStr := d.enrich.Incident(ctx, sig); bundleStr != "" {
			sig.Enrichment = &engine.Enrichment{Bundle: bundleStr}
		}
	}
	payload := incidentPayload(sig, result)
	sid, err, ok := d.openSession(ctx, payload, sig.Key.Reason)
	if !ok {
		log.Printf("dispatcher: retry create session for %s/%s: %v (unbound entry, count=%d)",
			sig.Namespace, sig.Name, err, result.Count)
		d.store.Record(sig, store.Outcome{Route: store.RouteSuppressed})
		return
	}
	d.dedup.BindIncident(key, sid, sig.IncidentRef())
	// The session's actual opener is THIS signal's source family —
	// re-stamp so cross-source join followups reference reality.
	d.dedup.NoteIncidentKind(key, sig.Kind)
	if d.storm != nil {
		d.storm.NoteMemberSession(key, sid)
	}
	if d.tracker != nil {
		d.tracker.Track(engine.Incident{
			Key:       key,
			SessionID: sid,
			FirstSeen: sig.FirstSeen,
			Ref:       sig.IncidentRef(),
		})
	}
	d.store.Record(sig, store.Outcome{Route: store.RouteInjected, SessionID: sid})
	if err != nil {
		log.Printf("dispatcher: inject for %s/%s (sid=%s): %v", sig.Namespace, sig.Name, sid, err)
		d.metrics.injectErrors.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), "inject").Inc()
		return
	}
	d.metrics.eventsInjected.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), sig.Namespace).Inc()
	log.Printf("fire %s pod=%s/%s → sid=%s (mode=%s, open deferred past a failed create)",
		sig.Key.Reason, sig.Namespace, sig.Name, sid, d.mode)
}

// injectTriageRegressed emits the §9.4 regression-evidence followup
// (M4 observation 3) into the downgraded incident's bound session:
// the symptom's dedup-window count reached the regression factor over
// its rate at downgrade time. SCHEMA-STABLE payload, evidence only —
// the routing decision stays with the open record (and therefore with
// the agent who wrote it).
func (d *dispatcher) injectTriageRegressed(ctx context.Context, sig engine.Signal, rec memory.TriageStatusRecord, baseline int, result engine.DedupResult) {
	payload := inject.TriageRegressedPayload{
		Kind:             inject.KindTriageRegressed,
		Reason:           sig.Key.Reason,
		Namespace:        sig.Namespace,
		KindOfObject:     sig.KindOfObject,
		Name:             sig.Name,
		Container:        sig.Container,
		UID:              sig.Key.UID,
		Fingerprint:      sig.Fingerprint,
		Cluster:          sig.Cluster,
		TriageStatus:     string(rec.Status),
		SeverityOverride: rec.SeverityOverride,
		TriageSession:    rec.Session,
		BaselineCount:    baseline,
		Count:            result.Count,
		Factor:           d.triage.regressFactor,
		FirstSeen:        sig.FirstSeen,
		LastSeen:         sig.LastSeen,
		Message: fmt.Sprintf(
			"downgraded incident regressed: %d occurrences this dedup window vs %d when the %s override was written (%dx or more) — override still routing; re-triage via `lookout triage status` or escalate",
			result.Count, baseline, rec.SeverityOverride, d.triage.regressFactor),
		Context: inject.PayloadContext{
			ControllerRef: sig.ControllerRef,
			Node:          sig.Node,
			Labels:        maskLabels(sig.Labels), // §6.5 inject surface (issue #82)
		},
	}
	d.metrics.triageRegressed.Inc()
	if d.dryRun {
		out, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", result.SessionID, string(out))
		return
	}
	if err := d.injector.Append(ctx, result.SessionID, payload); err != nil {
		log.Printf("triage-status: regression followup for %s/%s (sid=%s): %v", sig.Namespace, sig.Name, result.SessionID, err)
		d.metrics.injectErrors.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), "inject").Inc()
		return
	}
	log.Printf("triage-status: regressed %s %s/%s count=%d baseline=%d factor=%d → sid=%s (evidence only — no re-page)",
		sig.Kind, sig.Namespace, sig.Name, result.Count, baseline, d.triage.regressFactor, result.SessionID)
}

// injectJoinFollowup makes a cross-source dedup join visible inside
// the bound session (M4 observation 4): one schema-stable
// kind=family.member payload (§10.3 correlation) carrying the joining
// signal's identity, the canonical family both signals collapsed
// into, and the design_ref for the join contract — recorded as
// route=followup instead of route=suppressed. key is the canonical
// pipeline key (key.Reason is the family the join landed on);
// openedBy the source family whose signal opened the incident.
func (d *dispatcher) injectJoinFollowup(ctx context.Context, sig engine.Signal, result engine.DedupResult, key engine.EventKey, openedBy string) {
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
		Family:       key.Reason,
		OpenedBy:     openedBy,
		Cluster:      sig.Cluster,
		SessionID:    result.SessionID,
		Message: fmt.Sprintf(
			"cross-source join: %s (%s) attached to this session's %s incident, opened by the %s source — a second observation angle on the same incident; at most one family.member per source family per window",
			sig.Kind, sig.Key.Reason, key.Reason, openedBy),
		DesignRef: inject.FamilyMemberDesignRef,
	}
	// §9.1: the occurrence row records the JOIN routing decision with
	// the session it targeted, so lookback distinguishes "suppressed"
	// from "announced into the session".
	d.store.Record(sig, store.Outcome{Route: store.RouteFollowup, SessionID: result.SessionID})
	d.metrics.crossSourceFollowups.WithLabelValues(engine.SourceFamily(sig.Kind)).Inc()
	if d.dryRun {
		out, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", result.SessionID, string(out))
		return
	}
	if err := d.injector.Append(ctx, result.SessionID, payload); err != nil {
		log.Printf("dispatcher: cross-source followup for %s/%s (sid=%s): %v", sig.Namespace, sig.Name, result.SessionID, err)
		d.metrics.injectErrors.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), "inject").Inc()
		return
	}
	log.Printf("family.member %s %s/%s → sid=%s (cross-source join: %s joined a %s-opened incident)",
		sig.Key.Reason, sig.Namespace, sig.Name, result.SessionID,
		engine.SourceFamily(sig.Kind), openedBy)
}

// dispatchResolved routes a §7.4 outcome record into the incident's
// bound session as a followup. The dedup cache's binding — not
// tracker-local state — is authoritative: if the binding is unknown
// (sentinel restarted without --dedup-persist, or the entry was LRU-
// evicted), the record is logged, counted, and dropped; we never
// open a fresh session just to say something is fixed.
func (d *dispatcher) dispatchResolved(ctx context.Context, sig engine.Signal) {
	if sig.Recovery == nil {
		log.Printf("recovery: %s signal for %s/%s missing Recovery attachment — dropping (programming error)",
			sig.Kind, sig.Namespace, sig.Name)
		return
	}
	// §9.4 automatic lifecycle: the symptom cleared, so the
	// incident's triage-status record flips to resolved and joins
	// the §9.3 corpus (write-through — routing stops honoring it
	// immediately). resolved.reverted deliberately does NOT restore
	// it: a fix that failed to stick should page at its own class
	// until the agent re-triages.
	if d.triage != nil && sig.Kind == engine.KindResolved {
		d.triage.resolve(ctx, sig)
	}
	// Storm bookkeeping first (§7.5): member clearance feeds the
	// storm's recovery — the LAST member to clear resolves the storm.
	// Recorded before the member's own routing so a lost binding
	// (dropped outcome below) still keeps storm accounting correct.
	var stormFinal *engine.StormInfo
	if d.storm != nil {
		switch sig.Kind {
		case engine.KindResolved:
			if info, done, ok := d.storm.MemberResolved(sig.Key); ok && done {
				stormFinal = &info
			}
		case engine.KindResolvedReverted:
			d.storm.MemberReverted(sig.Key)
		}
		defer func() {
			if stormFinal != nil {
				d.stormResolved(ctx, sig, *stormFinal)
			}
		}()
	}
	sid := d.targetSid
	if d.mode == "per-incident" {
		bound, ok := d.dedup.LookupSession(sig.Key)
		if !ok {
			d.metrics.recoveryDrops.WithLabelValues("unknown_session").Inc()
			log.Printf("recovery: no bound session for %s %s/%s (uid=%s) — dropping %s (restart without --dedup-persist, or entry evicted)",
				sig.Key.Reason, sig.Namespace, sig.Name, sig.Key.UID, sig.Kind)
			// The OUTCOME still happened even though the inject has
			// nowhere to go — keep it (NULL session) so §7.4 stability
			// windows and recommendation history stay complete.
			d.store.Record(sig, store.Outcome{Route: store.RouteResolved})
			return
		}
		sid = bound
	}
	// §9.1: resolved / resolved.reverted rows are what the stability-
	// window and recommendation-history queries join against.
	d.store.Record(sig, store.Outcome{Route: store.RouteResolved, SessionID: sid})
	payload := resolvedPayload(sig)
	if d.dryRun {
		out, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", sid, string(out))
		d.countResolved(sig)
		return
	}
	if err := d.injector.Append(ctx, sid, payload); err != nil {
		log.Printf("recovery: inject %s for %s/%s (sid=%s): %v", sig.Kind, sig.Namespace, sig.Name, sid, err)
		d.metrics.injectErrors.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), "inject").Inc()
		return
	}
	d.countResolved(sig)
	log.Printf("%s %s pod=%s/%s → sid=%s (resolution=%s, cleared_after=%s, stable_for=%s)",
		sig.Kind, sig.Key.Reason, sig.Namespace, sig.Name, sid,
		sig.Recovery.Resolution, sig.Recovery.ClearedAfter, sig.Recovery.ObservedStableFor)
}

func (d *dispatcher) countResolved(sig engine.Signal) {
	if sig.Kind == engine.KindResolvedReverted {
		d.metrics.recoveriesReverted.Inc()
		return
	}
	d.metrics.recoveriesObserved.WithLabelValues(string(sig.Recovery.Resolution)).Inc()
}

// resolveIdentity resolves the §8 zone/project deployment identity:
// explicit --project/--zone flags win; blanks are filled best-effort
// from a compiled-in provider's metadata (cloud.Identity — the gke
// provider resolves config pins, well-known env vars, then the GCE
// metadata server); whatever remains stays empty. Never fatal: a
// vanilla (untagged) build resolves the NoProvider sentinel — which
// implements no Identity — instantly, and a provider error only
// means empty fields, i.e. the zone-less fingerprints deployments
// hashed before this wiring, byte-identical.
func resolveIdentity(ctx context.Context, f *flags) (project, zone string) {
	project, zone = f.project, f.zone
	if project != "" && zone != "" {
		return project, zone
	}
	p, err := cloud.New(ctx, cloud.Config{Project: f.project, Cluster: f.clusterName})
	if err != nil {
		log.Printf("identity: cloud provider unavailable for zone/project detection: %v (stamping flag values only)", err)
		return project, zone
	}
	return identityFromProvider(p, project, zone)
}

// identityFromProvider applies the documented precedence — explicit
// flag > provider metadata > empty — against an already-constructed
// provider. Split from resolveIdentity so the precedence table is
// unit-testable without the global provider registry.
func identityFromProvider(p cloud.Provider, flagProject, flagZone string) (project, zone string) {
	project, zone = flagProject, flagZone
	id, ok := p.(cloud.Identity)
	if !ok {
		return project, zone
	}
	if project == "" {
		project = id.Project()
	}
	if zone == "" {
		zone = id.Location()
	}
	return project, zone
}

// stampIdentity completes the §8 schema on a source-namespaced
// payload (docs/signal-schema-v1.md, the M5 v1 freeze): fingerprint +
// source + severity + zone/project ride the wire so a fleet-level
// consumer can roll up a
// fleet-wide symptom as a join on (fingerprint, cluster/project/zone)
// instead of parsing payloads. The frozen kinds — k8s-event and
// k8s-event-followup — are deliberately excluded: their payloads stay
// byte-identical to M0 (the frozen wire pins), and their Signal-only
// fields remain in-process per the M0 contract.
func stampIdentity(p *inject.Payload, sig engine.Signal) {
	if p.Kind == engine.KindK8sEvent || p.Kind == engine.KindK8sEventFollowup {
		return
	}
	p.Project = sig.Project
	p.Zone = sig.Zone
	p.Source = sig.Source
	p.Severity = string(sig.Severity)
	p.Fingerprint = sig.Fingerprint
}

// resolvedPayload composes the §9.3 schema-stable outcome record from
// a resolved Signal. The frozen k8s-event payload is untouched — this
// is its own struct, serialized in the same inject envelope, pinned
// byte-exact by TestDispatchResolved_ExactWireShape.
func resolvedPayload(sig engine.Signal) inject.ResolvedPayload {
	rec := sig.Recovery
	p := inject.ResolvedPayload{
		Kind:              sig.Kind,
		Reason:            sig.Key.Reason,
		Namespace:         sig.Namespace,
		KindOfObject:      sig.KindOfObject,
		Name:              sig.Name,
		Container:         sig.Container,
		UID:               sig.Key.UID,
		Fingerprint:       sig.Fingerprint,
		Cluster:           sig.Cluster,
		FirstSeen:         sig.FirstSeen,
		ResolvedAt:        rec.ResolvedAt,
		ClearedAfter:      rec.ClearedAfter.String(),
		ObservedStableFor: rec.ObservedStableFor.String(),
		Resolution:        string(rec.Resolution),
		Context: inject.PayloadContext{
			ControllerRef: sig.ControllerRef,
			Node:          sig.Node,
			Labels:        maskLabels(sig.Labels), // §6.5 inject surface (issue #82)
		},
	}
	if sig.Kind == engine.KindResolvedReverted {
		p.RevertedAfter = rec.RevertedAfter.String()
	}
	return p
}
