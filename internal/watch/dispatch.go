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
	"errors"
	"fmt"
	"log"
	"net/http"
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
	// pullClass, when non-nil, resolves an image-pull failure's cause
	// class (issue #213) and stamps it on the signal before the filter
	// stage, for the transient debounce and the registry-scoped storm
	// key downstream. It is stateful on purpose: kubelet puts the CAUSE
	// on a `Failed` event that the --reason allow-list rejects by
	// default, and the "still failing" on a causeless `BackOff` event
	// that it accepts, so the class has to carry across the two. That
	// is also why the stamp happens BEFORE filter.Accept — a signal the
	// allow-list drops is still evidence about the object. Nil in unit
	// tests that predate the classifier: the class then stays
	// PullClassNA and every gate below reads as it did before.
	pullClass *engine.PullClassMemo
	// injector is the agent sink (docs/agent-sink-design.md) every
	// inject routes through: the core-agent daemon client by default
	// (--sink=core-agent — the field keeps its historical name), or
	// the generic webhook sink (--sink=webhook). Two verbs only:
	// OpenIncident for new incidents, Append for everything bound.
	injector inject.Sink
	metrics  *metrics
	cluster  string
	// project / region / zone are the resolved §8 deployment identity
	// (see resolveIdentity: explicit flag > provider metadata >
	// empty), stamped onto every signal whose source left them blank.
	// A regional cluster has an empty zone; the failure domain
	// (zone, else region) participates in the fingerprint hash, and
	// empty values reproduce the pre-wiring domain-less fingerprints
	// exactly.
	project   string
	region    string
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
	// resolver, when non-nil, is the topology index the §7.5
	// blast-radius keys come from — the SAME graph feed the storm
	// correlator uses. The dispatcher holds it directly (rather than
	// reaching through d.storm) for the §7.7 ancestor reattachment
	// stage, which is a separate mechanism from correlation: it binds
	// a watchboard warning to an EXISTING per-incident session instead
	// of forming an aggregate one (issue #220).
	//
	// Nil when the graph feed is not running — the feed is built only
	// under --storm (wiring.go), so storm is a de-facto precondition
	// for reattachment. A nil resolver makes the stage inert: every
	// warning digests exactly as it did pre-#220. Correlation is an
	// optimization of session count, never a gate on delivery.
	resolver engine.AncestorResolver
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
	// inflightMu + inflight provide fine-grained per-key in-flight session
	// creation / storm coordination so slow sink HTTP I/O or enrichment for one
	// key or storm never blocks unrelated incidents across the cluster (#642).
	inflightMu sync.Mutex
	inflight   map[inject.IncidentKey]chan struct{}

	// stormReadyMu + stormReady ensure a racing StormAttached waits for its
	// founding StormFormed open to complete without holding any global mutex.
	stormReadyMu sync.Mutex
	stormReady   map[string]chan struct{}
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
	d.stampClusterIdentity(&sig)
	if sig.Kind == engine.KindResolved || sig.Kind == engine.KindResolvedReverted {
		d.dispatchResolved(ctx, sig)
		return
	}

	key, downgraded := d.classifySignal(ctx, &sig)
	d.metrics.eventsSeen.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), d.metrics.boundNamespace(sig.Namespace)).Inc()
	if ok, gate := d.filter.Decide(sig); !ok {
		d.metrics.eventsFiltered.WithLabelValues(gate).Inc()
		return
	}

	result := d.dedup.ObserveSignal(key, engine.Observation{
		LastTS:     sig.LastSeen,
		Kind:       sig.Kind,
		Severity:   sig.Severity,
		Message:    sig.Message,
		Repeatable: d.opensSessions(sig),
		Downgraded: downgraded != nil,
	})
	d.metrics.activeIncidents.Set(float64(d.dedup.ActiveBoundCount()))
	if d.storm != nil && result.Storm != "" && (result.Kind == engine.DedupRepeat || result.Absorbed) {
		d.storm.NoteMemberActivity(key, sig.LastSeen)
	}
	if result.Kind == engine.DedupRepeat {
		d.injectRepeat(ctx, sig, key, result)
		return
	}
	if result.Kind == engine.DedupDuplicate {
		d.handleDuplicateSignal(ctx, sig, key, result, downgraded)
		return
	}

	d.metrics.findings.WithLabelValues(sig.Kind, string(sig.Severity)).Inc()
	d.dedup.NoteIncidentKind(key, sig.Kind)
	if d.triage != nil {
		d.triage.windowRolled(key)
	}
	if d.routeInfoSignal(sig) {
		return
	}
	if d.correlateStormSignal(ctx, sig) {
		return
	}
	if d.mode == "per-incident" && d.board != nil && engine.RouteFor(sig.Severity) == engine.RouteWatchboard {
		d.store.Record(sig, store.Outcome{Route: store.RouteWatchboard})
		d.board.Add(ctx, sig, result.Count)
		return
	}
	if d.mode == "per-incident" && !d.dryRun {
		d.dispatchPerIncidentOpen(ctx, sig, key, result)
		return
	}
	d.dispatchSharedOrDryRun(ctx, sig, key, result)
}

func (d *dispatcher) stampClusterIdentity(sig *engine.Signal) {
	if sig.Cluster == "" {
		sig.Cluster = d.cluster
	}
	if sig.Project == "" {
		sig.Project = d.project
	}
	if sig.Region == "" {
		sig.Region = d.region
	}
	if sig.Zone == "" {
		sig.Zone = d.zone
	}
	if sig.Source == "" {
		sig.Source = engine.SourceSentinel
	}
}

func (d *dispatcher) classifySignal(ctx context.Context, sig *engine.Signal) (engine.EventKey, *memory.TriageStatusRecord) {
	key := sig.CanonicalKey()
	sig.PullClass, sig.PullCause = d.pullClass.Resolve(*sig)
	if sig.Fingerprint == "" {
		sig.Fingerprint = engine.Fingerprint(sig.Kind, key.Reason, sig.KindOfObject,
			engine.FailureDomain(sig.Region, sig.Zone))
	}
	if d.routing != nil {
		sig.Severity = d.routing.Classify(*sig)
	}
	var downgraded *memory.TriageStatusRecord
	if d.triage != nil && d.mode == "per-incident" {
		sig.Severity, downgraded = d.triage.Apply(ctx, *sig)
	}
	return key, downgraded
}

func (d *dispatcher) handleDuplicateSignal(ctx context.Context, sig engine.Signal, key engine.EventKey, result engine.DedupResult, downgraded *memory.TriageStatusRecord) {
	d.metrics.eventsDedupSuppress.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), d.metrics.boundNamespace(sig.Namespace)).Inc()
	log.Printf("dedup %s pod=%s/%s (count=%d, window active)",
		sig.Key.Reason, sig.Namespace, sig.Name, result.Count)
	if downgraded != nil && result.SessionID != "" {
		if baseline, due := d.triage.noteRegression(key, result.Count); due {
			d.injectTriageRegressed(ctx, sig, *downgraded, baseline, result)
		}
	}
	if d.mode == "per-incident" && result.SessionID != "" && result.Storm == "" {
		if openedBy, join := d.dedup.CrossSourceJoin(key, sig.Kind); join {
			d.injectJoinFollowup(ctx, sig, result, key, openedBy)
			return
		}
	}
	if !d.dryRun && result.SessionID == "" && result.Storm == "" && d.opensSessions(sig) {
		d.retryIncidentOpen(ctx, sig, result, key)
		return
	}
	if result.SessionID == "" && result.Storm != "" {
		d.store.Record(sig, store.Outcome{Route: store.RouteStormMember, StormFingerprint: result.Storm})
		return
	}
	d.store.Record(sig, store.Outcome{Route: store.RouteSuppressed, SessionID: result.SessionID})
}

func (d *dispatcher) routeInfoSignal(sig engine.Signal) bool {
	if d.mode != "per-incident" || d.routing == nil || engine.RouteFor(sig.Severity) != engine.RouteStore {
		return false
	}
	d.metrics.infoDropped.WithLabelValues(sig.Kind).Inc()
	if d.store != nil {
		d.store.Record(sig, store.Outcome{Route: store.RouteInfoStored})
		log.Printf("info-store %s %s/%s (severity=info: stored-only class; persisted to --store)",
			sig.Kind, sig.Namespace, sig.Name)
	} else {
		log.Printf("info-drop %s %s/%s (severity=info: stored-only class; set --store to persist — counted and dropped)",
			sig.Kind, sig.Namespace, sig.Name)
	}
	return true
}

func (d *dispatcher) correlateStormSignal(ctx context.Context, sig engine.Signal) bool {
	if d.storm == nil {
		return false
	}
	d.stormReadyMu.Lock()
	v := d.storm.Observe(sig)
	var formDone chan struct{}
	switch v.Kind {
	case engine.StormFormed:
		if d.stormReady == nil {
			d.stormReady = make(map[string]chan struct{})
		}
		formDone = make(chan struct{})
		d.stormReady[v.Storm.ID] = formDone
	case engine.StormAttached:
		formDone = d.stormReady[v.Storm.ID]
	}
	d.stormReadyMu.Unlock()

	switch v.Kind {
	case engine.StormFormed:
		release := d.acquireKey(d.stormKey(v.Storm.Ancestor))
		defer func() {
			close(formDone)
			d.stormReadyMu.Lock()
			delete(d.stormReady, v.Storm.ID)
			d.stormReadyMu.Unlock()
			release()
		}()
		d.stormFormed(ctx, sig, v)
		return true
	case engine.StormAttached:
		if formDone != nil {
			select {
			case <-formDone:
			case <-ctx.Done():
				return true
			}
		}
		release := d.acquireKey(d.stormKey(v.Storm.Ancestor))
		defer release()
		if v.Storm.SessionID == "" {
			if boundSid, ok := d.dedup.LookupSession(sig.Key); ok && boundSid != "" {
				v.Storm.SessionID = boundSid
			}
		}
		d.stormAttached(ctx, sig, v)
		return true
	}
	return false
}

func (d *dispatcher) dispatchPerIncidentOpen(ctx context.Context, sig engine.Signal, key engine.EventKey, result engine.DedupResult) {
	release := d.acquireKey(d.perIncidentCoordKey(sig, key))
	defer release()
	if d.foldSibling(ctx, sig, key, result.Count) {
		if sid, ok := d.dedup.LookupSession(key); ok {
			d.recordBoundSession(key, sid, sig, false)
		}
		return
	}
	if d.enrich != nil && d.enrich.enabledFor(sig.Severity) {
		if bundleStr := d.enrich.Incident(ctx, sig); bundleStr != "" {
			sig.Enrichment = &engine.Enrichment{Bundle: bundleStr}
		}
	}
	payload := incidentPayload(sig, result)
	sid, err, ok := d.openSession(ctx, d.incidentKey(key), payload, sig.Key.Reason)
	if !ok {
		log.Printf("dispatcher: create session for %s/%s: %v", sig.Namespace, sig.Name, err)
		return
	}
	d.recordBoundSession(key, sid, sig, true)
	d.store.Record(sig, store.Outcome{Route: store.RouteInjected, SessionID: sid})
	if err != nil {
		log.Printf("dispatcher: inject for %s/%s (sid=%s): %v", sig.Namespace, sig.Name, sid, err)
		d.metrics.injectErrors.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), "inject").Inc()
		d.dedup.MarkUndelivered(key, engine.RepeatChangeOpen)
		return
	}
	d.metrics.eventsInjected.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), d.metrics.boundNamespace(sig.Namespace)).Inc()
	log.Printf("fire %s pod=%s/%s → sid=%s (mode=%s)",
		sig.Key.Reason, sig.Namespace, sig.Name, sid, d.mode)
}

func (d *dispatcher) dispatchSharedOrDryRun(ctx context.Context, sig engine.Signal, key engine.EventKey, result engine.DedupResult) {
	sid := d.targetSid
	if d.tracker != nil {
		d.tracker.Track(engine.Incident{
			Key:       key,
			SessionID: sid,
			FirstSeen: sig.FirstSeen,
			Ref:       sig.IncidentRef(),
		})
	}
	if d.enrich != nil && d.mode == "per-incident" && d.enrich.enabledFor(sig.Severity) {
		if bundleStr := d.enrich.Incident(ctx, sig); bundleStr != "" {
			sig.Enrichment = &engine.Enrichment{Bundle: bundleStr}
		}
	}
	payload := incidentPayload(sig, result)
	if d.injectMaxBytes > 0 {
		if shed := payload.FitTo(d.injectMaxBytes); len(shed) > 0 {
			d.noteInjectShrunk(sig.Namespace, sig.Name, shed)
		}
	}
	d.store.Record(sig, store.Outcome{Route: store.RouteInjected, SessionID: sid})
	if d.dryRun {
		out, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", sid, string(out))
		d.metrics.eventsInjected.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), d.metrics.boundNamespace(sig.Namespace)).Inc()
		log.Printf("would-fire %s pod=%s/%s (sid=%s, mode=%s, dry-run)",
			sig.Key.Reason, sig.Namespace, sig.Name, sid, d.mode)
		return
	}
	if err := d.injector.Append(ctx, sid, payload); err != nil {
		log.Printf("dispatcher: inject for %s/%s (sid=%s): %v", sig.Namespace, sig.Name, sid, err)
		d.metrics.injectErrors.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), "inject").Inc()
		return
	}
	d.metrics.eventsInjected.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), d.metrics.boundNamespace(sig.Namespace)).Inc()
	log.Printf("fire %s pod=%s/%s → sid=%s (mode=%s)",
		sig.Key.Reason, sig.Namespace, sig.Name, sid, d.mode)
}

// opensSessions reports whether sig's severity class routes to a
// session of its own: per-incident mode, and neither the info class
// (stored only, §7.7) nor — when a watchboard runs — the warning class.
// It is the guard the #84 unbound-retry uses, and the dedup cache's
// Repeatable input (issue #599): only such a signal, on an incident a
// session-class signal opened, can repeat into a bound session. A
// downgraded (§9.4) incident therefore keeps lapsing to the store or
// the watchboard exactly as before, and a watchboard warning that is
// escalated to critical still opens its own session.
func (d *dispatcher) opensSessions(sig engine.Signal) bool {
	return d.mode == "per-incident" &&
		(d.routing == nil || engine.RouteFor(sig.Severity) != engine.RouteStore) &&
		(d.board == nil || engine.RouteFor(sig.Severity) != engine.RouteWatchboard)
}

// injectRepeat delivers a DedupRepeat (issue #599) into the incident's
// bound session: the incident's own payload, re-kinded as a followup —
// k8s-event becomes the frozen k8s-event-followup, a source-namespaced
// kind keeps its kind — with repeat_count stamped (N > 0 for the N-th
// pure reminder, omitted for a material change). No session open: the
// session already exists. Recorded as route=followup against the bound
// session.
//
// A re-send of an open whose initial delivery failed
// (engine.RepeatChangeOpen) carries the full open payload instead —
// kind, shape and enrichment of the open — because that agent has not
// seen the incident at all.
//
// Delivery failures never go dark:
//   - the sink says the session is gone (404/410 — an ax task
//     recreated, a daemon restarted without its state): the entry is
//     unbound and the incident reopened NOW, through the #84 open path,
//     with this signal. Now rather than at the next occurrence because
//     the occurrence in hand is current evidence of a verified-open
//     incident, and a slow emitter's next occurrence may be an hour
//     away. If the reopen fails too, the unbound entry retries on every
//     occurrence through the #84 guard. A storm-claimed entry is
//     unbound but not reopened per-incident (#104): it stays a
//     session-less storm member and its next lapse goes back through
//     the storm stage, as a re-fire did before #599. The storm stage
//     does not reopen a storm whose session is gone (it retries only
//     an empty storm session id), so such a storm keeps its dead id —
//     the pre-#599 behavior, tracked separately.
//   - any other error: the entry is marked undelivered, so the next
//     occurrence re-sends the same inject instead of the schedule
//     skipping it.
func (d *dispatcher) injectRepeat(ctx context.Context, sig engine.Signal, key engine.EventKey, result engine.DedupResult) {
	var payload inject.Payload
	what := "reminder"
	switch {
	case result.Change == engine.RepeatChangeOpen:
		what = "open"
		if d.enrich != nil && d.enrich.enabledFor(sig.Severity) {
			if bundleStr := d.enrich.Incident(ctx, sig); bundleStr != "" {
				sig.Enrichment = &engine.Enrichment{Bundle: bundleStr}
			}
		}
		payload = incidentPayload(sig, result)
		if d.injectMaxBytes > 0 {
			if shed := payload.FitTo(d.injectMaxBytes); len(shed) > 0 {
				d.noteInjectShrunk(sig.Namespace, sig.Name, shed)
			}
		}
	case result.RepeatCount == 0:
		what = "change"
		payload = repeatPayload(sig, result)
	default:
		payload = repeatPayload(sig, result)
	}
	if d.dryRun {
		d.store.Record(sig, store.Outcome{Route: store.RouteFollowup, SessionID: result.SessionID})
		d.metrics.repeatFollowups.WithLabelValues(what).Inc()
		out, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", result.SessionID, string(out))
		return
	}
	if err := d.injector.Append(ctx, result.SessionID, payload); err != nil {
		d.metrics.injectErrors.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), "inject").Inc()
		if sessionGone(err) {
			log.Printf("dispatcher: repeat followup for %s/%s: session %s is gone (%v) — unbinding and reopening",
				sig.Namespace, sig.Name, result.SessionID, err)
			d.dedup.Unbind(key)
			if result.Storm == "" && d.opensSessions(sig) {
				d.retryIncidentOpen(ctx, sig, result, key)
				return
			}
			if result.Storm != "" {
				if info, ok := d.clearDeadStormSession(result.Storm, result.SessionID); ok && d.opensSessions(sig) {
					release := d.acquireKey(d.stormKey(info.Ancestor))
					newSid := d.retryStormOpen(ctx, sig, info)
					release()
					if newSid != "" {
						d.store.Record(sig, store.Outcome{Route: store.RouteStormMember, SessionID: newSid, StormFingerprint: result.Storm})
						return
					}
				}
			}
			d.store.Record(sig, store.Outcome{Route: store.RouteStormMember, StormFingerprint: result.Storm})
			return
		}
		log.Printf("dispatcher: repeat followup for %s/%s (sid=%s): %v (re-sent at the next occurrence)",
			sig.Namespace, sig.Name, result.SessionID, err)
		d.dedup.MarkUndelivered(key, result.Change)
		d.store.Record(sig, store.Outcome{Route: store.RouteFollowup, SessionID: result.SessionID})
		return
	}
	d.store.Record(sig, store.Outcome{Route: store.RouteFollowup, SessionID: result.SessionID})
	d.metrics.repeatFollowups.WithLabelValues(what).Inc()
	switch what {
	case "reminder":
		log.Printf("repeat %s pod=%s/%s → sid=%s (still open: reminder %d, count=%d)",
			sig.Key.Reason, sig.Namespace, sig.Name, result.SessionID, result.RepeatCount, result.Count)
	case "open":
		log.Printf("repeat %s pod=%s/%s → sid=%s (open re-sent: its initial delivery had failed, count=%d)",
			sig.Key.Reason, sig.Namespace, sig.Name, result.SessionID, result.Count)
	default:
		log.Printf("repeat %s pod=%s/%s → sid=%s (still open, changed: %s, count=%d)",
			sig.Key.Reason, sig.Namespace, sig.Name, result.SessionID, result.Change, result.Count)
	}
}

// sessionGone reports a sink error that means the bound session no
// longer exists on the agent side (HTTP 404 or 410 on the inject) —
// the same answer the ax sink's keyed open already treats as "start a
// new one".
func sessionGone(err error) bool {
	var se *inject.StatusError
	return errors.As(err, &se) && (se.Code == http.StatusNotFound || se.Code == http.StatusGone)
}

// repeatPayload composes a repeat followup's wire body: incidentPayload
// with the kind switched to the followup kind and repeat_count set.
// The kind switches BEFORE composition so stampIdentity and
// ReasonClass apply the frozen-pair rules to k8s-event-followup exactly
// as they do to k8s-event.
func repeatPayload(sig engine.Signal, result engine.DedupResult) inject.Payload {
	if sig.Kind == engine.KindK8sEvent {
		sig.Kind = engine.KindK8sEventFollowup
	}
	sig.Enrichment = nil
	payload := incidentPayload(sig, result)
	payload.RepeatCount = result.RepeatCount
	return payload
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
		Kind:   sig.Kind,
		Reason: sig.Key.Reason,
		// The class the pipeline keyed this signal on (issue #574),
		// beside the untouched wire Reason. Computed from the RAW
		// message, before masking, exactly as CanonicalKey does — the
		// value is a fixed reason-family name, never message text.
		ReasonClass:  engine.ReasonClass(sig.Kind, sig.Key.Reason, sig.Message),
		Namespace:    sig.Namespace,
		KindOfObject: sig.KindOfObject,
		Name:         sig.Name,
		Container:    sig.Container,
		UID:          sig.Key.UID,
		Message:      maskString(sig.Message), // §6.5 inject surface (issue #82)
		// Masked on the same terms as Message: this IS message text,
		// borrowed from an earlier event for the same object (#387),
		// so it reaches the same inject surface and must not be a way
		// around §6.5. Empty for everything that is not an image-pull
		// failure whose own message went silent on the cause.
		PullCause: maskString(sig.PullCause),
		Count:     result.Count,
		FirstSeen: sig.FirstSeen,
		LastSeen:  sig.LastSeen,
		Cluster:   sig.Cluster,
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
// acquireKey serializes operations on a single logical session/storm key
// while letting distinct incidents and storms execute enrichment and sink
// HTTP I/O concurrently (#642).
func (d *dispatcher) acquireKey(coordKey inject.IncidentKey) (release func()) {
	for {
		d.inflightMu.Lock()
		if d.inflight == nil {
			d.inflight = make(map[inject.IncidentKey]chan struct{})
		}
		waitCh, busy := d.inflight[coordKey]
		if !busy {
			ch := make(chan struct{})
			d.inflight[coordKey] = ch
			d.inflightMu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					d.inflightMu.Lock()
					delete(d.inflight, coordKey)
					close(ch)
					d.inflightMu.Unlock()
				})
			}
		}
		d.inflightMu.Unlock()
		<-waitCh
	}
}

// perIncidentCoordKey returns the serialization key for per-incident session
// creation: sibling pods under the same controller share a coordination key
// so concurrent pod crashes during a rollout fold cleanly into one session
// rather than racing past foldSibling, while unrelated workloads run in parallel.
func (d *dispatcher) perIncidentCoordKey(sig engine.Signal, key engine.EventKey) inject.IncidentKey {
	if sig.ControllerRef != "" {
		return inject.IncidentKey{
			Cluster: d.cluster,
			Kind:    inject.IncidentKeyIncident,
			ID:      "sibling/" + sig.Namespace + "/" + sig.ControllerRef + "/" + key.Reason,
		}
	}
	return d.incidentKey(key)
}

func (d *dispatcher) retryIncidentOpen(ctx context.Context, sig engine.Signal, result engine.DedupResult, key engine.EventKey) {
	release := d.acquireKey(d.perIncidentCoordKey(sig, key))
	defer release()
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
	sid, err, ok := d.openSession(ctx, d.incidentKey(key), payload, sig.Key.Reason)
	if !ok {
		log.Printf("dispatcher: retry create session for %s/%s: %v (unbound entry, count=%d)",
			sig.Namespace, sig.Name, err, result.Count)
		d.store.Record(sig, store.Outcome{Route: store.RouteSuppressed})
		return
	}
	// The session's actual opener is THIS signal's source family —
	// re-stamp so cross-source join followups reference reality.
	d.dedup.NoteIncidentKind(key, sig.Kind)
	d.recordBoundSession(key, sid, sig, true)
	d.store.Record(sig, store.Outcome{Route: store.RouteInjected, SessionID: sid})
	if err != nil {
		log.Printf("dispatcher: inject for %s/%s (sid=%s): %v", sig.Namespace, sig.Name, sid, err)
		d.metrics.injectErrors.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), "inject").Inc()
		// The session exists but never got the incident: the next
		// occurrence re-sends the full open, never a pure reminder (#599).
		d.dedup.MarkUndelivered(key, engine.RepeatChangeOpen)
		return
	}
	d.metrics.eventsInjected.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), d.metrics.boundNamespace(sig.Namespace)).Inc()
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
		ReasonClass:      engine.ReasonClass(sig.Kind, sig.Key.Reason, sig.Message),
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
	commitResolved := func(sessionID string) {
		if d.triage != nil && sig.Kind == engine.KindResolved {
			d.triage.resolve(ctx, sig)
		}
		if sig.Kind == engine.KindResolved {
			d.dedup.MarkResolved(sig.Key)
			d.metrics.activeIncidents.Set(float64(d.dedup.ActiveBoundCount()))
		}
		d.store.Record(sig, store.Outcome{Route: store.RouteResolved, SessionID: sessionID})
		if d.storm != nil {
			switch sig.Kind {
			case engine.KindResolved:
				if info, done, ok := d.storm.MemberResolved(sig.Key); ok && done {
					d.stormResolved(ctx, sig, info)
				}
			case engine.KindResolvedReverted:
				d.storm.MemberReverted(sig.Key)
			}
		}
	}

	sid := d.targetSid
	if d.mode == "per-incident" {
		bound, ok := d.dedup.LookupSession(sig.Key)
		if !ok {
			d.metrics.recoveryDrops.WithLabelValues("unknown_session").Inc()
			log.Printf("recovery: no bound session for %s %s/%s (uid=%s) — dropping %s (restart without --dedup-persist, or entry evicted)",
				sig.Key.Reason, sig.Namespace, sig.Name, sig.Key.UID, sig.Kind)
			commitResolved("")
			return
		}
		sid = bound
	}
	if d.deferSiblingResolved(ctx, sig, sid) {
		return
	}
	payload := resolvedPayload(sig)
	if d.dryRun {
		out, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", sid, string(out))
		commitResolved(sid)
		d.countResolved(sig)
		return
	}
	if err := d.injector.Append(ctx, sid, payload); err != nil {
		log.Printf("recovery: inject %s for %s/%s (sid=%s): %v", sig.Kind, sig.Namespace, sig.Name, sid, err)
		d.metrics.injectErrors.WithLabelValues(d.metrics.boundReason(sig.Key.Reason), "inject").Inc()
		if sessionGone(err) {
			d.dedup.Unbind(sig.Key)
			commitResolved("")
			return
		}
		if d.tracker != nil && sig.Kind == engine.KindResolved {
			d.tracker.RequeueResolved(sig.Key)
		}
		return
	}
	commitResolved(sid)
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

// resolveIdentity resolves the §8 project/region/zone deployment
// identity: explicit --project/--region/--zone flags win; blanks are
// filled best-effort from a compiled-in provider's metadata
// (cloud.Identity — the gke provider resolves config pins, well-known
// env vars, then the GCE metadata server); whatever remains stays
// empty. Never fatal: a vanilla (untagged) build resolves the
// NoProvider sentinel — which implements no Identity — instantly, and
// a provider error only means empty fields, i.e. the domain-less
// fingerprints deployments hashed before this wiring, byte-identical.
func resolveIdentity(ctx context.Context, f *flags) (project, region, zone string) {
	project, region, zone = f.project, f.region, f.zone
	if project != "" && region != "" {
		return project, region, zone
	}
	p, err := cloud.New(ctx, cloud.Config{Project: f.project, Cluster: f.clusterName})
	if err != nil {
		log.Printf("identity: cloud provider unavailable for project/region/zone detection: %v (stamping flag values only)", err)
		return project, region, zone
	}
	return identityFromProvider(p, project, region, zone)
}

// identityFromProvider applies the documented precedence — explicit
// flag > provider metadata > empty — against an already-constructed
// provider. Split from resolveIdentity so the precedence table is
// unit-testable without the global provider registry.
//
// Region and zone are resolved as a PAIR, not field by field: the
// provider reports a location and a region, and which of the two the
// cluster's zone is follows from whether they differ (cloud.ZoneOf).
// Filling one from metadata while the other came from a flag would let
// a --region=us-east1 override sit next to a metadata zone in
// us-central1, so a flag on either wins for both.
func identityFromProvider(p cloud.Provider, flagProject, flagRegion, flagZone string) (project, region, zone string) {
	project, region, zone = flagProject, flagRegion, flagZone
	id, ok := p.(cloud.Identity)
	if !ok {
		return project, region, zone
	}
	if project == "" {
		project = id.Project()
	}
	if region == "" && zone == "" {
		region, zone = id.Region(), cloud.ZoneOf(id.Location(), id.Region())
	}
	return project, region, zone
}

// stampIdentity completes the §8 schema on a payload
// (docs/signal-schema-v1.md, the M5 v1 freeze): fingerprint + source +
// severity + project/region/zone ride the wire so a fleet-level
// consumer can roll up a fleet-wide symptom as a join on
// (fingerprint, cluster/project/region/zone) instead of parsing
// payloads.
//
// The frozen kinds — k8s-event and k8s-event-followup — used to be
// excluded so their payloads stayed byte-identical to M0. As of the
// 2026-09-10 amendment (issue #389) they carry the IDENTITY half:
// `cluster` alone cannot identify a cluster in a fleet process, since
// a GKE cluster name is unique only within a (project, location) pair,
// so a consumer could neither tell two clusters named `prod` apart nor
// build a context to reach either. Every field is omitempty, so the
// single-cluster deployment that stamps no identity still emits the
// M0 bytes exactly; only a fleet deployment sees the new keys.
//
// Source/severity/fingerprint stay excluded from the frozen pair: they
// are OUR pipeline's verdicts about a signal rather than facts about
// where it happened, and the M0 contract keeps them in-process.
func stampIdentity(p *inject.Payload, sig engine.Signal) {
	p.Project = sig.Project
	p.Region = sig.Region
	p.Zone = sig.Zone
	if p.Kind == engine.KindK8sEvent || p.Kind == engine.KindK8sEventFollowup {
		return
	}
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
