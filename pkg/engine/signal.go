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

package engine

import "time"

// Signal kind constants for the shipped kinds (DESIGN.md §7.3).
//
// KindK8sEvent and KindK8sEventFollowup are FROZEN: playbook skills
// pattern-match these exact strings in inject payloads (AGENTS.md
// back-compat rule). They mirror pkg/inject's KindEvent/KindFollowup —
// the wire-contract owner — and a contract test in internal/watch pins
// the two packages to the same values. New kinds are namespaced by the
// source that emits them (`rollout.stall`, `saturation.forecast`, …)
// and land with their sources.
const (
	KindK8sEvent         = "k8s-event"
	KindK8sEventFollowup = "k8s-event-followup"
)

// Cross-cutting kinds (§7.3): resolved / resolved.reverted are the
// §7.4 recovery-inject outcome records. They are not namespaced by a
// source — any source that observed a symptom can observe its
// absence, and the outcome schema is shared. Mirrored by pkg/inject's
// wire constants; the internal/watch contract test pins the pairs to
// the same values.
const (
	KindResolved         = "resolved"
	KindResolvedReverted = "resolved.reverted"
)

// Storm kinds (§7.3/§7.5): cross-cutting like resolved — any mix of
// sources can supply a storm's members, so the kinds are not
// source-namespaced. Mirrored by pkg/inject's wire constants; the
// internal/watch contract test pins the pairs to the same values.
const (
	// KindStorm is the one aggregate incident a correlated burst
	// opens (§7.5); also the "kind" input of the storm fingerprint.
	KindStorm = "storm"
	// KindStormMember marks a late-arrival member attached to an
	// open storm's session as a followup.
	KindStormMember = "storm.member"
	// KindStormMemberSuperseded is injected into a member's OWN
	// per-incident session (opened before the storm formed) to point
	// it at the storm session that now owns the incident.
	KindStormMemberSuperseded = "storm.member_superseded"
	// KindStormUpdate is the size refresh injected into the storm's
	// session when membership grows past a reporting threshold: the
	// formation payload's affected_count is frozen at formation time
	// (schema stability), so freshness rides on this NEW kind.
	KindStormUpdate = "storm.update"
)

// KindTriageRegressed is the §9.4 regression-evidence followup (M4
// drill observation 3): a downgraded incident whose symptom rate
// escalated past the regression factor while the dedup window stayed
// open. Cross-cutting like resolved — any source's signals can carry
// the rate. Mirrored by pkg/inject's wire constant; the internal/watch
// contract test pins the pair to the same value.
const KindTriageRegressed = "triage.regressed"

// Values for Signal.Source (the §8 "source" field): which path
// produced the signal. Not to be confused with a signal *source*
// implementation (pkg/sources.Source), whose Name() namespaces new
// signal kinds instead.
const (
	// SourceSentinel marks signals pushed by the resident watch-path
	// (`lookout watch`).
	SourceSentinel = "sentinel"
	// SourceScan marks signals produced by a point-in-time read-path
	// scan (`lookout health`, `triage delta`, …). Scan findings dedupe
	// against sentinel pushes on the same fingerprint (§8).
	SourceScan = "scan"
)

// Severity is the §7.7 signal severity class. Classification lives on
// every Signal from this PR on; the per-class routing policy
// (per-incident session / shared watchboard / store-only) is the
// severity-routing stage and lands in a later M2 change.
type Severity string

const (
	// SeverityCritical routes (per §7.7 defaults) to a per-incident
	// session with full enrichment — today's watcher behavior.
	SeverityCritical Severity = "critical"
	// SeverityWarning routes to the shared watchboard session, batched.
	SeverityWarning Severity = "warning"
	// SeverityInfo is stored only (§9.1), surfaced by read-path
	// queries and digests.
	SeverityInfo Severity = "info"
)

// Valid reports whether s is one of the three defined severity classes.
func (s Severity) Valid() bool {
	switch s {
	case SeverityCritical, SeverityWarning, SeverityInfo:
		return true
	}
	return false
}

// Forecast is the optional trend-source projection (§8 "forecast"):
// when the signal is a leading indicator ("pod hits memory limit in
// ~14 min"), ETA is the projected exhaustion time and ConfidenceBasis
// names the model that produced it (e.g. "linear-90m-window") so the
// agent — and AX — can judge how much to trust it. Nil for reactive
// signals.
type Forecast struct {
	ETA             time.Time
	ConfidenceBasis string
}

// Enrichment is the §7.6 warm-session attachment: the pre-computed
// incident bundle the sentinel gathers in-process before injecting so
// the session starts warm. Populated by the enrichment stage
// (internal/watch/enrich.go) on the initial inject of per-incident
// sessions; nil when enrichment is off for the signal's severity.
type Enrichment struct {
	// Bundle is the size-capped, sanitized `lookout bundle` output
	// for the affected object.
	Bundle string
}

// Signal is the unit the watch-path pipeline carries: the §8 schema
// as an internal type, and the generalization of TriageEvent to many
// sources (§7.3). Every stage — filter, dedup, storm correlation,
// severity routing, enrichment, inject — operates on Signals.
//
// The embedded TriageEvent contributes the M0-frozen core every
// signal shares: the object reference (Key.UID, Namespace,
// KindOfObject, Name), Key.Reason/Message, Count/FirstSeen/LastSeen,
// and the context fields (ControllerRef, Node, Labels). Signal itself
// carries no k8s.io/api types (same rule as TriageEvent) so unit
// tests construct it bare.
//
// Wire shape: for Kind=k8s-event / k8s-event-followup the inject
// payload remains the frozen pkg/inject.Payload byte-for-byte — the
// Signal-only fields (severity, fingerprint, source, zone, forecast)
// are deliberately NOT serialized for those kinds. The one additive
// exception is Enrichment (§7.6): when the enrichment stage ran, the
// payload carries the extra "enrichment" key via omitempty — absent,
// the bytes are still the frozen M0 shape. The full §8 JSON
// serialization ships with the new kinds that need it.
type Signal struct {
	// Kind names the signal type (§7.3): "k8s-event",
	// "k8s-event-followup" (frozen), or a source-namespaced kind
	// like "rollout.stall".
	Kind string
	// Source is SourceSentinel or SourceScan (§8 "source"): pushed
	// by the resident sentinel, or found by a point-in-time scan.
	Source string
	// Severity is the §7.7 class assigned per signal kind.
	Severity Severity
	// Fingerprint is the stable cross-cluster incident-class hash —
	// see Fingerprint() for the frozen definition. Sources may leave
	// it empty; the pipeline stamps it before inject.
	Fingerprint string

	// Deployment identity (§8): where this signal was observed.
	// Stamped by the pipeline from sentinel configuration, not by
	// sources. Zone/Project are empty until cluster-metadata wiring
	// lands (no flag surface change in this PR).
	Cluster string
	Project string
	Zone    string

	// TriageEvent is the frozen per-object core; see type comment.
	TriageEvent

	// Forecast is set by trend sources only (§8); nil otherwise.
	Forecast *Forecast
	// Enrichment is the §7.6 attachment; nil until the enrichment
	// stage lands.
	Enrichment *Enrichment
	// Recovery is the §7.4 outcome attachment, set only on
	// Kind=resolved / Kind=resolved.reverted Signals; nil otherwise.
	Recovery *Recovery
	// QuotaDraft is the §10.3 drafted increase request, set only on
	// Kind=quota.forecast Signals; nil otherwise. lookout drafts,
	// the agent files — through core-agent's permission gate, never
	// from here.
	QuotaDraft *QuotaIncreaseDraft
}
