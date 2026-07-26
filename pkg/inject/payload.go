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

package inject

import "time"

// Payload is the JSON body POSTed to
// /sessions/<sid>/inject.message. Field names and casing mirror the
// design doc's "Inject payload shape" section verbatim so playbook
// skills can pattern-match against them.
type Payload struct {
	Kind         string         `json:"kind"`
	Reason       string         `json:"reason"`
	Namespace    string         `json:"namespace"`
	KindOfObject string         `json:"kind_of_object"`
	Name         string         `json:"name"`
	Container    string         `json:"container,omitempty"`
	UID          string         `json:"uid"`
	Message      string         `json:"message"`
	Count        int            `json:"count"`
	FirstSeen    time.Time      `json:"first_seen"`
	LastSeen     time.Time      `json:"last_seen"`
	Cluster      string         `json:"cluster"`
	Context      PayloadContext `json:"context"`
	// Enrichment is the §7.6 warm-session attachment, present only on
	// the INITIAL inject of a per-incident session when the enrichment
	// stage ran. ADDITIVE to the frozen k8s-event shape: omitempty
	// keeps un-enriched payloads byte-identical to M0 (the frozen wire
	// pins pass unchanged); playbooks that predate §7.6 simply ignore
	// the extra key.
	Enrichment *PayloadEnrichment `json:"enrichment,omitempty"`
	// Forecast is the §8 "forecast" field, set by trend/countdown
	// sources only ("cert expires in 72h", "pod hits memory limit in
	// ~14 min"). ADDITIVE like Enrichment: omitempty keeps every
	// reactive payload — the frozen k8s-event shape included —
	// byte-identical to before.
	Forecast *PayloadForecast `json:"forecast,omitempty"`
	// QuotaIncreaseDraft is the §10.3 drafted increase request,
	// present only on kind=quota.forecast payloads. ADDITIVE like
	// Enrichment/Forecast: omitempty keeps every other payload
	// byte-identical.
	QuotaIncreaseDraft *PayloadQuotaDraft `json:"quota_increase_draft,omitempty"`
}

// PayloadForecast mirrors DESIGN.md §8's forecast object: ETA is the
// projected (or, for countdowns, exact) exhaustion time and
// ConfidenceBasis names the model that produced it — e.g.
// "linear-90m-window" for a regression, "certificate-notAfter" for a
// countdown — so the agent and AX can judge how much to trust it.
type PayloadForecast struct {
	ETA             time.Time `json:"eta"`
	ConfidenceBasis string    `json:"confidence_basis"`
}

// PayloadEnrichment is the §8 "enrichment" envelope field: the
// size-capped, sanitized in-process bundle (§7.6) that pre-warms the
// incident session. Bundle is `lookout bundle`-shaped logfmt — one
// finding per line, each carrying a `section` key
// (spec|delta|edges|radius|logs), terminated by schema-stable trailer
// lines: `overflow section=<s> cmd="lookout …"` for sections the cap
// dropped (the named command reproduces them, §4.4.4) and
// `enrichment_error stage=<s> error="…"` for stages that failed
// (enrichment is best-effort; errors never block the inject).
type PayloadEnrichment struct {
	Bundle string `json:"bundle"`
}

// PayloadQuotaDraft is the wire shape of the §10.3 drafted quota
// increase request riding a kind=quota.forecast inject. SCHEMA-
// STABLE: the agent parses it structurally to file the request
// through core-agent's PERMISSION GATE — lookout only ever drafts;
// no code in this repository calls a QuotaPreference create (or any
// other quota mutation). suggested_limit / justification come from
// the quota source's slope math (pkg/sources/quota.Draft documents
// the formula); quota_id is the provider's canonical increase-
// request identifier when known (GCP: the Cloud Quotas
// "<service>/<quotaId>" pair a QuotaPreference names), else the
// inventory quota name.
type PayloadQuotaDraft struct {
	QuotaID        string  `json:"quota_id"`
	Region         string  `json:"region"`
	Unit           string  `json:"unit,omitempty"`
	CurrentUsage   float64 `json:"current_usage"`
	CurrentLimit   float64 `json:"current_limit"`
	SuggestedLimit float64 `json:"suggested_limit"`
	SlopePerDay    float64 `json:"slope_per_day"`
	Justification  string  `json:"justification"`
}

// PayloadContext is the nested "context" object on Payload.
type PayloadContext struct {
	ControllerRef string            `json:"controller_ref,omitempty"`
	Node          string            `json:"node,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
}

// Kind* are the constants we stamp on every payload's "kind"
// field. Skills match against this to distinguish k8s-triggered
// injects from other signal sources (Cloud Monitoring, PagerDuty,
// etc.) that would use different constants when they ship.
const (
	KindEvent    = "k8s-event"
	KindFollowup = "k8s-event-followup"
)

// KindResolved / KindResolvedReverted are the §7.4 outcome-record
// kinds. Like KindEvent/KindFollowup they are wire contract: playbook
// skills and the §9.3 corpus harvester match these exact strings.
const (
	KindResolved         = "resolved"
	KindResolvedReverted = "resolved.reverted"
)

// Storm wire kinds (DESIGN.md §7.5). Like the resolved kinds these are
// wire contract: playbook skills and AX match the exact strings.
const (
	// KindStorm is the aggregate incident payload injected into the
	// ONE session a correlated burst opens.
	KindStorm = "storm"
	// KindStormMember is a late-arrival member recorded as a followup
	// in the storm's session.
	KindStormMember = "storm.member"
	// KindStormMemberSuperseded is injected into a member's own
	// pre-storm session, pointing it at the storm session that now
	// owns the incident (followups and outcomes route there).
	KindStormMemberSuperseded = "storm.member_superseded"
	// KindStormUpdate is the storm-session size refresh: the initial
	// kind=storm payload's affected_count is frozen at formation time,
	// so when membership grows past a reporting threshold (doubling or
	// +10 members, at most one per minute) the current totals ride
	// this followup instead of mutating the frozen shape.
	KindStormUpdate = "storm.update"
)

// StormIncidentRef is one member incident reference on the storm
// payloads: fingerprint plus object identity. SessionID is set only
// for members that fired per-incident before the storm formed.
type StormIncidentRef struct {
	Fingerprint  string `json:"fingerprint"`
	Reason       string `json:"reason"`
	Namespace    string `json:"namespace,omitempty"`
	KindOfObject string `json:"kind_of_object"`
	Name         string `json:"name"`
	UID          string `json:"uid"`
	SessionID    string `json:"session_id,omitempty"`
}

// StormPayload is the JSON body injected for kind=storm (DESIGN.md
// §7.5): ONE incident for a correlated burst — "Node X NotReady; 30
// pods affected across 6 namespaces; 3 representative incidents
// attached". SCHEMA-STABLE: AX and playbook skills parse it
// structurally; the wire pin test in internal/watch is byte-exact.
//
// The ancestor fields carry the blast-radius key (the §6.4 nearest
// common ancestor: node, owner, shared ConfigMap/PVC, or namespace).
// Fingerprint is the STORM's fingerprint —
// sha256(kind="storm" ⊕ reason-class ⊕ ancestor-kind ⊕ zone) — so the
// same node-failure storm carries the same fingerprint across
// clusters. Reason is the reason-class of the first member (the
// burst's leading symptom). RepresentativeIncidents lists the first
// members (capped); MemberFingerprints records EVERY member in
// arrival order, one entry per member.
type StormPayload struct {
	Kind               string             `json:"kind"`
	Fingerprint        string             `json:"fingerprint"`
	Severity           string             `json:"severity"`
	Cluster            string             `json:"cluster"`
	AncestorKind       string             `json:"ancestor_kind"`
	AncestorNamespace  string             `json:"ancestor_namespace,omitempty"`
	AncestorName       string             `json:"ancestor_name"`
	Reason             string             `json:"reason"`
	Message            string             `json:"message"`
	AffectedCount      int                `json:"affected_count"`
	NamespacesCount    int                `json:"namespaces_count"`
	FirstSeen          time.Time          `json:"first_seen"`
	LastSeen           time.Time          `json:"last_seen"`
	Representatives    []StormIncidentRef `json:"representative_incidents"`
	MemberFingerprints []string           `json:"member_fingerprints"`
	Context            PayloadContext     `json:"context"`
	// Enrichment is the §7.6 attachment for the storm's ancestor
	// object — deliberately radius-only (a storm's first question is
	// "what hangs off this ancestor?", and N member log fetches would
	// multiply the cost §7.5 exists to collapse). Additive + omitempty
	// like Payload.Enrichment; the storm wire pin passes unchanged.
	Enrichment *PayloadEnrichment `json:"enrichment,omitempty"`
}

// StormMemberPayload is the JSON body for kind=storm.member (a late
// arrival recorded in the storm session) and
// kind=storm.member_superseded (the pointer left in a member's own
// pre-storm session). SCHEMA-STABLE. StormSessionID is the session
// that owns the storm — the routing pointer a superseded session's
// agent follows; on kind=storm.member it names the session the
// payload is already in.
type StormMemberPayload struct {
	Kind              string           `json:"kind"`
	StormFingerprint  string           `json:"storm_fingerprint"`
	StormSessionID    string           `json:"storm_session_id,omitempty"`
	AncestorKind      string           `json:"ancestor_kind"`
	AncestorNamespace string           `json:"ancestor_namespace,omitempty"`
	AncestorName      string           `json:"ancestor_name"`
	Cluster           string           `json:"cluster"`
	Message           string           `json:"message"`
	Incident          StormIncidentRef `json:"incident"`
}

// StormUpdatePayload is the JSON body for kind=storm.update: the
// size refresh injected into the STORM's own session when membership
// grows past a reporting threshold (M2 drill observation 4 — the
// formation payload undersold the final blast radius: affected_count
// froze at 3 while reality grew to 33). SCHEMA-STABLE: pinned
// byte-exact by TestStormUpdate_ExactWireShape. The initial
// kind=storm payload stays byte-identical; AX and playbooks read the
// storm's current size from the LAST storm.update in the session (or
// the formation payload when none fired).
type StormUpdatePayload struct {
	Kind              string `json:"kind"`
	StormFingerprint  string `json:"storm_fingerprint"`
	AncestorKind      string `json:"ancestor_kind"`
	AncestorNamespace string `json:"ancestor_namespace,omitempty"`
	AncestorName      string `json:"ancestor_name"`
	Cluster           string `json:"cluster"`
	Message           string `json:"message"`
	// AffectedCount / NamespacesCount are the storm's CURRENT totals
	// at emit time (same field names as the formation payload, so
	// consumers fold them with one rule: latest wins).
	AffectedCount   int `json:"affected_count"`
	NamespacesCount int `json:"namespaces_count"`
	// NewMembersSinceLast is the membership growth since the previous
	// size report (formation, or the prior storm.update).
	NewMembersSinceLast int `json:"new_members_since_last"`
}

// Watchboard wire kinds (DESIGN.md §7.7 + §15 Q2). Wire contract like
// the storm kinds: playbook skills and AX match the exact strings.
const (
	// KindWatchboardDigest is the rolling digest of warning-class
	// signals batched into the shared watchboard session.
	KindWatchboardDigest = "watchboard.digest"
	// KindWatchboardRotated is the final inject into a watchboard
	// session when size-based rotation closes it, pointing at the
	// successor session.
	KindWatchboardRotated = "watchboard.rotated"
)

// WatchboardEntry is one warning-class signal on a watchboard digest:
// its kind + fingerprint, the object reference, and the dedup
// counters. Compact by design — the digest is a board, not an
// incident record; the agent runs `lookout` reads if an entry needs
// investigation.
type WatchboardEntry struct {
	Kind         string    `json:"kind"`
	Fingerprint  string    `json:"fingerprint"`
	Reason       string    `json:"reason"`
	Namespace    string    `json:"namespace,omitempty"`
	KindOfObject string    `json:"kind_of_object"`
	Name         string    `json:"name"`
	UID          string    `json:"uid"`
	Count        int       `json:"count"`
	FirstSeen    time.Time `json:"first_seen"`
	LastSeen     time.Time `json:"last_seen"`
}

// WatchboardDigestPayload is the JSON body injected for
// kind=watchboard.digest (§7.7: warning severity routes to the shared
// watchboard session as a batched rolling digest). SCHEMA-STABLE:
// pinned byte-exact by TestWatchboardDigest_ExactWireShape.
//
// BoardGeneration + Sequence are the session's in-band identity
// marker: POST /sessions has no name parameter, so a watchboard
// session is recognized by its content — every inject into it is
// kind=watchboard.* — and by the (generation, sequence) lineage
// coordinates on each digest. Generation counts watchboard sessions
// this sentinel has opened (1-based; rotation increments it);
// Sequence counts digests within the current session (1-based).
type WatchboardDigestPayload struct {
	Kind            string            `json:"kind"`
	Cluster         string            `json:"cluster"`
	BoardGeneration int               `json:"board_generation"`
	Sequence        int               `json:"sequence"`
	WindowStart     time.Time         `json:"window_start"`
	WindowEnd       time.Time         `json:"window_end"`
	Entries         []WatchboardEntry `json:"entries"`
}

// WatchboardRotatedPayload is the JSON body of the FINAL inject into
// a watchboard session when size-based rotation (§15 Q2) closes it:
// the successor pointer that keeps the lineage walkable from either
// end. SCHEMA-STABLE: pinned byte-exact by
// TestWatchboardRotated_ExactWireShape. Existing dedup bindings into
// the closed session stay valid — only NEW warnings flow to the
// successor.
type WatchboardRotatedPayload struct {
	Kind               string    `json:"kind"`
	Cluster            string    `json:"cluster"`
	BoardGeneration    int       `json:"board_generation"`
	SuccessorSessionID string    `json:"successor_session_id"`
	InjectsCount       int       `json:"injects_count"`
	RotatedAt          time.Time `json:"rotated_at"`
}

// ResolvedPayload is the JSON body injected for kind=resolved and
// kind=resolved.reverted (DESIGN.md §7.4) — the ground-truth outcome
// record of an incident session. SCHEMA-STABLE per §9.3: outcome
// records are structured injects, never prose, so the corpus
// harvester can extract labeled trajectories from the eventlog
// without NLP. The frozen k8s-event Payload is untouched — new kinds
// get their own payload struct, serialized in the same
// /sessions/<sid>/inject envelope.
//
// Identity fields (reason … cluster, context) repeat the ORIGINAL
// incident's identity; in particular Fingerprint is the original
// incident's fingerprint, so AX and `lookout health` join outcome to
// incident on one key. Reason carries the canonical reason-class
// (the dedup/binding key — e.g. "ImagePullBackOff" even if the wire
// event said "ErrImagePull"), because the outcome closes the incident
// CLASS the session was opened for.
//
// Durations (cleared_after, observed_stable_for, reverted_after) are
// Go time.Duration strings ("2m30s") — fixed-grammar, parseable
// without NLP, and human-readable in a session transcript.
type ResolvedPayload struct {
	Kind         string `json:"kind"`
	Reason       string `json:"reason"`
	Namespace    string `json:"namespace"`
	KindOfObject string `json:"kind_of_object"`
	Name         string `json:"name"`
	Container    string `json:"container,omitempty"`
	UID          string `json:"uid"`
	Fingerprint  string `json:"fingerprint"`
	Cluster      string `json:"cluster"`
	// FirstSeen is the original incident's first_seen — the anchor
	// cleared_after is measured from.
	FirstSeen  time.Time `json:"first_seen"`
	ResolvedAt time.Time `json:"resolved_at"`
	// ClearedAfter is first_seen → symptom cleared.
	ClearedAfter string `json:"cleared_after"`
	// ObservedStableFor is how long the symptom stayed clear before
	// the sentinel called it resolved (>= --recovery-stable-for).
	ObservedStableFor string `json:"observed_stable_for"`
	// Resolution is "recovered" or "object_deleted" — the agent MUST
	// be able to distinguish fixed from deleted, so this is its own
	// field, never encoded in prose.
	Resolution string `json:"resolution"`
	// RevertedAfter is set only on kind=resolved.reverted: how long
	// after resolved_at the symptom recurred.
	RevertedAfter string         `json:"reverted_after,omitempty"`
	Context       PayloadContext `json:"context"`
}
