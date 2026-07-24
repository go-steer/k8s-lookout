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
