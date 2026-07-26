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

package memory

// Triage-status records (DESIGN.md §9.4): the compact record an
// incident agent writes at each material transition (diagnosed,
// action taken, escalated), keyed by the incident's fingerprint +
// resource_key, so every later reader — a health scan, the
// sentinel's severity routing, a bundle — reports the TRIAGED
// reality instead of re-deriving the symptom from raw telemetry.
//
// Storage and interface binding are the same decision as facts (see
// this package's comment): §9.4 routes these through the shared
// Memory interface, which core-agent v2.7.0 does not ship, so the
// sentinel store implements the TriageWriter/TriageReader interfaces
// below in-tree. Deliberately NO distributed locking anywhere in
// this design (§9.4 records the rejection): per cluster the sentinel
// is the single writer of record lifecycle, and dedup's
// followup-to-bound-session flow IS the claim flow.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// TriageStatus is the §9.4 status enum. The four agent-written
// values are the doc's; StatusResolved is the lifecycle terminal the
// SENTINEL writes when the symptom clears (§7.4 recovery injects
// close the loop — no manual TTL bookkeeping), moving the record
// into the §9.3 corpus.
type TriageStatus string

const (
	StatusInvestigating TriageStatus = "investigating"
	StatusTriaged       TriageStatus = "triaged"
	StatusActioned      TriageStatus = "actioned"
	StatusEscalated     TriageStatus = "escalated"
	StatusResolved      TriageStatus = "resolved"
)

// Valid reports whether s is a defined status.
func (s TriageStatus) Valid() bool {
	switch s {
	case StatusInvestigating, StatusTriaged, StatusActioned, StatusEscalated, StatusResolved:
		return true
	}
	return false
}

// Open reports whether the record still describes a live incident:
// everything but resolved. Consumers (severity routing, the
// health/bundle join) only honor OPEN records — a resolved record is
// §9.3 corpus material, not current truth.
func (s TriageStatus) Open() bool { return s != StatusResolved && s != "" }

// TriageStatusRecord is the §9.4 record, field-for-field the schema
// in the design doc. The JSON encoding is SCHEMA-STABLE (the doc's
// example is the wire shape); the golden test pins it. Identity is
// (Fingerprint, ResourceKey): a rewrite for the same incident
// replaces the record — the record is current state, not a journal
// (the eventlog/§9.3 corpus is the journal).
type TriageStatusRecord struct {
	// Fingerprint is the §8 incident-class fingerprint from the
	// inject payload the agent triaged.
	Fingerprint string `json:"fingerprint"`
	// ResourceKey pins the record to the specific resource (the
	// fingerprint alone is class-level and spans objects — §8). Use
	// ResourceKey() to compose it; see that function for the format.
	ResourceKey string `json:"resource_key"`
	// Session is the incident session that produced the record.
	Session string `json:"session,omitempty"`
	// Status is the current triage state.
	Status TriageStatus `json:"status"`
	// RootCauseHypothesis is the agent's diagnosis one-liner.
	RootCauseHypothesis string `json:"root_cause_hypothesis,omitempty"`
	// SeverityOverride is the agent's routing judgment ("warning":
	// traffic on backup, not page-worthy). Severity routing honors
	// it while the record is open; empty means no override. Values
	// are the §7.7 classes (critical|warning|info).
	SeverityOverride string `json:"severity_override,omitempty"`
	// Action is the paper trail ("PR #402 opened; escalated to
	// #platform-db").
	Action string `json:"action,omitempty"`
	// Updated is when the record last changed. Assigned by the
	// store on write.
	Updated time.Time `json:"updated"`
}

// Validate rejects records that cannot serve their consumers.
func (r TriageStatusRecord) Validate() error {
	if r.Fingerprint == "" {
		return errors.New("memory: triage-status record has no fingerprint")
	}
	if r.ResourceKey == "" {
		return errors.New("memory: triage-status record has no resource_key")
	}
	if !r.Status.Valid() {
		return fmt.Errorf("memory: triage-status record has invalid status %q (want investigating|triaged|actioned|escalated|resolved)", r.Status)
	}
	if r.SeverityOverride != "" {
		switch r.SeverityOverride {
		case "critical", "warning", "info":
		default:
			return fmt.Errorf("memory: severity_override %q is not a §7.7 class (critical|warning|info)", r.SeverityOverride)
		}
	}
	return nil
}

// ResourceKey composes the canonical resource key both sides of the
// §9.4 join can derive: writers (incident playbooks) from the inject
// payload's kind_of_object/namespace/name, readers (severity
// routing, health/bundle) from the Signal or Finding they hold. The
// format is
//
//	<KindOfObject>/<namespace>/<name>
//
// with the namespace segment empty for cluster-scoped objects
// ("Node//gke-node-1"). The §9.4 example shows a group/version
// prefix ("apps/v1/Deployment/prod/payment-service"); lookout's
// payloads carry kind_of_object without group/version, so the
// canonical key drops the prefix — MatchesResource tolerates
// records written WITH one by also comparing the trailing
// kind/namespace/name segments.
func ResourceKey(kindOfObject, namespace, name string) string {
	return kindOfObject + "/" + namespace + "/" + name
}

// MatchesResource reports whether the record's ResourceKey refers to
// the resource identified by key (a ResourceKey() composition).
// Exact match, plus the documented tolerance for group/version-
// prefixed keys: "apps/v1/Deployment/prod/x" matches
// "Deployment/prod/x".
func (r TriageStatusRecord) MatchesResource(key string) bool {
	if r.ResourceKey == key {
		return true
	}
	return strings.HasSuffix(r.ResourceKey, "/"+key)
}

// TriageQuery filters TriageStatuses. Zero-valued fields do not
// filter.
type TriageQuery struct {
	// Fingerprint matches exactly.
	Fingerprint string
	// OpenOnly keeps records whose Status.Open() — what routing and
	// the scan join consume.
	OpenOnly bool
	// UpdatedSince keeps records updated at or after this instant.
	UpdatedSince time.Time
	// Limit caps the result set (most recently updated first);
	// <= 0 means no limit.
	Limit int
}

// TriageWriter is the write half of the §9.4 surface. Upsert
// semantics on (Fingerprint, ResourceKey): the record is current
// state.
type TriageWriter interface {
	UpsertTriageStatus(ctx context.Context, rec TriageStatusRecord) (TriageStatusRecord, error)
	// ResolveTriageStatus is the §9.4 automatic lifecycle: flip the
	// open record(s) for (fingerprint, one of resourceKeys) to
	// resolved. Returns how many records flipped. Called by the
	// sentinel when a §7.4 recovery inject fires — the stability
	// window is the decay, no manual TTL bookkeeping.
	ResolveTriageStatus(ctx context.Context, fingerprint string, resourceKeys ...string) (int, error)
}

// TriageReader is the read half: severity routing's cache and the
// health/bundle join.
type TriageReader interface {
	TriageStatuses(ctx context.Context, q TriageQuery) ([]TriageStatusRecord, error)
}

// TriageStore is both halves.
type TriageStore interface {
	TriageWriter
	TriageReader
}
