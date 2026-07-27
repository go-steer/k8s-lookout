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

// Package memory holds lookout's durable, agent-queryable memory
// records (DESIGN.md §9.2/§9.4): the low-volume distilled facts a
// scheduled distiller pass derives from recurring raw occurrences,
// and (next change in this stack) the triage-status records incident
// agents write at material transitions. Both are RECORD TYPES with
// schema-stable JSON — never prose — so every consumer (severity
// routing, health/bundle merge, fleet rollup, the §9.3 corpus
// harvester) joins on fields, not NLP.
//
// # Why lookout owns this interface (the core-agent binding decision)
//
// DESIGN.md §9.2 specifies these records travel "through the
// core-agent Memory interface (docs/shared-memory-design.md)". That
// interface DOES NOT EXIST in core-agent v2.7.0, the version this
// module pins: there is no `package memory`, no
// Remember/Recall/Forget surface, and no daemon write endpoint —
// the attach listener's GET /sessions/{sid}/memory lists the memory
// *files* loaded into a session's context (a read-only
// MemoryProvider projection), which is a different feature. The
// shared-memory design is, as of v2.7.0, a design document only.
//
// Rather than invent a fake core-agent API, this package defines
// lookout's OWN minimal, record-typed interface ("FactWriter" /
// "FactReader" below) and the in-tree implementation binds it to the
// sentinel-local store (pkg/store adds the tables — consistent with
// the §9.4 tiering decision: the shared-memory design settles on
// FTS5-over-SQLite in-tree, so "we add a record type, not a
// database"). The types here deliberately shadow the shapes the
// shared-memory design freezes (scoped, tagged, timestamped items)
// so the eventual adapter is a field mapping, not a redesign.
//
// TODO(core-agent): when core-agent ships `package memory` (the
// Memory interface: Remember/Recall/Forget over Item/Query, per
// docs/shared-memory-design.md) together with a surface an EXTERNAL
// process can reach — either daemon HTTP endpoints for memory
// writes/search on the attach listener, or a supported
// library-over-shared-eventlog mode — add an adapter here that maps
// DistilledFact / TriageStatusRecord to Kind=semantic / Kind=episodic
// Items (scope keys and record fields into Topics/Body JSON) and
// retire the sentinel-store tables as the system of record. Until
// then the sentinel store IS the memory store, and this package is
// the seam that keeps that swap local.
package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Well-known Scope keys. Scope maps are free-form (a fact names the
// dimensions its pattern actually varies over), but producers use
// these spellings for the §9.2 dimensions so consumers can filter
// without a synonym table.
const (
	ScopeProject   = "project"
	ScopeCluster   = "cluster"
	ScopeZone      = "zone"
	ScopeNodeGroup = "nodegroup" // GKE's machine-type-homogeneous unit
	ScopeNamespace = "namespace"
	ScopeWorkload  = "workload"
	ScopeReason    = "reason_class" // canonical reason (engine.CanonicalReason)
	ScopeIssuer    = "issuer"
)

// DistilledFact is one §9.2 durable fact: a recurring pattern in the
// raw-occurrence telemetry, compressed to the statement an agent can
// act on ("us-east1-b nodegroup n2d-pool: 3 stockouts in 168h").
// Facts are what upgrade alerts into recommendations (§10.3).
//
// Identity is (Class, Scope): re-distilling the same pattern UPDATES
// the existing fact's evidence window and counts instead of
// duplicating it — the store stays low-volume by construction.
//
// The JSON encoding is SCHEMA-STABLE: field names and shapes are a
// wire contract consumed outside this repo (agents query these
// records; a fleet-level consumer may harvest them). Additive
// evolution only; the golden
// test pins the bytes.
type DistilledFact struct {
	// Class names the pattern that produced the fact, dot-namespaced
	// by subject (e.g. "capacity.stockout.recurrence"). Consumers
	// branch on it; values are append-only.
	Class string `json:"class"`
	// Scope is the dimension set the fact is about — project, zone,
	// nodegroup, workload, issuer, … (see the Scope* keys). Keys and
	// values are free-form strings; empty values are omitted.
	Scope map[string]string `json:"scope"`
	// Statement is the human/agent-readable fact, composed
	// deterministically from the evidence (golden-testable, no
	// relative words like "yesterday").
	Statement string `json:"statement"`
	// WindowStart/WindowEnd bound the evidence window the counts
	// were measured over.
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	// Occurrences is how many raw occurrences matched inside the
	// window; DistinctObjects how many distinct objects (UIDs) they
	// spanned (0 when the pattern's object dimension is its scope).
	Occurrences     int `json:"occurrences"`
	DistinctObjects int `json:"distinct_objects,omitempty"`
	// FirstSeen/LastSeen are the matched occurrences' emission
	// bounds — narrower than the window; the recurrence span.
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
	// SourceFingerprints are the §8 fingerprints of the incident
	// classes the evidence came from — the join key back to raw
	// occurrences, sessions, and fleet rollups. Sorted, deduplicated,
	// capped at MaxSourceFingerprints.
	SourceFingerprints []string `json:"source_fingerprints,omitempty"`
	// Created is when the fact first materialized; Updated when the
	// distiller last refreshed it. Implementation-assigned.
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
}

// MaxSourceFingerprints caps the fingerprint list a fact accumulates
// across updates. Oldest-lexicographic beyond the cap are dropped —
// the list is a join aid, not an exhaustive ledger.
const MaxSourceFingerprints = 16

// ScopeKey returns the canonical identity encoding of the fact's
// scope: keys sorted, empty values dropped, JSON-encoded. Two facts
// with equal (Class, ScopeKey) are the same fact. The encoding is
// deterministic so it can back a unique index.
func ScopeKey(scope map[string]string) string {
	keys := make([]string, 0, len(scope))
	for k, v := range scope {
		if v != "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		vb, _ := json.Marshal(scope[k])
		b.Write(kb)
		b.WriteByte(':')
		b.Write(vb)
	}
	b.WriteByte('}')
	return b.String()
}

// Validate rejects facts that would be unusable as records: no
// class, no scope dimension, or no statement.
func (f DistilledFact) Validate() error {
	if f.Class == "" {
		return errors.New("memory: fact has no class")
	}
	if ScopeKey(f.Scope) == "{}" {
		return fmt.Errorf("memory: fact %q has no scope dimensions", f.Class)
	}
	if f.Statement == "" {
		return fmt.Errorf("memory: fact %q has no statement", f.Class)
	}
	return nil
}

// FactQuery filters Facts. Zero-valued fields do not filter; all set
// fields are AND-combined.
type FactQuery struct {
	// Class matches exactly.
	Class string
	// Scope entries must ALL be present (with equal values) in a
	// fact's scope for it to match.
	Scope map[string]string
	// UpdatedSince keeps facts refreshed at or after this instant.
	UpdatedSince time.Time
	// Limit caps the result set (most recently updated first);
	// <= 0 means no limit.
	Limit int
}

// FactWriter is the write half of the §9.2 memory surface: the
// distiller (and only the distiller, today) records facts through
// it. Upsert semantics: a fact with the same (Class, ScopeKey) as an
// existing record updates that record's statement, window, counts,
// LastSeen, Updated, and fingerprint union — it never duplicates.
// The returned fact carries the stored Created/Updated stamps.
type FactWriter interface {
	UpsertFact(ctx context.Context, fact DistilledFact) (DistilledFact, error)
}

// FactReader is the read half: agent-facing queries and the
// recommendation composition (§10.3) come through it.
type FactReader interface {
	Facts(ctx context.Context, q FactQuery) ([]DistilledFact, error)
}

// FactStore is both halves — what a memory backend implements. The
// in-tree backend is *store.Store (the sentinel store; see the
// package comment for why, and the TODO for what replaces it).
type FactStore interface {
	FactWriter
	FactReader
}
