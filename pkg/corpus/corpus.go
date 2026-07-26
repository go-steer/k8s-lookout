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

// Package corpus is the §9.3 verified-fix corpus harvester: it
// extracts LABELED incident trajectories — symptom → diagnosis →
// action → externally verified outcome — from a captured inject
// stream, by pure schema walks over the frozen signal-schema v1
// payloads (docs/signal-schema-v1.md). No NLP anywhere: every stage
// is recovered from a structured field the wire contract pins
// (kind, fingerprint, status, resolution), which is exactly the §9.3
// requirement this package exists to validate end-to-end.
//
// Input is the drill capture format dev/drills/stub-daemon.py logs
// (one line per daemon request):
//
//	SESSION-CREATE sid=<sid> caller=<...> token=<...>
//	INJECT sid=<sid> kind=<kind> token=<...> body={"message":"<payload JSON>"}
//
// interleaved, optionally, with raw JSON lines holding §9.4
// triage-status records (memory.TriageStatusRecord wire shape — the
// output of exporting the sentinel store's records, e.g. via
// `lookout triage status --format=json` post-processing or the store
// itself). Triage-status records are the diagnosis/action stages; a
// capture without them still yields symptom→outcome trajectories,
// marked incomplete. Unrecognized lines are ignored, so `kubectl
// logs` of a stub-daemon pod feeds in verbatim.
//
// The package is a contract validator and drill tool, not a product
// surface: the thin CLI lives in dev/tools/harvest-corpus.
package corpus

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/memory"
)

// Symptom is the trajectory's opening stage: the initial inject of an
// incident session, verbatim schema fields.
type Symptom struct {
	Kind         string `json:"kind"`
	Reason       string `json:"reason,omitempty"`
	Namespace    string `json:"namespace,omitempty"`
	KindOfObject string `json:"kind_of_object,omitempty"`
	Name         string `json:"name,omitempty"`
	UID          string `json:"uid,omitempty"`
	Severity     string `json:"severity,omitempty"`
	Zone         string `json:"zone,omitempty"`
	FirstSeen    string `json:"first_seen,omitempty"`
	Message      string `json:"message,omitempty"`
}

// Diagnosis aggregates the trajectory's investigation artifacts: the
// §7.6 enrichment bundle riding the opening inject, and the freshest
// diagnosing §9.4 record (status investigating/triaged).
type Diagnosis struct {
	// EnrichmentBundle reports whether the opening inject carried
	// the §7.6 warm-session bundle (the bundle body itself is
	// context, not label material, so only its presence is recorded).
	EnrichmentBundle bool `json:"enrichment_bundle"`
	// TriageStatus / RootCause / Session echo the diagnosing §9.4
	// record when the capture carries one.
	TriageStatus string `json:"triage_status,omitempty"`
	RootCause    string `json:"root_cause,omitempty"`
	Session      string `json:"session,omitempty"`
}

// Action is the trajectory's acted stage: the §9.4 record that moved
// to actioned (or escalated — a handoff is an action too).
type Action struct {
	Status string `json:"status"`
	Action string `json:"action,omitempty"`
}

// Outcome is the externally verified terminal stage: the §7.4
// resolved / resolved.reverted record, verbatim schema fields.
type Outcome struct {
	Kind              string `json:"kind"`
	Resolution        string `json:"resolution"`
	ResolvedAt        string `json:"resolved_at,omitempty"`
	ClearedAfter      string `json:"cleared_after,omitempty"`
	ObservedStableFor string `json:"observed_stable_for,omitempty"`
	RevertedAfter     string `json:"reverted_after,omitempty"`
}

// Trajectory is one harvested incident: the §9.3 stages plus the
// ground-truth label. Label is the outcome's structured resolution
// ("recovered" / "object_deleted"), or "reverted" when the terminal
// record is kind=resolved.reverted — never derived from prose.
type Trajectory struct {
	Session     string     `json:"session"`
	Cluster     string     `json:"cluster,omitempty"`
	Fingerprint string     `json:"fingerprint,omitempty"`
	Symptom     *Symptom   `json:"symptom,omitempty"`
	Diagnosis   *Diagnosis `json:"diagnosis,omitempty"`
	Action      *Action    `json:"action,omitempty"`
	Outcome     *Outcome   `json:"outcome,omitempty"`
	Label       string     `json:"label,omitempty"`
	// Followups counts the additional injects (dedup followups,
	// cross-source joins, storm members, regression evidence) the
	// session accumulated between symptom and outcome.
	Followups int `json:"followups"`
	// Complete reports whether all four §9.3 stages are present:
	// symptom, diagnosis, action, outcome.
	Complete bool `json:"complete"`
}

// session accumulates one sid's injects during parsing.
type session struct {
	sid      string
	payloads []json.RawMessage
}

// Harvest reads a capture stream and returns the trajectories of
// every session that opened with an incident payload, in first-seen
// order. Sessions that never opened an incident (watchboards,
// supersede pointers) yield no trajectory.
func Harvest(r io.Reader) ([]Trajectory, error) {
	sessions := map[string]*session{}
	var order []string
	var records []memory.TriageStatusRecord

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		switch {
		case strings.HasPrefix(line, "INJECT "):
			sid, payload, err := parseInjectLine(line)
			if err != nil {
				return nil, fmt.Errorf("corpus: line %d: %w", lineNo, err)
			}
			s := sessions[sid]
			if s == nil {
				s = &session{sid: sid}
				sessions[sid] = s
				order = append(order, sid)
			}
			s.payloads = append(s.payloads, payload)
		case strings.HasPrefix(line, "{"):
			// A raw JSON line: a §9.4 triage-status record if it has
			// the record's required fields; anything else is ignored.
			var rec memory.TriageStatusRecord
			if err := json.Unmarshal([]byte(line), &rec); err == nil {
				if rec.Fingerprint != "" && rec.ResourceKey != "" && rec.Status.Valid() {
					records = append(records, rec)
				}
			}
		default:
			// SESSION-CREATE, stub banners, kubectl noise: sessions
			// exist when injected into; everything else is ignored.
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("corpus: reading capture: %w", err)
	}

	var out []Trajectory
	for _, sid := range order {
		if tr, ok := harvestSession(sessions[sid], records); ok {
			out = append(out, tr)
		}
	}
	return out, nil
}

// parseInjectLine splits a stub-daemon INJECT line into (sid, payload
// JSON). The payload is the "message" string of the inject envelope.
func parseInjectLine(line string) (string, json.RawMessage, error) {
	sid := kvToken(line, "sid=")
	if sid == "" {
		return "", nil, fmt.Errorf("INJECT line has no sid= token")
	}
	i := strings.Index(line, " body=")
	if i < 0 {
		return "", nil, fmt.Errorf("INJECT line has no body= token")
	}
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(line[i+len(" body="):]), &envelope); err != nil {
		return "", nil, fmt.Errorf("INJECT body is not the inject envelope: %w", err)
	}
	if !json.Valid([]byte(envelope.Message)) {
		return "", nil, fmt.Errorf("inject message is not JSON (schema-stable injects are a §9.3 hard requirement)")
	}
	return sid, json.RawMessage(envelope.Message), nil
}

// kvToken extracts the value of a space-delimited key=value token.
func kvToken(line, key string) string {
	i := strings.Index(line, key)
	if i < 0 {
		return ""
	}
	v := line[i+len(key):]
	if j := strings.IndexByte(v, ' '); j >= 0 {
		v = v[:j]
	}
	return v
}

// peekKind reads just the "kind" discriminator of a payload.
func peekKind(payload json.RawMessage) string {
	var k struct {
		Kind string `json:"kind"`
	}
	_ = json.Unmarshal(payload, &k)
	return k.Kind
}

// openingKind reports whether kind opens an incident record a
// trajectory can grow from: the frozen reactive kind, any
// source-namespaced kind, or a storm aggregate. Everything else —
// followups, outcomes, watchboard digests, supersede pointers,
// regression evidence — attaches to a session, it never opens one.
func openingKind(kind string) bool {
	switch kind {
	case "", inject.KindFollowup, inject.KindResolved, inject.KindResolvedReverted,
		inject.KindStormMember, inject.KindStormMemberSuperseded, inject.KindStormUpdate,
		inject.KindWatchboardDigest, inject.KindWatchboardRotated, inject.KindTriageRegressed:
		return false
	}
	return true
}

// harvestSession walks one session's payloads into a trajectory.
func harvestSession(s *session, records []memory.TriageStatusRecord) (Trajectory, bool) {
	if len(s.payloads) == 0 {
		return Trajectory{}, false
	}
	first := s.payloads[0]
	kind := peekKind(first)
	if !openingKind(kind) {
		return Trajectory{}, false
	}
	tr := Trajectory{Session: s.sid}

	// Symptom: the opening payload. Storm aggregates and plain
	// incidents differ only in which identity fields they carry —
	// both unmarshal losslessly into the superset below.
	var open struct {
		inject.Payload
		AncestorKind string `json:"ancestor_kind"`
		AncestorName string `json:"ancestor_name"`
	}
	if err := json.Unmarshal(first, &open); err != nil {
		return Trajectory{}, false
	}
	sym := &Symptom{
		Kind:         kind,
		Reason:       open.Reason,
		Namespace:    open.Namespace,
		KindOfObject: open.KindOfObject,
		Name:         open.Name,
		UID:          open.UID,
		Severity:     open.Severity,
		Zone:         open.Zone,
		Message:      open.Message,
	}
	if !open.FirstSeen.IsZero() {
		sym.FirstSeen = open.FirstSeen.UTC().Format("2006-01-02T15:04:05Z07:00")
	}
	if sym.KindOfObject == "" && open.AncestorKind != "" {
		sym.KindOfObject = open.AncestorKind
		sym.Name = open.AncestorName
	}
	tr.Symptom = sym
	tr.Cluster = open.Cluster
	tr.Fingerprint = open.Fingerprint
	diag := &Diagnosis{EnrichmentBundle: open.Enrichment != nil && open.Enrichment.Bundle != ""}

	// Walk the rest: outcome records terminate; everything else is a
	// followup. The LAST outcome wins (a resolve that reverted is a
	// reverted trajectory, not a recovered one).
	for _, payload := range s.payloads[1:] {
		k := peekKind(payload)
		switch k {
		case inject.KindResolved, inject.KindResolvedReverted:
			var res inject.ResolvedPayload
			if err := json.Unmarshal(payload, &res); err != nil {
				continue
			}
			out := &Outcome{
				Kind:              k,
				Resolution:        res.Resolution,
				ClearedAfter:      res.ClearedAfter,
				ObservedStableFor: res.ObservedStableFor,
				RevertedAfter:     res.RevertedAfter,
			}
			if !res.ResolvedAt.IsZero() {
				out.ResolvedAt = res.ResolvedAt.UTC().Format("2006-01-02T15:04:05Z07:00")
			}
			tr.Outcome = out
			if tr.Fingerprint == "" {
				// The frozen k8s-event opening carries no wire
				// fingerprint; the outcome record repeats the
				// incident's, so the trajectory still gets its
				// class key from a structured field.
				tr.Fingerprint = res.Fingerprint
			}
			if k == inject.KindResolvedReverted {
				tr.Label = "reverted"
			} else {
				tr.Label = res.Resolution
			}
		default:
			tr.Followups++
		}
	}

	// Diagnosis / action stages from the §9.4 records: matched by
	// session first, then by fingerprint, then by the symptom's
	// resource identity — all structured keys.
	resourceKey := memory.ResourceKey(sym.KindOfObject, sym.Namespace, sym.Name)
	for _, rec := range records {
		if !recordMatches(rec, s.sid, tr.Fingerprint, resourceKey) {
			continue
		}
		switch rec.Status {
		case memory.StatusInvestigating, memory.StatusTriaged:
			diag.TriageStatus = string(rec.Status)
			diag.RootCause = rec.RootCauseHypothesis
			diag.Session = rec.Session
		case memory.StatusActioned, memory.StatusEscalated:
			tr.Action = &Action{Status: string(rec.Status), Action: rec.Action}
			// An actioned record still carries the diagnosis it
			// acted on; keep it if no earlier record supplied one.
			if diag.TriageStatus == "" && rec.RootCauseHypothesis != "" {
				diag.TriageStatus = string(rec.Status)
				diag.RootCause = rec.RootCauseHypothesis
				diag.Session = rec.Session
			}
		case memory.StatusResolved:
			// The sentinel's §9.4 lifecycle flip. The record is
			// current state (upsert semantics), so a capture that
			// only exported records post-incident carries the
			// diagnosis/action FIELDS under the terminal status —
			// fold them as fallback stages rather than losing them.
			if diag.TriageStatus == "" && rec.RootCauseHypothesis != "" {
				diag.TriageStatus = string(rec.Status)
				diag.RootCause = rec.RootCauseHypothesis
				diag.Session = rec.Session
			}
			if tr.Action == nil && rec.Action != "" {
				tr.Action = &Action{Status: string(rec.Status), Action: rec.Action}
			}
		}
	}
	if diag.EnrichmentBundle || diag.TriageStatus != "" {
		tr.Diagnosis = diag
	}

	tr.Complete = tr.Symptom != nil && tr.Diagnosis != nil && tr.Action != nil && tr.Outcome != nil
	return tr, true
}

// recordMatches reports whether a §9.4 record belongs to the
// trajectory: same session, same incident class, or same resource.
func recordMatches(rec memory.TriageStatusRecord, sid, fingerprint, resourceKey string) bool {
	if rec.Session != "" && rec.Session == sid {
		return true
	}
	if fingerprint != "" && rec.Fingerprint == fingerprint {
		return true
	}
	return rec.MatchesResource(resourceKey)
}

// WriteJSONL writes trajectories one JSON object per line, sorted
// complete-first (the corpus consumer's priority order), stable
// within each class.
func WriteJSONL(w io.Writer, trajectories []Trajectory) error {
	sorted := make([]Trajectory, len(trajectories))
	copy(sorted, trajectories)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Complete && !sorted[j].Complete
	})
	enc := json.NewEncoder(w)
	for _, tr := range sorted {
		if err := enc.Encode(tr); err != nil {
			return err
		}
	}
	return nil
}
