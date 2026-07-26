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

// The §9.4 scan join: `lookout health` / `bundle` join open findings
// against triage-status records so a scan run mid-incident reports
// the triaged reality — diagnosis, paper trail, and the agent's
// severity judgment — instead of a fresh unknown (the M4 exit
// criterion, DESIGN.md §14).

import (
	"sort"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// Detail keys the join adds to a matched finding. Declared here so
// every joining command's output glossary uses the same spellings.
const (
	DetailTriageStatus    = "triage_status"
	DetailTriageRootCause = "triage_root_cause"
	DetailTriageAction    = "triage_action"
	DetailTriageSession   = "triage_session"
	DetailTriageAge       = "triage_age"
)

// Joiner matches scan findings against a set of triage-status
// records. Build one per scan from the store's open records; it is
// read-only and safe for sequential reuse across findings.
type Joiner struct {
	records []TriageStatusRecord
	now     time.Time
}

// NewJoiner keeps the OPEN records (resolved ones are corpus, not
// current truth), most recently updated first — when several records
// match one finding, the freshest wins.
func NewJoiner(records []TriageStatusRecord, now time.Time) *Joiner {
	open := make([]TriageStatusRecord, 0, len(records))
	for _, r := range records {
		if r.Status.Open() {
			open = append(open, r)
		}
	}
	sort.SliceStable(open, func(i, j int) bool { return open[i].Updated.After(open[j].Updated) })
	return &Joiner{records: open, now: now}
}

// Len reports how many open records the joiner holds.
func (j *Joiner) Len() int { return len(j.records) }

// Match finds the open record for the identified resource + reason.
// Records are keyed by the §9.4 PAIR (fingerprint, resource_key),
// and the join respects both halves:
//
//   - resource_key is the required pin: the §8 fingerprint is
//     class-level — it spans objects and clusters by design — so a
//     fingerprint-only join would smear one incident's diagnosis
//     over every same-class finding. MatchesResource compares the
//     canonical key (with the documented group/version-prefix
//     tolerance).
//   - fingerprint disambiguates several open records on ONE
//     resource: the finding's §8 scan candidate — the M0-frozen
//     reactive kind ("k8s-event") + canonicalized reason + object
//     class, zone empty on both sides until cluster-metadata wiring
//     lands — is preferred over recency. Records whose fingerprints
//     a scan cannot reproduce (leading-indicator kinds) still join
//     via the resource pin, freshest first.
func (j *Joiner) Match(kindOfObject, namespace, name, reason string) (TriageStatusRecord, bool) {
	if j == nil || len(j.records) == 0 {
		return TriageStatusRecord{}, false
	}
	key := ResourceKey(kindOfObject, namespace, name)
	var candidate string
	if reason != "" {
		candidate = engine.Fingerprint(engine.KindK8sEvent, engine.CanonicalReason(reason), kindOfObject, "")
	}
	best := -1
	for i, r := range j.records {
		if !r.MatchesResource(key) {
			continue
		}
		if candidate != "" && r.Fingerprint == candidate {
			return r, true // exact class match beats recency
		}
		if best < 0 {
			best = i // freshest resource match (records are sorted)
		}
	}
	if best < 0 {
		return TriageStatusRecord{}, false
	}
	return j.records[best], true
}

// Annotate merges the matched record's triage fields into the
// finding and reports whether it matched. Matched findings gain the
// DetailTriage* fields, and severity reflects the agent's judgment:
// an escalated record pins critical; otherwise a severity_override
// replaces the scan's own class. Unmatched findings are returned
// untouched — the join never invents state.
func (j *Joiner) Annotate(f *emit.Finding) bool {
	rec, ok := j.Match(f.KindOfObject, f.Namespace, f.Name, f.Reason)
	if !ok {
		return false
	}
	details := []emit.Field{{Key: DetailTriageStatus, Value: string(rec.Status)}}
	if rec.RootCauseHypothesis != "" {
		details = append(details, emit.Field{Key: DetailTriageRootCause, Value: rec.RootCauseHypothesis})
	}
	if rec.Action != "" {
		details = append(details, emit.Field{Key: DetailTriageAction, Value: rec.Action})
	}
	if rec.Session != "" {
		details = append(details, emit.Field{Key: DetailTriageSession, Value: rec.Session})
	}
	if !rec.Updated.IsZero() && j.now.After(rec.Updated) {
		details = append(details, emit.Field{Key: DetailTriageAge, Value: j.now.Sub(rec.Updated).Truncate(time.Second).String()})
	}
	f.Details = append(f.Details, details...)
	switch {
	case rec.Status == StatusEscalated:
		f.Severity = emit.SeverityCritical // escalated stays hot (§9.4)
	case rec.SeverityOverride != "":
		f.Severity = rec.SeverityOverride
	}
	return true
}
