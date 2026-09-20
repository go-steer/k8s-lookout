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

package leeway

import "time"

// BaselineRecord is one persisted BaselineSet: §9.1's second "yes, persist
// this" row, at the same (cluster, subject, topology key) grain as
// AlertRecord.
//
// It is a separate type from BaselineSet for the same reason AlertRecord is
// separate from AlertState: the set is a live estimator with runtime fields,
// and the record is the subset that is worth surviving a restart. Three
// fields deliberately do not cross:
//
//   - Frozen is re-derived. It follows the alert phase and §7.6 suppression,
//     both of which are recomputed on the first pass after arming, and a
//     persisted freeze could otherwise outlive the finding that justified it
//     and silently stop a subject learning for good.
//   - WidenBy and WidenUntil are *produced* by loading, not carried through
//     it. §9.3 step 7 decides them from how long the gap was, and a widening
//     persisted from a previous outage would be applied against the wrong
//     clock.
type BaselineRecord struct {
	Cluster     string
	SubjectKey  string
	TopologyKey string

	// Domains carries one row per domain, in the set's canonical order —
	// which is also §7.5's invalidation fingerprint, so the order is part of
	// the record rather than an artefact of how it was written.
	Domains []BaselineDomain

	// DevWeight is the accumulated EWMA weight the deviations are corrected
	// by. Persisting it is not optional: a restart that dropped it would
	// divide a converged deviation by a freshly-zeroed weight and produce
	// either a zero band or a division by zero, depending on how carefully
	// the reader was written.
	DevWeight float64

	Samples   uint64
	FirstSeen time.Time
	UpdatedAt time.Time
}

// BaselineDomain is one domain's learned pair.
type BaselineDomain struct {
	Domain    Domain  `json:"domain"`
	Share     float64 `json:"share"`
	Deviation float64 `json:"deviation"`
}

// Record projects a set for persistence. An empty or nil set produces a
// record with no domains, which Load reads back as "nothing learned".
func (b *BaselineSet) Record(cluster, subjectKey, topologyKey string) BaselineRecord {
	rec := BaselineRecord{
		Cluster:     cluster,
		SubjectKey:  subjectKey,
		TopologyKey: topologyKey,
	}
	if b == nil {
		return rec
	}
	rec.DevWeight = b.DevWeight
	rec.Samples = b.Samples
	rec.FirstSeen = b.FirstSeen
	rec.UpdatedAt = b.UpdatedAt
	rec.Domains = make([]BaselineDomain, len(b.Domains))
	for i, d := range b.Domains {
		rec.Domains[i] = BaselineDomain{
			Domain:    d,
			Share:     b.Baselines[i].Share,
			Deviation: b.Baselines[i].Deviation,
		}
	}
	return rec
}

// Set rebuilds a live estimator from a record, or returns nil for a record
// carrying nothing to rebuild from.
//
// The caller runs AssessDowntime on the result; this deliberately does not,
// because the gap is measured against the caller's single reconcile instant
// and a loader that read the clock itself would give two subjects loaded a
// second apart two different answers about the same outage.
//
// Everything here is repaired rather than trusted, on §9.3's rule: prefer the
// reading that loses information over the one that acts on a lie. A record
// with duplicate or unsorted domains is re-canonicalised, because the domain
// order is the invalidation fingerprint and a scrambled one would reset a
// perfectly good baseline on the next sample. A share outside [0,1] or a
// negative deviation is a damaged row, and the whole record is refused rather
// than half-believed — a baseline that relearns costs six hours, one built
// from a corrupt band fires on a cluster that is fine.
func (r BaselineRecord) Set() *BaselineSet {
	if len(r.Domains) == 0 || r.Samples == 0 {
		return nil
	}
	seen := make(map[Domain]BaselineDomain, len(r.Domains))
	for _, d := range r.Domains {
		if d.Domain == "" {
			return nil
		}
		if d.Share < 0 || d.Share > 1 || d.Deviation < 0 {
			return nil
		}
		if _, dup := seen[d.Domain]; dup {
			return nil
		}
		seen[d.Domain] = d
	}
	if r.DevWeight < 0 || r.DevWeight > 1 {
		return nil
	}

	domains := make([]Domain, 0, len(seen))
	for d := range seen {
		domains = append(domains, d)
	}
	SortDomains(domains)

	b := &BaselineSet{
		Domains:   domains,
		Baselines: make([]Baseline, len(domains)),
		DevWeight: r.DevWeight,
		Samples:   r.Samples,
		FirstSeen: r.FirstSeen,
		UpdatedAt: r.UpdatedAt,
	}
	for i, d := range domains {
		b.Baselines[i] = Baseline{Share: seen[d].Share, Deviation: seen[d].Deviation}
	}

	// Two timestamps, two different hazards. A record with no UpdatedAt
	// cannot be assessed for downtime at all — every gap measured against
	// the year 1 is two millennia — and there is no reading of it that is
	// both safe and useful, so it is refused. A record with no FirstSeen
	// would instead be mature the instant it loaded, which is the dangerous
	// direction; treating it as written now costs six hours of relearning
	// and cannot fire on a cluster that is fine.
	//
	// A FirstSeen in the *future* is left alone. §9.3 clamps future
	// timestamps because they wedge a dwell, but here the only effect is to
	// postpone maturity, and postponing a Tier C finding is never the error
	// worth repairing.
	if b.UpdatedAt.IsZero() {
		return nil
	}
	if b.FirstSeen.IsZero() {
		b.FirstSeen = b.UpdatedAt
	}
	return b
}
