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

// The §2.3 finding kinds this package produces.
//
// These names are frozen: `docs/signal-schema-v1.md` pins the kind inventory
// and v1 does not change, which is why §2.3 settled them before any source
// shipped. Nothing here may be renamed without a schema revision.
//
// KindDomainUnavailable is declared alongside the others because it belongs to
// the same inventory, but no verdict produces it. It is a different
// observation — a domain that held eligible nodes now has none — reached
// without scoring anything, and the source raises it directly.
const (
	KindContractViolated  = "leeway.contract_violated"
	KindPlacementDrift    = "leeway.placement_drift"
	KindBaselineBreach    = "leeway.baseline_breach"
	KindDomainUnavailable = "leeway.domain_unavailable"
)

// The §7.7.4 preference-rank kinds.
//
// They are in the same frozen inventory and settled in the same §2.3 table, but
// nothing in this file produces them: they come from a RankVerdict, which is
// judged against pod-seconds rather than against a distribution, and share none
// of FindingKind's inputs. RankRule.Kind is their FindingKind.
const (
	KindRankWedged      = "leeway.rank_wedged"
	KindRankDegraded    = "leeway.rank_degraded"
	KindRankNoMigration = "leeway.rank_no_migration"
	KindRankTierUnused  = "leeway.rank_tier_unused"
)

// FindingKind picks the §2.3 kind for a judged subject.
//
// The tier leads, so kind and tier cannot disagree: §2.3's table assigns each
// kind a tier, and deriving the kind from anything else would let a Tier C
// verdict carry the Tier A name. That matters because one combination is
// structurally reachable — a learned-baseline intent is not an assumed cluster
// default, so it satisfies `declaredContract` and could in principle carry a
// MaxSkew and fire a contract rule — and §8.1 has already decided that case:
// it is Tier C, because a bound we inferred from the subject's own history is
// not a promise anybody made.
//
// Within Tier C the intent source decides, because the kind states what we
// measured and the two Tier C cases measured different things. A learned
// baseline is a deviation from the subject's own history (§7.5), which is what
// `leeway.baseline_breach` names. A subject with no intent at all was scored
// against an apportioned expectation, which is placement drift — less
// confident than Tier B's, but the same observation.
func FindingKind(tier Tier, intent *Intent) string {
	switch {
	case tier == TierA:
		return KindContractViolated
	case tier == TierC && intent != nil && intent.Source == SourceLearnedBaseline:
		return KindBaselineBreach
	default:
		return KindPlacementDrift
	}
}

// FindingScore is §8.5's `score` object.
//
// It is a projection of Scores rather than Scores itself. Scores carries
// working values — χ², concentration, the gate reason — that belong in metrics
// and in a debugger, not in a payload an agent has to read; and freezing the
// payload shape against the internal struct would make every future scoring
// field a wire change.
type FindingScore struct {
	Drift              float64 `json:"drift"`
	ObservedSkew       int64   `json:"observedSkew"`
	MinAchievableSkew  int64   `json:"minAchievableSkew"`
	ExcessSkew         int64   `json:"excessSkew"`
	RelocationDistance int64   `json:"relocationDistance"`
	MaxDomainShare     float64 `json:"maxDomainShare"`
}

// FindingIntent is §8.5's `intent` object: where the expectation came from and
// how far we trust it.
//
// This is the half of the payload that answers "says who?", and it is why the
// evidence lines are carried verbatim. A reader who disagrees with a finding
// disagrees with the intent behind it far more often than with the arithmetic.
type FindingIntent struct {
	Source     string   `json:"source"`
	Confidence string   `json:"confidence"`
	Weighting  string   `json:"weighting"`
	MaxSkew    *int32   `json:"maxSkew,omitempty"`
	Evidence   []string `json:"evidence,omitempty"`
}

// FindingDomain is one row of §8.5's `domains` table.
type FindingDomain struct {
	Domain   Domain `json:"domain"`
	Actual   int64  `json:"actual"`
	Expected int64  `json:"expected"`
	Delta    int64  `json:"delta"`

	// ReadyNodes is the domain's eligible node count, which is what turns the
	// table from an assertion into an argument: three pods in a domain with
	// three nodes is a different finding from three pods in a domain with
	// twelve.
	ReadyNodes int64 `json:"readyNodes"`

	Note string `json:"note,omitempty"`
}

// Finding is §8.5's payload.
//
// It is a plain struct with JSON tags and no dependency on pkg/emit, which
// NFR-10 forbids this package from importing. The source converts it into a
// Signal; `cmd/leeway` marshals the same struct straight to stdout, and that
// the two agree is the point of building it here.
type Finding struct {
	Kind        string      `json:"kind"`
	Subject     SubjectRef  `json:"subject"`
	TopologyKey TopologyKey `json:"topologyKey"`
	Tier        string      `json:"tier"`
	Severity    string      `json:"severity"`

	Score  FindingScore   `json:"score"`
	Intent *FindingIntent `json:"intent,omitempty"`

	Domains []FindingDomain `json:"domains"`

	// SuspectedCause is also the fingerprint's `reasonClass` (§2.3): hashing
	// it alongside the kind is what gives a differently-caused episode its own
	// identity, and is why pinning did not need a kind of its own.
	SuspectedCause      SuspectedCause `json:"suspectedCause"`
	ContributingFactors []string       `json:"contributingFactors,omitempty"`

	// UntestedCauses names the more specific explanations this deployment
	// could not evaluate, because the source owning their evidence is not
	// running. Omitted — the normal case — on a deployment running the full
	// source set, where the ladder's silence really does mean "ruled out".
	//
	// Deliberately outside the fingerprint. Turning a source on would
	// otherwise re-identify every open episode in the cluster as new, which is
	// a page for each of them saying nothing happened.
	UntestedCauses []SuspectedCause `json:"untestedCauses,omitempty"`

	// FirstSeenAt is the dwell state's, not this evaluation's. A finding
	// re-emitted an hour into an episode still reports when the episode
	// started, because "how long has this been true" is the question that
	// decides whether anybody acts on it.
	FirstSeenAt time.Time `json:"firstSeenAt"`

	// Transient names the §7.6 state in force, empty when none was. A subject
	// that breached *through* a rollout is a stronger finding than one that
	// breached on a quiet cluster, and the scores do not say which happened.
	Transient string `json:"transient,omitempty"`

	// Relaxed reports that §7.6 multiplied the thresholds before judging, so
	// the numbers below cleared a bar this payload does not otherwise state.
	Relaxed bool `json:"relaxed,omitempty"`
}

// FindingInput is everything NewFinding assembles. A struct rather than eight
// positional arguments, because six of them are pointers or slices and a
// transposed pair would produce a plausible-looking wrong payload.
type FindingInput struct {
	Subject     SubjectRef
	TopologyKey TopologyKey
	Scores      *Scores
	Intent      *Intent
	Verdict     Verdict
	Attribution Attribution

	// Evidence supplies the per-domain ready-node counts. It is the same
	// value Attribute was given; passing it again rather than copying the
	// counts out keeps one assembly site for the whole finding.
	Evidence Evidence

	// State is the §8.2 dwell state, read only for FirstSeenAt.
	State AlertState
}

// NewFinding builds the §8.5 payload.
//
// It does not decide whether to emit. That is Route's job, and keeping them
// apart means a suppressed subject can still have its payload built for a
// `--dry-run` or a test without any risk of it reaching a sink.
func NewFinding(in FindingInput) Finding {
	f := Finding{
		Kind:                FindingKind(in.Verdict.Tier, in.Intent),
		Subject:             in.Subject,
		TopologyKey:         in.TopologyKey,
		Tier:                in.Verdict.Tier.String(),
		Severity:            in.Verdict.Severity,
		SuspectedCause:      in.Attribution.Cause,
		ContributingFactors: in.Attribution.Factors,
		UntestedCauses:      in.Attribution.Untested,
		FirstSeenAt:         in.State.FirstSeenAt.UTC(),
		Transient:           in.Verdict.Transient.String(),
		Relaxed:             in.Verdict.Relaxed,
	}
	if f.SuspectedCause == "" {
		f.SuspectedCause = CauseUnknown
	}
	if in.Scores != nil {
		f.Score = FindingScore{
			Drift:              in.Scores.Drift,
			ObservedSkew:       in.Scores.ObservedSkew,
			MinAchievableSkew:  in.Scores.MinAchievableSkew,
			ExcessSkew:         in.Scores.ExcessSkew,
			RelocationDistance: in.Scores.Relocation,
			MaxDomainShare:     in.Scores.MaxDomainShare,
		}
		f.Domains = domainRows(in.Scores, in.Evidence, in.Attribution.Notes)
	}
	f.Intent = findingIntent(in.Intent)
	return f
}

// domainRows renders the §8.5 domain table in the scores' canonical order.
//
// Expected and the notes are read defensively: Score leaves Expected unset
// when the apportionment did not line up, and Attribute's notes are only
// index-aligned to the scores it was given. A payload built from a mismatched
// pair should come out short a column, not out of bounds.
func domainRows(s *Scores, ev Evidence, notes []string) []FindingDomain {
	rows := make([]FindingDomain, 0, len(s.Domains))
	for i, d := range s.Domains {
		row := FindingDomain{Domain: d, ReadyNodes: ev.Domains[d].ReadyNodes}
		if i < len(s.Actual) {
			row.Actual = s.Actual[i]
		}
		if i < len(s.Expected) {
			row.Expected = s.Expected[i]
			row.Delta = row.Actual - row.Expected
		}
		if i < len(notes) {
			row.Note = notes[i]
		}
		rows = append(rows, row)
	}
	return rows
}

// findingIntent projects an Intent, or nil for a subject that had none.
//
// Nil is a meaningful payload value here and is not the same as an empty
// object: it says the scoring was against an apportioned expectation with
// nobody's intent behind it, which is exactly the Tier C case a reader should
// weigh differently.
func findingIntent(in *Intent) *FindingIntent {
	if in == nil {
		return nil
	}
	fi := &FindingIntent{
		Source:     in.Source.String(),
		Confidence: in.Confidence.String(),
		Weighting:  in.Weighting.String(),
		MaxSkew:    in.MaxSkew,
	}
	for _, e := range in.Evidence {
		detail := e.Detail
		if e.Ignored {
			// A retained-but-not-applied observation is why a lower-precedence
			// source is absent from the verdict, and dropping it here is how a
			// reader ends up asking why we ignored their annotation.
			detail = "(not applied) " + detail
		}
		fi.Evidence = append(fi.Evidence, detail)
	}
	return fi
}
