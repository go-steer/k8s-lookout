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

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// RankShare is one row of a rank finding's occupancy table: how much of the
// window one preference tier accounted for, and which rules sit at it.
//
// The rules are on the row rather than in a separate block because the number
// on its own is unactionable. "Rank 2 held 94 % of the time" does not tell
// anybody what to buy more of; "rank 2 (spot=true,machineFamily=n2) held 94 %"
// does.
type RankShare struct {
	Rank       Rank    `json:"rank"`
	Rules      string  `json:"rules,omitempty"`
	PodSeconds float64 `json:"podSeconds"`
	Share      float64 `json:"share"`
	Pods       int     `json:"pods"`
}

// RankFinding is the §8.5 payload for a preference-rank finding.
//
// A separate struct from Finding, not a variant of it. Finding's whole middle —
// the domain table, the skew scores, the intent provenance — answers "where are
// these objects and where should they be", and a rank finding answers "what
// hardware did this class actually get, for how long". They share the envelope
// fields and nothing else, and unioning them would produce a payload where
// two-thirds of the keys are null on every document.
type RankFinding struct {
	Kind    string     `json:"kind"`
	Subject SubjectRef `json:"subject"`

	// Provider and ObjectKind carry what the neutral subject vocabulary
	// deliberately drops: which mechanism this axis came from, and what an
	// operator would type into kubectl to see it.
	Provider   string `json:"provider"`
	ObjectKind string `json:"objectKind"`

	// SpecHash pins which version of the class the numbers are about. A
	// finding quoting a rank share is only interpretable against the rule list
	// that was in force, and §7.7.5 is about how quietly that changes.
	SpecHash string `json:"specHash"`

	Tier     string `json:"tier"`
	Severity string `json:"severity"`
	Rule     string `json:"rule"`

	// Focus is the tier a per-tier finding is about, omitted on axis-wide ones.
	Focus *Rank `json:"focus,omitempty"`

	Observed RankObservation `json:"observed"`
	Ranks    []RankShare     `json:"ranks"`

	// Ordering and Tiers restate how the ladder was built, because the single
	// most common way to misread a rank finding is to assume rank is list
	// position — which on a scored class it is not (§7.7.1).
	Ordering string `json:"ordering"`
	Tiers    int    `json:"tiers"`

	// ScaleUp and OptimizeRulePriority are §7.7.4's two gates, reported on
	// every finding rather than only on the ones they gated. They are the
	// first thing a reader changes in response, and the first thing they need
	// to know has not already been changed.
	ScaleUp              string `json:"scaleUp"`
	OptimizeRulePriority bool   `json:"optimizeRulePriority"`

	// Reason is the judging rule's own sentence.
	Reason string `json:"reason"`

	// FirstSeenAt is the dwell state's, not this evaluation's — the episode
	// start, because "how long has this been true" is what decides whether
	// anybody acts.
	FirstSeenAt time.Time `json:"firstSeenAt"`
}

// RankFindingInput is everything NewRankFinding assembles.
type RankFindingInput struct {
	Class   *ComputeClass
	Verdict RankVerdict
	Window  RankWindow
	State   AlertState

	// ObjectKind is the provider's own object kind — "ComputeClass" on GKE.
	// Supplied by the source rather than derived, because this package does
	// not know what CRD it is looking at and §7.7's provider abstraction is
	// the reason it does not.
	ObjectKind string
}

// NewRankFinding builds the payload.
//
// Like NewFinding it does not decide whether to emit; RouteRank does. Building
// a payload for a suppressed verdict is the point — `--dry-run` and the store
// both want it, and a Tier C finding nobody was told about is exactly what an
// operator asks to see when they consider turning the opt-in on.
func NewRankFinding(in RankFindingInput) RankFinding {
	axis := in.Class.Axis
	f := RankFinding{
		Kind:                 in.Verdict.Kind,
		Subject:              SubjectRef{Kind: SubjectPreferenceAxis, Name: axis.Key.Name},
		Provider:             string(axis.Key.Provider),
		ObjectKind:           in.ObjectKind,
		SpecHash:             axis.SpecHash,
		Tier:                 in.Verdict.Tier.String(),
		Severity:             in.Verdict.Severity,
		Rule:                 in.Verdict.Rule.String(),
		Observed:             in.Verdict.Observed,
		Ranks:                rankRows(axis, in.Window),
		Ordering:             axis.Ordering.String(),
		Tiers:                axis.Tiers,
		ScaleUp:              in.Class.ScaleUp.String(),
		OptimizeRulePriority: in.Class.OptimizeRulePriority,
		Reason:               in.Verdict.Reason,
		FirstSeenAt:          in.State.FirstSeenAt.UTC(),
	}
	if in.Verdict.Focus != RankUnknown {
		focus := in.Verdict.Focus
		f.Focus = &focus
	}
	return f
}

// rankRows renders the occupancy table in rank order, sentinels last.
//
// Every rank the window saw appears, including the sentinels, and including
// tiers that accrued nothing. A zero row is the most informative row in a
// dead-tier finding, and dropping empty rows would make the one finding that
// is *about* an empty row unable to show it.
func rankRows(axis *PreferenceAxis, w RankWindow) []RankShare {
	seen := map[Rank]struct{}{}
	ranks := make([]Rank, 0, len(w.PodSeconds)+axis.Tiers)
	add := func(r Rank) {
		if _, ok := seen[r]; ok {
			return
		}
		seen[r] = struct{}{}
		ranks = append(ranks, r)
	}
	for _, tier := range tiersOf(axis) {
		add(tier)
	}
	for rank := range w.PodSeconds {
		add(rank)
	}
	for rank := range w.Pods {
		add(rank)
	}

	// Tiers ascending first, then the sentinels in their own descending order
	// so that unknown, unsatisfiable and off-axis read in a stable sequence
	// rather than in the reverse of the one they are declared in.
	sort.Slice(ranks, func(i, j int) bool {
		a, b := ranks[i], ranks[j]
		if a.IsTier() != b.IsTier() {
			return a.IsTier()
		}
		if a.IsTier() {
			return a < b
		}
		return a > b
	})

	rows := make([]RankShare, 0, len(ranks))
	for _, rank := range ranks {
		row := RankShare{
			Rank:       rank,
			PodSeconds: w.PodSeconds[rank],
			Share:      w.Share(rank),
			Pods:       w.Pods[rank],
		}
		if rank.IsTier() {
			row.Rules = renderTier(axis, rank)
		}
		rows = append(rows, row)
	}
	return rows
}

// rankMessageRows caps how many occupancy rows the one-line message carries,
// for the reason §8.5 caps the domain lines: a ten-tier class is one story, and
// the whole table is in the payload for anyone who wants it.
const rankMessageRows = 3

// RankMessage renders a rank finding as one line of prose.
//
// The rows are chosen by share and then printed back in rank order, so two
// findings about the same axis an hour apart diff cleanly even when the
// busiest tier changed.
func RankMessage(f RankFinding) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s on %s %s (tier %s, mean achieved rank %.2f over %s)",
		rankHeadline(f.Kind), f.ObjectKind, f.Subject.Name, f.Tier,
		f.Observed.MeanRank, time.Duration(f.Observed.WindowSeconds*float64(time.Second)).Round(time.Second))

	if rows := busiestRanks(f.Ranks, rankMessageRows); len(rows) > 0 {
		b.WriteString(": ")
		for i, r := range rows {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "rank %s %.0f%%", r.Rank, r.Share*100)
			if r.Rules != "" {
				fmt.Fprintf(&b, " (%s)", r.Rules)
			}
		}
		if len(rows) < len(f.Ranks) {
			fmt.Fprintf(&b, " (+%d more)", len(f.Ranks)-len(rows))
		}
	}

	fmt.Fprintf(&b, " — %s", f.Reason)
	// Said out loud rather than left to the payload, because it is the
	// sentence that stops a reader treating rank as list position.
	fmt.Fprintf(&b, "; ordered by %s, %d tier(s)", f.Ordering, f.Tiers)
	return b.String()
}

// rankHeadline is the human name of a rank kind. The kind is already its own
// field on the wire, so repeating it verbatim would spend the most-read words
// in the payload on something the reader can see.
func rankHeadline(kind string) string {
	switch kind {
	case KindRankWedged:
		return "compute class wedged"
	case KindRankNoMigration:
		return "no migration back to preferred capacity"
	case KindRankTierUnused:
		return "preference tier unused"
	default:
		return "preference rank degraded"
	}
}

// busiestRanks picks the n rows with the largest share, restoring rank order
// before returning. Ties break on the rank so the choice is reproducible.
func busiestRanks(rows []RankShare, n int) []RankShare {
	if len(rows) <= n {
		return rows
	}
	order := make([]int, len(rows))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		x, y := rows[order[a]], rows[order[b]]
		if x.Share != y.Share {
			return x.Share > y.Share
		}
		return x.Rank < y.Rank
	})
	keep := order[:n]
	sort.Ints(keep)
	out := make([]RankShare, 0, n)
	for _, i := range keep {
		out = append(out, rows[i])
	}
	return out
}
