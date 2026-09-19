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

import "math"

// Thresholds are the scoring gates from §7.4 and §10.1. The zero value is not
// usable; call DefaultThresholds.
type Thresholds struct {
	// MinReplicasForScoring is the replica count below which skew is
	// meaningless and drift is not evaluated at all (default 3).
	MinReplicasForScoring int64
	// SmallNThreshold is the replica count below which a drift breach must
	// also move at least MinRelocationSmallN objects to count (default 10).
	SmallNThreshold int64
	// MinRelocationSmallN is that floor (§7.4 fixes it at 2: "a single
	// misplaced pod out of 4 never pages anyone").
	MinRelocationSmallN int64
	// Drift is the ρ threshold (default 0.2).
	Drift float64
	// MaxDomainShare is the concentration threshold that escalates severity
	// (default 0.5).
	MaxDomainShare float64
}

// DefaultThresholds returns the §10.1 and §7.4 defaults.
func DefaultThresholds() Thresholds {
	return Thresholds{
		MinReplicasForScoring: 3,
		SmallNThreshold:       10,
		MinRelocationSmallN:   2,
		Drift:                 0.2,
		MaxDomainShare:        0.5,
	}
}

// GateReason explains why a subject was not evaluated for drift.
type GateReason uint8

// The gating outcomes.
const (
	GateNone GateReason = iota
	GateNoDomains
	GateNoObjects
	GateBelowMinReplicas
	GateModeIgnore
)

// String implements fmt.Stringer.
func (g GateReason) String() string {
	switch g {
	case GateNone:
		return ""
	case GateNoDomains:
		return "no eligible domains"
	case GateNoObjects:
		return "no objects counted"
	case GateBelowMinReplicas:
		return "below minReplicasForScoring"
	case GateModeIgnore:
		return "intent mode is Ignore"
	default:
		return "unknown"
	}
}

// Scores is the full §7.3 metric set for one (subject, topology key).
//
// Every field is populated whenever it is meaningful, including when Evaluable
// is false: §7.4 says that below minReplicasForScoring we still "record
// distribution and max domain share", we just do not judge them. A caller that
// checks Evaluable before reading Drift gets the design's behaviour; one that
// reads MaxDomainShare unconditionally also gets it.
type Scores struct {
	Domains  []Domain
	Actual   []int64
	Expected []int64
	Total    int64 // n, the number of objects counted

	// MaxDomainObjects is max(a), the count in the fullest domain. It is the
	// numerator of MaxDomainShare, kept as a count because the per-domain
	// ceiling a required podAntiAffinity expresses is a count and comparing it
	// against a share would reintroduce the rounding the ceiling exists to
	// forbid.
	MaxDomainObjects int64

	ObservedSkew      int64 // S = max(a) − min(a)
	MinAchievableSkew int64 // S*, the floor imposed by arithmetic and caps
	ExcessSkew        int64 // E = max(0, S − max(S*, maxSkew))
	Relocation        int64 // R = Σ max(0, aᵢ − eᵢ)

	Drift          float64 // ρ = R/n
	Concentration  float64 // H = Σ (aᵢ/n)²
	MaxDomainShare float64 // max(aᵢ)/n

	ChiSquare        float64
	ChiSquareValid   bool // all eᵢ ≥ 5
	DegreesOfFreedom int

	Unplaceable int64 // carried from the apportionment
	Evaluable   bool
	Gate        GateReason
}

// Score computes §7.3 over the eligible domains, in the order given.
//
// domains, actual and the apportionment must be index-aligned and in the
// canonical order from SortDomains — the metrics are order-independent, but
// the expectation that feeds them is not, so passing a differently-ordered
// slice here silently scores against the wrong domain.
//
// maxSkew is the hard contract from the intent, or nil where there is none.
func Score(domains []Domain, actual []int64, ap Apportionment, maxSkew *int32, t Thresholds) Scores {
	s := Scores{
		Domains:     domains,
		Actual:      actual,
		Expected:    ap.Expected,
		Unplaceable: ap.Unplaceable,
	}
	m := len(domains)
	if m == 0 {
		s.Gate = GateNoDomains
		return s
	}
	for _, a := range actual {
		s.Total += a
	}
	if s.Total == 0 {
		s.Gate = GateNoObjects
		return s
	}

	minA, maxA := actual[0], actual[0]
	for _, a := range actual {
		if a < minA {
			minA = a
		}
		if a > maxA {
			maxA = a
		}
	}
	s.ObservedSkew = maxA - minA
	s.MaxDomainObjects = maxA
	s.MaxDomainShare = float64(maxA) / float64(s.Total)

	// S* is read off the expectation rather than computed as `0 if n mod m ==
	// 0 else 1`. The two agree for the equal-weight uncapped case the design
	// states, and reading it off the expectation also gets the two cases that
	// formula does not cover: unequal weights, where the intended distribution
	// is itself uneven, and saturated caps, which §7.2 says raise the floor.
	// The expectation is by construction the best achievable placement, so its
	// own skew is the floor.
	if len(ap.Expected) == m {
		minE, maxE := ap.Expected[0], ap.Expected[0]
		for _, e := range ap.Expected {
			if e < minE {
				minE = e
			}
			if e > maxE {
				maxE = e
			}
		}
		s.MinAchievableSkew = maxE - minE
	}

	floor := s.MinAchievableSkew
	if maxSkew != nil && int64(*maxSkew) > floor {
		floor = int64(*maxSkew)
	}
	if e := s.ObservedSkew - floor; e > 0 {
		s.ExcessSkew = e
	}

	// R counts only the surplus side. Because Σa and Σe both equal the placed
	// total, the surplus and deficit sides are equal, so R is also half the L1
	// distance — "how many objects must move", which is the number a human
	// acts on.
	if len(ap.Expected) == m {
		for i := range actual {
			if d := actual[i] - ap.Expected[i]; d > 0 {
				s.Relocation += d
			}
		}
	}
	s.Drift = float64(s.Relocation) / float64(s.Total)

	for _, a := range actual {
		share := float64(a) / float64(s.Total)
		s.Concentration += share * share
	}

	s.ChiSquare, s.ChiSquareValid = chiSquare(actual, ap.Expected)
	s.DegreesOfFreedom = m - 1

	switch {
	case s.Total < t.MinReplicasForScoring:
		s.Gate = GateBelowMinReplicas
	default:
		s.Evaluable = true
	}
	return s
}

// chiSquare returns Σ (aᵢ − eᵢ)²/eᵢ and whether the statistic is usable.
//
// The validity flag is the important half. χ²'s sampling distribution is only
// approximately chi-squared, and the approximation falls apart when expected
// cell counts are small — the conventional floor is 5, which §7.3 adopts. A
// small-cell χ² is not a conservative estimate, it is an inflated one, so
// reporting the number without the caveat would systematically over-call
// significance on exactly the small workloads §7.4 is already trying to
// protect.
func chiSquare(actual, expected []int64) (float64, bool) {
	if len(actual) != len(expected) || len(actual) == 0 {
		return 0, false
	}
	var x2 float64
	valid := true
	for i := range actual {
		e := expected[i]
		if e < 5 {
			valid = false
		}
		if e <= 0 {
			continue // an empty expected cell contributes nothing computable
		}
		d := float64(actual[i] - e)
		x2 += d * d / float64(e)
	}
	return x2, valid
}

// Breach reports whether the scores cross the configured thresholds, and why.
//
// This is deliberately separate from Score: Score is arithmetic and always
// produces the same numbers, while Breach is policy and depends on thresholds
// a user can change. Keeping them apart means a threshold change cannot alter
// a recorded metric, only the verdict drawn from it.
//
// Callers that need the tier as well as the verdict should use Judge, which
// calls this and classifies the result; this signature stays because "did it
// breach, and in one line why" is the whole question at most call sites.
func (s *Scores) Breach(intent *Intent, t Thresholds) (bool, string) {
	k, reason := s.breach(intent, t)
	return k != BreachNone, reason
}

// breach applies the §7.3/§7.4 rules in precedence order and names which one
// fired, because the rule decides the tier: a contract breach is Tier A and a
// distributional one is not, and reconstructing that from a prose reason
// string downstream would be guesswork.
//
// Where a hard contract exists it supersedes ρ (§7.3): the bound was violated
// or it was not, and no amount of distributional nuance changes that.
func (s *Scores) breach(intent *Intent, t Thresholds) (BreachKind, string) {
	if intent != nil && intent.Mode == ModeIgnore {
		return BreachNone, GateModeIgnore.String()
	}
	if !s.Evaluable {
		return BreachNone, s.Gate.String()
	}

	// The two hard contracts are checked separately, against the quantity each
	// one actually bounds. Treating them as interchangeable — asking
	// HardContract() and then testing ExcessSkew — reports a required
	// podAntiAffinity's ceiling as "observed skew exceeds the declared
	// maxSkew" on a subject that declared no maxSkew at all, and reports it
	// against a floor derived from the expectation rather than from the
	// ceiling. The per-domain rule goes first because it is the more specific
	// statement where a subject somehow carries both.
	if intent.HardPerDomainContract() && s.MaxDomainObjects > *intent.MaxPerDomain {
		return BreachPerDomainCeiling, "a domain holds more objects than the declared per-domain ceiling"
	}
	if intent.HardSkewContract() && s.ExcessSkew > 0 {
		return BreachMaxSkew, "observed skew exceeds the declared maxSkew"
	}

	// Colocation inverts the frame: the objects are meant to be together, so
	// what counts as deviation is how many of them are not in the fullest
	// domain. Running the spread rule here would report ρ against an
	// expectation that spreads them, which is drift from the opposite of what
	// the subject asked for — a guaranteed finding on every workload that
	// declares a podAffinity, and the reason this branch exists rather than
	// colocation simply falling through.
	if intent != nil && intent.Mode == ModeColocate {
		return s.breachColocate(t)
	}

	if s.Drift <= t.Drift {
		return BreachNone, ""
	}

	// §7.4: between minReplicasForScoring and smallNThreshold, ρ alone is too
	// twitchy — at n=4 a single misplaced pod is ρ=0.25, over any sane
	// threshold. Requiring R ≥ 2 as well means the finding always names at
	// least two objects that have to move.
	if s.Total < t.SmallNThreshold && s.Relocation < t.MinRelocationSmallN {
		return BreachNone, "small-n: relocation below floor"
	}
	return BreachDrift, "normalised drift over threshold"
}

// breachColocate applies the drift rule to a colocation intent, against
// Dispersion rather than ρ. The threshold and the small-n floor are the same
// ones, deliberately: both are statements about how many objects a human would
// have to move before the finding is worth reading, and that does not change
// with the direction they move in.
func (s *Scores) breachColocate(t Thresholds) (BreachKind, string) {
	if s.Dispersion() <= t.Drift {
		return BreachNone, ""
	}
	if s.Total < t.SmallNThreshold && s.Scattered() < t.MinRelocationSmallN {
		return BreachNone, "small-n: scattered objects below floor"
	}
	return BreachDrift, "objects scattered across domains against a colocation intent"
}

// Dispersion is the colocation counterpart of Drift: the fraction of objects
// outside the fullest domain, which is 1 − MaxDomainShare and is also
// Scattered/n, so it reads the same way ρ does — "what share of this subject
// is in the wrong place".
func (s *Scores) Dispersion() float64 {
	if s.Total == 0 {
		return 0
	}
	return 1 - s.MaxDomainShare
}

// Scattered is the colocation counterpart of Relocation: how many objects
// would have to move to bring the subject into one domain.
func (s *Scores) Scattered() int64 { return s.Total - s.MaxDomainObjects }

// Escalate reports whether concentration warrants raising severity, per §8.3's
// use of max domain share as the zone-failure-risk signal.
//
// It is meaningless for a colocation intent, where concentration is the goal,
// and Judge is what knows the intent; this stays a plain question about the
// numbers.
func (s *Scores) Escalate(t Thresholds) bool {
	return s.Evaluable && s.MaxDomainShare > t.MaxDomainShare
}

// ExpectedShares returns the expected fraction per domain, for export.
func (s *Scores) ExpectedShares() []float64 {
	out := make([]float64, len(s.Expected))
	var total int64
	for _, e := range s.Expected {
		total += e
	}
	if total == 0 {
		return out
	}
	for i, e := range s.Expected {
		out[i] = float64(e) / float64(total)
	}
	return out
}

// almostEqual is a test and invariant helper for float comparisons.
func almostEqual(a, b, tol float64) bool { return math.Abs(a-b) <= tol }
