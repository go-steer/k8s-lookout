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
	"time"

	v1 "k8s.io/api/core/v1"
)

// SuspectedCause is §8.5's attribution: what we think produced the drift.
//
// This is a separate namespace from the §2.3 kinds, deliberately — the kind
// states what we measured, the cause states what we think produced it, and
// only the first is an observation. `domain_outage` survives here as a cause
// while the kind that once bore the name became `leeway.domain_unavailable`.
//
// Attribution is therefore allowed to be wrong in a way a kind is not, which
// is why the field is called *suspected* and why every finding reports the
// numbers regardless of which cause won.
type SuspectedCause string

// The §8.5 causes. CauseUnknown is the fallback and the zero value's meaning,
// though the zero value itself is the empty string: an unattributed finding
// says so explicitly rather than by omission.
const (
	CauseUnknown           SuspectedCause = "unknown"
	CauseDomainOutage      SuspectedCause = "domain_outage"
	CauseCapacityShortfall SuspectedCause = "domain_capacity_shortfall"
	CauseTaintExclusion    SuspectedCause = "taint_exclusion"
	CauseConsolidation     SuspectedCause = "consolidation"
	CauseVolumePinning     SuspectedCause = "volume_pinning"
	CauseRolloutBias       SuspectedCause = "rollout_bias"
	CauseConstraintIgnored SuspectedCause = "constraint_ignored"
)

// DomainFacts is what the rest of the pipeline knows about one domain.
//
// Every field here is somebody else's observation. §8.5 is explicit that
// attribution consumes sibling sources rather than re-deriving them —
// `capacity` owns stockout and pending-pod aging, `objectstate` owns node
// Ready transitions — and keeping the inputs in a plain struct is what lets
// the whole rules engine be exercised without a cluster, a clock or an
// informer.
type DomainFacts struct {
	// ReadyNodes is the domain's current count of Ready, schedulable nodes.
	ReadyNodes int64

	// PeakReadyNodes is the highest ReadyNodes seen inside the attribution
	// window, and is compared against rather than the oldest sample for the
	// same reason §7.6's outage detector is: a domain that fell 12 → 3 → 3 is
	// still in an outage while the 12 is in the window, and an oldest-sample
	// rule stops calling it one the moment the fall scrolls off.
	PeakReadyNodes int64

	// LostAt is when ReadyNodes last fell. It reaches the finding verbatim,
	// because "when" is the first thing a reader correlating against their own
	// change log needs.
	LostAt time.Time

	// NotReadyNodes is how many of the domain's nodes are present but NotReady.
	//
	// It is not redundant with a fall in ReadyNodes. A process that started
	// *during* an outage has no peak to compare against — its history begins
	// with the domain already down — and the census is the only evidence left.
	NotReadyNodes int64

	// TaintedAt is when a taint made this domain ineligible, if one did.
	TaintedAt time.Time

	// ConsolidatedAt is when an autoscaler consolidation removed a node here.
	ConsolidatedAt time.Time

	// Pinned is how many of the subject's objects in this domain cannot move —
	// the Distribution's per-domain Pinned tally, which Scores does not carry.
	Pinned int64
}

// Evidence is the whole attribution input: per-domain facts plus the
// subject-wide observations that are not attributable to any one domain.
type Evidence struct {
	// Domains is keyed by domain rather than index-aligned to the scores. The
	// caller assembling this is merging several sources, none of which knows
	// the canonical domain order, and a silently misaligned slice would
	// attribute one zone's outage to another.
	Domains map[Domain]DomainFacts

	// InsufficientResource is how many of the subject's objects are Pending
	// with a FailedScheduling citing insufficient resources. The `capacity`
	// source owns that judgement; this is its answer, not a re-reading of it.
	InsufficientResource int64

	// SchedulingMessage is the verbatim FailedScheduling message, for the
	// contributing-factor line. Free text from the cluster: it is sanitized
	// upstream on the emit surface like every other message, and is carried
	// here unparsed on purpose — the numbers in it are the scheduler's and
	// re-deriving them from our own view would invent a second answer.
	SchedulingMessage string

	// ZonalVolumes reports that the subject's objects claim zone-bound
	// volumes. It corroborates volume pinning and never decides it: the pinned
	// counts do that, and a caller that cannot answer this question leaves it
	// false without changing any cause.
	ZonalVolumes bool

	// RolloutEndedAt is when the subject's most recent rollout completed.
	// A rollout still in progress is §7.6's business, not attribution's.
	RolloutEndedAt time.Time
}

// CauseConfig tunes the attribution rules.
type CauseConfig struct {
	// Window is how far back an event may be and still be offered as the
	// cause of drift happening now. §8.5's worked example reports over two
	// hours, which is the default.
	Window time.Duration

	// SettleWindow is how long after a rollout ends before its bias counts as
	// permanent. Below it the drift may still be recovering on its own, which
	// is the relaxation §7.6 already applies; `rollout_bias` is what is left
	// when that relaxation lapsed and the drift stayed.
	SettleWindow time.Duration

	// PinnedShare is the fraction of the relocation distance that must be
	// unmovable before pinning is called the cause. Half means "most of what
	// would have to move, can't", which is the point at which the finding's
	// only remedy stops being a rescheduling decision.
	PinnedShare float64
}

// DefaultCauseConfig returns §8.5's numbers.
func DefaultCauseConfig() CauseConfig {
	return CauseConfig{
		Window:       2 * time.Hour,
		SettleWindow: 10 * time.Minute,
		PinnedShare:  0.5,
	}
}

// Normalized is the config with its defaults filled in, for a caller that
// needs to read a window rather than only pass one back in. The source sizes
// the ready-count history it keeps off Window, and sizing it off a zero would
// leave attribution comparing a domain against nothing at all.
func (c CauseConfig) Normalized() CauseConfig { return c.normalize() }

func (c CauseConfig) normalize() CauseConfig {
	d := DefaultCauseConfig()
	if c.Window <= 0 {
		c.Window = d.Window
	}
	if c.SettleWindow <= 0 {
		c.SettleWindow = d.SettleWindow
	}
	if c.PinnedShare <= 0 {
		c.PinnedShare = d.PinnedShare
	}
	return c
}

// Attribution is §8.5's `suspectedCause` and `contributingFactors`, plus the
// per-domain `note`.
type Attribution struct {
	// Cause is the single best explanation. Exactly one wins.
	Cause SuspectedCause

	// Factors is the evidence, which is not singular. Several of these rules
	// can hold at once — a zone can lose nodes while the pods that remain are
	// pinned to it — and the loser still gets a line, because a reader
	// deciding what to do needs the whole picture and not just the headline.
	//
	// The order is fixed and independent of which cause won, so two findings
	// on the same subject an hour apart diff cleanly.
	Factors []string

	// Notes is index-aligned with the Scores' Domains, empty string where
	// there is nothing to say. This is the payload's per-domain `note`.
	Notes []string
}

// Attribute runs §8.5's rules engine over evidence we already hold.
//
// The ladder is: the under-filled side first, most specific mechanism
// downwards, with capacity shortfall as their catch-all; then the over-filled
// side; then history; then policy. Drift has two ends and the causes are not
// interchangeable between them — every rule here but pinning asks about a
// domain holding *less* than its share.
//
// Ordering the first four by specificity is what keeps the answer useful.
// Consolidation, an outage and a shortfall all show up as a domain that lost
// ready nodes, and "the autoscaler removed it" is a different remedy from
// "the zone is broken" even though the number that moved is the same.
// Policy comes last for the opposite reason: `constraint_ignored` says nothing
// malfunctioned and you permitted this, which is only the answer once no
// mechanism is available to be the answer instead.
func (s *Scores) Attribute(intent *Intent, ev Evidence, now time.Time, c CauseConfig) Attribution {
	c = c.normalize()
	if s == nil {
		return Attribution{Cause: CauseUnknown}
	}

	under, over := s.fillSides()
	a := Attribution{Cause: CauseUnknown, Notes: make([]string, len(s.Domains))}

	switch {
	case s.outage(ev, under):
		a.Cause = CauseDomainOutage
	case s.consolidated(ev, under, now, c):
		a.Cause = CauseConsolidation
	case s.tainted(ev, under, now, c):
		a.Cause = CauseTaintExclusion
	case s.shortfall(ev, under, now, c):
		a.Cause = CauseCapacityShortfall
	case s.pinned(ev, over, c):
		a.Cause = CauseVolumePinning
	case rolloutBias(ev, now, c):
		a.Cause = CauseRolloutBias
	case s.constraintIgnored(intent):
		a.Cause = CauseConstraintIgnored
	}

	a.Factors = s.factors(intent, ev, under, over, now, c)
	s.notes(ev, a.Notes, now, c)
	return a
}

// fillSides returns which domain indices hold more than their share and which
// hold less. A domain exactly on its expectation is on neither side.
//
// Expected can legitimately be absent — Score leaves it unset when the
// apportionment did not line up — and an attribution with no fill sides falls
// through every domain rule to the subject-wide ones, which is the right
// answer rather than an error: we still know a rollout happened.
func (s *Scores) fillSides() (under, over []int) {
	if len(s.Expected) != len(s.Actual) {
		return nil, nil
	}
	for i := range s.Actual {
		switch {
		case s.Actual[i] < s.Expected[i]:
			under = append(under, i)
		case s.Actual[i] > s.Expected[i]:
			over = append(over, i)
		}
	}
	return under, over
}

func (s *Scores) facts(ev Evidence, i int) DomainFacts {
	return ev.Domains[s.Domains[i]]
}

// outage is nodes that are still there and are broken: an under-filled domain
// in which the NotReady nodes are at least half the census.
//
// The distinction from a shortfall is *presence*, not magnitude, and §8.5's
// own worked example is what settles it — a domain falling 12 → 3 with nothing
// NotReady is attributed there to `domain_capacity_shortfall`, because the
// nine nodes did not break, they left. So the table's two signals for an
// outage, "sharp ready-node drop" and "NotReady nodes in one domain", are one
// condition seen twice: when nine of twelve nodes go NotReady the ready count
// falls sharply *and* the NotReady census passes half. Testing the census is
// the form that also answers when we have no history to have watched the fall,
// because the process started after the zone was already down.
//
// "At least half" is §7.6's line, reused rather than re-tuned: the two ask the
// same question of the same numbers — one to decide whether the counts can be
// trusted, one to decide what to blame — and a finding that suppressed at one
// threshold and attributed at another would be incoherent. That the same
// condition does both is not a conflict. §7.6's suppression is windowed; when
// it lapses and the domain is still down, the finding fires and this is why.
func (s *Scores) outage(ev Evidence, under []int) bool {
	for _, i := range under {
		f := s.facts(ev, i)
		if f.NotReadyNodes > 0 && f.NotReadyNodes >= f.ReadyNodes {
			return true
		}
	}
	return false
}

// shortfall is an under-filled domain that either lost ready nodes or cannot
// take the objects it is short of.
//
// It is the catch-all of the under-filled side, which is why it sits below the
// three specific mechanisms rather than above them: it fires on the same
// ready-node fall consolidation does, and means only that nothing told us who
// removed the nodes.
//
// Neither clause is individually required. A cluster that was always too small
// has no fall to point at, and a zone scaled down out from under a workload
// produces no FailedScheduling if the pods never got recreated; both are
// capacity shortfalls, and both are answers a reader can act on.
func (s *Scores) shortfall(ev Evidence, under []int, now time.Time, c CauseConfig) bool {
	if len(under) == 0 {
		return false
	}
	if ev.InsufficientResource > 0 {
		return true
	}
	for _, i := range under {
		f := s.facts(ev, i)
		if f.PeakReadyNodes > f.ReadyNodes && within(f.LostAt, now, c.Window) {
			return true
		}
	}
	return false
}

func (s *Scores) tainted(ev Evidence, under []int, now time.Time, c CauseConfig) bool {
	for _, i := range under {
		if within(s.facts(ev, i).TaintedAt, now, c.Window) {
			return true
		}
	}
	return false
}

func (s *Scores) consolidated(ev Evidence, under []int, now time.Time, c CauseConfig) bool {
	for _, i := range under {
		if within(s.facts(ev, i).ConsolidatedAt, now, c.Window) {
			return true
		}
	}
	return false
}

// pinned asks whether most of the relocation distance is unmovable.
//
// The test is against R, the number of objects that would have to move, and
// not against the raw pinned count: ten pinned pods in a domain holding two
// hundred explain nothing, and two pinned pods are the whole story when only
// three need to move. Surplus pinned objects are the ones that matter, so the
// count is taken over the over-filled side.
func (s *Scores) pinned(ev Evidence, over []int, c CauseConfig) bool {
	if s.Relocation <= 0 {
		return false
	}
	var n int64
	for _, i := range over {
		n += s.facts(ev, i).Pinned
	}
	return float64(n) >= c.PinnedShare*float64(s.Relocation)
}

// rolloutBias is a rollout that finished, had time to settle, and left the
// drift behind. All three clauses are load-bearing: a rollout in progress is
// suppressed by §7.6 and never reaches here, one inside its settle window may
// still be converging, and one older than the window is too far back to blame.
func rolloutBias(ev Evidence, now time.Time, c CauseConfig) bool {
	if !within(ev.RolloutEndedAt, now, c.Window) {
		return false
	}
	return now.Sub(ev.RolloutEndedAt) >= c.SettleWindow
}

// constraintIgnored is the subject having declared a skew bound and then told
// the scheduler to place the pod anyway when it could not be met.
//
// Nothing malfunctioned here, which is exactly the finding: the spread the
// subject asked for is not the spread it configured Kubernetes to enforce.
func (s *Scores) constraintIgnored(intent *Intent) bool {
	if intent == nil || intent.MaxSkew == nil {
		return false
	}
	return intent.WhenUnsatisfiable == v1.ScheduleAnyway && s.ObservedSkew > int64(*intent.MaxSkew)
}

// maxFactorDomains caps how many per-domain lines reach the finding. A
// forty-zone cluster losing nodes everywhere is one story, not forty, and the
// three worst-hit domains tell it.
const maxFactorDomains = 3

// factors builds §8.5's contributingFactors in a fixed order.
//
// The zero cases are reported too — §8.5's worked example carries "0 pods
// pinned by zonal volumes" in a finding that is not about pinning, because a
// ruled-out hypothesis is information. It is what turns "we think it was
// capacity" into "we think it was capacity, and here is what it wasn't".
func (s *Scores) factors(intent *Intent, ev Evidence, under, over []int, now time.Time, c CauseConfig) []string {
	var out []string

	for _, i := range s.droppedDomains(ev, now, c) {
		f := s.facts(ev, i)
		out = append(out, fmt.Sprintf("%s ready node count fell %d → %d at %s",
			s.Domains[i], f.PeakReadyNodes, f.ReadyNodes, f.LostAt.UTC().Format(time.RFC3339)))
	}
	for _, i := range under {
		if f := s.facts(ev, i); f.NotReadyNodes > 0 {
			out = append(out, fmt.Sprintf("%s has %d NotReady node(s)", s.Domains[i], f.NotReadyNodes))
		}
	}
	if ev.InsufficientResource > 0 {
		line := fmt.Sprintf("%d pods Pending with FailedScheduling", ev.InsufficientResource)
		if ev.SchedulingMessage != "" {
			line += ": " + ev.SchedulingMessage
		}
		out = append(out, line)
	}
	for _, i := range under {
		if f := s.facts(ev, i); within(f.TaintedAt, now, c.Window) {
			out = append(out, fmt.Sprintf("%s became ineligible via a taint at %s",
				s.Domains[i], f.TaintedAt.UTC().Format(time.RFC3339)))
		}
	}
	for _, i := range under {
		if f := s.facts(ev, i); within(f.ConsolidatedAt, now, c.Window) {
			out = append(out, fmt.Sprintf("%s lost a node to autoscaler consolidation at %s",
				s.Domains[i], f.ConsolidatedAt.UTC().Format(time.RFC3339)))
		}
	}

	out = append(out, pinnedFactor(s.pinnedCount(ev, over), ev.ZonalVolumes))

	if within(ev.RolloutEndedAt, now, c.Window) {
		out = append(out, fmt.Sprintf("rollout completed %s ago and the drift did not recover",
			shortDur(now.Sub(ev.RolloutEndedAt).Round(time.Minute))))
	}
	if intent != nil && intent.MaxSkew != nil && intent.WhenUnsatisfiable == v1.ScheduleAnyway {
		out = append(out, fmt.Sprintf("whenUnsatisfiable: ScheduleAnyway with maxSkew %d and observed skew %d",
			*intent.MaxSkew, s.ObservedSkew))
	}
	return out
}

func pinnedFactor(n int64, zonal bool) string {
	line := fmt.Sprintf("%d pods pinned by zonal volumes", n)
	if zonal {
		line += " (subject claims zone-bound volumes)"
	}
	return line
}

func (s *Scores) pinnedCount(ev Evidence, over []int) int64 {
	var n int64
	for _, i := range over {
		n += s.facts(ev, i).Pinned
	}
	return n
}

// droppedDomains returns the indices of domains that lost ready nodes inside
// the window, worst fall first, capped. Ties break on domain name so the list
// is stable across evaluations.
func (s *Scores) droppedDomains(ev Evidence, now time.Time, c CauseConfig) []int {
	var idx []int
	for i := range s.Domains {
		f := s.facts(ev, i)
		if f.PeakReadyNodes > f.ReadyNodes && within(f.LostAt, now, c.Window) {
			idx = append(idx, i)
		}
	}
	sort.Slice(idx, func(x, y int) bool {
		a, b := s.facts(ev, idx[x]), s.facts(ev, idx[y])
		da, db := a.PeakReadyNodes-a.ReadyNodes, b.PeakReadyNodes-b.ReadyNodes
		if da != db {
			return da > db
		}
		return s.Domains[idx[x]] < s.Domains[idx[y]]
	})
	if len(idx) > maxFactorDomains {
		idx = idx[:maxFactorDomains]
	}
	return idx
}

// notes fills the payload's per-domain `note`, which exists so a reader
// scanning the domain table sees why one row is short without cross-
// referencing the factor list.
func (s *Scores) notes(ev Evidence, out []string, now time.Time, c CauseConfig) {
	for i := range s.Domains {
		f := s.facts(ev, i)
		if drop := f.PeakReadyNodes - f.ReadyNodes; drop > 0 && within(f.LostAt, now, c.Window) {
			out[i] = fmt.Sprintf("%d nodes lost in last %s", drop, shortDur(c.Window))
		}
	}
}

// within reports whether an event happened inside the window ending now.
//
// A zero time is not within any window — it means the event never happened,
// and the year 1 is otherwise simply very long ago. A *future* timestamp is
// accepted, on the same clock-skew reasoning as §7.6's `recent`: a node whose
// taint is stamped a few seconds ahead of us did not stop being the cause.
func within(at, now time.Time, window time.Duration) bool {
	if at.IsZero() {
		return false
	}
	age := now.Sub(at)
	return age < window && age > -window
}

// shortDur renders a round duration the way a human writes it — "2h", not
// "2h0m0s". Anything that is not round falls back to the standard form.
func shortDur(d time.Duration) string {
	switch {
	case d >= time.Hour && d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d >= time.Minute && d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	}
	return d.String()
}
