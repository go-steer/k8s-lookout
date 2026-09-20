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
)

// RankRule names the §7.7.4 rule that produced a verdict.
//
// It exists for the same reason BreachKind does: the tier is read off the rule
// that fired rather than re-derived from the numbers. "Rank-0 share is 12 %" is
// a Tier B fact on a two-tier class whose ceiling was set deliberately and says
// nothing at all on a class nobody configured a floor for.
type RankRule uint8

// The rules, in the order JudgeRank returns them.
const (
	RankRuleNone RankRule = iota
	// RankRuleWedged is §7.7.4's Tier A row: pods Pending against a class that
	// has told the autoscaler not to provision outside its priority list.
	RankRuleWedged
	// RankRuleRank0Share is the rank-0 time share falling below a floor.
	RankRuleRank0Share
	// RankRuleLastRank is sustained time at the least-preferred tier.
	RankRuleLastRank
	// RankRuleNoMigration is a class that promised to migrate workloads back
	// to preferred capacity and did not once that capacity returned.
	RankRuleNoMigration
	// RankRuleTierUnused is a whole preference tier that has never been
	// occupied — a dead rung on the ladder, or a reservation being paid for
	// and never drawn on.
	RankRuleTierUnused
)

// String renders a rule for the `rule` metric label and the finding payload.
func (r RankRule) String() string {
	switch r {
	case RankRuleWedged:
		return "wedged"
	case RankRuleRank0Share:
		return "rank0-share"
	case RankRuleLastRank:
		return "last-rank"
	case RankRuleNoMigration:
		return "no-migration"
	case RankRuleTierUnused:
		return "tier-unused"
	default:
		return ""
	}
}

// Kind is the §2.3 finding kind this rule produces.
//
// Two rules share `leeway.rank_degraded` on purpose: §2.3 split the original
// `rank_depth` into `rank_degraded` and `rank_tier_unused` because a dead
// preference level is a different thing from a degraded one, but "too little
// time at the top" and "too much time at the bottom" are two measurements of
// the same complaint and do not want separate names.
func (r RankRule) Kind() string {
	switch r {
	case RankRuleWedged:
		return KindRankWedged
	case RankRuleRank0Share, RankRuleLastRank:
		return KindRankDegraded
	case RankRuleNoMigration:
		return KindRankNoMigration
	case RankRuleTierUnused:
		return KindRankTierUnused
	default:
		return ""
	}
}

// Tier is the §8.1 confidence tier for this rule.
//
// RankRuleTierUnused is Tier C and that needs saying, because it is the one
// place where the tier ladder is being used for urgency rather than for
// confidence. A tier nothing has ever occupied is not a low-confidence
// observation — it is arithmetic on a counter that has stayed at zero. What
// makes it Tier C is §7.7.4's own word for it, "info (cost)": it is a bill
// somebody should look at, not a page. Tier C is the only row that routes to
// severity info, and putting it anywhere else would page on a rung of a ladder
// being unused, which is frequently the correct configuration.
func (r RankRule) Tier() Tier {
	switch r {
	case RankRuleWedged:
		return TierA
	case RankRuleRank0Share, RankRuleLastRank, RankRuleNoMigration:
		return TierB
	case RankRuleTierUnused:
		return TierC
	default:
		return TierNone
	}
}

// RankWindow is one axis's occupancy over an evaluation window: the difference
// between two RankTracker snapshots, plus the transitions counted between them.
//
// A window rather than an instant, because §7.7.3's whole argument is that the
// question is time-weighted. Judging a scrape would make a ninety-second
// scale-up indistinguishable from three weeks on the spot fallback, which is
// the failure the pod-seconds counter exists to prevent — and it would be a
// waste to accumulate them correctly and then score a gauge.
type RankWindow struct {
	// Elapsed is the wall time the window covers. Shares are computed from
	// pod-seconds and not from this, but a window too short to mean anything
	// is refused on it.
	Elapsed time.Duration

	// PodSeconds is what accrued in the window, per rank. Sentinel ranks may
	// appear and are deliberately excluded from every share: a mean achieved
	// rank computed over a bucket whose rank is -2 is not a mean of anything.
	PodSeconds map[Rank]float64

	// Pods is occupancy at the end of the window, per rank.
	Pods map[Rank]int

	// Improving and Worsening count non-lateral node transitions in the
	// window — a move to a better tier and to a worse one respectively.
	// Lateral is carried for the payload and scored by nothing: an
	// equal-score alternative is not a demotion (§7.7.3).
	Improving int64
	Worsening int64
	Lateral   int64
}

// TierSeconds is pod-seconds at real preference tiers, which is the
// denominator every share in here uses.
func (w RankWindow) TierSeconds() float64 {
	var total float64
	for rank, secs := range w.PodSeconds {
		if rank.IsTier() {
			total += secs
		}
	}
	return total
}

// Share is the fraction of tier pod-seconds spent at one rank, and 0 for an
// axis that accrued none. Zero-over-zero is "we have nothing to say", not "no
// time at this rank" — callers gate on TierSeconds before reading a share.
func (w RankWindow) Share(rank Rank) float64 {
	total := w.TierSeconds()
	if total <= 0 {
		return 0
	}
	return w.PodSeconds[rank] / total
}

// MeanRank is the pod-second-weighted mean achieved rank, 0 being ideal. It is
// §7.7.3's second PromQL one-liner, computed here so a finding can quote the
// number the dashboard shows.
func (w RankWindow) MeanRank() float64 {
	total := w.TierSeconds()
	if total <= 0 {
		return 0
	}
	var weighted float64
	for rank, secs := range w.PodSeconds {
		if rank.IsTier() {
			weighted += float64(rank) * secs
		}
	}
	return weighted / total
}

// TierPods counts pods currently occupying real preference tiers.
func (w RankWindow) TierPods() int {
	var n int
	for rank, pods := range w.Pods {
		if rank.IsTier() {
			n += pods
		}
	}
	return n
}

// DegradedPods counts pods currently at any tier worse than the first choice.
func (w RankWindow) DegradedPods() int {
	var n int
	for rank, pods := range w.Pods {
		if rank.IsTier() && rank > 0 {
			n += pods
		}
	}
	return n
}

// RankConditions are the facts about an axis that a window cannot carry,
// because they are not time-weighted occupancy.
type RankConditions struct {
	// PendingPods is how many pods are waiting on this axis with no node —
	// §7.7.4's Tier A input. It is deliberately not part of the window: a
	// Pending pod occupies no rank, and charging it to one would put a pod
	// that got nothing in the same bucket as one that got its first choice.
	PendingPods int

	// Rank0Restored reports that the most preferred tier is occupied now and
	// was not at the previous observation. It is the closest observable thing
	// to "capacity returned", and the no-migration rule is gated on it rather
	// than on rank-0 merely being non-empty: a class where the top tier has
	// always carried some load is not a class that just recovered, and firing
	// on one would report every permanently mixed estate as a stuck migration.
	Rank0Restored bool

	// SinceRestore is how long ago Rank0Restored last became true. S1 measured
	// GKE acting 4 m 17 s after the class edit, so the rule needs a grace
	// period of minutes before it is entitled to conclude that nothing is
	// going to happen.
	SinceRestore time.Duration

	// Lifetime is how long this axis has been observed under its current spec
	// hash. The unused-tier rule needs it, and it is bounded by process uptime
	// — an axis whose tracker was reset by a re-tiering starts again, which is
	// correct, because a re-tiered rank 1 is not the rank 1 that was idle.
	Lifetime time.Duration

	// LifetimeSeconds is cumulative pod-seconds per rank over Lifetime. The
	// unused-tier rule reads it rather than the window, because a tier busy
	// yesterday and idle for the last minute has not been unused for 30 days.
	LifetimeSeconds map[Rank]float64
}

// RankThresholds are §7.7.4's knobs.
//
// The two share rules default to off, and that is the §7.7.5 caveat made
// operative rather than timidity: a class whose rank-1 is a cheaper family may
// be *intended* to carry most of the load, so an absolute depth threshold is a
// statement about one estate's intent that this tool cannot infer. The last
// rank is the exception — see DefaultRankThresholds.
type RankThresholds struct {
	// Rank0ShareFloor fires RankRuleRank0Share when the rank-0 share of window
	// pod-seconds drops below it. Zero disables the rule.
	Rank0ShareFloor float64

	// LastRankShareCeiling fires RankRuleLastRank when the least-preferred
	// tier's share rises above it. Zero disables the rule.
	LastRankShareCeiling float64

	// MinWindow is the shortest window a share is judged over. Below it every
	// share rule abstains: a two-second window in which one pod happened to be
	// at rank 2 reads as a 100 % last-rank share, and the dwell would not save
	// us because the machine would go on being handed that same reading.
	MinWindow time.Duration

	// MigrationGrace is how long after capacity returns the no-migration rule
	// waits before concluding that nothing will move.
	MigrationGrace time.Duration

	// UnusedTierFor is how long a tier must have accrued no pod-seconds at all
	// before it is reported as dead.
	UnusedTierFor time.Duration
}

// DefaultRankThresholds returns the shipped defaults.
//
// LastRankShareCeiling is 0.9 rather than something nearer a half. Nine-tenths
// of an axis's pod-seconds on its least-preferred tier admits only two
// readings: capacity at every better rule is chronically unavailable, or the
// priority list is in the wrong order. Both are findings. A half would also
// catch the class that deliberately runs most of its load on a cheaper
// fallback, which is a configuration and not a fault.
//
// Rank0ShareFloor is 0 — off. It is the same measurement from the other end,
// and on an axis with three or more tiers it fires on estates that are merely
// mixed. An operator who knows their class's intent can set it; we cannot.
func DefaultRankThresholds() RankThresholds {
	return RankThresholds{
		Rank0ShareFloor:      0,
		LastRankShareCeiling: 0.9,
		MinWindow:            5 * time.Minute,
		MigrationGrace:       15 * time.Minute,
		UnusedTierFor:        30 * 24 * time.Hour,
	}
}

// Normalized fills in what a partial RankThresholds leaves out, and clamps the
// two shares into (0, 1].
//
// A share threshold outside that range is not a disabled rule, it is a rule
// that can never fire or can never stop firing, and silently treating one as
// the other is how a policy typo becomes a paging storm.
func (t RankThresholds) Normalized() RankThresholds {
	d := DefaultRankThresholds()
	if t.Rank0ShareFloor < 0 || t.Rank0ShareFloor > 1 {
		t.Rank0ShareFloor = 0
	}
	if t.LastRankShareCeiling < 0 || t.LastRankShareCeiling > 1 {
		t.LastRankShareCeiling = 0
	}
	if t.MinWindow <= 0 {
		t.MinWindow = d.MinWindow
	}
	if t.MigrationGrace <= 0 {
		t.MigrationGrace = d.MigrationGrace
	}
	if t.UnusedTierFor <= 0 {
		t.UnusedTierFor = d.UnusedTierFor
	}
	return t
}

// RankObservation is the arithmetic behind a verdict, carried on every verdict
// whether it breached or not.
//
// The non-breaching case is the one that earns it. "Why is this axis not
// firing" is asked far more often than the other question, and an operator who
// has just set a floor wants to see the share it was compared against.
type RankObservation struct {
	WindowSeconds float64 `json:"windowSeconds"`
	TierSeconds   float64 `json:"tierSeconds"`
	Rank0Share    float64 `json:"rank0Share"`
	LastRank      Rank    `json:"lastRank"`
	LastRankShare float64 `json:"lastRankShare"`
	MeanRank      float64 `json:"meanRank"`
	PendingPods   int     `json:"pendingPods"`
	DegradedPods  int     `json:"degradedPods"`
	Improving     int64   `json:"improving"`
	Worsening     int64   `json:"worsening"`
	Lateral       int64   `json:"lateral"`
}

// RankVerdict is one rule's judgement of one axis.
type RankVerdict struct {
	Rule RankRule

	// Focus is the tier the verdict is about, for the rules that are per-tier
	// rather than per-axis. It is RankUnknown on every axis-wide rule, and it
	// is part of the episode identity: two dead tiers on one class are two
	// findings, because fixing one does not fix the other.
	Focus Rank

	Kind     string
	Tier     Tier
	Severity string
	Breached bool

	// Reason is one line of prose, populated on both outcomes.
	Reason string

	Observed RankObservation
}

// EpisodeKey identifies the dwell timer this verdict belongs to.
//
// Rule and focus, not kind: the two rules that share `leeway.rank_degraded`
// measure different things and a class can start breaching one while it stops
// breaching the other, so folding them into one timer would let a recovery on
// the floor silently extend an episode about the ceiling.
func (v RankVerdict) EpisodeKey() string {
	if v.Focus == RankUnknown {
		return v.Rule.String()
	}
	return v.Rule.String() + "/" + v.Focus.String()
}

// RankInput is everything JudgeRank reads. A struct rather than four
// positional arguments because two of them are maps of the same type.
type RankInput struct {
	// Class is the decoded provider object. Its two policy fields are what
	// §7.7.4 calls the gates, and a nil class means the object did not decode
	// — in which case nothing here is judged, because ranking against rules we
	// are not sure we understood is exactly what the annotation-primary design
	// exists to avoid.
	Class *ComputeClass

	Window     RankWindow
	Conditions RankConditions
}

// JudgeRank applies §7.7.4's rules to one axis.
//
// It returns a verdict for every rule that *applies* to this axis, breached or
// not, and that completeness is load-bearing rather than tidy. The §8.2 machine
// resolves an episode by being handed a non-breaching verdict; a judge that
// returned only the breaches would leave every episode open forever, and the
// axis would go on reporting a condition that ended an hour ago.
//
// A rule that does not apply at all — no-migration on a class that never
// promised to migrate — is returned as a non-breach with the gate as its
// reason, for the same purpose: turning the gate off must close the episode it
// was keeping open, not orphan it.
func JudgeRank(in RankInput, t RankThresholds) []RankVerdict {
	t = t.Normalized()
	if !in.Class.Scorable() {
		// An axis with fewer than two tiers has no fallback to detect and an
		// unorderable one has no tiers at all. Both are reported through
		// axis_info and the SLI gauges; neither can produce a degradation.
		return nil
	}
	axis := in.Class.Axis
	obs := observe(in, axis)

	out := []RankVerdict{
		judgeWedged(in, obs),
		judgeRank0Share(in, obs, t),
		judgeLastRank(in, obs, t),
		judgeNoMigration(in, obs, t),
	}
	out = append(out, judgeTierUnused(in, axis, obs, t)...)
	return out
}

// observe assembles the arithmetic once, so that six rules cannot disagree
// about what the window said.
func observe(in RankInput, axis *PreferenceAxis) RankObservation {
	w := in.Window
	last := axis.LastRank()
	return RankObservation{
		WindowSeconds: w.Elapsed.Seconds(),
		TierSeconds:   w.TierSeconds(),
		Rank0Share:    w.Share(0),
		LastRank:      last,
		LastRankShare: w.Share(last),
		MeanRank:      w.MeanRank(),
		PendingPods:   in.Conditions.PendingPods,
		DegradedPods:  w.DegradedPods(),
		Improving:     w.Improving,
		Worsening:     w.Worsening,
		Lateral:       w.Lateral,
	}
}

// verdict is the common construction. Severity comes from the tier, with no
// escalation path: §8.1's max-domain-share escalation is a topology rule and
// has no analogue here, so a rank verdict's severity is its tier's, always.
func verdict(rule RankRule, focus Rank, breached bool, obs RankObservation, reason string) RankVerdict {
	v := RankVerdict{
		Rule:     rule,
		Focus:    focus,
		Kind:     rule.Kind(),
		Tier:     rule.Tier(),
		Breached: breached,
		Reason:   reason,
		Observed: obs,
	}
	v.Severity = v.Tier.Severity()
	return v
}

// judgeWedged is §7.7.4's Tier A row.
//
// The gate is DoNotScaleUp *declared*, not merely defaulted. GKE's documented
// default is ScaleUpAnyway, so an absent field means the autoscaler will
// provision outside the list and a Pending pod is waiting for a node rather
// than wedged against a policy — a different condition, and one this rule
// would misattribute. §7.7.5 also records that every class on the inspected
// cluster carried DoNotScaleUp without anybody choosing it, which is a reason
// to be careful about what we say, not a reason to treat unset as set.
func judgeWedged(in RankInput, obs RankObservation) RankVerdict {
	switch {
	case in.Class.ScaleUp != ScaleUpPolicyDoNotScaleUp:
		return verdict(RankRuleWedged, RankUnknown, false, obs,
			fmt.Sprintf("class whenUnsatisfiable is %s, so a Pending pod is waiting for capacity rather than wedged against a policy", in.Class.ScaleUp))
	case obs.PendingPods == 0:
		return verdict(RankRuleWedged, RankUnknown, false, obs,
			"no pods are Pending on this class")
	}
	return verdict(RankRuleWedged, RankUnknown, true, obs,
		fmt.Sprintf("%d pod(s) Pending against a DoNotScaleUp class: no priority can be satisfied and the autoscaler will not provision outside the list", obs.PendingPods))
}

// judgeRank0Share is the rank-0 time share falling below a configured floor.
func judgeRank0Share(in RankInput, obs RankObservation, t RankThresholds) RankVerdict {
	switch {
	case t.Rank0ShareFloor <= 0:
		return verdict(RankRuleRank0Share, RankUnknown, false, obs,
			"no rank-0 share floor is configured for this axis")
	case !judgeable(in, obs, t):
		return verdict(RankRuleRank0Share, RankUnknown, false, obs, notJudgeable(in, t))
	case obs.Rank0Share >= t.Rank0ShareFloor:
		return verdict(RankRuleRank0Share, RankUnknown, false, obs,
			fmt.Sprintf("rank-0 share %.2f is at or above the floor %.2f", obs.Rank0Share, t.Rank0ShareFloor))
	}
	return verdict(RankRuleRank0Share, RankUnknown, true, obs,
		fmt.Sprintf("rank-0 share %.2f is below the floor %.2f: first-choice capacity has degraded", obs.Rank0Share, t.Rank0ShareFloor))
}

// judgeLastRank is sustained time at the least-preferred tier.
func judgeLastRank(in RankInput, obs RankObservation, t RankThresholds) RankVerdict {
	switch {
	case t.LastRankShareCeiling <= 0:
		return verdict(RankRuleLastRank, RankUnknown, false, obs,
			"no last-rank share ceiling is configured for this axis")
	case !judgeable(in, obs, t):
		return verdict(RankRuleLastRank, RankUnknown, false, obs, notJudgeable(in, t))
	case obs.LastRankShare <= t.LastRankShareCeiling:
		return verdict(RankRuleLastRank, RankUnknown, false, obs,
			fmt.Sprintf("rank %s share %.2f is at or below the ceiling %.2f", obs.LastRank, obs.LastRankShare, t.LastRankShareCeiling))
	}
	return verdict(RankRuleLastRank, RankUnknown, true, obs,
		fmt.Sprintf("rank %s share %.2f is above the ceiling %.2f: running on last-resort capacity, which on most classes is the spot or the cheapest rule", obs.LastRank, obs.LastRankShare, t.LastRankShareCeiling))
}

// judgeNoMigration is §7.7.4's "active migration is not happening" row.
//
// Three conditions, and the middle one is the one that keeps this honest.
// `optimizeRulePriority` makes the finding meaningful at all — without it
// nothing was ever going to move and a workload sitting on its fallback is the
// design working. Rank-0 having *returned* is what distinguishes a stuck
// migration from an estate that has always been mixed; a class whose top tier
// is permanently half-occupied would otherwise fire forever. And the grace
// period is S1's 4 m 17 s with room: migration is slow, and a rule that
// concluded after thirty seconds would report every successful migration on
// its way through.
func judgeNoMigration(in RankInput, obs RankObservation, t RankThresholds) RankVerdict {
	c := in.Conditions
	switch {
	case !in.Class.OptimizeRulePriority:
		return verdict(RankRuleNoMigration, RankUnknown, false, obs,
			"class does not set activeMigration.optimizeRulePriority, so nothing was going to migrate back")
	case !c.Rank0Restored:
		return verdict(RankRuleNoMigration, RankUnknown, false, obs,
			"the most preferred tier has not come back into use, so there is no returned capacity to migrate to")
	case obs.DegradedPods == 0:
		return verdict(RankRuleNoMigration, RankUnknown, false, obs,
			"no pods remain at a worse tier")
	case c.SinceRestore < t.MigrationGrace:
		return verdict(RankRuleNoMigration, RankUnknown, false, obs,
			fmt.Sprintf("capacity returned %s ago, inside the %s migration grace period", c.SinceRestore.Round(time.Second), t.MigrationGrace))
	case obs.Improving > 0:
		return verdict(RankRuleNoMigration, RankUnknown, false, obs,
			fmt.Sprintf("%d migration(s) to a better tier observed in this window", obs.Improving))
	}
	return verdict(RankRuleNoMigration, RankUnknown, true, obs,
		fmt.Sprintf("%d pod(s) still at a worse tier %s after the most preferred one came back, with no migration to a better tier observed, on a class that declares activeMigration.optimizeRulePriority",
			obs.DegradedPods, c.SinceRestore.Round(time.Second)))
}

// judgeTierUnused reports one verdict per preference tier on the axis.
//
// Per tier and not per axis, because two dead rungs are two facts: one may be a
// reservation nobody draws on and the other a machine family that no longer
// exists in the region, and resolving the first must not close the second.
//
// Lifetime is what makes this measurable at all, and it is bounded by process
// uptime and reset by a re-tiering. That is deliberate rather than a
// limitation: a rank 1 that was renumbered an hour ago has not been idle for
// thirty days, whatever the old counter said.
func judgeTierUnused(in RankInput, axis *PreferenceAxis, obs RankObservation, t RankThresholds) []RankVerdict {
	c := in.Conditions
	out := make([]RankVerdict, 0, axis.Tiers)
	for _, tier := range tiersOf(axis) {
		o := obs
		switch {
		case c.Lifetime < t.UnusedTierFor:
			out = append(out, verdict(RankRuleTierUnused, tier, false, o,
				fmt.Sprintf("this axis has only been observed for %s of the %s an unused tier is judged over", c.Lifetime.Round(time.Minute), t.UnusedTierFor)))
		case c.LifetimeSeconds[tier] > 0:
			out = append(out, verdict(RankRuleTierUnused, tier, false, o,
				fmt.Sprintf("rank %s has accrued %.0f pod-seconds", tier, c.LifetimeSeconds[tier])))
		default:
			out = append(out, verdict(RankRuleTierUnused, tier, true, o,
				fmt.Sprintf("rank %s (%s) has been unoccupied for the whole %s this axis has been observed: a dead preference level, or a reservation being paid for and never drawn on",
					tier, renderTier(axis, tier), c.Lifetime.Round(time.Hour))))
		}
	}
	return out
}

// tiersOf lists an axis's distinct preference tiers in order.
func tiersOf(axis *PreferenceAxis) []Rank {
	seen := map[Rank]struct{}{}
	out := make([]Rank, 0, axis.Tiers)
	for _, rule := range axis.Rules {
		if !rule.Rank.IsTier() {
			continue
		}
		if _, ok := seen[rule.Rank]; ok {
			continue
		}
		seen[rule.Rank] = struct{}{}
		out = append(out, rule.Rank)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// renderTier names the rules at one tier, so a dead-tier finding says which
// hardware nobody is getting rather than only which ordinal.
func renderTier(axis *PreferenceAxis, tier Rank) string {
	var names []string
	for _, rule := range axis.Rules {
		if rule.Rank == tier {
			names = append(names, rule.Render())
		}
	}
	if len(names) == 0 {
		return "no rules"
	}
	out := names[0]
	for _, n := range names[1:] {
		out += " | " + n
	}
	return out
}

// judgeable reports whether the window is long enough and busy enough for a
// share to mean anything.
func judgeable(in RankInput, obs RankObservation, t RankThresholds) bool {
	return in.Window.Elapsed >= t.MinWindow && obs.TierSeconds > 0
}

// notJudgeable says which of the two gates stopped it, because "the window was
// too short" and "nothing was running" lead to different next questions.
func notJudgeable(in RankInput, t RankThresholds) string {
	if in.Window.Elapsed < t.MinWindow {
		return fmt.Sprintf("window %s is shorter than the %s minimum", in.Window.Elapsed.Round(time.Second), t.MinWindow)
	}
	return "no pod-seconds accrued at any preference tier in this window"
}

// RouteRank applies §8.3 to a rank verdict.
//
// It is Verdict.Route's rule with no §7.6 branch. Transient suppression is a
// topology concept — a rollout or a drained node redistributes pods and makes
// a skew look worse than it is — and none of it applies to a preference rank:
// a pod that ran on the spot fallback for an hour ran there, whatever else the
// cluster was doing at the time.
func RouteRank(v RankVerdict, tierCSignals bool) Delivery {
	switch {
	case !v.Breached:
		return Delivery{Reason: "no breach"}
	case v.Tier == TierC && !tierCSignals:
		return Delivery{Reason: "tier C is metrics-only unless enabled by policy"}
	}
	return Delivery{Signal: true, Severity: v.Severity}
}
