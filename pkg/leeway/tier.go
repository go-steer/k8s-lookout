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

// Tier is a finding's confidence tier (§8.1) — how sure we are that what we
// measured is something the user did not want, as opposed to something we
// merely noticed.
//
// The tier is not a severity. It is the question severity is derived from, and
// it is derived from the *breach rule that fired*, not from how large the
// numbers were: a one-pod violation of a declared DoNotSchedule contract is
// Tier A and a fifty-pod distributional wobble on an inferred preference is
// Tier B, because the first is a promise Kubernetes made and broke and the
// second is our reading of what somebody probably meant.
type Tier uint8

// The tiers, most confident first. TierNone is the zero value and means no
// finding, so a Verdict that did not breach is safe to read.
const (
	TierNone Tier = iota
	TierA
	TierB
	TierC
)

// String implements fmt.Stringer. These single letters reach the finding
// payload (§8.5) and the `tier` metric label.
func (t Tier) String() string {
	switch t {
	case TierA:
		return "A"
	case TierB:
		return "B"
	case TierC:
		return "C"
	default:
		return ""
	}
}

// Severity levels, restated from pkg/emit rather than imported.
//
// pkg/emit transitively reaches client-go and NFR-10 forbids that here, so
// these are duplicated strings — the one case in this package where a
// constant exists twice in the tree. TestTierSeverities_MatchEmit in
// pkg/sources/topologydrift, which may import both, holds them equal.
const (
	severityInfo     = "info"
	severityWarning  = "warning"
	severityCritical = "critical"
)

// Severity is the routing level §8.3 assigns the tier.
//
// Tier C's `info` is the floor, not the answer: §8.1 escalates it to `warning`
// on a max-domain-share breach, which is why Judge returns a severity of its
// own rather than callers reading this directly.
func (t Tier) Severity() string {
	switch t {
	case TierA:
		return severityCritical
	case TierB:
		return severityWarning
	case TierC:
		return severityInfo
	default:
		return ""
	}
}

// BreachKind names the rule that fired.
//
// It exists so the tier is read off the rule rather than re-derived from the
// numbers downstream. "ExcessSkew > 0, therefore a contract was violated" is
// true only if a contract existed, and by the time a finding is being rendered
// that context is gone.
type BreachKind uint8

// The breach rules, in the order breach checks them.
const (
	BreachNone BreachKind = iota
	BreachPerDomainCeiling
	BreachMaxSkew
	BreachDrift
	BreachBaseline
)

// String implements fmt.Stringer. Reaches the `rule` metric label.
func (b BreachKind) String() string {
	switch b {
	case BreachPerDomainCeiling:
		return "per-domain-ceiling"
	case BreachMaxSkew:
		return "max-skew"
	case BreachDrift:
		return "drift"
	case BreachBaseline:
		return "baseline"
	default:
		return ""
	}
}

// Contract reports whether the rule that fired was a bound somebody declared,
// which is the §8.1 Tier A gate.
func (b BreachKind) Contract() bool {
	return b == BreachPerDomainCeiling || b == BreachMaxSkew
}

// Verdict is the judgement drawn from one (subject, topology key)'s scores: did
// it breach, under which rule, how confident are we, and how should it route.
//
// A Verdict is a pure function of the scores, the intent and the thresholds. It
// deliberately carries no time: dwell, hysteresis and flap handling operate on
// a sequence of verdicts and are the state machine's job (§8.2), not this
// one's. Keeping them apart is what lets the whole §12 false-positive corpus be
// evaluated without a clock.
type Verdict struct {
	Breached bool
	Kind     BreachKind
	Tier     Tier
	Severity string

	// Reason is one line of prose for a human, and is populated even when
	// Breached is false — "small-n: relocation below floor" is the answer to
	// "why is this subject not firing", which is the question asked far more
	// often than the other one.
	Reason string

	// Escalated records that max domain share crossed its threshold. §8.1 uses
	// it to raise Tier C from info to warning; at A and B the severity is
	// already at or above warning, so it is reported and changes nothing.
	Escalated bool

	// Transient is the §7.6 state in force, if any. It is recorded on both
	// outcomes: a subject that breached *through* a rollout is a more
	// interesting finding than one that breached on a quiet cluster, and a
	// reader cannot tell the two apart from the scores.
	Transient TransientState
	// Suppressed reports that §7.6 declined to evaluate this subject at all,
	// which is not the same as not breaching — Reason says which state.
	Suppressed bool
	// Relaxed reports that the thresholds were multiplied by §7.6's transient
	// multiplier before judging.
	Relaxed bool
}

// Judge applies the §7.3/§7.4 breach rules and the §8.1 tier table.
//
// A nil intent is not an error: it is the Tier C case, a subject with no
// declared or inferred intent at all, and it is judged against whatever
// expectation the caller apportioned.
func (s *Scores) Judge(intent *Intent, t Thresholds) Verdict {
	v := Verdict{}
	v.Kind, v.Reason = s.breach(intent, t)
	v.Breached = v.Kind != BreachNone
	if !v.Breached {
		return v
	}

	v.Tier = classify(intent, v.Kind)
	v.Severity = v.Tier.Severity()

	// Concentration is the goal of a colocation intent, so the share that
	// escalates a spread finding is the thing a colocation subject is trying to
	// achieve. Reporting it as escalation would raise severity on the subjects
	// behaving best.
	if intent == nil || intent.Mode != ModeColocate {
		v.Escalated = s.Escalate(t)
	}
	if v.Escalated && v.Tier == TierC {
		v.Severity = severityWarning
	}
	return v
}

// Delivery is §8.3's decision: does this verdict become a Signal at all, and
// at what severity.
//
// It is a small type because §8.3's table is mostly not ours to implement.
// "Tier A injects, Tier B reaches the watchboard, Tier C is stored only" is
// DESIGN §7.7's existing per-severity routing, which already does exactly
// that for every other source; leeway does not get a second delivery
// mechanism, it gets a severity. Tier B's "inject only if it correlates with
// an active storm" is likewise the storm correlator's job downstream, not a
// branch here.
//
// What is genuinely leeway's is the one row §7.7 cannot express: **Tier C is
// metrics only unless a policy opts in.** Nothing in the severity table says
// "do not emit at all", so that decision has to be taken before emitting, and
// it is the whole of this type.
//
// §8.3's fourth row — tool SLIs, metrics only and never a Signal — needs no
// code either. Those are self-observability counters (§8.4) and never become
// a Verdict, so the rule is enforced by there being no path from one to here.
type Delivery struct {
	// Signal reports whether to emit at all. False means metrics only.
	Signal bool

	// Severity is the DESIGN §7.7 level that decides inject, watchboard or
	// store. Empty when Signal is false.
	Severity string

	// Reason explains a metrics-only outcome. Populated only when Signal is
	// false, because "why did this not page anyone" is the question actually
	// asked of a quiet finding.
	Reason string
}

// Route applies §8.3.
//
// tierCSignals is the policy opt-in. It defaults off, and that default is what
// makes enabling leeway by default defensible: the overwhelming majority of
// subjects that breach anything breach the distributional rule with no
// declared intent behind it, and every one of those stays a metric.
//
// A suppressed verdict (§7.6) is metrics-only regardless of tier, including
// Tier A. That is the zone-outage case in §14's exit criteria: four hundred
// workloads all "violating" their spread contract because a zone went away is
// one fact about the cluster, not four hundred findings about the workloads.
func (v Verdict) Route(tierCSignals bool) Delivery {
	switch {
	case v.Suppressed:
		return Delivery{Reason: v.Reason}
	case !v.Breached:
		return Delivery{Reason: "no breach"}
	case v.Tier == TierC && !tierCSignals:
		return Delivery{Reason: "tier C is metrics-only unless enabled by policy"}
	}
	return Delivery{Signal: true, Severity: v.Severity}
}

// classify implements the §8.1 table.
//
// §8.1's rule that an assumed cluster default can never reach Tier A is not
// restated here. It is enforced one layer down, in declaredContract: an assumed
// intent satisfies neither hard-contract predicate, so no contract rule can
// fire for it and it cannot arrive here with a contract Kind. Writing the cap
// out again would be an unreachable branch asserting something already true —
// TestJudge_AnAssumedClusterDefaultNeverReachesTierA is where the rule is
// stated in a form that can fail.
func classify(intent *Intent, k BreachKind) Tier {
	// No intent, or an intent that is only our reading of the subject's own
	// history, is by definition not a deviation from anything anyone declared.
	if intent == nil || intent.Source == SourceLearnedBaseline {
		return TierC
	}
	if k.Contract() {
		return TierA
	}
	return TierB
}
