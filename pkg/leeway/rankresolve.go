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

// RankSource says where a resolved rank came from.
//
// It is carried on every rank-bearing series so that a cluster where inference
// is quietly doing all the work — because GKE stopped stamping the annotation,
// say — is visible rather than merely still functioning.
type RankSource int

const (
	// SourceNone means nothing could answer. The rank is not a rank.
	SourceNone RankSource = iota
	// SourceNodeAnnotation is the provider telling us which rule it used. It
	// is the primary path and it wins every disagreement.
	SourceNodeAnnotation
	// SourceInferred is our own matcher, used when the annotation is absent.
	SourceInferred
)

// String renders the source for a metric label.
func (s RankSource) String() string {
	switch s {
	case SourceNone:
		return "none"
	case SourceNodeAnnotation:
		return "annotation"
	case SourceInferred:
		return "inferred"
	default:
		return "unknown"
	}
}

// RankOutcome is what the resolver wants counted.
//
// pkg/leeway holds no meter (NFR-10), so the design's `r.metrics.X.Add(1)`
// becomes a value the caller translates. That is not only a purity dodge: it
// makes every outcome a thing a test can assert on directly, and the §7.7.3
// counters are the subsystem's own SLIs, so being able to assert them in a
// pure test is worth more than the brevity of incrementing in place.
type RankOutcome int

const (
	// OutcomeResolved is the ordinary answer: this node sits at this tier.
	OutcomeResolved RankOutcome = iota
	// OutcomeNoRuleMatching is the ccc_no_rule_matching sentinel — the class
	// was edited out from under running capacity.
	OutcomeNoRuleMatching
	// OutcomeOffAxis is the ccc_scale_up_anyway sentinel — the class stopped
	// constraining placement at all.
	OutcomeOffAxis
	// OutcomePending is the ~43 s at the start of every node's life before the
	// annotation is stamped, with inference unable to cover for it. Common and
	// benign; an alarming rate of it is not.
	OutcomePending
	// OutcomeOutOfRange is an index naming a rule the class no longer has.
	OutcomeOutOfRange
	// OutcomeAxisInvalid is an axis that cannot order its own rules, or one
	// that has not synced yet. Either way it can rank nothing.
	OutcomeAxisInvalid
	// OutcomeUnrecognised is an annotation value that is neither an index nor
	// a sentinel we know. See resolveDeclared for why this is not "absent".
	OutcomeUnrecognised
)

// String renders the outcome for a metric label. These spellings are the
// metric suffixes in §7.7.3, so they are API and not cosmetics.
func (o RankOutcome) String() string {
	switch o {
	case OutcomeResolved:
		return "resolved"
	case OutcomeNoRuleMatching:
		return "no-rule-matching"
	case OutcomeOffAxis:
		return "off-axis"
	case OutcomePending:
		return "pending"
	case OutcomeOutOfRange:
		return "out-of-range"
	case OutcomeAxisInvalid:
		return "axis-invalid"
	case OutcomeUnrecognised:
		return "unrecognised"
	default:
		return "unknown"
	}
}

// RankPlacement is where one node sits on one axis, and everything the source
// needs to count about how we got there.
type RankPlacement struct {
	// Rank is the preference tier, or one of the three negative states. It is
	// never a rule index — see §7.7.1 for why conflating them inverts the
	// ordering on any scored class.
	Rank Rank
	// RuleIndex is rule identity: the raw annotation value, or the matcher's
	// choice. -1 when no rule was named. Identity only, never ordering.
	RuleIndex int
	// Source is who answered.
	Source RankSource
	// Outcome is what to count.
	Outcome RankOutcome
	// Disagreed means the annotation and inference named different rules. The
	// annotation still wins; the disagreement is the alarm. Zero of these is
	// §7.7's exit criterion, and a non-zero rate most likely means either GKE
	// changed the annotation's meaning or our rule parsing is wrong.
	Disagreed bool
	// Ambiguous means more than one rule accepted the node.
	Ambiguous bool
	// Unmatched means inference ran and placed the node nowhere.
	//
	// A flag rather than an outcome, because it is not a state the node is in
	// — it is a fact about our matcher, and it can be true while the
	// annotation is answering perfectly well. Counting it only when inference
	// is the sole path would measure it exactly where it does least good: the
	// coverage gap that matters is the one the primary path is hiding.
	// Unsupported, when populated, is the explanation for it.
	Unmatched bool
	// Unsupported lists rules inference had to skip, for the field counter.
	// Populated even when the annotation answered, because the point of the
	// counter is to notice the matcher rotting while the primary path hides it.
	Unsupported []UnsupportedRule
}

// declaredKind is the shape of the annotation's value. Four of these, because
// spike S1 watched one node pass through three of them in thirteen minutes.
type declaredKind int

const (
	declaredAbsent declaredKind = iota
	declaredIndex
	declaredNoRuleMatching
	declaredScaleUpAnyway
	declaredUnrecognised
)

// The two sentinel values GKE stamps in place of an index (S1).
const (
	sentinelNoRuleMatching = "ccc_no_rule_matching"
	sentinelScaleUpAnyway  = "ccc_scale_up_anyway"
)

// declaredRank is a parsed annotation value.
type declaredRank struct {
	kind  declaredKind
	index int
}

// parseDeclared is total over strings, which is the entire point of it.
//
// strconv.Atoi fails on three of the four shapes this field takes, and an
// error branch would discard two of them — but the sentinels are not parse
// failures, they are findings. ccc_no_rule_matching says the class was edited
// while capacity kept running under it; ccc_scale_up_anyway says the class
// stopped constraining placement. Both are precisely the silent degradation
// this subsystem exists to surface.
func parseDeclared(v string) declaredRank {
	switch v {
	case "":
		return declaredRank{kind: declaredAbsent}
	case sentinelNoRuleMatching:
		return declaredRank{kind: declaredNoRuleMatching}
	case sentinelScaleUpAnyway:
		return declaredRank{kind: declaredScaleUpAnyway}
	}
	// Digits only, deliberately: no sign, no whitespace, no leading plus. A
	// negative value is not a stale index, it is a value we do not understand,
	// and routing it to out-of-range would mislabel a GKE-side change as a
	// class edit.
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return declaredRank{kind: declaredUnrecognised}
		}
		n = n*10 + int(c-'0')
		if n > maxSaneRuleIndex {
			// Not an index any class could have. Refusing to keep multiplying
			// also means this loop cannot overflow on a long digit string.
			return declaredRank{kind: declaredUnrecognised}
		}
	}
	return declaredRank{kind: declaredIndex, index: n}
}

// maxSaneRuleIndex bounds what we will read as an index. A ComputeClass with
// ten thousand priorities does not exist; a ten-thousand-digit annotation
// value is something else entirely, and should be reported as such.
const maxSaneRuleIndex = 10000

// RankResolver turns one node's facts into its place on one axis.
//
// It takes a NodeProfile and the raw annotation string rather than the design
// snippet's *v1.Node, because pkg/leeway may not import a cluster object
// (NFR-10). Extracting the profile and reading the annotation are the source's
// job; deciding what they mean is this one's.
type RankResolver struct {
	// Infer runs the matcher as a fallback and a cross-check. Switching it off
	// leaves the annotation as the only path — supported, and measurably
	// worse, since the disagreement counter goes silent with it.
	Infer bool
}

// NewRankResolver returns a resolver with inference on, which is the shipped
// configuration: on GKE the annotation is primary, and inference earns its
// keep entirely by disagreeing with it.
func NewRankResolver() *RankResolver { return &RankResolver{Infer: true} }

// Resolve returns which rule the node was provisioned from and the preference
// tier that rule belongs to.
//
// The annotation supplies rule identity; the tier always comes from
// assignRanks, because with priorityScore the two differ — S2 measured index 2
// on a node its class ranks first.
//
// Running both paths costs one match against a handful of rules and catches
// the two failures that are otherwise invisible: GKE changing the annotation's
// meaning, and our own reading of spec.priorities being wrong.
func (r *RankResolver) Resolve(annotation string, p NodeProfile, axis *PreferenceAxis) RankPlacement {
	declared := parseDeclared(annotation)

	// The sentinels are answered before the axis is consulted, because neither
	// needs a tier to be meaningful: "no rule matches this node" and "this node
	// was provisioned outside the list" are true whatever the ordering is. An
	// axis that cannot order its rules would otherwise swallow the two most
	// interesting things the provider ever tells us.
	switch declared.kind {
	case declaredNoRuleMatching:
		return RankPlacement{Rank: RankUnsatisfiable, RuleIndex: -1, Source: SourceNodeAnnotation, Outcome: OutcomeNoRuleMatching}
	case declaredScaleUpAnyway:
		return RankPlacement{Rank: RankOffAxis, RuleIndex: -1, Source: SourceNodeAnnotation, Outcome: OutcomeOffAxis}
	case declaredUnrecognised:
		// Fail closed rather than falling through to inference. A value we do
		// not recognise most likely means GKE added a third sentinel, and
		// inference would answer confidently about a node the provider has
		// just told us something unusual about. Being loud here is how an
		// undocumented field's change gets noticed at upgrade time.
		return RankPlacement{Rank: RankUnknown, RuleIndex: -1, Source: SourceNone, Outcome: OutcomeUnrecognised}
	}

	// A nil axis is the class not yet synced, and an invalid one is a class
	// that cannot order its own rules (§7.7.1). Both can rank nothing, and
	// both must yield unknown rather than zero.
	if axis == nil || axis.Ordering == OrderingInvalid {
		return RankPlacement{Rank: RankUnknown, RuleIndex: -1, Source: SourceNone, Outcome: OutcomeAxisInvalid}
	}

	var match RuleMatch
	if r.Infer {
		match = axis.Match(p)
	} else {
		match.Index = -1
	}

	out := RankPlacement{
		RuleIndex:   -1,
		Ambiguous:   match.Ambiguous,
		Unmatched:   r.Infer && !match.Matched,
		Unsupported: match.Unsupported,
	}

	switch {
	case declared.kind == declaredIndex:
		out.RuleIndex, out.Source = declared.index, SourceNodeAnnotation
		// The annotation wins, and the disagreement is recorded rather than
		// acted on. We are checking ourselves against the provider, not
		// overruling it. Silence is not dissent: a node inference could not
		// place has not contradicted anything, or the counter would measure
		// our own extractor coverage instead of GKE's behaviour.
		out.Disagreed = match.Matched && match.Index != declared.index
	case match.Matched:
		out.RuleIndex, out.Source = match.Index, SourceInferred
	default:
		// The annotation is absent — every other kind returned above — and
		// inference did not cover for it. Transient for the first ~43 s of
		// every node's life, so this is the common path at node-add and not
		// an anomaly. Stay unknown: defaulting to rank 0 here manufactures a
		// spurious rank-0 → rank-1 fallback forty seconds into the life of
		// every node that lands anywhere else.
		out.Rank, out.Source, out.Outcome = RankUnknown, SourceNone, OutcomePending
		return out
	}

	// A class can be edited while nodes provisioned under the old spec keep
	// running, so an index naming a rule that no longer exists is expected
	// rather than exceptional — and indexing the slice without this check is a
	// panic waiting for a rule deletion.
	rule, ok := axis.RuleAt(out.RuleIndex)
	if !ok {
		out.Rank, out.Outcome = RankUnknown, OutcomeOutOfRange
		return out
	}
	out.Rank, out.Outcome = rule.Rank, OutcomeResolved
	return out
}
