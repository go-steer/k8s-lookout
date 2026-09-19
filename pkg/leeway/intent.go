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
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// IntentMode is what the subject is trying to do on a topology key.
type IntentMode uint8

// The intent modes. Ignore is not the absence of intent — it is a positive
// statement that drift on this key is uninteresting, which is how a Karpenter
// pool that consolidates on purpose (§15 item 8) stops generating findings
// without being excluded from the subsystem entirely.
const (
	ModeSpread IntentMode = iota
	ModeColocate
	ModeIgnore
)

// String implements fmt.Stringer.
func (m IntentMode) String() string {
	switch m {
	case ModeSpread:
		return "Spread"
	case ModeColocate:
		return "Colocate"
	case ModeIgnore:
		return "Ignore"
	default:
		return "Unknown"
	}
}

// IntentSource is where an intent came from.
//
// The constants are declared in precedence order, highest first, and
// precedence is implemented as `<` on the value. That is deliberate: the
// design (§5.1) states precedence as an ordered list, and encoding it as a
// separate lookup table would let the table and the list drift apart. Here the
// declaration *is* the table, and TestIntentSource_PrecedenceOrder pins it so
// that inserting a source in the wrong place fails rather than silently
// reordering resolution.
type IntentSource uint8

// Intent sources, highest precedence first.
const (
	SourcePolicyCRD IntentSource = iota
	SourceWorkloadAnnotation
	SourceTopologySpreadConstraint
	SourcePodAntiAffinityRequired
	// SourcePodAffinityRequired and SourcePodAffinityPreferred are a Phase 3
	// addition: §5.1 as first written listed only the anti-affinity halves,
	// but FR-6 requires colocation intent from podAffinity and that intent has
	// to say where it came from. Each sits immediately below its anti-affinity
	// counterpart, so a subject declaring both on one key — a contradiction the
	// scheduler resolves by refusing to place the pod — resolves here to the
	// spread reading, with the colocation retained as evidence.
	SourcePodAffinityRequired
	// SourceClusterDefaultDeclared and SourceClusterDefaultAssumed are one
	// source in kube-scheduler and two here, because spike S4 confirmed the
	// scheduler's configuration is not readable on managed GKE. "The cluster
	// default is X" is therefore sometimes an operator's assertion and
	// sometimes our assumption, and collapsing those into one value would
	// lose exactly the distinction that keeps a guess from raising a
	// critical finding. See §13 S4.
	SourceClusterDefaultDeclared
	SourceClusterDefaultAssumed
	SourcePodAntiAffinityPreferred
	SourcePodAffinityPreferred
	SourceLearnedBaseline
)

// String implements fmt.Stringer. These spellings reach the `source` label on
// lookout.leeway.intent_info (§8.4), so they are kebab-case rather than Go
// identifiers and changing one is a dashboard-visible change.
func (s IntentSource) String() string {
	switch s {
	case SourcePolicyCRD:
		return "policy-crd"
	case SourceWorkloadAnnotation:
		return "workload-annotation"
	case SourceTopologySpreadConstraint:
		return "topology-spread-constraint"
	case SourcePodAntiAffinityRequired:
		return "pod-anti-affinity-required"
	case SourcePodAffinityRequired:
		return "pod-affinity-required"
	case SourceClusterDefaultDeclared:
		return "cluster-default-declared"
	case SourceClusterDefaultAssumed:
		return "cluster-default-assumed"
	case SourcePodAntiAffinityPreferred:
		return "pod-anti-affinity-preferred"
	case SourcePodAffinityPreferred:
		return "pod-affinity-preferred"
	case SourceLearnedBaseline:
		return "learned-baseline"
	default:
		return "unknown"
	}
}

// Outranks reports whether s takes precedence over other.
func (s IntentSource) Outranks(other IntentSource) bool { return s < other }

// Confidence is how much we trust that the intent is what the user meant.
type Confidence uint8

// Confidence levels.
//
// Assumed sits between Inferred and Learned and exists because of S4: an
// intent derived from a cluster default we were never told is not inferred
// from anything the user wrote, but it is not learned from behaviour either.
// §8.1 caps an Assumed intent at Tier B, so this value is not descriptive —
// it changes what severity the subject can reach.
const (
	ConfidenceDeclared Confidence = iota
	ConfidenceInferred
	ConfidenceAssumed
	ConfidenceLearned
)

// String implements fmt.Stringer. As with IntentSource, these reach the
// `confidence` metric label.
func (c Confidence) String() string {
	switch c {
	case ConfidenceDeclared:
		return "declared"
	case ConfidenceInferred:
		return "inferred"
	case ConfidenceAssumed:
		return "assumed"
	case ConfidenceLearned:
		return "learned"
	default:
		return "unknown"
	}
}

// Weighting is how expected share is apportioned across eligible domains.
type Weighting uint8

// The weightings. Equal is the default for workloads; AllocatableCPU is the
// default for node-group subjects and for workloads whose eligible domains
// have materially unequal capacity (§7.2).
const (
	WeightEqual Weighting = iota
	WeightNodeCount
	WeightAllocatableCPU
	WeightAllocatableMemory
)

// String implements fmt.Stringer.
func (w Weighting) String() string {
	switch w {
	case WeightEqual:
		return "Equal"
	case WeightNodeCount:
		return "NodeCount"
	case WeightAllocatableCPU:
		return "AllocatableCPU"
	case WeightAllocatableMemory:
		return "AllocatableMemory"
	default:
		return "Unknown"
	}
}

// EvidenceItem records one observation that contributed to an intent, so a
// finding can explain itself without the reader going back to the cluster.
type EvidenceItem struct {
	Source  IntentSource
	Detail  string
	Ignored bool // true when a lower-precedence source was retained but not applied
}

// Intent is the normalised expression of what a subject wants on one topology
// key, resolved from whatever source supplied it.
//
// Normalising into one shape regardless of source is what lets the scoring
// engine have a single input type, and it is why this package can be tested
// with no cluster at all: a TopologySpreadConstraint, a policy CRD and an
// anti-affinity rule are three very different objects that produce the same
// five fields here.
type Intent struct {
	TopologyKey TopologyKey
	Mode        IntentMode
	Source      IntentSource

	// Hard contract, when one exists (from a TopologySpreadConstraint).
	// Nil means the source expressed no contract, which is different from a
	// contract of zero.
	MaxSkew           *int32
	MinDomains        *int32
	WhenUnsatisfiable v1.UnsatisfiableConstraintAction

	// MaxPerDomain is a uniform ceiling on how many of the subject's objects
	// one domain may hold, which is the contract a *required* podAntiAffinity
	// expresses: at most one per domain, on the term's topology key.
	//
	// It is not a MaxSkew of one. A skew bound of one is satisfied by two pods
	// in every domain, which the anti-affinity forbids outright; and going the
	// other way, a required anti-affinity holds the observed skew at one by
	// construction, so a MaxSkew check against it could never fire. The
	// violation this field exists to catch is the IgnoredDuringExecution half
	// — a pod placed legally, then a node relabelled so two of them share a
	// domain after the fact.
	//
	// Nil means no ceiling. Callers turn it into the per-domain caps
	// Apportion takes once the eligible domain set is known; §7.1 is what
	// decides how many domains there are, and inference runs before that.
	MaxPerDomain *int64

	// Expectation shaping.
	Weighting       Weighting
	EligibleDomains sets.Set[Domain]
	DomainCaps      map[Domain]int64

	// ExplicitShares is §10.1's expectedDistribution: the operator naming the
	// split they want rather than a rule for deriving one. Only a policy can
	// express it — nothing in the Kubernetes API says "40/40/20" — so every
	// other source leaves it nil, and nil is what makes Weighting apply.
	//
	// The values are relative weights, not percentages and not counts. They
	// are normalised over whichever named domains turn out to be eligible, so
	// a declaration that names a zone the cluster has since drained keeps
	// working on the zones that remain instead of expecting objects where none
	// can go. Read it through Eligibility.WeightsFor, never directly.
	ExplicitShares map[Domain]float64

	// Policies are the node-inclusion policies that shape the eligible set
	// (§7.1). Only a TopologySpreadConstraint can express them, so every other
	// source leaves them empty — and empty means "kube-scheduler's default",
	// not "Ignore". Read them through EligibilityPolicies, never directly.
	Policies NodeInclusionPolicies

	Confidence Confidence
	Evidence   []EvidenceItem
}

// EligibilityPolicies returns the node-inclusion policies to compute
// eligibility with, substituting kube-scheduler's defaults for anything the
// intent did not state.
//
// The substitution lives here rather than at each parse site because the zero
// value of NodeInclusionPolicy is the empty string, and EligibleDomains reads
// "not Honor" as "do not apply this". An intent from a source that cannot
// express a policy — which is every source but a TSC — would therefore silently
// stop honouring nodeSelector: the eligible set would widen to the whole
// cluster and every pinned workload would look like it was drifting. One place
// to get it right, and a nil receiver answers too, because "no intent at all"
// still has to compute an eligible set.
func (i *Intent) EligibilityPolicies() NodeInclusionPolicies {
	out := DefaultNodeInclusionPolicies()
	if i == nil {
		return out
	}
	if i.Policies.NodeAffinityPolicy != "" {
		out.NodeAffinityPolicy = i.Policies.NodeAffinityPolicy
	}
	if i.Policies.NodeTaintsPolicy != "" {
		out.NodeTaintsPolicy = i.Policies.NodeTaintsPolicy
	}
	return out
}

// HardContract reports whether this intent carries a ceiling Kubernetes itself
// refuses to exceed — a DoNotSchedule TSC with a skew bound, or a required
// podAntiAffinity's per-domain ceiling. §8.1 names both as Tier A.
//
// This is the Tier A gate. An assumed cluster default can never satisfy it,
// per §8.1: not because such a default cannot say DoNotSchedule, but because
// raising a critical finding against a constraint nobody told us about would
// be asserting a contract we invented.
//
// A required podAffinity is deliberately not a hard contract even though the
// scheduler enforces it just as hard. §8.1's Tier A list is about placement
// Kubernetes promised and did not deliver; "these pods ended up further apart
// than the affinity would have put them" is a deviation to explain, not a
// broken promise, because the affinity was satisfied at every placement.
func (i *Intent) HardContract() bool {
	return i.HardPerDomainContract() || i.HardSkewContract()
}

// HardPerDomainContract reports the required-podAntiAffinity half of
// HardContract: a ceiling on how many objects one domain may hold. When it
// answers true, MaxPerDomain is non-nil.
func (i *Intent) HardPerDomainContract() bool {
	return i.declaredContract() && i.MaxPerDomain != nil
}

// HardSkewContract reports the DoNotSchedule-maxSkew half of HardContract.
// When it answers true, MaxSkew is non-nil.
//
// The two halves are separate predicates because they bound different
// quantities — a count in one domain, and the spread between the fullest and
// emptiest — and the rule that checks one must not be reached by an intent
// carrying only the other.
func (i *Intent) HardSkewContract() bool {
	return i.declaredContract() && i.MaxSkew != nil && i.WhenUnsatisfiable == v1.DoNotSchedule
}

// declaredContract is the precondition both halves share: there is an intent,
// and somebody other than us asserted it.
func (i *Intent) declaredContract() bool {
	return i != nil && i.Source != SourceClusterDefaultAssumed
}

// ResolveIntents reduces a set of candidate intents to at most one per
// topology key, applying §5.1 precedence.
//
// Intents on *different* keys coexist — a workload can legitimately want to
// spread across zones and colocate within a rack. On the same key the
// highest-precedence source wins and the losers are retained as evidence
// rather than discarded, because "we saw your preferred anti-affinity and the
// TSC overrode it" is the explanation that stops a finding from looking wrong.
//
// Ties (two candidates from the same source on the same key) are resolved by
// keeping the first in input order, so the caller controls the outcome and the
// result does not depend on map iteration.
func ResolveIntents(candidates []Intent) map[TopologyKey]*Intent {
	if len(candidates) == 0 {
		return map[TopologyKey]*Intent{}
	}

	winners := make(map[TopologyKey]*Intent, len(candidates))
	for idx := range candidates {
		c := candidates[idx]
		cur, ok := winners[c.TopologyKey]
		if !ok {
			cp := c
			winners[c.TopologyKey] = &cp
			continue
		}
		if c.Source.Outranks(cur.Source) {
			// The incumbent loses. Carry its evidence forward before
			// replacing it, or the explanation dies with it.
			cp := c
			cp.Evidence = append(cp.Evidence, demote(cur)...)
			carryContract(&cp, cur)
			winners[c.TopologyKey] = &cp
			continue
		}
		cur.Evidence = append(cur.Evidence, demote(&c)...)
		carryContract(cur, &c)
	}
	return winners
}

// carryContract preserves a scheduler-enforced ceiling that the winning source
// does not itself express.
//
// Precedence decides whose *description* of the intent wins. A hard contract is
// not a description: a DoNotSchedule maxSkew, or a required podAntiAffinity's
// one-per-domain ceiling, is a promise Kubernetes made at admission time, and
// it stayed true no matter who else had an opinion. Letting a higher-precedence
// source erase it downgrades a Tier A violation — placement Kubernetes
// guaranteed and did not deliver — into an ordinary distributional observation,
// silently, which is the failure direction this subsystem is least able to
// notice.
//
// The case that surfaced this is FR-10's: a policy CRD outranks everything and
// carries no maxSkew, so a subject with both would have lost its contract the
// moment an operator declared a policy.
//
// Only the DoNotSchedule skew bound carries, and deliberately not a required
// podAntiAffinity's MaxPerDomain. Both are equally real promises, and since
// Phase 4 the scoring rules do compare the ceiling against the observed
// per-domain maximum rather than laundering it through ExcessSkew — so the
// reason for the asymmetry is no longer that the ceiling cannot be reported
// honestly. It is that the ceiling is not only a reporting input: MaxPerDomain
// becomes a per-domain cap on the apportionment once the eligible set is known,
// and the expectation is precisely what an overriding source is entitled to
// redefine. Carrying the bound would let a superseded anti-affinity reshape the
// winner's expected distribution, which is the same objection MinDomains loses
// on below. The ceiling stays on its own demoted intent, where the evidence
// trail still carries it and nothing over-claims.
//
// Two further limits. ModeIgnore takes precedence over the carry: an operator
// who declared a key uninteresting has said so about the whole key, and
// re-admitting a contract through the back door would make Ignore mean "ignore,
// except when it matters most". And MinDomains does not carry, unlike MaxSkew —
// it shapes the expectation by adding synthetic domains, and the expectation is
// precisely what the overriding source is entitled to redefine.
func carryContract(winner, losing *Intent) {
	if winner == nil || losing == nil || winner.Mode == ModeIgnore {
		return
	}
	if winner.MaxSkew != nil || losing.MaxSkew == nil {
		return
	}
	if losing.Source == SourceClusterDefaultAssumed || losing.WhenUnsatisfiable != v1.DoNotSchedule {
		return
	}
	winner.MaxSkew = losing.MaxSkew
	winner.WhenUnsatisfiable = losing.WhenUnsatisfiable
}

// demote turns a losing candidate into evidence: its own evidence trail plus a
// marker for the candidate itself, all flagged Ignored.
func demote(losing *Intent) []EvidenceItem {
	out := make([]EvidenceItem, 0, len(losing.Evidence)+1)
	out = append(out, EvidenceItem{
		Source:  losing.Source,
		Detail:  "superseded by higher-precedence source",
		Ignored: true,
	})
	for _, e := range losing.Evidence {
		e.Ignored = true
		out = append(out, e)
	}
	return out
}

// SortedKeys returns the topology keys of a resolved intent set in
// lexicographic order, so callers iterating the result are deterministic.
func SortedKeys(intents map[TopologyKey]*Intent) []TopologyKey {
	out := make([]TopologyKey, 0, len(intents))
	for k := range intents {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
