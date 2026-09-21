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
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
)

// NodeView is the slice of a Node that eligibility needs.
//
// It is a plain struct rather than a *v1.Node so that this package stays
// testable without constructing API objects, and so the source layer controls
// what is retained from the informer cache — the shared transform
// (internal/watch/transform.go) already strips most of a Node, and eligibility
// must not be the reason something has to be kept.
type NodeView struct {
	Name string

	// Domain is this node's value for the topology key under consideration,
	// already normalised (empty becomes DomainUnknown by NormaliseDomain).
	Domain Domain

	// Ready is the node's Ready condition being true.
	Ready bool

	// Schedulable is the inverse of spec.unschedulable, i.e. not cordoned.
	Schedulable bool

	// MatchesSelector is whether the node satisfies the subject's
	// nodeSelector and required nodeAffinity. Evaluated by the caller: this
	// package owns the eligibility *rule*, the source owns the *predicate*.
	MatchesSelector bool

	// Tolerated is whether the subject tolerates this node's taints.
	// Evaluated by the caller, for the same reason.
	Tolerated bool

	// AllocatableCPU and AllocatableMemory are the node's allocatable
	// amounts, in whatever units the caller keeps them (millicores and bytes
	// for the source).
	//
	// Both are carried unconditionally rather than one field holding
	// "whichever resource the weighting selected", which is what this used to
	// be. §7.2's trigger has to compare CPU capacities in order to *decide*
	// the weighting, so a caller that had to name the weighting before
	// building these would have to compute eligibility, look at the ratio,
	// and then compute it again — twice per axis per subject on the hot path,
	// to answer a question two float64s answer for free.
	AllocatableCPU    float64
	AllocatableMemory float64
}

// NodeInclusionPolicies mirror the TopologySpreadConstraint fields of the same
// names. Defaults match kube-scheduler: node affinity is honoured, taints are
// not.
//
// Honouring affinity by default and ignoring taints by default is not
// symmetric and is not an oversight upstream — a nodeSelector is a statement
// about where the workload *may* go, while a taint is a statement the node
// makes about itself, and a workload with no toleration is usually unaware of
// it. Mirroring the scheduler here matters because the whole purpose of §7.1
// is to compute the same eligible set the scheduler used; diverging from its
// defaults would reintroduce the false positives the section exists to remove.
type NodeInclusionPolicies struct {
	NodeAffinityPolicy v1.NodeInclusionPolicy
	NodeTaintsPolicy   v1.NodeInclusionPolicy
}

// DefaultNodeInclusionPolicies returns kube-scheduler's defaults.
func DefaultNodeInclusionPolicies() NodeInclusionPolicies {
	return NodeInclusionPolicies{
		NodeAffinityPolicy: v1.NodeInclusionPolicyHonor,
		NodeTaintsPolicy:   v1.NodeInclusionPolicyIgnore,
	}
}

// EligibilityOptions controls the §7.1 computation.
type EligibilityOptions struct {
	Policies NodeInclusionPolicies

	// MinDomains comes from the intent. When set and fewer domains are
	// eligible than it requires, the shortfall is represented by padding the
	// result with synthetic present-with-zero domains, matching
	// kube-scheduler.
	MinDomains *int32

	// RequireReady and RequireSchedulable gate on node health. Both default
	// to true via DefaultEligibilityOptions; a node that is cordoned or
	// NotReady cannot receive pods, so counting its domain as eligible would
	// expect pods into a zone that cannot take them.
	RequireReady       bool
	RequireSchedulable bool

	// CapacityRatioTrigger is copied onto the result; see the field there.
	CapacityRatioTrigger float64
}

// DefaultEligibilityOptions returns the defaults described on the fields.
func DefaultEligibilityOptions() EligibilityOptions {
	return EligibilityOptions{
		Policies:             DefaultNodeInclusionPolicies(),
		RequireReady:         true,
		RequireSchedulable:   true,
		CapacityRatioTrigger: CapacityWeightingRatioTrigger,
	}
}

// Eligibility is the outcome of §7.1 for one (subject, topology key).
type Eligibility struct {
	// Domains are the eligible domains in canonical order, including any
	// synthetic domains added to satisfy MinDomains.
	Domains []Domain

	// CapacityCPU and CapacityMemory are the summed allocatable amounts per
	// domain, index-aligned with Domains. Synthetic domains have zero of
	// both.
	CapacityCPU    []float64
	CapacityMemory []float64

	// CapacityRatioTrigger is §7.2's max/min allocatable-CPU ratio above
	// which an undeclared weighting becomes AllocatableCPU. Zero means
	// CapacityWeightingRatioTrigger.
	//
	// It is a §7.2 knob living on a §7.1 result, which is a seam in the wrong
	// place on paper and the right one in practice: this is where the
	// capacities it compares are, and threading it through WeightsFor instead
	// would put a tuning parameter in the signature of the function every
	// scoring path calls.
	CapacityRatioTrigger float64

	// NodeCount is the number of eligible nodes per domain, index-aligned.
	NodeCount []int64

	// Excluded maps a domain that exists in the cluster but is not eligible
	// to the reason it was dropped. This is what stops eligibility from being
	// an unexplained narrowing: a finding that scores three zones when the
	// cluster has four must be able to say why.
	Excluded map[Domain]string

	// SyntheticDomains is how many present-with-zero domains were added for
	// MinDomains. They have no nodes, so they can never hold a pod — an
	// expectation placed in one is unachievable by construction, and §7.3's
	// S* absorbs that rather than reporting it as drift.
	SyntheticDomains int
}

// NormaliseDomain maps an empty label value to DomainUnknown.
func NormaliseDomain(d Domain) Domain {
	if d == "" {
		return DomainUnknown
	}
	return d
}

// EligibleDomains computes §7.1: the domains containing at least one node the
// subject could actually be scheduled onto.
//
// This single rule removes the largest class of false positives. A workload
// pinned by nodeSelector to two of five zones is not drifting because the
// other three are empty — it is doing exactly what it was told, and scoring it
// against five domains would report permanent drift that no action could fix.
//
// The existential quantifier is the whole rule: one usable node makes a domain
// eligible. It is deliberately not "enough capacity for the expected share",
// because that would fold a capacity question into a placement question and
// make eligibility depend on the replica count it is supposed to be
// apportioning. Capacity is handled downstream, by weighting (§7.2) and caps.
func EligibleDomains(nodes []NodeView, opts EligibilityOptions) Eligibility {
	res := Eligibility{Excluded: make(map[Domain]string)}

	agg := make(map[Domain]*domainAgg)
	for _, n := range nodes {
		d := NormaliseDomain(n.Domain)
		a := agg[d]
		if a == nil {
			a = &domainAgg{}
			agg[d] = a
		}
		a.seen++
		if reason, ok := nodeUsable(n, opts); !ok {
			a.note(reason)
			continue
		}
		a.eligible++
		a.cpu += n.AllocatableCPU
		a.memory += n.AllocatableMemory
	}

	for d, a := range agg {
		if a.eligible == 0 {
			res.Excluded[d] = a.reason()
		}
	}

	for d, a := range agg {
		if a.eligible > 0 {
			res.Domains = append(res.Domains, d)
		}
	}
	SortDomains(res.Domains)

	res.CapacityCPU = make([]float64, len(res.Domains))
	res.CapacityMemory = make([]float64, len(res.Domains))
	res.NodeCount = make([]int64, len(res.Domains))
	for i, d := range res.Domains {
		res.CapacityCPU[i] = agg[d].cpu
		res.CapacityMemory[i] = agg[d].memory
		res.NodeCount[i] = agg[d].eligible
	}
	res.CapacityRatioTrigger = opts.CapacityRatioTrigger

	// kube-scheduler treats a shortfall against minDomains as domains that
	// exist and hold zero, which makes the skew calculation see the gap. We
	// mirror that: without the padding, a workload required to span three
	// zones but eligible for one would show a perfect zero skew across its
	// single domain, and the constraint it is violating would be invisible.
	if opts.MinDomains != nil && int(*opts.MinDomains) > len(res.Domains) {
		want := int(*opts.MinDomains)
		for i := len(res.Domains); i < want; i++ {
			res.Domains = append(res.Domains, syntheticDomain(i))
			res.CapacityCPU = append(res.CapacityCPU, 0)
			res.CapacityMemory = append(res.CapacityMemory, 0)
			res.NodeCount = append(res.NodeCount, 0)
			res.SyntheticDomains++
		}
	}
	return res
}

// syntheticDomain names a present-with-zero placeholder. The names are stable
// for a given shortfall size so that repeated evaluations produce identical
// domain lists and therefore identical apportionments.
func syntheticDomain(i int) Domain {
	return Domain(syntheticPrefix + strconv.Itoa(i))
}

const syntheticPrefix = "__absent__/"

// IsSynthetic reports whether a domain is a MinDomains placeholder rather than
// a real topology value.
func IsSynthetic(d Domain) bool {
	return len(d) > len(syntheticPrefix) && strings.HasPrefix(string(d), syntheticPrefix)
}

// nodeUsable applies the per-node half of §7.1, returning the reason when a
// node is not usable.
//
// The reasons are phrased per node, not per domain, because domainAgg.reason
// may have to join several of them for one domain.
func nodeUsable(n NodeView, opts EligibilityOptions) (string, bool) {
	if opts.Policies.NodeAffinityPolicy == v1.NodeInclusionPolicyHonor && !n.MatchesSelector {
		return "selector or required affinity does not match", false
	}
	if opts.Policies.NodeTaintsPolicy == v1.NodeInclusionPolicyHonor && !n.Tolerated {
		return "tainted against this subject", false
	}
	if opts.RequireReady && !n.Ready {
		return "NotReady", false
	}
	if opts.RequireSchedulable && !n.Schedulable {
		return "cordoned", false
	}
	return "", true
}

type domainAgg struct {
	seen     int64
	eligible int64
	cpu      float64
	memory   float64
	reasons  []string
}

func (a *domainAgg) note(r string) {
	for _, existing := range a.reasons {
		if existing == r {
			return
		}
	}
	a.reasons = append(a.reasons, r)
}

// reason explains why a domain was excluded.
//
// Distinct reasons are joined rather than reduced to the first one. A domain
// with one cordoned node and one NotReady node is excluded for both, and
// reporting only "every node is cordoned" would send the reader to check the
// wrong thing on half the nodes. Order is the order nodeUsable checks in, so
// the string is stable across evaluations.
func (a *domainAgg) reason() string {
	if len(a.reasons) == 0 {
		return "no nodes in this domain"
	}
	return "no usable nodes: " + strings.Join(a.reasons, ", ")
}

// WeightsFor builds the apportionment weights an intent asks for: its
// explicitly declared shares where it has them, and its weighting mode
// otherwise. A nil intent weights every domain equally.
//
// This exists for the same reason EligibilityPolicies does — so no caller has
// to remember which of two fields shapes the expectation. Reading Weighting
// straight off an intent that carries ExplicitShares would silently apportion
// a declared 40/40/20 as an even third each, and the resulting drift would be
// reported against a distribution the operator never asked for.
func (e *Eligibility) WeightsFor(i *Intent) []float64 {
	if i == nil {
		return EqualWeights(len(e.Domains))
	}
	if len(i.ExplicitShares) > 0 {
		out := make([]float64, len(e.Domains))
		for idx, d := range e.Domains {
			// A domain the declaration does not name weights zero: the
			// operator enumerated where the objects belong, and somewhere
			// unnamed is not one of those places. If that leaves every weight
			// at zero — the declaration names no eligible domain at all —
			// Apportion falls back to equal shares rather than placing
			// nothing, because at that point the declaration tells us nothing
			// about the domains that actually exist.
			out[idx] = i.ExplicitShares[d]
		}
		return out
	}
	return e.Weights(e.EffectiveWeighting(i))
}

// EffectiveWeighting resolves what an intent's weighting actually means
// against these eligible domains: §7.2's trigger for an intent that declared
// none, and the declaration itself for one that did.
//
// A nil intent takes the trigger too. That is the Tier C case — a subject
// scored against an apportioned expectation with nobody's intent behind it —
// and it is precisely the population the trigger exists for, because a
// workload that declared a weighting was never going to be apportioned a
// share its zone cannot hold in the first place. The cost is that a nil
// intent has nowhere to record *why* the weighting changed; §8.5's payload
// already reports each domain's ready-node count, so the inequality is
// visible even where the explanation is not.
//
// An intent that bounds object counts — a maxSkew or a per-domain ceiling —
// does not take the trigger either. That is a correctness rule and not a
// courtesy, and it was found by breaking it: capacity weighting feeds
// MinAchievableSkew, which is the floor ExcessSkew is measured from, so a
// capacity-weighted expectation of [8 2] raises the floor to 6 and a declared
// `maxSkew: 1` stops being violable. kube-scheduler bounds *pods*, not
// millicores; a workload whose zones are unequal is exactly the one whose
// `DoNotSchedule` constraint will be violated, and inferring the operator into
// agreement with their cluster is the one thing a contract check must not do.
//
// Never returns WeightAuto: the point of calling this is to be rid of it.
func (e *Eligibility) EffectiveWeighting(i *Intent) Weighting {
	if i != nil {
		if i.Weighting != WeightAuto {
			return i.Weighting
		}
		if i.MaxSkew != nil || i.MaxPerDomain != nil {
			return WeightEqual
		}
	}
	if NeedsCapacityWeightingAt(e.realCapacityCPU(), e.CapacityRatioTrigger) {
		return WeightAllocatableCPU
	}
	return WeightEqual
}

// realCapacityCPU is CapacityCPU without the MinDomains padding.
//
// Excluding it is not tidiness, it is the difference between reporting a
// contract violation and hiding one. Synthetic domains carry zero capacity by
// construction, a zero makes the ratio infinite, and an infinite ratio would
// switch every minDomains-padded subject to capacity weighting — which
// apportions zero to the padding, which produces zero skew, which is exactly
// the gap the padding was added to make visible. A workload required to span
// three zones and eligible for one would come out perfectly balanced.
//
// The padding is always the tail of Domains, so the real set is a prefix.
func (e *Eligibility) realCapacityCPU() []float64 {
	n := len(e.CapacityCPU) - e.SyntheticDomains
	if n < 0 {
		n = 0
	}
	return e.CapacityCPU[:n]
}

// Weights builds the apportionment weights for a weighting mode.
//
// NodeCount and capacity weighting both come straight off the eligibility
// result, which is why Eligibility carries them: recomputing capacity later
// would mean re-walking the node set, and doing it from a different snapshot
// would let the weights disagree with the domain list they weight.
//
// WeightAuto falls through to Equal rather than resolving the trigger itself.
// Auto is not a weighting, it is the absence of one, and a caller that
// reached here still holding it skipped EffectiveWeighting — the honest
// outcome is the conservative expectation, not a silently different one.
func (e *Eligibility) Weights(w Weighting) []float64 {
	switch w {
	case WeightNodeCount:
		out := make([]float64, len(e.NodeCount))
		for i, c := range e.NodeCount {
			out[i] = float64(c)
		}
		return out
	case WeightAllocatableCPU:
		out := make([]float64, len(e.CapacityCPU))
		copy(out, e.CapacityCPU)
		return out
	case WeightAllocatableMemory:
		out := make([]float64, len(e.CapacityMemory))
		copy(out, e.CapacityMemory)
		return out
	default:
		return EqualWeights(len(e.Domains))
	}
}
