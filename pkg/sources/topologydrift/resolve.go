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

package topologydrift

import (
	"fmt"
	"math"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// Resolution is everything one evaluation knows about a subject's intended
// placement, before any score is computed from it.
type Resolution struct {
	// Intents is at most one intent per axis, after §5.1 precedence. Axes on
	// which the pod expressed nothing are absent — "no intent" is not an
	// intent, and inventing a neutral one here would put a row on the wire
	// claiming the workload asked for something it did not.
	Intents map[leeway.TopologyKey]*leeway.Intent

	// Eligible is §7.1's eligible set, present for *every* axis the inventory
	// tracks and not only those carrying an intent. A subject with no spread
	// constraint at all still has an eligible set — its nodeSelector and
	// tolerations still say where it may go — and the domains it is scored
	// against are those, not the cluster's.
	Eligible map[leeway.TopologyKey]leeway.Eligibility
}

// ResolveConfig is the cluster-level context an evaluation resolves against —
// everything that is true of the deployment rather than of the pod.
//
// A struct rather than more positional parameters because this is the third
// thing to arrive and it will not be the last: Phase 4 adds thresholds and a
// baseline. Growing a parameter list makes every call site read as a row of
// unlabelled arguments, and the two fields here are both nilable pointers, so
// transposing them would compile.
type ResolveConfig struct {
	// ClusterDefaults is §5.2's three-state cluster default: nil for unknown,
	// a pointer to an empty slice for "declared, and there are none", and a
	// populated slice for declared constraints. The three are not
	// interchangeable — see DescribeClusterDefaults.
	ClusterDefaults *[]corev1.TopologySpreadConstraint

	// Policy is the LeewayPolicy governing this subject, or nil. Nil is the
	// normal case and not a degraded one: the CRD is optional and most
	// clusters never install it.
	Policy *Policy

	// Baselines is §7.5's learned normal, at most one per axis, and nil for a
	// subject with nothing mature yet.
	//
	// It is a candidate like any other rather than a fallback applied after
	// precedence, because §5.1 ranks it: it sits below everything anybody
	// declared — including a preferred affinity — and above the one entry
	// nobody asserted, the assumed cluster default. So it fills a gap and
	// never overrules an answer.
	//
	// That last comparison is the one worth stating out loud, because it is a
	// Phase 5 correction rather than the original order. The assumed defaults
	// apply to precisely the population §7.5 exists for — pods declaring no
	// topologySpreadConstraints — so while the guess outranked the baseline,
	// no cluster that had left --topology-cluster-defaults unset ever scored a
	// subject against a learned intent. See the IntentSource list for the rest.
	Baselines map[leeway.TopologyKey]*leeway.Intent

	// CapacityRatioTrigger is §7.2's max/min allocatable-CPU ratio above
	// which an intent that declared no weighting is apportioned by capacity.
	// Zero means leeway.CapacityWeightingRatioTrigger.
	CapacityRatioTrigger float64
}

// Resolve infers a subject's placement intent from one admitted pod and
// computes the eligible domains that intent is scored against.
//
// **The pod, never the controller's template.** Every inference entry point in
// this package takes a *corev1.Pod for the reason §13 S3 measured: GKE injects
// tolerations at admission that the owning Deployment's PodTemplateSpec does
// not carry. A template is a strictly weaker document than the object the
// scheduler actually placed, and inferring from it computes an eligible set
// that is too small, concludes the workload is pinned, and suppresses drift
// that is really there — the fail-silent direction, which is why there is
// deliberately no template-shaped constructor to reach for.
//
// Candidate order is load-bearing only for ties, which ResolveIntents breaks by
// input order; spread constraints and affinity terms are different sources, so
// §5.1 precedence decides every cross-source case on its own. They are
// concatenated TSC-first anyway so that a reader of the candidate list sees the
// same order §5.1 lists.
func Resolve(pod *corev1.Pod, inv *Inventory, cfg ResolveConfig) Resolution {
	res := Resolution{
		Intents:  map[leeway.TopologyKey]*leeway.Intent{},
		Eligible: map[leeway.TopologyKey]leeway.Eligibility{},
	}
	if pod == nil || inv == nil {
		return res
	}

	candidates := SpreadConstraintIntents(pod)
	candidates = append(candidates, AffinityIntents(pod)...)
	// Last, and it costs nothing to order it so: ClusterDefaultIntents returns
	// nothing at all for a pod that declared any constraint of its own, which
	// is the scheduler's own rule and the reason a wrong assumption about the
	// defaults cannot reach a workload that expressed intent. Filtered to the
	// counted axes, unlike every other source here — see onlyTrackedAxes.
	candidates = append(candidates, onlyTrackedAxes(ClusterDefaultIntents(pod, cfg.ClusterDefaults), inv)...)
	// Below the cluster defaults, which is where §5.1 puts it. Sorted, because
	// map order is random and the candidate list is what evidence is rendered
	// from: an unordered one would make a finding's evidence block reshuffle
	// between passes that saw the same cluster.
	for _, key := range leeway.SortedKeys(cfg.Baselines) {
		if in := cfg.Baselines[key]; in != nil {
			candidates = append(candidates, *in)
		}
	}

	// The allowlist is applied before precedence, not after. A source the
	// operator excluded must not survive as demoted evidence either: evidence
	// is what a finding quotes back to justify itself, and quoting a term the
	// policy said to disregard would make the finding unanswerable. Never
	// filters the policy's own intent — see filterBySources.
	if cfg.Policy != nil {
		candidates = filterBySources(candidates, cfg.Policy.Inference)
		// Appended last so that on a tie ResolveIntents' input order does not
		// decide it; SourcePolicyCRD outranks every other source outright, so
		// position cannot matter, and putting it here keeps the inferred
		// candidates in the order §5.1 lists them.
		candidates = append(candidates, cfg.Policy.Intents()...)
	}
	res.Intents = leeway.ResolveIntents(candidates)

	constraints := ConstraintsOf(pod)
	for _, key := range inv.Keys() {
		intent := res.Intents[key]

		opts := leeway.DefaultEligibilityOptions()
		// Read through the accessor, never off the field: NodeInclusionPolicy's
		// zero value is the empty string and §7.1 reads "not Honor" as "do not
		// apply", so a bare copy of an unset Policies would silently stop
		// honouring nodeSelector and widen every pinned workload's eligible set
		// to the whole cluster. The accessor answers on a nil receiver, which
		// is the case for an axis with no intent.
		opts.Policies = intent.EligibilityPolicies()
		opts.CapacityRatioTrigger = cfg.CapacityRatioTrigger
		if intent != nil {
			opts.MinDomains = intent.MinDomains
		}

		eligible := leeway.EligibleDomains(inv.NodeViews(key, constraints), opts)
		res.Eligible[key] = eligible
		if intent != nil {
			intent.EligibleDomains = sets.New(eligible.Domains...)
			applyWeighting(intent, eligible)
		}
	}
	return res
}

// applyWeighting resolves §7.2's Auto against the domains an intent turned out
// to be eligible for, writing the answer back onto the intent.
//
// The write-back is what keeps one fact from having two values. Weighting is
// already an intent_info label and a §8.5 payload field; leaving Auto on the
// intent and resolving it again inside WeightsFor would export the string
// "Auto" next to an expectation apportioned by CPU, and a reader comparing the
// two would be right to conclude one of them was lying.
//
// An intent that declared a weighting is untouched, including one that
// declared Equal. That is the whole reason Auto exists as a distinct zero
// value: a trigger that overrode a declaration would not be a default.
func applyWeighting(in *leeway.Intent, eligible leeway.Eligibility) {
	if in.Weighting != leeway.WeightAuto {
		return
	}
	in.Weighting = eligible.EffectiveWeighting(in)
	if in.Weighting == leeway.WeightEqual {
		// The common case, and silent on purpose. An evidence line on every
		// homogeneous cluster in the world would say only that nothing
		// happened.
		return
	}
	in.Evidence = append(in.Evidence, leeway.EvidenceItem{
		Source: in.Source,
		Detail: fmt.Sprintf(
			"apportioned by allocatable CPU: eligible domains differ in capacity by %.2f× (over the %.2f× trigger), so an equal expectation would reserve a share the smallest domain cannot hold",
			capacityRatio(eligible), triggerOf(eligible)),
	})
}

// capacityRatio is the max/min allocatable-CPU ratio over the real eligible
// domains — the number the trigger compared. Reported rather than recomputed
// from a second snapshot, for the reason §8.5 gives about evidence: a finding
// that quotes a figure the decision was not made on is worse than one that
// quotes none.
func capacityRatio(e leeway.Eligibility) float64 {
	caps := e.CapacityCPU
	if n := len(caps) - e.SyntheticDomains; n >= 0 {
		caps = caps[:n]
	}
	minC, maxC := math.Inf(1), 0.0
	for _, c := range caps {
		if math.IsNaN(c) || math.IsInf(c, 0) || c < 0 {
			continue
		}
		minC = math.Min(minC, c)
		maxC = math.Max(maxC, c)
	}
	if minC <= 0 || math.IsInf(minC, 1) {
		return math.Inf(1)
	}
	return maxC / minC
}

func triggerOf(e leeway.Eligibility) float64 {
	if e.CapacityRatioTrigger > 0 {
		return e.CapacityRatioTrigger
	}
	return leeway.CapacityWeightingRatioTrigger
}
