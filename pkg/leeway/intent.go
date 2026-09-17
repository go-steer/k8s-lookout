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
	case SourceClusterDefaultDeclared:
		return "cluster-default-declared"
	case SourceClusterDefaultAssumed:
		return "cluster-default-assumed"
	case SourcePodAntiAffinityPreferred:
		return "pod-anti-affinity-preferred"
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

	// Expectation shaping.
	Weighting       Weighting
	EligibleDomains sets.Set[Domain]
	DomainCaps      map[Domain]int64

	Confidence Confidence
	Evidence   []EvidenceItem
}

// HardContract reports whether this intent carries an enforceable maxSkew —
// a DoNotSchedule constraint with a skew bound.
//
// This is the Tier A gate. An assumed cluster default can never satisfy it,
// per §8.1: not because such a default cannot say DoNotSchedule, but because
// raising a critical finding against a constraint nobody told us about would
// be asserting a contract we invented.
func (i *Intent) HardContract() bool {
	if i == nil || i.MaxSkew == nil {
		return false
	}
	if i.WhenUnsatisfiable != v1.DoNotSchedule {
		return false
	}
	return i.Source != SourceClusterDefaultAssumed
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
			winners[c.TopologyKey] = &cp
			continue
		}
		cur.Evidence = append(cur.Evidence, demote(&c)...)
	}
	return winners
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
