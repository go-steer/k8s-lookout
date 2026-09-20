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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// Provider names a mechanism that expresses placement preference as an ordered
// fallback list.
//
// GKE custom compute classes are the first one, but the shape is not theirs:
// Karpenter NodePool weights and weighted preferred node affinity say the same
// thing in different words, so nothing below this line mentions GKE.
type Provider string

// ProviderGKEComputeClass is the cloud.google.com/v1 ComputeClass CR (§7.7).
const ProviderGKEComputeClass Provider = "gke-computeclass"

// AxisKey identifies one preference axis. Names are unique per provider and
// not across providers, so the provider is part of the identity rather than
// just a label on it.
type AxisKey struct {
	Provider Provider
	Name     string
}

// String renders an axis key as provider/name.
func (k AxisKey) String() string { return string(k.Provider) + "/" + k.Name }

// OrderingMode says how a set of rules was turned into preference tiers.
//
// OrderingInvalid is the zero value on purpose. An axis whose ordering was
// never established must not read as a valid one, and of the three answers the
// only safe default is "refuse to order these".
type OrderingMode int

// The three ways an axis can be ordered, per §7.7.1's assignRanks.
const (
	// OrderingInvalid means the rules could not be ordered — some carry an
	// explicit score and some do not, so neither list position nor score is
	// the whole answer and inventing one would silently invert preference.
	OrderingInvalid OrderingMode = iota
	// ByListPosition is a class with no scores anywhere: position in
	// spec.priorities is the preference order and Rank == Index.
	ByListPosition
	// ByPriorityScore is a fully-scored class: rules group by descending
	// score, and every rule in a group is a peer at the same rank.
	ByPriorityScore
)

// String renders an ordering mode for the axis_info metric label.
func (m OrderingMode) String() string {
	switch m {
	case ByListPosition:
		return "list-position"
	case ByPriorityScore:
		return "priority-score"
	case OrderingInvalid:
		return "invalid"
	default:
		return "unknown(" + strconv.Itoa(int(m)) + ")"
	}
}

// Rank is a preference tier ordinal: 0 is most preferred, and larger is worse.
//
// It is always derived — from list position or from priorityScore — and never
// read off the wire. §7.7.1 records the measurement that forces this: a live
// class stamped ccc_priority_index: "2" on a node it ranked *first*, because
// the annotation carries rule identity and identity is not preference.
//
// The three non-tier values are negative so that nothing can mistake one for
// the most-preferred tier. That still leaves the zero value of a bare Rank
// meaning "rank 0", which is why callers read a placement through
// RankPlacement.Tier rather than touching the field.
type Rank int

// Ranks that are states rather than tiers.
const (
	// RankUnknown is "we do not know", which is not the same as rank 0 and
	// must never be rendered as it. It covers a node whose class has not
	// synced, a node in the first ~43 s of its life before the annotation is
	// stamped (§7.7.2), and a node no rule matched.
	RankUnknown Rank = -1
	// RankUnsatisfiable is the ccc_no_rule_matching sentinel: the class was
	// edited out from under a running node and now matches none of its rules.
	RankUnsatisfiable Rank = -2
	// RankOffAxis is the ccc_scale_up_anyway sentinel: the node was
	// provisioned outside the priority list entirely, so the class has
	// stopped constraining placement here.
	RankOffAxis Rank = -3
)

// String renders a rank for a metric label, spelling the sentinels rather than
// emitting a negative number into a series nobody can read.
func (r Rank) String() string {
	switch r {
	case RankUnknown:
		return "unknown"
	case RankUnsatisfiable:
		return "unsatisfiable"
	case RankOffAxis:
		return "off-axis"
	default:
		return strconv.Itoa(int(r))
	}
}

// IsTier reports whether this rank is an actual preference tier and so may be
// scored, bucketed or compared. The three sentinels are not.
func (r Rank) IsTier() bool { return r >= 0 }

// PreferenceRule is one entry in an axis's ordered priority list.
type PreferenceRule struct {
	// Index is the position in the provider's list — 0-based, and exactly
	// what a GKE node's ccc_priority_index annotation holds. Identity, never
	// ordering.
	Index int
	// Score is the rule's declared preference, higher meaning *more*
	// preferred — the opposite direction to Index. Nil when the class sets
	// none. GKE requires the field on all rules or none, and enforces that
	// at admission (§7.7.1, spike S2).
	Score *int
	// Rank is the derived preference tier this rule belongs to. Rules
	// sharing a score share a Rank and are peers.
	Rank Rank
	// Raw is the rule as the provider wrote it, carried unmodified into
	// finding evidence and into the spec hash.
	Raw map[string]any
}

// Peer reports whether two rules sit at the same preference tier. A workload
// moving between peers has taken an equally-preferred alternative and has not
// degraded, which is the distinction §7.7.1 exists to preserve.
func (r PreferenceRule) Peer(other PreferenceRule) bool {
	return r.Rank.IsTier() && r.Rank == other.Rank
}

// Render describes a rule compactly enough for the `rule` metric label and a
// finding body: machineFamily=n4, or spot=true,machineFamily=n2.
//
// Deliberately lossy. It is for a human deciding which rule a number is about,
// not for reconstructing the rule — Raw is what carries that.
func (r PreferenceRule) Render() string {
	if len(r.Raw) == 0 {
		return "index=" + strconv.Itoa(r.Index)
	}
	keys := make([]string, 0, len(r.Raw))
	for k := range r.Raw {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]byte, 0, 48)
	for i, k := range keys {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, k...)
		out = append(out, '=')
		out = append(out, renderRuleValue(r.Raw[k])...)
	}
	return string(out)
}

// renderRuleValue flattens one rule field for Render. Scalars print as
// themselves; anything structured collapses to its type, because a rendered
// reservations block would swamp the label it is a hint for.
func renderRuleValue(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case nil:
		return ""
	case map[string]any:
		return "{…}"
	case []any:
		return "[…]"
	default:
		return fmt.Sprintf("%v", t)
	}
}

// PreferenceAxis is one ordered fallback list, with its rules already assigned
// to preference tiers.
//
// Build one with NewPreferenceAxis rather than by literal: Rank, Ordering,
// Tiers and SpecHash are all derived together and a hand-filled axis whose
// ranks disagree with its ordering would score every workload on it wrongly.
type PreferenceAxis struct {
	Key      AxisKey
	SpecHash string
	Rules    []PreferenceRule
	Ordering OrderingMode
	// Tiers is the number of distinct preference levels, which is at most
	// len(Rules) and is 0 for an axis that could not be ordered.
	Tiers int
}

// NewPreferenceAxis assigns preference tiers to rules given in the provider's
// own list order, and hashes the result.
//
// The rules are copied, and their Index is taken from position rather than
// trusted from the input, because Index *is* position: it is what a node's
// annotation will be compared against, and letting a caller set it
// independently would make the two disagree silently.
func NewPreferenceAxis(key AxisKey, rules []PreferenceRule) *PreferenceAxis {
	axis := &PreferenceAxis{Key: key, Rules: make([]PreferenceRule, len(rules))}
	copy(axis.Rules, rules)
	for i := range axis.Rules {
		axis.Rules[i].Index = i
	}

	axis.Ordering = assignRanks(axis.Rules)
	axis.Tiers = countTiers(axis.Rules)
	axis.SpecHash = specHash(axis.Rules)
	return axis
}

// assignRanks fills in each rule's Rank and reports how it did it.
//
// This is §7.7.1. The subtle case is the middle one: scores run the opposite
// direction to list position, and several rules may share a score, so a scored
// class has fewer tiers than rules and its rank order is not its list order.
func assignRanks(rules []PreferenceRule) OrderingMode {
	scored := 0
	for _, r := range rules {
		if r.Score != nil {
			scored++
		}
	}

	switch {
	case len(rules) == 0:
		// Nothing to order. Not invalid — an empty list is a well-formed
		// answer to "what are this axis's priorities", it just has no tiers,
		// so Scorable will decline it on the tier count instead.
		return ByListPosition

	case scored == 0:
		for i := range rules {
			rules[i].Rank = Rank(rules[i].Index)
		}
		return ByListPosition

	case scored == len(rules):
		// Descending, because a higher score is more preferred and rank 0 is
		// the most preferred tier.
		distinct := descendingDistinctScores(rules)
		for i := range rules {
			rules[i].Rank = Rank(sort.Search(len(distinct), func(j int) bool {
				return distinct[j] <= *rules[i].Score
			}))
		}
		return ByPriorityScore

	default:
		// Unreachable against a current GKE API server, which refuses a
		// partially-scored class at admission — S2 saw the rejection, so this
		// is measured and not read off a doc page. Kept because we decode
		// objects we did not admit: an older control plane, a CRD restored
		// from a backup, or another provider reusing the shape. Every rank
		// stays at its zero value, which is why callers must gate on
		// Scorable rather than on the rank alone.
		for i := range rules {
			rules[i].Rank = RankUnknown
		}
		return OrderingInvalid
	}
}

// descendingDistinctScores returns the distinct scores, most preferred first.
// Position in this slice is the tier ordinal.
func descendingDistinctScores(rules []PreferenceRule) []int {
	all := make([]int, 0, len(rules))
	for _, r := range rules {
		all = append(all, *r.Score)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(all)))

	distinct := all[:0]
	for i, s := range all {
		if i == 0 || s != distinct[len(distinct)-1] {
			distinct = append(distinct, s)
		}
	}
	return distinct
}

// countTiers counts distinct preference levels, and reports none for an axis
// whose rules could not be ordered.
func countTiers(rules []PreferenceRule) int {
	seen := make(map[Rank]struct{}, len(rules))
	for _, r := range rules {
		if !r.Rank.IsTier() {
			return 0
		}
		seen[r.Rank] = struct{}{}
	}
	return len(seen)
}

// Scorable reports whether this axis can produce a degradation signal at all.
//
// An axis with fewer than two tiers has no fallback to detect — every node on
// it is at rank 0 by construction — so §7.7.1 excludes it from scoring and
// publishes axis_info only. That is not a corner case: the four GKE-managed
// Autopilot classes each declare exactly one priority, so without this rule
// every GKE cluster would start with four axes of guaranteed-silent noise.
func (a *PreferenceAxis) Scorable() bool {
	return a != nil && a.Ordering != OrderingInvalid && a.Tiers >= 2
}

// LastRank is the least-preferred tier — the one "sustained time at the last
// rank" in §7.7.4 is about. It reports RankUnknown for an axis with no tiers.
func (a *PreferenceAxis) LastRank() Rank {
	if a == nil || a.Tiers == 0 {
		return RankUnknown
	}
	return Rank(a.Tiers - 1)
}

// RuleAt returns the rule at a list index, and false if the index is outside
// the current rule list.
//
// Out of range is expected rather than exceptional: a class can be edited
// while nodes provisioned under the old spec are still running, so a node's
// annotation routinely names a rule that no longer exists. Indexing the slice
// directly is a panic waiting for the first rule deletion.
func (a *PreferenceAxis) RuleAt(index int) (PreferenceRule, bool) {
	if a == nil || index < 0 || index >= len(a.Rules) {
		return PreferenceRule{}, false
	}
	return a.Rules[index], true
}

// specHash fingerprints the ordered rule list, including scores.
//
// Scores are in it deliberately, and it is the easiest thing to get wrong.
// Changing only a priorityScore leaves every rule body byte-identical while
// moving rules between tiers — so a hash over rule bodies alone would let the
// single edit most likely to be made casually slip past the baseline-reset
// guard, and every historical rank sample would quietly start meaning
// something else (§7.7.5).
func specHash(rules []PreferenceRule) string {
	h := sha256.New()
	for _, r := range rules {
		fmt.Fprintf(h, "%d\x00", r.Index)
		if r.Score != nil {
			fmt.Fprintf(h, "%d", *r.Score)
		}
		// The separator goes outside the conditional so that a nil score and
		// a score of zero hash differently. They are different classes: one
		// is ordered by list position, the other by score.
		h.Write([]byte{0})
		h.Write([]byte(canonicalJSON(r.Raw)))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// canonicalJSON renders a decoded rule body deterministically.
//
// encoding/json sorts map keys, which is the whole reason this is a marshal
// and not a fmt: two decodes of the same YAML must hash identically or a
// resync would look like a class edit. The fallback exists because Marshal can
// fail on a value type that cannot appear in a decoded object — and fmt's %#v
// also sorts map keys, so even the unreachable path is stable.
func canonicalJSON(raw map[string]any) string {
	b, err := json.Marshal(raw)
	if err != nil {
		return fmt.Sprintf("%#v", raw)
	}
	return string(b)
}
