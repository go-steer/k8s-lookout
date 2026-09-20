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

import "fmt"

// ScaleUpPolicy is a compute class's answer to "no priority rule can be
// satisfied — now what?".
//
// This is `spec.whenUnsatisfiable` on a ComputeClass, and it is a deliberately
// separate type from the identically named field on a topology spread
// constraint. The two share a field name and nothing else: the spread
// constraint chooses between DoNotSchedule and ScheduleAnyway for one pod, the
// compute class between DoNotScaleUp and ScaleUpAnyway for the whole cluster
// autoscaler. Reusing UnsatisfiableConstraintAction here would put
// "DoNotSchedule" one typo away from a field that has never held it, in a
// package that now holds both concepts.
type ScaleUpPolicy int

const (
	// ScaleUpPolicyUnset is the field absent. GKE's documented default is
	// ScaleUpAnyway, but an absent field and a declared one are different
	// evidence and §7.7.4 gates a finding on which it is.
	ScaleUpPolicyUnset ScaleUpPolicy = iota
	// ScaleUpPolicyAnyway provisions outside the priority list rather than
	// leave the pod Pending. Nodes so provisioned carry ccc_scale_up_anyway.
	ScaleUpPolicyAnyway
	// ScaleUpPolicyDoNotScaleUp leaves pods Pending instead. This is the
	// precondition for leeway.rank_wedged: without it, a class that cannot
	// satisfy any rule degrades rather than stalls.
	ScaleUpPolicyDoNotScaleUp
	// ScaleUpPolicyUnrecognised is a value GKE has that we do not. Kept
	// distinct from unset so a new enum member does not read as a default.
	ScaleUpPolicyUnrecognised
)

// String renders the policy for a metric label.
func (p ScaleUpPolicy) String() string {
	switch p {
	case ScaleUpPolicyUnset:
		return "unset"
	case ScaleUpPolicyAnyway:
		return "scale-up-anyway"
	case ScaleUpPolicyDoNotScaleUp:
		return "do-not-scale-up"
	case ScaleUpPolicyUnrecognised:
		return "unrecognised"
	default:
		return "unknown"
	}
}

// ComputeClass is one decoded provider object: the axis it defines, and the
// two policy fields §7.7.4 needs to decide what a bad rank share means.
type ComputeClass struct {
	// Axis is the preference axis, always non-nil — a class with no
	// priorities yields an axis with no rules rather than a nil one, because
	// "this class exists and constrains nothing" is a state worth exporting.
	Axis *PreferenceAxis
	// ScaleUp is spec.whenUnsatisfiable.
	ScaleUp ScaleUpPolicy
	// OptimizeRulePriority is spec.activeMigration.optimizeRulePriority. When
	// it is off, a workload that fell to a worse rule stays there forever by
	// design, and the finding to raise is that nothing will move it back
	// rather than that something has not yet.
	OptimizeRulePriority bool
}

// Scorable reports whether this class is worth tracking ranks for.
//
// Single-tier classes are skipped, and that is not a corner case: the four
// GKE-managed Autopilot classes each declare exactly one priority, so without
// this every GKE cluster starts with four axes on which every node is rank 0
// by construction — four axes of guaranteed-silent noise.
func (c *ComputeClass) Scorable() bool { return c != nil && c.Axis.Scorable() }

// DecodeComputeClass reads a ComputeClass's `spec` into an axis.
//
// The input is the decoded object's spec map rather than a typed object: the
// CRD is optional and watched dynamically, so nothing here has a Go type to
// unmarshal into, and pkg/leeway would not be allowed to import the provider's
// scheme even if one existed.
//
// It is strict about shape and permissive about content. A spec whose
// priorities are not a list, or whose rules are not objects, is a decode
// failure — we would be guessing at the meaning of an object we cannot read,
// and a half-read class produces a plausible-looking axis that is wrong. An
// unrecognised *field inside* a rule is not a failure at all: that is the
// matcher's problem and it already fails closed on it (see rankmatch.go).
func DecodeComputeClass(name string, spec map[string]any) (*ComputeClass, error) {
	rules, err := decodePriorities(spec["priorities"])
	if err != nil {
		return nil, fmt.Errorf("compute class %q: %w", name, err)
	}

	out := &ComputeClass{
		Axis:    NewPreferenceAxis(AxisKey{Provider: ProviderGKEComputeClass, Name: name}, rules),
		ScaleUp: decodeScaleUpPolicy(spec["whenUnsatisfiable"]),
	}
	if migration, ok := spec["activeMigration"].(map[string]any); ok {
		out.OptimizeRulePriority, _ = migration["optimizeRulePriority"].(bool)
	}
	return out, nil
}

// decodePriorities turns spec.priorities into rules, preserving list order —
// which is what ccc_priority_index indexes into, and is therefore not a
// detail this may normalise away.
func decodePriorities(v any) ([]PreferenceRule, error) {
	if v == nil {
		// A class with no priorities. Legal, and it constrains nothing.
		return nil, nil
	}
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("spec.priorities is %T, want a list", v)
	}

	rules := make([]PreferenceRule, 0, len(list))
	for i, entry := range list {
		body, ok := entry.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("spec.priorities[%d] is %T, want an object", i, entry)
		}
		rule := PreferenceRule{Index: i, Raw: body}
		if raw, present := body[ruleFieldPriorityScore]; present {
			n, ok := asFloat(raw)
			if !ok {
				return nil, fmt.Errorf("spec.priorities[%d].priorityScore is %T, want a number", i, raw)
			}
			// Truncating rather than rejecting a fractional score: the field
			// is an integer in the CRD, so a fraction means something upstream
			// re-encoded it, and the ordering it implies is still the ordering
			// the author wrote.
			score := int(n)
			rule.Score = &score
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

// decodeScaleUpPolicy reads spec.whenUnsatisfiable.
//
// An absent field is reported as unset rather than as GKE's documented default
// of ScaleUpAnyway. §7.7.4 gates leeway.rank_wedged on DoNotScaleUp being
// declared, and a default assumed on our side is not a declaration — the same
// distinction §5.1 draws for cluster defaults, for the same reason.
func decodeScaleUpPolicy(v any) ScaleUpPolicy {
	s, ok := v.(string)
	if !ok || s == "" {
		return ScaleUpPolicyUnset
	}
	switch s {
	case "ScaleUpAnyway":
		return ScaleUpPolicyAnyway
	case "DoNotScaleUp":
		return ScaleUpPolicyDoNotScaleUp
	default:
		return ScaleUpPolicyUnrecognised
	}
}
