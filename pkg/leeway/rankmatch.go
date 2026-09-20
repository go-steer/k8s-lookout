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

import "sort"

// ruleFieldPriorityScore is in the rule body but is not a constraint on the
// node — it is what orders the rules. Treating it as an unknown field would
// make every scored class unmatchable, which is the one class of class where
// getting attribution right matters most.
const ruleFieldPriorityScore = "priorityScore"

// UnsupportedRule records a rule that inference had to skip, and why.
//
// It exists to be counted. §7.7.2's hazard is that priority rules are
// open-ended — GKE adds fields over time, and the managed Autopilot classes
// already use one this matcher does not model — and a matcher that ignored
// fields it did not understand would match rules it should not, silently
// attributing nodes to the wrong rule and corrupting the cross-check into
// agreement with nothing. So an unmodelled field is neither a match nor a
// miss: it is a dashboard entry.
type UnsupportedRule struct {
	Index int
	// Field names the offending key, dotted for a subfield (gpu.count). It
	// becomes a metric label, so it is the rule's vocabulary and never a
	// value from the cluster.
	Field string
}

// RuleMatch is the result of matching one node against one axis's rules.
type RuleMatch struct {
	// Index is the list index of the matched rule, or -1 when none matched.
	Index int
	// Matched is false when no supported rule accepted the node — which is
	// not an error: on GKE the annotation is the primary path and inference
	// is a fallback and a cross-check.
	Matched bool
	// Ambiguous means more than one supported rule accepted the node, and
	// Index is simply the first. First-match-wins is a choice, not a fact,
	// so the arbitrariness is reported rather than hidden. It is also less
	// harmful than it looks: two overlapping rules that share a tier yield
	// the same rank either way.
	Ambiguous bool
	// Unsupported lists the rules skipped, in list order.
	Unsupported []UnsupportedRule
}

// Match attributes a node to the first rule on this axis that accepts it.
//
// Rules are sparse — {machineFamily: n4} constrains the family and wildcards
// everything else — so an unspecified field is unconstrained and not
// required-empty. A rule with no constraints at all matches every node, which
// is what the provider means by it.
func (a *PreferenceAxis) Match(p NodeProfile) RuleMatch {
	out := RuleMatch{Index: -1}
	if a == nil {
		return out
	}
	for _, rule := range a.Rules {
		switch ok, field := matchRule(rule.Raw, p); {
		case field != "":
			out.Unsupported = append(out.Unsupported, UnsupportedRule{Index: rule.Index, Field: field})
		case ok && !out.Matched:
			out.Index, out.Matched = rule.Index, true
		case ok:
			out.Ambiguous = true
		}
	}
	return out
}

// matchRule evaluates one rule body against a node.
//
// It reports (satisfied, "") for a decision, and ("", field) — well,
// (false, field) — for a rule it declines to judge. Declining is the fail-
// closed answer and covers two cases that look different and behave the same:
// a field this matcher has never heard of, and a field it understands but
// cannot evaluate because the node carries nothing to evaluate it against.
// Both would otherwise resolve to a wrong answer with no way to notice.
func matchRule(raw map[string]any, p NodeProfile) (bool, string) {
	// Sorted so that a rule with two unsupported fields always reports the
	// same one, and the metric does not flap between them on every resync.
	for _, field := range sortedKeys(raw) {
		v := raw[field]
		var ok, evaluable bool

		switch field {
		case ruleFieldPriorityScore:
			continue
		case "machineFamily":
			ok, evaluable = matchString(v, p.MachineFamily)
		case "machineType":
			ok, evaluable = matchString(v, p.InstanceType)
		case "podFamily":
			ok, evaluable = matchString(v, p.PodFamily)
		case "spot":
			ok, evaluable = matchSpot(v, p.Spot)
		case "minCores":
			ok, evaluable = matchAtLeast(v, float64(p.Cores), p.Cores > 0)
		case "minMemoryGb":
			ok, evaluable = matchAtLeast(v, p.MemoryGB, p.MemoryGB > 0)
		case "gpu":
			var sub string
			ok, sub = matchGPU(v, p.Accelerator)
			if sub != "" {
				return false, "gpu." + sub
			}
			evaluable = true
		case "reservations":
			var sub string
			ok, sub = matchReservations(v, p.Reservation)
			if sub != "" {
				return false, "reservations." + sub
			}
			evaluable = true
		default:
			return false, field
		}

		if !evaluable {
			return false, field
		}
		if !ok {
			return false, ""
		}
	}
	return true, ""
}

// matchString compares a rule's scalar against the node's, and declines when
// the node carries no value — an unlabelled node is not a node whose family
// is the empty string.
func matchString(want any, got string) (ok, evaluable bool) {
	s, isString := want.(string)
	if !isString {
		return false, false
	}
	if got == "" {
		return false, false
	}
	return s == got, true
}

// matchSpot compares a rule's spot flag against the node's tri-state.
func matchSpot(want any, got *bool) (ok, evaluable bool) {
	b, isBool := want.(bool)
	if !isBool || got == nil {
		return false, false
	}
	return b == *got, true
}

// matchAtLeast is the minCores / minMemoryGb shape: the node must meet or
// exceed the rule's floor.
func matchAtLeast(want any, got float64, known bool) (ok, evaluable bool) {
	n, isNumber := asFloat(want)
	if !isNumber || !known {
		return false, false
	}
	return got >= n, true
}

// matchGPU compares the rule's accelerator against the node's.
//
// Type only. A rule's gpu.count is not verifiable from the node's labels —
// nothing observed carries it — so a rule that constrains count is declined
// rather than matched on the type alone, which would attribute a 1-GPU node
// to an 8-GPU rule. Declining costs nothing on GKE, where the annotation is
// the primary path; matching wrongly would corrupt the cross-check.
func matchGPU(want any, got string) (ok bool, unsupportedSub string) {
	spec, isMap := want.(map[string]any)
	if !isMap {
		return false, "<malformed>"
	}
	for _, k := range sortedKeys(spec) {
		if k != "type" {
			return false, k
		}
	}
	s, _ := spec["type"].(string)
	if s == "" {
		return false, "type"
	}
	// An absent accelerator label means the node has no GPU, which is a
	// clean miss rather than something we failed to evaluate — unlike a
	// missing machine-family label, absence here is itself the answer.
	return s == got, ""
}

// matchReservations compares the rule's reservation requirement against what
// the node actually consumed.
//
// The pair, never the name: a reservation name is unique only within its
// project, and GKE can consume one shared from another project (S3), so
// comparing names alone would attribute a node to a same-named reservation in
// a project it never touched.
func matchReservations(want any, got ReservationRef) (ok bool, unsupportedSub string) {
	spec, isMap := want.(map[string]any)
	if !isMap {
		return false, "<malformed>"
	}
	for _, k := range sortedKeys(spec) {
		if k != "affinity" && k != "specific" {
			return false, k
		}
	}

	affinity, _ := spec["affinity"].(string)
	switch lowerASCII(affinity) {
	case "any":
		// The rule asks for a reservation and does not care which. A node
		// that consumed none does not satisfy it.
		return !got.Empty(), ""
	case "specific":
		list, isList := spec["specific"].([]any)
		if !isList {
			return false, "specific"
		}
		for _, entry := range list {
			e, isMap := entry.(map[string]any)
			if !isMap {
				return false, "specific"
			}
			name, _ := e["name"].(string)
			project, _ := e["project"].(string)
			if name != "" && name == got.Name && project == got.Project {
				return true, ""
			}
		}
		return false, ""
	default:
		return false, "affinity"
	}
}

// asFloat accepts both numeric shapes a decoded object can hold: an
// unstructured decode gives int64 for whole numbers and float64 otherwise.
func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case int:
		return float64(n), true
	default:
		return 0, false
	}
}

// lowerASCII lowercases without pulling in Unicode case folding, which has no
// business deciding whether a reservation affinity says "specific".
func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

// sortedKeys is the determinism rule of this package applied to rule bodies:
// two evaluations of the same rule must report the same unsupported field.
func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
