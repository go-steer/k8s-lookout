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

import "strings"

// The parsers here are the inverse of the String methods, for the one caller
// that needs to read the model's vocabulary back off a document a human wrote:
// the §10.1 policy CRD. They live beside the enums rather than with that
// decoder so that the round-trip is testable exhaustively — TestParse_RoundTrip
// walks every declared constant, and a new Mode or Weighting with no parse arm
// fails the build's tests rather than silently rejecting a valid policy.
//
// Matching is on a folded form (lower-cased, hyphens and underscores removed)
// for one reason worth stating: IntentSource.String is kebab-case because it
// reaches a metric label, while Kubernetes API enums are CamelCase, so the
// same concept has two correct spellings depending on where you read it. An
// operator copying `topology-spread-constraint` off a dashboard into a CRD is
// not making a mistake worth an error message. The CRD's own enum lists the
// CamelCase spelling, which is what validation pins.

// fold normalises an enum spelling for comparison.
func fold(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "-", "")
	return strings.ReplaceAll(s, "_", "")
}

// ParseIntentMode reads an IntentMode by name.
func ParseIntentMode(s string) (IntentMode, bool) {
	for _, m := range []IntentMode{ModeSpread, ModeColocate, ModeIgnore} {
		if fold(m.String()) == fold(s) {
			return m, true
		}
	}
	return ModeSpread, false
}

// ParseWeighting reads a Weighting by name.
func ParseWeighting(s string) (Weighting, bool) {
	for _, w := range []Weighting{WeightEqual, WeightNodeCount, WeightAllocatableCPU, WeightAllocatableMemory} {
		if fold(w.String()) == fold(s) {
			return w, true
		}
	}
	return WeightEqual, false
}

// IntentSources returns every intent source in precedence order. It is the
// list a policy's inference allowlist is validated against.
func IntentSources() []IntentSource {
	return []IntentSource{
		SourcePolicyCRD,
		SourceWorkloadAnnotation,
		SourceTopologySpreadConstraint,
		SourcePodAntiAffinityRequired,
		SourcePodAffinityRequired,
		SourceClusterDefaultDeclared,
		SourceClusterDefaultAssumed,
		SourcePodAntiAffinityPreferred,
		SourcePodAffinityPreferred,
		SourceLearnedBaseline,
	}
}

// ParseIntentSource reads an IntentSource by name, in either the kebab-case
// spelling that reaches the metric label or the CamelCase spelling the CRD
// enum uses.
func ParseIntentSource(s string) (IntentSource, bool) {
	for _, src := range IntentSources() {
		if fold(src.String()) == fold(s) {
			return src, true
		}
	}
	return SourcePolicyCRD, false
}

// APIName renders an intent source in the CamelCase spelling the policy CRD's
// `inference.sources` enum uses. It is deliberately not String: that one is
// kebab-case and already on dashboards, and Kubernetes API enums are not.
func (s IntentSource) APIName() string {
	parts := strings.Split(s.String(), "-")
	for i, p := range parts {
		switch p {
		case "":
		case "crd":
			parts[i] = "CRD"
		default:
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, "")
}
