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

package watch

import "github.com/go-steer/k8s-lookout/pkg/emit"

// §6.5 on the inject surface (issue #82): pkg/inject and pkg/emit are
// both zero-intra-repo-dep leaves, so inject payloads cannot be
// sanitized at marshal time — the dispatcher masks cluster-sourced
// free text at ASSEMBLY time instead, through these helpers. Every
// assembly site that copies cluster free text (event messages, label
// values) into an inject payload must route it through here.
// Structurally constrained identifiers — object names, namespaces,
// kinds, label keys, fingerprints, timestamps — are NOT masked: k8s
// validation (DNS-1123 / qualified-name / enum shapes) cannot carry
// the credential shapes MaskString catches. Event.reason is NOT in
// that constrained set — it is free-form (raw k8s events + scheduler-
// predicate text) — but it is not a credential surface either; its
// only unbounded-input risk is /metrics label cardinality, handled
// separately by metrics.boundReason (#109), not by masking here.

// maskString applies the §6.5 value-shape heuristics (URL passwords,
// JWTs, auth headers, credential flags, …) to one cluster-sourced
// string. Innocent text passes through byte-identical, so the frozen
// wire pins are unaffected.
func maskString(s string) string { return emit.MaskString(s) }

// maskLabels returns labels with every VALUE masked; keys are
// preserved (never secret-shaped by k8s qualified-name validation).
// nil stays nil for omitempty parity.
func maskLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = emit.MaskString(v)
	}
	return out
}
