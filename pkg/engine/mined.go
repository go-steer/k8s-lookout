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

package engine

import "fmt"

// This file is the third correlation tier (issue #225): keys that are
// DISCOVERED in the window rather than declared in advance.
//
// The first two tiers both require somebody to have anticipated the
// fault class — a topology ancestor modelled in pkg/graph, or an
// ExternalAncestor extractor written for a known external dependency.
// That is a treadmill: every new way for a cluster to break costs a
// code change before its blast radius can be seen as one incident.
//
// Mining inverts it. Every windowed incident carries the values of a
// few nameable attributes; when enough incidents in the window share
// an exact value, that value becomes the storm's key. Nobody had to
// predict which attribute would matter.
//
// # The explainability gate
//
// A mined key may only group incidents if it can SAY why they are one
// incident. That is not decoration — it is the constraint that makes
// this tier safe to page on. "These seven are one incident" is only
// actionable next to "…because they all pull
// gcr.io/proj/sidecar:v2.3". A key mined from an attribute nobody can
// name would suppress six sessions and explain nothing.
//
// The gate is structural rather than advisory: a dimension cannot
// exist without a Name and a Kind, ValidateMinedDimensions rejects the
// list otherwise, and the storm's key renders through the same
// Ancestor.Display() every declared key uses. There is no path to a
// mined storm whose payload cannot state its own reason.
//
// # What this deliberately is NOT
//
// It is not a significance test. There is no baseline of "normal
// co-occurrence" for a cluster to compare a burst against, and
// building one from a sentinel's own short window would mostly measure
// the window. Instead the guard is: an EXACT value match on a specific
// attribute, plus a threshold higher than declared keys use
// (DefaultMinedMin). Sharing one precise image reference or one node
// is already strong evidence of a shared cause; the extra members are
// the price for the key being circumstantial rather than modelled.
//
// A deliberate non-guard: mined keys do NOT require the group to
// dominate the window. Requiring, say, 60% of windowed incidents to
// share the value would suppress a true five-pod finding on a busy
// cluster while doing nothing about the failure mode that actually
// worries us — and that failure mode (two unrelated outages folding
// together) needs them to share an exact attribute value anyway, at
// which point they are plausibly not unrelated.

// MinedDimension is one nameable attribute a burst might have in
// common. Ordered lists of these are searched most-specific first.
type MinedDimension struct {
	// Name identifies the dimension in KeySource ("mined:image") and
	// in metrics. Closed vocabulary, like ExternalAncestor.Name.
	Name string
	// Kind is the Ancestor.Kind of the synthesized key, and the noun
	// the explanation reads with: Ancestor{Kind: "Image"}.Display()
	// renders "Image gcr.io/proj/app:v1". Must be human-readable —
	// this is the explainability gate's teeth.
	Kind string
	// Value extracts this incident's value for the dimension. "" means
	// the attribute is absent from the signal, and the incident simply
	// does not participate in this dimension — never a key of its own,
	// or every signal missing the attribute would correlate together.
	Value func(Signal) string
}

// DefaultMinedMin is the formation threshold for a mined key.
// Deliberately above DefaultStormMin: a declared key means somebody
// modelled this blast radius, a mined one means we noticed a
// coincidence, and the second should have to be a bigger coincidence.
const DefaultMinedMin = 5

// DefaultMinedDimensions is the shipped dimension list, most specific
// first. Each is populated from the SIGNAL, never the topology graph,
// so mining keeps working on a cluster whose index answers nothing —
// which is also what makes it a fallback for the node and owner
// grouping the graph would otherwise provide (issue #225).
//
// Not shipped, and why:
//
//   - owner / ControllerRef: the k8s-events source leaves it empty
//     (populating it needs a Pod GET it does not have in hand), so a
//     dimension over it would never fire. It becomes worth adding the
//     day that field is populated.
//   - labels: the highest-value dimension here in principle
//     (app.kubernetes.io/part-of groups a whole application) and the
//     riskiest — arbitrary operator-chosen keys, unbounded
//     cardinality, and a label shared by half the cluster would key a
//     storm over half the cluster. Wants its own allow-list of label
//     keys before it ships.
var DefaultMinedDimensions = []MinedDimension{
	{
		// The exact image reference. Catches the fleet-wide bad
		// digest — one broken image rolled out everywhere — which no
		// declared key covers: the registry-host key is far coarser
		// and, by design, only fires for retryable failures.
		Name:  "image",
		Kind:  "Image",
		Value: func(sig Signal) string { ref, _ := quotedImageRef(sig.Message); return ref },
	},
	{
		// Event.Source.Host, as kubelet reports it. The graph has a
		// Node ancestor too, and the keys are identical by
		// construction (Ancestor{Kind: "Node"}), so the two agree
		// rather than opening a second storm for the same node — this
		// one just also works when Snapshot.Lookup misses.
		Name:  "node",
		Kind:  "Node",
		Value: func(sig Signal) string { return sig.Node },
	},
	{
		// InvolvedObject.FieldPath, e.g. `spec.containers{istio-proxy}`.
		// Catches one sidecar failing across unrelated workloads.
		Name:  "container",
		Kind:  "Container",
		Value: func(sig Signal) string { return sig.Container },
	},
}

// ValidateMinedDimensions enforces the explainability gate on a
// dimension list: every entry must be able to name itself and to
// render a key a human can read. Returns the first problem found.
func ValidateMinedDimensions(dims []MinedDimension) error {
	seen := make(map[string]bool, len(dims))
	for i, d := range dims {
		if d.Name == "" {
			return fmt.Errorf("mined dimension %d: Name is required (it is the key's attribution)", i)
		}
		if d.Kind == "" {
			return fmt.Errorf("mined dimension %q: Kind is required — a mined key must be able to explain itself", d.Name)
		}
		if d.Value == nil {
			return fmt.Errorf("mined dimension %q: Value is required", d.Name)
		}
		if seen[d.Name] {
			return fmt.Errorf("mined dimension %q: duplicate name", d.Name)
		}
		seen[d.Name] = true
	}
	return nil
}

// MinedKeySource renders the KeySource for a storm keyed on a mined
// dimension. Prefixed so downstream can tell a discovered key from a
// modelled one without a second field: these are the storms whose
// grouping is circumstantial.
func MinedKeySource(dimension string) string { return "mined:" + dimension }

// minedValues snapshots a signal's value for every dimension, so the
// window can be compared later without retaining Signals.
func minedValues(dims []MinedDimension, sig Signal) map[string]string {
	if len(dims) == 0 {
		return nil
	}
	out := make(map[string]string, len(dims))
	for _, d := range dims {
		if v := d.Value(sig); v != "" {
			out[d.Name] = v
		}
	}
	return out
}
