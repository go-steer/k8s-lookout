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

// This file is the seam for blast-radius keys that are NOT in the
// topology graph (issue #225).
//
// §7.5 correlation was built on the premise that what incidents share
// is a Kubernetes ancestor: a node, an owner, a mounted ConfigMap, a
// namespace. pkg/graph models exactly that, and the AncestorResolver
// reads it. But a large share of the faults that actually produce
// storms live OUTSIDE the cluster — a registry rate limit, a cloud API
// quota, a DNS resolver, an IAM endpoint. The graph has no vertex for
// any of them, so the first such key (the registry host, #213) was
// added as a hardcoded special case inside Observe. A second one would
// have been a second special case.
//
// An extractor reads the key off the SIGNAL, never off the topology.
// That is a deliberate constraint, not an implementation shortcut:
//
//   - It cannot be sourced from the graph. An external dependency is
//     not in the graph by definition, and even the objects that depend
//     on it are only in the graph while the index is populated and its
//     Lookup matches. The reported incident on #225 correlated on
//     nothing at all — evidence that Ancestors() can return empty on a
//     live cluster — and the registry key kept working there precisely
//     because it never consulted the resolver.
//   - It must be able to decline. The registry key applies only to
//     retryable pull failures: two workloads with two different bad
//     tags on one host are two incidents (see registryAncestor). A
//     graph edge is unconditional and could not express that, which is
//     the second reason these are not vertices.
//
// Adding a fault class is now one entry in DefaultExternalAncestors,
// with no change to the correlator.

// ExternalAncestor is one signal-derived blast-radius key extractor.
type ExternalAncestor struct {
	// Name identifies the extractor as the SOURCE of a storm's
	// correlation key — what to say when asked why these N incidents
	// are one incident. Kept short and stable: it is a metric label
	// and an explanation, so it is a closed vocabulary, not free text.
	Name string
	// Extract returns the blast-radius key for sig, and false when
	// this extractor does not apply to it. Must be pure: the
	// correlator calls it under its lock, once per observed incident.
	Extract func(Signal) (Ancestor, bool)
}

// KeySourceTopology is the KeySource of a storm keyed on a
// Kubernetes ancestor from the topology graph — the §7.5 default, and
// what every storm was before #225.
const KeySourceTopology = "topology"

// KeySourceRegistry names the registry-host extractor.
const KeySourceRegistry = "registry-host"

// DefaultExternalAncestors is the shipped extractor list, in priority
// order: earlier entries outrank later ones AND outrank every topology
// ancestor, because an external dependency spans workloads. Letting an
// owner-chain or namespace candidate win first would shatter one
// cluster-wide incident into per-workload storms — the exact fan-out
// §7.5 exists to prevent.
var DefaultExternalAncestors = []ExternalAncestor{
	{Name: KeySourceRegistry, Extract: registryAncestor},
}

// externalKeys returns the external candidates for sig in extractor
// order, paired with the name of the extractor that produced each.
func externalKeys(extractors []ExternalAncestor, sig Signal) ([]Ancestor, []string) {
	var keys []Ancestor
	var sources []string
	for _, e := range extractors {
		a, ok := e.Extract(sig)
		if !ok {
			continue
		}
		keys = append(keys, a)
		sources = append(sources, e.Name)
	}
	return keys, sources
}
