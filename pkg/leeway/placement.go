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

// Placement is where one object was last counted (§5).
//
// It exists so that a delete or a move can be applied without consulting the
// previous version of the object. The delta rules decrement from the stored
// Placement and never from an event's old object, because after a resync or a
// missed event the old object may not reflect what was actually counted, while
// this index does by construction. That is what keeps the counters
// self-consistent when the event stream is not.
//
// One of these exists per pod, so its layout dominates leeway's marginal
// memory (NFR-2): at 200k pods every 32 bytes here costs another 6.4 MiB.
// Domains is a slice indexed by topology-key ordinal rather than a map keyed by
// TopologyKey — the key set is fixed at startup, and a Go map per pod would
// cost roughly 250 B against roughly 50 B for the slice. NodeName and the
// Domain values are expected to be interned by the caller; this package does
// not intern them, because the lifetime of the table belongs with the state
// that owns the placements.
type Placement struct {
	NodeName string
	Domains  []Domain
	State    CountState
	Pinned   bool
}

// Domain returns the placement's domain on the given topology-key ordinal, or
// DomainUnknown when the ordinal is out of range.
//
// Out of range is reachable in one real case: the configured key set changes
// and placements recorded under the old set are still in the index. Returning
// the sentinel rather than panicking keeps that a counting inaccuracy the §6.5
// verifier will repair, instead of a crash in an informer handler.
func (p Placement) Domain(ordinal int) Domain {
	if ordinal < 0 || ordinal >= len(p.Domains) {
		return DomainUnknown
	}
	return p.Domains[ordinal]
}

// Equal reports whether two placements are the same in every respect that
// affects a count.
//
// This is the relevance filter behind §6.3's early return, and it is the reason
// leeway can sit on the shared informer without drowning: most pod updates are
// status churn on fields no distribution depends on, and an unchanged placement
// means there is nothing to apply and no subject to enqueue. Comparing the
// derived placement rather than the objects is what makes that cheap — two
// pods differing only in a container's restart count produce identical
// placements.
func (p Placement) Equal(other Placement) bool {
	if p.NodeName != other.NodeName || p.State != other.State || p.Pinned != other.Pinned {
		return false
	}
	if len(p.Domains) != len(other.Domains) {
		return false
	}
	for i := range p.Domains {
		if p.Domains[i] != other.Domains[i] {
			return false
		}
	}
	return true
}
