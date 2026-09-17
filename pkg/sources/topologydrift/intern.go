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
	"strings"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// interner deduplicates the two string families that appear once per pod:
// node names and domain values (§5).
//
// The saving is not marginal at the scale this is designed for. A 50k-node
// cluster has 50k distinct node names and perhaps a dozen distinct zones, but
// 200k Placements each holding a node name and a domain per axis. Without
// interning, every pod carries its own copy of a ~40-byte node name that a few
// dozen other pods also carry; with it, each distinct string exists once and a
// Placement holds a pointer-and-length into it. The strings themselves come
// from JSON decoding, so they are freshly allocated per object and share
// nothing by default.
//
// pkg/graph has an interner of its own and §5 suggests sharing it. It is not
// reusable here: it maps "Kind/namespace/name" identity keys to NodeIDs for
// graph node identity, which is a different job from deduplicating label
// values, and it is unexported. Two small tables are the right answer over
// widening a type in another package to fit a caller it was not built for.
//
// Not safe for concurrent use. The owner serialises access — see State, which
// holds one under its own lock, and Inventory, which does the same.
type interner struct {
	names   map[string]string
	domains map[leeway.Domain]leeway.Domain
	tuples  map[string][]leeway.Domain
}

func newInterner() *interner {
	return &interner{
		names:   make(map[string]string),
		domains: make(map[leeway.Domain]leeway.Domain),
		tuples:  make(map[string][]leeway.Domain),
	}
}

// name returns the canonical copy of a node name.
func (i *interner) name(s string) string {
	if s == "" {
		return ""
	}
	if got, ok := i.names[s]; ok {
		return got
	}
	i.names[s] = s
	return s
}

// domain returns the canonical copy of a domain value.
func (i *interner) domain(d leeway.Domain) leeway.Domain {
	if d == "" {
		return ""
	}
	if got, ok := i.domains[d]; ok {
		return got
	}
	i.domains[d] = d
	return d
}

// tuple returns the canonical copy of a whole domains-by-ordinal slice.
//
// This is the interning that actually pays, and it is why Placement.Domains can
// be a slice at all. The number of distinct (zone, pool, …) combinations in a
// cluster is tiny — a handful of zones times a handful of node pools — while
// the number of Placements holding one is the pod count. Interning the tuple
// means 200k pods share a few dozen backing arrays instead of allocating 200k
// of their own, which is the difference between the ~50 B per pod §5 budgets
// for and several times that.
//
// The returned slice is shared and MUST NOT be mutated. Nothing does: a
// Placement's domains are decided when it is built and only ever read
// afterwards, and a node that is relabelled produces a new tuple rather than an
// edit to the old one — which is also what keeps existing placements showing
// where their pods were actually counted.
func (i *interner) tuple(domains []leeway.Domain) []leeway.Domain {
	if len(domains) == 0 {
		return nil
	}
	// NUL cannot appear in a Kubernetes label value, so it cannot make two
	// different tuples collide on one key.
	var b strings.Builder
	for n, d := range domains {
		if n > 0 {
			b.WriteByte(0)
		}
		b.WriteString(string(d))
	}
	key := b.String()
	if got, ok := i.tuples[key]; ok {
		return got
	}
	canonical := make([]leeway.Domain, len(domains))
	for n, d := range domains {
		canonical[n] = i.domain(d)
	}
	i.tuples[key] = canonical
	return canonical
}

// forget drops a node name from the table.
//
// Domains are deliberately never forgotten: there are a handful of them, they
// recur, and a zone that briefly has no nodes is the exact case leeway exists
// to notice. Node names are the opposite — a cluster that has cycled through
// ten thousand preemptible nodes would otherwise hold ten thousand dead strings
// for the life of the process, which is a slow leak rather than a cache.
func (i *interner) forget(s string) {
	delete(i.names, s)
}

// size reports the table sizes, for the state's memory metric and for tests.
func (i *interner) size() (names, domains, tuples int) {
	return len(i.names), len(i.domains), len(i.tuples)
}
