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
	"strings"
)

// TopologyKey is a node label key defining a domain axis, such as
// topology.kubernetes.io/zone.
type TopologyKey string

// Domain is a value of a TopologyKey's label.
type Domain string

// DomainUnknown is the domain of a node that carries no value for the key
// under consideration.
//
// Normalising to a sentinel rather than dropping the node is deliberate: an
// unlabelled node still runs pods, and pods on it still constitute a share of
// the workload. Dropping them would make a distribution that does not sum to
// the replica count, and would hide the specific failure — a node pool created
// without the topology label — that most often causes real skew. It is a
// visible domain, and it sorts last (see SortDomains).
const DomainUnknown Domain = "__unknown__"

// SubjectKind is the type of the thing whose distribution is tracked.
type SubjectKind string

// The subject kinds leeway scores. NodeGroup is a node-pool subject (§7,
// phase 7) and Custom covers a user-defined grouping from policy.
const (
	SubjectDeployment  SubjectKind = "Deployment"
	SubjectStatefulSet SubjectKind = "StatefulSet"
	SubjectDaemonSet   SubjectKind = "DaemonSet"
	SubjectJob         SubjectKind = "Job"
	SubjectNodeGroup   SubjectKind = "NodeGroup"
	SubjectCustom      SubjectKind = "Custom"
)

// SubjectRef identifies the thing whose distribution we track.
//
// The JSON tags are §8.5's payload shape. A cluster-scoped subject omits the
// namespace rather than carrying an empty one, so a reader never has to decide
// whether "" means the default namespace.
type SubjectRef struct {
	Kind      SubjectKind `json:"kind"`
	Namespace string      `json:"namespace,omitempty"` // empty for cluster-scoped subjects
	Name      string      `json:"name"`
}

// String renders a subject as kind/namespace/name, with the namespace segment
// omitted for cluster-scoped subjects.
func (s SubjectRef) String() string {
	if s.Namespace == "" {
		return string(s.Kind) + "/" + s.Name
	}
	return string(s.Kind) + "/" + s.Namespace + "/" + s.Name
}

// ParseSubjectRef reads back what String wrote, and reports false for anything
// that is not one of its two shapes.
//
// It exists for §9.1: the persisted alert state is keyed by the rendered
// subject, and a restart has to turn that key back into the subject whose
// distribution it is about. Round-tripping through a string rather than
// persisting three columns keeps the store's primary key one field, and the
// encoding is unambiguous because none of the three segments may contain a
// slash — a Kubernetes name and namespace are DNS labels, and SubjectKind is
// a fixed vocabulary.
//
// A key this build cannot parse is not an error the caller should stop for. It
// is a row written by a different build, and §9.2's rule is that a history we
// cannot read costs one dwell rather than the monitoring.
func ParseSubjectRef(s string) (SubjectRef, bool) {
	parts := strings.Split(s, "/")
	switch len(parts) {
	case 2:
		if parts[0] == "" || parts[1] == "" {
			return SubjectRef{}, false
		}
		return SubjectRef{Kind: SubjectKind(parts[0]), Name: parts[1]}, true
	case 3:
		// The namespace segment is never empty in a three-part key: String
		// omits it entirely for a cluster-scoped subject, so "Kind//name" is
		// something neither half of this package produced.
		if parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return SubjectRef{}, false
		}
		return SubjectRef{Kind: SubjectKind(parts[0]), Namespace: parts[1], Name: parts[2]}, true
	default:
		return SubjectRef{}, false
	}
}

// CountState is the scheduling state an object is counted under. Keeping
// states apart rather than summing them is what lets §7.6 suppress transient
// churn: a distribution that looks skewed only because half of it is
// Terminating is a rollout, not drift.
type CountState uint8

// The states an object can be counted under.
const (
	StateRunning CountState = iota
	StatePending
	StateUnschedulable
	StateTerminating
)

// String implements fmt.Stringer.
func (c CountState) String() string {
	switch c {
	case StateRunning:
		return "running"
	case StatePending:
		return "pending"
	case StateUnschedulable:
		return "unschedulable"
	case StateTerminating:
		return "terminating"
	default:
		return "unknown"
	}
}

// DomainCount is the per-domain tally, split by state.
//
// Pinned is a subset of the other counters rather than a peer of them: a pod
// bound to a zonal volume is also Running. It is tracked because a pinned pod
// cannot move, so it is not relocatable and counting it as drift would produce
// a finding whose only remedy is to delete data.
type DomainCount struct {
	Running       int64
	Pending       int64
	Unschedulable int64
	Terminating   int64
	Pinned        int64
}

// Add increments the counter for state by one.
func (d *DomainCount) Add(state CountState, pinned bool) {
	switch state {
	case StateRunning:
		d.Running++
	case StatePending:
		d.Pending++
	case StateUnschedulable:
		d.Unschedulable++
	case StateTerminating:
		d.Terminating++
	}
	if pinned {
		d.Pinned++
	}
}

// Sub decrements the counter for state by one, mirroring Add exactly.
//
// Mirroring matters more than it looks: the delta rules decrement from a stored
// Placement, so an asymmetry between these two — pinned handled on one side and
// not the other, say — would not show up as a wrong answer on the next event
// but as a counter that drifts a little on every move and is meaningfully wrong
// a week later.
func (d *DomainCount) Sub(state CountState, pinned bool) {
	switch state {
	case StateRunning:
		d.Running--
	case StatePending:
		d.Pending--
	case StateUnschedulable:
		d.Unschedulable--
	case StateTerminating:
		d.Terminating--
	}
	if pinned {
		d.Pinned--
	}
}

// Total returns the number of objects counted in this domain across all
// states. Pinned is excluded because it overlaps the state counters.
func (d *DomainCount) Total() int64 {
	return d.Running + d.Pending + d.Unschedulable + d.Terminating
}

// IsZero reports whether the count holds nothing at all, Pinned included.
func (d *DomainCount) IsZero() bool {
	return *d == DomainCount{}
}

// Distribution is where a subject's objects currently are, on one topology
// key.
type Distribution struct {
	ByDomain map[Domain]*DomainCount
	Total    int64
}

// NewDistribution returns an empty Distribution ready to be added to.
func NewDistribution() *Distribution {
	return &Distribution{ByDomain: make(map[Domain]*DomainCount)}
}

// Add counts one object in the given domain. An empty domain normalises to
// DomainUnknown, so a caller that forgets to normalise cannot silently create
// a second, invisible bucket keyed by "".
func (d *Distribution) Add(domain Domain, state CountState, pinned bool) {
	if domain == "" {
		domain = DomainUnknown
	}
	if d.ByDomain == nil {
		d.ByDomain = make(map[Domain]*DomainCount)
	}
	c := d.ByDomain[domain]
	if c == nil {
		c = &DomainCount{}
		d.ByDomain[domain] = c
	}
	c.Add(state, pinned)
	d.Total++
}

// Remove uncounts one object from the given domain, undoing an Add with the
// same arguments.
//
// A domain whose last object leaves is deleted rather than kept as a zero row.
// That is safe because the eligible domain list comes from the node inventory
// and not from the distribution — Counts already yields zero for a domain the
// distribution has never heard of, which is the case that matters. Keeping the
// rows instead would make a subject that has been rescheduled around the
// cluster over months accumulate a row per domain it ever touched, so this is a
// bound on memory rather than a tidiness preference.
//
// Removing something that was never added is ignored. The delta rules make that
// unreachable by construction, since they decrement from the stored Placement,
// but an informer handler is the wrong place to turn a bookkeeping slip into a
// negative count that every downstream score then inherits.
func (d *Distribution) Remove(domain Domain, state CountState, pinned bool) {
	if domain == "" {
		domain = DomainUnknown
	}
	c, ok := d.ByDomain[domain]
	if !ok {
		return
	}
	c.Sub(state, pinned)
	d.Total--
	if c.IsZero() {
		delete(d.ByDomain, domain)
	}
}

// Clone returns a deep copy, so that a snapshot handed to a reader cannot be
// mutated by the next delta. §6.5's verifier compares one against live counts
// that are still moving underneath it.
func (d *Distribution) Clone() *Distribution {
	out := &Distribution{ByDomain: make(map[Domain]*DomainCount, len(d.ByDomain)), Total: d.Total}
	for dom, c := range d.ByDomain {
		cc := *c
		out.ByDomain[dom] = &cc
	}
	return out
}

// Equal reports whether two distributions hold identical counts. Used by the
// verifier to decide whether a rebuild disagrees with the incremental state.
func (d *Distribution) Equal(other *Distribution) bool {
	if d == nil || other == nil {
		return d == other
	}
	if d.Total != other.Total || len(d.ByDomain) != len(other.ByDomain) {
		return false
	}
	for dom, c := range d.ByDomain {
		oc, ok := other.ByDomain[dom]
		if !ok || *c != *oc {
			return false
		}
	}
	return true
}

// Counts projects the distribution onto the given ordered domain list,
// counting only the states in the supplied set.
//
// Domains absent from the distribution yield zero rather than being skipped:
// an eligible domain holding no pods is the single most important cell in the
// whole calculation, and dropping it would make an empty zone indistinguishable
// from a zone that does not exist.
func (d *Distribution) Counts(domains []Domain, states ...CountState) []int64 {
	out := make([]int64, len(domains))
	for i, dom := range domains {
		c, ok := d.ByDomain[dom]
		if !ok {
			continue
		}
		for _, s := range states {
			switch s {
			case StateRunning:
				out[i] += c.Running
			case StatePending:
				out[i] += c.Pending
			case StateUnschedulable:
				out[i] += c.Unschedulable
			case StateTerminating:
				out[i] += c.Terminating
			}
		}
	}
	return out
}

// Domains returns the distribution's domains in canonical order.
func (d *Distribution) Domains() []Domain {
	out := make([]Domain, 0, len(d.ByDomain))
	for dom := range d.ByDomain {
		out = append(out, dom)
	}
	SortDomains(out)
	return out
}

// SortDomains puts domains into the canonical order used everywhere in this
// package: lexicographic, with DomainUnknown last.
//
// A canonical order is load-bearing rather than cosmetic. Apportionment breaks
// fractional ties by position, so two evaluations that ordered domains
// differently would apportion differently and the resulting finding would flap
// with nothing in the cluster having changed. Sorting unknown last keeps a
// sentinel from displacing real domains in a body that lists the first few.
func SortDomains(domains []Domain) {
	sort.Slice(domains, func(i, j int) bool {
		a, b := domains[i], domains[j]
		if (a == DomainUnknown) != (b == DomainUnknown) {
			return b == DomainUnknown
		}
		return a < b
	})
}
