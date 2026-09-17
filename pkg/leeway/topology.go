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
type SubjectRef struct {
	Kind      SubjectKind
	Namespace string // empty for cluster-scoped subjects
	Name      string
}

// String renders a subject as kind/namespace/name, with the namespace segment
// omitted for cluster-scoped subjects.
func (s SubjectRef) String() string {
	if s.Namespace == "" {
		return string(s.Kind) + "/" + s.Name
	}
	return string(s.Kind) + "/" + s.Namespace + "/" + s.Name
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

// Total returns the number of objects counted in this domain across all
// states. Pinned is excluded because it overlaps the state counters.
func (d *DomainCount) Total() int64 {
	return d.Running + d.Pending + d.Unschedulable + d.Terminating
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
