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
	"fmt"
	"time"
)

// DomainAvailability is one domain's usability on one axis at one instant —
// the whole input to §2.3's `leeway.domain_unavailable`.
//
// This kind is the one leeway name reached without scoring anything. Every
// other finding starts from a subject's distribution and asks whether it sits
// where its intent implies; this one asks a question about the cluster, and the
// domain is the subject. That is why it is a plain struct of counts rather than
// a Verdict: there is no expectation, no apportionment and nothing to relocate.
//
// The complementary reading matters. §7.6's domain-outage row asks whether a
// *workload's* drift is excusable and holds its finding back; this asks whether
// a *domain* is gone and says so once, for the cluster. A dead zone is one
// signal here and four hundred suppressions there, which is the point.
type DomainAvailability struct {
	Domain Domain

	// Nodes is every node in the domain, whatever its condition.
	Nodes int64

	// Ready is the nodes reporting Ready, cordoned or not.
	Ready int64

	// Usable is the nodes a scheduler could actually place on: Ready and
	// schedulable. This is the count the rule reads, and reading it rather
	// than Ready is what makes the cordon case detectable at all — a fully
	// cordoned zone has every node Ready and holds nothing new.
	Usable int64

	// Known says the cluster has this domain, or had it recently enough that
	// its absence is news rather than history. Without it a domain that never
	// existed and a domain that just died are the same two zeroes.
	Known bool

	// LostAt is when the usable count last fell, for the reader correlating
	// against their own change log. Zero when this process never watched it
	// happen — which is the honest answer for a process that started after the
	// domain was already gone, and is not the same as "just now".
	LostAt time.Time
}

// Unavailable reports §2.3's condition: a domain that the cluster has, or
// recently had, and that now has no node anything can be scheduled onto.
//
// Deliberately not "fewer usable nodes than it had", which is §7.6's halving
// rule. A domain at one node out of twenty is degraded and its workloads will
// report their own drift; a domain at zero is a domain the scheduler cannot
// use, and no workload finding says that about the cluster as a whole.
func (a DomainAvailability) Unavailable() bool { return a.Known && a.Usable == 0 }

// Cause is §8.5's guess at what emptied the domain.
//
// The kind states what we measured and the cause states what we think produced
// it — the distinction the rename from `leeway.domain_outage` exists to make
// (§2.3). These three are told apart by which count survived:
//
//   - No nodes at all: something removed them. `consolidation` is the honest
//     label for an autoscaler or a pool deletion, and it is a guess in exactly
//     the way the field name says.
//   - Nodes present, none Ready: the machines are there and not answering,
//     which is what a zone failure looks like from inside the cluster.
//   - Nodes present and Ready, none schedulable: somebody cordoned or tainted
//     them. `taint_exclusion` covers both, because a cordon *is* the
//     `node.kubernetes.io/unschedulable` taint.
func (a DomainAvailability) Cause() SuspectedCause {
	switch {
	case a.Nodes == 0:
		return CauseConsolidation
	case a.Ready == 0:
		return CauseDomainOutage
	default:
		return CauseTaintExclusion
	}
}

// DomainVerdict is the fixed §8.1 judgement for an unavailable domain.
//
// Tier B per §2.3's table, and a constant rather than a computation: there is
// no declared contract to violate (which would be A) and no inferred
// expectation to have been less sure about (which would be C). The cluster
// either has a usable node in the domain or it does not.
func DomainVerdict() Verdict {
	return Verdict{
		Breached: true,
		Tier:     TierB,
		Severity: TierB.Severity(),
		Reason:   "no usable nodes in this domain",
	}
}

// NewDomainFinding builds the §8.5 payload for an unavailable domain.
//
// The domain table has exactly one row — itself. That looks degenerate next to
// a workload finding's table of every eligible domain, and it is the right
// shape: the other rows would be other domains' business, and a reader who
// wants the cluster-wide picture has one of these per domain that is out.
//
// Score stays at its zero value. Every field in it is a statement about a
// distribution, and this finding has none; a zero drift here means "not
// applicable", which is why the message for this kind does not print it.
func NewDomainFinding(a DomainAvailability, key TopologyKey, st AlertState) Finding {
	v := DomainVerdict()
	return Finding{
		Kind:           KindDomainUnavailable,
		Subject:        SubjectRef{Kind: SubjectDomain, Name: string(a.Domain)},
		TopologyKey:    key,
		Tier:           v.Tier.String(),
		Severity:       v.Severity,
		SuspectedCause: a.Cause(),
		Domains: []FindingDomain{{
			Domain:     a.Domain,
			ReadyNodes: a.Ready,
			Note:       a.note(),
		}},
		FirstSeenAt: st.FirstSeenAt.UTC(),
	}
}

// note is the domain row's one line of evidence: the census that distinguishes
// the three causes, stated as counts rather than as the conclusion drawn from
// them, so a reader who disagrees with the cause can see why it was picked.
func (a DomainAvailability) note() string {
	switch {
	case a.Nodes == 0:
		return "no nodes remain in this domain"
	case a.Ready == 0:
		return fmt.Sprintf("%d node(s) present, none Ready", a.Nodes)
	default:
		return fmt.Sprintf("%d node(s) Ready, none schedulable", a.Ready)
	}
}
