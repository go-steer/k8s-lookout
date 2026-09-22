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
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// evidenceFor assembles §8.5's attribution input for one subject-axis.
//
// Everything here is an observation this source already holds: the inventory's
// per-domain node census and its ready-count series, and the subject's own
// distribution. Nothing is re-derived, which is the discipline leeway.Evidence
// exists to enforce — a rules engine that could recompute its own inputs would
// eventually disagree with the source that owns them.
//
// The pending-pod rows come from the `capacity` source through CapacityOracle,
// which is §8.5's "consumes sibling sources" made literal: that source owns
// the judgement about why a pod did not land, and reading FailedScheduling
// here would be a second, worse answer to a question already answered.
//
// RolloutEndedAt comes from the `rollout` source the same way, through
// RolloutEndOracle and sampled on the cluster tick rather than per finding
// because it is a cluster-wide answer.
//
// ConsolidatedAt comes from the inventory rather than from a sibling: the
// autoscaler announces itself on the node object, with a taint nobody else
// applies, and this source is already the one watching nodes. There is nothing
// to be unavailable, which is why EvidenceGaps names only the two seams that
// can genuinely be absent.
//
// Every gap that remains is reported rather than implied. Evidence.Unavailable
// says which sources could not be asked, and Attribute turns that into the
// finding's `untested` list, so a deployment missing a source is
// distinguishable from one where the hypothesis was tested and lost. The cost
// stays bounded either way: the cause simply cannot win, and attribution falls
// through to the one below it on the ladder — never to a wrong one, because
// every rule needs positive evidence.
func (s *Source) evidenceFor(sub leeway.SubjectRef, key leeway.TopologyKey, eligible leeway.Eligibility, dist *leeway.Distribution, now time.Time) leeway.Evidence {
	ev := leeway.Evidence{
		Domains:        make(map[leeway.Domain]leeway.DomainFacts, len(eligible.Domains)),
		RolloutEndedAt: s.rolloutEndedAt(sub),
		Unavailable: leeway.EvidenceGaps{
			Capacity:          s.capacity == nil,
			RolloutCompletion: s.rolloutEnd == nil,
		},
	}
	s.pendingEvidence(&ev, sub)

	stats := s.inv.Stats(key)
	drains := s.inv.DrainTimes(key, eligible.Domains)
	packed := s.inv.ConsolidationTimes(key, eligible.Domains)
	cutoff := now.Add(-s.cfg.Cause.Window)

	for _, d := range eligible.Domains {
		facts := leeway.DomainFacts{TaintedAt: drains[d], ConsolidatedAt: packed[d]}
		if st, ok := stats[d]; ok {
			facts.ReadyNodes = st.Ready
			// Present but not Ready. Not redundant with a fall in the series:
			// a process that started during an outage has no peak to compare
			// against, and the census is the only evidence left.
			facts.NotReadyNodes = st.Nodes - st.Ready
		}
		facts.PeakReadyNodes, facts.LostAt = peakAndFall(s.inv.ReadyHistory(key, d), cutoff, facts.ReadyNodes)
		if dist != nil {
			if c, ok := dist.ByDomain[d]; ok {
				facts.Pinned = c.Pinned
				ev.ZonalVolumes = ev.ZonalVolumes || c.Pinned > 0
			}
		}
		ev.Domains[d] = facts
	}
	return ev
}

// pendingEvidence fills the two pending-pod rows from the capacity oracle.
//
// The intersection runs from the oracle's side: its snapshot holds only pods
// the scheduler has refused, which is a handful even on a cluster in trouble,
// while this source's index holds every pod in the cluster. Walking the small
// set and asking the index who owns each one costs the size of the problem.
//
// Only pods citing insufficient resources are counted. A subject whose pods
// are all refused for an untolerated taint has a real problem, but it is not
// the one `domain_capacity_shortfall` names, and counting them here would let
// the ladder answer "not enough room" to a question about a taint.
//
// The message is one pod's, not a summary: the scheduler's text already
// enumerates every predicate that failed and across how many nodes, so the
// second message is nearly always the first one again. The earliest refusal
// wins, tie-broken by UID, because a stable choice is what lets two findings
// an hour apart be diffed — picking whichever pod the map yielded first would
// churn the payload on every pass with nothing having changed.
func (s *Source) pendingEvidence(ev *leeway.Evidence, sub leeway.SubjectRef) {
	if s.capacity == nil {
		return
	}
	var (
		bestUID   types.UID
		bestSince time.Time
	)
	for uid, fact := range s.capacity() {
		if !fact.InsufficientResource {
			continue
		}
		if owner, ok := s.state.SubjectOfPod(uid); !ok || owner != sub {
			continue
		}
		ev.InsufficientResource++
		if bestUID == "" || fact.Since.Before(bestSince) ||
			(fact.Since.Equal(bestSince) && uid < bestUID) {
			bestUID, bestSince = uid, fact.Since
			ev.SchedulingMessage = fact.Message
		}
	}
}

// peakAndFall reads the two history-derived facts out of one domain's ready
// series: the highest count inside the attribution window, and when the count
// last went down.
//
// Samples at or before the cutoff are skipped rather than the slice being
// pruned, because the series is shared with §7.6's outage test, which scans its
// own — shorter — window. Both read the same samples and neither may shorten
// them for the other.
//
// The peak floors at the current count, so a domain with no history at all
// reports peak == ready and no fall: that is a process that has not been
// running long enough to have watched anything happen, and it should say so
// rather than report a drop from zero.
//
// The fall is the *last* decrease, not the first. A zone that fell 12 → 3 an
// hour ago and 3 → 1 a minute ago is a zone that lost nodes a minute ago; the
// older step is still in the peak, which is the number the finding compares
// against.
func peakAndFall(hist []leeway.ReadyCount, cutoff time.Time, ready int64) (peak int64, lostAt time.Time) {
	peak = ready
	var prev leeway.ReadyCount
	var seen bool
	for _, sample := range hist {
		if !sample.At.After(cutoff) {
			continue
		}
		if sample.Ready > peak {
			peak = sample.Ready
		}
		if seen && sample.Ready < prev.Ready {
			lostAt = sample.At
		}
		prev, seen = sample, true
	}
	return peak, lostAt
}
