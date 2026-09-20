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
// Four of the evidence fields are deliberately left at their zero values —
// three on Evidence and DomainFacts.ConsolidatedAt on every row — and each one
// is a whole sibling source rather than a missing line:
//
//   - InsufficientResource and SchedulingMessage are `capacity`'s judgement
//     about Pending pods. Reading FailedScheduling events here would be a
//     second, worse answer to a question another source already answers.
//   - ConsolidatedAt is the autoscaler's, for the same reason.
//   - RolloutEndedAt needs a rollout's *completion* time; the §7.6 seam reports
//     only which workloads are rolling out right now.
//
// §14 puts "cause attribution consuming sibling sources" in Phase 8, and the
// cost of their absence is bounded and known: `domain_capacity_shortfall` loses
// its corroborating pod evidence and `consolidation` and `rollout_bias` cannot
// win at all, so attribution falls through to the cause below them on the
// ladder — never to a wrong one, because every rule needs positive evidence.
func (s *Source) evidenceFor(key leeway.TopologyKey, eligible leeway.Eligibility, dist *leeway.Distribution, now time.Time) leeway.Evidence {
	ev := leeway.Evidence{Domains: make(map[leeway.Domain]leeway.DomainFacts, len(eligible.Domains))}

	stats := s.inv.Stats(key)
	drains := s.inv.DrainTimes(key, eligible.Domains)
	cutoff := now.Add(-s.cfg.Cause.Window)

	for _, d := range eligible.Domains {
		facts := leeway.DomainFacts{TaintedAt: drains[d]}
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
