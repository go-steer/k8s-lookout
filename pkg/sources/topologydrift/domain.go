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
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// DefaultDomainUnavailableKeys are the axes `leeway.domain_unavailable` is
// judged on: the two that are failure domains.
//
// `kubernetes.io/hostname` is deliberately absent, and leaving it in would have
// been the defect that made this kind useless. It is a topology key like the
// others — kube-scheduler spreads over it, and §7.1 computes eligibility for it
// — but every node is its own domain on that axis, so one NotReady node would
// raise a `leeway.domain_unavailable` naming that node. A cluster doing a
// rolling upgrade would produce one per node in the pool. That observation is
// already lookout's: `objectstate` watches node Ready transitions and says so
// with the node as the subject, which is the right subject for it.
//
// The list is a configuration rather than a hard-coded pair because a custom
// failure-domain label is a real thing — `topology.gke.io/zone` on some
// estates, a rack or a cell label on bare metal — and the only wrong answer
// here is an axis whose domains are per-node.
var DefaultDomainUnavailableKeys = []leeway.TopologyKey{
	corev1.LabelTopologyZone,
	corev1.LabelTopologyRegion,
}

// domainAxes is the configured set, restricted to axes the inventory actually
// tracks. An axis nobody asked the inventory to index has no domains and no
// history, so judging it would be judging the empty set every pass.
func (s *Source) domainAxes() []leeway.TopologyKey {
	out := make([]leeway.TopologyKey, 0, len(s.cfg.DomainUnavailableKeys))
	for _, key := range s.cfg.DomainUnavailableKeys {
		if _, ok := s.inv.Ordinal(key); ok {
			out = append(out, key)
		}
	}
	return out
}

// sampleDomains latches which domains this process has seen the cluster
// actually use, and forgets the ones that have aged out. It runs on the same
// 30 s tick as SampleReady, immediately after it.
//
// The latch is what makes the finding survive its own evidence. The ready
// series is retained for twice §7.6's outage window and no longer, so a rule
// reading only "was this domain usable inside the window" would report a dead
// zone for half an hour and then quietly decide it had always been dead — the
// episode resolving itself while the zone was still down, which is the one
// failure mode this kind cannot have.
//
// It stays bounded anyway, because the two conditions it forgets on cover every
// way a domain can stop being interesting. A domain the cluster still has nodes
// in is re-latched every tick and needs no memory. A domain whose nodes are all
// gone *and* whose history has aged out is a domain this process can no longer
// say anything about: it cannot distinguish a zone deleted on purpose from one
// that failed thirty-one minutes ago, and asserting the second would be
// inventing the distinction. So the entry goes, the episode resolves, and the
// number of entries is bounded by the failure domains a cluster has ever had.
func (s *Source) sampleDomains(now time.Time) {
	axes := s.domainAxes()
	if len(axes) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.domainSeen == nil {
		s.domainSeen = make(map[leeway.TopologyKey]map[leeway.Domain]time.Time, len(axes))
	}
	for _, key := range axes {
		seen, ok := s.domainSeen[key]
		if !ok {
			seen = make(map[leeway.Domain]time.Time)
			s.domainSeen[key] = seen
		}
		stats := s.inv.Stats(key)
		// Any node at all, not just a usable one. A process that starts while
		// a zone is already down has no fall to have watched, and refusing to
		// report the zone on that basis would make the finding depend on
		// whether lookout happened to be running an hour ago. The nodes are
		// there and none of them can take a pod; that is the whole claim.
		for d, st := range stats {
			if st.Nodes > 0 {
				seen[d] = now
			}
		}
		for d := range seen {
			if _, live := stats[d]; live {
				continue
			}
			if len(s.inv.ReadyHistory(key, d)) == 0 {
				delete(seen, d)
			}
		}
	}
}

// knownDomains is the latched domain set for one axis, in a stable order so
// that two passes produce the same judgement sequence.
func (s *Source) knownDomains(key leeway.TopologyKey) []leeway.Domain {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]leeway.Domain, 0, len(s.domainSeen[key]))
	for d := range s.domainSeen[key] {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// domainAvailability assembles one domain's §2.3 reading. `stats` is the axis's
// census, hoisted by the caller because Inventory.Stats builds a fresh map.
//
// Known is passed in rather than read here: the caller already holds the
// latched domain list, and a second lock per domain would be answering a
// question it has the answer to.
//
// LostAt is read off the same ready series §8.5's attribution uses, through the
// same helper, so a domain finding and a workload finding raised in the same
// second cannot disagree about when the zone went. The peak that helper also
// returns is discarded: `Unavailable` compares against the latch, not against
// the window, for the reason sampleDomains gives.
func (s *Source) domainAvailability(key leeway.TopologyKey, d leeway.Domain, stats map[leeway.Domain]DomainStats, cutoff time.Time) leeway.DomainAvailability {
	a := leeway.DomainAvailability{Domain: d, Known: true}
	if st, ok := stats[d]; ok {
		a.Nodes, a.Ready, a.Usable = st.Nodes, st.Ready, st.Usable
	}
	_, a.LostAt = peakAndFall(s.inv.ReadyHistory(key, d), cutoff, a.Ready)
	return a
}

// eachKnownDomain walks every latched (axis, domain) pair with its reading.
func (s *Source) eachKnownDomain(now time.Time, fn func(key leeway.TopologyKey, a leeway.DomainAvailability)) {
	cutoff := now.Add(-s.cfg.Cause.Window)
	for _, key := range s.domainAxes() {
		domains := s.knownDomains(key)
		if len(domains) == 0 {
			continue
		}
		stats := s.inv.Stats(key)
		for _, d := range domains {
			fn(key, s.domainAvailability(key, d, stats, cutoff))
		}
	}
}

// domainJudgements is §8.2's input for the domain subjects: one per (axis,
// domain) the cluster has, breached where the domain has no usable node.
//
// Every known domain is judged, not only the broken ones. `Alerts.Pass` reads
// the *absence* of a judgement as "this subject is gone" and abandons the
// episode without resolving it, so a healthy domain has to be present and
// saying so — otherwise a zone that came back would have its finding dropped on
// the floor instead of clearing through the resolve dwell.
//
// The domains are enumerated from the latch rather than from the inventory,
// because the case this kind exists for is precisely the one where the
// inventory no longer has the domain.
func (s *Source) domainJudgements(now time.Time) []Judgement {
	var js []Judgement
	s.eachKnownDomain(now, func(key leeway.TopologyKey, a leeway.DomainAvailability) {
		js = append(js, Judgement{
			Subject:  leeway.SubjectRef{Kind: leeway.SubjectDomain, Name: string(a.Domain)},
			Key:      key,
			Breached: a.Unavailable(),
			Tier:     leeway.TierB,
		})
	})
	return js
}

// unavailableDomains counts the domains currently judged unavailable, per axis,
// for the §8.4 gauge.
//
// Every configured axis is in the map, at zero if nothing is out. Zero is the
// reading that matters here — it is the difference between "no zone is down"
// and "nobody is asking" — and a gauge that only appears during an outage
// cannot be alerted on before the first one.
//
// This reads ahead of the dwell, which `lookout_leeway_alert_state` does not:
// the gauge says a domain is empty now, the episode says it has been empty long
// enough to tell somebody.
func (s *Source) unavailableDomains() map[leeway.TopologyKey]int64 {
	axes := s.domainAxes()
	if len(axes) == 0 {
		return nil
	}
	out := make(map[leeway.TopologyKey]int64, len(axes))
	for _, key := range axes {
		out[key] = 0
	}
	s.eachKnownDomain(time.Now(), func(key leeway.TopologyKey, a leeway.DomainAvailability) {
		if a.Unavailable() {
			out[key]++
		}
	})
	return out
}

// domainReading recovers one domain's availability for the emission path, where
// §8.2 has handed back an Outcome naming a subject and the finding needs the
// counts behind it.
func (s *Source) domainReading(key leeway.TopologyKey, d leeway.Domain, now time.Time) leeway.DomainAvailability {
	return s.domainAvailability(key, d, s.inv.Stats(key), now.Add(-s.cfg.Cause.Window))
}
