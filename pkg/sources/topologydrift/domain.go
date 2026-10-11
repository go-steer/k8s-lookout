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

// DefaultDomainUnavailableKeys are the failure-domain axes `leeway.domain_unavailable`
// is judged on; per-node axes such as `kubernetes.io/hostname` are excluded
// intentionally (see docs/leeway-design.md §2.3, §16).
var DefaultDomainUnavailableKeys = []leeway.TopologyKey{
	corev1.LabelTopologyZone,
	corev1.LabelTopologyRegion,
}

// domainAxes restricts configured axes to those tracked by the inventory.
func (s *Source) domainAxes() []leeway.TopologyKey {
	out := make([]leeway.TopologyKey, 0, len(s.cfg.DomainUnavailableKeys))
	for _, key := range s.cfg.DomainUnavailableKeys {
		if _, ok := s.inv.Ordinal(key); ok {
			out = append(out, key)
		}
	}
	return out
}

// sampleDomains latches which domains this process has observed with nodes and
// forgets domains whose nodes are gone and whose ready history has aged out
// (see docs/leeway-design.md §2.3, §16).
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

// domainAvailability assembles one domain's §2.3 reading from the hoisted axis census.
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

// domainJudgements is §8.2's input for domain subjects: one per known (axis, domain).
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

// unavailableDomains counts the domains currently judged unavailable, per axis (§8.4).
func (s *Source) unavailableDomains() map[leeway.TopologyKey]int64 {
	axes := s.domainAxes()
	if len(axes) == 0 {
		return nil
	}
	out := make(map[leeway.TopologyKey]int64, len(axes))
	for _, key := range axes {
		out[key] = 0
	}
	s.eachKnownDomain(s.now(), func(key leeway.TopologyKey, a leeway.DomainAvailability) {
		if a.Unavailable() {
			out[key]++
		}
	})
	return out
}

// domainReading recovers one domain's availability for the emission path.
func (s *Source) domainReading(key leeway.TopologyKey, d leeway.Domain, now time.Time) leeway.DomainAvailability {
	return s.domainAvailability(key, d, s.inv.Stats(key), now.Add(-s.cfg.Cause.Window))
}
