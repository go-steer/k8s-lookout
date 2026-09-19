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
	"sync"

	"k8s.io/apimachinery/pkg/labels"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// PolicyStore holds the decoded policies from both informers and answers the
// only question the rest of the source asks of them: which policy, if any,
// governs this subject.
//
// It is a flat slice scan rather than an index. A selector cannot be indexed by
// the labels it matches without inverting it, the fleet baseline is tens of
// policies against tens of thousands of subjects, and the scan only runs on the
// coalesced evaluation path — one per subject per window, not one per event.
// Indexing by namespace would be the first thing to add if that stops holding.
type PolicyStore struct {
	mu sync.RWMutex
	// byRef is keyed by the policy's own identity, so an update replaces
	// rather than duplicates.
	byRef map[string]*Policy
}

// NewPolicyStore returns an empty store, which is the shape a cluster with no
// CRD installed keeps forever.
func NewPolicyStore() *PolicyStore {
	return &PolicyStore{byRef: map[string]*Policy{}}
}

// Upsert adds or replaces a policy.
func (s *PolicyStore) Upsert(p *Policy) {
	if p == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byRef[p.ref()] = p
}

// Delete removes a policy by identity. clusterScoped and namespace must match
// how it was stored, which is why the key is built the same way both times.
func (s *PolicyStore) Delete(namespace, name string) {
	p := &Policy{Namespace: namespace, Name: name}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byRef, p.ref())
}

// Len reports how many policies are held.
func (s *PolicyStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byRef)
}

// PolicyMatch is the outcome of resolving a subject against the store.
type PolicyMatch struct {
	// Policy is the governing policy, nil when none matched.
	Policy *Policy

	// Ambiguous is true when more than one policy matched at the winning
	// specificity and the tie was broken by name.
	//
	// It is reported rather than resolved on purpose. The obvious tiebreak —
	// "the more specific selector wins" — has no honest definition: a
	// matchExpressions Exists term and three matchLabels are not comparable,
	// and a rule that pretends they are would silently pick a policy the
	// operator did not intend and never tell them. A deterministic, arbitrary
	// choice plus a counter is a misconfiguration the operator can see.
	Ambiguous bool

	// Competing names the policies that tied, winner first, for the log line.
	Competing []string
}

// For returns the policy governing a subject, given the labels of the pod the
// subject was resolved from.
//
// Specificity is namespace-scoped beats cluster-scoped, and nothing else. That
// is the only comparison between two policies that is defensible without
// inventing an ordering on selectors — a LeewayPolicy names the namespace it
// applies to, so it is unambiguously about a smaller set of subjects than a
// ClusterLeewayPolicy is.
//
// The winning policy applies whole. Merging two matching policies key by key
// would let a workload end up governed by a document nobody wrote, and the
// result would depend on which keys each happened to declare.
func (s *PolicyStore) For(sub leeway.SubjectRef, podLabels labels.Set) PolicyMatch {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.byRef) == 0 {
		return PolicyMatch{}
	}

	var namespaced, clusterScoped []*Policy
	for _, p := range s.byRef {
		if !p.Matches(sub, podLabels) {
			continue
		}
		if p.ClusterScoped() {
			clusterScoped = append(clusterScoped, p)
		} else {
			namespaced = append(namespaced, p)
		}
	}

	winners := namespaced
	if len(winners) == 0 {
		winners = clusterScoped
	}
	if len(winners) == 0 {
		return PolicyMatch{}
	}
	sort.Slice(winners, func(i, j int) bool { return winners[i].Name < winners[j].Name })

	m := PolicyMatch{Policy: winners[0], Ambiguous: len(winners) > 1}
	if m.Ambiguous {
		m.Competing = make([]string, 0, len(winners))
		for _, p := range winners {
			m.Competing = append(m.Competing, p.ref())
		}
	}
	return m
}

// filterBySources drops inference candidates a policy's allowlist excludes.
//
// It runs before precedence resolution rather than after, which is the
// difference between "that source may not contribute" and "that source may not
// win". A source excluded here does not survive as demoted evidence either,
// and that is the point: an operator who says they do not trust preferred
// anti-affinity should not find it quoted in a finding's explanation.
func filterBySources(candidates []leeway.Intent, cfg InferenceConfig) []leeway.Intent {
	out := candidates[:0:0]
	for _, c := range candidates {
		if c.Source == leeway.SourcePolicyCRD || cfg.Allows(c.Source) {
			out = append(out, c)
		}
	}
	return out
}
