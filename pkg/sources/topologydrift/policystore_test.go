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
	"fmt"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/labels"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

func apiSubject() leeway.SubjectRef {
	return leeway.SubjectRef{Kind: "Deployment", Namespace: "payments", Name: "api"}
}

func apiLabels() labels.Set { return labels.Set{"app": "api"} }

// storeWith builds a store from YAML documents, cluster-scoped ones marked by
// their kind so a fixture reads the way the cluster does.
func storeWith(t *testing.T, docs ...string) *PolicyStore {
	t.Helper()
	s := NewPolicyStore()
	for _, doc := range docs {
		u := policyFrom(t, doc)
		p, err := DecodePolicy(u, u.GetKind() == ClusterPolicyKind)
		if err != nil {
			t.Fatalf("fixture does not decode: %v", err)
		}
		s.Upsert(p)
	}
	return s
}

func clusterPolicy(name string) string {
	return fmt.Sprintf(`
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: ClusterLeewayPolicy
metadata: { name: %s }
spec:
  topologyKeys:
    - { key: topology.kubernetes.io/zone }
`, name)
}

func nsPolicy(name string) string {
	return fmt.Sprintf(`
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: LeewayPolicy
metadata: { name: %s, namespace: payments }
spec:
  topologyKeys:
    - { key: topology.kubernetes.io/zone }
`, name)
}

func TestPolicyStore_EmptyIsTheNormalCase(t *testing.T) {
	// A cluster with no CRD installed keeps this shape forever, and it is the
	// default deployment, not a degraded one.
	s := NewPolicyStore()
	if got := s.For(apiSubject(), apiLabels()); got.Policy != nil {
		t.Errorf("For() on an empty store = %v, want no policy", got.Policy)
	}
	if s.Len() != 0 {
		t.Errorf("Len() = %d", s.Len())
	}
}

func TestPolicyStore_UpsertReplacesRatherThanDuplicates(t *testing.T) {
	s := storeWith(t, nsPolicy("api"))
	s.Upsert(decode(t, nsPolicy("api"), false))
	if s.Len() != 1 {
		t.Errorf("Len() = %d after re-upserting the same policy, want 1", s.Len())
	}
}

func TestPolicyStore_Delete(t *testing.T) {
	s := storeWith(t, nsPolicy("api"), clusterPolicy("fleet"))

	s.Delete("payments", "api")
	if got := s.For(apiSubject(), apiLabels()); got.Policy == nil || got.Policy.Name != "fleet" {
		t.Errorf("after deleting the namespaced policy, For() = %v, want the cluster one", got.Policy)
	}
	s.Delete("", "fleet")
	if s.Len() != 0 {
		t.Errorf("Len() = %d, want 0", s.Len())
	}
	// Deleting something absent is a normal informer event, not an error.
	s.Delete("", "never-existed")
}

func TestPolicyStore_NamespacedBeatsClusterScoped(t *testing.T) {
	// The only comparison between two policies that is defensible without
	// inventing an ordering on selectors: a LeewayPolicy names the namespace
	// it applies to, so it is unambiguously about a smaller set of subjects.
	s := storeWith(t, clusterPolicy("fleet"), nsPolicy("api"))
	got := s.For(apiSubject(), apiLabels())
	if got.Policy == nil || got.Policy.Name != "api" {
		t.Fatalf("For() = %v, want the namespaced policy", got.Policy)
	}
	if got.Ambiguous {
		t.Error("Ambiguous = true: a namespaced and a cluster policy are not a tie")
	}
}

func TestPolicyStore_ClusterScopedAppliesWhereNoNamespacedOneDoes(t *testing.T) {
	s := storeWith(t, clusterPolicy("fleet"), nsPolicy("api"))
	other := leeway.SubjectRef{Kind: "Deployment", Namespace: "search", Name: "indexer"}
	got := s.For(other, labels.Set{"app": "indexer"})
	if got.Policy == nil || got.Policy.Name != "fleet" {
		t.Fatalf("For() = %v, want the cluster policy in a namespace the other does not cover", got.Policy)
	}
}

func TestPolicyStore_AmbiguityIsReportedNotResolved(t *testing.T) {
	// The obvious tiebreak — "the more specific selector wins" — has no honest
	// definition, so the store picks deterministically and says it had to.
	s := storeWith(t, nsPolicy("zeta"), nsPolicy("alpha"))
	got := s.For(apiSubject(), apiLabels())
	if !got.Ambiguous {
		t.Fatal("Ambiguous = false with two matching namespaced policies")
	}
	if got.Policy.Name != "alpha" {
		t.Errorf("winner = %q, want the lexicographically first", got.Policy.Name)
	}
	if len(got.Competing) != 2 || got.Competing[0] != got.Policy.ref() {
		t.Errorf("Competing = %v, want both policies with the winner first", got.Competing)
	}
	// Deterministic across repeats: map iteration order must not reach the
	// answer, or an operator sees the governing policy flap between restarts.
	for i := 0; i < 50; i++ {
		if s.For(apiSubject(), apiLabels()).Policy.Name != "alpha" {
			t.Fatal("the winner changed between calls")
		}
	}
}

func TestPolicyStore_AmbiguityAmongClusterPoliciesToo(t *testing.T) {
	s := storeWith(t, clusterPolicy("zeta"), clusterPolicy("alpha"))
	got := s.For(apiSubject(), apiLabels())
	if !got.Ambiguous || got.Policy.Name != "alpha" {
		t.Errorf("For() = %v ambiguous=%v", got.Policy, got.Ambiguous)
	}
}

func TestPolicyStore_NonMatchingPoliciesAreNotATie(t *testing.T) {
	s := storeWith(t, nsPolicy("api"), `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: LeewayPolicy
metadata: { name: other, namespace: payments }
spec:
  selector: { matchLabels: { app: something-else } }
`)
	got := s.For(apiSubject(), apiLabels())
	if got.Ambiguous {
		t.Error("a policy that does not match counted toward the tie")
	}
	if got.Policy.Name != "api" {
		t.Errorf("winner = %q", got.Policy.Name)
	}
}

func TestPolicyStore_ConcurrentReadsAndWrites(t *testing.T) {
	// Informer handlers write while the coalesced evaluation path reads. Under
	// -race this is the test that would catch a missing lock.
	s := NewPolicyStore()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				s.Upsert(&Policy{Name: fmt.Sprintf("p%d", i), Namespace: "payments",
					Selector: labels.Everything()})
				s.For(apiSubject(), apiLabels())
				s.Delete("payments", fmt.Sprintf("p%d", i))
				s.Len()
			}
		}(i)
	}
	wg.Wait()
}

func TestPolicyStore_UpsertNilIsANoOp(t *testing.T) {
	s := NewPolicyStore()
	s.Upsert(nil)
	if s.Len() != 0 {
		t.Errorf("Len() = %d", s.Len())
	}
}

func TestFilterBySources(t *testing.T) {
	candidates := []leeway.Intent{
		{TopologyKey: "zone", Source: leeway.SourceTopologySpreadConstraint},
		{TopologyKey: "zone", Source: leeway.SourcePodAntiAffinityPreferred},
		{TopologyKey: "zone", Source: leeway.SourceClusterDefaultAssumed},
	}

	t.Run("a nil allowlist keeps everything", func(t *testing.T) {
		got := filterBySources(candidates, InferenceConfig{Enabled: true})
		if len(got) != 3 {
			t.Errorf("kept %d of 3", len(got))
		}
	})

	t.Run("an allowlist drops what it does not name", func(t *testing.T) {
		got := filterBySources(candidates, InferenceConfig{
			Enabled: true,
			Sources: map[leeway.IntentSource]bool{leeway.SourceTopologySpreadConstraint: true},
		})
		if len(got) != 1 || got[0].Source != leeway.SourceTopologySpreadConstraint {
			t.Errorf("kept %v", got)
		}
	})

	t.Run("disabled inference drops every inferred source", func(t *testing.T) {
		if got := filterBySources(candidates, InferenceConfig{}); len(got) != 0 {
			t.Errorf("kept %v with inference off", got)
		}
	})

	t.Run("the policy's own intent is never filtered", func(t *testing.T) {
		// A policy cannot exclude itself by omitting its own source from the
		// allowlist — the allowlist is about what may be *inferred*.
		withPolicy := append([]leeway.Intent{{TopologyKey: "zone", Source: leeway.SourcePolicyCRD}}, candidates...)
		got := filterBySources(withPolicy, InferenceConfig{})
		if len(got) != 1 || got[0].Source != leeway.SourcePolicyCRD {
			t.Errorf("got %v, want the policy intent alone", got)
		}
	})

	t.Run("does not alias the input", func(t *testing.T) {
		in := []leeway.Intent{
			{TopologyKey: "zone", Source: leeway.SourceTopologySpreadConstraint},
			{TopologyKey: "zone", Source: leeway.SourcePodAffinityPreferred},
		}
		filterBySources(in, InferenceConfig{
			Enabled: true,
			Sources: map[leeway.IntentSource]bool{leeway.SourcePodAffinityPreferred: true},
		})
		if in[0].Source != leeway.SourceTopologySpreadConstraint {
			t.Error("filtering overwrote the caller's slice")
		}
	})
}
