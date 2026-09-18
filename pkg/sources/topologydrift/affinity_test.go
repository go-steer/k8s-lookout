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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

const hostKey = corev1.LabelHostname

// affPod wraps an Affinity on the same app=api subject infer_test.go uses.
func affPod(aff *corev1.Affinity) *corev1.Pod {
	p := tscPod()
	p.Spec.Affinity = aff
	return p
}

func podTerm(topologyKey string, sel *metav1.LabelSelector) corev1.PodAffinityTerm {
	return corev1.PodAffinityTerm{TopologyKey: topologyKey, LabelSelector: sel}
}

func selfTerm(topologyKey string) corev1.PodAffinityTerm {
	return podTerm(topologyKey, appSelector())
}

func antiRequired(terms ...corev1.PodAffinityTerm) *corev1.Affinity {
	return &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: terms,
	}}
}

func antiPreferred(weight int32, t corev1.PodAffinityTerm) *corev1.Affinity {
	return &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
			{Weight: weight, PodAffinityTerm: t},
		},
	}}
}

func affRequired(terms ...corev1.PodAffinityTerm) *corev1.Affinity {
	return &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: terms,
	}}
}

func TestAffinityIntents_RequiredAntiAffinityIsAPerDomainCeiling(t *testing.T) {
	got := AffinityIntents(affPod(antiRequired(selfTerm(hostKey))))
	if len(got) != 1 {
		t.Fatalf("got %d intents, want 1: %+v", len(got), got)
	}
	in := got[0]

	if in.Mode != leeway.ModeSpread {
		t.Errorf("Mode = %v, want Spread", in.Mode)
	}
	if in.Source != leeway.SourcePodAntiAffinityRequired {
		t.Errorf("Source = %v", in.Source)
	}
	if in.Confidence != leeway.ConfidenceInferred {
		t.Errorf("Confidence = %v, want Inferred: nothing here declared a distribution", in.Confidence)
	}
	// The contract is a ceiling, not a skew bound. A maxSkew of one is
	// satisfied by two pods in every domain, which this term forbids outright;
	// and the anti-affinity holds the observed skew at one by construction, so
	// a skew check against it could never fire.
	if in.MaxPerDomain == nil || *in.MaxPerDomain != 1 {
		t.Errorf("MaxPerDomain = %v, want 1", in.MaxPerDomain)
	}
	if in.MaxSkew != nil {
		t.Errorf("MaxSkew = %d, want nil", *in.MaxSkew)
	}
	// §8.1 lists a violated required anti-affinity as Tier A.
	if !in.HardContract() {
		t.Error("a required anti-affinity did not reach the Tier A gate")
	}
}

func TestAffinityIntents_PreferredAntiAffinityIsTheCommonCase(t *testing.T) {
	// S3's live sample: no topologySpreadConstraints at all, one preferred
	// hostname anti-affinity. A subsystem that only understood FR-4 would have
	// nothing to say about this workload.
	got := AffinityIntents(affPod(antiPreferred(100, selfTerm(hostKey))))
	if len(got) != 1 {
		t.Fatalf("got %d intents, want 1", len(got))
	}
	in := got[0]

	if in.Source != leeway.SourcePodAntiAffinityPreferred || in.Mode != leeway.ModeSpread {
		t.Errorf("source/mode = %v/%v", in.Source, in.Mode)
	}
	if in.MaxPerDomain != nil {
		t.Errorf("MaxPerDomain = %d: a preference is not a ceiling", *in.MaxPerDomain)
	}
	if in.HardContract() {
		t.Error("a preferred anti-affinity reached the Tier A gate")
	}
	if !strings.Contains(in.Evidence[0].Detail, "weight=100") {
		t.Errorf("evidence lost the weight: %q", in.Evidence[0].Detail)
	}
}

func TestAffinityIntents_AntiAffinityAgainstAnotherWorkloadIsNotSpreadIntent(t *testing.T) {
	// "Do not sit with the batch job" constrains where replicas may go — which
	// is §7.1's business — but says nothing about how they distribute among
	// themselves. Scoring spread against it would report drift the term never
	// asked to prevent.
	other := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "batch"}}
	if got := AffinityIntents(affPod(antiRequired(podTerm(hostKey, other)))); got != nil {
		t.Errorf("got %+v, want no intent", got)
	}
}

func TestAffinityIntents_PodAffinityIsColocation(t *testing.T) {
	t.Run("self-referential", func(t *testing.T) {
		got := AffinityIntents(affPod(affRequired(selfTerm(corev1.LabelTopologyZone))))
		if len(got) != 1 {
			t.Fatalf("got %d intents, want 1", len(got))
		}
		in := got[0]
		if in.Mode != leeway.ModeColocate {
			t.Errorf("Mode = %v, want Colocate", in.Mode)
		}
		if in.Source != leeway.SourcePodAffinityRequired {
			t.Errorf("Source = %v", in.Source)
		}
		// The scheduler enforces this as hard as an anti-affinity, but it was
		// satisfied at every placement — there is no promise to have broken.
		if in.HardContract() {
			t.Error("a required podAffinity reached the Tier A gate")
		}
		if strings.Contains(in.Evidence[0].Detail, "induced") {
			t.Errorf("a self-referential term was described as induced: %q", in.Evidence[0].Detail)
		}
	})

	t.Run("a pull toward another population is kept and flagged", func(t *testing.T) {
		// "Run next to the cache" is a real concentrating force even though it
		// names somebody else — unlike the anti-affinity case, where the
		// mirror-image term says nothing about our own distribution. But it is
		// much weaker: if the cache is spread out, we are free to spread too.
		cache := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "cache"}}
		got := AffinityIntents(affPod(affRequired(podTerm(corev1.LabelTopologyZone, cache))))
		if len(got) != 1 {
			t.Fatalf("got %d intents, want 1", len(got))
		}
		if !strings.Contains(got[0].Evidence[0].Detail, "colocation is induced") {
			t.Errorf("evidence = %q", got[0].Evidence[0].Detail)
		}
	})

	t.Run("preferred", func(t *testing.T) {
		aff := &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{Weight: 50, PodAffinityTerm: selfTerm(corev1.LabelTopologyZone)},
			},
		}}
		got := AffinityIntents(affPod(aff))
		if len(got) != 1 || got[0].Source != leeway.SourcePodAffinityPreferred || got[0].Mode != leeway.ModeColocate {
			t.Fatalf("got %+v", got)
		}
	})
}

func TestAffinityIntents_Skipped(t *testing.T) {
	cases := map[string]*corev1.Affinity{
		"no affinity at all": nil,
		"an affinity block with neither half": {
			NodeAffinity: &corev1.NodeAffinity{},
		},
		"empty topologyKey": antiRequired(podTerm("", appSelector())),
		"nil labelSelector": antiRequired(podTerm(hostKey, nil)),
		"invalid selector": antiRequired(podTerm(hostKey, &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "app", Operator: metav1.LabelSelectorOperator("Roughly"), Values: []string{"api"}},
			},
		})),
		// Self-matching, but aimed at somebody else's namespace — so it cannot
		// be counting this subject's replicas, which all live in ours.
		"a foreign namespace list": antiRequired(corev1.PodAffinityTerm{
			TopologyKey:   hostKey,
			LabelSelector: appSelector(),
			Namespaces:    []string{"staging"},
		}),
		// A non-empty namespaceSelector needs Namespace objects, which this
		// package does not watch. Dropping the intent is the direction that
		// does not invent one.
		"an unevaluatable namespaceSelector": antiRequired(corev1.PodAffinityTerm{
			TopologyKey:       hostKey,
			LabelSelector:     appSelector(),
			NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "prod"}},
		}),
	}
	for name, aff := range cases {
		t.Run(name, func(t *testing.T) {
			if got := AffinityIntents(affPod(aff)); got != nil {
				t.Errorf("got %+v, want no intent", got)
			}
		})
	}

	t.Run("an unusable podAffinity term is dropped too", func(t *testing.T) {
		// The two halves share the usability rules and only diverge on whose
		// pods the selector has to match, so the podAffinity path needs its
		// own case or the shared guard is only half tested.
		if got := AffinityIntents(affPod(affRequired(podTerm(corev1.LabelTopologyZone, nil)))); got != nil {
			t.Errorf("got %+v, want no intent", got)
		}
	})
}

func TestAffinityIntents_WiderScopesAreKeptAndNoted(t *testing.T) {
	cases := map[string]struct {
		term corev1.PodAffinityTerm
		want string
	}{
		"empty labelSelector": {
			term: podTerm(hostKey, &metav1.LabelSelector{}),
			want: "labelSelector is empty",
		},
		"our namespace named explicitly alongside another": {
			term: corev1.PodAffinityTerm{
				TopologyKey: hostKey, LabelSelector: appSelector(),
				Namespaces: []string{"payments", "staging"},
			},
			want: "namespaces=payments,staging",
		},
		"an empty namespaceSelector spans every namespace": {
			term: corev1.PodAffinityTerm{
				TopologyKey: hostKey, LabelSelector: appSelector(),
				NamespaceSelector: &metav1.LabelSelector{},
			},
			want: "namespaceSelector matches every namespace",
		},
		"matchLabelKeys": {
			term: corev1.PodAffinityTerm{
				TopologyKey: hostKey, LabelSelector: appSelector(),
				MatchLabelKeys: []string{"pod-template-hash"},
			},
			want: "matchLabelKeys=pod-template-hash",
		},
		"mismatchLabelKeys": {
			term: corev1.PodAffinityTerm{
				TopologyKey: hostKey, LabelSelector: appSelector(),
				MismatchLabelKeys: []string{"role"},
			},
			want: "mismatchLabelKeys=role",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := AffinityIntents(affPod(antiRequired(tc.term)))
			if len(got) != 1 {
				t.Fatalf("got %d intents, want 1", len(got))
			}
			if !strings.Contains(got[0].Evidence[0].Detail, tc.want) {
				t.Errorf("evidence = %q, want it to mention %q", got[0].Evidence[0].Detail, tc.want)
			}
		})
	}

	t.Run("an unevaluatable namespaceSelector is noted on a podAffinity term", func(t *testing.T) {
		// podAffinity does not need own-namespace membership — a pull toward
		// another namespace still concentrates us — so this term survives
		// where the anti-affinity one above is dropped, and the note is the
		// only thing that says the scope was never checked.
		aff := affRequired(corev1.PodAffinityTerm{
			TopologyKey: corev1.LabelTopologyZone, LabelSelector: appSelector(),
			NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "prod"}},
		})
		got := AffinityIntents(affPod(aff))
		if len(got) != 1 {
			t.Fatalf("got %d intents, want 1", len(got))
		}
		if !strings.Contains(got[0].Evidence[0].Detail, "was not evaluated") {
			t.Errorf("evidence = %q", got[0].Evidence[0].Detail)
		}
	})
}

func TestAffinityIntents_StrongestFirstAndPrecedenceResolves(t *testing.T) {
	// A subject declaring both halves on one key is a contradiction the
	// scheduler resolves by refusing to place the pod at all. Resolution here
	// has to be deterministic and has to keep the loser, or a finding would
	// describe half of what the object says.
	aff := &corev1.Affinity{
		PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{selfTerm(hostKey)},
		},
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{selfTerm(hostKey)},
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{Weight: 10, PodAffinityTerm: selfTerm(hostKey)},
			},
		},
	}
	got := AffinityIntents(affPod(aff))
	if len(got) != 3 {
		t.Fatalf("got %d intents, want 3: %+v", len(got), got)
	}
	if got[0].Source != leeway.SourcePodAntiAffinityRequired {
		t.Errorf("first candidate = %v, want the required anti-affinity", got[0].Source)
	}

	winner := leeway.ResolveIntents(got)[leeway.TopologyKey(hostKey)]
	if winner == nil || winner.Source != leeway.SourcePodAntiAffinityRequired || winner.Mode != leeway.ModeSpread {
		t.Fatalf("winner = %+v, want the required anti-affinity's Spread", winner)
	}
	var ignored int
	for _, e := range winner.Evidence {
		if e.Ignored {
			ignored++
		}
	}
	if ignored == 0 {
		t.Errorf("the two superseded terms left no evidence: %+v", winner.Evidence)
	}
}

func TestAffinityIntents_TSCOutranksAntiAffinityOnTheSameKey(t *testing.T) {
	// §5.1's precedence in the shape it will actually be hit: a workload with
	// both a TSC and the hostname anti-affinity everybody copies into their
	// templates.
	pod := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	pod.Spec.Affinity = antiRequired(selfTerm(corev1.LabelTopologyZone))

	candidates := append(SpreadConstraintIntents(pod), AffinityIntents(pod)...)
	winner := leeway.ResolveIntents(candidates)[leeway.TopologyKey(corev1.LabelTopologyZone)]
	if winner == nil || winner.Source != leeway.SourceTopologySpreadConstraint {
		t.Fatalf("winner = %+v, want the TSC", winner)
	}
	// The anti-affinity's ceiling must not ride along on the winner: the TSC
	// said maxSkew, and a per-domain cap of one it never asked for would make
	// every domain holding two replicas unplaceable.
	if winner.MaxPerDomain != nil {
		t.Errorf("MaxPerDomain = %d leaked onto the TSC's intent", *winner.MaxPerDomain)
	}
}
