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
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// This file is the **scenario corpus**: the first half of Phase 3's exit
// criterion in §14, "correct intent on the scenario corpus". Its sibling
// falsepositive_test.go is the second half.
//
// Every other test file in this package asks one question of one function.
// This one asks the question a user asks — "given this workload, what did
// leeway decide it wanted?" — of the whole of Resolve, on workload shapes that
// exist in real clusters. That makes it the file that catches a regression
// living in the seam between two correct functions: a source that outranks the
// wrong neighbour, an evidence trail dropped on demotion, a policy read off the
// struct instead of through the accessor.
//
// **Every fixture is an admitted *corev1.Pod.** Not a PodTemplateSpec, not a
// Deployment. Spike S3 measured GKE injecting tolerations at admission that the
// owning controller's template does not carry, so a template is a strictly
// weaker document than the object the scheduler placed — inferring from one
// computes too small an eligible set, concludes the workload is pinned, and
// suppresses drift that is really there. There is deliberately no
// template-shaped constructor in this package and this corpus does not
// introduce one.

// hostSpread is a TopologySpreadConstraint on the hostname axis, the second
// most common one after zone and the one that makes "intents on different keys
// coexist" testable.
func hostSpread(maxSkew int32, action corev1.UnsatisfiableConstraintAction) corev1.TopologySpreadConstraint {
	return corev1.TopologySpreadConstraint{
		MaxSkew:           maxSkew,
		TopologyKey:       hostKey,
		WhenUnsatisfiable: action,
		LabelSelector:     appSelector(),
	}
}

// taintedThirdZone is threeZones with both of zone-c's nodes carrying a
// NoSchedule taint. It exists so that nodeTaintsPolicy has something to change:
// the policy is invisible on a cluster with no taints, which is exactly how a
// regression in it would ship unnoticed.
func taintedThirdZone(t *testing.T) *Inventory {
	t.Helper()
	inv := threeZones(t)
	for _, name := range []string{"n5", "n6"} {
		inv.Upsert(node(name, "zone-c",
			withLabel(string(poolKey), "general"),
			withTaint("dedicated", "batch", corev1.TaintEffectNoSchedule)))
	}
	return inv
}

// batchToleration matches the taint taintedThirdZone applies.
func batchToleration() corev1.Toleration {
	return corev1.Toleration{
		Key:      "dedicated",
		Operator: corev1.TolerationOpEqual,
		Value:    "batch",
		Effect:   corev1.TaintEffectNoSchedule,
	}
}

// wantIntent is the resolved intent one corpus row expects on one axis.
//
// The pointer fields are asserted as pointers rather than as values: nil and
// zero are different answers everywhere in §5.1 — a nil MaxSkew means the
// source expressed no contract, a MaxSkew of zero would be a contract of zero —
// and a table that could not tell them apart would pass on either.
type wantIntent struct {
	mode         leeway.IntentMode
	source       leeway.IntentSource
	confidence   leeway.Confidence
	when         corev1.UnsatisfiableConstraintAction
	maxSkew      *int32
	minDomains   *int32
	maxPerDomain *int64

	// hardContract is what §8.1 gates Tier A on, and it is the one field that
	// is a consequence rather than a copy — a DoNotSchedule TSC has it, an
	// identical constraint supplied by an *assumed* cluster default does not.
	hardContract bool

	// policies is the *effective* policy pair, read through
	// EligibilityPolicies. Nil means kube-scheduler's defaults, which is what
	// every source but a TSC must produce: the zero value of
	// NodeInclusionPolicy is the empty string and §7.1 reads anything that is
	// not Honor as "do not apply", so an unset field travelling as itself would
	// stop nodeSelector being honoured and widen every pinned workload's
	// eligible set to the whole cluster.
	policies *leeway.NodeInclusionPolicies

	// demoted are the sources §5.1 says lose on this key and survive as
	// evidence. Losing a candidate silently is the failure mode that makes a
	// finding look wrong to the person reading it.
	demoted []leeway.IntentSource

	// evidenceContains is a substring the winning intent's evidence must carry,
	// for the cases where the *reason* leeway ignored a field is the point.
	evidenceContains string
}

type corpusCase struct {
	name string

	// inv is the cluster the workload sits in. Nil means threeZones.
	inv func(*testing.T) *Inventory

	// pod is the admitted object under inference.
	pod *corev1.Pod

	// want is the resolved intent per axis, and it is exhaustive: an axis
	// absent from the map must carry no intent at all. "No intent" is a
	// result, not a gap — inventing a neutral one would put a row on the wire
	// claiming the workload asked for something it did not.
	want map[leeway.TopologyKey]wantIntent

	// eligible is the eligible domain set on the zone axis, sorted, for the
	// rows where §7.1 is the point. Nil skips the check.
	eligible []string
}

// TestCorpus_ResolvedIntentPerAxis is the scenario corpus proper: one row per
// inference source, plus every precedence interaction §5.1 names.
func TestCorpus_ResolvedIntentPerAxis(t *testing.T) {
	zoneDoNotSchedule := tscPod(zoneSpread(1, corev1.DoNotSchedule))

	zoneScheduleAnyway := tscPod(zoneSpread(2, corev1.ScheduleAnyway))

	minDomainsHonoured := zoneSpread(1, corev1.DoNotSchedule)
	minDomainsHonoured.MinDomains = ptr(int32(5))

	minDomainsIgnored := zoneSpread(1, corev1.ScheduleAnyway)
	minDomainsIgnored.MinDomains = ptr(int32(5))

	taintsHonoured := zoneSpread(1, corev1.DoNotSchedule)
	taintsHonoured.NodeTaintsPolicy = ptr(corev1.NodeInclusionPolicyHonor)

	affinityIgnored := zoneSpread(1, corev1.DoNotSchedule)
	affinityIgnored.NodeAffinityPolicy = ptr(corev1.NodeInclusionPolicyIgnore)

	selectorNarrowed := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	selectorNarrowed.Spec.NodeSelector = map[string]string{string(poolKey): "gpu"}

	affinityIgnoredPod := tscPod(affinityIgnored)
	affinityIgnoredPod.Spec.NodeSelector = map[string]string{string(poolKey): "gpu"}

	tolerated := tscPod(taintsHonoured)
	tolerated.Spec.Tolerations = []corev1.Toleration{batchToleration()}

	bothWaysOnOneKey := affPod(&corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{selfTerm(string(zoneKey))},
		},
		PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{selfTerm(string(zoneKey))},
		},
	})

	constraintBeatsAffinity := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	constraintBeatsAffinity.Spec.Affinity = antiRequired(selfTerm(string(zoneKey)))

	twoAxes := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	twoAxes.Spec.Affinity = antiPreferred(100, selfTerm(hostKey))

	honourTaints := leeway.NodeInclusionPolicies{
		NodeAffinityPolicy: corev1.NodeInclusionPolicyHonor,
		NodeTaintsPolicy:   corev1.NodeInclusionPolicyHonor,
	}
	ignoreAffinity := leeway.NodeInclusionPolicies{
		NodeAffinityPolicy: corev1.NodeInclusionPolicyIgnore,
		NodeTaintsPolicy:   corev1.NodeInclusionPolicyIgnore,
	}

	cases := []corpusCase{{
		// FR-4, the shape most workloads that have thought about topology
		// carry. DoNotSchedule is the API's default for an unset field, and it
		// is the only reading that makes maxSkew a contract §8.1 can escalate
		// on.
		name: "zonal spread constraint, DoNotSchedule",
		pod:  zoneDoNotSchedule,
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:         leeway.ModeSpread,
				source:       leeway.SourceTopologySpreadConstraint,
				confidence:   leeway.ConfidenceDeclared,
				when:         corev1.DoNotSchedule,
				maxSkew:      ptr(int32(1)),
				hardContract: true,
			},
		},
		eligible: []string{"zone-a", "zone-b", "zone-c"},
	}, {
		// Same object, one field different, and the consequence is the tier a
		// finding can reach. ScheduleAnyway is advisory: the scheduler will
		// break it rather than leave a pod pending, so a violation is a
		// deviation to explain and not a broken promise.
		name: "zonal spread constraint, ScheduleAnyway",
		pod:  zoneScheduleAnyway,
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:       leeway.ModeSpread,
				source:     leeway.SourceTopologySpreadConstraint,
				confidence: leeway.ConfidenceDeclared,
				when:       corev1.ScheduleAnyway,
				maxSkew:    ptr(int32(2)),
			},
		},
	}, {
		// minDomains is honoured only alongside DoNotSchedule, and honouring it
		// pads the eligible set with present-with-zero domains so the shortfall
		// is visible instead of showing as a perfect skew over the zones that
		// do exist.
		name: "minDomains honoured under DoNotSchedule",
		pod:  tscPod(minDomainsHonoured),
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:         leeway.ModeSpread,
				source:       leeway.SourceTopologySpreadConstraint,
				confidence:   leeway.ConfidenceDeclared,
				when:         corev1.DoNotSchedule,
				maxSkew:      ptr(int32(1)),
				minDomains:   ptr(int32(5)),
				hardContract: true,
			},
		},
		eligible: []string{"__absent__/3", "__absent__/4", "zone-a", "zone-b", "zone-c"},
	}, {
		// The other half, and the one that would manufacture skew out of
		// nothing if it were carried: the scheduler ignores minDomains on a
		// ScheduleAnyway constraint, so padding the eligible set with two
		// synthetic empty zones would charge the workload for domains no
		// placement was ever going to fill.
		name: "minDomains ignored under ScheduleAnyway",
		pod:  tscPod(minDomainsIgnored),
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:             leeway.ModeSpread,
				source:           leeway.SourceTopologySpreadConstraint,
				confidence:       leeway.ConfidenceDeclared,
				when:             corev1.ScheduleAnyway,
				maxSkew:          ptr(int32(1)),
				evidenceContains: "minDomains ignored",
			},
		},
		eligible: []string{"zone-a", "zone-b", "zone-c"},
	}, {
		// The §5.1 Policies hazard, stated as a corpus row because it is
		// invisible at every other layer. Neither policy is set, so the
		// *effective* pair must be kube-scheduler's defaults — Honor affinity,
		// Ignore taints — and not the struct's zero value, which §7.1 would
		// read as "honour nothing".
		name: "node-inclusion policies unset default to kube-scheduler's",
		pod:  selectorNarrowed,
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:         leeway.ModeSpread,
				source:       leeway.SourceTopologySpreadConstraint,
				confidence:   leeway.ConfidenceDeclared,
				when:         corev1.DoNotSchedule,
				maxSkew:      ptr(int32(1)),
				hardContract: true,
			},
		},
		// The default honours nodeSelector, so the gpu-less zone is gone.
		eligible: []string{"zone-a", "zone-b"},
	}, {
		// nodeTaintsPolicy=Honor is the opt-in that makes a taint an
		// eligibility bar. Both of zone-c's nodes are tainted and the pod
		// tolerates nothing, so the zone leaves the eligible set.
		name: "nodeTaintsPolicy Honor drops a tainted zone",
		inv:  taintedThirdZone,
		pod:  tscPod(taintsHonoured),
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:         leeway.ModeSpread,
				source:       leeway.SourceTopologySpreadConstraint,
				confidence:   leeway.ConfidenceDeclared,
				when:         corev1.DoNotSchedule,
				maxSkew:      ptr(int32(1)),
				hardContract: true,
				policies:     &honourTaints,
			},
		},
		eligible: []string{"zone-a", "zone-b"},
	}, {
		// The same opt-in from the other side: the pod tolerates the taint, so
		// Honor changes nothing and the zone stays. Without this row the one
		// above would also pass on an implementation that dropped every tainted
		// domain regardless of toleration.
		name: "nodeTaintsPolicy Honor keeps a tolerated zone",
		inv:  taintedThirdZone,
		pod:  tolerated,
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:         leeway.ModeSpread,
				source:       leeway.SourceTopologySpreadConstraint,
				confidence:   leeway.ConfidenceDeclared,
				when:         corev1.DoNotSchedule,
				maxSkew:      ptr(int32(1)),
				hardContract: true,
				policies:     &honourTaints,
			},
		},
		eligible: []string{"zone-a", "zone-b", "zone-c"},
	}, {
		// nodeAffinityPolicy=Ignore tells the scheduler to compute skew over
		// every node whether or not the workload could land on it, and §7.1
		// mirrors that: the gpu selector stops narrowing and all three zones
		// come back. Note the *pair* that results — Ignore/Ignore — because the
		// taints half was never set and still has to default.
		name: "nodeAffinityPolicy Ignore widens past the nodeSelector",
		pod:  affinityIgnoredPod,
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:         leeway.ModeSpread,
				source:       leeway.SourceTopologySpreadConstraint,
				confidence:   leeway.ConfidenceDeclared,
				when:         corev1.DoNotSchedule,
				maxSkew:      ptr(int32(1)),
				hardContract: true,
				policies:     &ignoreAffinity,
			},
		},
		eligible: []string{"zone-a", "zone-b", "zone-c"},
	}, {
		// FR-5 required. The contract is a ceiling of one pod per domain and
		// deliberately not a MaxSkew of one: a skew bound of one is satisfied
		// by two pods in every zone, which the anti-affinity forbids outright.
		name: "required podAntiAffinity is a per-domain ceiling",
		pod:  affPod(antiRequired(selfTerm(string(zoneKey)))),
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:         leeway.ModeSpread,
				source:       leeway.SourcePodAntiAffinityRequired,
				confidence:   leeway.ConfidenceInferred,
				when:         corev1.DoNotSchedule,
				maxPerDomain: ptr(int64(1)),
				hardContract: true,
			},
		},
	}, {
		// FR-5 preferred, which spike S3's live sample says is what most
		// workloads actually carry. No ceiling, because the scheduler will
		// happily stack two of them on one node to get the pod running.
		name: "preferred podAntiAffinity is spread without a ceiling",
		pod:  affPod(antiPreferred(100, selfTerm(string(zoneKey)))),
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:       leeway.ModeSpread,
				source:     leeway.SourcePodAntiAffinityPreferred,
				confidence: leeway.ConfidenceInferred,
				when:       corev1.ScheduleAnyway,
			},
		},
	}, {
		// FR-6: for these subjects concentration is desired and spread is the
		// anomaly. The required form gets no contract even though the scheduler
		// enforces it just as hard — an affinity satisfied at every placement
		// broke no promise, so §8.1 puts it in Tier B. WhenUnsatisfiable stays
		// empty, which is honest: a colocation intent makes no statement about
		// what the scheduler does when it cannot be met.
		name: "required podAffinity is colocation without a contract",
		pod:  affPod(affRequired(selfTerm(string(zoneKey)))),
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:       leeway.ModeColocate,
				source:     leeway.SourcePodAffinityRequired,
				confidence: leeway.ConfidenceInferred,
			},
		},
	}, {
		name: "preferred podAffinity is colocation",
		pod: affPod(&corev1.Affinity{PodAffinity: &corev1.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{Weight: 50, PodAffinityTerm: selfTerm(string(zoneKey))},
			},
		}}),
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:       leeway.ModeColocate,
				source:     leeway.SourcePodAffinityPreferred,
				confidence: leeway.ConfidenceInferred,
			},
		},
	}, {
		// A pod declaring both on one key is a contradiction the scheduler
		// resolves by refusing to place it at all. §5.1 resolves it here to the
		// spread reading — anti-affinity-required outranks affinity-required —
		// and keeps the colocation as evidence, because a finding that says
		// "spread" about a workload whose manifest says "together" has to be
		// able to show it saw both.
		name: "spread beats colocation on one key, colocation retained",
		pod:  bothWaysOnOneKey,
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:         leeway.ModeSpread,
				source:       leeway.SourcePodAntiAffinityRequired,
				confidence:   leeway.ConfidenceInferred,
				when:         corev1.DoNotSchedule,
				maxPerDomain: ptr(int64(1)),
				hardContract: true,
				demoted:      []leeway.IntentSource{leeway.SourcePodAffinityRequired},
			},
		},
	}, {
		// A TopologySpreadConstraint outranks every affinity form, so the
		// declared maxSkew replaces the inferred ceiling. Worth stating
		// explicitly that MaxPerDomain does *not* survive the merge: the
		// intents are alternatives, not layers, and the demoted term's ceiling
		// lives on as evidence rather than as a second contract the winner
		// silently inherited.
		name: "a spread constraint beats an affinity term on the same key",
		pod:  constraintBeatsAffinity,
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:         leeway.ModeSpread,
				source:       leeway.SourceTopologySpreadConstraint,
				confidence:   leeway.ConfidenceDeclared,
				when:         corev1.DoNotSchedule,
				maxSkew:      ptr(int32(1)),
				hardContract: true,
				demoted:      []leeway.IntentSource{leeway.SourcePodAntiAffinityRequired},
			},
		},
	}, {
		// Intents on different keys coexist, which is the half of §5.1 that
		// stops precedence from being a single global winner. Spreading across
		// zones and preferring one pod per node are compatible statements and
		// both have to survive.
		name: "intents on different keys coexist",
		pod:  twoAxes,
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:         leeway.ModeSpread,
				source:       leeway.SourceTopologySpreadConstraint,
				confidence:   leeway.ConfidenceDeclared,
				when:         corev1.DoNotSchedule,
				maxSkew:      ptr(int32(1)),
				hardContract: true,
			},
			hostKey: {
				mode:       leeway.ModeSpread,
				source:     leeway.SourcePodAntiAffinityPreferred,
				confidence: leeway.ConfidenceInferred,
				when:       corev1.ScheduleAnyway,
			},
		},
	}, {
		// FR-7 on its own, with no policy field in sight: a workload confined
		// by nodeSelector to the two zones that have gpu nodes is not drifting
		// because the third is empty. §7.1 calls this the single rule that
		// removes the largest class of false positives.
		name: "nodeSelector narrows the eligible set",
		pod:  selectorNarrowed,
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:         leeway.ModeSpread,
				source:       leeway.SourceTopologySpreadConstraint,
				confidence:   leeway.ConfidenceDeclared,
				when:         corev1.DoNotSchedule,
				maxSkew:      ptr(int32(1)),
				hardContract: true,
			},
		},
		eligible: []string{"zone-a", "zone-b"},
	}, {
		// A pod that declared nothing has no intent, and that is a result
		// rather than a gap. It still has an eligible set, which is why
		// Resolve computes one for every tracked axis.
		name:     "a pod that declared nothing has no intent",
		pod:      tscPod(),
		want:     map[leeway.TopologyKey]wantIntent{},
		eligible: []string{"zone-a", "zone-b", "zone-c"},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			build := tc.inv
			if build == nil {
				build = threeZones
			}
			inv := build(t)

			// noClusterDefaults is the DECLARED EMPTY state, so nothing in this
			// table can have its intent quietly supplied by FR-9. The cluster
			// defaults have their own table below.
			res := Resolve(tc.pod, inv, noClusterDefaults)

			checkIntents(t, res, tc.want)
			if tc.eligible != nil {
				if got := domains(res.Eligible[zoneKey]); !slices.Equal(got, tc.eligible) {
					t.Errorf("eligible zones = %v, want %v", got, tc.eligible)
				}
			}
		})
	}
}

// TestCorpus_ClusterDefaults is FR-9 as its own table, because the three-state
// flag cannot be expressed in the table above: there a nil means "this row said
// nothing about defaults", and here it means UNSET, which is the whole point of
// the distinction spike S4 established.
func TestCorpus_ClusterDefaults(t *testing.T) {
	declaredHard := corev1.TopologySpreadConstraint{
		TopologyKey:       string(zoneKey),
		MaxSkew:           2,
		WhenUnsatisfiable: corev1.DoNotSchedule,
	}

	cases := []struct {
		name     string
		pod      *corev1.Pod
		defaults *[]corev1.TopologySpreadConstraint
		want     map[leeway.TopologyKey]wantIntent
	}{{
		// UNSET. Nobody told us, so the upstream System set is assumed, and the
		// assumption is visible in both Source and Confidence. maxSkew 5 on the
		// zone axis is upstream's number and not anyone in this cluster's,
		// which is exactly why the intent can never reach Tier A.
		//
		// The upstream set also names kubernetes.io/hostname, and that intent
		// is dropped rather than resolved: onlyTrackedAxes filters cluster
		// defaults to the counted axes because, unlike a constraint a human
		// wrote, this one would land on every subject in the cluster.
		name:     "unset assumes the upstream System defaults",
		pod:      tscPod(),
		defaults: nil,
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:             leeway.ModeSpread,
				source:           leeway.SourceClusterDefaultAssumed,
				confidence:       leeway.ConfidenceAssumed,
				when:             corev1.ScheduleAnyway,
				maxSkew:          ptr(int32(5)),
				evidenceContains: "ASSUMED cluster default",
			},
		},
	}, {
		// DECLARED. An operator asserting a DoNotSchedule default is making an
		// assertion of their own, so this one *can* raise a Tier A finding —
		// the same object shape as the row above, opposite tier, and the only
		// difference is who said it.
		name:     "declared defaults are trusted",
		pod:      tscPod(),
		defaults: declaredDefaults(declaredHard),
		want: map[leeway.TopologyKey]wantIntent{
			zoneKey: {
				mode:             leeway.ModeSpread,
				source:           leeway.SourceClusterDefaultDeclared,
				confidence:       leeway.ConfidenceDeclared,
				when:             corev1.DoNotSchedule,
				maxSkew:          ptr(int32(2)),
				hardContract:     true,
				evidenceContains: "configured cluster default",
			},
		},
	}, {
		// DECLARED EMPTY is an answer, not a gap: the operator says this
		// cluster configures no defaults, so a pod that declared nothing has no
		// intent at all rather than an assumed one.
		name:     "declared empty produces no intent",
		pod:      tscPod(),
		defaults: noClusterDefaults,
		want:     map[leeway.TopologyKey]wantIntent{},
	}, {
		// The blast-radius rule, and the reason a wrong assumption cannot
		// corrupt a workload that expressed intent: one constraint on the pod
		// and kube-scheduler ignores the cluster defaults entirely. The pod
		// constrains the hostname axis and says nothing about zones, and the
		// declared zone default still does not appear.
		name:     "a pod with its own constraint ignores the defaults entirely",
		pod:      tscPod(hostSpread(1, corev1.DoNotSchedule)),
		defaults: declaredDefaults(declaredHard),
		want: map[leeway.TopologyKey]wantIntent{
			hostKey: {
				mode:         leeway.ModeSpread,
				source:       leeway.SourceTopologySpreadConstraint,
				confidence:   leeway.ConfidenceDeclared,
				when:         corev1.DoNotSchedule,
				maxSkew:      ptr(int32(1)),
				hardContract: true,
			},
		},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checkIntents(t, Resolve(tc.pod, threeZones(t), tc.defaults), tc.want)
		})
	}
}

// TestCorpus_ZonalVolumePinsAPodWithoutNarrowingItsIntent is FR-8 in the
// corpus, because pinning is a fact about one pod and intent is a statement
// about the subject — and the two must not be allowed to leak into each other.
//
// A StatefulSet replica holding a zonal disk cannot move, but the *workload*
// is still free to place its other replicas anywhere: nothing about the bound
// volume narrows the eligible set, and an implementation that let it would
// score every zonal-disk StatefulSet against the single zone its first replica
// happened to land in.
func TestCorpus_ZonalVolumePinsAPodWithoutNarrowingItsIntent(t *testing.T) {
	inv := threeZones(t)
	pod := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	withClaim("data-api-abc")(pod)

	zonal := affinityPV(term(string(zoneKey), corev1.NodeSelectorOpIn, "zone-a"))
	if !VolumePins(oneVolume("data-api-abc", zonal), inv.Keys())(pod) {
		t.Error("a pod bound to a zonal disk did not read as pinned")
	}

	res := Resolve(pod, inv, noClusterDefaults)
	if got, want := domains(res.Eligible[zoneKey]), []string{"zone-a", "zone-b", "zone-c"}; !slices.Equal(got, want) {
		t.Errorf("eligible zones = %v, want %v — a pinned pod must not narrow the subject's eligible set", got, want)
	}
	if got := res.Intents[zoneKey]; got == nil || got.MaxSkew == nil || *got.MaxSkew != 1 {
		t.Errorf("zone intent = %+v, want the pod's own maxSkew=1 constraint", got)
	}

	// The mirror case, and the reason FR-8's rule is narrower than "carries
	// nodeAffinity": a volume constrained to linux pins nothing, because every
	// domain on every scored axis still holds a linux node.
	unscoped := affinityPV(term(corev1.LabelOSStable, corev1.NodeSelectorOpIn, "linux"))
	if VolumePins(oneVolume("data-api-abc", unscoped), inv.Keys())(pod) {
		t.Error("a volume constrained only to linux read as pinned; it confines no scored axis")
	}
}

// checkIntents asserts the resolved intent map exhaustively. Exhaustively is
// the point: a row that only checked the axes it named would pass on an
// implementation that invented an intent on a third one.
func checkIntents(t *testing.T, res Resolution, want map[leeway.TopologyKey]wantIntent) {
	t.Helper()

	if len(res.Intents) != len(want) {
		t.Errorf("resolved %d intents, want %d: got %v, want %v",
			len(res.Intents), len(want), leeway.SortedKeys(res.Intents), sortedWantKeys(want))
	}
	for key, w := range want {
		got, ok := res.Intents[key]
		if !ok {
			t.Errorf("%s: no intent resolved", key)
			continue
		}
		checkIntent(t, key, got, w)
	}
	for _, key := range leeway.SortedKeys(res.Intents) {
		if _, expected := want[key]; !expected {
			t.Errorf("%s: unexpected intent %s/%s", key, res.Intents[key].Source, res.Intents[key].Mode)
		}
	}
}

func checkIntent(t *testing.T, key leeway.TopologyKey, got *leeway.Intent, want wantIntent) {
	t.Helper()

	if got.Mode != want.mode {
		t.Errorf("%s: Mode = %v, want %v", key, got.Mode, want.mode)
	}
	if got.Source != want.source {
		t.Errorf("%s: Source = %v, want %v", key, got.Source, want.source)
	}
	if got.Confidence != want.confidence {
		t.Errorf("%s: Confidence = %v, want %v", key, got.Confidence, want.confidence)
	}
	if got.WhenUnsatisfiable != want.when {
		t.Errorf("%s: WhenUnsatisfiable = %q, want %q", key, got.WhenUnsatisfiable, want.when)
	}
	if !eqPtr(got.MaxSkew, want.maxSkew) {
		t.Errorf("%s: MaxSkew = %s, want %s", key, showPtr(got.MaxSkew), showPtr(want.maxSkew))
	}
	if !eqPtr(got.MinDomains, want.minDomains) {
		t.Errorf("%s: MinDomains = %s, want %s", key, showPtr(got.MinDomains), showPtr(want.minDomains))
	}
	if !eqPtr(got.MaxPerDomain, want.maxPerDomain) {
		t.Errorf("%s: MaxPerDomain = %s, want %s", key, showPtr(got.MaxPerDomain), showPtr(want.maxPerDomain))
	}
	if got.HardContract() != want.hardContract {
		t.Errorf("%s: HardContract() = %v, want %v — this is the §8.1 Tier A gate",
			key, got.HardContract(), want.hardContract)
	}

	wantPolicies := leeway.DefaultNodeInclusionPolicies()
	if want.policies != nil {
		wantPolicies = *want.policies
	}
	if got.EligibilityPolicies() != wantPolicies {
		t.Errorf("%s: EligibilityPolicies() = %+v, want %+v", key, got.EligibilityPolicies(), wantPolicies)
	}

	if got, want := demotedSources(got), want.demoted; !slices.Equal(got, want) {
		t.Errorf("%s: demoted sources = %v, want %v — §5.1 keeps the losers as evidence", key, got, want)
	}
	if want.evidenceContains != "" && !evidenceContains(got, want.evidenceContains) {
		t.Errorf("%s: no evidence item mentions %q; evidence = %v",
			key, want.evidenceContains, renderEvidence(got))
	}
}

// demotedSources returns the distinct sources §5.1 demoted on this key, in
// precedence order. Reading them off the Ignored flag rather than off the
// detail string is deliberate: the flag is what a finding renderer will branch
// on, so it is the thing worth pinning.
func demotedSources(in *leeway.Intent) []leeway.IntentSource {
	var out []leeway.IntentSource
	for _, e := range in.Evidence {
		if e.Ignored && !slices.Contains(out, e.Source) {
			out = append(out, e.Source)
		}
	}
	slices.Sort(out)
	return out
}

func evidenceContains(in *leeway.Intent, substr string) bool {
	for _, e := range in.Evidence {
		if strings.Contains(e.Detail, substr) {
			return true
		}
	}
	return false
}

func renderEvidence(in *leeway.Intent) []string {
	out := make([]string, 0, len(in.Evidence))
	for _, e := range in.Evidence {
		out = append(out, fmt.Sprintf("[%s ignored=%v] %s", e.Source, e.Ignored, e.Detail))
	}
	return out
}

func sortedWantKeys(want map[leeway.TopologyKey]wantIntent) []leeway.TopologyKey {
	out := make([]leeway.TopologyKey, 0, len(want))
	for k := range want {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// eqPtr compares two optional values, treating nil as a value in its own right
// rather than as a missing one.
func eqPtr[T comparable](got, want *T) bool {
	if got == nil || want == nil {
		return got == want
	}
	return *got == *want
}

func showPtr[T any](v *T) string {
	if v == nil {
		return "nil"
	}
	return fmt.Sprintf("%v", *v)
}
