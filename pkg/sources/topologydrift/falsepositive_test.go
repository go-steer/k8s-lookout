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

// This file is the **false-positive corpus** (§12): a fixture set of clusters
// that are *fine*. Reporting on any of them is a test failure, and the set
// grows with every false positive found in production.
//
// # What "reporting" means at Phase 3
//
// Nothing emits a finding yet — the state machine is Phase 4 — so the
// assertion is made at the last layer that exists, which is the verdict a
// finding would be built from: Resolve for the intent and the eligible set,
// then Apportion, Score and Scores.Breach. If Breach says false on all of
// these, no Phase 4 state machine can page anybody about them. When Phase 4
// lands, these fixtures should be re-pointed at the finding pipeline rather
// than rewritten: the clusters are the asset, not the plumbing.
//
// # Why each fixture is built from admitted pods
//
// Same reason as the scenario corpus, and it matters more here. Spike S3 found
// GKE injecting tolerations at admission, so eligibility computed from a
// controller's template is too narrow — which in this file would make the
// fixtures pass for the wrong reason, by pinning workloads that are not
// pinned. Every pod below is an admitted *corev1.Pod with a NodeName.
//
// # Deliberately absent
//
// §12 also names a compute-class fixture ("a class-pinned workload whose class
// exists in one zone reports zero drift, not maximal skew"). That is Phase 6 —
// the ComputeClass informer and the rank extractors do not exist yet — so it is
// skipped here rather than faked. Its eligibility half is already covered by
// the restricted-eligibility fixture below, which is the same §7.1 rule with a
// nodeSelector instead of a compute class.

// zoneSpec describes one zone's worth of nodes in a fixture cluster.
type zoneSpec struct {
	zone  string
	nodes int
	opts  []nodeOpt
}

// zonedInventory builds a zone-axis inventory, naming the i-th node of a zone
// "<zone>-<i>" so a fixture can place a pod by reading its own zone layout.
//
// Only the zone axis is tracked. The fixtures still use node *labels* other
// than the zone — a nodeSelector reads labels directly and does not care
// whether they are a scored axis — but tracking one axis keeps the arithmetic
// in each fixture small enough to state in its comment, which is the whole
// value of a corpus you are meant to read when it goes red.
func zonedInventory(t *testing.T, zones ...zoneSpec) *Inventory {
	t.Helper()
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	for _, z := range zones {
		for i := range z.nodes {
			inv.Upsert(node(fmt.Sprintf("%s-%d", z.zone, i), z.zone, z.opts...))
		}
	}
	return inv
}

// replicas clones one admitted pod onto a list of nodes, which is what a
// subject's pods are: the same admitted spec, N times, each bound somewhere.
//
// Cloning an admitted Pod is not the same thing as reading a PodTemplateSpec.
// The object being cloned already carries whatever admission injected, so the
// S3 invariant holds — what would break it is deriving the spec from the
// owning controller, and nothing here does.
func replicas(rep *corev1.Pod, nodes ...string) []*corev1.Pod {
	out := make([]*corev1.Pod, 0, len(nodes))
	for i, n := range nodes {
		p := rep.DeepCopy()
		p.Name = fmt.Sprintf("%s-%d", rep.Name, i)
		p.Spec.NodeName = n
		out = append(out, p)
	}
	return out
}

// domainOf is the domain a placed pod occupies on one axis, read back through
// the inventory the same way State's delta path reads it.
func domainOf(inv *Inventory, key leeway.TopologyKey, nodeName string) leeway.Domain {
	ordinal, ok := inv.Ordinal(key)
	if !ok {
		return leeway.DomainUnknown
	}
	doms := inv.DomainsOf(nodeName)
	if ordinal >= len(doms) {
		return leeway.DomainUnknown
	}
	return doms[ordinal]
}

// fpFixture is one cluster that is fine.
type fpFixture struct {
	name string
	inv  *Inventory

	// rep is the admitted pod intent is inferred from, and pods is every
	// placed pod of the subject. They are separate because inference reads one
	// object and measurement reads all of them — the same split the source
	// makes between a representative lookup and the counters.
	rep  *corev1.Pod
	pods []*corev1.Pod

	// caps supplies §7.2's per-domain ceilings. Nil means uncapped. §5.1 says
	// callers turn an intent's MaxPerDomain into these once §7.1 has decided
	// how many domains there are; that wiring is Phase 4, so the one fixture
	// that needs it does the conversion inline.
	caps func(leeway.Eligibility) []int64

	// also is the fixture's own assertion — the thing that proves it is
	// testing what its name claims rather than passing by accident.
	also func(t *testing.T, got fpResult)
}

// fpResult is one fixture scored on one axis.
type fpResult struct {
	intent   *leeway.Intent
	eligible leeway.Eligibility
	ap       leeway.Apportionment
	scores   leeway.Scores
	verdict  leeway.Verdict
	breach   bool
	reason   string
}

// String renders the whole verdict, because a red line in this file has to say
// why on its own: the point of the corpus is that somebody reading the failure
// months from now can tell whether the cluster changed or the scoring did.
func (r fpResult) String() string {
	return fmt.Sprintf("eligible=%v actual=%v expected=%v S=%d S*=%d E=%d R=%d rho=%.3f maxShare=%.3f gate=%q reason=%q",
		r.eligible.Domains, r.scores.Actual, r.scores.Expected,
		r.scores.ObservedSkew, r.scores.MinAchievableSkew, r.scores.ExcessSkew,
		r.scores.Relocation, r.scores.Drift, r.scores.MaxDomainShare,
		r.scores.Gate, r.reason)
}

// scoreSubject runs a fixture through the production scoring pass: inference,
// eligibility, apportionment, scoring and the threshold verdict.
//
// It builds the distribution and then calls ScoreAxis, rather than reproducing
// the chain. The corpus's whole claim is that the shipped code does not report
// on these clusters, and a corpus that assembled its own version of the pass
// could keep passing while the source scored something else entirely — which
// is exactly what happened to the caps conversion, inline here until capsFor
// existed to do it.
func scoreSubject(t *testing.T, f fpFixture, key leeway.TopologyKey) fpResult {
	t.Helper()

	// Declared-empty defaults throughout. A fixture that is fine must be fine
	// because of what its pods say, not because an assumed cluster default
	// happened to supply a lenient maxSkew.
	res := Resolve(f.rep, f.inv, ResolveConfig{ClusterDefaults: noClusterDefaults})

	dist := leeway.NewDistribution()
	for _, p := range f.pods {
		dist.Add(domainOf(f.inv, key, p.Spec.NodeName), leeway.StateRunning, false)
	}

	intent := res.Intents[key]
	eligible := res.Eligible[key]
	if f.caps != nil {
		intent = withDomainCaps(intent, eligible, f.caps(eligible))
	}

	// Judge rather than Breach, which is what ScoreAxis runs: from Phase 4 on,
	// the tier is part of what the corpus asserts is absent. A fixture that
	// stopped breaching but started producing a Tier A verdict would pass a
	// boolean check.
	ev := ScoreAxis(key, intent, eligible, dist, leeway.DefaultThresholds())
	return fpResult{
		intent:   ev.Intent,
		eligible: ev.Eligible,
		ap:       ev.Apportionment,
		scores:   ev.Scores,
		verdict:  ev.Verdict,
		breach:   ev.Verdict.Breached,
		reason:   ev.Verdict.Reason,
	}
}

// withDomainCaps copies an intent with a per-domain ceiling attached, so a
// fixture can express a ceiling no inference source produces yet.
//
// Setting DomainCaps rather than handing ScoreAxis a caps slice is the point:
// the conversion from a declared ceiling to §7.2's index-aligned slice is
// capsFor's job, and a fixture that bypassed it would stop testing it.
func withDomainCaps(in *leeway.Intent, e leeway.Eligibility, caps []int64) *leeway.Intent {
	out := &leeway.Intent{}
	if in != nil {
		*out = *in
	}
	out.DomainCaps = make(map[leeway.Domain]int64, len(caps))
	for i, d := range e.Domains {
		if i < len(caps) {
			out.DomainCaps[d] = caps[i]
		}
	}
	return out
}

// TestFalsePositiveCorpus_NoneOfTheseClustersIsDrifting is §12's rule as a
// test: reporting on any of them is a failure.
func TestFalsePositiveCorpus_NoneOfTheseClustersIsDrifting(t *testing.T) {
	for _, f := range falsePositiveCorpus(t) {
		t.Run(f.name, func(t *testing.T) {
			got := scoreSubject(t, f, zoneKey)
			if got.breach {
				t.Errorf("reported drift on a cluster that is fine: %s", got)
			}
			if got.verdict.Tier != leeway.TierNone {
				t.Errorf("verdict carries tier %v on a cluster that is fine: %s", got.verdict.Tier, got)
			}
			if f.also != nil {
				f.also(t, got)
			}
		})
	}
}

func falsePositiveCorpus(t *testing.T) []fpFixture {
	t.Helper()
	return []fpFixture{
		unavoidableSkew(t),
		restrictedEligibility(t),
		pinnedVolumes(t),
		midRollout(t),
		smallN(t),
		cappedDomains(t),
		deliberateColocation(t),
	}
}

// deliberateColocation: a workload that asked to be in one zone, and is.
//
// This is the most embarrassing false positive available, because the subject
// is not merely fine — it is doing exactly and only what it was told to do,
// and it maximises every spread metric by doing so. Judged against a spread
// expectation a perfectly colocated subject scores ρ = 1 − 1/m, which is over
// any threshold anyone would set, for as long as the workload exists. It would
// have been the first finding a user with a cache-affinity deployment ever saw.
//
// §8.1's inversion is what saves it: a ModeColocate intent is judged on
// dispersion, the share of objects *outside* the fullest domain, so the right
// answer here is zero.
func deliberateColocation(t *testing.T) fpFixture {
	inv := zonedInventory(t,
		zoneSpec{zone: "zone-a", nodes: 2},
		zoneSpec{zone: "zone-b", nodes: 2},
		zoneSpec{zone: "zone-c", nodes: 2},
	)
	rep := affPod(affRequired(selfTerm(corev1.LabelTopologyZone)))

	return fpFixture{
		name: "deliberate colocation: a podAffinity workload in one zone",
		inv:  inv,
		rep:  rep,
		pods: replicas(rep, "zone-a-0", "zone-a-0", "zone-a-1", "zone-a-1", "zone-a-1", "zone-a-1"),
		also: func(t *testing.T, got fpResult) {
			if got.intent == nil || got.intent.Mode != leeway.ModeColocate {
				t.Fatalf("intent = %+v, want a colocation intent — the fixture is not testing what it claims", got.intent)
			}

			// The counterfactual: the same numbers under the spread rule. It
			// must breach, and loudly. If this goes quiet the fixture above has
			// stopped proving anything.
			spread := *got.intent
			spread.Mode = leeway.ModeSpread
			if v := got.scores.Judge(&spread, leeway.DefaultThresholds()); !v.Breached {
				t.Errorf("the same placement under the spread rule did not breach (%q); "+
					"the mode inversion is no longer what is saving this fixture", v.Reason)
			}
			if got.scores.Dispersion() != 0 {
				t.Errorf("dispersion = %v, want 0 — every pod is in the fullest domain: %s",
					got.scores.Dispersion(), got)
			}
		},
	}
}

// unavoidableSkew: four pods over three zones. No placement divides evenly, so
// one zone holds two and a max-min skew of one is the best any scheduler can
// do. S* is what has to absorb it — the expectation is [2,1,1], the same shape
// as the actual, so there is nothing to relocate and no excess over the
// declared maxSkew.
func unavoidableSkew(t *testing.T) fpFixture {
	inv := zonedInventory(t,
		zoneSpec{zone: "zone-a", nodes: 2},
		zoneSpec{zone: "zone-b", nodes: 2},
		zoneSpec{zone: "zone-c", nodes: 2},
	)
	rep := tscPod(zoneSpread(1, corev1.DoNotSchedule))

	return fpFixture{
		name: "unavoidable skew: 4 pods over 3 zones",
		inv:  inv,
		rep:  rep,
		pods: replicas(rep, "zone-a-0", "zone-a-1", "zone-b-0", "zone-c-0"),
		also: func(t *testing.T, got fpResult) {
			if got.scores.MinAchievableSkew != 1 {
				t.Errorf("S* = %d, want 1 — the arithmetic floor is what makes this cluster fine: %s",
					got.scores.MinAchievableSkew, got)
			}
			if got.scores.ObservedSkew != 1 || got.scores.ExcessSkew != 0 {
				t.Errorf("S = %d, E = %d, want the observed skew fully absorbed by the floor: %s",
					got.scores.ObservedSkew, got.scores.ExcessSkew, got)
			}
			if got.scores.Relocation != 0 {
				t.Errorf("R = %d, want 0 — no pod has anywhere better to be: %s", got.scores.Relocation, got)
			}
		},
	}
}

// restrictedEligibility is the most important fixture in the file, and §7.1
// calls the rule behind it the single change that removes the largest class of
// false positives.
//
// Six pods, all in zone-a, on a cluster with three zones. Scored against the
// cluster that is maximal skew and a violated maxSkew — a confident, critical,
// completely wrong finding. Scored against the zones the workload is actually
// allowed into, it is one eligible domain holding all six pods: a perfect
// placement, which is what it is. The workload is correct, not drifting.
func restrictedEligibility(t *testing.T) fpFixture {
	inv := zonedInventory(t,
		zoneSpec{zone: "zone-a", nodes: 2, opts: []nodeOpt{withLabel(string(poolKey), "gpu")}},
		zoneSpec{zone: "zone-b", nodes: 2, opts: []nodeOpt{withLabel(string(poolKey), "general")}},
		zoneSpec{zone: "zone-c", nodes: 2, opts: []nodeOpt{withLabel(string(poolKey), "general")}},
	)
	rep := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	rep.Spec.NodeSelector = map[string]string{string(poolKey): "gpu"}

	return fpFixture{
		name: "restricted eligibility: a nodeSelector leaves one zone",
		inv:  inv,
		rep:  rep,
		pods: replicas(rep, "zone-a-0", "zone-a-0", "zone-a-0", "zone-a-1", "zone-a-1", "zone-a-1"),
		also: func(t *testing.T, got fpResult) {
			if want := []leeway.Domain{"zone-a"}; !slices.Equal(got.eligible.Domains, want) {
				t.Fatalf("eligible = %v, want %v — the fixture is not testing what it claims", got.eligible.Domains, want)
			}

			// The counterfactual, which is what makes this fixture worth
			// having: the same pods scored over the whole cluster, as they
			// would be if §7.1 were skipped or regressed. It must breach. If
			// this assertion ever goes red, the fixture above has stopped
			// proving anything and is passing for some other reason.
			naive := leeway.EligibleDomains(
				unnarrowedViews("zone-a", "zone-b", "zone-c"),
				leeway.DefaultEligibilityOptions())
			ap := leeway.Apportion(6, leeway.EqualWeights(len(naive.Domains)), nil)
			scores := leeway.Score(naive.Domains, []int64{6, 0, 0}, ap, got.intent.MaxSkew, leeway.DefaultThresholds())
			if breach, reason := scores.Breach(got.intent, leeway.DefaultThresholds()); !breach {
				t.Errorf("scoring the same pods over all three zones did not breach (%q); "+
					"the eligible-set narrowing is no longer what is saving this fixture", reason)
			}
		},
	}
}

// unnarrowedViews fabricates the NodeViews a cluster-wide eligibility
// computation would see, for restrictedEligibility's counterfactual. It
// deliberately does not go through Constraints: the point is to model the code
// path where the subject's nodeSelector was never consulted.
func unnarrowedViews(zones ...string) []leeway.NodeView {
	views := make([]leeway.NodeView, 0, len(zones))
	for _, z := range zones {
		views = append(views, leeway.NodeView{
			Name:            z + "-0",
			Domain:          leeway.Domain(z),
			Ready:           true,
			Schedulable:     true,
			MatchesSelector: true,
			Tolerated:       true,
		})
	}
	return views
}

// pinnedVolumes: nine replicas of a StatefulSet, each holding its own zonal
// disk, spread [4,3,2] because the disks were provisioned over months as the
// cluster grew. None of these pods can move without destroying data.
//
// The distribution is uneven and the unevenness is permanent, and ρ=0.111 sits
// under the threshold, so nothing fires. See
// TestFalsePositiveCorpus_PinnedSkewIsNotYetSuppressed for the case this
// fixture does *not* cover and why.
func pinnedVolumes(t *testing.T) fpFixture {
	inv := zonedInventory(t,
		zoneSpec{zone: "zone-a", nodes: 4},
		zoneSpec{zone: "zone-b", nodes: 3},
		zoneSpec{zone: "zone-c", nodes: 2},
	)
	rep := tscPod(zoneSpread(1, corev1.ScheduleAnyway))
	withClaim("data")(rep)

	pods := replicas(rep,
		"zone-a-0", "zone-a-1", "zone-a-2", "zone-a-3",
		"zone-b-0", "zone-b-1", "zone-b-2",
		"zone-c-0", "zone-c-1")
	// A StatefulSet's volumeClaimTemplate gives every replica its own claim,
	// and every claim its own zonal disk.
	for _, p := range pods {
		p.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "data-" + p.Name
	}
	pins := VolumePins(zonalDisks(), inv.Keys())

	return fpFixture{
		name: "pinned volumes: nine replicas on zonal disks",
		inv:  inv,
		rep:  rep,
		pods: pods,
		also: func(t *testing.T, got fpResult) {
			for _, p := range pods {
				if !pins(p) {
					t.Fatalf("%s did not read as pinned; the fixture is not testing pinned skew", p.Name)
				}
			}
			if got.scores.Relocation == 0 {
				t.Errorf("R = 0, so the distribution is not actually uneven and the fixture proves nothing: %s", got)
			}
		},
	}
}

// zonalDisks is a VolumeLookup where every claim resolves to a disk that names
// one zone — the shape a zonal StorageClass provisions.
func zonalDisks() VolumeLookup {
	return func(_, claim string) (*corev1.PersistentVolume, bool) {
		if !strings.HasPrefix(claim, "data-") {
			return nil, false
		}
		return affinityPV(term(string(zoneKey), corev1.NodeSelectorOpIn, "zone-a")), true
	}
}

// midRollout: a Deployment of six partway through a rollout, two surge pods of
// the new ReplicaSet already up. Both ReplicaSets' pods count toward the
// Deployment subject (FR-2), so n is 8 rather than 6 and the two new pods both
// landed in zone-a because that is where the capacity was.
//
// The constraint is ScheduleAnyway, which is what makes [4,2,2] reachable at
// all: a DoNotSchedule maxSkew of one would have left the surge pods Pending
// rather than stacked, so a fixture asserting a hard contract survives this
// shape would be asserting something no cluster can produce.
func midRollout(t *testing.T) fpFixture {
	inv := zonedInventory(t,
		zoneSpec{zone: "zone-a", nodes: 3},
		zoneSpec{zone: "zone-b", nodes: 3},
		zoneSpec{zone: "zone-c", nodes: 3},
	)
	perRS := zoneSpread(1, corev1.ScheduleAnyway)
	// The usual key, and the one that makes the constraint per-ReplicaSet while
	// leeway scores per Deployment: the two populations agree once a rollout
	// finishes and diverge exactly while one is in flight.
	perRS.MatchLabelKeys = []string{"pod-template-hash"}

	rep := tscPod(perRS)
	old := replicas(rep, "zone-a-0", "zone-a-1", "zone-b-0", "zone-b-1", "zone-c-0", "zone-c-1")

	surge := rep.DeepCopy()
	surge.Name = "api-def"
	surge.Labels["pod-template-hash"] = "8a1"
	fresh := replicas(surge, "zone-a-2", "zone-a-2")

	return fpFixture{
		name: "mid-rollout: two ReplicaSets of one Deployment coexisting",
		inv:  inv,
		rep:  rep,
		pods: append(old, fresh...),
		also: func(t *testing.T, got fpResult) {
			if got.scores.Total != 8 {
				t.Errorf("n = %d, want 8 — both ReplicaSets' pods belong to the Deployment subject: %s",
					got.scores.Total, got)
			}
			// A Phase 4 finding raised here has to be able to say the
			// constraint was counting a narrower population than leeway was.
			if !evidenceContains(got.intent, "matchLabelKeys=pod-template-hash") {
				t.Errorf("evidence = %v, want the matchLabelKeys note", renderEvidence(got.intent))
			}
		},
	}
}

// smallN is §7.4: four pods, one of them somewhere other than where the
// expectation put it. ρ is 0.25, comfortably over the 0.2 threshold, and R is
// 1 — below the small-n relocation floor, so nobody is paged.
//
// The misplacement is also the mildest kind there is. The expectation is
// [2,1,1] only because Hamilton hands the single remainder to the lowest index;
// [1,1,2] is an equally good placement and the model charges it R=1 purely for
// the tie-break. That is the exact shape §7.4's floor exists to absorb.
func smallN(t *testing.T) fpFixture {
	inv := zonedInventory(t,
		zoneSpec{zone: "zone-a", nodes: 2},
		zoneSpec{zone: "zone-b", nodes: 2},
		zoneSpec{zone: "zone-c", nodes: 2},
	)
	rep := tscPod(zoneSpread(1, corev1.ScheduleAnyway))

	return fpFixture{
		name: "small n: 4 pods, one off the expectation",
		inv:  inv,
		rep:  rep,
		pods: replicas(rep, "zone-a-0", "zone-b-0", "zone-c-0", "zone-c-1"),
		also: func(t *testing.T, got fpResult) {
			if drift := leeway.DefaultThresholds().Drift; got.scores.Drift <= drift {
				t.Errorf("rho = %.3f, want it over the %.2f threshold — otherwise the small-n floor "+
					"is not what saved this fixture and §7.4 is untested: %s", got.scores.Drift, drift, got)
			}
			if want := "small-n: relocation below floor"; got.reason != want {
				t.Errorf("reason = %q, want %q: %s", got.reason, want, got)
			}
		},
	}
}

// cappedDomains: nine pods under a required hostname anti-affinity, on a
// cluster whose third zone has a single node. One pod per node is the contract,
// so zone-c can hold exactly one and the expectation is [4,4,1] — itself
// uneven, by three.
//
// This is why §7.3 reads S* off the expectation instead of computing it as
// `n mod m`: the latter says zero here, and charging the difference as excess
// skew would report drift for a cluster doing the only thing it can. Scored
// without the caps the same placement breaches outright, which the fixture
// asserts, because a cap that is not actually load-bearing would let this pass
// for the wrong reason.
//
// The zone constraint is ScheduleAnyway for the same reachability reason as the
// rollout fixture: a hard zone maxSkew of one would have left the ninth pod
// Pending rather than produce [4,4,1]. It is present at all so the axis under
// test carries a real intent — a nil intent would make the fixture pass through
// a weaker path than the one a Phase 4 finding will take.
func cappedDomains(t *testing.T) fpFixture {
	inv := zonedInventory(t,
		zoneSpec{zone: "zone-a", nodes: 4},
		zoneSpec{zone: "zone-b", nodes: 4},
		zoneSpec{zone: "zone-c", nodes: 1},
	)
	rep := tscPod(zoneSpread(1, corev1.ScheduleAnyway))
	rep.Spec.Affinity = antiRequired(selfTerm(hostKey))

	return fpFixture{
		name: "capped domains: a zone saturated at one pod per node",
		inv:  inv,
		rep:  rep,
		pods: replicas(rep,
			"zone-a-0", "zone-a-1", "zone-a-2", "zone-a-3",
			"zone-b-0", "zone-b-1", "zone-b-2", "zone-b-3",
			"zone-c-0"),
		// The required hostname anti-affinity is a ceiling of one object per
		// node (§5.1 MaxPerDomain), so a zone can hold as many of this subject
		// as it has eligible nodes. That conversion is the caller's job once
		// §7.1 has decided the domain set, and Phase 4 is where it will live.
		caps: func(el leeway.Eligibility) []int64 {
			out := make([]int64, len(el.NodeCount))
			copy(out, el.NodeCount)
			return out
		},
		also: func(t *testing.T, got fpResult) {
			if want := []int64{4, 4, 1}; !slices.Equal(got.scores.Expected, want) {
				t.Fatalf("expected = %v, want %v — water-filling did not saturate zone-c: %s",
					got.scores.Expected, want, got)
			}
			if got.scores.MinAchievableSkew != 3 {
				t.Errorf("S* = %d, want 3 — a saturated cap raises the floor (§7.2 step 3): %s",
					got.scores.MinAchievableSkew, got)
			}
			if idx := slices.Index(got.eligible.Domains, leeway.Domain("zone-c")); idx < 0 || !got.ap.Saturated[idx] {
				t.Errorf("zone-c is not marked saturated: %+v", got.ap)
			}

			// Without the caps the expectation is [3,3,3] and the same
			// placement relocates two pods at rho=0.222, over threshold and
			// over the small-n floor. The caps are doing the work.
			uncapped := leeway.Apportion(9, leeway.EqualWeights(3), nil)
			scores := leeway.Score(got.eligible.Domains, got.scores.Actual, uncapped, got.intent.MaxSkew, leeway.DefaultThresholds())
			if breach, reason := scores.Breach(got.intent, leeway.DefaultThresholds()); !breach {
				t.Errorf("the same placement scored uncapped did not breach (%q); "+
					"the caps are no longer what is saving this fixture", reason)
			}
		},
	}
}

// TestFalsePositiveCorpus_PinnedSkewIsNotYetSuppressed records a gap rather
// than a passing cluster, and it is here so the gap cannot close silently.
//
// §12 lists "pinned volumes" among the clusters that are fine, but FR-8's
// 2026-09-19 amendment supersedes that reading: `leeway.pinned_skew` was
// dropped as a kind and pinning became a `suspectedCause: volume_pinning` on
// `leeway.placement_drift` (§8.2). Under the amendment a pinned subject with
// real skew *does* report — at lower severity, with the cause attached — so
// suppressing it here would contradict the design rather than satisfy it.
//
// What is true today is that nothing consumes pinning at all: Placement.Pinned
// and DomainCount.Pinned are maintained, and neither Score nor Breach reads
// them, so a pinned subject's verdict is identical to an unpinned one's. This
// test asserts that current behaviour on a shape that pinning would otherwise
// excuse — nine replicas whose disks landed [5,4,0] — so that whenever Phase 4
// introduces the severity split, this test goes red and forces the decision to
// be made deliberately.
func TestFalsePositiveCorpus_PinnedSkewIsNotYetSuppressed(t *testing.T) {
	inv := zonedInventory(t,
		zoneSpec{zone: "zone-a", nodes: 5},
		zoneSpec{zone: "zone-b", nodes: 4},
		zoneSpec{zone: "zone-c", nodes: 2},
	)
	rep := tscPod(zoneSpread(1, corev1.ScheduleAnyway))
	withClaim("data")(rep)

	pods := replicas(rep,
		"zone-a-0", "zone-a-1", "zone-a-2", "zone-a-3", "zone-a-4",
		"zone-b-0", "zone-b-1", "zone-b-2", "zone-b-3")
	for _, p := range pods {
		p.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "data-" + p.Name
	}

	f := fpFixture{name: "pinned, [5,4,0]", inv: inv, rep: rep, pods: pods}
	got := scoreSubject(t, f, zoneKey)

	pins := VolumePins(zonalDisks(), inv.Keys())
	for _, p := range pods {
		if !pins(p) {
			t.Fatalf("%s did not read as pinned", p.Name)
		}
	}
	if !got.breach {
		t.Fatalf("a pinned subject at [5,4,0] no longer breaches: %s\n"+
			"If pinning is now suppressed, check it against FR-8 as amended — the design says a pinned "+
			"subject reports at lower severity with suspectedCause=volume_pinning, not that it is silent — "+
			"and then move this cluster into falsePositiveCorpus.", got)
	}
	if want := "normalised drift over threshold"; got.reason != want {
		t.Errorf("reason = %q, want %q: %s", got.reason, want, got)
	}
}
