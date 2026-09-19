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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// Resolution is everything one evaluation knows about a subject's intended
// placement, before any score is computed from it.
type Resolution struct {
	// Intents is at most one intent per axis, after §5.1 precedence. Axes on
	// which the pod expressed nothing are absent — "no intent" is not an
	// intent, and inventing a neutral one here would put a row on the wire
	// claiming the workload asked for something it did not.
	Intents map[leeway.TopologyKey]*leeway.Intent

	// Eligible is §7.1's eligible set, present for *every* axis the inventory
	// tracks and not only those carrying an intent. A subject with no spread
	// constraint at all still has an eligible set — its nodeSelector and
	// tolerations still say where it may go — and the domains it is scored
	// against are those, not the cluster's.
	Eligible map[leeway.TopologyKey]leeway.Eligibility
}

// Resolve infers a subject's placement intent from one admitted pod and
// computes the eligible domains that intent is scored against.
//
// **The pod, never the controller's template.** Every inference entry point in
// this package takes a *corev1.Pod for the reason §13 S3 measured: GKE injects
// tolerations at admission that the owning Deployment's PodTemplateSpec does
// not carry. A template is a strictly weaker document than the object the
// scheduler actually placed, and inferring from it computes an eligible set
// that is too small, concludes the workload is pinned, and suppresses drift
// that is really there — the fail-silent direction, which is why there is
// deliberately no template-shaped constructor to reach for.
//
// Candidate order is load-bearing only for ties, which ResolveIntents breaks by
// input order; spread constraints and affinity terms are different sources, so
// §5.1 precedence decides every cross-source case on its own. They are
// concatenated TSC-first anyway so that a reader of the candidate list sees the
// same order §5.1 lists.
func Resolve(pod *corev1.Pod, inv *Inventory, clusterDefaults *[]corev1.TopologySpreadConstraint) Resolution {
	res := Resolution{
		Intents:  map[leeway.TopologyKey]*leeway.Intent{},
		Eligible: map[leeway.TopologyKey]leeway.Eligibility{},
	}
	if pod == nil || inv == nil {
		return res
	}

	candidates := SpreadConstraintIntents(pod)
	candidates = append(candidates, AffinityIntents(pod)...)
	// Last, and it costs nothing to order it so: ClusterDefaultIntents returns
	// nothing at all for a pod that declared any constraint of its own, which
	// is the scheduler's own rule and the reason a wrong assumption about the
	// defaults cannot reach a workload that expressed intent. Filtered to the
	// counted axes, unlike every other source here — see onlyTrackedAxes.
	candidates = append(candidates, onlyTrackedAxes(ClusterDefaultIntents(pod, clusterDefaults), inv)...)
	res.Intents = leeway.ResolveIntents(candidates)

	constraints := ConstraintsOf(pod)
	for _, key := range inv.Keys() {
		intent := res.Intents[key]

		opts := leeway.DefaultEligibilityOptions()
		// Read through the accessor, never off the field: NodeInclusionPolicy's
		// zero value is the empty string and §7.1 reads "not Honor" as "do not
		// apply", so a bare copy of an unset Policies would silently stop
		// honouring nodeSelector and widen every pinned workload's eligible set
		// to the whole cluster. The accessor answers on a nil receiver, which
		// is the case for an axis with no intent.
		opts.Policies = intent.EligibilityPolicies()

		weighting := leeway.WeightEqual
		if intent != nil {
			opts.MinDomains = intent.MinDomains
			weighting = intent.Weighting
		}

		eligible := leeway.EligibleDomains(inv.NodeViews(key, weighting, constraints), opts)
		res.Eligible[key] = eligible
		if intent != nil {
			intent.EligibleDomains = sets.New(eligible.Domains...)
		}
	}
	return res
}
