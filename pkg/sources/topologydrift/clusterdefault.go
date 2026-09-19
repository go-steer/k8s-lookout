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
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// SystemDefaultConstraints is kube-scheduler's `PodTopologySpread`
// `defaultConstraints` under `defaultingType: System`, which is what a cluster
// that has not configured the plugin runs with.
//
// These are assumed, not measured. Spike S4 confirmed on a managed GKE control
// plane (1.36.4-gke.1082000) that there is no scheduler pod, no
// scheduler-shaped ConfigMap and no API surface exposing
// KubeSchedulerConfiguration, so a sentinel cannot read what its cluster
// actually runs. Every intent derived from this set is therefore marked
// SourceClusterDefaultAssumed / ConfidenceAssumed, and §8.1 caps it below the
// Tier A that a contract violation needs.
//
// The values are version-dependent upstream. Keeping them here — rather than
// in a comment in a config file — is what makes the assumption visible on the
// wire via intent_info, which is the mitigation S4 actually settled on:
// declaring the real set is cheap, and an estate can drive
// `cluster-default-assumed` to zero once it cares.
var SystemDefaultConstraints = []corev1.TopologySpreadConstraint{
	{
		TopologyKey:       corev1.LabelHostname,
		MaxSkew:           3,
		WhenUnsatisfiable: corev1.ScheduleAnyway,
	},
	{
		TopologyKey:       corev1.LabelTopologyZone,
		MaxSkew:           5,
		WhenUnsatisfiable: corev1.ScheduleAnyway,
	},
}

// ClusterDefaultIntents derives FR-9 intents from the cluster's
// `PodTopologySpread` default constraints.
//
// # Three states, and unset is not empty
//
// declared is a pointer because a nil slice and an empty one are the same value
// after decoding, and collapsing them reintroduces the exact bug S4 exists to
// stop — confident findings built on an assumption nobody made:
//
//   - nil: UNSET. Nobody has told us, so SystemDefaultConstraints is assumed and
//     every intent is SourceClusterDefaultAssumed / ConfidenceAssumed.
//   - non-nil and empty: DECLARED EMPTY. An operator asserts this cluster
//     configures no defaults. No intents, and that is an answer rather than a
//     gap.
//   - non-nil and populated: DECLARED. Trusted, SourceClusterDefaultDeclared /
//     ConfidenceDeclared — and an operator who declares a DoNotSchedule default
//     is making an assertion of their own, so that one *can* reach Tier A.
//
// # Blast radius
//
// Default constraints apply only to a pod declaring no
// topologySpreadConstraints of its own; one constraint on the pod and the
// scheduler ignores the cluster defaults entirely. So a wrong assumption here
// cannot corrupt the intent of any workload that has actually expressed intent
// — it reaches only the population where leeway's reading was weakest anyway.
//
// # Why there is no label selector to check
//
// A pod's own constraint carries a labelSelector, and FR-4 has to ask whether
// it selects this subject. A cluster default carries none: kube-scheduler
// derives the population from the pod's own owner, which is the subject leeway
// scores. The default therefore maps onto the subject without a selector test,
// and the absence of one here is the correct reading rather than a missing
// check.
func ClusterDefaultIntents(pod *corev1.Pod, declared *[]corev1.TopologySpreadConstraint) []leeway.Intent {
	if pod == nil || len(pod.Spec.TopologySpreadConstraints) > 0 {
		return nil
	}

	constraints, source, confidence := SystemDefaultConstraints, leeway.SourceClusterDefaultAssumed, leeway.ConfidenceAssumed
	if declared != nil {
		constraints, source, confidence = *declared, leeway.SourceClusterDefaultDeclared, leeway.ConfidenceDeclared
	}
	if len(constraints) == 0 {
		return nil
	}

	out := make([]leeway.Intent, 0, len(constraints))
	for i := range constraints {
		c := &constraints[i]
		if c.TopologyKey == "" {
			continue
		}
		in, notes := spreadIntent(c)
		in.Source = source
		in.Confidence = confidence
		in.Evidence = []leeway.EvidenceItem{{
			Source: source,
			Detail: describeClusterDefault(c, i, declared == nil, notes),
		}}
		out = append(out, in)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// onlyTrackedAxes drops cluster-default intents on axes the source does not
// count, and it is the one place inference filters by configured key.
//
// SpreadConstraintIntents deliberately keeps a constraint on an untracked axis:
// a workload that wrote `kubernetes.io/hostname` said something about itself,
// and a reader of a finding is entitled to see it even on a sentinel scoped to
// zones. A cluster default is the opposite case. Nobody wrote it, it will never
// be scored, and it cannot appear as the demoted evidence behind a real intent
// because it is the lowest-precedence candidate in the list. All it would do is
// put a row on `intent_info` — and unlike a declared constraint, which only
// exists where a human opted in, this one lands on *every* subject in the
// cluster.
//
// The upstream assumed set alone is two axes, so leaving it unfiltered would
// take intent_info from ~3.5k series at §8.4's 20k-subject baseline to ~40k,
// for one axis nobody scores. Filtering leaves the zone row — which is the row
// S4 wants visible, so an estate can ask what fraction of its intent rests on a
// guess and drive it to zero by declaring.
func onlyTrackedAxes(intents []leeway.Intent, inv *Inventory) []leeway.Intent {
	if len(intents) == 0 || inv == nil {
		return nil
	}
	out := intents[:0]
	for _, in := range intents {
		if _, tracked := inv.Ordinal(in.TopologyKey); tracked {
			out = append(out, in)
		}
	}
	return out
}

// describeClusterDefault renders one default constraint for a finding body,
// and says outright whether it is an assumption. A reader who sees drift scored
// against a maxSkew nobody in their organisation wrote needs the next sentence
// to be where the number came from.
func describeClusterDefault(c *corev1.TopologySpreadConstraint, pos int, assumed bool, notes []string) string {
	var b strings.Builder
	origin := "configured cluster default"
	if assumed {
		origin = "ASSUMED cluster default (kube-scheduler PodTopologySpread defaultingType=System; not readable from a managed control plane, see spike S4) — declare --topology-cluster-defaults to replace this guess"
	}
	fmt.Fprintf(&b, "%s[%d]: topologyKey=%s maxSkew=%d whenUnsatisfiable=%s",
		origin, pos, c.TopologyKey, c.MaxSkew, whenUnsatisfiable(c))
	if c.MinDomains != nil {
		fmt.Fprintf(&b, " minDomains=%d", *c.MinDomains)
	}
	b.WriteString("; applies because the pod declares no topologySpreadConstraints of its own")
	for _, n := range notes {
		b.WriteString("; ")
		b.WriteString(n)
	}
	return b.String()
}

// ParseClusterDefaults reads the three-state --topology-cluster-defaults flag.
//
// The grammar is `<topologyKey>=<maxSkew>[:<DoNotSchedule|ScheduleAnyway>]`,
// comma-separated; the literal "none" is the declared-empty state, and "" is
// unset. A CLI flag can express three states only if one of them has a
// spelling, and "absent" is the one that cannot — so "none" carries the
// assertion an operator is making.
//
// An omitted action means ScheduleAnyway, which is deliberately *not* the API's
// default for a pod's own constraint. The API defaults an unset
// whenUnsatisfiable to DoNotSchedule, the reading that turns a constraint into
// a hard contract; here that would let a typo in a flag promote a cluster-wide
// assumption to something that can raise a Tier A finding. An operator
// asserting a hard default has to write DoNotSchedule.
func ParseClusterDefaults(raw string) (*[]corev1.TopologySpreadConstraint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	if raw == "none" {
		return &[]corev1.TopologySpreadConstraint{}, nil
	}

	var out []corev1.TopologySpreadConstraint
	for _, field := range strings.Split(raw, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		key, rest, ok := strings.Cut(field, "=")
		if !ok {
			return nil, fmt.Errorf("%q is not <topologyKey>=<maxSkew>[:<whenUnsatisfiable>]", field)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("%q names no topology key", field)
		}

		skewText, action := rest, corev1.ScheduleAnyway
		if text, actionText, hasAction := strings.Cut(rest, ":"); hasAction {
			skewText = text
			switch strings.TrimSpace(actionText) {
			case string(corev1.DoNotSchedule):
				action = corev1.DoNotSchedule
			case string(corev1.ScheduleAnyway):
				action = corev1.ScheduleAnyway
			default:
				return nil, fmt.Errorf("%q: whenUnsatisfiable must be %s or %s",
					field, corev1.DoNotSchedule, corev1.ScheduleAnyway)
			}
		}

		var skew int32
		if _, err := fmt.Sscanf(strings.TrimSpace(skewText), "%d", &skew); err != nil {
			return nil, fmt.Errorf("%q: maxSkew is not a number", field)
		}
		if skew < 1 {
			// The API server refuses maxSkew < 1, so accepting one here would
			// score every workload in the cluster against a constraint no
			// scheduler could be running.
			return nil, fmt.Errorf("%q: maxSkew must be at least 1", field)
		}

		out = append(out, corev1.TopologySpreadConstraint{
			TopologyKey:       key,
			MaxSkew:           skew,
			WhenUnsatisfiable: action,
		})
	}
	if len(out) == 0 {
		// Every field was blank — "  ,  " and the like. That is a malformed
		// declaration rather than the declared-empty state, which has its own
		// spelling.
		return nil, fmt.Errorf("%q declares no constraints (use \"none\" to assert the cluster has none)", raw)
	}
	return &out, nil
}

// DescribeClusterDefaults renders the effective default set for the one-line
// startup log S4 asked for: an operator should be able to see, without reading
// the code, whether the numbers their fleet is being scored against are theirs
// or ours.
func DescribeClusterDefaults(declared *[]corev1.TopologySpreadConstraint) string {
	constraints, origin := SystemDefaultConstraints, "assumed"
	if declared != nil {
		constraints, origin = *declared, "declared"
	}
	if len(constraints) == 0 {
		return "cluster default topology spread: none (declared empty)"
	}
	parts := make([]string, 0, len(constraints))
	for i := range constraints {
		c := &constraints[i]
		parts = append(parts, fmt.Sprintf("%s maxSkew=%d %s", c.TopologyKey, c.MaxSkew, whenUnsatisfiable(c)))
	}
	return fmt.Sprintf("cluster default topology spread (%s): %s", origin, strings.Join(parts, ", "))
}
