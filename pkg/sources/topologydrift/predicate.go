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
	"strconv"

	corev1 "k8s.io/api/core/v1"
)

// Constraints is the placement half of a subject's intent: the fields of an
// admitted Pod that decided which nodes the scheduler was allowed to pick
// (FR-7). Eligibility is computed over them in §7.1.
//
// **Read from an admitted Pod, never from a workload template.** Spike S3 found
// GKE injecting tolerations at Pod admission that the owning Deployment's
// `PodTemplateSpec` does not carry: a GPU probe pod whose manifest declared no
// tolerations at all was admitted carrying two, one per `NoSchedule` taint on
// the node it landed on (§7.7.6). A template is therefore a strictly weaker
// document than the object that got scheduled, and computing eligibility from
// it would conclude the workload is pinned to fewer domains than it really is
// and suppress genuine drift. That direction fails silent, which is why
// ConstraintsOf takes a *corev1.Pod and there is no template-shaped
// constructor to reach for by mistake.
//
// The zero value constrains nothing, which is the honest reading of a pod that
// declares none of these fields: it matches every node and tolerates no taint.
// Note the asymmetry — "no tolerations" is a real restriction, not an absence —
// and that it usually does not bite, because kube-scheduler's default
// NodeTaintsPolicy is Ignore and pkg/leeway mirrors that default (§7.1).
type Constraints struct {
	// NodeSelector is spec.nodeSelector: a flat label equality requirement,
	// ANDed with the affinity below.
	NodeSelector map[string]string

	// RequiredNodeAffinity is
	// spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.
	// Nil means none was declared, which admits every node.
	//
	// Only the *required* form appears here. A preferred affinity ranks nodes
	// the scheduler may still ignore, so treating it as an eligibility bar
	// would shrink the domain set on a preference and suppress the very drift
	// this subsystem exists to report.
	RequiredNodeAffinity *corev1.NodeSelector

	// Tolerations is spec.tolerations, post-admission.
	Tolerations []corev1.Toleration
}

// ConstraintsOf extracts the placement constraints of an admitted pod.
//
// The maps and slices are shared with the pod, not copied: everything here is
// read-only, the objects come from an informer cache that must not be mutated
// anyway, and a per-pod copy of three fields on every evaluation is exactly the
// kind of allocation §6.6.1's cost model does not have room for.
func ConstraintsOf(pod *corev1.Pod) Constraints {
	c := Constraints{
		NodeSelector: pod.Spec.NodeSelector,
		Tolerations:  pod.Spec.Tolerations,
	}
	if a := pod.Spec.Affinity; a != nil && a.NodeAffinity != nil {
		c.RequiredNodeAffinity = a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	}
	return c
}

// MatchesNode reports whether the subject's nodeSelector and required node
// affinity admit this node. It is the `n matches S.nodeSelector ∧
// S.nodeAffinity.required` half of §7.1.
func (c Constraints) MatchesNode(name string, labels map[string]string) bool {
	for k, v := range c.NodeSelector {
		// Presence is checked separately from equality: a node that has never
		// heard of the key must not satisfy a selector asking for the empty
		// string, which is what a bare map lookup would do. `labels.Set`
		// matching upstream has the same Has() guard.
		have, present := labels[k]
		if !present || have != v {
			return false
		}
	}
	if c.RequiredNodeAffinity == nil {
		return true
	}
	return matchNodeSelectorTerms(c.RequiredNodeAffinity.NodeSelectorTerms, name, labels)
}

// ToleratesNode reports whether the subject tolerates every taint on this node
// that would keep the scheduler off it.
//
// Only NoSchedule and NoExecute are considered, mirroring kube-scheduler's
// TaintToleration filter. PreferNoSchedule is a scoring input upstream, not a
// filter, so treating it as an eligibility bar here would remove domains the
// scheduler was perfectly willing to use and report a pinned workload that is
// not pinned.
func (c Constraints) ToleratesNode(taints []corev1.Taint) bool {
	for i := range taints {
		t := &taints[i]
		if t.Effect != corev1.TaintEffectNoSchedule && t.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		if !c.toleratesTaint(t) {
			return false
		}
	}
	return true
}

func (c Constraints) toleratesTaint(taint *corev1.Taint) bool {
	for i := range c.Tolerations {
		if tolerationMatches(&c.Tolerations[i], taint) {
			return true
		}
	}
	return false
}

// tolerationMatches implements corev1.Toleration.ToleratesTaint, which lives in
// k8s.io/api but on the type rather than as a free function — reproduced here
// so the rule is visible next to the eligibility code that depends on it, and
// so the empty-key and empty-effect wildcards are pinned by our own tests
// rather than assumed.
func tolerationMatches(tol *corev1.Toleration, taint *corev1.Taint) bool {
	// An empty effect tolerates every effect; otherwise they must be equal.
	if tol.Effect != "" && tol.Effect != taint.Effect {
		return false
	}
	// An empty key with operator Exists is the "tolerate everything" wildcard
	// that DaemonSets and system add-ons use. With any other operator an empty
	// key is meaningless and matches nothing.
	if tol.Key == "" {
		return tol.Operator == corev1.TolerationOpExists
	}
	if tol.Key != taint.Key {
		return false
	}
	switch tol.Operator {
	case corev1.TolerationOpExists:
		return true
	case corev1.TolerationOpEqual, "":
		// An unset operator defaults to Equal; the API server defaults it, but
		// this code also runs against objects from tests and from a
		// hand-written fixture corpus, so the default is applied here too.
		return tol.Value == taint.Value
	default:
		return false
	}
}

// matchNodeSelectorTerms reports whether any term matches. Terms are ORed and
// the requirements within a term are ANDed, per the NodeSelector API contract.
//
// An empty term list matches nothing. So does a term with neither expressions
// nor fields, which upstream skips rather than treats as a match-all — an
// important difference, because "select every node" and "select no node" are
// the two possible readings of an empty term and only one of them is safe.
func matchNodeSelectorTerms(terms []corev1.NodeSelectorTerm, name string, labels map[string]string) bool {
	for i := range terms {
		t := &terms[i]
		if len(t.MatchExpressions) == 0 && len(t.MatchFields) == 0 {
			continue
		}
		if matchExpressions(t.MatchExpressions, labels) && matchFields(t.MatchFields, name) {
			return true
		}
	}
	return false
}

func matchExpressions(reqs []corev1.NodeSelectorRequirement, labels map[string]string) bool {
	for i := range reqs {
		v, present := labels[reqs[i].Key]
		if !requirementMatches(&reqs[i], v, present) {
			return false
		}
	}
	return true
}

// matchFields evaluates a term's matchFields against the node.
//
// `metadata.name` is the only field the scheduler supports, and an unrecognised
// one is treated as not matching. That is the conservative direction for a
// *term*: terms are ORed, so a term we cannot evaluate drops out and the
// remaining terms still decide, rather than a field we do not understand
// silently widening the eligible set.
func matchFields(reqs []corev1.NodeSelectorRequirement, name string) bool {
	for i := range reqs {
		if reqs[i].Key != "metadata.name" {
			return false
		}
		if !requirementMatches(&reqs[i], name, true) {
			return false
		}
	}
	return true
}

// requirementMatches evaluates one requirement against the value the node
// carries, with present distinguishing "the label is set to the empty string"
// from "the label is absent".
func requirementMatches(req *corev1.NodeSelectorRequirement, value string, present bool) bool {
	switch req.Operator {
	case corev1.NodeSelectorOpIn:
		return present && contains(req.Values, value)
	case corev1.NodeSelectorOpNotIn:
		// An absent label satisfies NotIn. The node does not have the value,
		// which is what was asked.
		return !present || !contains(req.Values, value)
	case corev1.NodeSelectorOpExists:
		return present
	case corev1.NodeSelectorOpDoesNotExist:
		return !present
	case corev1.NodeSelectorOpGt, corev1.NodeSelectorOpLt:
		return compareInt(req, value, present)
	default:
		return false
	}
}

// compareInt evaluates Gt and Lt, which the API restricts to a single integer
// value against an integer-valued label. A label that does not parse fails the
// requirement rather than erroring: the alternative is to drop the whole
// evaluation on one mislabelled node, and a node whose value is not a number
// genuinely is not greater than anything.
func compareInt(req *corev1.NodeSelectorRequirement, value string, present bool) bool {
	if !present || len(req.Values) != 1 {
		return false
	}
	have, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return false
	}
	want, err := strconv.ParseInt(req.Values[0], 10, 64)
	if err != nil {
		return false
	}
	if req.Operator == corev1.NodeSelectorOpGt {
		return have > want
	}
	return have < want
}

func contains(values []string, v string) bool {
	for _, s := range values {
		if s == v {
			return true
		}
	}
	return false
}
