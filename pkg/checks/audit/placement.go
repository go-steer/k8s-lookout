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

package audit

import (
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// kindRigidScheduling is the placement posture kind: the workload
// restricts where it may run, and the restriction resolves to too few
// nodes for the replica count it asks for.
//
// A nodeSelector is not a defect — it is how anyone targets a node
// class. The defect is a nodeSelector whose ARITHMETIC does not work,
// and that is undecidable from the spec alone: `disktype=ssd` onto a
// 40-node pool and `disktype=ssd` onto one surviving node are the same
// three lines of YAML. Resolving the constraint against the live node
// labels is what turns the slug into a claim.
const kindRigidScheduling = "audit.rigid_scheduling"

const (
	reasonNoEligibleNodes = "NoEligibleNodes"
	reasonSingleNode      = "SingleEligibleNode"
	reasonFewerNodes      = "FewerEligibleNodesThanReplicas"
)

// maxConstraintKeys caps the rendered constraint key list.
const maxConstraintKeys = 8

// placement judges the required placement constraint against the node
// inventory.
//
// DaemonSets are excluded, as they are from every other availability
// claim here: a nodeSelector on a DaemonSet IS its replica count, so
// "few eligible nodes" restates the spec rather than faulting it.
// Workloads scaled to zero are excluded for the same reason as
// elsewhere — nothing is running to lose.
//
// A single-replica workload gets only the zero-node claim. That it
// dies with its one node is already said, better, by
// audit.single_replica; saying it twice would name two remedies for
// one outage.
//
// # What the count deliberately does not subtract
//
// Taints the pod does not tolerate, and cordoned nodes, both make a
// matching node ineligible in practice. Neither is subtracted here.
// The error is one-directional — the eligible count is an upper bound,
// so this check UNDER-reports and never invents a finding — and the
// alternative is reimplementing the taint/toleration matcher to make a
// posture claim marginally sharper.
func (ix *workloadIndex) placement(w workload) []emit.Finding {
	if w.kind == "DaemonSet" {
		return nil
	}
	want := w.wantReplicas()
	if want == 0 {
		return nil
	}
	sel, ok := requiredPlacement(w.template.Spec)
	if !ok {
		return nil
	}

	eligible := 0
	for _, n := range ix.nodes {
		if sel.matches(n) {
			eligible++
		}
	}

	details := func(extra ...emit.Field) []emit.Field {
		return append([]emit.Field{
			{Key: "eligible_nodes", Value: itoa(eligible)},
			{Key: "cluster_nodes", Value: itoa(len(ix.nodes))},
			{Key: "constraint", Value: cappedList(sel.keys)},
		}, extra...)
	}

	switch {
	case eligible == 0:
		// Info, and its own reason: a node pool scaled to zero by the
		// cluster autoscaler reports exactly this, and on a cluster that
		// uses one it is the normal resting state rather than the worst
		// case. Reporting it as a warning would make the check flap with
		// the autoscaler.
		return []emit.Finding{{
			Kind:     kindRigidScheduling,
			Severity: emit.SeverityInfo,
			Reason:   reasonNoEligibleNodes,
			Message: fmt.Sprintf("no node in the cluster satisfies the required placement constraint (%s): the pods cannot schedule until one appears — expected if a scaled-to-zero node pool backs this workload, a stuck rollout if not",
				cappedList(sel.keys)),
			Details: details(),
		}}
	case eligible == 1 && want > 1:
		return []emit.Finding{{
			Kind:     kindRigidScheduling,
			Severity: emit.SeverityWarning,
			Reason:   reasonSingleNode,
			Message: fmt.Sprintf("%d replicas but exactly one node satisfies the required placement constraint (%s): every replica lands on it, so losing that one node takes the workload down whatever the replica count or PodDisruptionBudget says",
				want, cappedList(sel.keys)),
			Details: details(emit.Field{Key: "replicas", Value: itoa(int(want))}),
		}}
	case eligible < int(want):
		return []emit.Finding{{
			Kind:     kindRigidScheduling,
			Severity: emit.SeverityWarning,
			Reason:   reasonFewerNodes,
			Message: fmt.Sprintf("%d replicas but only %d nodes satisfy the required placement constraint (%s): replicas must stack, so one node loss takes more than one of them, and a DoNotSchedule spread rule leaves the surplus Pending",
				want, eligible, cappedList(sel.keys)),
			Details: details(emit.Field{Key: "replicas", Value: itoa(int(want))}),
		}}
	}
	return nil
}

// nodeSelection is a pod's REQUIRED placement constraint: the
// spec.nodeSelector map ANDed with the required node affinity, whose
// terms are ORed with each other.
//
// Only the required forms are read. preferredDuringScheduling is a
// weight, not a restriction — the scheduler will place the pod
// anywhere if it must — so counting it would report a constraint that
// cannot actually strand anything.
type nodeSelection struct {
	selector labels.Selector
	terms    []nodeTerm
	keys     []string
}

// nodeTerm is one ORed nodeSelectorTerm: label expressions and field
// expressions, ANDed within the term.
type nodeTerm struct {
	labelSel labels.Selector
	fieldSel labels.Selector
}

// requiredPlacement builds the matcher, reporting false when the pod
// places no requirement on the node at all — the common case, and the
// reason this check is quiet on most workloads.
//
// A malformed requirement also reports false. The scheduler would
// reject the object, and guessing at a partial constraint would
// produce a node count that is not the one the cluster will use.
func requiredPlacement(spec corev1.PodSpec) (nodeSelection, bool) {
	var ns nodeSelection
	keys := map[string]bool{}

	if len(spec.NodeSelector) > 0 {
		ns.selector = labels.SelectorFromSet(labels.Set(spec.NodeSelector))
		for k := range spec.NodeSelector {
			keys[k] = true
		}
	}

	if spec.Affinity != nil && spec.Affinity.NodeAffinity != nil {
		req := spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if req != nil {
			for _, term := range req.NodeSelectorTerms {
				// Since k8s 1.13 an entirely empty term matches NOTHING,
				// where an empty labels.Selector matches everything. Encode
				// the API's meaning, not the library's default.
				if len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0 {
					ns.terms = append(ns.terms, nodeTerm{
						labelSel: labels.Nothing(), fieldSel: labels.Everything(),
					})
					continue
				}
				lsel, err := requirementSelector(term.MatchExpressions)
				if err != nil {
					return nodeSelection{}, false
				}
				fsel, err := requirementSelector(term.MatchFields)
				if err != nil {
					return nodeSelection{}, false
				}
				ns.terms = append(ns.terms, nodeTerm{labelSel: lsel, fieldSel: fsel})
				for _, e := range term.MatchExpressions {
					keys[e.Key] = true
				}
				for _, e := range term.MatchFields {
					keys[e.Key] = true
				}
			}
		}
	}

	if ns.selector == nil && len(ns.terms) == 0 {
		return nodeSelection{}, false
	}
	for k := range keys {
		ns.keys = append(ns.keys, k)
	}
	sort.Strings(ns.keys)
	if len(ns.keys) > maxConstraintKeys {
		ns.keys = ns.keys[:maxConstraintKeys]
	}
	return ns, true
}

// requirementSelector converts node selector requirements into an
// apimachinery selector, which already implements the operator
// semantics — including Gt/Lt's single-integer-value rule — that this
// check would otherwise have to restate and could restate wrongly.
func requirementSelector(rs []corev1.NodeSelectorRequirement) (labels.Selector, error) {
	sel := labels.NewSelector()
	for _, r := range rs {
		op, err := selectionOperator(r.Operator)
		if err != nil {
			return nil, err
		}
		req, err := labels.NewRequirement(r.Key, op, r.Values)
		if err != nil {
			return nil, err
		}
		sel = sel.Add(*req)
	}
	return sel, nil
}

func selectionOperator(op corev1.NodeSelectorOperator) (selection.Operator, error) {
	switch op {
	case corev1.NodeSelectorOpIn:
		return selection.In, nil
	case corev1.NodeSelectorOpNotIn:
		return selection.NotIn, nil
	case corev1.NodeSelectorOpExists:
		return selection.Exists, nil
	case corev1.NodeSelectorOpDoesNotExist:
		return selection.DoesNotExist, nil
	case corev1.NodeSelectorOpGt:
		return selection.GreaterThan, nil
	case corev1.NodeSelectorOpLt:
		return selection.LessThan, nil
	}
	return "", fmt.Errorf("unknown node selector operator %q", op)
}

// matches reports whether one node satisfies the whole constraint.
func (ns nodeSelection) matches(n *corev1.Node) bool {
	l := labels.Set(n.Labels)
	if ns.selector != nil && !ns.selector.Matches(l) {
		return false
	}
	if len(ns.terms) == 0 {
		return true
	}
	// matchFields addresses the node object rather than its labels;
	// metadata.name is the only field the scheduler supports.
	f := labels.Set{"metadata.name": n.Name}
	for _, t := range ns.terms {
		if t.labelSel.Matches(l) && t.fieldSel.Matches(f) {
			return true
		}
	}
	return false
}
