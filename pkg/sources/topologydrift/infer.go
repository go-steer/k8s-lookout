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
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// SpreadConstraintIntents derives §5.1 intents from an admitted pod's
// `spec.topologySpreadConstraints` (FR-4).
//
// As with ConstraintsOf, the input is an admitted Pod and not a workload
// template, for the reason spike S3 recorded: the object that got scheduled is
// the only document that reflects admission-time mutation.
//
// Every constraint produces at most one candidate intent and the result is not
// deduplicated by key — resolution is leeway.ResolveIntents's job, and handing
// it every candidate is what lets a loser be retained as evidence instead of
// being dropped here where nothing can explain it.
//
// Constraints on topology keys the source does not count are still returned.
// Filtering to the configured axes belongs to the evaluator, which knows them;
// inference's job is to report what the pod asked for, and a constraint on
// `kubernetes.io/hostname` is real intent even on a sentinel configured only
// for zones.
//
// # Which pods the constraint counts
//
// kube-scheduler computes skew over the pods in the namespace matching the
// constraint's `labelSelector`, narrowed further by `matchLabelKeys`. Leeway
// computes it over the *subject's* pods. Those two populations coincide in the
// ordinary case and can diverge, so the rule here is: a constraint is treated
// as this subject's intent when its selector matches the subject's own pods,
// and the selector is recorded as evidence either way.
//
// The direction that is not handled is a selector *wider* than the subject —
// an empty selector spreading a whole namespace, say, or one matching two
// Deployments. Scoring the subject against a namespace-wide maxSkew is not the
// same question, and answering it needs a population walk this function does
// not do. It is a known narrowing, not an oversight; the evidence line is what
// makes it visible in a finding.
func SpreadConstraintIntents(pod *corev1.Pod) []leeway.Intent {
	tscs := pod.Spec.TopologySpreadConstraints
	if len(tscs) == 0 {
		return nil
	}
	podLabels := labels.Set(pod.Labels)

	out := make([]leeway.Intent, 0, len(tscs))
	for _, i := range constraintOrder(tscs) {
		if in, ok := intentFromConstraint(&tscs[i], i, podLabels); ok {
			out = append(out, in)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// constraintOrder returns indexes into tscs with DoNotSchedule constraints
// first and declaration order preserved within each group.
//
// The API permits two constraints on one topology key as long as they differ in
// `whenUnsatisfiable`, and both then resolve from the same IntentSource — a tie
// that leeway.ResolveIntents breaks by input order. Left in declaration order
// the winner would be whichever the author happened to write first, so a
// workload could lose its enforced contract to an advisory one by an edit that
// reordered a list. Ordering here makes the stronger constraint win, and the
// weaker one is still retained as evidence.
func constraintOrder(tscs []corev1.TopologySpreadConstraint) []int {
	idx := make([]int, len(tscs))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		return enforced(&tscs[idx[a]]) && !enforced(&tscs[idx[b]])
	})
	return idx
}

// enforced reports whether a constraint is one the scheduler refuses to
// violate. An unset whenUnsatisfiable is DoNotSchedule, which is the API's
// default and the stricter reading of a field somebody left out.
func enforced(c *corev1.TopologySpreadConstraint) bool {
	return c.WhenUnsatisfiable != corev1.ScheduleAnyway
}

func intentFromConstraint(c *corev1.TopologySpreadConstraint, pos int, podLabels labels.Set) (leeway.Intent, bool) {
	if c.TopologyKey == "" {
		return leeway.Intent{}, false
	}
	// A nil labelSelector selects no pods, so the constraint governs nothing.
	// metav1.LabelSelectorAsSelector spells this labels.Nothing(); checking the
	// nil explicitly keeps the reason visible, because "matches no pods" and
	// "matches every pod" are the two readings of an absent selector and the
	// API picked the first.
	if c.LabelSelector == nil {
		return leeway.Intent{}, false
	}
	sel, err := metav1.LabelSelectorAsSelector(c.LabelSelector)
	if err != nil {
		// A selector the API server would have rejected. Silently skipping it
		// is right: it cannot be the intent of a pod that is running.
		return leeway.Intent{}, false
	}
	if !sel.Matches(podLabels) {
		// The constraint spreads some other population. It is real and the
		// scheduler honours it, but it is not a statement about where this
		// subject's own replicas should sit, and scoring the subject against
		// it would report drift the constraint never asked to prevent.
		return leeway.Intent{}, false
	}

	in := leeway.Intent{
		TopologyKey:       leeway.TopologyKey(c.TopologyKey),
		Mode:              leeway.ModeSpread,
		Source:            leeway.SourceTopologySpreadConstraint,
		Confidence:        leeway.ConfidenceDeclared,
		WhenUnsatisfiable: corev1.DoNotSchedule,
	}
	if !enforced(c) {
		in.WhenUnsatisfiable = corev1.ScheduleAnyway
	}
	maxSkew := c.MaxSkew
	in.MaxSkew = &maxSkew
	if c.NodeAffinityPolicy != nil {
		in.Policies.NodeAffinityPolicy = *c.NodeAffinityPolicy
	}
	if c.NodeTaintsPolicy != nil {
		in.Policies.NodeTaintsPolicy = *c.NodeTaintsPolicy
	}

	notes := []string{}
	if sel.Empty() {
		// A present-but-empty selector matches every pod in the namespace, so
		// the population the scheduler balanced is strictly wider than the
		// subject leeway scores. The intent is kept — the workload does want
		// spreading — but the reader of a finding needs to know the maxSkew
		// was never a bound on this subject alone.
		notes = append(notes, "labelSelector is empty: the constraint spreads every pod in the namespace, of which this subject is only a part")
	}
	// minDomains is only honoured alongside DoNotSchedule — the API validates
	// that today, but this code also sees objects from a fixture corpus and
	// from clusters older than the validation, and carrying a minDomains the
	// scheduler ignored would pad the eligible set with synthetic
	// present-with-zero domains (§7.1) and manufacture skew out of nothing.
	if c.MinDomains != nil {
		if enforced(c) {
			minDomains := *c.MinDomains
			in.MinDomains = &minDomains
		} else {
			notes = append(notes, "minDomains ignored: only honoured with DoNotSchedule")
		}
	}
	if len(c.MatchLabelKeys) > 0 {
		// matchLabelKeys narrows the counted population to pods sharing this
		// pod's value for each key. The usual key is pod-template-hash, which
		// makes the constraint per-ReplicaSet while leeway scores per
		// Deployment (§11). The two populations agree once a rollout finishes
		// and diverge while one is in flight, which is §7.6's job rather than
		// this function's — but a finding raised mid-rollout should be able to
		// say so.
		notes = append(notes, "matchLabelKeys="+strings.Join(c.MatchLabelKeys, ","))
	}

	in.Evidence = []leeway.EvidenceItem{{
		Source: leeway.SourceTopologySpreadConstraint,
		Detail: describeConstraint(c, pos, sel, notes),
	}}
	return in, true
}

// describeConstraint renders the constraint for a finding body. It reads off
// the object rather than off the derived Intent so that a field leeway decided
// to ignore still appears, with the reason next to it.
func describeConstraint(c *corev1.TopologySpreadConstraint, pos int, sel labels.Selector, notes []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "topologySpreadConstraints[%d]: topologyKey=%s maxSkew=%d whenUnsatisfiable=%s",
		pos, c.TopologyKey, c.MaxSkew, whenUnsatisfiable(c))
	if c.MinDomains != nil {
		fmt.Fprintf(&b, " minDomains=%d", *c.MinDomains)
	}
	if s := sel.String(); s != "" {
		fmt.Fprintf(&b, " labelSelector=%s", s)
	}
	for _, n := range notes {
		b.WriteString("; ")
		b.WriteString(n)
	}
	return b.String()
}

func whenUnsatisfiable(c *corev1.TopologySpreadConstraint) corev1.UnsatisfiableConstraintAction {
	if enforced(c) {
		return corev1.DoNotSchedule
	}
	return corev1.ScheduleAnyway
}
