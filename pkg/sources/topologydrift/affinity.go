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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// onePerDomain is the ceiling a required podAntiAffinity term imposes.
const onePerDomain int64 = 1

// AffinityIntents derives §5.1 intents from an admitted pod's
// `spec.affinity.podAntiAffinity` (FR-5) and `spec.affinity.podAffinity`
// (FR-6).
//
// This is the common case, not the exotic one. Spike S3's live sample — a
// Deployment with no topologySpreadConstraints at all and a single preferred
// hostname anti-affinity — is what most workloads actually look like, and a
// subsystem that only understood FR-4 would have nothing to say about them.
//
// As with the rest of inference the input is an admitted Pod, never a workload
// template, for the S3 reason: the scheduled object is the only document that
// reflects admission-time mutation.
//
// Candidates are emitted strongest first — required before preferred,
// anti-affinity before affinity — and returned unresolved, so that
// leeway.ResolveIntents can apply §5.1 precedence and keep the losers as
// evidence.
//
// # Anti-affinity and affinity are not mirror images
//
// An anti-affinity term is this subject's spread intent only when its selector
// matches the subject's own pods: "do not sit with the batch job" constrains
// where replicas may go but says nothing about how they should be distributed
// among themselves, and scoring spread against it would report drift the term
// never asked to prevent.
//
// A podAffinity term is kept either way. "Run next to the cache" is a genuine
// pull toward wherever the cache is, and the resulting concentration is real
// even though the subject's own pods are not what it names. The evidence line
// says which of the two it is, because the induced case is much weaker — if
// the referenced population is itself spread out, the subject is free to
// spread with it.
func AffinityIntents(pod *corev1.Pod) []leeway.Intent {
	aff := pod.Spec.Affinity
	if aff == nil {
		return nil
	}
	podLabels := labels.Set(pod.Labels)
	ns := pod.Namespace

	var out []leeway.Intent
	add := func(in leeway.Intent, ok bool) {
		if ok {
			out = append(out, in)
		}
	}

	if anti := aff.PodAntiAffinity; anti != nil {
		for i := range anti.RequiredDuringSchedulingIgnoredDuringExecution {
			t := &anti.RequiredDuringSchedulingIgnoredDuringExecution[i]
			add(antiAffinityIntent(t, i, ns, podLabels, leeway.SourcePodAntiAffinityRequired, 0))
		}
	}
	if a := aff.PodAffinity; a != nil {
		for i := range a.RequiredDuringSchedulingIgnoredDuringExecution {
			t := &a.RequiredDuringSchedulingIgnoredDuringExecution[i]
			add(affinityIntent(t, i, podLabels, leeway.SourcePodAffinityRequired, 0))
		}
	}
	if anti := aff.PodAntiAffinity; anti != nil {
		for i := range anti.PreferredDuringSchedulingIgnoredDuringExecution {
			w := &anti.PreferredDuringSchedulingIgnoredDuringExecution[i]
			add(antiAffinityIntent(&w.PodAffinityTerm, i, ns, podLabels, leeway.SourcePodAntiAffinityPreferred, w.Weight))
		}
	}
	if a := aff.PodAffinity; a != nil {
		for i := range a.PreferredDuringSchedulingIgnoredDuringExecution {
			w := &a.PreferredDuringSchedulingIgnoredDuringExecution[i]
			add(affinityIntent(&w.PodAffinityTerm, i, podLabels, leeway.SourcePodAffinityPreferred, w.Weight))
		}
	}
	return out
}

// antiAffinityIntent turns one anti-affinity term into a Spread intent, or
// reports that the term is not about this subject.
func antiAffinityIntent(t *corev1.PodAffinityTerm, pos int, ns string, podLabels labels.Set, src leeway.IntentSource, weight int32) (leeway.Intent, bool) {
	sel, notes, ok := usableTerm(t)
	if !ok {
		return leeway.Intent{}, false
	}
	if !sel.Matches(podLabels) {
		// The term keeps this subject away from some *other* population. Real,
		// and the scheduler honours it, but it constrains the eligible set
		// rather than describing a distribution, and §7.1 is where that
		// belongs.
		return leeway.Intent{}, false
	}
	if !termScopeIncludesOwnNamespace(t, ns) {
		// A self-matching selector aimed at other namespaces cannot be about
		// this subject's replicas, which all live in this one.
		return leeway.Intent{}, false
	}

	in := leeway.Intent{
		TopologyKey: leeway.TopologyKey(t.TopologyKey),
		Mode:        leeway.ModeSpread,
		Source:      src,
		Confidence:  leeway.ConfidenceInferred,
	}
	if src == leeway.SourcePodAntiAffinityRequired {
		// The contract is a ceiling of one per domain, not a skew bound; see
		// Intent.MaxPerDomain for why those are different statements.
		ceiling := onePerDomain
		in.MaxPerDomain = &ceiling
		in.WhenUnsatisfiable = corev1.DoNotSchedule
	} else {
		in.WhenUnsatisfiable = corev1.ScheduleAnyway
	}
	in.Evidence = []leeway.EvidenceItem{{
		Source: src,
		Detail: describeTerm(t, pos, sel, src, weight, notes),
	}}
	return in, true
}

// affinityIntent turns one podAffinity term into a Colocate intent.
func affinityIntent(t *corev1.PodAffinityTerm, pos int, podLabels labels.Set, src leeway.IntentSource, weight int32) (leeway.Intent, bool) {
	sel, notes, ok := usableTerm(t)
	if !ok {
		return leeway.Intent{}, false
	}
	if !sel.Matches(podLabels) {
		notes = append(notes, "colocation is induced: the term names another population, so the subject concentrates only as far as that population is itself concentrated")
	}

	return leeway.Intent{
		TopologyKey: leeway.TopologyKey(t.TopologyKey),
		Mode:        leeway.ModeColocate,
		Source:      src,
		Confidence:  leeway.ConfidenceInferred,
		// Deliberately no MaxPerDomain and no MaxSkew even for the required
		// form: see Intent.HardContract. A satisfied affinity that produced a
		// spread-out placement is a deviation to explain, not a broken promise.
		Evidence: []leeway.EvidenceItem{{
			Source: src,
			Detail: describeTerm(t, pos, sel, src, weight, notes),
		}},
	}, true
}

// usableTerm parses the term's selector and reports whether the term can
// produce an intent at all, along with any notes about how wide it reaches.
func usableTerm(t *corev1.PodAffinityTerm) (labels.Selector, []string, bool) {
	if t.TopologyKey == "" {
		// Not a statement about any axis. The API rejects this; a fixture or
		// an older cluster may not have.
		return nil, nil, false
	}
	// As in a TopologySpreadConstraint, a nil selector selects no pods rather
	// than every pod, so the term governs nothing.
	if t.LabelSelector == nil {
		return nil, nil, false
	}
	sel, err := metav1.LabelSelectorAsSelector(t.LabelSelector)
	if err != nil {
		return nil, nil, false
	}

	var notes []string
	if sel.Empty() {
		notes = append(notes, "labelSelector is empty: the term reaches every pod in scope, of which this subject is only a part")
	}
	if scope := describeNamespaceScope(t); scope != "" {
		notes = append(notes, scope)
	}
	if len(t.MatchLabelKeys) > 0 {
		notes = append(notes, "matchLabelKeys="+strings.Join(t.MatchLabelKeys, ","))
	}
	if len(t.MismatchLabelKeys) > 0 {
		notes = append(notes, "mismatchLabelKeys="+strings.Join(t.MismatchLabelKeys, ","))
	}
	return sel, notes, true
}

// termScopeIncludesOwnNamespace reports whether the term counts pods in the
// subject's own namespace.
//
// The empty case — no `namespaces` and no `namespaceSelector` — means the
// pod's own namespace, which is the ordinary shape. A `namespaceSelector` is
// unioned with `namespaces` rather than intersected, and an *empty* one
// selects every namespace; a non-empty one needs Namespace objects to
// evaluate, which this package does not watch, so it is read as not
// establishing own-namespace membership. Being wrong in that direction drops
// an intent rather than inventing one.
func termScopeIncludesOwnNamespace(t *corev1.PodAffinityTerm, ns string) bool {
	if t.NamespaceSelector == nil && len(t.Namespaces) == 0 {
		return true
	}
	if t.NamespaceSelector != nil && len(t.NamespaceSelector.MatchLabels) == 0 && len(t.NamespaceSelector.MatchExpressions) == 0 {
		return true
	}
	return slices.Contains(t.Namespaces, ns)
}

// describeNamespaceScope returns a note when the term reaches beyond the
// subject's own namespace, and "" when it is the ordinary same-namespace form.
func describeNamespaceScope(t *corev1.PodAffinityTerm) string {
	if t.NamespaceSelector == nil && len(t.Namespaces) == 0 {
		return ""
	}
	var parts []string
	if len(t.Namespaces) > 0 {
		parts = append(parts, "namespaces="+strings.Join(t.Namespaces, ","))
	}
	if t.NamespaceSelector != nil {
		if len(t.NamespaceSelector.MatchLabels) == 0 && len(t.NamespaceSelector.MatchExpressions) == 0 {
			parts = append(parts, "namespaceSelector matches every namespace")
		} else {
			parts = append(parts, "namespaceSelector is set and was not evaluated: this package does not watch Namespace objects")
		}
	}
	return "term scope is not this namespace alone: " + strings.Join(parts, "; ")
}

// describeTerm renders an affinity term for a finding body.
func describeTerm(t *corev1.PodAffinityTerm, pos int, sel labels.Selector, src leeway.IntentSource, weight int32, notes []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s[%d]: topologyKey=%s", src, pos, t.TopologyKey)
	if weight != 0 {
		fmt.Fprintf(&b, " weight=%d", weight)
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
