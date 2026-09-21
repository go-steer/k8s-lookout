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
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// uidPrefix namespaces the synthetic incident UIDs this source mints, the way
// `capacity` writes `nodegroup:` and `quota` writes its own scope key. A leeway
// subject is a workload and does have a real UID, but the incident is not about
// the workload as a whole: a Deployment can be drifting on its zone axis and
// fine on its region axis, and those are two findings with two dwell timers.
// The dedup key has to carry the axis or they would fold into one incident and
// the second would be silently swallowed as a repeat of the first.
const uidPrefix = "leeway:"

// uidAxisSep separates the subject from the topology key inside the UID. A
// character that cannot appear in either: a subject renders as kind/ns/name and
// a topology key is a label key, and neither admits a pipe.
const uidAxisSep = "|"

// findingUID renders the incident identity for one subject-axis.
func findingUID(sub leeway.SubjectRef, key leeway.TopologyKey) string {
	return uidPrefix + sub.String() + uidAxisSep + string(key)
}

// parseFindingUID reads back what findingUID wrote. It reports false for any
// other source's UID, which is how the clearance observer declines the
// incidents it has no business judging.
func parseFindingUID(uid string) (leeway.SubjectRef, leeway.TopologyKey, bool) {
	rest, ok := strings.CutPrefix(uid, uidPrefix)
	if !ok {
		return leeway.SubjectRef{}, "", false
	}
	subject, key, ok := strings.Cut(rest, uidAxisSep)
	if !ok {
		return leeway.SubjectRef{}, "", false
	}
	sub, ok := leeway.ParseSubjectRef(subject)
	if !ok || !scoredHere(sub.Kind) {
		return leeway.SubjectRef{}, "", false
	}
	return sub, leeway.TopologyKey(key), true
}

// scoredHere reports whether a subject kind is one this source owns an episode
// for — every kind it places, plus SubjectDomain, which it judges without
// placing anything.
//
// The guard exists because "is this mine" is answered by parsing a string, and
// a string another source minted can parse cleanly here — `compute-class`
// writes a PreferenceAxis subject and a rule name where this one expects a
// workload and a topology key. Declining on the kind rather than on the shape
// is what makes the answer right instead of lucky: an unrecognised kind is not
// in the pod index, so Clearance would otherwise report the other source's
// finding recovered-object_deleted on its first sweep.
func scoredHere(kind leeway.SubjectKind) bool {
	switch kind {
	case leeway.SubjectDeployment, leeway.SubjectStatefulSet, leeway.SubjectDaemonSet,
		leeway.SubjectJob, leeway.SubjectNodeGroup, leeway.SubjectCustom,
		leeway.SubjectDomain:
		return true
	default:
		return false
	}
}

// findingFor builds §8.5's payload and §8.3's routing decision for one
// subject-axis that the §8.2 machine has just moved to Firing.
//
// Both are returned even when the delivery says metrics-only, because the
// payload is the argument for the decision: a Tier C finding that nobody was
// told about is still the thing an operator asks to see when they turn the
// opt-in on.
func (s *Source) findingFor(sub leeway.SubjectRef, ev *Evaluation, st leeway.AlertState, dist *leeway.Distribution, now time.Time) (leeway.Finding, leeway.Delivery) {
	evidence := s.evidenceFor(ev.Key, ev.Eligible, dist, now)
	return leeway.NewFinding(leeway.FindingInput{
		Subject:     sub,
		TopologyKey: ev.Key,
		Scores:      &ev.Scores,
		Intent:      ev.Intent,
		Verdict:     ev.Verdict,
		Attribution: ev.Scores.Attribute(ev.Intent, evidence, now, s.cfg.Cause),
		Evidence:    evidence,
		State:       st,
	}), ev.Verdict.Route(s.cfg.TierCSignals)
}

// signalFor converts a finding into the Signal the watch pipeline carries.
//
// The finding rides `inject.Payload` like every other source-namespaced kind
// (pkg/inject/schema), which means the §8.5 JSON document does not go on the
// wire as a document — `Message` is a rendering of it. That is a deliberate
// choice rather than a shortfall. Adding a fifth wire struct to signal-schema
// v1 would freeze the whole of the finding shape against fleet consumers, and
// §8.5's payload is barely a day old; the same struct is what `cmd/leeway`
// marshals and what the store keeps, so nothing is lost to a reader who wants
// the table rather than the sentence.
//
// The deployment identity and the fingerprint stay empty for the pipeline to
// stamp (§7.2), like every other source. Reason is the suspected cause, so the
// fingerprint's `reasonClass` is the cause — which is §8.5's rule that a
// differently-caused episode gets its own identity, and the reason pinning did
// not need a kind of its own.
func signalFor(f leeway.Finding, now time.Time) sources.Signal {
	return sources.Signal{
		Kind:     f.Kind,
		Source:   engine.SourceSentinel,
		Severity: engine.Severity(f.Severity),
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: findingUID(f.Subject, f.TopologyKey), Reason: string(f.SuspectedCause)},
			Namespace:    f.Subject.Namespace,
			KindOfObject: string(f.Subject.Kind),
			Name:         f.Subject.Name,
			Message:      findingMessage(f),
			FirstSeen:    f.FirstSeenAt,
			LastSeen:     now,
			Count:        1,
		},
	}
}

// messageDomains is how many domain rows the message carries. §8.5 caps the
// per-domain fall lines at three for the same reason: a forty-zone cluster
// losing nodes everywhere is one story, not forty — and the whole table is in
// the stored payload for anyone who wants it.
const messageDomains = 3

// findingMessage renders a finding as one line of prose.
//
// The three rows with the largest absolute delta are chosen, then printed in
// the payload's canonical order rather than in rank order, so two findings on
// one subject an hour apart diff cleanly even when the worst domain changed.
func findingMessage(f leeway.Finding) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s on %s (tier %s, drift %.2f", headline(f.Kind), f.TopologyKey, f.Tier, f.Score.Drift)
	if f.Score.ExcessSkew > 0 {
		fmt.Fprintf(&b, ", excess skew %d", f.Score.ExcessSkew)
	}
	if f.Score.RelocationDistance > 0 {
		fmt.Fprintf(&b, ", %d object(s) misplaced", f.Score.RelocationDistance)
	}
	b.WriteString(")")

	if rows := worstDomains(f.Domains, messageDomains); len(rows) > 0 {
		b.WriteString(": ")
		for i, r := range rows {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%s %d/%d", r.Domain, r.Actual, r.Expected)
		}
		if len(rows) < len(f.Domains) {
			fmt.Fprintf(&b, " (+%d more)", len(f.Domains)-len(rows))
		}
	}

	fmt.Fprintf(&b, " — suspected %s", f.SuspectedCause)
	for _, factor := range f.ContributingFactors {
		b.WriteString("; ")
		b.WriteString(factor)
	}
	if f.Intent != nil {
		fmt.Fprintf(&b, "; intent %s (%s)", f.Intent.Source, f.Intent.Confidence)
	} else {
		// Said out loud rather than left to be inferred from an absent clause.
		// "Nobody declared anything and we apportioned an expectation" is the
		// first thing a reader disputing a Tier C finding needs to know.
		b.WriteString("; no declared or inferred intent — scored against an even apportionment")
	}
	if f.Transient != "" {
		fmt.Fprintf(&b, "; breached during %s", f.Transient)
		if f.Relaxed {
			b.WriteString(" with thresholds relaxed")
		}
	}
	return b.String()
}

// headline is the human name of a finding kind, for the front of the message.
// The kind itself is already on the wire in its own field; repeating
// `leeway.placement_drift` verbatim would spend the most-read words in the
// payload on something the reader can see.
func headline(kind string) string {
	switch kind {
	case leeway.KindContractViolated:
		return "topology contract violated"
	case leeway.KindBaselineBreach:
		return "placement baseline breach"
	case leeway.KindDomainUnavailable:
		return "topology domain unavailable"
	default:
		return "placement drift"
	}
}

// worstDomains picks the n rows furthest from their expectation, restoring the
// input order before returning.
//
// Ties break on the domain name so the choice is reproducible: two zones equally
// over-filled must not swap places between two findings about the same subject.
func worstDomains(rows []leeway.FindingDomain, n int) []leeway.FindingDomain {
	if len(rows) <= n {
		return rows
	}
	order := make([]int, len(rows))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		x, y := rows[order[a]], rows[order[b]]
		if abs64(x.Delta) != abs64(y.Delta) {
			return abs64(x.Delta) > abs64(y.Delta)
		}
		return x.Domain < y.Domain
	})
	keep := order[:n]
	sort.Ints(keep)
	out := make([]leeway.FindingDomain, 0, n)
	for _, i := range keep {
		out = append(out, rows[i])
	}
	return out
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
