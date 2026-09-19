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
	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// Evaluation is one subject scored on one topology axis: everything §7.1–§7.4
// produced for it.
//
// The inputs are carried alongside the outputs because a score is not
// interpretable without them. "Drift 0.33" is a different statement about a
// subject eligible for three zones and one eligible for two, and a reader —
// whether that is a finding, a dashboard or somebody in a debugger — needs the
// eligible set and the intent that shaped the expectation to know which was
// measured.
type Evaluation struct {
	Key leeway.TopologyKey

	// Intent is §5.1's winner for this axis, or nil where the subject
	// expressed nothing. Nil is not a degraded evaluation: an apportioned
	// expectation over the eligible domains is still a measurement, it is just
	// a Tier C one.
	Intent *leeway.Intent

	Eligible      leeway.Eligibility
	Apportionment leeway.Apportionment
	Scores        leeway.Scores

	// Suppression is §7.6's answer for this axis at the moment it was scored.
	// Carried rather than folded away into the verdict because "no breach" and
	// "a breach we declined to report because a zone was down" are different
	// statements, and only one of them should reassure anybody.
	Suppression leeway.Suppression

	// Verdict is §7.4, §7.6 and §8.1 applied to the scores. It is computed even
	// for a gated subject, where it reads as "not breached" with the gate's
	// reason, so that a caller never has to remember to check Evaluable first.
	Verdict leeway.Verdict
}

// Suppressor answers §7.6 for one subject-axis, given the domains the subject
// is eligible for on that axis.
//
// A callback rather than a precomputed map because the domain-outage row is
// per-axis *and* per-subject: it asks whether any domain this particular
// subject could have used is out, and two subjects on the same axis have
// different eligible sets.
//
// Nil is "no transient", which is what every caller that does not watch a
// cluster — the score tests, the false-positive corpus — should pass.
type Suppressor func(key leeway.TopologyKey, eligible leeway.Eligibility) leeway.Suppression

// ScoreSubject runs §7.1–§7.4 for one subject, on every axis its resolution
// covers.
//
// The axes come from Eligible rather than from Intents, because §7.1 computes
// an eligible set for every tracked key and not only for the keys carrying an
// intent — a subject with no spread constraint at all is still scored, against
// an apportionment over the domains its nodeSelector and tolerations leave it.
// Driving the loop off Intents instead would silently stop measuring the
// majority of an estate.
//
// dists is the subject's distributions per axis, as returned by
// State.Snapshot. A missing axis scores as an empty distribution rather than
// being skipped, which is how a subject whose pods have all gone Pending still
// reports its eligible domains holding nothing.
//
// The result is in the resolution's canonical key order, so two evaluations of
// the same subject produce the same slice order and a caller may index it.
func ScoreSubject(res Resolution, dists map[leeway.TopologyKey]*leeway.Distribution, t leeway.Thresholds, sup Suppressor) []Evaluation {
	keys := leeway.SortedKeys(res.Eligible)
	out := make([]Evaluation, 0, len(keys))
	for _, key := range keys {
		var s leeway.Suppression
		if sup != nil {
			s = sup(key, res.Eligible[key])
		}
		out = append(out, ScoreAxis(key, res.Intents[key], res.Eligible[key], dists[key], t, s))
	}
	return out
}

// ScoreAxis runs §7.2–§7.4 for one (subject, topology key), and applies §7.6.
//
// Only Running objects are counted. §7.6 is the reason: a distribution that
// includes Terminating pods describes where the subject *was*, and one that
// includes Pending pods describes where the scheduler has so far declined to
// put it. Both are real facts about a cluster mid-move and neither is where the
// workload is, which is the question drift asks. The other states are counted
// and exported; they feed suppression, not the score.
//
// sup is §7.6's verdict on the cluster's state, and it reaches only the
// judgement: the scores themselves are recorded as measured whatever is going
// on, because §7.4's metrics are the evidence a suppression was right.
func ScoreAxis(key leeway.TopologyKey, intent *leeway.Intent, eligible leeway.Eligibility, dist *leeway.Distribution, t leeway.Thresholds, sup leeway.Suppression) Evaluation {
	ev := Evaluation{Key: key, Intent: intent, Eligible: eligible, Suppression: sup}

	var actual []int64
	if dist != nil {
		actual = dist.Counts(eligible.Domains, leeway.StateRunning)
	} else {
		actual = make([]int64, len(eligible.Domains))
	}

	var n int64
	for _, a := range actual {
		n += a
	}

	var maxSkew *int32
	if intent != nil {
		maxSkew = intent.MaxSkew
	}

	// WeightsFor, not Weights(intent.Weighting): a policy's
	// expectedDistribution *replaces* the weighting rule rather than
	// parameterising it, and reading the weighting straight off the intent
	// would apportion a declared 40/40/20 as even thirds and then report the
	// difference as drift.
	ev.Apportionment = leeway.Apportion(n, eligible.WeightsFor(intent), capsFor(intent, eligible))
	ev.Scores = leeway.Score(eligible.Domains, actual, ev.Apportionment, maxSkew, t)
	ev.Verdict = ev.Scores.JudgeTransient(intent, t, sup)
	return ev
}

// capsFor turns an intent's §5.1 per-domain ceiling into the §7.2 caps slice
// Apportion takes.
//
// This conversion cannot happen during inference, which is why the intent
// carries a ceiling and not a caps slice: §7.1 is what decides how many domains
// there are, and it runs afterwards. Doing it here also means the caps are
// index-aligned with the eligible set by construction rather than by
// convention.
//
// Nil for an intent that declares no ceiling at all, which is Apportion's
// "no domain is capped" and avoids allocating a slice of Uncapped per
// evaluation for the overwhelmingly common case.
func capsFor(intent *leeway.Intent, eligible leeway.Eligibility) []int64 {
	if intent == nil || (intent.MaxPerDomain == nil && len(intent.DomainCaps) == 0) {
		return nil
	}
	caps := make([]int64, len(eligible.Domains))
	for i, d := range eligible.Domains {
		switch c, named := intent.DomainCaps[d]; {
		case named:
			// A named domain's own ceiling wins over the uniform one: naming
			// it is the more specific statement, and it is the only way to say
			// that one domain is different.
			caps[i] = c
		case intent.MaxPerDomain != nil:
			caps[i] = *intent.MaxPerDomain
		default:
			caps[i] = leeway.Uncapped
		}
	}
	return caps
}
