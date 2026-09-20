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

package computeclass

import (
	"strings"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// uidPrefix namespaces the synthetic incident UIDs this source mints.
//
// Distinct from topologydrift's `leeway:` on purpose, and not merely as a
// nicety. That source's clearance observer parses every incident UID beginning
// `leeway:` and answers for the ones it can read; a rank UID sharing the prefix
// would parse cleanly there — a PreferenceAxis subject and a rule name where it
// expects a topology key — and then be reported cleared-object_deleted on the
// first sweep, because no Deployment by that name is in its pod index. A
// hyphen rather than a colon is what makes the prefix test fail outright
// instead of half-succeeding.
const uidPrefix = "leeway-rank:"

// uidRuleSep separates the axis from the rule inside the UID. A pipe, for the
// reason topologydrift uses one: it cannot appear in either half.
const uidRuleSep = "|"

// findingUID renders the incident identity for one rule on one axis.
//
// The rule is in the key because the episodes are per-rule (see alerts.go), and
// a UID that carried only the axis would fold a dead tier and a last-rank
// breach into one incident — the second silently swallowed as a repeat of the
// first, which is the one failure mode a dedup key has.
func findingUID(k alertKey) string {
	return uidPrefix + leeway.SubjectRef{Kind: leeway.SubjectPreferenceAxis, Name: k.Axis.Name}.String() +
		uidRuleSep + k.stateKey()
}

// parseFindingUID reads back what findingUID wrote, reporting false for any
// other source's UID.
func parseFindingUID(uid string) (alertKey, bool) {
	rest, ok := strings.CutPrefix(uid, uidPrefix)
	if !ok {
		return alertKey{}, false
	}
	sub, state, ok := strings.Cut(rest, uidRuleSep)
	if !ok {
		return alertKey{}, false
	}
	return parseStateKey(sub, state)
}

// objectKind is the provider's own object kind, which §8.5's payload carries
// because the neutral subject vocabulary deliberately does not.
//
// Derived from the provider rather than configured: the mapping is a property
// of the mechanism, and a second provider arriving means a second case here
// alongside its extractor and its resolver, not a flag.
func objectKind(p leeway.Provider) string {
	if p == leeway.ProviderGKEComputeClass {
		return "ComputeClass"
	}
	return "PreferenceAxis"
}

// findingFor builds §8.5's payload and §8.3's routing decision for one episode
// that has just crossed its dwell.
//
// Both are returned even when the delivery says metrics-only, for the reason
// topologydrift returns both: the payload is the argument for the decision, and
// a Tier C finding nobody was told about is exactly what an operator asks to
// see before turning the opt-in on.
func (s *Source) findingFor(j *rankJudgement, v leeway.RankVerdict, st leeway.AlertState) (leeway.RankFinding, leeway.Delivery) {
	return leeway.NewRankFinding(leeway.RankFindingInput{
		Class:      j.class,
		Verdict:    v,
		Window:     j.window,
		State:      st,
		ObjectKind: objectKind(j.axis.Provider),
	}), leeway.RouteRank(v, s.cfg.TierCSignals)
}

// signalFor converts a rank finding into the Signal the watch pipeline carries.
//
// The finding rides `inject.Payload` like every other source-namespaced kind
// and `Message` is a rendering of it, not the document — the same call
// topologydrift made and for the same reason: adding a wire struct to
// signal-schema v1 would freeze §8.5's payload shape against fleet consumers
// while it is still days old.
//
// Reason is the judging rule, so the fingerprint's `reasonClass` is the rule.
// That is what gives the two `leeway.rank_degraded` rules separate identities
// downstream: they share a kind, and without the rule in the reason an estate
// that tripped the floor and then the ceiling would look like one recurring
// incident rather than two conditions.
func signalFor(f leeway.RankFinding, k alertKey, now time.Time) sources.Signal {
	return sources.Signal{
		Kind:     f.Kind,
		Source:   engine.SourceSentinel,
		Severity: engine.Severity(f.Severity),
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: findingUID(k), Reason: f.Rule},
			KindOfObject: string(f.Subject.Kind),
			Name:         f.Subject.Name,
			Message:      leeway.RankMessage(f),
			FirstSeen:    f.FirstSeenAt,
			LastSeen:     now,
			Count:        1,
		},
	}
}

// ClearanceObserver exposes the source to the §7.4 recovery tracker.
func (s *Source) ClearanceObserver() engine.ClearanceObserver { return s }

// Clearance implements engine.ClearanceObserver (DESIGN §7.4: every source that
// can observe a symptom can observe its absence).
//
// The symptom is an open §8.2 episode, exactly as it is for topologydrift, and
// the two dwells compose the same way: §8.2 decides when the rank condition is
// over, `--recovery-stable-for` decides when it has been over long enough to
// say so.
//
// ok=false for incidents this source did not raise, and for a process whose
// caches have not synced — before the barrier every axis looks unoccupied, and
// reporting a fleet of findings recovered because we have not read the cluster
// yet is the one wrong answer that cannot be taken back.
func (s *Source) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	k, ok := parseFindingUID(inc.Key.UID)
	if !ok {
		return engine.Clearance{}, false
	}
	if !s.HasSynced() {
		return engine.Clearance{}, false
	}

	if !s.axisLive(k.Axis) {
		// The class is gone. StableSince stays zero: the deletion is the fact,
		// and dating it from the sweep that noticed would be inventing a
		// timestamp.
		return engine.Clearance{Cleared: true, Resolution: engine.ResolutionObjectDeleted}, true
	}
	if s.alerts.open(k) {
		// Firing or Pending. Either way the condition is back or never left,
		// and PhaseResolving reports Firing, so this stays symptomatic for the
		// whole of §8.2's resolve dwell.
		return engine.Clearance{}, true
	}
	return engine.Clearance{Cleared: true, Resolution: engine.ResolutionRecovered}, true
}

// axisLive reports whether a decoded class still declares this axis.
func (s *Source) axisLive(key leeway.AxisKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	class, ok := s.classes[key.Name]
	return ok && class.Axis.Key == key
}
