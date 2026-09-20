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
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

func TestFindingUID_RoundTrips(t *testing.T) {
	for _, tc := range []struct {
		name string
		sub  leeway.SubjectRef
		key  leeway.TopologyKey
	}{
		{"deployment on zone", webSubject, zoneKey},
		{"statefulset on region", leeway.SubjectRef{Kind: leeway.SubjectStatefulSet, Namespace: "prod", Name: "db"}, regionKey},
		{"cluster-scoped subject", leeway.SubjectRef{Kind: leeway.SubjectDaemonSet, Name: "node-agent"}, poolKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sub, key, ok := parseFindingUID(findingUID(tc.sub, tc.key))
			if !ok {
				t.Fatalf("parseFindingUID(%q) refused its own output", findingUID(tc.sub, tc.key))
			}
			if sub != tc.sub || key != tc.key {
				t.Errorf("round-trip = (%v, %q), want (%v, %q)", sub, key, tc.sub, tc.key)
			}
		})
	}
}

func TestFindingUID_CarriesTheAxis(t *testing.T) {
	// The whole reason the UID is synthetic rather than the workload's own:
	// one subject drifting on two axes is two incidents with two dwell timers,
	// and a shared UID would fold the second into the first as a repeat.
	if findingUID(webSubject, zoneKey) == findingUID(webSubject, regionKey) {
		t.Error("the zone and region findings for one subject share a UID")
	}
}

func TestParseFindingUID_DeclinesForeignIncidents(t *testing.T) {
	// The clearance observer's gate: anything this source did not mint must
	// come back false so another source's observer gets to judge it.
	for _, uid := range []string{
		"",
		"nodegroup:default-pool",               // capacity's synthetic UID
		"b0a1c2d3-4e5f-6071-8293-a4b5c6d7e8f9", // a real object UID
		"leeway:",                              // prefix only
		"leeway:Deployment/prod/web",           // no axis
		"leeway:nonsense|topology.kubernetes.io/zone",
	} {
		if _, _, ok := parseFindingUID(uid); ok {
			t.Errorf("parseFindingUID(%q) claimed an incident it did not mint", uid)
		}
	}
}

// finding builds a minimal §8.5 payload for the rendering tests.
func finding(kind string, domains ...leeway.FindingDomain) leeway.Finding {
	return leeway.Finding{
		Kind:           kind,
		Subject:        webSubject,
		TopologyKey:    zoneKey,
		Tier:           leeway.TierB.String(),
		Severity:       "warning",
		Score:          leeway.FindingScore{Drift: 0.5},
		Domains:        domains,
		SuspectedCause: leeway.CauseUnknown,
		FirstSeenAt:    t0,
	}
}

func row(d leeway.Domain, actual, expected int64) leeway.FindingDomain {
	return leeway.FindingDomain{Domain: d, Actual: actual, Expected: expected, Delta: actual - expected}
}

func TestFindingMessage(t *testing.T) {
	f := finding(leeway.KindPlacementDrift, row("us-central1-a", 6, 2), row("us-central1-b", 0, 2))
	f.Score.ExcessSkew = 4
	f.Score.RelocationDistance = 4
	f.SuspectedCause = leeway.CauseDomainOutage
	f.ContributingFactors = []string{"us-central1-b lost 3 of 3 ready nodes"}
	f.Intent = &leeway.FindingIntent{Source: "spread-constraint", Confidence: "declared"}

	got := findingMessage(f)
	for _, want := range []string{
		"placement drift on topology.kubernetes.io/zone",
		"drift 0.50",
		"excess skew 4",
		"4 object(s) misplaced",
		"us-central1-a 6/2, us-central1-b 0/2",
		"suspected " + string(leeway.CauseDomainOutage),
		"us-central1-b lost 3 of 3 ready nodes",
		"intent spread-constraint (declared)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("message is missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "more)") {
		t.Errorf("two domains should not be truncated:\n%s", got)
	}
}

func TestFindingMessage_SaysWhenNobodyDeclaredAnIntent(t *testing.T) {
	// The Tier C reader's first question. An absent clause would leave them to
	// infer it, and the whole dispute about a learned baseline turns on it.
	got := findingMessage(finding(leeway.KindBaselineBreach, row("us-central1-a", 3, 1)))
	if !strings.Contains(got, "no declared or inferred intent") {
		t.Errorf("an intent-less finding did not say so:\n%s", got)
	}
	if !strings.HasPrefix(got, "placement baseline breach") {
		t.Errorf("headline = %q, want the baseline-breach wording", got)
	}
}

func TestFindingMessage_NamesTheTransient(t *testing.T) {
	f := finding(leeway.KindPlacementDrift, row("us-central1-a", 3, 1))
	f.Transient = "rollout"
	f.Relaxed = true
	got := findingMessage(f)
	if !strings.Contains(got, "breached during rollout with thresholds relaxed") {
		t.Errorf("message did not report the §7.6 state:\n%s", got)
	}
}

func TestFindingMessage_TruncatesToTheWorstDomains(t *testing.T) {
	f := finding(leeway.KindPlacementDrift,
		row("a", 1, 1), // delta 0
		row("b", 9, 2), // delta +7, worst
		row("c", 0, 4), // delta -4
		row("d", 5, 2), // delta +3
		row("e", 2, 2), // delta 0
	)
	got := findingMessage(f)
	if !strings.Contains(got, "b 9/2, c 0/4, d 5/2") {
		t.Errorf("the three worst domains are not in canonical order:\n%s", got)
	}
	if !strings.Contains(got, "(+2 more)") {
		t.Errorf("the truncation is not declared:\n%s", got)
	}
}

func TestWorstDomains(t *testing.T) {
	rows := []leeway.FindingDomain{
		{Domain: "a", Delta: 2}, {Domain: "b", Delta: -2}, {Domain: "c", Delta: 1},
	}
	if got := worstDomains(rows, 5); len(got) != 3 {
		t.Errorf("a short table was truncated: %+v", got)
	}

	// Equal magnitudes break on the name, so the pick is reproducible across
	// two findings about the same subject.
	got := worstDomains(rows, 2)
	if len(got) != 2 || got[0].Domain != "a" || got[1].Domain != "b" {
		t.Errorf("worstDomains = %+v, want a and b (ties break on the name)", got)
	}
}

func TestSignalFor(t *testing.T) {
	f := finding(leeway.KindContractViolated, row("us-central1-a", 4, 2))
	f.Severity = "critical"
	f.SuspectedCause = leeway.CauseVolumePinning

	sig := signalFor(f, t0.Add(time.Hour))
	if sig.Kind != leeway.KindContractViolated {
		t.Errorf("Kind = %q, want %q", sig.Kind, leeway.KindContractViolated)
	}
	if sig.Source != engine.SourceSentinel {
		t.Errorf("Source = %q, want the sentinel", sig.Source)
	}
	if sig.Severity != engine.Severity("critical") {
		t.Errorf("Severity = %q, want critical", sig.Severity)
	}
	if sig.Key.UID != findingUID(webSubject, zoneKey) {
		t.Errorf("UID = %q, want the synthetic subject-axis key", sig.Key.UID)
	}
	if sig.Key.Reason != string(leeway.CauseVolumePinning) {
		t.Errorf("Reason = %q, want the suspected cause — it is the fingerprint's reasonClass", sig.Key.Reason)
	}
	if sig.Namespace != "prod" || sig.Name != "web" || sig.KindOfObject != string(leeway.SubjectDeployment) {
		t.Errorf("subject identity = %s/%s %s, want the Deployment prod/web", sig.Namespace, sig.Name, sig.KindOfObject)
	}
	if !sig.FirstSeen.Equal(t0) {
		t.Errorf("FirstSeen = %v, want the episode's start %v — not this evaluation's", sig.FirstSeen, t0)
	}
	if sig.Count != 1 {
		t.Errorf("Count = %d, want 1", sig.Count)
	}
	if sig.Fingerprint != "" {
		t.Error("the source stamped a fingerprint; the pipeline owns that (§7.2)")
	}
}

func TestHeadline(t *testing.T) {
	for kind, want := range map[string]string{
		leeway.KindContractViolated:  "topology contract violated",
		leeway.KindPlacementDrift:    "placement drift",
		leeway.KindBaselineBreach:    "placement baseline breach",
		leeway.KindDomainUnavailable: "topology domain unavailable",
		"something.else":             "placement drift",
	} {
		if got := headline(kind); got != want {
			t.Errorf("headline(%q) = %q, want %q", kind, got, want)
		}
	}
}
