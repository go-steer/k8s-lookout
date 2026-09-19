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

package leeway

import (
	"encoding/json"
	"testing"
	"time"
)

// designExample is §8.5's payload, rebuilt from the pieces each layer
// produces. Its numbers are the design's verbatim, including the two the
// example does not derive from its own domain table.
func designExample() FindingInput {
	lost := time.Date(2026, 9, 10, 9, 14, 22, 0, time.UTC)
	return FindingInput{
		Subject:     SubjectRef{Kind: SubjectDeployment, Namespace: "payments", Name: "api"},
		TopologyKey: "topology.kubernetes.io/zone",
		Scores: &Scores{
			Domains:           []Domain{zoneA, zoneB, zoneC},
			Actual:            []int64{8, 3, 1},
			Expected:          []int64{4, 4, 4},
			Total:             12,
			Drift:             0.42,
			ObservedSkew:      7,
			MinAchievableSkew: 1,
			ExcessSkew:        6,
			Relocation:        5,
			MaxDomainShare:    0.67,
		},
		Intent: &Intent{
			TopologyKey: "topology.kubernetes.io/zone",
			Mode:        ModeSpread,
			Source:      SourcePodAntiAffinityPreferred,
			Confidence:  ConfidenceInferred,
			Weighting:   WeightAllocatableCPU,
			Evidence: []EvidenceItem{
				{Detail: "preferredDuringScheduling podAntiAffinity on topology.kubernetes.io/zone (weight 100)"},
				{Detail: "no topologySpreadConstraints declared"},
				{Detail: "eligible domains restricted to [us-east-1a,us-east-1b,us-east-1c] by nodeSelector workload=general"},
			},
		},
		Verdict: Verdict{Breached: true, Kind: BreachDrift, Tier: TierB, Severity: severityWarning},
		Attribution: Attribution{
			Cause: CauseCapacityShortfall,
			Factors: []string{
				"us-east-1c ready node count fell 12 → 3 at 2026-09-10T09:14:22Z",
				"2 pods Pending with FailedScheduling: 0/26 nodes available: 9 Insufficient cpu",
				"0 pods pinned by zonal volumes",
			},
			Notes: []string{"", "", "9 nodes lost in last 2h"},
		},
		Evidence: Evidence{Domains: map[Domain]DomainFacts{
			zoneA: {ReadyNodes: 12, PeakReadyNodes: 12},
			zoneB: {ReadyNodes: 11, PeakReadyNodes: 11},
			zoneC: {ReadyNodes: 3, PeakReadyNodes: 12, LostAt: lost},
		}},
		State: AlertState{
			Phase:       PhaseFiring,
			FirstSeenAt: time.Date(2026, 9, 10, 9, 31, 10, 0, time.UTC),
		},
	}
}

// TestNewFinding_IsTheDesignsWorkedExample pins §8.5 byte for byte. The one
// deliberate departure is the intent's source and confidence spellings: the
// design wrote them as Go identifiers, and the shipped values are the
// kebab-case §8.4 metric labels, because a fact should not have two names.
func TestNewFinding_IsTheDesignsWorkedExample(t *testing.T) {
	got, err := json.MarshalIndent(NewFinding(designExample()), "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{
  "kind": "leeway.placement_drift",
  "subject": {
    "kind": "Deployment",
    "namespace": "payments",
    "name": "api"
  },
  "topologyKey": "topology.kubernetes.io/zone",
  "tier": "B",
  "severity": "warning",
  "score": {
    "drift": 0.42,
    "observedSkew": 7,
    "minAchievableSkew": 1,
    "excessSkew": 6,
    "relocationDistance": 5,
    "maxDomainShare": 0.67
  },
  "intent": {
    "source": "pod-anti-affinity-preferred",
    "confidence": "inferred",
    "weighting": "AllocatableCPU",
    "evidence": [
      "preferredDuringScheduling podAntiAffinity on topology.kubernetes.io/zone (weight 100)",
      "no topologySpreadConstraints declared",
      "eligible domains restricted to [us-east-1a,us-east-1b,us-east-1c] by nodeSelector workload=general"
    ]
  },
  "domains": [
    {
      "domain": "us-east-1a",
      "actual": 8,
      "expected": 4,
      "delta": 4,
      "readyNodes": 12
    },
    {
      "domain": "us-east-1b",
      "actual": 3,
      "expected": 4,
      "delta": -1,
      "readyNodes": 11
    },
    {
      "domain": "us-east-1c",
      "actual": 1,
      "expected": 4,
      "delta": -3,
      "readyNodes": 3,
      "note": "9 nodes lost in last 2h"
    }
  ],
  "suspectedCause": "domain_capacity_shortfall",
  "contributingFactors": [
    "us-east-1c ready node count fell 12 → 3 at 2026-09-10T09:14:22Z",
    "2 pods Pending with FailedScheduling: 0/26 nodes available: 9 Insufficient cpu",
    "0 pods pinned by zonal volumes"
  ],
  "firstSeenAt": "2026-09-10T09:31:10Z"
}`
	if string(got) != want {
		t.Errorf("payload =\n%s\n\nwant\n%s", got, want)
	}
}

func TestFindingKind_TheTierLeadsAndTierCSplitsOnTheSource(t *testing.T) {
	learned := &Intent{Source: SourceLearnedBaseline, Confidence: ConfidenceLearned}
	cases := []struct {
		name   string
		tier   Tier
		intent *Intent
		want   string
	}{
		{"a declared contract", TierA, spreadIntent(), KindContractViolated},
		{"an inferred deviation", TierB, spreadIntent(), KindPlacementDrift},
		{"a subject's own history", TierC, learned, KindBaselineBreach},
		{"no intent at all", TierC, nil, KindPlacementDrift},
		{"an inferred intent at tier C", TierC, spreadIntent(), KindPlacementDrift},
		{
			// Structurally reachable: a learned baseline is not an assumed
			// cluster default, so it passes declaredContract and could carry a
			// MaxSkew. §8.1 caps it at Tier C all the same, and the kind must
			// follow rather than claim a contract nobody declared.
			"a learned baseline that somehow fired a contract rule",
			TierC, learned, KindBaselineBreach,
		},
		{"a tier A learned intent keeps the tier's kind", TierA, learned, KindContractViolated},
	}
	for _, tc := range cases {
		if got := FindingKind(tc.tier, tc.intent); got != tc.want {
			t.Errorf("%s: FindingKind = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestNewFinding_ASubjectWithNoIntentOmitsTheObjectRatherThanEmptyingIt(t *testing.T) {
	in := designExample()
	in.Intent = nil
	in.Verdict.Tier = TierC
	in.Verdict.Severity = severityInfo

	f := NewFinding(in)
	if f.Intent != nil {
		t.Errorf("Intent = %+v, want nil", f.Intent)
	}
	if f.Kind != KindPlacementDrift {
		t.Errorf("Kind = %q, want %q", f.Kind, KindPlacementDrift)
	}
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var round map[string]any
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := round["intent"]; ok {
		t.Errorf("payload carries an intent key: %s", b)
	}
}

func TestNewFinding_AnIgnoredEvidenceItemSaysSo(t *testing.T) {
	in := designExample()
	in.Intent.Evidence = append(in.Intent.Evidence, EvidenceItem{
		Detail:  "workload annotation naming a 50/50 split",
		Ignored: true,
	})
	f := NewFinding(in)
	want := "(not applied) workload annotation naming a 50/50 split"
	if got := f.Intent.Evidence[3]; got != want {
		t.Errorf("evidence = %q, want %q", got, want)
	}
}

func TestNewFinding_TheDeclaredBoundReachesThePayload(t *testing.T) {
	in := designExample()
	in.Intent.MaxSkew = ptr(int32(1))
	f := NewFinding(in)
	if f.Intent.MaxSkew == nil || *f.Intent.MaxSkew != 1 {
		t.Fatalf("MaxSkew = %v, want 1", f.Intent.MaxSkew)
	}
	// And is absent, not zero, where no bound was declared.
	if f := NewFinding(designExample()); f.Intent.MaxSkew != nil {
		t.Errorf("MaxSkew = %v, want nil", f.Intent.MaxSkew)
	}
}

func TestNewFinding_AnUnattributedFindingSaysUnknownRatherThanNothing(t *testing.T) {
	in := designExample()
	in.Attribution = Attribution{}
	f := NewFinding(in)
	if f.SuspectedCause != CauseUnknown {
		t.Errorf("SuspectedCause = %q, want %q", f.SuspectedCause, CauseUnknown)
	}
	if len(f.Domains) != 3 {
		t.Errorf("Domains = %+v, want the table built with no notes", f.Domains)
	}
	for _, d := range f.Domains {
		if d.Note != "" {
			t.Errorf("domain %s carries a note %q", d.Domain, d.Note)
		}
	}
}

func TestNewFinding_TheTransientStateReachesThePayload(t *testing.T) {
	in := designExample()
	in.Verdict.Transient = TransientRollout
	in.Verdict.Relaxed = true
	f := NewFinding(in)
	if f.Transient != "rollout" || !f.Relaxed {
		t.Errorf("transient = %q relaxed = %v, want rollout/true", f.Transient, f.Relaxed)
	}
	// A quiet cluster carries neither key.
	b, err := json.Marshal(NewFinding(designExample()))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var round map[string]any
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := round["transient"]; ok {
		t.Errorf("payload carries a transient key: %s", b)
	}
	if _, ok := round["relaxed"]; ok {
		t.Errorf("payload carries a relaxed key: %s", b)
	}
}

func TestNewFinding_FirstSeenAtIsTheEpisodesNotTheEvaluations(t *testing.T) {
	in := designExample()
	f := NewFinding(in)
	if !f.FirstSeenAt.Equal(in.State.FirstSeenAt) {
		t.Errorf("FirstSeenAt = %s, want the dwell state's %s", f.FirstSeenAt, in.State.FirstSeenAt)
	}
	// A local zone is normalised away, because two sentinels reporting the
	// same episode must not disagree about when it started.
	in.State.FirstSeenAt = in.State.FirstSeenAt.In(time.FixedZone("somewhere", 5*3600))
	if got := NewFinding(in).FirstSeenAt; got.Location() != time.UTC {
		t.Errorf("FirstSeenAt = %s, want UTC", got)
	}
}

func TestNewFinding_AClusterScopedSubjectOmitsTheNamespace(t *testing.T) {
	in := designExample()
	in.Subject = SubjectRef{Kind: SubjectNodeGroup, Name: "np-general"}
	b, err := json.Marshal(NewFinding(in).Subject)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"kind":"NodeGroup","name":"np-general"}`
	if string(b) != want {
		t.Errorf("subject = %s, want %s", b, want)
	}
}

// TestNewFinding_AMismatchedScoreAndNoteSetComesOutShortNotPanicking: the
// three slices are assembled by different layers, and a payload builder is the
// wrong place to discover they disagree.
func TestNewFinding_AMismatchedScoreAndNoteSetComesOutShortNotPanicking(t *testing.T) {
	in := designExample()
	in.Scores.Expected = nil
	in.Scores.Actual = []int64{8}
	in.Attribution.Notes = []string{"only one note"}

	f := NewFinding(in)
	if len(f.Domains) != 3 {
		t.Fatalf("Domains = %+v, want one row per domain", f.Domains)
	}
	if f.Domains[0] != (FindingDomain{Domain: zoneA, Actual: 8, ReadyNodes: 12, Note: "only one note"}) {
		t.Errorf("Domains[0] = %+v", f.Domains[0])
	}
	if f.Domains[2] != (FindingDomain{Domain: zoneC, ReadyNodes: 3}) {
		t.Errorf("Domains[2] = %+v, want the columns that were not supplied left at zero", f.Domains[2])
	}
}

func TestNewFinding_NoScoresStillProducesAWellFormedPayload(t *testing.T) {
	in := designExample()
	in.Scores = nil
	f := NewFinding(in)
	if f.Kind != KindPlacementDrift || f.Domains != nil {
		t.Errorf("Finding = %+v, want an empty domain table", f)
	}
	if f.Score != (FindingScore{}) {
		t.Errorf("Score = %+v, want the zero score", f.Score)
	}
}
