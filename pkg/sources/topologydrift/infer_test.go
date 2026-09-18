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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// appSelector is the selector a workload's own TSC almost always carries.
func appSelector() *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}}
}

// tscPod builds a pod labelled app=api carrying the given constraints.
func tscPod(tscs ...corev1.TopologySpreadConstraint) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-abc",
			Namespace: "payments",
			Labels:    map[string]string{"app": "api", "pod-template-hash": "7f9"},
		},
		Spec: corev1.PodSpec{TopologySpreadConstraints: tscs},
	}
}

func zoneSpread(maxSkew int32, action corev1.UnsatisfiableConstraintAction) corev1.TopologySpreadConstraint {
	return corev1.TopologySpreadConstraint{
		MaxSkew:           maxSkew,
		TopologyKey:       corev1.LabelTopologyZone,
		WhenUnsatisfiable: action,
		LabelSelector:     appSelector(),
	}
}

func ptr[T any](v T) *T { return &v }

func TestSpreadConstraintIntents_TheOrdinaryConstraint(t *testing.T) {
	got := SpreadConstraintIntents(tscPod(zoneSpread(1, corev1.DoNotSchedule)))
	if len(got) != 1 {
		t.Fatalf("got %d intents, want 1: %+v", len(got), got)
	}
	in := got[0]

	if in.TopologyKey != leeway.TopologyKey(corev1.LabelTopologyZone) {
		t.Errorf("TopologyKey = %q", in.TopologyKey)
	}
	if in.Mode != leeway.ModeSpread {
		t.Errorf("Mode = %v, want Spread", in.Mode)
	}
	if in.Source != leeway.SourceTopologySpreadConstraint {
		t.Errorf("Source = %v", in.Source)
	}
	if in.Confidence != leeway.ConfidenceDeclared {
		t.Errorf("Confidence = %v, want Declared", in.Confidence)
	}
	if in.MaxSkew == nil || *in.MaxSkew != 1 {
		t.Errorf("MaxSkew = %v, want 1", in.MaxSkew)
	}
	if !in.HardContract() {
		t.Error("a DoNotSchedule constraint with a maxSkew is the Tier A gate and must pass it")
	}
	if len(in.Evidence) != 1 || !strings.Contains(in.Evidence[0].Detail, "labelSelector=app=api") {
		t.Errorf("evidence = %+v", in.Evidence)
	}
}

func TestSpreadConstraintIntents_FieldMapping(t *testing.T) {
	t.Run("ScheduleAnyway is not a hard contract", func(t *testing.T) {
		in := SpreadConstraintIntents(tscPod(zoneSpread(2, corev1.ScheduleAnyway)))[0]
		if in.WhenUnsatisfiable != corev1.ScheduleAnyway {
			t.Errorf("WhenUnsatisfiable = %q", in.WhenUnsatisfiable)
		}
		// §8.1: the user asked for best-effort, so this is Tier B however
		// lopsided it gets.
		if in.HardContract() {
			t.Error("a ScheduleAnyway constraint reached the Tier A gate")
		}
	})

	t.Run("an unset whenUnsatisfiable is DoNotSchedule", func(t *testing.T) {
		// The API defaults it, and the stricter reading is the right one for
		// an object that reached us without the default applied.
		in := SpreadConstraintIntents(tscPod(zoneSpread(1, "")))[0]
		if in.WhenUnsatisfiable != corev1.DoNotSchedule || !in.HardContract() {
			t.Errorf("unset whenUnsatisfiable = %q, hard=%v", in.WhenUnsatisfiable, in.HardContract())
		}
	})

	t.Run("minDomains with DoNotSchedule", func(t *testing.T) {
		c := zoneSpread(1, corev1.DoNotSchedule)
		c.MinDomains = ptr[int32](3)
		in := SpreadConstraintIntents(tscPod(c))[0]
		if in.MinDomains == nil || *in.MinDomains != 3 {
			t.Fatalf("MinDomains = %v, want 3", in.MinDomains)
		}
		if !strings.Contains(in.Evidence[0].Detail, "minDomains=3") {
			t.Errorf("evidence = %q", in.Evidence[0].Detail)
		}
	})

	t.Run("minDomains with ScheduleAnyway is dropped", func(t *testing.T) {
		// The scheduler ignores it. Carrying it would pad the eligible set
		// with synthetic present-with-zero domains and manufacture skew that
		// nothing in the cluster caused.
		c := zoneSpread(1, corev1.ScheduleAnyway)
		c.MinDomains = ptr[int32](3)
		in := SpreadConstraintIntents(tscPod(c))[0]
		if in.MinDomains != nil {
			t.Errorf("MinDomains = %v, want nil", *in.MinDomains)
		}
		if !strings.Contains(in.Evidence[0].Detail, "minDomains ignored") {
			t.Errorf("the dropped field left no trace: %q", in.Evidence[0].Detail)
		}
	})

	t.Run("node inclusion policies", func(t *testing.T) {
		c := zoneSpread(1, corev1.DoNotSchedule)
		c.NodeAffinityPolicy = ptr(corev1.NodeInclusionPolicyIgnore)
		c.NodeTaintsPolicy = ptr(corev1.NodeInclusionPolicyHonor)
		in := SpreadConstraintIntents(tscPod(c))[0]
		pol := in.EligibilityPolicies()
		if pol.NodeAffinityPolicy != corev1.NodeInclusionPolicyIgnore ||
			pol.NodeTaintsPolicy != corev1.NodeInclusionPolicyHonor {
			t.Errorf("policies = %+v", pol)
		}
	})

	t.Run("unset policies read as the scheduler defaults", func(t *testing.T) {
		in := SpreadConstraintIntents(tscPod(zoneSpread(1, corev1.DoNotSchedule)))[0]
		if pol := in.EligibilityPolicies(); pol != leeway.DefaultNodeInclusionPolicies() {
			t.Errorf("policies = %+v, want the kube-scheduler defaults", pol)
		}
	})

	t.Run("matchLabelKeys is recorded", func(t *testing.T) {
		// pod-template-hash makes the constraint per-ReplicaSet while leeway
		// scores per Deployment. The populations agree once a rollout finishes,
		// so this is evidence rather than a reason to drop the intent.
		c := zoneSpread(1, corev1.DoNotSchedule)
		c.MatchLabelKeys = []string{"pod-template-hash"}
		in := SpreadConstraintIntents(tscPod(c))[0]
		if !strings.Contains(in.Evidence[0].Detail, "matchLabelKeys=pod-template-hash") {
			t.Errorf("evidence = %q", in.Evidence[0].Detail)
		}
	})
}

func TestSpreadConstraintIntents_Skipped(t *testing.T) {
	cases := map[string]corev1.TopologySpreadConstraint{
		// A nil selector selects no pods, so the constraint governs nothing.
		// "Matches every pod" is the other reading of an absent selector and
		// it is not the API's.
		"nil labelSelector": {MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone, WhenUnsatisfiable: corev1.DoNotSchedule},
		"empty topologyKey": {MaxSkew: 1, WhenUnsatisfiable: corev1.DoNotSchedule, LabelSelector: appSelector()},
		// Real, honoured by the scheduler, and not a statement about where
		// *this* subject's replicas should sit.
		"a selector for some other workload": {
			MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone, WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "cache"}},
		},
		"an invalid selector": {
			MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone, WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "app", Operator: metav1.LabelSelectorOperator("Approximately"), Values: []string{"api"}},
			}},
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if got := SpreadConstraintIntents(tscPod(c)); got != nil {
				t.Errorf("got %+v, want no intent", got)
			}
		})
	}

	t.Run("no constraints at all", func(t *testing.T) {
		if got := SpreadConstraintIntents(tscPod()); got != nil {
			t.Errorf("got %+v, want nil", got)
		}
	})
}

func TestSpreadConstraintIntents_EmptySelectorIsKeptButFlagged(t *testing.T) {
	// An empty selector spreads the whole namespace. The workload does want
	// spreading, so the intent stands — but the maxSkew was never a bound on
	// this subject alone and a finding has to be able to say so.
	c := zoneSpread(1, corev1.DoNotSchedule)
	c.LabelSelector = &metav1.LabelSelector{}
	got := SpreadConstraintIntents(tscPod(c))
	if len(got) != 1 {
		t.Fatalf("got %d intents, want 1", len(got))
	}
	if !strings.Contains(got[0].Evidence[0].Detail, "labelSelector is empty") {
		t.Errorf("evidence = %q", got[0].Evidence[0].Detail)
	}
}

func TestSpreadConstraintIntents_EnforcedConstraintWinsItsKey(t *testing.T) {
	// The API allows two constraints on one key as long as whenUnsatisfiable
	// differs, and both resolve from the same IntentSource — so precedence
	// cannot separate them and ResolveIntents breaks the tie by input order.
	// Declaration order would hand the key to whichever the author wrote
	// first, meaning an edit that reorders a list could quietly demote an
	// enforced contract to an advisory one.
	soft := zoneSpread(5, corev1.ScheduleAnyway)
	hard := zoneSpread(1, corev1.DoNotSchedule)

	for _, order := range [][]corev1.TopologySpreadConstraint{{soft, hard}, {hard, soft}} {
		got := SpreadConstraintIntents(tscPod(order...))
		if len(got) != 2 {
			t.Fatalf("got %d intents, want 2", len(got))
		}
		winner := leeway.ResolveIntents(got)[leeway.TopologyKey(corev1.LabelTopologyZone)]
		if winner == nil || *winner.MaxSkew != 1 || !winner.HardContract() {
			t.Fatalf("resolved winner = %+v, want the DoNotSchedule constraint", winner)
		}
		// The loser is not discarded: "we saw your ScheduleAnyway constraint
		// and the enforced one overrode it" is what stops a finding looking
		// wrong.
		var sawIgnored bool
		for _, e := range winner.Evidence {
			sawIgnored = sawIgnored || e.Ignored
		}
		if !sawIgnored {
			t.Errorf("the superseded constraint left no evidence: %+v", winner.Evidence)
		}
	}
}

func TestSpreadConstraintIntents_DistinctKeysCoexist(t *testing.T) {
	zone := zoneSpread(1, corev1.DoNotSchedule)
	host := corev1.TopologySpreadConstraint{
		MaxSkew: 1, TopologyKey: corev1.LabelHostname,
		WhenUnsatisfiable: corev1.DoNotSchedule, LabelSelector: appSelector(),
	}

	got := SpreadConstraintIntents(tscPod(zone, host))
	if len(got) != 2 {
		t.Fatalf("got %d intents, want 2", len(got))
	}
	// A constraint on an axis the source does not count is still returned:
	// filtering to the configured keys is the evaluator's job, and inference
	// reporting less than the pod asked for would be a silent loss.
	resolved := leeway.ResolveIntents(got)
	if len(resolved) != 2 {
		t.Errorf("resolved %d keys, want 2: %v", len(resolved), leeway.SortedKeys(resolved))
	}
}
