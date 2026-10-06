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
	"strings"
	"testing"
	"time"
)

// TestLatestScaleUp pins the fold the wedged rule's exoneration rests on
// (#532): the most recent verdict by the Event's own timestamp, scoped to this
// pod's life.
func TestLatestScaleUp(t *testing.T) {
	created := time.Date(2026, 10, 2, 15, 15, 22, 0, time.UTC)
	at := created.Add
	ev := func(reason string, d time.Duration) AutoscalerEvent {
		return AutoscalerEvent{Reason: reason, At: at(d), PodUID: "u1"}
	}
	tests := []struct {
		name   string
		events []AutoscalerEvent
		want   ScaleUpVerdict
	}{
		{"nothing", nil, ScaleUpUnheard},
		{"the drill's fallback pod", []AutoscalerEvent{ev(ReasonTriggeredScaleUp, 7*time.Second)}, ScaleUpProvisioning},
		{"the drill's wedged pod", []AutoscalerEvent{ev(ReasonNotTriggerScaleUp, 7*time.Second)}, ScaleUpDeclined},
		{"a trigger that then failed", []AutoscalerEvent{
			ev(ReasonTriggeredScaleUp, 7*time.Second), ev(ReasonFailedScaleUp, 15*time.Minute),
		}, ScaleUpDeclined},
		{"a decline then a retry that triggered", []AutoscalerEvent{
			ev(ReasonFailedScaleUp, time.Minute), ev(ReasonTriggeredScaleUp, 6*time.Minute),
		}, ScaleUpProvisioning},
		{"order is by timestamp, not by slice position", []AutoscalerEvent{
			ev(ReasonNotTriggerScaleUp, 3*time.Minute), ev(ReasonTriggeredScaleUp, 7*time.Second),
		}, ScaleUpDeclined},
		{"a same-second tie goes to the trigger", []AutoscalerEvent{
			ev(ReasonNotTriggerScaleUp, time.Minute), ev(ReasonTriggeredScaleUp, time.Minute),
		}, ScaleUpProvisioning},
		{"other reasons are not verdicts", []AutoscalerEvent{
			ev("FailedScheduling", time.Minute), ev("ScaleDown", 2*time.Minute),
		}, ScaleUpUnheard},
		{"an event about a previous pod of the same name, by UID", []AutoscalerEvent{
			{Reason: ReasonNotTriggerScaleUp, At: at(time.Minute), PodUID: "u0"},
		}, ScaleUpUnheard},
		{"an event from before the pod existed", []AutoscalerEvent{
			{Reason: ReasonNotTriggerScaleUp, At: at(-time.Minute)},
		}, ScaleUpUnheard},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := LatestScaleUp(tc.events, "u1", created); got != tc.want {
				t.Errorf("verdict %s, want %s", got, tc.want)
			}
		})
	}
}

func TestIsScaleUpReason(t *testing.T) {
	for _, r := range []string{ReasonTriggeredScaleUp, ReasonNotTriggerScaleUp, ReasonFailedScaleUp} {
		if !IsScaleUpReason(r) {
			t.Errorf("%s is a verdict", r)
		}
	}
	if IsScaleUpReason("FailedScheduling") {
		t.Error("FailedScheduling is the scheduler's, not the autoscaler's")
	}
}

// TestJudgeWedged_TheAutoscalerVerdictDecides is #532: a Pending pod on a
// DoNotScaleUp class is wedged unless the autoscaler said it is provisioning a
// node for it, and the reason says only what was observed.
func TestJudgeWedged_TheAutoscalerVerdictDecides(t *testing.T) {
	tests := []struct {
		name                            string
		pending, provisioning, declined int
		want                            bool
		say, never                      string
	}{
		{"declined", 1, 0, 1, true, "the autoscaler declined to scale up for 1", "no priority can be satisfied"},
		{"provisioning", 1, 1, 0, false, "TriggeredScaleUp", ""},
		{"no verdict observed fires, so a process without Events is not silenced", 1, 0, 0, true, "1 have no autoscaler verdict observed", "declined"},
		{"one provisioning does not hide one declined", 2, 1, 1, true, "1 are waiting on a triggered scale-up", ""},
		{"an over-count is clamped", 1, 3, 3, false, "1 pod(s) Pending", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := RankInput{
				Class:  judgeClass(t, s2ScoredSpec),
				Window: steadyWindow(),
				Conditions: RankConditions{
					PendingPods: tc.pending, PendingProvisioning: tc.provisioning, PendingDeclined: tc.declined,
					Lifetime: time.Hour,
				},
			}
			v := ruleOf(t, JudgeRank(in, DefaultRankThresholds()), RankRuleWedged)
			if v.Breached != tc.want {
				t.Errorf("breached=%v, want %v (%s)", v.Breached, tc.want, v.Reason)
			}
			if !strings.Contains(v.Reason, tc.say) {
				t.Errorf("reason %q does not say %q", v.Reason, tc.say)
			}
			if tc.never != "" && strings.Contains(v.Reason, tc.never) {
				t.Errorf("reason %q claims %q, which was not observed", v.Reason, tc.never)
			}
			if v.Observed.PendingProvisioning != tc.provisioning || v.Observed.PendingDeclined != tc.declined {
				t.Errorf("observed split %d/%d, want %d/%d", v.Observed.PendingProvisioning, v.Observed.PendingDeclined, tc.provisioning, tc.declined)
			}
		})
	}
}
