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

import "time"

// The cluster autoscaler's three per-pod verdicts, as Event reasons on the Pod.
// They are the only autoscaler output the wedged rule reads (§7.7.4, amended
// 2026-10-06, #532).
const (
	// ReasonTriggeredScaleUp is the autoscaler starting a scale-up that will
	// fit the pod: a node is being provisioned for it.
	ReasonTriggeredScaleUp = "TriggeredScaleUp"
	// ReasonNotTriggerScaleUp is the autoscaler finding no node group it may
	// grow that would fit the pod.
	ReasonNotTriggerScaleUp = "NotTriggerScaleUp"
	// ReasonFailedScaleUp is a scale-up that was attempted and failed — a
	// stockout, a quota, a provisioning timeout.
	ReasonFailedScaleUp = "FailedScaleUp"
)

// IsScaleUpReason reports whether an Event reason is one of the three verdicts.
func IsScaleUpReason(reason string) bool {
	switch reason {
	case ReasonTriggeredScaleUp, ReasonNotTriggerScaleUp, ReasonFailedScaleUp:
		return true
	default:
		return false
	}
}

// ScaleUpVerdict is the autoscaler's latest word on one Pending pod.
type ScaleUpVerdict uint8

const (
	// ScaleUpUnheard is no verdict since the pod was created. Either the
	// autoscaler has not reached the pod yet — it runs every ten seconds or
	// so — or this process cannot see Events at all. The wedged rule treats it
	// as no evidence either way, which means it does NOT exonerate the pod.
	ScaleUpUnheard ScaleUpVerdict = iota
	// ScaleUpProvisioning is TriggeredScaleUp with nothing later.
	ScaleUpProvisioning
	// ScaleUpDeclined is NotTriggerScaleUp or FailedScaleUp with nothing later.
	ScaleUpDeclined
)

// String renders a verdict for prose and tests.
func (v ScaleUpVerdict) String() string {
	switch v {
	case ScaleUpProvisioning:
		return "provisioning"
	case ScaleUpDeclined:
		return "declined"
	default:
		return "unheard"
	}
}

// AutoscalerEvent is one autoscaler verdict Event about one pod, reduced to the
// three facts the fold needs. The source gathers them; this package never sees
// an Event object.
type AutoscalerEvent struct {
	Reason string
	// At is the latest time the Event was observed — its last timestamp, not
	// its first, so a NotTriggerScaleUp the autoscaler keeps repeating stays
	// as recent as the autoscaler's latest opinion.
	At time.Time
	// PodUID is the involved object's UID, empty if the Event did not carry
	// one.
	PodUID string
}

// LatestScaleUp folds a pod's autoscaler Events into one verdict.
//
// The most recent verdict wins, and ordering is by the Event's own timestamp,
// not by arrival: the 2026-10-02 drill saw a NotTriggerScaleUp land on the
// fallback pod after a TriggeredScaleUp, and two informers deliver in no
// particular order relative to each other. An Event is ignored if it is not
// one of the three verdicts, if its UID names a different pod (a name reused
// after a delete), or if it predates the pod's creation (the same reuse, with
// no UID to tell). A tie between a trigger and a decline goes to the trigger:
// the autoscaler gives one verdict per pod per loop, so a same-second tie is
// two loops colliding, and on a Tier A rule the conservative reading is the
// one that fires less.
func LatestScaleUp(events []AutoscalerEvent, podUID string, podCreated time.Time) ScaleUpVerdict {
	verdict := ScaleUpUnheard
	var latest time.Time
	for _, ev := range events {
		var v ScaleUpVerdict
		switch ev.Reason {
		case ReasonTriggeredScaleUp:
			v = ScaleUpProvisioning
		case ReasonNotTriggerScaleUp, ReasonFailedScaleUp:
			v = ScaleUpDeclined
		default:
			continue
		}
		if podUID != "" && ev.PodUID != "" && ev.PodUID != podUID {
			continue
		}
		if !podCreated.IsZero() && ev.At.Before(podCreated) {
			continue
		}
		switch {
		case verdict == ScaleUpUnheard, ev.At.After(latest):
			verdict, latest = v, ev.At
		case ev.At.Equal(latest) && v == ScaleUpProvisioning:
			verdict = v
		}
	}
	return verdict
}
