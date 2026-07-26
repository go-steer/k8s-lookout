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

package capacity

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

func unschedulablePod(uid, name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "shop", Name: name, UID: types.UID(uid),
			CreationTimestamp: metav1.Time{Time: since},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.PodScheduled,
				Status:             corev1.ConditionFalse,
				Reason:             corev1.PodReasonUnschedulable,
				Message:            "0/3 nodes are available: 3 Insufficient cpu.",
				LastTransitionTime: metav1.Time{Time: since},
			}},
		},
	}
}

// TestPendingAged_Timing walks the countdown: silent before
// PendingAge, warning at PendingAge, critical at CriticalPendingAge,
// one fire per level.
func TestPendingAged_Timing(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})
	var got []engine.Signal
	s.emit = func(sig engine.Signal) { got = append(got, sig) }

	t0 := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	s.trackPod(unschedulablePod("uid-1", "web-1", t0))

	s.sweepPending(t0.Add(4 * time.Minute))
	if len(got) != 0 {
		t.Fatalf("fired before --pending-age: %+v", got)
	}

	s.sweepPending(t0.Add(5 * time.Minute))
	if len(got) != 1 {
		t.Fatalf("at PendingAge fired %d, want 1", len(got))
	}
	if got[0].Kind != KindPendingAged || got[0].Severity != engine.SeverityWarning {
		t.Errorf("crossing = (%s, %s), want (capacity.pending-aged, warning)", got[0].Kind, got[0].Severity)
	}
	if got[0].Key.UID != "uid-1" || got[0].Key.Reason != "pending-aged" {
		t.Errorf("key = %+v", got[0].Key)
	}
	if !strings.Contains(got[0].Message, "Insufficient cpu") {
		t.Errorf("message %q must carry the scheduler's explanation", got[0].Message)
	}

	// Latch: no re-fire at the same level.
	s.sweepPending(t0.Add(10 * time.Minute))
	if len(got) != 1 {
		t.Fatalf("warning re-fired: %d signals", len(got))
	}

	// Escalation at the design-fixed 15m.
	s.sweepPending(t0.Add(15 * time.Minute))
	if len(got) != 2 || got[1].Severity != engine.SeverityCritical {
		t.Fatalf("critical escalation: got %d signals, last %+v", len(got), got[len(got)-1])
	}
	s.sweepPending(t0.Add(30 * time.Minute))
	if len(got) != 2 {
		t.Fatalf("critical re-fired: %d signals", len(got))
	}
}

// TestPendingAged_ScheduledOrDeletedPodRetires: a pod that schedules
// (or disappears) before the threshold never fires.
func TestPendingAged_ScheduledOrDeletedPodRetires(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})
	var got []engine.Signal
	s.emit = func(sig engine.Signal) { got = append(got, sig) }

	t0 := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	scheduled := unschedulablePod("uid-2", "web-2", t0)
	s.trackPod(scheduled)
	// The pod schedules: condition flips, phase moves on.
	scheduled.Status.Phase = corev1.PodRunning
	scheduled.Status.Conditions[0].Status = corev1.ConditionTrue
	scheduled.Status.Conditions[0].Reason = ""
	s.trackPod(scheduled)

	deleted := unschedulablePod("uid-3", "web-3", t0)
	s.trackPod(deleted)
	s.forgetPod(deleted)

	s.sweepPending(t0.Add(time.Hour))
	if len(got) != 0 {
		t.Errorf("retired pods fired: %+v", got)
	}
}

// TestPendingAged_MerelyPendingIsNotCapacity: Pending without the
// scheduler's Unschedulable verdict (image pull, volume binding) is
// not a capacity signal.
func TestPendingAged_MerelyPendingIsNotCapacity(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})
	var got []engine.Signal
	s.emit = func(sig engine.Signal) { got = append(got, sig) }

	t0 := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	p := unschedulablePod("uid-4", "web-4", t0)
	p.Status.Conditions = nil // Pending, but no Unschedulable condition
	s.trackPod(p)
	s.sweepPending(t0.Add(time.Hour))
	if len(got) != 0 {
		t.Errorf("merely-Pending pod fired: %+v", got)
	}
}

// TestPendingAged_HigherPendingAgeMovesCritical: --pending-age above
// the design-fixed 15m moves the critical threshold with it (warning
// can never outrank critical).
func TestPendingAged_HigherPendingAgeMovesCritical(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{PendingAge: 30 * time.Minute})
	var got []engine.Signal
	s.emit = func(sig engine.Signal) { got = append(got, sig) }

	t0 := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	s.trackPod(unschedulablePod("uid-5", "web-5", t0))
	s.sweepPending(t0.Add(16 * time.Minute))
	if len(got) != 0 {
		t.Fatalf("fired before the raised threshold: %+v", got)
	}
	s.sweepPending(t0.Add(30 * time.Minute))
	if len(got) != 1 || got[0].Severity != engine.SeverityCritical {
		t.Fatalf("at raised threshold: %+v, want one critical (thresholds coincide)", got)
	}
}
