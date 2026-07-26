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
	"reflect"
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

// TestParseNoScaleUpReasons covers the real NotTriggerScaleUp message
// shapes upstream cluster-autoscaler emits (core/scale_up.go event
// text), including multi-nodegroup rejections and the taint form
// whose reason itself contains a comma.
func TestParseNoScaleUpReasons(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		msg  string
		want []rejection
	}{
		{
			name: "multi-nodegroup rejection list",
			msg:  "pod didn't trigger scale-up: 2 max node group size reached, 1 not ready for scale-up",
			want: []rejection{
				{Count: 2, Reason: "max node group size reached"},
				{Count: 1, Reason: "not ready for scale-up"},
			},
		},
		{
			name: "parenthetical wouldn't-fit prefix with predicate reasons",
			msg:  "pod didn't trigger scale-up (it wouldn't fit if a new node is added): 3 Insufficient cpu, 1 Insufficient memory",
			want: []rejection{
				{Count: 3, Reason: "Insufficient cpu"},
				{Count: 1, Reason: "Insufficient memory"},
			},
		},
		{
			name: "taint reason containing a comma",
			msg:  "pod didn't trigger scale-up: 1 node(s) had taint {dedicated: gpu}, that the pod didn't tolerate, 2 max node group size reached",
			want: []rejection{
				{Count: 1, Reason: "node(s) had taint {dedicated: gpu}, that the pod didn't tolerate"},
				{Count: 2, Reason: "max node group size reached"},
			},
		},
		{
			name: "affinity reason with slashes",
			msg:  "pod didn't trigger scale-up: 3 node(s) didn't match Pod's node affinity/selector, 1 in backoff after failed scale-up",
			want: []rejection{
				{Count: 3, Reason: "node(s) didn't match Pod's node affinity/selector"},
				{Count: 1, Reason: "in backoff after failed scale-up"},
			},
		},
		{
			name: "old-CA bare form (no reason list) keeps raw",
			msg:  "pod didn't trigger scale-up (it wouldn't fit if a new node is added)",
			want: nil,
		},
		{
			name: "unrelated message",
			msg:  "0/3 nodes are available: 3 Insufficient cpu",
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := parseNoScaleUpReasons(tc.msg)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseNoScaleUpReasons(%q) =\n  %+v\nwant\n  %+v", tc.msg, got, tc.want)
			}
		})
	}
}

func caEvent(reason, msg string) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "web-1.evt"},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod", Namespace: "shop", Name: "web-1", UID: types.UID("uid-web-1"),
		},
		Reason:        reason,
		Message:       msg,
		Count:         1,
		LastTimestamp: metav1.Time{Time: time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)},
	}
}

// TestEventSignal_KindsAndSeverities pins the CA-event → kind/severity
// mapping and the parsed-rejection evidence in the message.
func TestEventSignal_KindsAndSeverities(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})

	sig := s.eventSignal(KindPending, caEvent("NotTriggerScaleUp",
		"pod didn't trigger scale-up: 2 max node group size reached, 1 not ready for scale-up"))
	if sig.Kind != "capacity.pending" || sig.Severity != engine.SeverityWarning {
		t.Errorf("NotTriggerScaleUp → (%s, %s), want (capacity.pending, warning)", sig.Kind, sig.Severity)
	}
	if sig.Key.UID != "uid-web-1" || sig.Key.Reason != "pending" {
		t.Errorf("key = %+v, want pod UID + reason pending", sig.Key)
	}
	for _, want := range []string{`2× "max node group size reached"`, `1× "not ready for scale-up"`, "raw:"} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("message %q missing %q", sig.Message, want)
		}
	}

	up := s.eventSignal(KindScaleUp, caEvent("TriggeredScaleUp", "pod triggered scale-up: [{mig-a 1->2 (max: 3)}]"))
	if up.Kind != "capacity.scaleup" || up.Severity != engine.SeverityInfo {
		t.Errorf("TriggeredScaleUp → (%s, %s), want (capacity.scaleup, info)", up.Kind, up.Severity)
	}

	down := s.eventSignal(KindScaleDown, caEvent("ScaleDown", "node removed by cluster autoscaler"))
	if down.Kind != "capacity.scaledown" || down.Severity != engine.SeverityInfo {
		t.Errorf("ScaleDown → (%s, %s), want (capacity.scaledown, info)", down.Kind, down.Severity)
	}

	failed := s.eventSignal(KindScaleDown, caEvent("ScaleDownFailed", "failed to delete node"))
	if failed.Severity != engine.SeverityWarning {
		t.Errorf("ScaleDownFailed severity = %s, want warning", failed.Severity)
	}
	if !strings.HasPrefix(failed.Message, "ScaleDownFailed: ") {
		t.Errorf("ScaleDownFailed message %q must carry the raw reason", failed.Message)
	}
}

// TestOnEvent_AllowListAndArming: non-CA reasons never emit; CA
// reasons emit only once armed (pre-sync history is dropped — the
// package's arm-after-sync posture).
func TestOnEvent_AllowListAndArming(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})
	var got []engine.Signal
	s.emit = func(sig engine.Signal) { got = append(got, sig) }

	s.onEvent(caEvent("NotTriggerScaleUp", "pod didn't trigger scale-up: 1 max node group size reached"))
	if len(got) != 0 {
		t.Fatalf("unarmed source emitted %d signals, want 0", len(got))
	}
	s.arm()
	s.onEvent(caEvent("CrashLoopBackOff", "back-off restarting container")) // k8s-events' turf
	s.onEvent(caEvent("FailedScheduling", "0/3 nodes are available"))       // ditto
	if len(got) != 0 {
		t.Fatalf("non-CA reasons emitted %d signals, want 0", len(got))
	}
	s.onEvent(caEvent("NotTriggerScaleUp", "pod didn't trigger scale-up: 1 max node group size reached"))
	if len(got) != 1 || got[0].Kind != KindPending {
		t.Fatalf("armed CA event → %d signals (%v), want 1 capacity.pending", len(got), got)
	}
}
