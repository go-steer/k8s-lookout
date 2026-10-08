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
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// The #532 tests run the source over a fake clientset carrying the Pod and the
// autoscaler's Events, through the real informers, and then drive §8.2's
// machine across the dwell by hand. The class is the drill's: DoNotScaleUp,
// two priorities.

const lockedSpec = `{
  "priorities": [{"machineFamily": "n4"}, {"machineFamily": "n2"}],
  "whenUnsatisfiable": "DoNotScaleUp"
}`

// pendingClassPod is wedgedPod with the identity a real pod carries, which the
// Event fold scopes verdicts by.
func pendingClassPod(name, class string) *corev1.Pod {
	p := wedgedPod(name, class)
	p.UID = types.UID("uid-" + name)
	p.CreationTimestamp = metav1.NewTime(t0)
	return p
}

// autoscalerEvent is a cluster-autoscaler verdict about a pod, in the core/v1
// shape its recorder writes: involvedObject the Pod, the reason, and a last
// timestamp.
func autoscalerEvent(name string, p *corev1.Pod, reason string, after time.Duration) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: name},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod", Namespace: p.Namespace, Name: p.Name, UID: p.UID,
		},
		Reason:         reason,
		Source:         corev1.EventSource{Component: "cluster-autoscaler"},
		FirstTimestamp: metav1.NewTime(t0.Add(after)),
		LastTimestamp:  metav1.NewTime(t0.Add(after)),
	}
}

// testClock is a clock a test can move while Run is live. Run reads s.now
// without the lock (the flush ticker, the post-barrier reconcile), so the
// function has to be installed before Run starts and only the time it returns
// may change afterwards. Unset, it is the wall clock.
type testClock struct{ at atomic.Pointer[time.Time] }

func (c *testClock) now() time.Time {
	if at := c.at.Load(); at != nil {
		return *at
	}
	return time.Now()
}

func (c *testClock) set(at time.Time) { c.at.Store(&at) }

// withTestClock installs a testClock on a source that is not running yet.
func withTestClock(s *Source) *testClock {
	c := &testClock{}
	s.now = c.now
	return c
}

// runWithEvents starts a source over the fakes and waits until the scaleUps
// map holds want verdicts for the pod — the Event informer is outside the sync
// barrier, so the barrier alone does not promise they have arrived.
func runWithEvents(t *testing.T, p *corev1.Pod, events ...*corev1.Event) (*Source, *testClock) {
	t.Helper()
	objs := []runtime.Object{p}
	for _, ev := range events {
		objs = append(objs, ev)
	}
	s := newRunnableSource(t, servedClient(objs...), dynClient(classObject(t, "locked", lockedSpec)))
	clock := withTestClock(s)
	runSource(t, s)
	awaitVerdicts(t, s, p, len(events))
	awaitPending(t, s, "locked", 1)
	return s, clock
}

func awaitVerdicts(t *testing.T, s *Source, p *corev1.Pod, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		got := len(s.scaleUps[podRef{p.Namespace, p.Name}])
		s.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d autoscaler verdict(s) recorded for %s, want %d", got, p.Name, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func awaitPending(t *testing.T, s *Source, class string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		got := len(s.pendingByClass[class])
		_, decoded := s.classes[class]
		s.mu.Unlock()
		if got == want && decoded {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d Pending pod(s) on %s (class decoded: %v), want %d", got, class, decoded, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// passAcrossDwell runs one alert pass at T0+16s (the drill's first judge pass)
// and one after the dwell, returning the wedged signals emitted and the wedged
// verdict's reason at the second pass.
func passAcrossDwell(t *testing.T, s *Source, clock *testClock) ([]sources.Signal, string) {
	t.Helper()
	var wedged []sources.Signal
	s.mu.Lock()
	s.emit = func(sig sources.Signal) {
		if sig.Kind == leeway.KindRankWedged {
			wedged = append(wedged, sig)
		}
	}
	s.mu.Unlock()

	clock.set(t0.Add(16 * time.Second))
	s.runAlertPass(context.Background())
	now := t0.Add(16*time.Second + leeway.DefaultDwell().For + time.Minute)
	clock.set(now)
	s.runAlertPass(context.Background())

	var reason string
	for _, j := range s.judgeAll(now) {
		for _, v := range j.verdicts {
			if v.Rule == leeway.RankRuleWedged {
				reason = v.Reason
			}
		}
	}
	return wedged, reason
}

// TestWedged_TheAutoscalerDeclined is the drill's Part A: NotTriggerScaleUp at
// 7s, so the pod is wedged, and the finding says the autoscaler declined
// rather than asserting that no priority can be satisfied.
func TestWedged_TheAutoscalerDeclined(t *testing.T) {
	p := pendingClassPod("stuck", "locked")
	s, clock := runWithEvents(t, p, autoscalerEvent("stuck.1", p, leeway.ReasonNotTriggerScaleUp, 7*time.Second))

	wedged, reason := passAcrossDwell(t, s, clock)
	if len(wedged) != 1 {
		t.Fatalf("%d rank_wedged signal(s), want one (reason: %s)", len(wedged), reason)
	}
	if !strings.Contains(wedged[0].Message, "the autoscaler declined to scale up for 1") {
		t.Errorf("message %q does not say the autoscaler declined", wedged[0].Message)
	}
	if strings.Contains(wedged[0].Message, "no priority can be satisfied") {
		t.Errorf("message %q asserts something no Event said", wedged[0].Message)
	}
}

// TestWedged_AProvisioningFallbackIsNotWedged is #532 itself, the drill's Part
// B: TriggeredScaleUp at 7s and the pod still Pending past the dwell — a slow
// shape, or a dwell shorter than the provision. Not wedged.
func TestWedged_AProvisioningFallbackIsNotWedged(t *testing.T) {
	p := pendingClassPod("fallback", "locked")
	s, clock := runWithEvents(t, p, autoscalerEvent("fallback.1", p, leeway.ReasonTriggeredScaleUp, 7*time.Second))

	wedged, reason := passAcrossDwell(t, s, clock)
	if len(wedged) != 0 {
		t.Fatalf("rank_wedged fired for a pod the autoscaler is provisioning a node for: %s", wedged[0].Message)
	}
	if !strings.Contains(reason, "TriggeredScaleUp") {
		t.Errorf("verdict reason %q does not say why the Pending pod was excused", reason)
	}
}

// TestWedged_ProvisioningThenFailedScaleUp: the trigger excuses the pod only
// until the autoscaler says the scale-up failed — a stockout, a quota, a
// provisioning timeout — after which it is wedged.
func TestWedged_ProvisioningThenFailedScaleUp(t *testing.T) {
	p := pendingClassPod("stockout", "locked")
	client := servedClient(p, autoscalerEvent("stockout.1", p, leeway.ReasonTriggeredScaleUp, 7*time.Second))
	s := newRunnableSource(t, client, dynClient(classObject(t, "locked", lockedSpec)))
	clock := withTestClock(s)
	runSource(t, s)
	awaitVerdicts(t, s, p, 1)
	awaitPending(t, s, "locked", 1)

	s.mu.Lock()
	if prov, dec := s.scaleUpSplit("locked"); prov != 1 || dec != 0 {
		t.Errorf("before the failure: %d provisioning / %d declined, want 1/0", prov, dec)
	}
	s.mu.Unlock()

	failed := autoscalerEvent("stockout.2", p, leeway.ReasonFailedScaleUp, 15*time.Minute)
	if _, err := client.CoreV1().Events(p.Namespace).Create(context.Background(), failed, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create FailedScaleUp: %v", err)
	}
	awaitVerdicts(t, s, p, 2)

	wedged, reason := passAcrossDwell(t, s, clock)
	if len(wedged) != 1 {
		t.Fatalf("%d rank_wedged signal(s) after a FailedScaleUp, want one (reason: %s)", len(wedged), reason)
	}
}

// TestWedged_NoAutoscalerEventsStillFires pins the no-Events decision. A pod
// with no verdict at all is NOT excused: the commonest reason for having none
// is a process that cannot read Events, and that must not switch a Tier A
// rule off. Where Events are readable the autoscaler answers within a loop, far
// inside the dwell. The message says no verdict was observed, and nothing more.
func TestWedged_NoAutoscalerEventsStillFires(t *testing.T) {
	p := pendingClassPod("silent", "locked")
	s, clock := runWithEvents(t, p)

	wedged, reason := passAcrossDwell(t, s, clock)
	if len(wedged) != 1 {
		t.Fatalf("%d rank_wedged signal(s) with no autoscaler Events, want one (reason: %s)", len(wedged), reason)
	}
	msg := wedged[0].Message
	if !strings.Contains(msg, "1 have no autoscaler verdict observed") {
		t.Errorf("message %q does not say no verdict was observed", msg)
	}
	if strings.Contains(msg, "declined") || strings.Contains(msg, "no priority can be satisfied") {
		t.Errorf("message %q claims a cause nothing observed", msg)
	}
}

// TestUpsertEvent_KeepsOnlyVerdictsAboutPods and drops them on delete, which
// is what bounds the map.
func TestUpsertEvent_KeepsOnlyVerdictsAboutPods(t *testing.T) {
	s := newTestSource(t)
	p := pendingClassPod("x", "locked")

	s.UpsertEvent(autoscalerEvent("x.sched", p, "FailedScheduling", time.Second))
	node := autoscalerEvent("n.1", p, leeway.ReasonTriggeredScaleUp, time.Second)
	node.InvolvedObject.Kind = "Node"
	s.UpsertEvent(node)
	if len(s.scaleUps) != 0 {
		t.Fatalf("kept %d non-verdict Event(s)", len(s.scaleUps))
	}

	ev := autoscalerEvent("x.1", p, leeway.ReasonNotTriggerScaleUp, time.Second)
	s.UpsertEvent(ev)
	if len(s.scaleUps[podRef{"default", "x"}]) != 1 {
		t.Fatal("the verdict was not kept")
	}
	s.DeleteEvent(ev)
	if len(s.scaleUps) != 0 {
		t.Errorf("a deleted Event's verdict was kept: %v", s.scaleUps)
	}
}

// TestEventTime_TakesTheLatestObservation: a repeated NotTriggerScaleUp is
// one Event with a bumped last timestamp, and it is as recent as that.
func TestEventTime_TakesTheLatestObservation(t *testing.T) {
	p := pendingClassPod("x", "locked")
	ev := autoscalerEvent("x.1", p, leeway.ReasonNotTriggerScaleUp, time.Second)
	ev.LastTimestamp = metav1.NewTime(t0.Add(5 * time.Minute))
	if got := eventTime(ev); !got.Equal(t0.Add(5 * time.Minute)) {
		t.Errorf("eventTime = %s, want the last timestamp", got)
	}
	series := &corev1.Event{
		EventTime: metav1.NewMicroTime(t0),
		Series:    &corev1.EventSeries{LastObservedTime: metav1.NewMicroTime(t0.Add(time.Minute))},
	}
	if got := eventTime(series); !got.Equal(t0.Add(time.Minute)) {
		t.Errorf("eventTime = %s, want the series' last observation", got)
	}
}
