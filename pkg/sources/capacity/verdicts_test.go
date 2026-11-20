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
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

func TestVerdicts_PreArmRecordingAndFailedScaleUp(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})
	s.now = func() time.Time { return now }
	var emitted []engine.Signal
	s.emit = func(sig engine.Signal) { emitted = append(emitted, sig) }

	ev := caEvent(leeway.ReasonTriggeredScaleUp, "pod triggered scale-up")
	ev.LastTimestamp = metav1.Time{Time: now.Add(-1 * time.Minute)}
	s.onEvent(ev)
	if len(emitted) != 0 {
		t.Fatalf("pre-arm event emitted %d signals, want 0", len(emitted))
	}
	mark := s.LookupScaleUpVerdict("shop", "uid-web-1")
	if mark.Verdict != engine.ScaleUpProvisioning || !mark.At.Equal(now.Add(-1*time.Minute)) {
		t.Fatalf("pre-arm verdict = %+v, want provisioning at %v", mark, now.Add(-1*time.Minute))
	}

	failed := caEvent(leeway.ReasonFailedScaleUp, "scale-up failed: GCE_STOCKOUT")
	failed.LastTimestamp = metav1.Time{Time: now}
	s.onEvent(failed)
	mark = s.LookupScaleUpVerdict("shop", "uid-web-1")
	if mark.Verdict != engine.ScaleUpDeclined || !mark.At.Equal(now) {
		t.Fatalf("after FailedScaleUp verdict = %+v, want declined at %v", mark, now)
	}
	snap := s.ScaleUpVerdicts()
	if len(snap) != 1 || snap["shop/uid-web-1"].Verdict != engine.ScaleUpDeclined {
		t.Fatalf("ScaleUpVerdicts snapshot = %+v", snap)
	}
}

func TestVerdicts_SameSecondTieTriggerWins(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 10, 0, 5, 0, time.UTC)

	s1 := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})
	s1.now = func() time.Time { return now }
	trig := caEvent(leeway.ReasonTriggeredScaleUp, "scale-up")
	trig.LastTimestamp = metav1.Time{Time: now}
	decl := caEvent(leeway.ReasonNotTriggerScaleUp, "declined")
	decl.LastTimestamp = metav1.Time{Time: now}
	s1.onEvent(trig)
	s1.onEvent(decl)
	if got := s1.LookupScaleUpVerdict("shop", "uid-web-1").Verdict; got != engine.ScaleUpProvisioning {
		t.Errorf("trigger-then-decline same-second tie = %v, want provisioning", got)
	}

	s2 := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})
	s2.now = func() time.Time { return now }
	s2.onEvent(decl)
	s2.onEvent(trig)
	if got := s2.LookupScaleUpVerdict("shop", "uid-web-1").Verdict; got != engine.ScaleUpProvisioning {
		t.Errorf("decline-then-trigger same-second tie = %v, want provisioning", got)
	}
}

func TestVerdicts_ReporterFilterAndClamping(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})
	s.now = func() time.Time { return now }
	var logs []string
	s.logf = func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) }
	hookCalls := 0
	s.WithRejectedVerdictHook(func() { hookCalls++ })

	forged := caEvent(leeway.ReasonTriggeredScaleUp, "forged scale-up")
	forged.Source.Component = "fake-controller"
	forged.LastTimestamp = metav1.Time{Time: now}
	// Repeat 3 times within 15m: counter increments each time, but log is
	// rate-limited to 1 line per (namespace, reporter, reason, cause) per 15m.
	s.onEvent(forged)
	s.onEvent(forged)
	s.onEvent(forged)

	if got := s.LookupScaleUpVerdict("shop", "uid-web-1").Verdict; got != engine.ScaleUpUnheard {
		t.Fatalf("forged verdict recorded as %v, want unheard", got)
	}
	if s.RejectedVerdicts() != 3 || hookCalls != 3 {
		t.Fatalf("rejected = %d, hookCalls = %d, want 3/3", s.RejectedVerdicts(), hookCalls)
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "fake-controller") {
		t.Fatalf("rejected verdict log = %v, want 1 rate-limited unexpected-reporter line", logs)
	}

	// Cross-namespace spoofing via events.k8s.io/v1 (eventTime + ReportingController):
	// an Event living in namespace "tenant" claiming a Pod in "kube-system"
	// MUST be rejected (never stored under kube-system or evicting its marks).
	crossNS := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "spoof.evt"},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod", Namespace: "kube-system", Name: "victim", UID: types.UID("uid-victim"),
		},
		ReportingController: ScaleUpReporter,
		Reason:              leeway.ReasonTriggeredScaleUp,
		EventTime:           metav1.MicroTime{Time: now},
	}
	s.onEvent(crossNS)
	if got := s.LookupScaleUpVerdict("kube-system", "uid-victim").Verdict; got != engine.ScaleUpUnheard {
		t.Fatalf("cross-namespace spoof stored under kube-system: %v, want unheard", got)
	}
	if got := len(s.ScaleUpVerdicts()); got != 0 {
		t.Fatalf("ScaleUpVerdicts len = %d after rejected events, want 0", got)
	}
	if s.RejectedVerdicts() != 4 || hookCalls != 4 {
		t.Fatalf("after cross-ns spoof rejected = %d, hookCalls = %d, want 4/4", s.RejectedVerdicts(), hookCalls)
	}
	if len(logs) != 2 || !strings.Contains(logs[1], "cross-namespace") {
		t.Fatalf("cross-namespace spoof log = %v, want 2nd line naming cross-namespace", logs)
	}

	rcEv := caEvent(leeway.ReasonTriggeredScaleUp, "real scale-up via events.k8s.io/v1")
	rcEv.Source.Component = ""
	rcEv.ReportingController = ScaleUpReporter
	rcEv.LastTimestamp = metav1.Time{}
	rcEv.Series = &corev1.EventSeries{
		Count:            2,
		LastObservedTime: metav1.MicroTime{Time: now.Add(10 * time.Minute)},
	}
	s.onEvent(rcEv)
	mark := s.LookupScaleUpVerdict("shop", "uid-web-1")
	if mark.Verdict != engine.ScaleUpProvisioning || !mark.At.Equal(now) {
		t.Fatalf("ReportingController + future clamp = %+v, want provisioning clamped to %v", mark, now)
	}
}

func TestVerdicts_NamespaceIsolationFairEvictionAndTTL(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	memo := newVerdictMemo(30*time.Minute, 4, func() time.Time { return now })

	memo.Record("kube-system", "uid-shared", leeway.ReasonTriggeredScaleUp, now)
	if got := memo.Lookup("tenant", "uid-shared").Verdict; got != engine.ScaleUpUnheard {
		t.Fatalf("cross-namespace lookup = %v, want unheard", got)
	}

	memo.Record("noisy", "u1", leeway.ReasonTriggeredScaleUp, now.Add(1*time.Second))
	memo.Record("noisy", "u2", leeway.ReasonTriggeredScaleUp, now.Add(2*time.Second))
	memo.Record("noisy", "u3", leeway.ReasonTriggeredScaleUp, now.Add(3*time.Second))

	memo.Record("noisy", "u4", leeway.ReasonTriggeredScaleUp, now.Add(4*time.Second))
	if got := memo.Lookup("kube-system", "uid-shared").Verdict; got != engine.ScaleUpProvisioning {
		t.Fatalf("fair eviction dropped kube-system mark: got %v", got)
	}
	if got := memo.Lookup("noisy", "u1").Verdict; got != engine.ScaleUpUnheard {
		t.Fatalf("fair eviction kept noisy/u1: got %v", got)
	}

	now = now.Add(31 * time.Minute)
	if got := memo.Lookup("kube-system", "uid-shared").Verdict; got != engine.ScaleUpUnheard {
		t.Fatalf("expired mark returned %v, want unheard", got)
	}
}

func TestLookupPodSchedulingState(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)

	pods := []corev1.Pod{
		*unschedulablePod("uid-unsched", "unsched", now),
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "sched-err", UID: types.UID("uid-sched-err")},
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{{
					Type:   corev1.PodScheduled,
					Status: corev1.ConditionFalse,
					Reason: corev1.PodReasonSchedulerError,
				}},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "cache-lag", UID: types.UID("uid-lag")},
			Status:     corev1.PodStatus{Phase: corev1.PodPending},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "bound-pod", UID: types.UID("uid-bound")},
			Spec:       corev1.PodSpec{NodeName: "node-1"},
			Status:     corev1.PodStatus{Phase: corev1.PodPending},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "running-pod", UID: types.UID("uid-run")},
			Spec:       corev1.PodSpec{NodeName: "node-1"},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "scheduled-true", UID: types.UID("uid-true")},
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{{
					Type:   corev1.PodScheduled,
					Status: corev1.ConditionTrue,
				}},
			},
		},
	}

	client := fake.NewSimpleClientset()
	for i := range pods {
		if _, err := client.CoreV1().Pods("shop").Create(context.Background(), &pods[i], metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	factory := informers.NewSharedInformerFactory(client, 0)
	podInf := factory.Core().V1().Pods().Informer()
	podLister := factory.Core().V1().Pods().Lister()

	s := New(client, cloud.NoProvider, Config{})
	s.mu.Lock()
	s.podLister = podLister
	s.mu.Unlock()

	if got := s.LookupPodSchedulingState("shop", "deleted-pod", "uid-gone"); got != engine.PodSchedulingUnknown {
		t.Fatalf("unsynced lookup = %v, want PodSchedulingUnknown", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), podInf.HasSynced) {
		t.Fatal("pod informer failed to sync")
	}
	// Pod informer synced, but capacity source is NOT armed yet (e.g. node or
	// event informer still syncing, so sweepPending is not running): MUST still
	// return PodSchedulingUnknown so the gate fails open.
	if got := s.LookupPodSchedulingState("shop", "unsched", "uid-unsched"); got != engine.PodSchedulingUnknown {
		t.Fatalf("pod-synced but unarmed lookup = %v, want PodSchedulingUnknown", got)
	}
	s.arm()

	cases := []struct {
		name string
		pod  string
		uid  string
		want engine.PodSchedulingState
	}{
		{"unschedulable", "unsched", "uid-unsched", engine.PodSchedulingUnschedulable},
		{"scheduler error fails open", "sched-err", "uid-sched-err", engine.PodSchedulingOtherFailure},
		{"pending cache lag treated as unschedulable", "cache-lag", "uid-lag", engine.PodSchedulingUnschedulable},
		{"bound pod", "bound-pod", "uid-bound", engine.PodSchedulingNotPending},
		{"running pod", "running-pod", "uid-run", engine.PodSchedulingNotPending},
		{"PodScheduled=True", "scheduled-true", "uid-true", engine.PodSchedulingNotPending},
		{"absent/deleted pod", "deleted-pod", "uid-gone", engine.PodSchedulingNotPending},
		{"UID mismatch (recreated pod)", "unsched", "uid-old", engine.PodSchedulingNotPending},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := s.LookupPodSchedulingState("shop", tc.pod, tc.uid); got != tc.want {
				t.Errorf("LookupPodSchedulingState(%q, %q) = %v, want %v", tc.pod, tc.uid, got, tc.want)
			}
		})
	}
}

func TestRun_InitialListRecordsVerdictBeforeArming(t *testing.T) {
	t.Parallel()
	trig := caEvent(leeway.ReasonTriggeredScaleUp, "pod triggered scale-up")
	trig.Name = "init-scaleup.evt"
	trig.LastTimestamp = metav1.Time{Time: time.Now().Add(-30 * time.Second)}
	client := fake.NewSimpleClientset(trig)

	s := New(client, cloud.NoProvider, Config{PollInterval: time.Hour})
	s.logf = func(string, ...any) {}
	var mu sync.Mutex
	var emitted []engine.Signal

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.Run(ctx, func(sig engine.Signal) {
			mu.Lock()
			emitted = append(emitted, sig)
			mu.Unlock()
		})
	}()

	waitFor(t, func() bool { return s.HasSynced() })
	mark := s.LookupScaleUpVerdict("shop", "uid-web-1")
	if mark.Verdict != engine.ScaleUpProvisioning {
		t.Fatalf("initial-LIST TriggeredScaleUp verdict = %+v, want provisioning", mark)
	}
	mu.Lock()
	nEmitted := len(emitted)
	mu.Unlock()
	if nEmitted != 0 {
		t.Fatalf("initial-LIST event emitted %d signals, want 0", nEmitted)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

func TestRun_UnchangedRVRedeliverySkipped(t *testing.T) {
	t.Parallel()
	forged := caEvent(leeway.ReasonTriggeredScaleUp, "forged scale-up")
	forged.Name = "forged-rv.evt"
	forged.ResourceVersion = "42"
	forged.Source.Component = "fake-controller"
	client := fake.NewSimpleClientset(forged)

	s := New(client, cloud.NoProvider, Config{PollInterval: time.Hour})
	s.logf = func(string, ...any) {}

	// Use a short resync period so the informer re-delivers UpdateFunc with
	// the exact same ResourceVersion ("42"); unchanged-RV filtering must
	// keep RejectedVerdicts() == 1 (only the initial Add).
	factory := informers.NewSharedInformerFactory(client, 50*time.Millisecond)
	s.WithFactory(factory)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, func(engine.Signal) {}) }()

	waitFor(t, func() bool { return s.HasSynced() })
	time.Sleep(180 * time.Millisecond)

	if got := s.RejectedVerdicts(); got != 1 {
		t.Fatalf("RejectedVerdicts after resync redeliveries = %d, want 1", got)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run error: %v", err)
	}
}
