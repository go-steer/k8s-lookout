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

package watch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources/capacity"
)

type fakeClusterConfigProvider struct {
	cloud.Provider
	name string
	cfg  cloud.ClusterConfig
	err  error
}

func (p fakeClusterConfigProvider) Name() string {
	if p.name != "" {
		return p.name
	}
	return "gke"
}

func (p fakeClusterConfigProvider) ClusterConfig() (cloud.ClusterConfigAPI, bool) {
	return fakeClusterConfigAPI{cfg: p.cfg, err: p.err}, true
}

type fakeClusterConfigAPI struct {
	cfg cloud.ClusterConfig
	err error
}

func (a fakeClusterConfigAPI) Config(context.Context) (cloud.ClusterConfig, error) {
	return a.cfg, a.err
}

func (a fakeClusterConfigAPI) UpgradeTargets(context.Context, string) (cloud.UpgradeTargets, error) {
	return cloud.UpgradeTargets{}, nil
}

func TestFailedSchedulingFlags_DefaultsAndValidation(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate defaults: %v", err)
	}
	if f.failedSchedulingMinCount != 5 {
		t.Errorf("default failedscheduling-min-count = %d, want 5", f.failedSchedulingMinCount)
	}
	if f.failedSchedulingMinSpan != 2*time.Minute {
		t.Errorf("default failedscheduling-min-span = %v, want 2m", f.failedSchedulingMinSpan)
	}
	if f.scaleUpHold != 15*time.Minute {
		t.Errorf("default scaleup-hold = %v, want 15m", f.scaleUpHold)
	}
	if !f.autopilotScaleToZeroHold {
		t.Error("default autopilot-scale-to-zero-hold = false, want true")
	}

	bad := [][]string{
		{"--dry-run", "--failedscheduling-min-count=0"},
		{"--dry-run", "--failedscheduling-min-span=-1s"},
		{"--dry-run", "--scaleup-hold=-5m"},
	}
	for _, argv := range bad {
		fl, err := parseFlags(argv)
		if err != nil {
			t.Fatalf("parseFlags(%v): %v", argv, err)
		}
		if err := fl.validate(); err == nil {
			t.Errorf("validate(%v) = nil, want error", argv)
		}
	}
}

func TestConfigureFailedSchedulingGate_CapacityOffAndAllKnobsOff(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--dry-run"})
	if err != nil {
		t.Fatal(err)
	}
	filter := engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 0, 0))
	m := newMetrics()
	var logs []string
	logf := func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) }

	configureFailedSchedulingGate(context.Background(), f, nil, cloud.NoProvider, filter, m, logf)
	if len(logs) != 1 || !strings.Contains(logs[0], "DISABLED — capacity source is not enabled") {
		t.Fatalf("capacity-off logs = %v", logs)
	}
	now := time.Now()
	sig := engine.Signal{
		Kind:     engine.KindK8sEvent,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "u1", Reason: "FailedScheduling"},
			Namespace:    "default",
			KindOfObject: "Pod",
			Name:         "hog-1",
			Count:        1,
			FirstSeen:    now,
			LastSeen:     now,
		},
	}
	if ok, gate := filter.Decide(sig); !ok || gate != "" {
		t.Fatalf("capacity-off Decide = (%v, %q), want (true, \"\")", ok, gate)
	}

	fOff, err := parseFlags([]string{
		"--dry-run",
		"--failedscheduling-min-count=1",
		"--failedscheduling-min-span=0",
		"--scaleup-hold=0",
		"--autopilot-scale-to-zero-hold=false",
	})
	if err != nil {
		t.Fatal(err)
	}
	logs = nil
	capSrc := capacity.New(fake.NewSimpleClientset(), cloud.NoProvider, capacity.Config{})
	configureFailedSchedulingGate(context.Background(), fOff, capSrc, cloud.NoProvider, filter, m, logf)
	if len(logs) != 1 || !strings.Contains(logs[0], "disabled by flags") {
		t.Fatalf("all-knobs-off logs = %v", logs)
	}
	if ok, gate := filter.Decide(sig); !ok || gate != "" {
		t.Fatalf("all-knobs-off Decide = (%v, %q), want (true, \"\")", ok, gate)
	}
}

func TestConfigureFailedSchedulingGate_AutopilotProviderPostures(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		provider cloud.Provider
		wantGate string
		wantLog  string
	}{
		{
			name:     "autopilot cluster holds zero-nodes message",
			provider: fakeClusterConfigProvider{Provider: cloud.NoProvider, cfg: cloud.ClusterConfig{Autopilot: true}},
			wantGate: engine.GateAutopilotScaleToZero,
			wantLog:  "Autopilot cluster detected",
		},
		{
			name:     "standard cluster passes zero-nodes message at high count/span",
			provider: fakeClusterConfigProvider{Provider: cloud.NoProvider, cfg: cloud.ClusterConfig{Autopilot: false}},
			wantGate: "",
			wantLog:  "Standard (non-Autopilot) cluster detected",
		},
		{
			name:     "absent provider fails open with explicit log",
			provider: cloud.NoProvider,
			wantGate: "",
			wantLog:  "Autopilot scale-to-zero hold inactive",
		},
		{
			name:     "ClusterConfig error fails open with explicit log",
			provider: fakeClusterConfigProvider{Provider: cloud.NoProvider, err: errors.New("403 forbidden")},
			wantGate: "",
			wantLog:  "ClusterConfig unavailable",
		},
	}

	now := time.Now()
	sig := engine.Signal{
		Kind:     engine.KindK8sEvent,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "u-dns", Reason: "FailedScheduling"},
			Namespace:    "kube-system",
			KindOfObject: "Pod",
			Name:         "kube-dns-1",
			Message:      engine.NoNodesAvailableMessage,
			Count:        10,
			FirstSeen:    now.Add(-5 * time.Minute),
			LastSeen:     now,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			f, err := parseFlags([]string{"--dry-run"})
			if err != nil {
				t.Fatal(err)
			}
			dnsPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "kube-dns-1", UID: types.UID("u-dns")},
				Status: corev1.PodStatus{
					Phase: corev1.PodPending,
					Conditions: []corev1.PodCondition{{
						Type:   corev1.PodScheduled,
						Status: corev1.ConditionFalse,
						Reason: corev1.PodReasonUnschedulable,
					}},
				},
			}
			client := fake.NewSimpleClientset(dnsPod)
			capSrc := capacity.New(client, tc.provider, capacity.Config{PollInterval: time.Hour})
			filter := engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 0, 0))
			var logs []string
			configureFailedSchedulingGate(ctx, f, capSrc, tc.provider, filter, newMetrics(), func(format string, args ...any) {
				logs = append(logs, fmt.Sprintf(format, args...))
			})
			joined := strings.Join(logs, "\n")
			if !strings.Contains(joined, tc.wantLog) {
				t.Errorf("startup logs %q missing %q", joined, tc.wantLog)
			}

			// Before capacity syncs/arms (PodSchedulingUnknown), gate must fail
			// open so no pod is held without capacity.pending-aged running.
			if ok, gate := filter.Decide(sig); !ok || gate != "" {
				t.Errorf("unarmed Decide = (%v, %q), want (true, \"\")", ok, gate)
			}

			// Start capacity so podLister syncs & arms, then verify Autopilot posture.
			done := make(chan error, 1)
			go func() { done <- capSrc.Run(ctx, func(engine.Signal) {}) }()
			deadline := time.Now().Add(5 * time.Second)
			for !capSrc.HasSynced() {
				if time.Now().After(deadline) {
					t.Fatal("capacity source failed to sync")
				}
				time.Sleep(10 * time.Millisecond)
			}
			ok, gate := filter.Decide(sig)
			if gate != tc.wantGate || ok != (tc.wantGate == "") {
				t.Errorf("armed Decide = (%v, %q), want (%v, %q)", ok, gate, tc.wantGate == "", tc.wantGate)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("capacity Run error: %v", err)
			}
		})
	}
}

func TestDispatch_FailedSchedulingEndToEndDrill(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t0 := time.Now().Add(-30 * time.Second).Truncate(time.Second)

	lagPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "hog-lag", UID: types.UID("uid-lag")},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}
	schedErrPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "err-pod", UID: types.UID("uid-err")},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionFalse,
				Reason: corev1.PodReasonSchedulerError,
			}},
		},
	}
	runningPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "already-running", UID: types.UID("uid-run")},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}

	client := fake.NewSimpleClientset(lagPod, schedErrPod, runningPod)
	factory := informers.NewSharedInformerFactory(client, 0)
	capSrc := capacity.New(client, cloud.NoProvider, capacity.Config{PollInterval: time.Hour})
	capSrc.WithFactory(factory)

	f, err := parseFlags([]string{"--dry-run"})
	if err != nil {
		t.Fatal(err)
	}
	m := newMetrics()
	filter := engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 0, 0))
	configureFailedSchedulingGate(ctx, f, capSrc, cloud.NoProvider, filter, m, func(string, ...any) {})

	dedup, err := engine.NewDedupCache(5*time.Minute, "")
	if err != nil {
		t.Fatal(err)
	}
	base, captured, _ := newFakeDaemon(t)
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	disp := &dispatcher{
		filter:         filter,
		dedup:          dedup,
		pullClass:      engine.NewPullClassMemo(),
		routing:        engine.NewRoutingPolicy(nil),
		injector:       inj,
		metrics:        m,
		cluster:        "drill",
		mode:           "per-incident",
		injectMaxBytes: 64 * 1024,
	}

	capDone := make(chan error, 1)
	go func() { capDone <- capSrc.Run(ctx, func(engine.Signal) {}) }()
	deadline := time.Now().Add(5 * time.Second)
	for !capSrc.HasSynced() {
		if time.Now().After(deadline) {
			t.Fatal("capacity source failed to sync")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !cache.WaitForCacheSync(ctx.Done(), factory.Core().V1().Pods().Informer().HasSynced) {
		t.Fatal("pod cache did not sync")
	}

	// 1. Cache-lag Pending pod: held on failedscheduling_min_count.
	disp.Dispatch(ctx, engine.TriageEvent{
		Key:          engine.EventKey{UID: "uid-lag", Reason: "FailedScheduling"},
		Namespace:    "prod",
		KindOfObject: "Pod",
		Name:         "hog-lag",
		Message:      "0/3 nodes are available: 3 Insufficient cpu",
		Count:        1,
		FirstSeen:    t0,
		LastSeen:     t0,
	})
	if got := testutil.ToFloat64(m.eventsFiltered.WithLabelValues(engine.GateFailedSchedulingMinCount)); got != 1 {
		t.Fatalf("cache-lag min_count filtered = %v, want 1", got)
	}
	if len(*captured) != 0 {
		t.Fatalf("cache-lag leaked inject: %v", *captured)
	}

	// 2. Wrong-reporter TriggeredScaleUp rejected & counted.
	forged := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "forged-su.evt"},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod", Namespace: "prod", Name: "hog-lag", UID: types.UID("uid-lag"),
		},
		Source:        corev1.EventSource{Component: "fake-controller"},
		Reason:        leeway.ReasonTriggeredScaleUp,
		LastTimestamp: metav1.Time{Time: t0},
	}
	if _, err := client.CoreV1().Events("prod").Create(ctx, forged, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for testutil.ToFloat64(m.scaleUpVerdictsRejected) < 1 {
		if time.Now().After(deadline) {
			t.Fatal("wrong-reporter verdict was not rejected/counted")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 3. Real cluster-autoscaler TriggeredScaleUp holds even at high count & span.
	realSU := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "real-su.evt"},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod", Namespace: "prod", Name: "hog-lag", UID: types.UID("uid-lag"),
		},
		Source:        corev1.EventSource{Component: capacity.ScaleUpReporter},
		Reason:        leeway.ReasonTriggeredScaleUp,
		LastTimestamp: metav1.Time{Time: t0.Add(1 * time.Second)},
	}
	if _, err := client.CoreV1().Events("prod").Create(ctx, realSU, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for capSrc.LookupScaleUpVerdict("prod", "uid-lag").Verdict != engine.ScaleUpProvisioning {
		if time.Now().After(deadline) {
			t.Fatal("TriggeredScaleUp verdict never recorded")
		}
		time.Sleep(10 * time.Millisecond)
	}
	disp.Dispatch(ctx, engine.TriageEvent{
		Key:          engine.EventKey{UID: "uid-lag", Reason: "FailedScheduling"},
		Namespace:    "prod",
		KindOfObject: "Pod",
		Name:         "hog-lag",
		Message:      "0/3 nodes are available: 3 Insufficient cpu",
		Count:        10,
		FirstSeen:    t0.Add(-5 * time.Minute),
		LastSeen:     t0.Add(2 * time.Second),
	})
	if got := testutil.ToFloat64(m.eventsFiltered.WithLabelValues(engine.GateScaleUpHold)); got != 1 {
		t.Fatalf("scaleup_hold filtered = %v, want 1", got)
	}

	// 4. FailedScaleUp releases the 15m hold immediately → next sighting opens session.
	failedSU := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "failed-su.evt"},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod", Namespace: "prod", Name: "hog-lag", UID: types.UID("uid-lag"),
		},
		Source:        corev1.EventSource{Component: capacity.ScaleUpReporter},
		Reason:        leeway.ReasonFailedScaleUp,
		LastTimestamp: metav1.Time{Time: t0.Add(5 * time.Second)},
	}
	if _, err := client.CoreV1().Events("prod").Create(ctx, failedSU, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for capSrc.LookupScaleUpVerdict("prod", "uid-lag").Verdict != engine.ScaleUpDeclined {
		if time.Now().After(deadline) {
			t.Fatal("FailedScaleUp verdict never recorded")
		}
		time.Sleep(10 * time.Millisecond)
	}
	disp.Dispatch(ctx, engine.TriageEvent{
		Key:          engine.EventKey{UID: "uid-lag", Reason: "FailedScheduling"},
		Namespace:    "prod",
		KindOfObject: "Pod",
		Name:         "hog-lag",
		Message:      "0/3 nodes are available: 3 Insufficient cpu",
		Count:        2,
		FirstSeen:    t0,
		LastSeen:     t0.Add(6 * time.Second),
	})
	if len(*captured) != 1 || !strings.Contains((*captured)[0], "hog-lag") {
		t.Fatalf("after FailedScaleUp expected 1 inject for hog-lag, got %v", *captured)
	}

	// 5. SchedulerError pod passes on event #1.
	disp.Dispatch(ctx, engine.TriageEvent{
		Key:          engine.EventKey{UID: "uid-err", Reason: "FailedScheduling"},
		Namespace:    "prod",
		KindOfObject: "Pod",
		Name:         "err-pod",
		Message:      "scheduler extender failed",
		Count:        1,
		FirstSeen:    t0,
		LastSeen:     t0,
	})
	if len(*captured) != 2 || !strings.Contains((*captured)[1], "err-pod") {
		t.Fatalf("SchedulerError pod did not pass on event #1: %v", *captured)
	}

	// 6. Late finishSeries write on an already-Running pod held pre-dedup.
	disp.Dispatch(ctx, engine.TriageEvent{
		Key:          engine.EventKey{UID: "uid-run", Reason: "FailedScheduling"},
		Namespace:    "prod",
		KindOfObject: "Pod",
		Name:         "already-running",
		Message:      "0/3 nodes are available: 3 Insufficient cpu",
		Count:        8,
		FirstSeen:    t0.Add(-10 * time.Minute),
		LastSeen:     t0.Add(10 * time.Second),
	})
	if got := testutil.ToFloat64(m.eventsFiltered.WithLabelValues(engine.GateFailedSchedulingNotPending)); got != 1 {
		t.Fatalf("late finishSeries not_pending filtered = %v, want 1", got)
	}
	if len(*captured) != 2 {
		t.Fatalf("late finishSeries on Running pod leaked inject: %v", *captured)
	}

	cancel()
	if err := <-capDone; err != nil {
		t.Fatalf("capacity Run error: %v", err)
	}
}
