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

package engine

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type stubPodState struct {
	state PodSchedulingState
	byKey map[string]PodSchedulingState
}

func (s stubPodState) LookupPodSchedulingState(namespace, name, uid string) PodSchedulingState {
	if s.byKey != nil {
		if st, ok := s.byKey[namespace+"/"+name+"/"+uid]; ok {
			return st
		}
	}
	return s.state
}

type stubVerdicts struct {
	marks map[string]ScaleUpMark
}

func (s stubVerdicts) LookupScaleUpVerdict(namespace, uid string) ScaleUpMark {
	if s.marks == nil {
		return ScaleUpMark{}
	}
	return s.marks[namespace+"/"+uid]
}

func makeFailedScheduling(ns, name, uid, msg string, count int, first, last time.Time) Signal {
	return Signal{
		Kind:     KindK8sEvent,
		Source:   SourceSentinel,
		Severity: SeverityCritical,
		Cluster:  "test-cluster",
		TriageEvent: TriageEvent{
			Key:          EventKey{UID: uid, Reason: "FailedScheduling"},
			Namespace:    ns,
			KindOfObject: "Pod",
			Name:         name,
			Message:      msg,
			Count:        count,
			FirstSeen:    first,
			LastSeen:     last,
		},
	}
}

func TestFailedSchedulingGate_DisabledAndAllKnobsOff(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

	stale := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes are available", 1, now.Add(-40*time.Minute), now.Add(-30*time.Minute))
	fOff := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0))
	if ok, gate := fOff.Decide(stale); !ok || gate != "" {
		t.Fatalf("disabled gate Decide = (%v, %q), want (true, \"\")", ok, gate)
	}

	cfgAllOff := FailedSchedulingConfig{
		Enabled:                  true,
		MinCount:                 1,
		MinSpan:                  0,
		ScaleUpHold:              0,
		AutopilotScaleToZeroHold: false,
		Autopilot:                true,
		PodState:                 stubPodState{state: PodSchedulingNotPending},
		Now:                      func() time.Time { return now },
	}
	if cfgAllOff.Active() {
		t.Fatal("all-4-knobs-off FailedSchedulingConfig.Active() = true, want false")
	}
	fAllOff := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfgAllOff))
	if ok, gate := fAllOff.Decide(stale); !ok || gate != "" {
		t.Fatalf("all-4-knobs-off Decide = (%v, %q), want (true, \"\")", ok, gate)
	}
}

func TestFailedSchedulingGate_PodStatePreCheck(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

	cfg := DefaultFailedSchedulingConfig()
	cfg.Enabled = true
	cfg.PodState = stubPodState{state: PodSchedulingNotPending}
	cfg.Verdicts = stubVerdicts{marks: map[string]ScaleUpMark{
		"default/uid-1": {Verdict: ScaleUpDeclined, At: now.Add(-5 * time.Minute)},
	}}
	cfg.Now = func() time.Time { return now }
	cfg.Logf = func(string, ...any) {}

	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))
	sig := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes are available", 10, now.Add(-10*time.Minute), now.Add(-1*time.Minute))
	if ok, gate := f.Decide(sig); ok || gate != GateFailedSchedulingNotPending {
		t.Fatalf("NotPending Decide = (%v, %q), want (false, %q)", ok, gate, GateFailedSchedulingNotPending)
	}

	cfg.PodState = stubPodState{state: PodSchedulingOtherFailure}
	cfg.Verdicts = stubVerdicts{marks: map[string]ScaleUpMark{
		"default/uid-1": {Verdict: ScaleUpProvisioning, At: now.Add(-1 * time.Minute)},
	}}
	f = NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))
	sig1 := makeFailedScheduling("default", "hog-1", "uid-1", "scheduler plugin error", 1, now, now)
	if ok, gate := f.Decide(sig1); !ok || gate != "" {
		t.Fatalf("SchedulerError Decide = (%v, %q), want (true, \"\")", ok, gate)
	}

	cfg.PodState = stubPodState{state: PodSchedulingUnschedulable}
	cfg.Verdicts = stubVerdicts{}
	f = NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))
	if ok, gate := f.Decide(sig1); ok || gate != GateFailedSchedulingMinCount {
		t.Fatalf("Unschedulable count=1 Decide = (%v, %q), want (false, %q)", ok, gate, GateFailedSchedulingMinCount)
	}

	// PodSchedulingUnknown (capacity pod cache not synced/armed yet →
	// capacity.pending-aged is not sweeping yet) → fail open immediately.
	cfg.PodState = stubPodState{state: PodSchedulingUnknown}
	cfg.Verdicts = stubVerdicts{}
	f = NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))
	if ok, gate := f.Decide(sig1); !ok || gate != "" {
		t.Fatalf("PodSchedulingUnknown Decide = (%v, %q), want (true, \"\")", ok, gate)
	}
}

func TestFailedSchedulingGate_StaleCheck(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	cfg := DefaultFailedSchedulingConfig()
	cfg.Enabled = true
	cfg.PodState = stubPodState{state: PodSchedulingUnschedulable}
	cfg.Now = func() time.Time { return now }
	cfg.Logf = func(string, ...any) {}
	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))

	stale := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes are available", 10, now.Add(-20*time.Minute), now.Add(-15*time.Minute-time.Second))
	if ok, gate := f.Decide(stale); ok || gate != GateFailedSchedulingStale {
		t.Fatalf("stale Decide = (%v, %q), want (false, %q)", ok, gate, GateFailedSchedulingStale)
	}

	edge := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes are available", 10, now.Add(-20*time.Minute), now.Add(-15*time.Minute))
	if ok, gate := f.Decide(edge); !ok || gate != "" {
		t.Fatalf("15m boundary Decide = (%v, %q), want (true, \"\")", ok, gate)
	}

	zeroTS := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes are available", 5, time.Time{}, time.Time{})
	if ok, gate := f.Decide(zeroTS); !ok || gate != "" {
		t.Fatalf("zero timestamps Decide = (%v, %q), want (true, \"\")", ok, gate)
	}
}

func TestFailedSchedulingGate_VerdictsAndTruncation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 12, 0, 30, 0, time.UTC)
	verdictAt := time.Date(2026, 11, 20, 12, 0, 25, 0, time.UTC)

	cfg := DefaultFailedSchedulingConfig()
	cfg.Enabled = true
	cfg.PodState = stubPodState{state: PodSchedulingUnschedulable}
	cfg.Now = func() time.Time { return now }
	cfg.Logf = func(string, ...any) {}

	cfg.Verdicts = stubVerdicts{marks: map[string]ScaleUpMark{
		"default/uid-1": {Verdict: ScaleUpDeclined, At: verdictAt},
	}}
	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))

	postVerdict := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes", 1, verdictAt.Add(time.Second), verdictAt.Add(time.Second))
	if ok, gate := f.Decide(postVerdict); !ok || gate != "" {
		t.Fatalf("declined post-verdict Decide = (%v, %q), want (true, \"\")", ok, gate)
	}

	sameSecMicros := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes", 1, verdictAt.Add(400*time.Millisecond), verdictAt.Add(400*time.Millisecond))
	if ok, gate := f.Decide(sameSecMicros); ok || gate != GateFailedSchedulingMinCount {
		t.Fatalf("declined same-second micros Decide = (%v, %q), want (false, %q)", ok, gate, GateFailedSchedulingMinCount)
	}

	preVerdict := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes", 1, verdictAt.Add(-time.Second), verdictAt.Add(-time.Second))
	if ok, gate := f.Decide(preVerdict); ok || gate != GateFailedSchedulingMinCount {
		t.Fatalf("declined pre-verdict Decide = (%v, %q), want (false, %q)", ok, gate, GateFailedSchedulingMinCount)
	}

	triggeredAt := now.Add(-10 * time.Minute)
	cfg.Verdicts = stubVerdicts{marks: map[string]ScaleUpMark{
		"default/uid-2": {Verdict: ScaleUpProvisioning, At: triggeredAt},
	}}
	f = NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))

	duringHold := makeFailedScheduling("default", "hog-2", "uid-2", "0/3 nodes", 12, triggeredAt, now)
	if ok, gate := f.Decide(duringHold); ok || gate != GateScaleUpHold {
		t.Fatalf("within scaleup-hold Decide = (%v, %q), want (false, %q)", ok, gate, GateScaleUpHold)
	}

	expiredAt := now.Add(-16 * time.Minute)
	cfg.Verdicts = stubVerdicts{marks: map[string]ScaleUpMark{
		"default/uid-2": {Verdict: ScaleUpProvisioning, At: expiredAt},
	}}
	f = NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))
	afterHold := makeFailedScheduling("default", "hog-2", "uid-2", "0/3 nodes", 6, now.Add(-5*time.Minute), now)
	if ok, gate := f.Decide(afterHold); !ok || gate != "" {
		t.Fatalf("past scaleup-hold Decide = (%v, %q), want (true, \"\")", ok, gate)
	}

	cfg.ScaleUpHold = 0
	cfg.Verdicts = stubVerdicts{marks: map[string]ScaleUpMark{
		"default/uid-2": {Verdict: ScaleUpProvisioning, At: triggeredAt},
	}}
	f = NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))
	if ok, gate := f.Decide(duringHold); !ok || gate != "" {
		t.Fatalf("scaleup-hold=0 Decide = (%v, %q), want (true, \"\")", ok, gate)
	}

	// Discriminate sig.LastSeen-based hold from wall clock:
	// (a) mark is 20m old by wall clock (> 15m), but sighting LastSeen is
	//     10m ago (LastSeen - mark.At = 10m <= 15m, not stale) → MUST hold!
	cfg.ScaleUpHold = DefaultScaleUpHold
	cfg.Verdicts = stubVerdicts{marks: map[string]ScaleUpMark{
		"default/uid-3": {Verdict: ScaleUpProvisioning, At: now.Add(-20 * time.Minute)},
	}}
	f = NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))
	replayInHold := makeFailedScheduling("default", "hog-3", "uid-3", "0/3 nodes", 10, now.Add(-20*time.Minute), now.Add(-10*time.Minute))
	if ok, gate := f.Decide(replayInHold); ok || gate != GateScaleUpHold {
		t.Fatalf("LastSeen-relative hold (wall>15m, LastSeen-mark=10m) Decide = (%v, %q), want (false, %q)", ok, gate, GateScaleUpHold)
	}

	// (b) mark is 10m old by wall clock (<= 15m), but sighting LastSeen is
	//     16m after mark.At → hold MUST expire and pass on count/span!
	cfg.Verdicts = stubVerdicts{marks: map[string]ScaleUpMark{
		"default/uid-4": {Verdict: ScaleUpProvisioning, At: now.Add(-10 * time.Minute)},
	}}
	f = NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))
	pastByLastSeen := makeFailedScheduling("default", "hog-4", "uid-4", "0/3 nodes", 10, now.Add(-10*time.Minute), now.Add(6*time.Minute))
	if ok, gate := f.Decide(pastByLastSeen); !ok || gate != "" {
		t.Fatalf("LastSeen-relative expiry (wall=10m, LastSeen-mark=16m) Decide = (%v, %q), want (true, \"\")", ok, gate)
	}
}

func TestFailedSchedulingGate_AutopilotScaleToZero(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

	baseSig := makeFailedScheduling("kube-system", "kube-dns-1", "uid-dns", "  no nodes available to schedule pods\n", 20, now.Add(-10*time.Minute), now)
	fitSig := makeFailedScheduling("kube-system", "kube-dns-1", "uid-dns", "0/3 nodes are available: no nodes available to schedule pods", 20, now.Add(-10*time.Minute), now)

	cfg := DefaultFailedSchedulingConfig()
	cfg.Enabled = true
	cfg.Autopilot = true
	cfg.PodState = stubPodState{state: PodSchedulingUnschedulable}
	cfg.Now = func() time.Time { return now }
	cfg.Logf = func(string, ...any) {}
	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))
	if ok, gate := f.Decide(baseSig); ok || gate != GateAutopilotScaleToZero {
		t.Fatalf("Autopilot zero-nodes Decide = (%v, %q), want (false, %q)", ok, gate, GateAutopilotScaleToZero)
	}
	if ok, gate := f.Decide(fitSig); !ok || gate != "" {
		t.Fatalf("Autopilot fit error Decide = (%v, %q), want (true, \"\")", ok, gate)
	}

	cfg.Autopilot = false
	f = NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))
	if ok, gate := f.Decide(baseSig); !ok || gate != "" {
		t.Fatalf("Standard zero-nodes Decide = (%v, %q), want (true, \"\")", ok, gate)
	}

	cfg.Autopilot = true
	cfg.AutopilotScaleToZeroHold = false
	f = NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))
	if ok, gate := f.Decide(baseSig); !ok || gate != "" {
		t.Fatalf("flag-disabled Autopilot Decide = (%v, %q), want (true, \"\")", ok, gate)
	}

	cfg.AutopilotScaleToZeroHold = true
	cfg.Verdicts = stubVerdicts{marks: map[string]ScaleUpMark{
		"kube-system/uid-dns": {Verdict: ScaleUpDeclined, At: now.Add(-1 * time.Minute)},
	}}
	f = NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))
	if ok, gate := f.Decide(baseSig); !ok || gate != "" {
		t.Fatalf("Autopilot with declined verdict Decide = (%v, %q), want (true, \"\")", ok, gate)
	}
}

func TestFailedSchedulingGate_CountAndSpanBackstop(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	cfg := DefaultFailedSchedulingConfig()
	cfg.Enabled = true
	cfg.PodState = stubPodState{state: PodSchedulingUnschedulable}
	cfg.Now = func() time.Time { return now }
	cfg.Logf = func(string, ...any) {}
	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))

	lowCount := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes", 4, now.Add(-5*time.Minute), now)
	if ok, gate := f.Decide(lowCount); ok || gate != GateFailedSchedulingMinCount {
		t.Fatalf("count=4 Decide = (%v, %q), want (false, %q)", ok, gate, GateFailedSchedulingMinCount)
	}

	zeroCountShortSpan := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes", 0, now.Add(-30*time.Second), now)
	if ok, gate := f.Decide(zeroCountShortSpan); ok || gate != GateFailedSchedulingMinSpan {
		t.Fatalf("count=0 short span Decide = (%v, %q), want (false, %q)", ok, gate, GateFailedSchedulingMinSpan)
	}
	zeroCountLongSpan := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes", 0, now.Add(-3*time.Minute), now)
	if ok, gate := f.Decide(zeroCountLongSpan); !ok || gate != "" {
		t.Fatalf("count=0 long span Decide = (%v, %q), want (true, \"\")", ok, gate)
	}

	shortSpan := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes", 5, now.Add(-90*time.Second), now)
	if ok, gate := f.Decide(shortSpan); ok || gate != GateFailedSchedulingMinSpan {
		t.Fatalf("count=5 span=90s Decide = (%v, %q), want (false, %q)", ok, gate, GateFailedSchedulingMinSpan)
	}

	metBoth := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes", 5, now.Add(-2*time.Minute), now)
	if ok, gate := f.Decide(metBoth); !ok || gate != "" {
		t.Fatalf("count=5 span=2m Decide = (%v, %q), want (true, \"\")", ok, gate)
	}

	zeroFirst := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes", 5, time.Time{}, now)
	if ok, gate := f.Decide(zeroFirst); !ok || gate != "" {
		t.Fatalf("zero FirstSeen Decide = (%v, %q), want (true, \"\")", ok, gate)
	}
}

func TestFailedSchedulingGate_PerPodDecisionLogMemo(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	var logs []string

	verdicts := &stubVerdicts{marks: map[string]ScaleUpMark{}}
	cfg := DefaultFailedSchedulingConfig()
	cfg.Enabled = true
	cfg.PodState = stubPodState{state: PodSchedulingUnschedulable}
	cfg.Verdicts = verdicts
	cfg.Now = func() time.Time { return now }
	cfg.Logf = func(format string, args ...any) {
		mu.Lock()
		logs = append(logs, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0).WithFailedScheduling(cfg))

	sig := makeFailedScheduling("default", "hog-1", "uid-1", "0/3 nodes", 1, now, now)

	f.Decide(sig)
	now = now.Add(5 * time.Minute)
	sig.LastSeen = now
	f.Decide(sig)
	if len(logs) != 1 {
		t.Fatalf("same decision within 15m logged %d lines, want 1: %v", len(logs), logs)
	}

	verdicts.marks["default/uid-1"] = ScaleUpMark{Verdict: ScaleUpProvisioning, At: now}
	f.Decide(sig)
	if len(logs) != 2 || !strings.Contains(logs[1], "triggered scale-up") {
		t.Fatalf("decision change logged %d lines (%v), want 2 with scale-up hold", len(logs), logs)
	}

	now = now.Add(15*time.Minute + time.Second)
	verdicts.marks["default/uid-1"] = ScaleUpMark{Verdict: ScaleUpProvisioning, At: now.Add(-time.Minute)}
	sig.LastSeen = now
	f.Decide(sig)
	if len(logs) != 3 {
		t.Fatalf("same decision after >15m logged %d lines, want 3: %v", len(logs), logs)
	}

	unkeyed := makeFailedScheduling("default", "hog-anon", "", "0/3 nodes", 1, now, now)
	f.Decide(unkeyed)
	f.Decide(unkeyed)
	if len(logs) != 5 {
		t.Fatalf("unkeyed sightings logged %d total lines, want 5", len(logs))
	}

	// Overflow suppression: when fsLogMemo is full of live entries, new keys
	// must be suppressed (return false) rather than flooding logs.
	memo := fsLogMemo{entries: make(map[string]fsLogEntry, maxFailedSchedulingLogEntries), lastSweep: now}
	for i := range maxFailedSchedulingLogEntries {
		memo.entries[fmt.Sprintf("k-%d", i)] = fsLogEntry{gate: GateFailedSchedulingMinCount, at: now}
	}
	if memo.shouldLog("overflow-pod", GateFailedSchedulingMinCount, now) {
		t.Fatal("full fsLogMemo shouldLog(overflow-pod) = true, want false (suppress overflow)")
	}
}
