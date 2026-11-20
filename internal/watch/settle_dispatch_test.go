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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

func TestSettleFlag_DefaultsAndValidation(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate defaults: %v", err)
	}
	if f.settle != 0 {
		t.Errorf("default --settle = %v, want 0", f.settle)
	}

	fPos, err := parseFlags([]string{"--dry-run", "--settle=2m"})
	if err != nil {
		t.Fatalf("parseFlags(--settle=2m): %v", err)
	}
	if err := fPos.validate(); err != nil {
		t.Fatalf("validate(--settle=2m): %v", err)
	}
	if fPos.settle != 2*time.Minute {
		t.Errorf("--settle=2m parsed as %v", fPos.settle)
	}

	fNeg, err := parseFlags([]string{"--dry-run", "--settle=-1s"})
	if err != nil {
		t.Fatalf("parseFlags(--settle=-1s): %v", err)
	}
	if err := fNeg.validate(); err == nil {
		t.Fatal("validate(--settle=-1s) = nil, want error")
	}
}

func newSettleTestDispatcher(t *testing.T, settle time.Duration, clock func() time.Time) (*dispatcher, *[]routedInject) {
	t.Helper()
	base, injects := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
	d.filter = engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 3, 3, 3).WithSettle(settle))
	dedup, err := engine.NewDedupCache(5*time.Minute, "")
	if err != nil {
		t.Fatalf("NewDedupCache: %v", err)
	}
	dedup.SetClock(clock)
	d.dedup = dedup
	if settle > 0 {
		d.settle = newSettleTracker(settle, defaultSettleClearedTTL)
		d.settle.setClock(clock)
	}
	return d, injects
}

func sweepAndCountSettle(d *dispatcher) int {
	n := d.settle.Sweep()
	if n > 0 {
		d.metrics.gateOutcomes.WithLabelValues(engine.GateSettle, settleOutcomeSelfResolved).Add(float64(n))
	}
	return n
}

func TestSettleDispatch_CrashBlipSelfResolves(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 18, 0, 0, 0, time.UTC)
	d, injects := newSettleTestDispatcher(t, 2*time.Minute, func() time.Time { return now })

	t0 := now
	sig := crashLoopSignal()
	sig.Key.Reason = "BackOff"
	sig.Message = "Back-off restarting failed container worker in pod worker-0"
	sig.FirstSeen = t0
	sig.LastSeen = t0.Add(45 * time.Second)
	sig.Count = 3
	now = sig.LastSeen
	d.DispatchSignal(context.Background(), sig)

	// 4th BackOff at 99s (live shipmind-stage cadence) still < 2m.
	sig2 := sig
	sig2.Count = 4
	sig2.LastSeen = t0.Add(99 * time.Second)
	now = sig2.LastSeen
	d.DispatchSignal(context.Background(), sig2)

	if len(*injects) != 0 {
		t.Fatalf("crash blip opened %d sessions, want 0", len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.eventsFiltered.WithLabelValues(engine.GateSettle)); got != 2 {
		t.Errorf("events_filtered_total{gate=settle} = %v, want 2", got)
	}
	if d.settle.Len() != 1 {
		t.Fatalf("held count = %d, want 1", d.settle.Len())
	}

	// Just before settle + 5m (6m59s after last held sighting): still held.
	now = sig2.LastSeen.Add(6*time.Minute + 59*time.Second)
	if n := sweepAndCountSettle(d); n != 0 {
		t.Fatalf("early sweep expired %d, want 0", n)
	}

	// At settle + 5m (7m): swept as self_resolved.
	now = sig2.LastSeen.Add(7 * time.Minute)
	if n := sweepAndCountSettle(d); n != 1 {
		t.Fatalf("sweep at settle+5m expired %d, want 1", n)
	}
	if got := testutil.ToFloat64(d.metrics.gateOutcomes.WithLabelValues(engine.GateSettle, settleOutcomeSelfResolved)); got != 1 {
		t.Errorf("gate_outcomes_total{settle,self_resolved} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(d.metrics.gateOutcomes.WithLabelValues(engine.GateSettle, settleOutcomeForwarded)); got != 0 {
		t.Errorf("gate_outcomes_total{settle,forwarded} = %v, want 0", got)
	}

	// Informer relist replay of the cleared blip must NOT reopen a hold or
	// double-count self_resolved on the next sweep.
	now = now.Add(1 * time.Minute)
	d.DispatchSignal(context.Background(), sig2)
	if d.settle.Len() != 0 {
		t.Fatalf("relist replay reopened hold: len = %d, want 0", d.settle.Len())
	}
	now = now.Add(10 * time.Minute)
	if n := sweepAndCountSettle(d); n != 0 {
		t.Fatalf("post-replay sweep expired %d, want 0", n)
	}
}

func TestSettleDispatch_GenuineCrashLoopForwards(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 18, 0, 0, 0, time.UTC)
	d, injects := newSettleTestDispatcher(t, 2*time.Minute, func() time.Time { return now })

	t0 := now
	sig := crashLoopSignal()
	sig.Key.Reason = "BackOff"
	sig.Message = "Back-off restarting failed container worker in pod worker-0"
	sig.FirstSeen = t0
	sig.LastSeen = t0.Add(40 * time.Second)
	sig.Count = 3
	now = sig.LastSeen
	d.DispatchSignal(context.Background(), sig)

	if len(*injects) != 0 {
		t.Fatalf("early sighting injected %d times, want 0", len(*injects))
	}

	// 5th BackOff crosses --settle=2m → forwards and counts outcome=forwarded.
	sig2 := sig
	sig2.Count = 5
	sig2.LastSeen = t0.Add(2*time.Minute + 10*time.Second)
	now = sig2.LastSeen
	d.DispatchSignal(context.Background(), sig2)

	if len(*injects) != 1 {
		t.Fatalf("settled crash loop injects = %d, want 1", len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.gateOutcomes.WithLabelValues(engine.GateSettle, settleOutcomeForwarded)); got != 1 {
		t.Errorf("gate_outcomes_total{settle,forwarded} = %v, want 1", got)
	}

	// Subsequent sighting of the ongoing incident must NOT double-count forwarded.
	sig3 := sig
	sig3.Count = 6
	sig3.LastSeen = t0.Add(3 * time.Minute)
	now = sig3.LastSeen
	d.DispatchSignal(context.Background(), sig3)
	if got := testutil.ToFloat64(d.metrics.gateOutcomes.WithLabelValues(engine.GateSettle, settleOutcomeForwarded)); got != 1 {
		t.Errorf("subsequent sighting bumped forwarded to %v, want 1", got)
	}
}

func TestSettleDispatch_RepeatBlipAfterSelfResolvedClampsStart(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 18, 0, 0, 0, time.UTC)
	d, injects := newSettleTestDispatcher(t, 2*time.Minute, func() time.Time { return now })

	// Blip #1: held, then clears and sweeps as self_resolved.
	t0 := now
	sig := crashLoopSignal()
	sig.Key.Reason = "BackOff"
	sig.Message = "Back-off restarting failed container worker in pod worker-0"
	sig.FirstSeen = t0
	sig.LastSeen = t0.Add(90 * time.Second)
	sig.Count = 4
	now = sig.LastSeen
	d.DispatchSignal(context.Background(), sig)

	now = sig.LastSeen.Add(7 * time.Minute)
	if n := sweepAndCountSettle(d); n != 1 {
		t.Fatalf("blip 1 sweep = %d, want 1", n)
	}

	// Blip #2 inside the 1h Event TTL: carries the OLD FirstSeen (t0) and
	// Count=5, but LastSeen = t0+20m. Must be clamped to t0+20m, NOT forwarded
	// immediately!
	tBlip2 := t0.Add(20 * time.Minute)
	now = tBlip2
	blip2a := sig
	blip2a.Count = 5
	blip2a.LastSeen = tBlip2
	d.DispatchSignal(context.Background(), blip2a)
	if len(*injects) != 0 {
		t.Fatalf("repeat blip #2 sighting 1 bypassed settle: injects = %d, want 0", len(*injects))
	}

	// 1m into blip #2: still below 2m.
	now = tBlip2.Add(1 * time.Minute)
	blip2b := sig
	blip2b.Count = 6
	blip2b.LastSeen = now
	d.DispatchSignal(context.Background(), blip2b)
	if len(*injects) != 0 {
		t.Fatalf("repeat blip #2 sighting 2 (1m) bypassed settle: injects = %d, want 0", len(*injects))
	}

	// 2m10s into blip #2: crosses settle and forwards!
	now = tBlip2.Add(2*time.Minute + 10*time.Second)
	blip2c := sig
	blip2c.Count = 7
	blip2c.LastSeen = now
	d.DispatchSignal(context.Background(), blip2c)
	if len(*injects) != 1 {
		t.Fatalf("repeat blip #2 sighting 3 (2m10s) injects = %d, want 1", len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.gateOutcomes.WithLabelValues(engine.GateSettle, settleOutcomeForwarded)); got != 1 {
		t.Errorf("gate_outcomes_total{settle,forwarded} = %v, want 1", got)
	}
}

func TestSettleDispatch_RepeatBlipAfterResolvedIncidentClampsWithoutMutatingTombstone(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 18, 0, 0, 0, time.UTC)
	d, injects := newSettleTestDispatcher(t, 2*time.Minute, func() time.Time { return now })

	t0 := now
	sig := crashLoopSignal()
	sig.Key.Reason = "BackOff"
	sig.Message = "Back-off restarting failed container worker in pod worker-0"
	sig.FirstSeen = t0
	sig.LastSeen = t0.Add(2*time.Minute + 30*time.Second)
	sig.Count = 5
	now = sig.LastSeen
	d.DispatchSignal(context.Background(), sig)
	if len(*injects) != 1 {
		t.Fatalf("initial incident open injects = %d, want 1", len(*injects))
	}

	// Resolve incident and retire into #816 replay tombstone.
	now = t0.Add(4 * time.Minute)
	d.DispatchSignal(context.Background(), engine.Signal{
		Kind:        engine.KindResolved,
		TriageEvent: sig.TriageEvent,
		Recovery: &engine.Recovery{
			Resolution:        engine.ResolutionRecovered,
			ResolvedAt:        now,
			ClearedAfter:      2 * time.Minute,
			ObservedStableFor: 1 * time.Minute,
		},
	})
	if len(*injects) != 2 {
		t.Fatalf("resolved inject count = %d, want 2", len(*injects))
	}
	now = now.Add(6 * time.Minute)
	d.dedup.Sweep()

	// Unchanged relist at sig.LastSeen must stay suppressed as Replay and NOT
	// increment forwarded.
	d.DispatchSignal(context.Background(), sig)
	if len(*injects) != 2 {
		t.Fatalf("relist after retirement injected: count = %d, want 2", len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.gateOutcomes.WithLabelValues(engine.GateSettle, settleOutcomeForwarded)); got != 0 {
		t.Errorf("relist incremented forwarded to %v, want 0", got)
	}

	// Repeat blip inside Event TTL (t0+20m) carrying old FirstSeen=t0:
	// must be clamped to t0+20m via read-only LastRetiredTS and held!
	tBlip := t0.Add(20 * time.Minute)
	now = tBlip
	blip := sig
	blip.Count = 6
	blip.LastSeen = tBlip
	d.DispatchSignal(context.Background(), blip)
	if len(*injects) != 2 {
		t.Fatalf("repeat blip after resolved incident bypassed settle: injects = %d, want 2", len(*injects))
	}
	// Verify #816 tombstone was NOT deleted by LastRetiredTS!
	if ts, ok := d.dedup.LastRetiredTS(sig.CanonicalKey()); !ok || !ts.Equal(sig.LastSeen) {
		t.Fatalf("#816 tombstone mutated/deleted: LastRetiredTS = (%v, %v), want (%v, true)", ts, ok, sig.LastSeen)
	}

	// Blip clears without reaching 2m → swept as self_resolved.
	now = tBlip.Add(7 * time.Minute)
	if n := sweepAndCountSettle(d); n != 1 {
		t.Fatalf("repeat blip sweep = %d, want 1", n)
	}
	if got := testutil.ToFloat64(d.metrics.gateOutcomes.WithLabelValues(engine.GateSettle, settleOutcomeSelfResolved)); got != 1 {
		t.Errorf("gate_outcomes_total{settle,self_resolved} = %v, want 1", got)
	}

	// Redelivering the cleared repeat blip's last sighting (on the reused Event
	// whose FirstSeen is still t0, 20m earlier!) must stay suppressed — zero
	// sessions opened, zero extra self_resolved, and #816 tombstone untouched.
	now = now.Add(1 * time.Minute)
	d.DispatchSignal(context.Background(), blip)
	if len(*injects) != 2 {
		t.Fatalf("redelivered cleared repeat blip opened a session: injects = %d, want 2", len(*injects))
	}
	if d.settle.Len() != 0 {
		t.Fatalf("redelivered cleared repeat blip reopened hold: len = %d, want 0", d.settle.Len())
	}
	if ts, ok := d.dedup.LastRetiredTS(sig.CanonicalKey()); !ok || !ts.Equal(sig.LastSeen) {
		t.Fatalf("redelivered repeat blip mutated #816 tombstone: LastRetiredTS = (%v, %v), want (%v, true)", ts, ok, sig.LastSeen)
	}
}

func TestSettleDispatch_RedeliveredRepeatBlipAfterSelfResolvedSuppressed(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 18, 0, 0, 0, time.UTC)
	d, injects := newSettleTestDispatcher(t, 2*time.Minute, func() time.Time { return now })

	t0 := now
	sig := crashLoopSignal()
	sig.Key.Reason = "BackOff"
	sig.Message = "Back-off restarting failed container worker in pod worker-0"
	sig.FirstSeen = t0
	sig.LastSeen = t0.Add(60 * time.Second)
	sig.Count = 3
	now = sig.LastSeen
	d.DispatchSignal(context.Background(), sig)
	now = sig.LastSeen.Add(7 * time.Minute)
	if n := sweepAndCountSettle(d); n != 1 {
		t.Fatalf("blip 1 sweep = %d, want 1", n)
	}

	// Blip 2 on the same reused Event (FirstSeen still t0, LastSeen t0+20m),
	// clamped and swept as self_resolved.
	tBlip2 := t0.Add(20 * time.Minute)
	blip2 := sig
	blip2.Count = 5
	blip2.LastSeen = tBlip2.Add(45 * time.Second)
	now = blip2.LastSeen
	d.DispatchSignal(context.Background(), blip2)
	now = blip2.LastSeen.Add(7 * time.Minute)
	if n := sweepAndCountSettle(d); n != 1 {
		t.Fatalf("blip 2 sweep = %d, want 1", n)
	}

	// Redeliver blip 2's last sighting: reused Event has LastSeen - FirstSeen =
	// 20m45s >= 2m, but BlipStart clamps to sig.LastSeen so it is held and
	// Hold ignores the replay without opening a session or double-counting.
	now = now.Add(1 * time.Minute)
	d.DispatchSignal(context.Background(), blip2)
	if len(*injects) != 0 {
		t.Fatalf("redelivered repeat blip opened %d session(s), want 0", len(*injects))
	}
	if d.settle.Len() != 0 {
		t.Fatalf("redelivered repeat blip reopened hold: len = %d, want 0", d.settle.Len())
	}
	now = now.Add(10 * time.Minute)
	if n := sweepAndCountSettle(d); n != 0 {
		t.Fatalf("post-replay sweep expired %d, want 0", n)
	}
}

func TestSettleDispatch_FreshEventObjectAfterTombstoneUsesOwnFirstSeen(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 18, 0, 0, 0, time.UTC)
	d, injects := newSettleTestDispatcher(t, 2*time.Minute, func() time.Time { return now })

	// Blip 1 ends at t0+60s and sweeps.
	t0 := now
	sig := crashLoopSignal()
	sig.Key.Reason = "BackOff"
	sig.Message = "Back-off restarting failed container worker in pod worker-0"
	sig.FirstSeen = t0
	sig.LastSeen = t0.Add(60 * time.Second)
	sig.Count = 3
	now = sig.LastSeen
	d.DispatchSignal(context.Background(), sig)
	now = sig.LastSeen.Add(7 * time.Minute)
	if n := sweepAndCountSettle(d); n != 1 {
		t.Fatalf("blip 1 sweep = %d, want 1", n)
	}

	// Later, a brand-new Event object appears with FirstSeen = t0+70m (already
	// after cleared.EventLastSeen = t0+60s) and crosses count=3 at t0+70m35s.
	// Start must be sig.FirstSeen (t0+70m), NOT over-clamped to t0+70m35s!
	tFresh := t0.Add(70 * time.Minute)
	fresh1 := sig
	fresh1.FirstSeen = tFresh
	fresh1.LastSeen = tFresh.Add(35 * time.Second)
	fresh1.Count = 3
	now = fresh1.LastSeen
	d.DispatchSignal(context.Background(), fresh1)
	if len(*injects) != 0 {
		t.Fatalf("fresh Event at 35s injected: got %d, want 0", len(*injects))
	}

	// At tFresh+2m05s (only 1m30s after fresh1.LastSeen, but 2m05s after
	// fresh1.FirstSeen): must forward without the extra 35s delay!
	fresh2 := fresh1
	fresh2.LastSeen = tFresh.Add(2*time.Minute + 5*time.Second)
	fresh2.Count = 5
	now = fresh2.LastSeen
	d.DispatchSignal(context.Background(), fresh2)
	if len(*injects) != 1 {
		t.Fatalf("fresh Event at 2m05s from its own FirstSeen injects = %d, want 1", len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.gateOutcomes.WithLabelValues(engine.GateSettle, settleOutcomeForwarded)); got != 1 {
		t.Errorf("gate_outcomes_total{settle,forwarded} = %v, want 1", got)
	}
}

func TestSettleDispatch_RestartFailsOpenOnRepeatBlipStart(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 18, 20, 0, 0, time.UTC)
	// Fresh dispatcher with empty memory (post-restart).
	d, injects := newSettleTestDispatcher(t, 2*time.Minute, func() time.Time { return now })

	sig := crashLoopSignal()
	sig.Key.Reason = "BackOff"
	sig.Message = "Back-off restarting failed container worker in pod worker-0"
	sig.FirstSeen = now.Add(-20 * time.Minute)
	sig.LastSeen = now
	sig.Count = 6
	d.DispatchSignal(context.Background(), sig)

	if len(*injects) != 1 {
		t.Fatalf("post-restart repeat blip did not fail open: injects = %d, want 1", len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.gateOutcomes.WithLabelValues(engine.GateSettle, settleOutcomeForwarded)); got != 0 {
		t.Errorf("never-held event incremented forwarded to %v, want 0", got)
	}
}

func TestSettleTracker_BoundedCapAndRaceSafety(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 18, 0, 0, 0, time.UTC)
	d, _ := newSettleTestDispatcher(t, 2*time.Minute, func() time.Time { return now })
	d.settle.max = 8

	for i := range 20 {
		sig := crashLoopSignal()
		sig.Key.UID = fmt.Sprintf("uid-%d", i)
		sig.Name = fmt.Sprintf("pod-%d", i)
		sig.Key.Reason = "BackOff"
		sig.Message = "Back-off restarting failed container app"
		sig.FirstSeen = now
		sig.LastSeen = now.Add(10 * time.Second)
		sig.Count = 3
		d.DispatchSignal(context.Background(), sig)
	}
	if d.settle.Len() > 8 {
		t.Fatalf("settle entries exceeded cap 8: got %d", d.settle.Len())
	}

	// Concurrent DispatchSignal + Sweep under -race.
	var clockMu sync.Mutex
	cur := now
	d.settle.setClock(func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return cur
	})
	d.dedup.SetClock(func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return cur
	})

	var wg sync.WaitGroup
	for g := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range 25 {
				sig := crashLoopSignal()
				sig.Key.UID = fmt.Sprintf("race-%d-%d", id, i%5)
				sig.Name = fmt.Sprintf("race-pod-%d-%d", id, i%5)
				sig.Key.Reason = "BackOff"
				sig.Message = "Back-off restarting failed container app"
				sig.FirstSeen = now
				sig.LastSeen = now.Add(time.Duration(i) * time.Second)
				sig.Count = 3
				d.DispatchSignal(context.Background(), sig)
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 25 {
			clockMu.Lock()
			cur = cur.Add(time.Minute)
			clockMu.Unlock()
			_ = d.settle.Sweep()
		}
	}()
	wg.Wait()
}
