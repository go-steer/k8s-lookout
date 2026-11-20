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

// Issue #816: a Kubernetes Event lookout already delivered, re-delivered
// unchanged by an informer relist after its incident resolved (here: the
// pod was deleted), must never open the incident again. v0.43.0 dropped
// a resolved incident's dedup entry one window after its last activity,
// and with it the Event's last-seen timestamp, so the next relist looked
// like a first sighting. On shipmind-stage that re-opened the incident
// every ~10 minutes until the Event expired, kept its AX task from ever
// idle-suspending, and re-created the task after it was deleted.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// staleEventSignal is the stage incident: the BackOff Event on
// triage-lab/missing-env-1101-031702's pod, count 1, last seen 03:19:22.
func staleEventSignal() engine.Signal {
	sig := crashLoopSignal()
	sig.Key = engine.EventKey{UID: "1329dd83-0000-4000-8000-000000000001", Reason: "BackOff"}
	sig.Namespace = "triage-lab"
	sig.Name = "missing-env-1101-031702-6475fbb946-bwb9g"
	sig.ControllerRef = "ReplicaSet/missing-env-1101-031702-6475fbb946"
	sig.Message = "Back-off restarting failed container app in pod missing-env-1101-031702-6475fbb946-bwb9g"
	sig.FirstSeen = time.Date(2026, 11, 1, 3, 17, 54, 0, time.UTC)
	sig.LastSeen = time.Date(2026, 11, 1, 3, 19, 22, 0, time.UTC)
	sig.Count = 1
	return sig
}

// openThenDelete opens the stage incident and resolves it as
// object_deleted, returning the rig and the inject log.
func openThenDelete(t *testing.T) (*repeatRig, *[]routedInject) {
	t.Helper()
	base, injects := newRoutingFakeDaemon(t)
	r := wireRepeat(t, newRecoveryDispatcher(t, base))
	r.now = time.Date(2026, 11, 1, 3, 19, 25, 0, time.UTC)

	r.d.DispatchSignal(context.Background(), staleEventSignal()) // 03:19:25: the open
	r.now = r.now.Add(time.Minute)                               // ~03:20: the workload is removed
	r.obs.cleared, r.obs.resolution = true, engine.ResolutionObjectDeleted
	r.d.tracker.Tick() // symptomatic → clearing
	r.d.tracker.Tick() // clearing → resolved (object_deleted) into sess-1
	if len(*injects) != 2 {
		t.Fatalf("injects after open + delete = %d, want 2 (open, resolved)", len(*injects))
	}
	if got := payloadFields(t, (*injects)[1].Body)["kind"]; got != engine.KindResolved {
		t.Fatalf("second inject kind = %v, want resolved", got)
	}
	return r, injects
}

func TestDispatch_StaleEventAfterObjectDeletedNeverReopens(t *testing.T) {
	t.Parallel()
	r, injects := openThenDelete(t)

	// The informer re-delivers the unchanged Event (same uid, count and
	// last_seen) on every relist until it expires, an hour after
	// last_seen. Each lands well past the 5m dedup window.
	for _, gap := range []time.Duration{6 * time.Minute, 10 * time.Minute, 10 * time.Minute, 10 * time.Minute, 15 * time.Minute} {
		r.now = r.now.Add(gap)
		r.d.DispatchSignal(context.Background(), staleEventSignal())
	}
	if got := testutil.ToFloat64(r.d.metrics.sessionCreates.WithLabelValues("ok")); got != 1 {
		t.Errorf("session creates = %v, want 1: a re-delivered Event re-opened a resolved incident", got)
	}
	if len(*injects) != 2 {
		t.Errorf("injects = %d, want 2 (open, resolved); the stale Event was sent again: %v", len(*injects), *injects)
	}
}

// The tombstone holds only the Event as it was: a genuinely new
// occurrence (last_seen advanced) still fires, even after a resolution.
func TestDispatch_ChangedEventAfterResolutionStillFires(t *testing.T) {
	t.Parallel()
	r, injects := openThenDelete(t)

	r.now = r.now.Add(20 * time.Minute)
	r.d.DispatchSignal(context.Background(), staleEventSignal()) // stale: suppressed
	sig := staleEventSignal()
	sig.LastSeen = r.now // the kubelet reported again
	sig.Count = 2
	r.d.DispatchSignal(context.Background(), sig)

	if got := testutil.ToFloat64(r.d.metrics.sessionCreates.WithLabelValues("ok")); got != 2 {
		t.Errorf("session creates = %v, want 2: new activity after a resolution must open again", got)
	}
	if len(*injects) != 3 || (*injects)[2].SessionID != "sess-2" {
		t.Errorf("injects = %v, want the new occurrence opened into sess-2", *injects)
	}
}

// seriesFailedSchedulingSignal constructs a series-only FailedScheduling signal
// matching upstream / GKE 1.37 schedulers (issue #833).
func seriesFailedSchedulingSignal(uid, name string, first, last time.Time, count int) engine.Signal {
	return engine.Signal{
		Kind:     engine.KindK8sEvent,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: uid, Reason: "FailedScheduling"},
			Namespace:    "triage-lab",
			KindOfObject: "Pod",
			Name:         name,
			Message:      "0/3 nodes are available: 3 Insufficient cpu.",
			FirstSeen:    first,
			LastSeen:     last,
			Count:        count,
		},
	}
}

// TestDispatch_SeriesFinishAfterResolveSuppressed exercises the full #833
// finishSeries lifecycle through the dispatcher:
//   - occ #1 opens sess-1, occ #2 dedups into sess-1
//   - pod schedules and RecoveryTracker resolves sess-1 at T+2m
//   - delayed finishSeries arrives pre-retirement (T+3m), post-retirement (T+8m),
//     and relists at T+64m (within 1h of finishSeries write) — all suppressed,
//     no new session, no CrossSourceJoin leak
//   - genuine recurrence strictly after ResolvedAt opens sess-2.
func TestDispatch_SeriesFinishAfterResolveSuppressed(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)
	r := wireRepeat(t, d)

	t0 := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	r.now = t0
	obs := &testClearanceObserver{cleared: make(map[engine.EventKey]bool)}
	tracker := engine.NewRecoveryTrackerWithClock(time.Minute, func(sig engine.Signal) {
		d.DispatchSignal(context.Background(), sig)
	}, func() time.Time { return r.now })
	tracker.AddObserver(obs)
	d.tracker = tracker
	d.dedup.SetOpenCheck(tracker.Open)

	ctx := context.Background()
	// Occurrence #1 (Series == nil → Count=1, LastSeen=t0) opens sess-1.
	d.DispatchSignal(ctx, seriesFailedSchedulingSignal("uid-fs-1", "hog-1", t0, t0, 1))
	// Occurrence #2 (Series{Count:2, LastObservedTime: t0+2s}) dedups into sess-1.
	r.now = t0.Add(2 * time.Second)
	d.DispatchSignal(ctx, seriesFailedSchedulingSignal("uid-fs-1", "hog-1", t0, r.now, 2))

	// Pod schedules & clears; tracker resolves at T+2m.
	r.now = t0.Add(1 * time.Minute)
	obs.cleared[engine.EventKey{UID: "uid-fs-1", Reason: "FailedScheduling"}] = true
	tracker.Tick() // symptomatic → clearing
	r.now = t0.Add(2 * time.Minute)
	tracker.Tick() // clearing → resolved (ResolvedAt = T+2m)

	if len(*injects) != 2 {
		t.Fatalf("injects after open + resolve = %d, want 2", len(*injects))
	}

	// Pre-retirement finishSeries (T+3m, LastObservedTime=T+10s <= ResolvedAt): suppressed.
	r.now = t0.Add(3 * time.Minute)
	d.DispatchSignal(ctx, seriesFailedSchedulingSignal("uid-fs-1", "hog-1", t0, t0.Add(10*time.Second), 3))

	// Cross-source warning with pre-resolve timestamp must also stay suppressed (no CrossSourceJoin).
	capSig := seriesFailedSchedulingSignal("uid-fs-1", "hog-1", t0, t0.Add(10*time.Second), 1)
	capSig.Kind = "capacity.pending"
	capSig.Key.Reason = "pending"
	capSig.Severity = engine.SeverityWarning
	d.DispatchSignal(ctx, capSig)

	// Retire the resolved entry to a tombstone at T+6m, then receive a post-retirement
	// finishSeries at T+8m with LastObservedTime=T+20s <= ResolvedAt.
	r.now = t0.Add(6 * time.Minute)
	d.dedup.Sweep()
	r.now = t0.Add(8 * time.Minute)
	d.DispatchSignal(ctx, seriesFailedSchedulingSignal("uid-fs-1", "hog-1", t0, t0.Add(20*time.Second), 4))

	// Relist at T+64m (past 1h from t0, within 1h of T+8m finishSeries write): still suppressed.
	r.now = t0.Add(64 * time.Minute)
	d.dedup.Sweep()
	d.DispatchSignal(ctx, seriesFailedSchedulingSignal("uid-fs-1", "hog-1", t0, t0.Add(20*time.Second), 4))

	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 1 {
		t.Errorf("session creates after finishSeries + relist = %v, want 1", got)
	}
	if len(*injects) != 2 {
		t.Errorf("injects after finishSeries + relist = %d, want 2 (open, resolved); got %v", len(*injects), *injects)
	}

	// Genuine recurrence strictly after ResolvedAt opens sess-2.
	r.now = t0.Add(65 * time.Minute)
	d.DispatchSignal(ctx, seriesFailedSchedulingSignal("uid-fs-1", "hog-1", t0, r.now, 5))
	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 2 {
		t.Errorf("session creates after genuine recurrence = %v, want 2", got)
	}
	if len(*injects) != 3 || (*injects)[2].SessionID != "sess-2" {
		t.Errorf("injects = %v, want genuine recurrence opened into sess-2", *injects)
	}
}

// TestDispatch_ResolvedRevertedThenSecondResolvedUpdatesResolvedAt verifies that
// resolved → resolved.reverted → second resolved stamps the newer ResolvedAt so
// a finishSeries flush observed between the first and second resolution is still
// suppressed as Replay.
func TestDispatch_ResolvedRevertedThenSecondResolvedUpdatesResolvedAt(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)
	r := wireRepeat(t, d)

	t0 := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	r.now = t0
	obs := &testClearanceObserver{cleared: make(map[engine.EventKey]bool)}
	tracker := engine.NewRecoveryTrackerWithClock(time.Minute, func(sig engine.Signal) {
		d.DispatchSignal(context.Background(), sig)
	}, func() time.Time { return r.now })
	tracker.AddObserver(obs)
	d.tracker = tracker
	d.dedup.SetOpenCheck(tracker.Open)

	ctx := context.Background()
	key := engine.EventKey{UID: "uid-rev-1", Reason: "FailedScheduling"}
	d.DispatchSignal(ctx, seriesFailedSchedulingSignal(key.UID, "hog-rev", t0, t0, 1))

	// First resolve at T+2m.
	r.now = t0.Add(1 * time.Minute)
	obs.cleared[key] = true
	tracker.Tick()
	r.now = t0.Add(2 * time.Minute)
	tracker.Tick() // kind=resolved (ResolvedAt = T+2m)

	// Revert within revertWindow (T+2m30s): symptom returns, then clears again at T+4m.
	r.now = t0.Add(2*time.Minute + 30*time.Second)
	obs.cleared[key] = false
	tracker.Tick() // kind=resolved.reverted
	r.now = t0.Add(3 * time.Minute)
	obs.cleared[key] = true
	tracker.Tick()
	r.now = t0.Add(4 * time.Minute)
	tracker.Tick() // second kind=resolved (ResolvedAt = T+4m)

	if len(*injects) != 4 {
		t.Fatalf("injects after open + resolved + reverted + resolved = %d, want 4", len(*injects))
	}

	// Retire and deliver finishSeries whose LastObservedTime (T+2m45s) is after
	// the first resolution (T+2m) but before the second resolution (T+4m).
	r.now = t0.Add(10 * time.Minute)
	d.dedup.Sweep()
	d.DispatchSignal(ctx, seriesFailedSchedulingSignal(key.UID, "hog-rev", t0, t0.Add(2*time.Minute+45*time.Second), 3))

	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 1 {
		t.Errorf("session creates = %v, want 1 (finishSeries before second resolve must stay suppressed)", got)
	}
	if len(*injects) != 4 {
		t.Errorf("injects = %d, want 4 (no reopen after second resolve)", len(*injects))
	}
}

// TestDispatch_SiblingFoldPreservesRootResolvedAt verifies that when rootKey
// clears at T1 and a folded sibling clears later at T2, deferSiblingResolved's
// rootKey backstop preserves rootKey's T1 ResolvedAt rather than overwriting it
// with T2.
func TestDispatch_SiblingFoldPreservesRootResolvedAt(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)
	r := wireRepeat(t, d)

	t0 := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	r.now = t0
	obs := &testClearanceObserver{cleared: make(map[engine.EventKey]bool)}
	tracker := engine.NewRecoveryTrackerWithClock(time.Minute, func(sig engine.Signal) {
		d.DispatchSignal(context.Background(), sig)
	}, func() time.Time { return r.now })
	tracker.AddObserver(obs)
	d.tracker = tracker
	d.dedup.SetOpenCheck(tracker.Open)

	ctx := context.Background()
	sigA := seriesFailedSchedulingSignal("uid-sib-a", "hog-a", t0, t0, 1)
	sigA.ControllerRef = "Deployment/hog"
	sigB := seriesFailedSchedulingSignal("uid-sib-b", "hog-b", t0, t0, 1)
	sigB.ControllerRef = "Deployment/hog"

	d.DispatchSignal(ctx, sigA)
	d.DispatchSignal(ctx, sigB) // folds into sess-1

	// Root pod A clears first at T+2m (deferred while B is still open).
	r.now = t0.Add(1 * time.Minute)
	obs.cleared[sigA.CanonicalKey()] = true
	tracker.Tick()
	r.now = t0.Add(2 * time.Minute)
	tracker.Tick() // MarkResolved(keyA) stamps ResolvedAt = T+2m

	// Sibling pod B clears later at T+4m (coalesced kind=resolved; calls
	// MarkResolvedIfUnresolved(keyA), which must keep keyA's T+2m ResolvedAt).
	r.now = t0.Add(3 * time.Minute)
	obs.cleared[sigB.CanonicalKey()] = true
	tracker.Tick()
	r.now = t0.Add(4 * time.Minute)
	tracker.Tick()

	// Advance past keyA's window and sweep so keyA retires with ResolvedAt = T+2m.
	r.now = t0.Add(10 * time.Minute)
	d.dedup.Sweep()

	// A pre-T+2m finishSeries on pod A stays suppressed.
	d.DispatchSignal(ctx, seriesFailedSchedulingSignal("uid-sib-a", "hog-a", t0, t0.Add(30*time.Second), 2))
	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 1 {
		t.Fatalf("session creates after pre-T1 finishSeries on root = %v, want 1", got)
	}

	// Genuine new activity on pod A at T+3m (strictly after keyA's T+2m resolve,
	// though before sibling B's T+4m resolve) must open a fresh session.
	sigARecur := seriesFailedSchedulingSignal("uid-sib-a", "hog-a", t0, t0.Add(3*time.Minute), 3)
	sigARecur.ControllerRef = "Deployment/hog"
	d.DispatchSignal(ctx, sigARecur)
	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 2 {
		t.Errorf("session creates after root recurrence at T+3m (> root ResolvedAt T+2m) = %v, want 2 (injects=%d)", got, len(*injects))
	}
}
