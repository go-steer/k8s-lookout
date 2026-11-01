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
