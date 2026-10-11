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

// Issue #599 at dispatcher scale: an incident that stays broken past
// its dedup window is a followup into its bound session — kind
// k8s-event-followup (or the source's own kind) with repeat_count —
// on a doubling schedule, never a second open. The #585 soak saw one
// unschedulable pod open 225 sessions in 24h before this.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// switchObserver is a §7.4 clearance observer whose verdict the test
// flips: stuck (symptom present) until cleared is set.
type switchObserver struct{ cleared bool }

func (o *switchObserver) Clearance(engine.Incident) (engine.Clearance, bool) {
	if !o.cleared {
		return engine.Clearance{Cleared: false}, true
	}
	return engine.Clearance{Cleared: true, StableSince: time.Now().Add(-time.Hour), Resolution: engine.ResolutionRecovered}, true
}

// repeatRig is a dispatcher with the production #599 wiring — §7.7
// severity routing with a watchboard (which per-incident mode always
// builds, so a warning is NOT session class), and a recovery tracker
// whose Open is the dedup cache's open check — on a fake dedup clock.
type repeatRig struct {
	d   *dispatcher
	obs *switchObserver
	now time.Time
}

func wireRepeat(t *testing.T, d *dispatcher) *repeatRig {
	t.Helper()
	if d.routing == nil {
		d.routing = engine.NewRoutingPolicy(nil)
	}
	if d.board == nil {
		d.board = newWatchboard(watchboardConfig{
			injector:      d.injector,
			metrics:       d.metrics,
			cluster:       d.cluster,
			batch:         100,
			flushInterval: time.Hour,
			rotateAfter:   200,
		})
		d.board.bind = d.bindWatchboardIncident
	}
	r := &repeatRig{d: d, obs: &switchObserver{}, now: time.Date(2026, 10, 9, 16, 56, 19, 0, time.UTC)}
	d.dedup.SetClock(func() time.Time { return r.now })
	tracker := engine.NewRecoveryTracker(time.Minute, func(sig engine.Signal) {
		d.DispatchSignal(context.Background(), sig)
	})
	tracker.AddObserver(r.obs)
	d.tracker = tracker
	d.dedup.SetOpenCheck(tracker.Open)
	return r
}

// unschedulableSignal is the soak's stuck pod: required zone
// anti-affinity with one more replica than zones.
func unschedulableSignal() engine.Signal {
	sig := crashLoopSignal()
	sig.Key = engine.EventKey{UID: "uid-cf29j", Reason: "FailedScheduling"}
	sig.Namespace = "triage-lab"
	sig.Name = "batch-worker-664c6c98fb-cf29j"
	sig.Container = ""
	sig.ControllerRef = "Deployment/batch-worker"
	sig.Message = "0/6 nodes are available: 3 node(s) didn't match pod anti-affinity rules, 3 node(s) had untolerated taint {node-role: infra}."
	sig.FirstSeen = time.Date(2026, 10, 9, 16, 56, 19, 0, time.UTC)
	return sig
}

// emit advances the rig clock by gap and dispatches sig as a fresh
// occurrence (LastSeen = now: the scheduler re-emitted).
func (r *repeatRig) emit(gap time.Duration, sig engine.Signal) {
	r.now = r.now.Add(gap)
	sig.LastSeen = r.now
	r.d.DispatchSignal(context.Background(), sig)
}

// repeatPayloadOf decodes an inject body into the generic payload map.
func repeatPayloadOf(t *testing.T, in routedInject) map[string]any {
	t.Helper()
	return payloadFields(t, in.Body)
}

// TestDispatchRepeat_SoakScenario: the stuck pod re-emits every ~5m
// for four hours. ONE session; the open, then reminders 1..6 into it
// as k8s-event-followup with repeat_count counting up (gaps 5m, 10m,
// 20m, 40m, 1h, 1h); everything else absorbed.
func TestDispatchRepeat_SoakScenario(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	r := wireRepeat(t, newRecoveryDispatcher(t, base))
	sig := unschedulableSignal()
	const cadence = 5*time.Minute + 5*time.Second

	r.emit(0, sig)
	for elapsed := time.Duration(0); elapsed < 4*time.Hour; elapsed += cadence {
		r.emit(cadence, sig)
	}
	if got := testutil.ToFloat64(r.d.metrics.sessionCreates.WithLabelValues("ok")); got != 1 {
		t.Fatalf("session creates = %v over 4h, want 1 (the #599 bug opened one per lapse)", got)
	}
	if len(*injects) != 7 {
		t.Fatalf("injects = %d, want the open + 6 reminders", len(*injects))
	}
	open := repeatPayloadOf(t, (*injects)[0])
	if open["kind"] != engine.KindK8sEvent {
		t.Errorf("first inject kind = %v, want k8s-event", open["kind"])
	}
	if _, ok := open["repeat_count"]; ok {
		t.Errorf("the open carries repeat_count: %v", open)
	}
	for i, in := range (*injects)[1:] {
		if in.SessionID != "sess-1" {
			t.Errorf("reminder %d routed to %q, want sess-1", i+1, in.SessionID)
		}
		p := repeatPayloadOf(t, in)
		if p["kind"] != engine.KindK8sEventFollowup {
			t.Errorf("reminder %d kind = %v, want k8s-event-followup", i+1, p["kind"])
		}
		if got, _ := p["repeat_count"].(float64); int(got) != i+1 {
			t.Errorf("reminder %d repeat_count = %v, want %d", i+1, p["repeat_count"], i+1)
		}
		if _, ok := p["enrichment"]; ok {
			t.Errorf("reminder %d carries an enrichment bundle", i+1)
		}
	}
	if got := testutil.ToFloat64(r.d.metrics.repeatFollowups.WithLabelValues("reminder")); got != 6 {
		t.Errorf("repeat_followups{type=reminder} = %v, want 6", got)
	}
}

// TestDispatchRepeat_ExactWireShape pins the first reminder's bytes:
// the frozen k8s-event-followup shape (no source/severity/fingerprint,
// reason_class always set) with the trailing repeat_count.
func TestDispatchRepeat_ExactWireShape(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	r := wireRepeat(t, newRecoveryDispatcher(t, base))
	sig := unschedulableSignal()
	r.emit(0, sig)
	r.emit(5*time.Minute+5*time.Second, sig)
	if len(*injects) != 2 {
		t.Fatalf("injects = %d, want open + reminder", len(*injects))
	}
	want := `{"kind":"k8s-event-followup","reason":"FailedScheduling","namespace":"triage-lab","kind_of_object":"Pod","name":"batch-worker-664c6c98fb-cf29j","uid":"uid-cf29j","message":"0/6 nodes are available: 3 node(s) didn't match pod anti-affinity rules, 3 node(s) had untolerated taint {node-role: infra}.","count":2,"first_seen":"2026-10-09T16:56:19Z","last_seen":"2026-10-09T17:01:24Z","cluster":"prod-us-central1","context":{"controller_ref":"Deployment/batch-worker"},"type":"Warning","reason_class":"FailedScheduling","repeat_count":1}`
	if got := messageOf(t, (*injects)[1].Body); got != want {
		t.Errorf("reminder wire shape drifted:\n got: %s\nwant: %s", got, want)
	}
}

// TestDispatchRepeat_MaterialChange: a new message shape mid-backoff
// is a followup with repeat_count omitted, at the first occurrence
// past max(2 windows, the next reminder interval) after the last inject.
func TestDispatchRepeat_MaterialChange(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	r := wireRepeat(t, newRecoveryDispatcher(t, base))
	sig := unschedulableSignal()
	r.emit(0, sig)
	r.emit(5*time.Minute+5*time.Second, sig) // reminder 1
	r.emit(time.Minute, sig)                 // absorbed
	sig.Message = "0/6 nodes are available: 6 Insufficient memory."
	r.emit(time.Minute, sig) // held: within a window of reminder 1
	if len(*injects) != 2 {
		t.Fatalf("injects = %d, want the change held inside the window", len(*injects))
	}
	r.emit(8*time.Minute+time.Second, sig) // past max(2 windows, reminder 2 interval) after reminder 1
	if len(*injects) != 3 {
		t.Fatalf("injects = %d, want open + reminder + change", len(*injects))
	}
	p := repeatPayloadOf(t, (*injects)[2])
	if p["kind"] != engine.KindK8sEventFollowup || (*injects)[2].SessionID != "sess-1" {
		t.Errorf("change followup = kind %v sid %q, want k8s-event-followup into sess-1", p["kind"], (*injects)[2].SessionID)
	}
	if _, ok := p["repeat_count"]; ok {
		t.Errorf("a material change must omit repeat_count, got %v", p["repeat_count"])
	}
	if got := testutil.ToFloat64(r.d.metrics.repeatFollowups.WithLabelValues("change")); got != 1 {
		t.Errorf("repeat_followups{type=change} = %v, want 1", got)
	}
}

// TestDispatchRepeat_ResolvedThenRecurOpensFresh: once §7.4 resolves
// the incident, the recurrence is a fresh open in a new session.
func TestDispatchRepeat_ResolvedThenRecurOpensFresh(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	r := wireRepeat(t, newRecoveryDispatcher(t, base))
	sig := unschedulableSignal()
	r.emit(0, sig)
	r.emit(5*time.Minute+5*time.Second, sig) // reminder 1
	r.obs.cleared = true
	r.d.tracker.Tick() // symptomatic → clearing
	r.d.tracker.Tick() // clearing → resolved: kind=resolved into sess-1
	if len(*injects) != 3 || repeatPayloadOf(t, (*injects)[2])["kind"] != engine.KindResolved {
		t.Fatalf("want open, reminder, resolved; got %d injects", len(*injects))
	}
	r.obs.cleared = false
	r.emit(20*time.Minute, sig)
	if got := testutil.ToFloat64(r.d.metrics.sessionCreates.WithLabelValues("ok")); got != 2 {
		t.Fatalf("session creates = %v, want 2 (the recurrence opens fresh)", got)
	}
	last := (*injects)[len(*injects)-1]
	if p := repeatPayloadOf(t, last); p["kind"] != engine.KindK8sEvent || last.SessionID != "sess-2" {
		t.Errorf("recurrence = kind %v into %q, want a k8s-event open in sess-2", p["kind"], last.SessionID)
	}
}

// TestDispatchRepeat_SourceKindKeepsKind: a source-namespaced incident
// repeats under its own kind, with the §8 identity block it always
// carries.
func TestDispatchRepeat_SourceKindKeepsKind(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	r := wireRepeat(t, newRecoveryDispatcher(t, base))
	sig := crashLoopSignal()
	sig.Kind = "objectstate.restart_burst"
	sig.Key.Reason = "restart_burst"
	sig.Message = "container server restarted 6 times in 10m"
	r.emit(0, sig)
	r.emit(6*time.Minute, sig)
	if len(*injects) != 2 {
		t.Fatalf("injects = %d, want open + reminder", len(*injects))
	}
	p := repeatPayloadOf(t, (*injects)[1])
	if p["kind"] != "objectstate.restart_burst" {
		t.Errorf("repeat kind = %v, want the source's own objectstate.restart_burst", p["kind"])
	}
	if p["repeat_count"] != float64(1) || p["fingerprint"] == nil || p["severity"] != "critical" {
		t.Errorf("repeat payload = %v, want repeat_count 1 plus fingerprint/severity", p)
	}
}

// TestDispatchRepeat_MessageMasked: the repeat followup is an inject
// surface like the open (§6.5, issue #82).
func TestDispatchRepeat_MessageMasked(t *testing.T) {
	t.Parallel()
	tc := secretShapeCases[0]
	requireMaskedForm(t, tc.planted, tc.masked)
	base, injects := newRoutingFakeDaemon(t)
	r := wireRepeat(t, newRecoveryDispatcher(t, base))
	sig := unschedulableSignal()
	sig.Message = tc.planted
	r.emit(0, sig)
	r.emit(6*time.Minute, sig)
	if len(*injects) != 2 {
		t.Fatalf("injects = %d, want open + reminder", len(*injects))
	}
	if got := repeatPayloadOf(t, (*injects)[1])["message"]; got != tc.masked {
		t.Errorf("reminder message = %q, want the MaskString form %q", got, tc.masked)
	}
}

// TestDispatchRepeat_StormClaimedRoutesToStorm: a storm member that
// stays broken repeats into the STORM session — no per-incident
// session, no second storm.member, and the storm stays open.
func TestDispatchRepeat_StormClaimedRoutesToStorm(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, sigs := newStormDispatcher(t, base, 5)
	r := wireRepeat(t, d)
	for _, sig := range sigs {
		r.emit(time.Second, sig)
	}
	// sess-1, sess-2 per-incident, then sess-3 the storm.
	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 3 {
		t.Fatalf("setup: session creates = %v, want 3", got)
	}
	before := len(*injects)
	member := sigs[4]
	r.emit(6*time.Minute, member)
	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 3 {
		t.Errorf("session creates = %v after the member lapsed, want still 3", got)
	}
	if len(*injects) != before+1 {
		t.Fatalf("member lapse produced %d injects, want exactly 1 followup", len(*injects)-before)
	}
	in := (*injects)[before]
	p := repeatPayloadOf(t, in)
	if in.SessionID != "sess-3" || p["kind"] != engine.KindK8sEventFollowup || p["repeat_count"] != float64(1) {
		t.Errorf("member repeat = kind %v repeat %v into %q, want k8s-event-followup repeat 1 into the storm's sess-3",
			p["kind"], p["repeat_count"], in.SessionID)
	}
	if d.storm.ActiveStorms() != 1 {
		t.Errorf("active storms = %d, want the storm still open", d.storm.ActiveStorms())
	}
}

// TestDispatchRepeat_DowngradeStillLeavesTheSession: §9.4 — once an
// agent downgrades the incident, its lapse routes to the watchboard as
// before; a downgraded signal is not session class, so it never
// repeats into the session. Inside the backoff the downgraded
// duplicates still feed the regression evidence count.
func TestDispatchRepeat_DowngradeStillLeavesTheSession(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, s := triageDispatcher(t, base)
	r := wireRepeat(t, d)
	sig := unschedulableSignal()
	r.emit(0, sig)
	r.emit(5*time.Minute+5*time.Second, sig) // reminder 1
	if len(*injects) != 2 {
		t.Fatalf("setup: injects = %d, want open + reminder", len(*injects))
	}
	writeRecord(t, s, memory.TriageStatusRecord{
		Fingerprint:      stampedFingerprint(sig),
		ResourceKey:      memory.ResourceKey("Pod", sig.Namespace, sig.Name),
		Session:          (*injects)[0].SessionID,
		Status:           memory.StatusTriaged,
		SeverityOverride: "warning",
	})
	refreshTriage(d)
	r.emit(time.Minute, sig)   // downgraded, inside the window: a duplicate
	r.emit(6*time.Minute, sig) // downgraded lapse: legacy → watchboard
	if len(*injects) != 2 {
		t.Errorf("downgraded incident injected again: %d injects, want 2", len(*injects))
	}
	byRoute := occurrencesByRoute(t, s)
	if len(byRoute[store.RouteWatchboard]) != 1 {
		t.Errorf("routes = %v, want the downgraded lapse on the watchboard", routes(byRoute))
	}
	if len(byRoute[store.RouteFollowup]) != 1 {
		t.Errorf("routes = %v, want exactly the one pre-downgrade reminder as route=followup", routes(byRoute))
	}
}

// TestDispatchRepeat_ReplaysStaySuppressed: an informer re-list that
// replays the already-seen Event past the window neither reminds nor
// reopens.
func TestDispatchRepeat_ReplaysStaySuppressed(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	r := wireRepeat(t, newRecoveryDispatcher(t, base))
	sig := unschedulableSignal()
	r.emit(0, sig)
	replay := sig
	replay.LastSeen = r.now
	for range 4 {
		r.now = r.now.Add(20 * time.Minute)
		r.d.DispatchSignal(context.Background(), replay)
	}
	if len(*injects) != 1 {
		t.Fatalf("replays produced %d injects, want only the open", len(*injects))
	}
	if got := testutil.ToFloat64(r.d.metrics.sessionCreates.WithLabelValues("ok")); got != 1 {
		t.Errorf("session creates = %v, want 1", got)
	}
}
