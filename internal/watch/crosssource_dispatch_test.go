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

// Cross-source join visibility (M4 drill observation 4): a
// dedup-window duplicate from a DIFFERENT source family than the one
// that opened the incident is a leading↔reactive join the bound
// session must hear about — route it as a schema-stable
// kind=family.member followup (route=followup) instead of silent
// suppression, at most once per source family per incident per
// window. Same-source duplicates keep the M0 suppression behavior
// byte-for-byte, and a storm-claimed member never fans followups into
// the storm session (§7.5).

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// restartBurstFor fabricates the object-state leading signal that
// shares crashLoopSignal's dedup family (restart_burst →
// CrashLoopBackOff on the same pod UID).
func restartBurstFor(orig engine.Signal, lastSeen time.Time) engine.Signal {
	return engine.Signal{
		Kind:     "objectstate.restart_burst",
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityWarning,
		TriageEvent: engine.TriageEvent{
			Key:           engine.EventKey{UID: orig.Key.UID, Reason: "restart_burst"},
			Namespace:     orig.Namespace,
			KindOfObject:  orig.KindOfObject,
			Name:          orig.Name,
			Message:       "3 container restarts within 10m0s",
			FirstSeen:     lastSeen,
			LastSeen:      lastSeen,
			ControllerRef: orig.ControllerRef,
			Count:         1,
		},
	}
}

// TestCrossSourceJoinInjectsFollowup is the M4 drill's exact gap: the
// reactive k8s-event opened the session; the object-state source's
// leading signal for the same incident deduped into it silently.
// Now: ONE kind=family.member followup inject carrying the joining
// signal's identity + the canonical family + the design_ref,
// route=followup with the bound session; repeats from the same
// source, and same-source duplicates, stay suppressed.
func TestCrossSourceJoinInjectsFollowup(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
	s := openDispatchStore(t, d)
	ctx := context.Background()

	// The reactive event opens the per-incident session.
	sig := crashLoopSignal()
	d.DispatchSignal(ctx, sig)
	if len(*injects) != 1 {
		t.Fatalf("opener produced %d injects, want 1", len(*injects))
	}
	openerSid := (*injects)[0].SessionID

	// The object-state source's angle on the same incident: a
	// cross-source join → followup into the bound session.
	join := restartBurstFor(sig, sig.LastSeen.Add(30*time.Second))
	d.DispatchSignal(ctx, join)
	if len(*injects) != 2 {
		t.Fatalf("cross-source join produced %d injects, want 2 (opener + followup)", len(*injects))
	}
	fu := (*injects)[1]
	if fu.SessionID != openerSid {
		t.Errorf("followup routed to %q, want the opener's session %q", fu.SessionID, openerSid)
	}
	var payload inject.FamilyMemberPayload
	if err := json.Unmarshal([]byte(messageOf(t, fu.Body)), &payload); err != nil {
		t.Fatalf("followup payload: %v", err)
	}
	if payload.Kind != inject.KindFamilyMember {
		t.Errorf("followup kind = %q, want %q", payload.Kind, inject.KindFamilyMember)
	}
	if payload.MemberKind != "objectstate.restart_burst" || payload.Reason != "restart_burst" ||
		payload.Severity != "warning" || payload.UID != sig.Key.UID {
		t.Errorf("followup payload = %+v, want the joining signal's own kind/reason/severity/uid", payload)
	}
	if payload.Family != "CrashLoopBackOff" || payload.OpenedBy != "k8s-event" {
		t.Errorf("family/opened_by = %q/%q, want CrashLoopBackOff/k8s-event", payload.Family, payload.OpenedBy)
	}
	if payload.SessionID != openerSid {
		t.Errorf("payload session_id = %q, want the bound session %q", payload.SessionID, openerSid)
	}
	if payload.DesignRef != inject.FamilyMemberDesignRef {
		t.Errorf("design_ref = %q, want %q", payload.DesignRef, inject.FamilyMemberDesignRef)
	}

	// Bound: a SECOND object-state duplicate is plain suppression.
	d.DispatchSignal(ctx, restartBurstFor(sig, sig.LastSeen.Add(time.Minute)))
	// Same-source duplicate (another k8s-event): suppressed as ever.
	rep := sig
	rep.LastSeen = sig.LastSeen.Add(2 * time.Minute)
	d.DispatchSignal(ctx, rep)
	if len(*injects) != 2 {
		t.Fatalf("repeat joins re-injected: %d injects, want 2 (max 1 followup per source per window)", len(*injects))
	}

	byRoute := occurrencesByRoute(t, s)
	if len(byRoute[store.RouteFollowup]) != 1 || byRoute[store.RouteFollowup][0].SessionID != openerSid {
		t.Errorf("followup rows = %+v, want exactly 1 carrying the bound session", byRoute[store.RouteFollowup])
	}
	if len(byRoute[store.RouteSuppressed]) != 2 {
		t.Errorf("suppressed rows = %d, want 2 (the bounded repeat + the same-source duplicate)", len(byRoute[store.RouteSuppressed]))
	}
	if got := testutil.ToFloat64(d.metrics.crossSourceFollowups.WithLabelValues("objectstate")); got != 1 {
		t.Errorf("cross_source_followups_total{source=objectstate} = %v, want 1", got)
	}
}

// TestCrossSourceJoinEventKindStaysFrozen: when the JOINING signal is
// a k8s-event (the leading source opened the incident first — the
// whole point of leading indicators), the followup is still the
// schema-stable kind=family.member, and the joining EVENT's own
// frozen kind rides inside it untouched (member_kind=k8s-event) so
// playbooks can still pattern-match the frozen string.
func TestCrossSourceJoinEventKindStaysFrozen(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
	openDispatchStore(t, d)
	ctx := context.Background()

	// The leading object-state signal opens the incident. Severity
	// warning routes to the watchboard (no session yet) — force the
	// paged path via critical so the entry is session-bound, like an
	// escalated leading indicator.
	lead := restartBurstFor(crashLoopSignal(), time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC))
	lead.Severity = engine.SeverityCritical
	d.DispatchSignal(ctx, lead)
	if len(*injects) != 1 {
		t.Fatalf("leading signal produced %d injects, want 1", len(*injects))
	}

	// The reactive kubelet event catches up: cross-source join → a
	// kind=family.member followup whose member_kind is the joining
	// event's own frozen kind.
	ev := crashLoopSignal()
	ev.LastSeen = lead.LastSeen.Add(45 * time.Second)
	d.DispatchSignal(ctx, ev)
	if len(*injects) != 2 {
		t.Fatalf("event join produced %d injects, want 2", len(*injects))
	}
	var payload inject.FamilyMemberPayload
	if err := json.Unmarshal([]byte(messageOf(t, (*injects)[1].Body)), &payload); err != nil {
		t.Fatalf("followup payload: %v", err)
	}
	if payload.Kind != inject.KindFamilyMember {
		t.Errorf("followup kind = %q, want %q", payload.Kind, inject.KindFamilyMember)
	}
	if payload.MemberKind != engine.KindK8sEvent {
		t.Errorf("joining k8s-event member_kind = %q, want the frozen %q", payload.MemberKind, engine.KindK8sEvent)
	}
	if payload.OpenedBy != "objectstate" {
		t.Errorf("opened_by = %q, want objectstate (the leading source opened the incident)", payload.OpenedBy)
	}
}

// TestFamilyMember_ExactWireShape pins the kind=family.member payload
// byte-exact (§9.3 discipline: schema-stable structured injects,
// never prose — harvesters and playbooks parse fields).
func TestFamilyMember_ExactWireShape(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
	ctx := context.Background()

	sig := crashLoopSignal()
	d.DispatchSignal(ctx, sig)
	join := restartBurstFor(sig, sig.LastSeen.Add(30*time.Second))
	d.DispatchSignal(ctx, join)
	if len(*injects) != 2 {
		t.Fatalf("expected the family.member inject, got %d injects", len(*injects))
	}

	want := `{"kind":"family.member","member_kind":"objectstate.restart_burst","reason":"restart_burst","severity":"warning",` +
		`"namespace":"checkout","kind_of_object":"Pod","name":"checkout-svc-7b9d-x4kzq","uid":"abc-123",` +
		`"fingerprint":"` + stampedFingerprint(join) + `",` +
		`"family":"CrashLoopBackOff","opened_by":"k8s-event","cluster":"prod-us-central1","session_id":"sess-1",` +
		`"message":"cross-source join: objectstate.restart_burst (restart_burst) attached to this session's CrashLoopBackOff incident, opened by the k8s-event source — a second observation angle on the same incident; at most one family.member per source family per window",` +
		`"design_ref":"DESIGN.md §10.3"}`
	if got := messageOf(t, (*injects)[1].Body); got != want {
		t.Errorf("family.member wire shape drifted:\n got: %s\nwant: %s", got, want)
	}
}

// TestCrossSourceJoinStormMemberStaysQuiet: §7.5 storm suppression
// outranks join visibility. A storm-claimed member's bound session IS
// the storm session, and a storm exists to collapse member-level
// chatter — so a cross-source duplicate on a storm-claimed key is
// store-recorded (route=suppressed, carrying the storm's session) but
// never fans a family.member inject into the storm session.
func TestCrossSourceJoinStormMemberStaysQuiet(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
	s := openDispatchStore(t, d)
	ctx := context.Background()

	// The reactive event opens its per-incident session, then a storm
	// claims the incident: the binding now routes to the storm session.
	sig := crashLoopSignal()
	d.DispatchSignal(ctx, sig)
	if len(*injects) != 1 {
		t.Fatalf("opener produced %d injects, want 1", len(*injects))
	}
	d.dedup.AttachToStorm(sig.CanonicalKey(), "sess-storm", "sha256:storm", sig.IncidentRef())

	// The object-state angle on the same incident: without the guard
	// this would be a cross-source join into the STORM session.
	d.DispatchSignal(ctx, restartBurstFor(sig, sig.LastSeen.Add(30*time.Second)))
	if len(*injects) != 1 {
		t.Fatalf("storm-claimed join fanned out: %d injects, want 1 (storm members never inject family.member)", len(*injects))
	}

	byRoute := occurrencesByRoute(t, s)
	if len(byRoute[store.RouteFollowup]) != 0 {
		t.Errorf("followup rows = %+v, want none for a storm-claimed member", byRoute[store.RouteFollowup])
	}
	suppressed := byRoute[store.RouteSuppressed]
	if len(suppressed) != 1 || suppressed[0].SessionID != "sess-storm" {
		t.Errorf("suppressed rows = %+v, want exactly 1 carrying the storm session", suppressed)
	}
	if got := testutil.ToFloat64(d.metrics.crossSourceFollowups.WithLabelValues("objectstate")); got != 0 {
		t.Errorf("cross_source_followups_total{source=objectstate} = %v, want 0", got)
	}
}

// TestSharedModeKeepsSuppression: --mode=shared keeps its frozen
// duplicate contract — no followup injects, route=suppressed.
func TestSharedModeKeepsSuppression(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
	s := openDispatchStore(t, d)
	d.mode = "shared"
	d.targetSid = "shared-1"
	d.board = nil
	ctx := context.Background()

	sig := crashLoopSignal()
	d.DispatchSignal(ctx, sig)
	d.DispatchSignal(ctx, restartBurstFor(sig, sig.LastSeen.Add(30*time.Second)))
	if len(*injects) != 1 {
		t.Fatalf("shared mode: %d injects, want 1 (duplicates never inject)", len(*injects))
	}
	byRoute := occurrencesByRoute(t, s)
	if len(byRoute[store.RouteFollowup]) != 0 || len(byRoute[store.RouteSuppressed]) != 1 {
		t.Errorf("shared mode routes = %v, want the duplicate suppressed", routes(byRoute))
	}
}
