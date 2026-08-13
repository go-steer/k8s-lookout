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
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// §7.7 ancestor reattachment (issue #220). The scenario throughout is
// the one that motivated the change, observed on a live cluster: a
// missing secret makes a pod fail to mount (critical k8s-event, own
// session) AND its Deployment fail to progress (warning, watchboard).
// One root cause, and before #220, two sessions.

var (
	ancDeploy = engine.Ancestor{Kind: "Deployment", Namespace: "online-boutique", Name: "emailservice"}
	ancNsOB   = engine.Ancestor{Kind: "Namespace", Name: "online-boutique"}
)

// failedMountPod is the critical pod event that opens the incident.
func failedMountPod() engine.Signal {
	ts := time.Date(2026, 8, 13, 11, 2, 28, 0, time.UTC)
	return engine.Signal{
		Kind:     engine.KindK8sEvent,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "pod-uid", Reason: "FailedMount"},
			Namespace:    "online-boutique",
			KindOfObject: "Pod",
			Name:         "emailservice-55bb5bf786-sz5vf",
			Message:      `MountVolume.SetUp failed for volume "demo-broken-creds" : secret "smtp-credentials-typo" not found`,
			Count:        1,
			FirstSeen:    ts,
			LastSeen:     ts,
		},
	}
}

// progressDeadline is the warning the Deployment fires for the same
// root cause — the signal that used to become a lone digest entry.
func progressDeadline() engine.Signal {
	ts := time.Date(2026, 8, 13, 11, 2, 27, 0, time.UTC)
	return engine.Signal{
		Kind:     "objectstate.progress_deadline",
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityWarning,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "deploy-uid", Reason: "progress_deadline"},
			Namespace:    "online-boutique",
			KindOfObject: "Deployment",
			Name:         "emailservice",
			Count:        1,
			FirstSeen:    ts,
			LastSeen:     ts,
		},
	}
}

// rolloutStall is a SECOND warning for the same workload from a
// different source family (rollout, not objectstate).
func rolloutStall() engine.Signal {
	ts := time.Date(2026, 8, 13, 11, 5, 38, 0, time.UTC)
	sig := progressDeadline()
	sig.Kind = "rollout.stall"
	sig.Key = engine.EventKey{UID: "deploy-uid-2", Reason: "stall"}
	sig.FirstSeen, sig.LastSeen = ts, ts
	return sig
}

// newReattachDispatcher is newBoardDispatcher plus a scripted topology
// in which both the pod and the Deployment resolve to the SAME
// Deployment ancestor (what graphfeed.Ancestors returns live: the
// owner chain for the pod, the self-ancestor for the Deployment).
func newReattachDispatcher(t *testing.T, base string, ancestors []engine.Ancestor) *dispatcher {
	t.Helper()
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
	pod, deploy := failedMountPod(), progressDeadline()
	d.resolver = &scriptedResolver{byObject: map[engine.ObjectRef][]engine.Ancestor{
		{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}:              ancestors,
		{Kind: "Deployment", Namespace: deploy.Namespace, Name: deploy.Name}: ancestors,
	}}
	d.board.reattach = d.reattachWatchboardEntry
	return d
}

// familyMembers extracts the kind=family.member payloads from the
// captured injects, in arrival order.
func familyMembers(t *testing.T, injects []routedInject) []inject.FamilyMemberPayload {
	t.Helper()
	var out []inject.FamilyMemberPayload
	for _, in := range injects {
		var p inject.FamilyMemberPayload
		if err := json.Unmarshal([]byte(messageOf(t, in.Body)), &p); err != nil {
			continue
		}
		if p.Kind == inject.KindFamilyMember {
			p.SessionID = in.SessionID
			out = append(out, p)
		}
	}
	return out
}

// TestReattach_WarningJoinsAncestorIncident is the headline case: the
// warning lands in the pod incident's session as a family.member
// followup, no watchboard session is created, and no digest is
// injected.
func TestReattach_WarningJoinsAncestorIncident(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newReattachDispatcher(t, base, []engine.Ancestor{ancDeploy, ancNsOB})
	ctx := context.Background()

	d.DispatchSignal(ctx, failedMountPod())   // critical → sess-1
	d.DispatchSignal(ctx, progressDeadline()) // warning → buffered
	d.board.FlushNow(ctx)

	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 1 {
		t.Errorf("session creates = %v, want exactly 1 (the pod incident) — the watchboard session must never open", got)
	}
	if got := testutil.ToFloat64(d.metrics.watchboardDigests); got != 0 {
		t.Errorf("digests = %v, want 0 (every entry reattached)", got)
	}
	if got := testutil.ToFloat64(d.metrics.watchboardReattached.WithLabelValues("objectstate.progress_deadline")); got != 1 {
		t.Errorf("reattached = %v, want 1", got)
	}
	fm := familyMembers(t, *injects)
	if len(fm) != 1 {
		t.Fatalf("family.member injects = %d, want 1: %+v", len(fm), *injects)
	}
	if fm[0].SessionID != "sess-1" {
		t.Errorf("family.member session = %q, want sess-1 (the pod incident's)", fm[0].SessionID)
	}
	if fm[0].MemberKind != "objectstate.progress_deadline" || fm[0].Family != ancDeploy.Key() {
		t.Errorf("family.member kind/family = %q/%q, want objectstate.progress_deadline/%s", fm[0].MemberKind, fm[0].Family, ancDeploy.Key())
	}
	if fm[0].Severity != "warning" {
		t.Errorf("family.member severity = %q, want warning (the class it was routed at)", fm[0].Severity)
	}
	// The reattached incident is BOUND: its §7.4 outcome closes into
	// the session where its evidence landed, not nowhere.
	if sid, ok := d.dedup.LookupSession(progressDeadline().Key); !ok || sid != "sess-1" {
		t.Errorf("warning binding = (%q, %v), want (sess-1, true)", sid, ok)
	}
}

// TestReattach_NamespaceAncestorNeverMatches: a namespace is shared by
// every incident in it, so it must not be a reattachment key — the
// warning digests normally instead.
func TestReattach_NamespaceAncestorNeverMatches(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newReattachDispatcher(t, base, []engine.Ancestor{ancNsOB})
	ctx := context.Background()

	d.DispatchSignal(ctx, failedMountPod())
	d.DispatchSignal(ctx, progressDeadline())
	d.board.FlushNow(ctx)

	if got := testutil.ToFloat64(d.metrics.watchboardReattached.WithLabelValues("objectstate.progress_deadline")); got != 0 {
		t.Errorf("reattached = %v, want 0 (namespace-class ancestor)", got)
	}
	if got := testutil.ToFloat64(d.metrics.watchboardDigests); got != 1 {
		t.Errorf("digests = %v, want 1 (the warning digested as before)", got)
	}
	if len(familyMembers(t, *injects)) != 0 {
		t.Errorf("family.member injects = %d, want 0", len(familyMembers(t, *injects)))
	}
}

// TestReattach_StormClaimedSessionIsNotATarget: a storm exists to
// collapse member chatter into ONE session (§7.5). Fanning
// reattachment followups into it would re-create that fan-out.
func TestReattach_StormClaimedSessionIsNotATarget(t *testing.T) {
	t.Parallel()
	base, _ := newRoutingFakeDaemon(t)
	d := newReattachDispatcher(t, base, []engine.Ancestor{ancDeploy, ancNsOB})
	ctx := context.Background()

	pod := failedMountPod()
	d.DispatchSignal(ctx, pod)
	// Simulate the storm claiming the pod incident after it opened.
	d.dedup.AttachToStorm(pod.Key, "storm-sess", "fp-storm", pod.IncidentRef())

	d.DispatchSignal(ctx, progressDeadline())
	d.board.FlushNow(ctx)

	if got := testutil.ToFloat64(d.metrics.watchboardReattached.WithLabelValues("objectstate.progress_deadline")); got != 0 {
		t.Errorf("reattached = %v, want 0 (storm-claimed target)", got)
	}
	if got := testutil.ToFloat64(d.metrics.watchboardDigests); got != 1 {
		t.Errorf("digests = %v, want 1", got)
	}
}

// TestReattach_OncePerFamilyPerWindow: the CrossSourceJoin bound
// applies here too — a flapping warning stream cannot spray followups
// into a live incident. A DIFFERENT source family still gets its one.
func TestReattach_OncePerFamilyPerWindow(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newReattachDispatcher(t, base, []engine.Ancestor{ancDeploy, ancNsOB})
	stall := rolloutStall()
	d.resolver.(*scriptedResolver).byObject[engine.ObjectRef{
		Kind: "Deployment", Namespace: stall.Namespace, Name: stall.Name,
	}] = []engine.Ancestor{ancDeploy, ancNsOB}
	ctx := context.Background()

	d.DispatchSignal(ctx, failedMountPod())
	d.DispatchSignal(ctx, progressDeadline())
	d.board.FlushNow(ctx)

	// Second objectstate warning, same family, same window: bound.
	second := progressDeadline()
	second.Key.UID = "deploy-uid-b"
	second.Name = "emailservice-b"
	d.resolver.(*scriptedResolver).byObject[engine.ObjectRef{
		Kind: "Deployment", Namespace: second.Namespace, Name: second.Name,
	}] = []engine.Ancestor{ancDeploy, ancNsOB}
	d.DispatchSignal(ctx, second)
	// A different family (rollout) still gets its one reattachment.
	d.DispatchSignal(ctx, stall)
	d.board.FlushNow(ctx)

	fm := familyMembers(t, *injects)
	if len(fm) != 2 {
		t.Fatalf("family.member injects = %d, want 2 (one per source family)", len(fm))
	}
	if fm[0].MemberKind != "objectstate.progress_deadline" || fm[1].MemberKind != "rollout.stall" {
		t.Errorf("family.member kinds = %q, %q, want objectstate.progress_deadline then rollout.stall", fm[0].MemberKind, fm[1].MemberKind)
	}
	// The over-bound objectstate warning digested instead of vanishing.
	if got := testutil.ToFloat64(d.metrics.watchboardDigests); got != 1 {
		t.Errorf("digests = %v, want 1 (the second objectstate warning still reaches the board)", got)
	}
}

// TestReattach_SurvivorsStillDigest: a partially reattached flush
// digests the entries that found no ancestor session, in arrival
// order, exactly as before.
func TestReattach_SurvivorsStillDigest(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newReattachDispatcher(t, base, []engine.Ancestor{ancDeploy, ancNsOB})
	ctx := context.Background()

	d.DispatchSignal(ctx, failedMountPod())
	d.DispatchSignal(ctx, progressDeadline()) // reattaches
	d.DispatchSignal(ctx, warningSignal(1))   // unrelated: no ancestors scripted
	d.board.FlushNow(ctx)

	if got := testutil.ToFloat64(d.metrics.watchboardDigests); got != 1 {
		t.Fatalf("digests = %v, want 1 (the unrelated warning)", got)
	}
	var digest inject.WatchboardDigestPayload
	for _, in := range *injects {
		var probe struct {
			Kind string `json:"kind"`
		}
		_ = json.Unmarshal([]byte(messageOf(t, in.Body)), &probe)
		if probe.Kind == inject.KindWatchboardDigest {
			if err := json.Unmarshal([]byte(messageOf(t, in.Body)), &digest); err != nil {
				t.Fatalf("unmarshal digest: %v", err)
			}
		}
	}
	if len(digest.Entries) != 1 || digest.Entries[0].Kind != "objectstate.restart_burst" {
		t.Errorf("digest entries = %+v, want only the unrelated restart_burst", digest.Entries)
	}
}

// TestReattach_NilResolverKeepsPreIssue220Behavior: without a topology
// graph (--storm off) the stage is inert — every warning digests, and
// the wire is byte-identical to before the change.
func TestReattach_NilResolverKeepsPreIssue220Behavior(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200) // no resolver, no reattach
	ctx := context.Background()

	d.DispatchSignal(ctx, failedMountPod())
	d.DispatchSignal(ctx, progressDeadline())
	d.board.FlushNow(ctx)

	if got := testutil.ToFloat64(d.metrics.watchboardDigests); got != 1 {
		t.Errorf("digests = %v, want 1 (unchanged pre-#220 behavior)", got)
	}
	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 2 {
		t.Errorf("session creates = %v, want 2 (the pre-#220 split: incident + watchboard)", got)
	}
	if len(familyMembers(t, *injects)) != 0 {
		t.Error("family.member injected without a resolver")
	}
}

// TestReattach_LiveTraceCollapsesToOneSession replays the issue #220
// trace end to end — FailedMount, progress_deadline, rollout.stall,
// one workload — and pins the outcome the change exists for: ONE
// session, with both warnings inside it as followups.
func TestReattach_LiveTraceCollapsesToOneSession(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newReattachDispatcher(t, base, []engine.Ancestor{ancDeploy, ancNsOB})
	stall := rolloutStall()
	d.resolver.(*scriptedResolver).byObject[engine.ObjectRef{
		Kind: "Deployment", Namespace: stall.Namespace, Name: stall.Name,
	}] = []engine.Ancestor{ancDeploy, ancNsOB}
	ctx := context.Background()

	d.DispatchSignal(ctx, failedMountPod())
	d.DispatchSignal(ctx, progressDeadline())
	d.board.FlushNow(ctx)
	d.DispatchSignal(ctx, stall)
	d.board.FlushNow(ctx)

	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 1 {
		t.Errorf("session creates = %v, want exactly 1 — the whole point of #220", got)
	}
	if got := testutil.ToFloat64(d.metrics.watchboardDigests); got != 0 {
		t.Errorf("digests = %v, want 0", got)
	}
	sessions := map[string]bool{}
	for _, in := range *injects {
		sessions[in.SessionID] = true
	}
	if len(sessions) != 1 || !sessions["sess-1"] {
		t.Errorf("injects spread over sessions %v, want only sess-1", sessions)
	}
	if len(familyMembers(t, *injects)) != 2 {
		t.Errorf("family.member injects = %d, want 2 (progress_deadline + rollout.stall)", len(familyMembers(t, *injects)))
	}
}
