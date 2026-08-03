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

package ingress

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// gceEvent builds an ingress-gce-shaped Event: eventType is the k8s
// Event.Type (Normal/Warning), objKind the involved object's kind.
func gceEvent(eventType, objKind, reason, msg string) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "web.evt"},
		InvolvedObject: corev1.ObjectReference{
			Kind: objKind, Namespace: "shop", Name: "web", UID: types.UID("uid-web"),
		},
		Type:          eventType,
		Reason:        reason,
		Message:       msg,
		Count:         1,
		LastTimestamp: metav1.Time{Time: time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)},
	}
}

func TestSourceContract(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset())
	if s.Name() != "ingress" {
		t.Errorf("Name() = %q, want ingress (§7.2 table)", s.Name())
	}
	if s.Scope() != sources.ScopeCluster {
		t.Errorf("Scope() = %v, want cluster", s.Scope())
	}
	var _ sources.Source = s
	var _ sources.AccessDeclarer = s
}

// TestRequiredAccess pins the §11 declaration: events list+watch and
// NOTHING else — the source rides the same grant k8s-events does
// (deploy/12-clusterrole-watcher.yaml), no new RBAC rule.
func TestRequiredAccess(t *testing.T) {
	t.Parallel()
	reqs := New(fake.NewSimpleClientset()).RequiredAccess()
	want := map[string]bool{
		"list events cluster-wide":  false,
		"watch events cluster-wide": false,
	}
	for _, r := range reqs {
		key := r.String()
		if _, ok := want[key]; !ok {
			t.Errorf("unexpected requirement %q", key)
			continue
		}
		want[key] = true
	}
	for key, seen := range want {
		if !seen {
			t.Errorf("missing requirement %q", key)
		}
	}
}

// TestKindsAreFrozenStrings pins the §7.3 kind names — playbooks and
// fleet consumers match on these exact strings.
func TestKindsAreFrozenStrings(t *testing.T) {
	t.Parallel()
	frozen := map[string]string{
		KindSyncFailed:      "ingress.sync_failed",
		KindTranslateFailed: "ingress.translate_failed",
		KindNEGFailed:       "ingress.neg_failed",
	}
	for got, want := range frozen {
		if got != want {
			t.Errorf("kind %q, want %q (frozen)", got, want)
		}
	}
	// The dedup reasons (kind suffixes) map to themselves — no
	// cross-source dedup family.
	for _, reason := range []string{"sync_failed", "translate_failed", "neg_failed"} {
		if engine.CanonicalReason(reason) != reason {
			t.Errorf("CanonicalReason(%q) = %q, want identity", reason, engine.CanonicalReason(reason))
		}
	}
}

// TestOnEvent_WarningSyncOnIngress: the headline path — a Warning
// `Sync` on an Ingress becomes ingress.sync_failed at warning, keyed
// by the Ingress's UID with dedup reason "sync_failed", raw reason
// and message preserved.
func TestOnEvent_WarningSyncOnIngress(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset())
	var got []engine.Signal
	s.emit = func(sig engine.Signal) { got = append(got, sig) }
	s.arm()

	ev := gceEvent(corev1.EventTypeWarning, "Ingress", "Sync",
		"Error syncing to GCP: error running load balancer syncing routine: googleapi: Error 403: QUOTA_EXCEEDED")
	ev.Count = 4
	s.onEvent(ev)

	if len(got) != 1 {
		t.Fatalf("emitted %d signals, want 1", len(got))
	}
	sig := got[0]
	if sig.Kind != KindSyncFailed || sig.Severity != engine.SeverityWarning {
		t.Errorf("Warning Sync → (%s, %s), want (ingress.sync_failed, warning)", sig.Kind, sig.Severity)
	}
	if sig.Key.UID != "uid-web" || sig.Key.Reason != "sync_failed" {
		t.Errorf("key = %+v, want the Ingress UID + reason sync_failed", sig.Key)
	}
	if sig.Namespace != "shop" || sig.KindOfObject != "Ingress" || sig.Name != "web" {
		t.Errorf("identity = %s/%s/%s, want shop/Ingress/web (from the involved object)", sig.Namespace, sig.KindOfObject, sig.Name)
	}
	if !strings.HasPrefix(sig.Message, "Sync: Error syncing to GCP:") {
		t.Errorf("message %q must carry the raw reason and message", sig.Message)
	}
	if sig.Count != 4 {
		t.Errorf("Count = %d, want the event's own repeat count 4", sig.Count)
	}
}

// TestOnEvent_NormalSyncDropped is THE load-bearing test — the reason
// this is a source-owned filter and not a k8s-events allow-list
// entry: ingress-gce uses reason `Sync` for BOTH Normal housekeeping
// ("Scheduled for sync", "ForwardingRule … deleted") and Warning
// failures, and the reactive allow-list path carries no event type.
// A Normal `Sync` must never emit, or every routine sync becomes an
// incident.
func TestOnEvent_NormalSyncDropped(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset())
	var got []engine.Signal
	s.emit = func(sig engine.Signal) { got = append(got, sig) }
	s.arm()

	s.onEvent(gceEvent(corev1.EventTypeNormal, "Ingress", "Sync", "Scheduled for sync"))
	s.onEvent(gceEvent(corev1.EventTypeNormal, "Ingress", "Sync", "ForwardingRule \"k8s2-fr-x\" deleted"))
	if len(got) != 0 {
		t.Fatalf("Normal Sync emitted %d signals, want 0 (the Normal/Warning `Sync` collision)", len(got))
	}
}

// TestOnEvent_TranslateOnIngress: Warning `Translate` → translate_failed.
func TestOnEvent_TranslateOnIngress(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset())
	var got []engine.Signal
	s.emit = func(sig engine.Signal) { got = append(got, sig) }
	s.arm()

	s.onEvent(gceEvent(corev1.EventTypeWarning, "Ingress", "Translate",
		"Translation failed: invalid ingress spec: service \"shop/missing\" does not exist"))
	if len(got) != 1 || got[0].Kind != KindTranslateFailed || got[0].Key.Reason != "translate_failed" {
		t.Fatalf("Warning Translate → %+v, want 1 ingress.translate_failed", got)
	}
}

// TestOnEvent_KindGating: the reason table is scoped per involved
// object's kind — a Warning `Sync` on anything but an Ingress, or a
// NEG reason on anything but a Service, is not ours.
func TestOnEvent_KindGating(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset())
	var got []engine.Signal
	s.emit = func(sig engine.Signal) { got = append(got, sig) }
	s.arm()

	s.onEvent(gceEvent(corev1.EventTypeWarning, "Service", "Sync", "Error syncing to GCP"))
	s.onEvent(gceEvent(corev1.EventTypeWarning, "Pod", "Sync", "Error syncing to GCP"))
	s.onEvent(gceEvent(corev1.EventTypeWarning, "Ingress", "AttachFailed", "Failed to Attach 2 network endpoint(s)"))
	if len(got) != 0 {
		t.Fatalf("kind-mismatched events emitted %d signals (%v), want 0", len(got), got)
	}
}

// TestOnEvent_NEGReasonsOnService: each NEG-controller failure reason
// on a Service maps to ingress.neg_failed, all under the single
// neg_failed dedup reason so one broken Service is one incident.
func TestOnEvent_NEGReasonsOnService(t *testing.T) {
	t.Parallel()
	reasons := map[string]string{
		"SyncNetworkEndpointGroupFailed": "Failed to sync NEG \"k8s1-x\" (will retry): googleapi: Error 400",
		"AttachFailed":                   "Failed to Attach 2 network endpoint(s) (NEG \"k8s1-x\" in zone \"us-east1-b\")",
		"DetachFailed":                   "Failed to Detach 1 network endpoint(s) (NEG \"k8s1-x\" in zone \"us-east1-b\")",
		"RetryFailed":                    "Failed to retry NEG sync",
	}
	for reason, msg := range reasons {
		s := New(fake.NewSimpleClientset())
		var got []engine.Signal
		s.emit = func(sig engine.Signal) { got = append(got, sig) }
		s.arm()

		s.onEvent(gceEvent(corev1.EventTypeWarning, "Service", reason, msg))
		if len(got) != 1 {
			t.Fatalf("%s on Service emitted %d signals, want 1", reason, len(got))
		}
		sig := got[0]
		if sig.Kind != KindNEGFailed || sig.Severity != engine.SeverityWarning {
			t.Errorf("%s → (%s, %s), want (ingress.neg_failed, warning)", reason, sig.Kind, sig.Severity)
		}
		if sig.Key.Reason != "neg_failed" {
			t.Errorf("%s dedup reason = %q, want neg_failed (one incident per Service)", reason, sig.Key.Reason)
		}
		if sig.KindOfObject != "Service" {
			t.Errorf("%s object kind = %q, want Service", reason, sig.KindOfObject)
		}
		if !strings.HasPrefix(sig.Message, reason+": ") {
			t.Errorf("%s message %q must lead with the raw reason", reason, sig.Message)
		}
	}
}

// TestOnEvent_UnknownWarningReasonDropped: Warning events outside the
// owned table never emit — GarbageCollection is deliberately out of
// scope (#135 names Sync/Translate/NEG), and other sources' turf
// stays theirs.
func TestOnEvent_UnknownWarningReasonDropped(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset())
	var got []engine.Signal
	s.emit = func(sig engine.Signal) { got = append(got, sig) }
	s.arm()

	s.onEvent(gceEvent(corev1.EventTypeWarning, "Ingress", "GarbageCollection", "Error during garbage collection"))
	s.onEvent(gceEvent(corev1.EventTypeWarning, "Pod", "CrashLoopBackOff", "back-off restarting container")) // k8s-events' turf
	s.onEvent(gceEvent(corev1.EventTypeWarning, "Pod", "FailedScheduling", "0/3 nodes are available"))       // ditto
	if len(got) != 0 {
		t.Fatalf("unknown reasons emitted %d signals (%v), want 0", len(got), got)
	}
}

// TestOnEvent_PreArmDropped: nothing emits before the informer cache
// syncs — the initial LIST is history, not observation.
func TestOnEvent_PreArmDropped(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset())
	var got []engine.Signal
	s.emit = func(sig engine.Signal) { got = append(got, sig) }

	s.onEvent(gceEvent(corev1.EventTypeWarning, "Ingress", "Sync", "Error syncing to GCP: quota"))
	if len(got) != 0 {
		t.Fatalf("unarmed source emitted %d signals, want 0", len(got))
	}
	s.arm()
	s.onEvent(gceEvent(corev1.EventTypeWarning, "Ingress", "Sync", "Error syncing to GCP: quota"))
	if len(got) != 1 {
		t.Fatalf("armed source emitted %d signals, want 1", len(got))
	}
}

// TestRun_ArmAfterSync is the integration cut of the arming rule: a
// Warning Sync event present BEFORE Run (the informer's initial LIST
// — up to an hour of stale history) never emits; the same shape
// arriving after sync does.
func TestRun_ArmAfterSync(t *testing.T) {
	t.Parallel()
	stale := gceEvent(corev1.EventTypeWarning, "Ingress", "Sync", "Error syncing to GCP: stale history")
	stale.Name = "stale.evt"
	client := fake.NewSimpleClientset(stale)

	s := New(client)
	sigs := make(chan engine.Signal, 16)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, func(sig engine.Signal) { sigs <- sig }) }()

	// Wait for arming (sync completed).
	waitFor(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.armed })

	live := gceEvent(corev1.EventTypeWarning, "Ingress", "Sync", "Error syncing to GCP: live failure")
	live.Name = "live.evt"
	if _, err := client.CoreV1().Events("shop").Create(ctx, live, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	select {
	case sig := <-sigs:
		if sig.Kind != KindSyncFailed || !strings.Contains(sig.Message, "live failure") {
			t.Fatalf("got %+v, want the LIVE ingress.sync_failed", sig)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("live Warning Sync never emitted")
	}
	select {
	case sig := <-sigs:
		t.Fatalf("extra signal %+v — the pre-sync event leaked through arming", sig)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned error on clean shutdown: %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition never became true within 5s")
}
