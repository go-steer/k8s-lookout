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

package k8sevents

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// signalRecorder collects everything the source emits. Thread-safe;
// the informer delivers asynchronously.
type signalRecorder struct {
	mu      sync.Mutex
	signals []sources.Signal
	// first is closed after the first emit — tests wait on it to
	// avoid racing the informer's async delivery.
	firstOnce sync.Once
	first     chan struct{}
}

func newSignalRecorder() *signalRecorder {
	return &signalRecorder{first: make(chan struct{})}
}

func (r *signalRecorder) emit(sig sources.Signal) {
	r.mu.Lock()
	r.signals = append(r.signals, sig)
	r.mu.Unlock()
	r.firstOnce.Do(func() { close(r.first) })
}

func (r *signalRecorder) snapshot() []sources.Signal {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]sources.Signal, len(r.signals))
	copy(out, r.signals)
	return out
}

// TestSource_EmitsSignalsFromInformer is the M0 watcher harness
// (fake clientset seeded with an Event; informer initial list
// surfaces it) retargeted at the Source interface: the same event
// must now come out as a kind=k8s-event Signal.
func TestSource_EmitsSignalsFromInformer(t *testing.T) {
	t.Parallel()
	seed := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "checkout-svc.evt",
			Namespace: "checkout",
		},
		Reason:  "CrashLoopBackOff",
		Message: "Back-off restarting failed container",
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Name:      "checkout-svc-7b9d-x4kzq",
			Namespace: "checkout",
			UID:       "abc-123",
		},
		FirstTimestamp: metav1.Time{Time: time.Now().Add(-2 * time.Minute)},
		LastTimestamp:  metav1.Time{Time: time.Now()},
		Count:          3,
	}
	client := fake.NewClientset(seed)

	rec := newSignalRecorder()
	src := New(client, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- src.Run(ctx, rec.emit) }()

	// Wait for the informer's cache to sync + first emit.
	select {
	case <-rec.first:
	case <-ctx.Done():
		t.Fatal("no signal within timeout")
	}
	// Let Run finish. Its error is deliberately not asserted: the
	// first emit can arrive while WaitForCacheSync is still polling,
	// so this cancel may interrupt the sync and surface as a startup
	// error — same race the M0 watcher harness ignored.
	cancel()
	<-done

	signals := rec.snapshot()
	if len(signals) == 0 {
		t.Fatal("expected at least one emitted signal")
	}
	got := signals[0]
	if got.Kind != engine.KindK8sEvent {
		t.Errorf("Kind = %q, want %q", got.Kind, engine.KindK8sEvent)
	}
	if got.Source != engine.SourceSentinel {
		t.Errorf("Source = %q, want %q", got.Source, engine.SourceSentinel)
	}
	if got.Severity != engine.SeverityCritical {
		t.Errorf("Severity = %q, want critical (§7.7 default for k8s-event)", got.Severity)
	}
	if got.Key.Reason != "CrashLoopBackOff" {
		t.Errorf("Reason = %q, want CrashLoopBackOff", got.Key.Reason)
	}
	if got.Key.UID != "abc-123" {
		t.Errorf("UID = %q, want abc-123", got.Key.UID)
	}
	if got.Namespace != "checkout" {
		t.Errorf("Namespace = %q, want checkout", got.Namespace)
	}
	if got.Count != 3 {
		t.Errorf("Count = %d, want 3", got.Count)
	}
	// Deployment identity + fingerprint are the pipeline's to stamp,
	// never the source's (§7.2).
	if got.Cluster != "" || got.Zone != "" || got.Fingerprint != "" {
		t.Errorf("source must leave Cluster/Zone/Fingerprint empty; got %q/%q/%q",
			got.Cluster, got.Zone, got.Fingerprint)
	}
}

// TestSource_InterfaceMetadata pins the §7.2/§11 declarations: the
// stable name from the design's source table, cluster scope, and the
// RBAC the startup probe checks (which must stay in sync with
// deploy/12-clusterrole-watcher.yaml).
func TestSource_InterfaceMetadata(t *testing.T) {
	t.Parallel()
	src := New(fake.NewClientset(), 0)
	// Compile-time: Source satisfies the interface + declares access.
	var _ sources.Source = src
	var _ sources.AccessDeclarer = src

	if src.Name() != "k8s-events" {
		t.Errorf("Name() = %q, want k8s-events (stable, used in schema + config)", src.Name())
	}
	if src.Scope() != sources.ScopeCluster {
		t.Errorf("Scope() = %v, want cluster", src.Scope())
	}
	reqs := src.RequiredAccess()
	want := map[string]bool{"list events cluster-wide": false, "watch events cluster-wide": false}
	for _, r := range reqs {
		if _, ok := want[r.String()]; !ok {
			t.Errorf("unexpected requirement %q", r)
			continue
		}
		want[r.String()] = true
	}
	for req, seen := range want {
		if !seen {
			t.Errorf("missing requirement %q", req)
		}
	}
}

func TestToTriageEvent_PopulatesAllFields(t *testing.T) {
	t.Parallel()
	// Direct unit test for the Event → TriageEvent conversion so
	// we don't need the informer running to cover edge cases.
	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test.evt",
			Namespace: "default",
			Labels:    map[string]string{"team": "checkout"},
		},
		Reason:  "OOMKilled",
		Message: "Container was OOMKilled",
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Name:      "test-pod",
			Namespace: "default",
			UID:       "uid-oom",
			FieldPath: "spec.containers{app}",
		},
		Source: corev1.EventSource{
			Host: "node-1",
		},
		FirstTimestamp: metav1.Time{Time: time.Unix(1000, 0)},
		LastTimestamp:  metav1.Time{Time: time.Unix(2000, 0)},
		Count:          7,
		Type:           corev1.EventTypeWarning,
	}
	got := toTriageEvent(ev)
	if got.Key.Reason != "OOMKilled" {
		t.Errorf("Reason = %q", got.Key.Reason)
	}
	if got.Key.UID != "uid-oom" {
		t.Errorf("UID = %q", got.Key.UID)
	}
	if got.Namespace != "default" {
		t.Errorf("Namespace = %q", got.Namespace)
	}
	if got.KindOfObject != "Pod" {
		t.Errorf("KindOfObject = %q", got.KindOfObject)
	}
	if got.Container != "spec.containers{app}" {
		t.Errorf("Container = %q", got.Container)
	}
	if got.Node != "node-1" {
		t.Errorf("Node = %q", got.Node)
	}
	if got.Labels["team"] != "checkout" {
		t.Errorf("Labels[team] = %q", got.Labels["team"])
	}
	if got.Count != 7 {
		t.Errorf("Count = %d", got.Count)
	}
	if got.FirstSeen.Unix() != 1000 || got.LastSeen.Unix() != 2000 {
		t.Errorf("Timestamps: FirstSeen=%d LastSeen=%d", got.FirstSeen.Unix(), got.LastSeen.Unix())
	}
	if got.Type != "Warning" {
		t.Errorf("Type = %q, want Warning (Event.Type must ride into the payload's \"type\" field)", got.Type)
	}
}

func TestTruncateMessage_LongPayload(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("x", 3000)
	got := truncateMessage(long)
	if len(got) > 2200 { // 2048 + " [truncated by ...]" suffix ~ 30 chars
		t.Errorf("truncated len = %d, expected <= ~2100", len(got))
	}
	// The marker text is frozen wire-message content from the M0
	// watcher — it must survive the source refactor unchanged.
	if !strings.Contains(got, "truncated by k8s-event-watcher") {
		t.Errorf("truncation marker missing from truncated output")
	}
}

func TestTruncateMessage_ShortPayloadUnchanged(t *testing.T) {
	t.Parallel()
	msg := "small message"
	if got := truncateMessage(msg); got != msg {
		t.Errorf("short message should pass through unchanged; got %q", got)
	}
}
