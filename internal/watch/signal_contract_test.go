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

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// TestKindConstantsAlignedWithWireContract pins pkg/engine's signal
// kind constants (§7.3) to pkg/inject's frozen wire constants. The
// two packages declare them independently — engine so sources don't
// import the HTTP client package for a string, inject because it owns
// the frozen payload — and this test is what keeps them one value.
func TestKindConstantsAlignedWithWireContract(t *testing.T) {
	t.Parallel()
	if engine.KindK8sEvent != inject.KindEvent {
		t.Errorf("engine.KindK8sEvent (%q) != inject.KindEvent (%q)", engine.KindK8sEvent, inject.KindEvent)
	}
	if engine.KindK8sEventFollowup != inject.KindFollowup {
		t.Errorf("engine.KindK8sEventFollowup (%q) != inject.KindFollowup (%q)", engine.KindK8sEventFollowup, inject.KindFollowup)
	}
}

// TestDispatchSignal_SourcePathWireShapeFrozen is the Signal-engine
// companion to TestDispatcher_ExactInjectPayloadWireShape: it drives
// the SAME incident through DispatchSignal the way a pkg/sources
// source emits it (kind/source/severity set; cluster, zone, and
// fingerprint left for the pipeline to stamp) and asserts the exact
// bytes the M0 test pins. Together they prove the TriageEvent shim
// and the Signal path are one wire contract: the Signal-only fields
// must NOT leak into a kind=k8s-event payload.
func TestDispatchSignal_SourcePathWireShapeFrozen(t *testing.T) {
	t.Parallel()
	base, injects, _ := newFakeDaemon(t)
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{
		filter:    engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1, 0)),
		dedup:     dedup,
		injector:  inj,
		metrics:   newMetrics(),
		cluster:   "prod-us-central1",
		mode:      "shared",
		targetSid: "sess-shared",
	}
	disp.DispatchSignal(context.Background(), engine.Signal{
		Kind:     engine.KindK8sEvent,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:           engine.EventKey{UID: "abc-123", Reason: "CrashLoopBackOff"},
			Namespace:     "checkout",
			KindOfObject:  "Pod",
			Name:          "checkout-svc-7b9d-x4kzq",
			Container:     "spec.containers{server}",
			Message:       "Back-off restarting failed container",
			FirstSeen:     time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC),
			LastSeen:      time.Date(2026, 7, 24, 10, 5, 0, 0, time.UTC),
			ControllerRef: "ReplicaSet/checkout-svc-7b9d",
			Node:          "node-1",
			Labels:        map[string]string{"team": "checkout"},
			Count:         1,
			Type:          "Warning",
		},
	})
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects))
	}
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte((*injects)[0]), &envelope); err != nil {
		t.Fatalf("captured body isn't the inject envelope: %v (body=%q)", err, (*injects)[0])
	}
	// Byte-for-byte the `want` of TestDispatcher_ExactInjectPayloadWireShape.
	want := `{"kind":"k8s-event","reason":"CrashLoopBackOff","namespace":"checkout","kind_of_object":"Pod","name":"checkout-svc-7b9d-x4kzq","container":"spec.containers{server}","uid":"abc-123","message":"Back-off restarting failed container","count":1,"first_seen":"2026-07-24T10:00:00Z","last_seen":"2026-07-24T10:05:00Z","cluster":"prod-us-central1","context":{"controller_ref":"ReplicaSet/checkout-svc-7b9d","node":"node-1","labels":{"team":"checkout"}},"type":"Warning"}`
	if envelope.Message != want {
		t.Errorf("Signal-path inject payload drifted from the frozen wire shape:\n got: %s\nwant: %s", envelope.Message, want)
	}
}

// TestDispatchSignal_StampsPipelineFields verifies the stamping
// contract of §7.2/§8: sources leave Cluster/Source/Fingerprint
// empty; the dispatcher fills cluster + source and computes the
// fingerprint from (kind, reason-class, object-class, zone) — with
// the reason CANONICALIZED, so both variants of one failure family
// fingerprint identically across clusters.
func TestDispatchSignal_StampsPipelineFields(t *testing.T) {
	t.Parallel()
	base, injects, _ := newFakeDaemon(t)
	inj, _ := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "t", AssertedCaller: "a@b"})
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{
		filter:    engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1, 0)),
		dedup:     dedup,
		injector:  inj,
		metrics:   newMetrics(),
		cluster:   "prod-east",
		mode:      "shared",
		targetSid: "sess-shared",
	}
	// ErrImagePull canonicalizes to ImagePullBackOff for both the
	// dedup key and the fingerprint's reason-class.
	disp.DispatchSignal(context.Background(), engine.Signal{
		Kind:     engine.KindK8sEvent,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "u9", Reason: "ErrImagePull"},
			Namespace:    "default",
			KindOfObject: "Pod",
			Name:         "pod-9",
			Count:        1,
		},
	})
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects))
	}
	// The stamped cluster reaches the wire; fingerprint/severity do
	// not (frozen k8s-event payload), so assert the fingerprint via
	// the pure function the dispatcher uses.
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte((*injects)[0]), &envelope); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	var payload inject.Payload
	if err := json.Unmarshal([]byte(envelope.Message), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.Cluster != "prod-east" {
		t.Errorf("Cluster = %q, want stamped prod-east", payload.Cluster)
	}
	if payload.Reason != "ErrImagePull" {
		t.Errorf("wire Reason = %q — canonicalization is dedup/fingerprint-only, the wire keeps the original", payload.Reason)
	}
	want := engine.Fingerprint("k8s-event", "ImagePullBackOff", "Pod", "")
	if got := engine.Fingerprint(engine.KindK8sEvent, engine.CanonicalReason("ErrImagePull"), "Pod", ""); got != want {
		t.Errorf("fingerprint recipe drifted: %s vs %s", got, want)
	}
}
