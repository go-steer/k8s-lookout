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
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// capturingSink records every payload delivered so a test can inspect
// the exact bytes that would go on the wire.
type capturingSink struct {
	opened   []any
	appended []any
}

func (c *capturingSink) OpenIncident(_ context.Context, payload any) (string, error) {
	c.opened = append(c.opened, payload)
	return "sess-fit", nil
}

func (c *capturingSink) Append(_ context.Context, _ string, payload any) error {
	c.appended = append(c.appended, payload)
	return nil
}

var _ inject.Sink = (*capturingSink)(nil)

// TestOpenSession_FitsOversizedPayload is the issue #198 regression: an
// incident whose enrichment bundle pushes the payload past the daemon's
// per-inject ceiling must be shrunk to fit BEFORE the open, so the
// session opens with (trimmed) context instead of the daemon 400ing the
// whole inject and leaving an empty session.
func TestOpenSession_FitsOversizedPayload(t *testing.T) {
	sink := &capturingSink{}
	d := &dispatcher{
		injector:       sink,
		metrics:        newMetrics(),
		injectMaxBytes: inject.MaxInjectBytes,
	}

	p := inject.Payload{
		Kind:         inject.KindEvent,
		Reason:       "CrashLoopBackOff",
		Namespace:    "kube-system",
		Name:         "metrics-server-v1.35.1-578bff4857-qt6v4",
		KindOfObject: "Pod",
		UID:          "019fec9d-57b9-773d-a698-610c1749a808",
		Message:      "Back-off restarting failed container metrics-server",
		Fingerprint:  "sha256:1f4e6a7b8c9d0e1f2a3b4c5d6e7f8091",
		Cluster:      "ap-ap-deploy-test",
		Enrichment:   &inject.PayloadEnrichment{Bundle: strings.Repeat("section=logs line=oomkilled ", 1500)},
	}
	if inject.WireSize(p) <= inject.MaxInjectBytes {
		t.Fatalf("setup: payload should exceed the ceiling, got WireSize=%d", inject.WireSize(p))
	}

	sid, err, ok := d.openSession(context.Background(), p, p.Reason)
	if !ok || err != nil || sid == "" {
		t.Fatalf("openSession: sid=%q err=%v ok=%v", sid, err, ok)
	}
	if len(sink.opened) != 1 {
		t.Fatalf("sink saw %d opens, want 1", len(sink.opened))
	}

	got, isPayload := sink.opened[0].(inject.Payload)
	if !isPayload {
		t.Fatalf("delivered payload was %T, want inject.Payload", sink.opened[0])
	}
	if inject.WireSize(got) > inject.MaxInjectBytes {
		t.Errorf("delivered payload still over the ceiling: %d > %d", inject.WireSize(got), inject.MaxInjectBytes)
	}
	if got.Enrichment != nil {
		t.Errorf("enrichment not dropped before delivery")
	}
	// Identity must survive so the shrunk incident still routes + dedups.
	if got.Reason != p.Reason || got.UID != p.UID || got.Fingerprint != p.Fingerprint || got.Name != p.Name {
		t.Errorf("fit dropped identity on the wire: %+v", got)
	}
	if v := testutil.ToFloat64(d.metrics.injectShrinks.WithLabelValues("enrichment")); v != 1 {
		t.Errorf("injectShrinks{shed=enrichment} = %v, want 1", v)
	}
}

// TestOpenSession_NoShrinkWhenUnderCeiling proves the guard is inert on
// a normal payload: no mutation, no metric — the frozen wire is
// untouched for the common case.
func TestOpenSession_NoShrinkWhenUnderCeiling(t *testing.T) {
	sink := &capturingSink{}
	d := &dispatcher{
		injector:       sink,
		metrics:        newMetrics(),
		injectMaxBytes: inject.MaxInjectBytes,
	}
	p := inject.Payload{
		Kind:      inject.KindEvent,
		Reason:    "CrashLoopBackOff",
		Namespace: "kube-system",
		Name:      "metrics-server",
		UID:       "uid-1",
		Message:   "Back-off restarting failed container",
		Enrichment: &inject.PayloadEnrichment{
			Bundle: "section=spec container=metrics-server restarts=3",
		},
	}
	if _, err, ok := d.openSession(context.Background(), p, p.Reason); !ok || err != nil {
		t.Fatalf("openSession: err=%v ok=%v", err, ok)
	}
	got := sink.opened[0].(inject.Payload)
	if got.Enrichment == nil {
		t.Errorf("enrichment dropped on a payload that fit")
	}
	if v := testutil.ToFloat64(d.metrics.injectShrinks.WithLabelValues("enrichment")); v != 0 {
		t.Errorf("injectShrinks incremented on a payload that fit: %v", v)
	}
}
