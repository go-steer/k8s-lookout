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

// §9.4 regression evidence (M4 drill observation 3): a steady symptom
// stream never leaves the dedup window, so a downgraded incident's
// only visible effect used to be the per-signal downgrade log until
// the loop paused. Now, when the window count reaches
// --triage-regress-factor × the count at downgrade time, ONE
// schema-stable kind=triage.regressed followup lands in the bound
// session. Evidence only — the routing decision stays with the open
// record (no auto-re-page; docs/triage-status-write-design.md).

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/memory"
)

// refreshTriage forces the §9.4 routing cache to reload on the next
// signal (tests write records mid-incident, inside the 30s refresh).
func refreshTriage(d *dispatcher) {
	d.triage.mu.Lock()
	d.triage.loaded = time.Time{}
	d.triage.mu.Unlock()
}

// dupAt returns sig with LastSeen advanced by delta — genuinely new
// kubelet activity inside the window (dedup case 3).
func dupAt(sig engine.Signal, delta time.Duration) engine.Signal {
	out := sig
	out.LastSeen = sig.LastSeen.Add(delta)
	return out
}

// TestTriageRegressedFiresOnceAtFactor drives the M4 scenario: the
// incident pages and gets a session; the agent downgrades it; the
// steady stream keeps deduping. The count at the FIRST downgraded
// duplicate is the baseline; at factor × baseline exactly one
// triage.regressed followup lands in the bound session, and further
// duplicates stay quiet.
func TestTriageRegressedFiresOnceAtFactor(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, s := triageDispatcher(t, base) // regress factor 3
	ctx := context.Background()

	// (1) The incident pages before any triage: session bound.
	sig := crashLoopSignal()
	d.DispatchSignal(ctx, sig)
	if len(*injects) != 1 {
		t.Fatalf("opener produced %d injects, want 1", len(*injects))
	}
	sid := (*injects)[0].SessionID

	// (2) The agent triages and downgrades mid-incident.
	writeRecord(t, s, memory.TriageStatusRecord{
		Fingerprint:      stampedFingerprint(sig),
		ResourceKey:      memory.ResourceKey("Pod", "checkout", "checkout-svc-7b9d-x4kzq"),
		Session:          sid,
		Status:           memory.StatusTriaged,
		SeverityOverride: "warning",
	})
	refreshTriage(d)

	// (3) First downgraded duplicate: count=2 becomes the baseline.
	// Threshold is 3× → count 6.
	d.DispatchSignal(ctx, dupAt(sig, time.Minute)) // count=2, baseline
	for i := 2; i <= 4; i++ {                      // counts 3,4,5 — below threshold
		d.DispatchSignal(ctx, dupAt(sig, time.Duration(i)*time.Minute))
	}
	if len(*injects) != 1 {
		t.Fatalf("below-threshold duplicates injected: %d injects, want 1", len(*injects))
	}
	d.DispatchSignal(ctx, dupAt(sig, 5*time.Minute)) // count=6 = 3×2 → evidence
	if len(*injects) != 2 {
		t.Fatalf("threshold crossing produced %d injects, want 2 (opener + triage.regressed)", len(*injects))
	}
	fu := (*injects)[1]
	if fu.SessionID != sid {
		t.Errorf("triage.regressed routed to %q, want the bound session %q", fu.SessionID, sid)
	}
	var payload inject.TriageRegressedPayload
	if err := json.Unmarshal([]byte(messageOf(t, fu.Body)), &payload); err != nil {
		t.Fatalf("triage.regressed payload: %v", err)
	}
	if payload.Kind != inject.KindTriageRegressed || payload.BaselineCount != 2 || payload.Count != 6 || payload.Factor != 3 {
		t.Errorf("payload = kind=%s baseline=%d count=%d factor=%d, want triage.regressed 2 6 3",
			payload.Kind, payload.BaselineCount, payload.Count, payload.Factor)
	}
	if payload.TriageStatus != "triaged" || payload.SeverityOverride != "warning" || payload.TriageSession != sid {
		t.Errorf("payload record echo = %+v, want the open record's status/override/session", payload)
	}

	// (4) Once per window: further duplicates stay quiet.
	d.DispatchSignal(ctx, dupAt(sig, 6*time.Minute))
	d.DispatchSignal(ctx, dupAt(sig, 7*time.Minute))
	if len(*injects) != 2 {
		t.Fatalf("post-evidence duplicates re-injected: %d injects, want 2", len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.triageRegressed); got != 1 {
		t.Errorf("triage_regressed_total = %v, want 1", got)
	}
}

// TestTriageRegressed_ExactWireShape pins the kind=triage.regressed
// payload byte-exact (§9.3 discipline: schema-stable structured
// injects, never prose — harvesters and playbooks parse fields).
func TestTriageRegressed_ExactWireShape(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, s := triageDispatcher(t, base)
	ctx := context.Background()

	sig := crashLoopSignal()
	d.DispatchSignal(ctx, sig)
	sid := (*injects)[0].SessionID
	writeRecord(t, s, memory.TriageStatusRecord{
		Fingerprint:      stampedFingerprint(sig),
		ResourceKey:      memory.ResourceKey("Pod", "checkout", "checkout-svc-7b9d-x4kzq"),
		Session:          sid,
		Status:           memory.StatusTriaged,
		SeverityOverride: "warning",
	})
	refreshTriage(d)
	d.DispatchSignal(ctx, dupAt(sig, time.Minute)) // baseline: count=2
	d.DispatchSignal(ctx, dupAt(sig, 2*time.Minute))
	d.DispatchSignal(ctx, dupAt(sig, 3*time.Minute))
	d.DispatchSignal(ctx, dupAt(sig, 4*time.Minute))
	d.DispatchSignal(ctx, dupAt(sig, 5*time.Minute)) // count=6 → fires
	if len(*injects) != 2 {
		t.Fatalf("expected the evidence inject, got %d injects", len(*injects))
	}

	want := `{"kind":"triage.regressed","reason":"CrashLoopBackOff","namespace":"checkout","kind_of_object":"Pod","name":"checkout-svc-7b9d-x4kzq","container":"spec.containers{server}","uid":"abc-123",` +
		`"fingerprint":"` + stampedFingerprint(sig) + `","cluster":"prod-us-central1",` +
		`"triage_status":"triaged","severity_override":"warning","triage_session":"` + sid + `",` +
		`"baseline_count":2,"count":6,"factor":3,` +
		`"first_seen":"2026-07-24T10:00:00Z","last_seen":"2026-07-24T10:10:00Z",` +
		`"message":"downgraded incident regressed: 6 occurrences this dedup window vs 2 when the warning override was written (3x or more) — override still routing; re-triage via ` + "`lookout triage status`" + ` or escalate",` +
		`"context":{"controller_ref":"ReplicaSet/checkout-svc-7b9d"}}`
	if got := messageOf(t, (*injects)[1].Body); got != want {
		t.Errorf("triage.regressed wire shape drifted:\n got: %s\nwant: %s", got, want)
	}
}

// TestTriageRegressedKindPinned pins the engine/inject constant pair
// to one value, like every cross-cutting kind.
func TestTriageRegressedKindPinned(t *testing.T) {
	t.Parallel()
	if engine.KindTriageRegressed != "triage.regressed" {
		t.Errorf("engine.KindTriageRegressed = %q, want triage.regressed (pinned)", engine.KindTriageRegressed)
	}
	if inject.KindTriageRegressed != engine.KindTriageRegressed {
		t.Errorf("inject.KindTriageRegressed (%q) != engine.KindTriageRegressed (%q)", inject.KindTriageRegressed, engine.KindTriageRegressed)
	}
}
