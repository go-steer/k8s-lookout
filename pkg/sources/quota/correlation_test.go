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

package quota

import (
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// The §10.3 correlation seam, tested with scripted signals at the
// engine level: quota_forecast (leading) and quota_blocked
// (reactive) share the QuotaExhausted dedup family, and — keyed on
// the same canonical quota UID — the dedup collapse IS the
// claim-and-attach flow: whichever fires first opens the session,
// the other routes to it as a followup. One diagnosed incident, not
// two.

func TestCanonicalFamily_QuotaExhausted(t *testing.T) {
	t.Parallel()
	if got := engine.CanonicalReason(Reason); got != "QuotaExhausted" {
		t.Errorf("CanonicalReason(quota_forecast) = %q, want QuotaExhausted", got)
	}
	if got := engine.CanonicalReason("quota_blocked"); got != "QuotaExhausted" {
		t.Errorf("CanonicalReason(quota_blocked) = %q, want QuotaExhausted (capacity's reactive half)", got)
	}
	// Self-canonical stays self-canonical: the family is append-only.
	if got := engine.CanonicalReason("QuotaExhausted"); got != "QuotaExhausted" {
		t.Errorf("CanonicalReason(QuotaExhausted) = %q, want itself", got)
	}
}

// TestJoinedFlow_ForecastThenScaleupFailure: the leading forecast
// opens the session; the later GCE_QUOTA_EXCEEDED scaleup failure
// (same quota UID, quota_blocked reason) dedups into it.
func TestJoinedFlow_ForecastThenScaleupFailure(t *testing.T) {
	t.Parallel()
	dedup, err := engine.NewDedupCache(5*time.Minute, "")
	if err != nil {
		t.Fatal(err)
	}
	uid := UID("CPUS", "us-east1")
	t0 := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)

	forecast := engine.EventKey{UID: uid, Reason: Reason}
	if r := dedup.Observe(forecast, t0); r.Kind != engine.DedupNewIncident {
		t.Fatalf("forecast observe = %+v, want a new incident", r)
	}
	dedup.BindSession(forecast, "sid-quota-1")

	blocked := engine.EventKey{UID: uid, Reason: "quota_blocked"}
	r := dedup.Observe(blocked, t0.Add(2*time.Minute))
	if r.Kind != engine.DedupDuplicate {
		t.Fatalf("quota_blocked observe = %+v, want a duplicate (ONE diagnosed incident, §10.3)", r)
	}
	if r.SessionID != "sid-quota-1" {
		t.Errorf("quota_blocked routed to session %q, want the open quota session sid-quota-1", r.SessionID)
	}
}

// TestJoinedFlow_ScaleupFailureFirst: order independence — when the
// reactive failure lands first (no forecast yet), it opens the
// session and the forecast attaches.
func TestJoinedFlow_ScaleupFailureFirst(t *testing.T) {
	t.Parallel()
	dedup, err := engine.NewDedupCache(5*time.Minute, "")
	if err != nil {
		t.Fatal(err)
	}
	uid := UID("CPUS", "us-east1")
	t0 := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)

	blocked := engine.EventKey{UID: uid, Reason: "quota_blocked"}
	if r := dedup.Observe(blocked, t0); r.Kind != engine.DedupNewIncident {
		t.Fatalf("quota_blocked observe = %+v, want a new incident", r)
	}
	dedup.BindSession(blocked, "sid-blocked-1")

	forecast := engine.EventKey{UID: uid, Reason: Reason}
	r := dedup.Observe(forecast, t0.Add(time.Minute))
	if r.Kind != engine.DedupDuplicate || r.SessionID != "sid-blocked-1" {
		t.Fatalf("forecast observe = %+v, want a duplicate routed to sid-blocked-1", r)
	}
}

// TestNoJoinAcrossQuotas: different quotas (or the nodegroup
// fallback key) must NOT collapse — the family shares a reason
// class, the join requires the same quota UID.
func TestNoJoinAcrossQuotas(t *testing.T) {
	t.Parallel()
	dedup, err := engine.NewDedupCache(5*time.Minute, "")
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	if r := dedup.Observe(engine.EventKey{UID: UID("CPUS", "us-east1"), Reason: Reason}, t0); r.Kind != engine.DedupNewIncident {
		t.Fatal("first quota must open")
	}
	if r := dedup.Observe(engine.EventKey{UID: UID("CPUS", "us-west1"), Reason: "quota_blocked"}, t0.Add(time.Second)); r.Kind != engine.DedupNewIncident {
		t.Error("another region's quota_blocked must be its own incident")
	}
	if r := dedup.Observe(engine.EventKey{UID: "nodegroup:mig-a", Reason: "quota_blocked"}, t0.Add(2*time.Second)); r.Kind != engine.DedupNewIncident {
		t.Error("the nodegroup-keyed fallback must not join a quota session")
	}
}

func TestUIDFromDecisionMessage(t *testing.T) {
	t.Parallel()
	cases := []struct {
		msg  string
		want string
		ok   bool
	}{
		{"Instance creation failed: Quota 'CPUS' exceeded. Limit: 2000.0 in region us-east1.", "quota:CPUS/us-east1", true},
		{"scale-up result error scale.up.error.quota.exceeded — Quota 'N2_CPUS' exceeded. Limit: 500.0 in region europe-west4.", "quota:N2_CPUS/europe-west4", true},
		{"Quota 'BACKEND_SERVICES' exceeded. Limit: 9.0 globally.", "quota:BACKEND_SERVICES/global", true},
		// Name without a scope: conservative no-join (a partial match
		// must not attach to the wrong region's session).
		{"Quota 'CPUS' exceeded.", "", false},
		{"scale-up result error scale.up.error.quota.exceeded (parameters: mig-a)", "", false},
		{"", "", false},
	}
	for _, tc := range cases {
		got, ok := UIDFromDecisionMessage(tc.msg)
		if got != tc.want || ok != tc.ok {
			t.Errorf("UIDFromDecisionMessage(%q) = (%q, %v), want (%q, %v)", tc.msg, got, ok, tc.want, tc.ok)
		}
	}
}
