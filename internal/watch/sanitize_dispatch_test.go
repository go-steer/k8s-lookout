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

// Regression drills for issue #82: DESIGN.md §6.5 promises the
// sanitizer runs on every payload on every surface (CLI, MCP,
// inject), and the M1 exit criterion is "no secret value can reach
// any output surface" — but incidentPayload (and the cross-source
// followup assembly) copy sig.Message and sig.Labels raw into
// inject.Payload, and injectJSON plain-marshals. Only the enrichment
// bundle is sanitized. sig.Message is raw truncated Kubernetes event
// text, so exactly the shapes emit.MaskString is built to catch —
// URL passwords, JWTs, auth-header tokens — reach the per-incident
// agent session unmasked.
//
// These tests capture the bytes the fake daemon receives and assert
// cluster-sourced string fields (message, label values) arrive with
// each planted shape masked EXACTLY as emit.MaskString masks it
// (every expectation is sanity-checked against emit.MaskString in
// the test itself, so the masked forms can never be invented), while
// innocent content passes through byte-identical — the frozen wire
// contract (TestDispatcher_ExactInjectPayloadWireShape) keeps field
// names, shape, and non-secret values unchanged.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// secretShapeCases are the planted MaskString shapes and the exact
// masked forms MaskString produces for them (verified by
// requireMaskedForm before each is used, never assumed).
var secretShapeCases = []struct {
	name    string
	planted string
	masked  string
}{
	{
		name:    "url-password",
		planted: "failed dialing postgres://svc:hunter2@db:5432: connection refused",
		masked:  "failed dialing postgres://svc:[REDACTED]@db:5432: connection refused",
	},
	{
		name:    "bare-jwt",
		planted: "webhook auth failed: token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJsb29rb3V0LXNhIn0.dGVzdC1zaWduYXR1cmU found in request body",
		masked:  "webhook auth failed: token [REDACTED] found in request body",
	},
	{
		name:    "bearer-jwt",
		planted: "webhook rejected: Bearer eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJrOHMtbG9va291dCJ9.c2lnbmF0dXJlLXNpZ25hdHVyZQ",
		masked:  "webhook rejected: Bearer [REDACTED]",
	},
	{
		name:    "basic-auth-header",
		planted: "request rejected: Authorization: Basic dXNlcjpodW50ZXIyLXNlY3JldA== (auth header replayed)",
		masked:  "request rejected: Authorization: Basic [REDACTED] (auth header replayed)",
	},
}

// jwtLabelValue is a JWT that is also a syntactically legal k8s label
// value (base64url + dots, 52 chars) — the realistic way a bearer
// credential ends up in a label. MaskString redacts it whole.
const jwtLabelValue = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkYmcifQ.c2lnbmF0dXJl"

// requireMaskedForm pins the test's expectation to what MaskString
// actually produces: if the sanitizer wouldn't catch the shape (or
// masks it differently), the test is wrong, not the code.
func requireMaskedForm(t *testing.T, planted, want string) {
	t.Helper()
	if got := emit.MaskString(planted); got != want {
		t.Fatalf("test fixture bug: emit.MaskString(%q) = %q, want %q — fix the fixture, not the sanitizer", planted, got, want)
	}
	if want == planted {
		t.Fatalf("test fixture bug: MaskString does not change %q — shape not worth testing", planted)
	}
}

// payloadFields decodes the captured inject envelope into the generic
// payload map.
func payloadFields(t *testing.T, body string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(messageOf(t, body)), &payload); err != nil {
		t.Fatalf("captured payload is not JSON: %v", err)
	}
	return payload
}

// TestDispatch_IncidentPayloadMessageMasked is the issue #82 core:
// a per-incident open whose Message carries a credential shape must
// arrive at the daemon with the shape masked exactly as
// emit.MaskString masks it — the §6.5 contract on the inject surface.
func TestDispatch_IncidentPayloadMessageMasked(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)
	ctx := context.Background()

	for i, tc := range secretShapeCases {
		requireMaskedForm(t, tc.planted, tc.masked)
		sig := crashLoopSignal()
		sig.Key.UID = "uid-secret-" + tc.name // fresh incident per shape
		sig.Message = tc.planted
		d.DispatchSignal(ctx, sig)
		if len(*injects) != i+1 {
			t.Fatalf("%s: dispatch produced %d injects, want %d", tc.name, len(*injects), i+1)
		}
		got, _ := payloadFields(t, (*injects)[i].Body)["message"].(string)
		if got == tc.planted {
			t.Errorf("%s: secret reached the wire UNMASKED — payload message = %q, want %q (§6.5: sanitizer must run on the inject surface; issue #82)",
				tc.name, got, tc.masked)
			continue
		}
		if got != tc.masked {
			t.Errorf("%s: payload message = %q, want the exact MaskString form %q", tc.name, got, tc.masked)
		}
	}
}

// TestDispatch_IncidentPayloadLabelValuesMasked: label VALUES are
// cluster content too — a JWT stuffed into a label must be redacted
// on the wire while innocent labels pass through untouched (only
// values are radioactive; keys and innocent values are triage
// signal).
func TestDispatch_IncidentPayloadLabelValuesMasked(t *testing.T) {
	t.Parallel()
	requireMaskedForm(t, jwtLabelValue, emit.Redacted)
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)

	sig := crashLoopSignal()
	sig.Labels = map[string]string{
		"team":      "checkout",
		"telemetry": jwtLabelValue,
	}
	d.DispatchSignal(context.Background(), sig)
	if len(*injects) != 1 {
		t.Fatalf("dispatch produced %d injects, want 1", len(*injects))
	}
	payload := payloadFields(t, (*injects)[0].Body)
	pctx, _ := payload["context"].(map[string]any)
	if pctx == nil {
		t.Fatalf("payload has no context object: %v", payload)
	}
	labels, _ := pctx["labels"].(map[string]any)
	if labels == nil {
		t.Fatalf("payload context has no labels: %v", pctx)
	}
	if got := labels["telemetry"]; got != emit.Redacted {
		t.Errorf("label value with a JWT reached the wire as %q, want %q (issue #82)", got, emit.Redacted)
	}
	if got := labels["team"]; got != "checkout" {
		t.Errorf("innocent label value changed: %q, want checkout (only secret-shaped values may be touched)", got)
	}
}

// TestDispatch_CrossSourceFollowupMessageMasked covers the second
// assembly site (the cross-source join followup): the
// kind=family.member payload composes its own message from signal
// identity fields and deliberately carries NO free-text from the
// joining signal — a secret-bearing duplicate from another source
// family must not put its raw Message on the wire at all.
func TestDispatch_CrossSourceFollowupMessageMasked(t *testing.T) {
	t.Parallel()
	tc := secretShapeCases[0] // url-password
	requireMaskedForm(t, tc.planted, tc.masked)
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)
	ctx := context.Background()

	// The reactive k8s-event opens the session with an innocent
	// message.
	opener := crashLoopSignal()
	d.DispatchSignal(ctx, opener)
	if len(*injects) != 1 {
		t.Fatalf("opener produced %d injects, want 1", len(*injects))
	}

	// The object-state source's angle on the same incident carries the
	// secret-bearing text: cross-source join → followup Append into
	// the bound session.
	join := restartBurstFor(opener, opener.LastSeen.Add(30*time.Second))
	join.Message = tc.planted
	d.DispatchSignal(ctx, join)
	if len(*injects) != 2 {
		t.Fatalf("cross-source join produced %d injects, want 2 (opener + followup)", len(*injects))
	}
	fu := payloadFields(t, (*injects)[1].Body)
	if kind, _ := fu["kind"].(string); kind != "family.member" {
		t.Fatalf("second inject is not the join followup (kind=%q)", kind)
	}
	if strings.Contains((*injects)[1].Body, tc.planted) {
		t.Errorf("followup: secret reached the wire — family.member composes its message from identity fields and must never copy the joining signal's raw Message (issue #82); body = %q", (*injects)[1].Body)
	}
}

// TestDispatch_InnocentPayloadPassesThroughByteIdentical is the
// control for the frozen wire contract: masking is surgical, so an
// ordinary event message and innocent label values must reach the
// wire byte-for-byte unchanged (field names and shape are pinned by
// TestDispatcher_ExactInjectPayloadWireShape; this pins the VALUES
// against overzealous sanitizing).
func TestDispatch_InnocentPayloadPassesThroughByteIdentical(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d := newRecoveryDispatcher(t, base)

	sig := crashLoopSignal()
	sig.Labels = map[string]string{"team": "checkout"}
	d.DispatchSignal(context.Background(), sig)
	if len(*injects) != 1 {
		t.Fatalf("dispatch produced %d injects, want 1", len(*injects))
	}
	wire := messageOf(t, (*injects)[0].Body)
	for _, want := range []string{
		`"message":"Back-off restarting failed container"`,
		`"labels":{"team":"checkout"}`,
	} {
		if !strings.Contains(wire, want) {
			t.Errorf("innocent payload drifted: wire %s missing byte-identical %s", wire, want)
		}
	}
	if strings.Contains(wire, emit.Redacted) {
		t.Errorf("innocent payload contains %s — sanitizer over-masked: %s", emit.Redacted, wire)
	}
}
