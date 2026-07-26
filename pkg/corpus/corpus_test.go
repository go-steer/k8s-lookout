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

package corpus

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// envelope wraps a payload the way the injector does and renders the
// stub-daemon capture line for it.
func injectLine(t *testing.T, sid string, payload any) string {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	env, err := json.Marshal(map[string]string{"message": string(body)})
	if err != nil {
		t.Fatal(err)
	}
	var k struct {
		Kind string `json:"kind"`
	}
	_ = json.Unmarshal(body, &k)
	return "INJECT sid=" + sid + " kind=" + k.Kind + " token=present body=" + string(env)
}

// TestHarvest_FullTrajectory walks a capture holding one complete
// lifecycle — enriched critical inject, dedup followup, §9.4
// triaged/actioned records, resolved outcome — plus watchboard noise
// and an unrelated unresolved incident, and asserts exactly one
// COMPLETE labeled trajectory comes out.
func TestHarvest_FullTrajectory(t *testing.T) {
	t.Parallel()
	fp := "sha256:e2957792a0b3ad9e29db2051dbc69ff01dfe3a52da8dbb6d1331aa44fe946f8b"
	capture := strings.Join([]string{
		"stub-daemon listening on :7777",
		"SESSION-CREATE sid=stub-sess-0001 caller=lookout token=present",
		injectLine(t, "stub-sess-0001", map[string]any{
			"kind": "k8s-event", "reason": "CrashLoopBackOff", "namespace": "prod",
			"kind_of_object": "Pod", "name": "api-0", "uid": "u1",
			"message": "Back-off restarting failed container", "count": 1,
			"first_seen": "2026-07-26T10:00:00Z", "last_seen": "2026-07-26T10:00:00Z",
			"cluster": "prod-east", "context": map[string]any{},
			"enrichment": map[string]any{"bundle": "kind=bundle.target severity=info"},
		}),
		injectLine(t, "stub-sess-0001", map[string]any{
			"kind": "k8s-event-followup", "reason": "CrashLoopBackOff", "namespace": "prod",
			"kind_of_object": "Pod", "name": "api-0", "uid": "u1", "count": 4,
			"cluster": "prod-east", "context": map[string]any{},
		}),
		// §9.4 records as the incident playbook exported them at each
		// material transition.
		`{"fingerprint":"` + fp + `","resource_key":"Pod/prod/api-0","session":"stub-sess-0001","status":"triaged","root_cause_hypothesis":"bad connection string in checkout-config","severity_override":"warning","updated":"2026-07-26T10:05:00Z"}`,
		`{"fingerprint":"` + fp + `","resource_key":"Pod/prod/api-0","session":"stub-sess-0001","status":"actioned","root_cause_hypothesis":"bad connection string in checkout-config","action":"PR #402 opened; config rollout pending","updated":"2026-07-26T10:09:00Z"}`,
		injectLine(t, "stub-sess-0001", map[string]any{
			"kind": "resolved", "reason": "CrashLoopBackOff", "namespace": "prod",
			"kind_of_object": "Pod", "name": "api-0", "uid": "u1",
			"fingerprint": fp, "cluster": "prod-east",
			"first_seen": "2026-07-26T10:00:00Z", "resolved_at": "2026-07-26T10:15:00Z",
			"cleared_after": "10m0s", "observed_stable_for": "5m0s",
			"resolution": "recovered", "context": map[string]any{},
		}),
		// Watchboard session: digests only — no trajectory.
		"SESSION-CREATE sid=stub-sess-0002 caller=lookout token=present",
		injectLine(t, "stub-sess-0002", map[string]any{
			"kind": "watchboard.digest", "cluster": "prod-east",
			"board_generation": 1, "sequence": 1, "entries": []any{},
		}),
		// Unresolved incident: symptom-only trajectory, incomplete.
		"SESSION-CREATE sid=stub-sess-0003 caller=lookout token=present",
		injectLine(t, "stub-sess-0003", map[string]any{
			"kind": "objectstate.endpoints_empty", "reason": "endpoints_empty",
			"namespace": "prod", "kind_of_object": "Service", "name": "web", "uid": "u9",
			"cluster": "prod-east", "source": "sentinel", "severity": "critical",
			"fingerprint": "sha256:ffff", "context": map[string]any{},
		}),
	}, "\n")

	trajectories, err := Harvest(strings.NewReader(capture))
	if err != nil {
		t.Fatalf("Harvest: %v", err)
	}
	if len(trajectories) != 2 {
		t.Fatalf("got %d trajectories, want 2 (incident + unresolved; watchboard yields none): %+v", len(trajectories), trajectories)
	}

	tr := trajectories[0]
	if !tr.Complete {
		t.Errorf("first trajectory not complete: %+v", tr)
	}
	if tr.Session != "stub-sess-0001" || tr.Cluster != "prod-east" {
		t.Errorf("session/cluster = %q/%q", tr.Session, tr.Cluster)
	}
	// The frozen k8s-event opening carries no wire fingerprint; the
	// outcome record supplies the class key.
	if tr.Fingerprint != fp {
		t.Errorf("fingerprint = %q, want the outcome record's %q", tr.Fingerprint, fp)
	}
	if tr.Symptom == nil || tr.Symptom.Kind != "k8s-event" || tr.Symptom.Reason != "CrashLoopBackOff" || tr.Symptom.Name != "api-0" {
		t.Errorf("symptom stage wrong: %+v", tr.Symptom)
	}
	if tr.Diagnosis == nil || !tr.Diagnosis.EnrichmentBundle || tr.Diagnosis.TriageStatus != "triaged" ||
		tr.Diagnosis.RootCause != "bad connection string in checkout-config" {
		t.Errorf("diagnosis stage wrong: %+v", tr.Diagnosis)
	}
	if tr.Action == nil || tr.Action.Status != "actioned" || !strings.Contains(tr.Action.Action, "PR #402") {
		t.Errorf("action stage wrong: %+v", tr.Action)
	}
	if tr.Outcome == nil || tr.Outcome.Resolution != "recovered" || tr.Outcome.ObservedStableFor != "5m0s" {
		t.Errorf("outcome stage wrong: %+v", tr.Outcome)
	}
	if tr.Label != "recovered" {
		t.Errorf("label = %q, want recovered (the §9.3 ground truth)", tr.Label)
	}
	if tr.Followups != 1 {
		t.Errorf("followups = %d, want 1", tr.Followups)
	}

	un := trajectories[1]
	if un.Complete || un.Outcome != nil || un.Label != "" {
		t.Errorf("unresolved incident must stay unlabeled: %+v", un)
	}
	if un.Fingerprint != "sha256:ffff" || un.Symptom.Zone != "" || un.Symptom.Severity != "critical" {
		t.Errorf("source-kind symptom fields wrong: %+v", un)
	}
}

// TestHarvest_RevertedOutcomeWins pins the label rule: the LAST
// outcome record decides, and resolved.reverted labels the
// trajectory "reverted" — a fix that failed to stick is negative
// ground truth, not a recovery.
func TestHarvest_RevertedOutcomeWins(t *testing.T) {
	t.Parallel()
	capture := strings.Join([]string{
		injectLine(t, "s1", map[string]any{
			"kind": "rollout.stall", "reason": "rollout_stall", "namespace": "prod",
			"kind_of_object": "Deployment", "name": "web", "uid": "u1",
			"cluster": "prod-west", "fingerprint": "sha256:aa", "context": map[string]any{},
		}),
		injectLine(t, "s1", map[string]any{
			"kind": "resolved", "resolution": "recovered", "fingerprint": "sha256:aa",
			"cleared_after": "1m0s", "observed_stable_for": "5m0s",
		}),
		injectLine(t, "s1", map[string]any{
			"kind": "resolved.reverted", "resolution": "recovered", "fingerprint": "sha256:aa",
			"reverted_after": "2m0s",
		}),
	}, "\n")
	trajectories, err := Harvest(strings.NewReader(capture))
	if err != nil {
		t.Fatalf("Harvest: %v", err)
	}
	if len(trajectories) != 1 {
		t.Fatalf("got %d trajectories, want 1", len(trajectories))
	}
	tr := trajectories[0]
	if tr.Label != "reverted" || tr.Outcome.Kind != "resolved.reverted" || tr.Outcome.RevertedAfter != "2m0s" {
		t.Errorf("reverted outcome must win: %+v", tr.Outcome)
	}
}

// TestWriteJSONL_CompleteFirst pins the output contract: one JSON
// object per line, complete trajectories first.
func TestWriteJSONL_CompleteFirst(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := WriteJSONL(&buf, []Trajectory{
		{Session: "a", Complete: false},
		{Session: "b", Complete: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	var first Trajectory
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if first.Session != "b" {
		t.Errorf("complete trajectory must sort first, got %q", first.Session)
	}
}
