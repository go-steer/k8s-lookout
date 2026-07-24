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

package engine

import "testing"

func TestSeverity_Valid(t *testing.T) {
	t.Parallel()
	for _, s := range []Severity{SeverityCritical, SeverityWarning, SeverityInfo} {
		if !s.Valid() {
			t.Errorf("Severity(%q).Valid() = false, want true", s)
		}
	}
	for _, s := range []Severity{"", "CRITICAL", "page", "warn"} {
		if s.Valid() {
			t.Errorf("Severity(%q).Valid() = true, want false", s)
		}
	}
}

// TestSignal_FrozenKindConstants pins the §7.3 kind strings: playbook
// skills pattern-match them in inject payloads, so the values are
// back-compat frozen (AGENTS.md). A companion test in internal/watch
// pins them to pkg/inject's wire constants.
func TestSignal_FrozenKindConstants(t *testing.T) {
	t.Parallel()
	if KindK8sEvent != "k8s-event" {
		t.Errorf("KindK8sEvent = %q, want k8s-event (frozen)", KindK8sEvent)
	}
	if KindK8sEventFollowup != "k8s-event-followup" {
		t.Errorf("KindK8sEventFollowup = %q, want k8s-event-followup (frozen)", KindK8sEventFollowup)
	}
	if SourceSentinel != "sentinel" || SourceScan != "scan" {
		t.Errorf("source constants = %q/%q, want sentinel/scan (§8)", SourceSentinel, SourceScan)
	}
}

// TestSignal_PromotesTriageEventCore verifies the embedding contract
// the pipeline relies on: the frozen TriageEvent fields are reachable
// directly on Signal, so filter/dedup/inject code reads sig.Key,
// sig.Namespace, sig.Count without caring which source emitted it.
func TestSignal_PromotesTriageEventCore(t *testing.T) {
	t.Parallel()
	sig := Signal{
		Kind:     KindK8sEvent,
		Severity: SeverityCritical,
		TriageEvent: TriageEvent{
			Key:       EventKey{UID: "u1", Reason: "CrashLoopBackOff"},
			Namespace: "checkout",
			Count:     3,
		},
	}
	if sig.Key.UID != "u1" || sig.Key.Reason != "CrashLoopBackOff" {
		t.Errorf("promoted Key = %+v", sig.Key)
	}
	if sig.Namespace != "checkout" || sig.Count != 3 {
		t.Errorf("promoted Namespace/Count = %q/%d", sig.Namespace, sig.Count)
	}
	if sig.Forecast != nil || sig.Enrichment != nil {
		t.Error("Forecast/Enrichment must default to nil (reactive signal)")
	}
}
