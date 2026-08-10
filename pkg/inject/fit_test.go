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

package inject

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// samplePayload is a representative new-incident payload used across the
// fit tests: real identity fields, a modest message, an enrichment
// bundle sized by the caller.
func samplePayload(message, bundle string) Payload {
	p := Payload{
		Kind:         KindEvent,
		Reason:       "CrashLoopBackOff",
		Namespace:    "kube-system",
		KindOfObject: "Pod",
		Name:         "metrics-server-v1.35.1-578bff4857-qt6v4",
		Container:    "spec.containers{metrics-server}",
		UID:          "019fec9d-57b9-773d-a698-610c1749a808",
		Message:      message,
		Count:        3,
		FirstSeen:    time.Date(2026, 8, 10, 16, 59, 22, 0, time.UTC),
		LastSeen:     time.Date(2026, 8, 10, 17, 1, 12, 0, time.UTC),
		Cluster:      "ap-ap-deploy-test",
		Fingerprint:  "sha256:1f4e6a7b8c9d0e1f2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091",
		Context:      PayloadContext{ControllerRef: "ReplicaSet/metrics-server-v1.35.1-578bff4857", Node: "gke-node-abc"},
		Type:         "Warning",
	}
	if bundle != "" {
		p.Enrichment = &PayloadEnrichment{Bundle: bundle}
	}
	return p
}

// TestWireSize matches the exact byte count injectJSON POSTs: the
// double-encoded {"message":"<payload JSON>"} envelope.
func TestWireSize(t *testing.T) {
	p := samplePayload("Back-off restarting failed container", "")
	body, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	wrapped, err := json.Marshal(injectMessageRequest{Message: string(body)})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if got, want := WireSize(p), len(wrapped); got != want {
		t.Errorf("WireSize = %d, want %d (the wrapped envelope length)", got, want)
	}
	// The envelope is strictly larger than the bare payload — escaping
	// the payload JSON into a string only ever adds bytes.
	if WireSize(p) <= len(body) {
		t.Errorf("WireSize %d should exceed bare payload %d (double-encoding overhead)", WireSize(p), len(body))
	}
}

func TestPayloadFitTo_AlreadyFits(t *testing.T) {
	p := samplePayload("Back-off restarting failed container", "")
	before, _ := json.Marshal(p)
	shed := p.FitTo(MaxInjectBytes)
	if shed != nil {
		t.Errorf("FitTo shed %v on a payload that already fits; want nil", shed)
	}
	if after, _ := json.Marshal(p); string(after) != string(before) {
		t.Errorf("FitTo mutated a payload that already fits:\n before %s\n after  %s", before, after)
	}
}

func TestPayloadFitTo_DropsEnrichmentFirst(t *testing.T) {
	// A bundle well over the ceiling, but a small message: dropping
	// enrichment alone must be enough, and the message must survive.
	msg := "Back-off restarting failed container metrics-server"
	p := samplePayload(msg, strings.Repeat("section=logs line=oomkilled ", 1000))
	if WireSize(p) <= MaxInjectBytes {
		t.Fatalf("test setup: payload should start over the ceiling, got WireSize=%d", WireSize(p))
	}

	shed := p.FitTo(MaxInjectBytes)

	if len(shed) != 1 || shed[0] != "enrichment" {
		t.Errorf("shed = %v, want [enrichment]", shed)
	}
	if p.Enrichment != nil {
		t.Errorf("enrichment not dropped")
	}
	if p.Message != msg {
		t.Errorf("message changed while only enrichment should have dropped: %q", p.Message)
	}
	if WireSize(p) > MaxInjectBytes {
		t.Errorf("still over the ceiling after fit: WireSize=%d > %d", WireSize(p), MaxInjectBytes)
	}
}

func TestPayloadFitTo_TruncatesMessageWhenNoEnrichment(t *testing.T) {
	huge := strings.Repeat("A", 40_000)
	p := samplePayload(huge, "")

	shed := p.FitTo(MaxInjectBytes)

	if len(shed) != 1 || shed[0] != "message" {
		t.Errorf("shed = %v, want [message]", shed)
	}
	if WireSize(p) > MaxInjectBytes {
		t.Errorf("still over the ceiling after fit: WireSize=%d > %d", WireSize(p), MaxInjectBytes)
	}
	if !strings.HasSuffix(p.Message, truncMarker) {
		t.Errorf("truncated message missing marker: ...%q", tail(p.Message, 20))
	}
	if len(p.Message) >= len(huge) {
		t.Errorf("message not shortened: len=%d", len(p.Message))
	}
}

func TestPayloadFitTo_DropsThenTruncates(t *testing.T) {
	huge := strings.Repeat("B", 40_000)
	p := samplePayload(huge, strings.Repeat("section=logs x ", 2000))

	shed := p.FitTo(MaxInjectBytes)

	if len(shed) != 2 || shed[0] != "enrichment" || shed[1] != "message" {
		t.Errorf("shed = %v, want [enrichment message]", shed)
	}
	if p.Enrichment != nil {
		t.Errorf("enrichment not dropped")
	}
	if WireSize(p) > MaxInjectBytes {
		t.Errorf("still over the ceiling after fit: WireSize=%d > %d", WireSize(p), MaxInjectBytes)
	}
}

// TestPayloadFitTo_PreservesIdentity is the load-bearing guarantee: a
// shrunk incident must still route and dedup, so identity/routing fields
// survive even the most aggressive fit.
func TestPayloadFitTo_PreservesIdentity(t *testing.T) {
	orig := samplePayload(strings.Repeat("C", 40_000), strings.Repeat("d ", 20_000))
	p := orig
	p.FitTo(MaxInjectBytes)

	if p.Reason != orig.Reason || p.UID != orig.UID || p.Fingerprint != orig.Fingerprint ||
		p.Cluster != orig.Cluster || p.Namespace != orig.Namespace || p.Name != orig.Name ||
		p.Container != orig.Container || p.Count != orig.Count ||
		p.Context.ControllerRef != orig.Context.ControllerRef || p.Context.Node != orig.Context.Node {
		t.Errorf("fit dropped an identity/routing field:\n got  %+v\n want %+v", p, orig)
	}
}

// TestPayloadFitTo_RuneSafe ensures message truncation never splits a
// multi-byte rune (the daemon would reject invalid UTF-8, and the
// double-encode would mangle it).
func TestPayloadFitTo_RuneSafe(t *testing.T) {
	// A long run of 3-byte runes so a byte-wise cut would land mid-rune.
	p := samplePayload(strings.Repeat("界", 20_000), "")
	p.FitTo(MaxInjectBytes)
	if !utf8.ValidString(p.Message) {
		t.Errorf("truncated message is not valid UTF-8")
	}
}

func TestStormPayloadFitTo(t *testing.T) {
	p := StormPayload{
		Kind:              KindStorm,
		Fingerprint:       "sha256:aa",
		Severity:          "critical",
		Cluster:           "prod",
		AncestorKind:      "Node",
		AncestorNamespace: "",
		AncestorName:      "gke-node-xyz",
		Reason:            "NodeNotReady",
		Message:           "Node gke-node-xyz NotReady; 30 pods affected",
		AffectedCount:     30,
		Enrichment:        &PayloadEnrichment{Bundle: strings.Repeat("section=radius pod ", 1000)},
	}
	if WireSize(p) <= MaxInjectBytes {
		t.Fatalf("test setup: storm payload should start over the ceiling, got %d", WireSize(p))
	}
	shed := p.FitTo(MaxInjectBytes)
	if len(shed) == 0 {
		t.Fatalf("storm FitTo shed nothing on an over-limit payload")
	}
	if p.Enrichment != nil {
		t.Errorf("storm enrichment not dropped")
	}
	if WireSize(p) > MaxInjectBytes {
		t.Errorf("storm still over the ceiling after fit: %d > %d", WireSize(p), MaxInjectBytes)
	}
	if p.AncestorName != "gke-node-xyz" || p.Reason != "NodeNotReady" {
		t.Errorf("storm fit dropped identity")
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
