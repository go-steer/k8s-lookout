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

// Signal-schema v1 freeze (docs/signal-schema-v1.md, DESIGN.md §8/§14
// M5): these tests are the machine-readable ledger of the frozen wire
// contract fleet consumers consume as-is.
//
//   - Every SHIPPED signal kind is enumerated and mapped to the wire
//     struct that serializes it — a kind added without extending the
//     ledger fails the enumeration test.
//   - Every wire struct's field list (json tags, in order) is pinned.
//     A frozen field disappearing or changing its json tag fails the
//     pin: that is a BREAKING change requiring v2 negotiation with
//     fleet consumers, never a test to update. Adding a field is allowed within
//     v1 but must consciously extend the pinned list here and the
//     schema doc (additive-only evolution).
//   - Every payload type round-trips marshal → unmarshal → marshal
//     byte-identically, so a §9.3 harvester or a fleet-level ingester can
//     re-serialize what it read without corrupting the record.
package inject_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/inject/schema"
	"github.com/go-steer/k8s-lookout/pkg/sources/capacity"
	"github.com/go-steer/k8s-lookout/pkg/sources/rollout"
)

// shippedKinds is the v1 kind inventory: every signal kind any
// shipped code path can put on the wire, mapped to the struct that
// serializes it. The two frozen M0 kinds and the source-namespaced
// kinds share inject.Payload; the cross-cutting kinds carry their own
// schema-stable structs.
//
// The ledger itself lives in pkg/inject/schema as exported data so
// the docs-site generator (internal/sitedoc) renders the signal-kind
// catalog from the SAME slice these freeze tests pin — the tests
// below keep validating count, struct mapping, and engine-constant
// alignment exactly as before the export.
var shippedKinds = func() map[string]reflect.Type {
	m := make(map[string]reflect.Type)
	for _, k := range schema.Kinds() {
		if _, dup := m[k.Kind]; dup {
			panic("duplicate kind in pkg/inject/schema ledger: " + k.Kind)
		}
		m[k.Kind] = reflect.TypeOf(k.Payload)
	}
	return m
}()

// frozenFields pins, per wire struct, the ordered json field names of
// signal-schema v1 (nested objects pinned separately below). Removing
// or renaming an entry here — or shipping a struct whose tags no
// longer produce this list — is a v2 negotiation with fleet consumers, not a test
// update. Additions land at the end of a struct AND of this ledger
// AND in docs/signal-schema-v1.md, in the same change.
var frozenFields = map[string][]string{
	// "type" landed 2026-07-27 (pre-consumer amendment, adopted from
	// kube-agents' watcher): the k8s Event.Type, positioned after
	// "context" to match their wire byte-for-byte on the k8s-event
	// kinds. NOT omitempty (their contract); empty for synthetic
	// source signals. See docs/signal-schema-v1.md §Amendments.
	"Payload": {"kind", "reason", "namespace", "kind_of_object", "name",
		"container", "uid", "message", "count", "first_seen", "last_seen",
		"cluster", "project", "zone", "source", "severity", "fingerprint",
		"context", "type", "enrichment", "forecast", "quota_increase_draft"},
	"ResolvedPayload": {"kind", "reason", "namespace", "kind_of_object",
		"name", "container", "uid", "fingerprint", "cluster", "first_seen",
		"resolved_at", "cleared_after", "observed_stable_for", "resolution",
		"reverted_after", "context"},
	"StormPayload": {"kind", "fingerprint", "severity", "cluster",
		"ancestor_kind", "ancestor_namespace", "ancestor_name", "reason",
		"message", "affected_count", "namespaces_count", "first_seen",
		"last_seen", "representative_incidents", "member_fingerprints",
		"context", "enrichment"},
	"StormMemberPayload": {"kind", "storm_fingerprint", "storm_session_id",
		"ancestor_kind", "ancestor_namespace", "ancestor_name", "cluster",
		"message", "incident"},
	"StormUpdatePayload": {"kind", "storm_fingerprint", "ancestor_kind",
		"ancestor_namespace", "ancestor_name", "cluster", "message",
		"affected_count", "namespaces_count", "new_members_since_last"},
	"WatchboardDigestPayload": {"kind", "cluster", "board_generation",
		"sequence", "window_start", "window_end", "entries"},
	"WatchboardRotatedPayload": {"kind", "cluster", "board_generation",
		"successor_session_id", "injects_count", "rotated_at"},
	"TriageRegressedPayload": {"kind", "reason", "namespace",
		"kind_of_object", "name", "container", "uid", "fingerprint",
		"cluster", "triage_status", "severity_override", "triage_session",
		"baseline_count", "count", "factor", "first_seen", "last_seen",
		"message", "context"},
	// Nested objects.
	"PayloadContext":    {"controller_ref", "node", "labels"},
	"PayloadForecast":   {"eta", "confidence_basis"},
	"PayloadEnrichment": {"bundle"},
	"PayloadQuotaDraft": {"quota_id", "region", "unit", "current_usage",
		"current_limit", "suggested_limit", "slope_per_day", "justification"},
	"StormIncidentRef": {"fingerprint", "reason", "namespace",
		"kind_of_object", "name", "uid", "session_id"},
	"WatchboardEntry": {"kind", "fingerprint", "reason", "namespace",
		"kind_of_object", "name", "uid", "count", "first_seen", "last_seen"},
}

// jsonFields returns a struct type's json field names in declaration
// order (the marshal order), stripping ",omitempty" etc.
func jsonFields(t reflect.Type) []string {
	var out []string
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		out = append(out, strings.Split(tag, ",")[0])
	}
	return out
}

// TestSchemaV1_FieldSetsFrozen pins every wire struct's ordered json
// field list to the v1 ledger.
func TestSchemaV1_FieldSetsFrozen(t *testing.T) {
	t.Parallel()
	types := map[string]reflect.Type{
		"Payload":                  reflect.TypeOf(inject.Payload{}),
		"ResolvedPayload":          reflect.TypeOf(inject.ResolvedPayload{}),
		"StormPayload":             reflect.TypeOf(inject.StormPayload{}),
		"StormMemberPayload":       reflect.TypeOf(inject.StormMemberPayload{}),
		"StormUpdatePayload":       reflect.TypeOf(inject.StormUpdatePayload{}),
		"WatchboardDigestPayload":  reflect.TypeOf(inject.WatchboardDigestPayload{}),
		"WatchboardRotatedPayload": reflect.TypeOf(inject.WatchboardRotatedPayload{}),
		"TriageRegressedPayload":   reflect.TypeOf(inject.TriageRegressedPayload{}),
		"PayloadContext":           reflect.TypeOf(inject.PayloadContext{}),
		"PayloadForecast":          reflect.TypeOf(inject.PayloadForecast{}),
		"PayloadEnrichment":        reflect.TypeOf(inject.PayloadEnrichment{}),
		"PayloadQuotaDraft":        reflect.TypeOf(inject.PayloadQuotaDraft{}),
		"StormIncidentRef":         reflect.TypeOf(inject.StormIncidentRef{}),
		"WatchboardEntry":          reflect.TypeOf(inject.WatchboardEntry{}),
	}
	if len(types) != len(frozenFields) {
		t.Fatalf("ledger covers %d structs, test enumerates %d — keep them identical", len(frozenFields), len(types))
	}
	for name, typ := range types {
		got := jsonFields(typ)
		want := frozenFields[name]
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s wire fields drifted from the v1 freeze (docs/signal-schema-v1.md):\n got: %v\nwant: %v\nRemoving/renaming is a v2 negotiation with fleet consumers; additions must extend the ledger consciously.", name, got, want)
		}
	}
}

// TestSchemaV1_KindInventory keeps the shipped-kind enumeration
// aligned with the constants the engine and the sources own: a kind
// constant that exists in code but not in the ledger (or vice versa)
// fails, so the inventory in docs/signal-schema-v1.md cannot rot.
func TestSchemaV1_KindInventory(t *testing.T) {
	t.Parallel()
	// Cross-cutting kinds must mirror pkg/engine's constants.
	pairs := map[string]string{
		inject.KindEvent:                 engine.KindK8sEvent,
		inject.KindFollowup:              engine.KindK8sEventFollowup,
		inject.KindResolved:              engine.KindResolved,
		inject.KindResolvedReverted:      engine.KindResolvedReverted,
		inject.KindStorm:                 engine.KindStorm,
		inject.KindStormMember:           engine.KindStormMember,
		inject.KindStormMemberSuperseded: engine.KindStormMemberSuperseded,
		inject.KindStormUpdate:           engine.KindStormUpdate,
		inject.KindTriageRegressed:       engine.KindTriageRegressed,
	}
	for injectKind, engineKind := range pairs {
		if injectKind != engineKind {
			t.Errorf("kind constant mismatch: inject %q vs engine %q", injectKind, engineKind)
		}
	}
	// Every enumerated kind resolves to a struct the field ledger
	// pins.
	for kind, typ := range shippedKinds {
		if kind == "" {
			t.Fatal("empty kind in the inventory")
		}
		if _, ok := frozenFields[typ.Name()]; !ok {
			t.Errorf("kind %q maps to %s, which has no frozen field ledger", kind, typ.Name())
		}
	}
	// The inventory is complete: 11 cross-cutting + 21 source kinds.
	if len(shippedKinds) != 32 {
		t.Errorf("shipped kind inventory has %d kinds, want 32 — a kind shipped (or was removed) without updating the v1 ledger and docs/signal-schema-v1.md", len(shippedKinds))
	}
}

// TestSchemaV1_RoundTrip serializes a fully-populated instance of
// every wire payload, unmarshals it, and re-serializes: the bytes
// must be identical, so any schema-walking consumer (the §9.3
// harvester, a fleet-level ingester) can re-emit records losslessly.
func TestSchemaV1_RoundTrip(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	payloads := []any{
		inject.Payload{
			Kind: capacity.KindStockout, Reason: "stockout", Namespace: "",
			KindOfObject: "NodeGroup", Name: "pool-a", Container: "", UID: "nodegroup:pool-a",
			Message: "autoscaler noScaleUp decision", Count: 1, FirstSeen: ts, LastSeen: ts,
			Cluster: "prod-east", Project: "acme-prod", Zone: "us-east1-b",
			Source: "sentinel", Severity: "critical",
			Fingerprint: engine.Fingerprint(capacity.KindStockout, "stockout", "NodeGroup", "us-east1-b"),
			Context:     inject.PayloadContext{Node: "n1", Labels: map[string]string{"a": "b"}},
			Enrichment:  &inject.PayloadEnrichment{Bundle: "kind=bundle.target"},
			Forecast:    &inject.PayloadForecast{ETA: ts.Add(time.Hour), ConfidenceBasis: "linear-90m-window"},
			QuotaIncreaseDraft: &inject.PayloadQuotaDraft{
				QuotaID: "compute.googleapis.com/CpusPerProjectPerRegion", Region: "us-east1",
				Unit: "count", CurrentUsage: 1700, CurrentLimit: 2000, SuggestedLimit: 3000,
				SlopePerDay: 50, Justification: "growth",
			},
		},
		inject.ResolvedPayload{
			Kind: inject.KindResolvedReverted, Reason: "CrashLoopBackOff", Namespace: "prod",
			KindOfObject: "Pod", Name: "api-0", Container: "app", UID: "u1",
			Fingerprint: "sha256:aa", Cluster: "prod-east", FirstSeen: ts,
			ResolvedAt: ts.Add(time.Minute), ClearedAfter: "2m30s",
			ObservedStableFor: "5m0s", Resolution: "recovered", RevertedAfter: "1m0s",
			Context: inject.PayloadContext{ControllerRef: "ReplicaSet/api"},
		},
		inject.StormPayload{
			Kind: inject.KindStorm, Fingerprint: "sha256:bb", Severity: "critical",
			Cluster: "prod-east", AncestorKind: "Node", AncestorNamespace: "",
			AncestorName: "node-1", Reason: "NodeNotReady", Message: "storm",
			AffectedCount: 3, NamespacesCount: 2, FirstSeen: ts, LastSeen: ts,
			Representatives: []inject.StormIncidentRef{{
				Fingerprint: "sha256:cc", Reason: "BackOff", Namespace: "prod",
				KindOfObject: "Pod", Name: "p1", UID: "u2", SessionID: "sess-1",
			}},
			MemberFingerprints: []string{"sha256:cc"},
			Context:            inject.PayloadContext{Node: "node-1"},
			Enrichment:         &inject.PayloadEnrichment{Bundle: "kind=radius.neighbor"},
		},
		inject.StormMemberPayload{
			Kind: inject.KindStormMemberSuperseded, StormFingerprint: "sha256:bb",
			StormSessionID: "sess-9", AncestorKind: "Node", AncestorName: "node-1",
			Cluster: "prod-east", Message: "superseded",
			Incident: inject.StormIncidentRef{Fingerprint: "sha256:cc", Reason: "BackOff",
				KindOfObject: "Pod", Name: "p1", UID: "u2"},
		},
		inject.StormUpdatePayload{
			Kind: inject.KindStormUpdate, StormFingerprint: "sha256:bb",
			AncestorKind: "Node", AncestorName: "node-1", Cluster: "prod-east",
			Message: "grew", AffectedCount: 33, NamespacesCount: 4, NewMembersSinceLast: 21,
		},
		inject.WatchboardDigestPayload{
			Kind: inject.KindWatchboardDigest, Cluster: "prod-east",
			BoardGeneration: 1, Sequence: 2, WindowStart: ts, WindowEnd: ts.Add(time.Minute),
			Entries: []inject.WatchboardEntry{{
				Kind: rollout.KindStall, Fingerprint: "sha256:dd", Reason: "rollout_stall",
				Namespace: "prod", KindOfObject: "Deployment", Name: "web", UID: "u3",
				Count: 2, FirstSeen: ts, LastSeen: ts,
			}},
		},
		inject.WatchboardRotatedPayload{
			Kind: inject.KindWatchboardRotated, Cluster: "prod-east",
			BoardGeneration: 1, SuccessorSessionID: "sess-2", InjectsCount: 200, RotatedAt: ts,
		},
		inject.TriageRegressedPayload{
			Kind: inject.KindTriageRegressed, Reason: "CrashLoopBackOff", Namespace: "prod",
			KindOfObject: "Pod", Name: "api-0", Container: "app", UID: "u1",
			Fingerprint: "sha256:aa", Cluster: "prod-east", TriageStatus: "triaged",
			SeverityOverride: "warning", TriageSession: "sess-1", BaselineCount: 3,
			Count: 9, Factor: 3, FirstSeen: ts, LastSeen: ts, Message: "regressed",
			Context: inject.PayloadContext{Node: "n1"},
		},
	}
	for _, p := range payloads {
		first, err := json.Marshal(p)
		if err != nil {
			t.Fatalf("marshal %T: %v", p, err)
		}
		fresh := reflect.New(reflect.TypeOf(p))
		if err := json.Unmarshal(first, fresh.Interface()); err != nil {
			t.Fatalf("unmarshal %T: %v", p, err)
		}
		second, err := json.Marshal(fresh.Elem().Interface())
		if err != nil {
			t.Fatalf("re-marshal %T: %v", p, err)
		}
		if !bytes.Equal(first, second) {
			t.Errorf("%T does not round-trip:\n first: %s\nsecond: %s", p, first, second)
		}
	}
}
