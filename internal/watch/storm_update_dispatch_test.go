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
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// TestStormUpdate_ExactWireShape pins the kind=storm.update payload
// byte-for-byte (M2 drill observation 4: size freshness rides a NEW
// schema-stable kind; the formation payload's counts stay frozen).
// SCHEMA-STABLE: treat a failing pin as a breaking schema change,
// never as a test to update.
func TestStormUpdate_ExactWireShape(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, _ := newStormDispatcher(t, base, 0)
	ctx := context.Background()

	info := engine.StormInfo{
		ID:          "Node//kl-m2-worker2",
		Ancestor:    engine.Ancestor{Kind: "Node", Name: "kl-m2-worker2"},
		Fingerprint: "sha256:48bb2e3a000000000000000000000000000000000000000000000000000000ff",
	}
	upd := engine.StormSizeUpdate{AffectedCount: 33, NamespaceCount: 4, NewSinceLast: 21}
	d.stormSizeUpdate(ctx, "sess-storm", upd, info)

	if len(*injects) != 1 {
		t.Fatalf("injects = %d, want 1", len(*injects))
	}
	if (*injects)[0].SessionID != "sess-storm" {
		t.Errorf("update landed in %q, want the storm session", (*injects)[0].SessionID)
	}
	want := `{"kind":"storm.update","storm_fingerprint":"sha256:48bb2e3a000000000000000000000000000000000000000000000000000000ff","ancestor_kind":"Node","ancestor_name":"kl-m2-worker2","cluster":"prod-us-central1","message":"Node kl-m2-worker2 storm grew to 33 incidents across 4 namespace(s) (+21 since the last size report); the initial kind=storm payload carries formation-time counts","affected_count":33,"namespaces_count":4,"new_members_since_last":21}`
	if got := messageOf(t, (*injects)[0].Body); got != want {
		t.Errorf("storm.update payload drifted from the schema-stable wire shape:\n got: %s\nwant: %s", got, want)
	}
	if got := testutil.ToFloat64(d.metrics.stormUpdates); got != 1 {
		t.Errorf("storm_updates = %v, want 1", got)
	}
}

// TestStormUpdateKindConstantsAligned pins engine's storm.update kind
// to inject's, mirroring the other storm kind pins.
func TestStormUpdateKindConstantsAligned(t *testing.T) {
	t.Parallel()
	if engine.KindStormUpdate != inject.KindStormUpdate {
		t.Errorf("engine kind %q != inject kind %q", engine.KindStormUpdate, inject.KindStormUpdate)
	}
}

// nodeNotReadySignal fabricates the objectstate.node_notready leading
// indicator for the storm's anchor node — the drill A storm seed.
func nodeNotReadySignal(node string) engine.Signal {
	ts := time.Date(2026, 7, 25, 0, 40, 0, 0, time.UTC)
	return engine.Signal{
		Kind:     "objectstate.node_notready",
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "node-uid-1", Reason: "node_notready"},
			KindOfObject: "Node",
			Name:         node,
			Node:         node,
			Message:      "node Ready condition went True→False",
			FirstSeen:    ts,
			LastSeen:     ts,
		},
	}
}

// TestStormRecovery_NodeAnchoredFullResolve is drill A's missing 33rd
// member (M2 observation 2) closed: a storm whose ANCHOR incident is
// the node itself resolves fully — the aggregate kind=resolved fires
// only after every member INCLUDING the node incident clears.
func TestStormRecovery_NodeAnchoredFullResolve(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, pods := newStormDispatcher(t, base, 2)
	nodeSig := nodeNotReadySignal("gke-a")
	ctx := context.Background()

	// Rebuild the correlator over a resolver that also maps the node
	// incident to its own node key (§7.5: a groupable-ancestor object
	// includes itself as the first candidate).
	res := &scriptedResolver{byObject: map[engine.ObjectRef][]engine.Ancestor{}}
	for _, sig := range pods {
		res.byObject[engine.ObjectRef{Kind: "Pod", Namespace: sig.Namespace, Name: sig.Name}] = []engine.Ancestor{
			{Kind: "Node", Name: "gke-a"},
			{Kind: "Namespace", Name: sig.Namespace},
		}
	}
	res.byObject[engine.ObjectRef{Kind: "Node", Name: "gke-a"}] = []engine.Ancestor{{Kind: "Node", Name: "gke-a"}}
	correlator, err := engine.NewStormCorrelator(engine.DefaultStormWindow, engine.DefaultStormMin, res)
	if err != nil {
		t.Fatalf("NewStormCorrelator: %v", err)
	}
	d.storm = correlator

	// Node incident first (per-incident sess-1), then the two pods
	// (sess-2; the third incident forms the storm in sess-3).
	d.DispatchSignal(ctx, nodeSig)
	for _, sig := range pods {
		d.DispatchSignal(ctx, sig)
	}
	if got := testutil.ToFloat64(d.metrics.stormsFormed); got != 1 {
		t.Fatalf("storms_formed = %v, want 1", got)
	}

	// Pods clear first: the storm must NOT resolve while the node
	// member is still symptomatic — the drill saw 32/33 forever.
	for _, sig := range pods {
		d.DispatchSignal(ctx, resolvedSignalFor(sig, engine.KindResolved))
	}
	if got := testutil.ToFloat64(d.metrics.stormsResolved); got != 0 {
		t.Fatalf("storm resolved with the node member still open (storms_resolved=%v)", got)
	}

	// The node's own kind=resolved (now producible: the object-state
	// source's NodeClearance judges it) is the last member clearing —
	// the storm's aggregate outcome fires into the storm session.
	before := len(*injects)
	d.DispatchSignal(ctx, resolvedSignalFor(nodeSig, engine.KindResolved))
	if got := testutil.ToFloat64(d.metrics.stormsResolved); got != 1 {
		t.Fatalf("storms_resolved = %v, want 1 after the node member cleared", got)
	}
	post := (*injects)[before:]
	if len(post) != 2 {
		t.Fatalf("post injects = %d, want 2 (node outcome + storm outcome)", len(post))
	}
	var final inject.ResolvedPayload
	if err := json.Unmarshal([]byte(messageOf(t, post[1].Body)), &final); err != nil {
		t.Fatalf("unmarshal storm outcome: %v", err)
	}
	if final.Kind != inject.KindResolved || final.Reason != "storm" {
		t.Errorf("storm outcome kind/reason = %q/%q, want resolved/storm", final.Kind, final.Reason)
	}
	if final.KindOfObject != "Node" || final.Name != "gke-a" || final.UID != "storm:Node//gke-a" {
		t.Errorf("storm outcome anchored wrong: %+v", final)
	}
	if !strings.HasPrefix(final.Fingerprint, "sha256:") {
		t.Errorf("storm outcome fingerprint = %q", final.Fingerprint)
	}
}
