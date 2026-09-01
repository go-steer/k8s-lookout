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

// Issue #366: what enrichment does when the incident object is not a
// workload. A Service names one through its selector; a Node names
// none. Both used to produce a bundle whose entire content was the
// resolver's complaint about kinds.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// serviceSignal is the shape objectstate.endpoints_empty arrives in:
// critical, so enriched under the default policy, and its object is
// the Service — which owns nothing.
func serviceSignal() engine.Signal {
	return engine.Signal{
		Kind:     "objectstate.endpoints_empty",
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "uid-svc-1", Reason: "endpoints_empty"},
			Namespace:    enrichNS,
			KindOfObject: "Service",
			Name:         "api",
			Message:      "service has no ready endpoints",
			FirstSeen:    enrichNow.Add(-2 * time.Minute),
			LastSeen:     enrichNow,
			Count:        1,
		},
	}
}

// nodeSignal is the shape objectstate.node_notready arrives in:
// cluster-scoped, and a Node is not a workload and names none.
func nodeSignal() engine.Signal {
	return engine.Signal{
		Kind:     "objectstate.node_notready",
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "uid-node-1", Reason: "node_notready"},
			KindOfObject: "Node",
			Name:         "node-1",
			Message:      "node is not ready",
			FirstSeen:    enrichNow.Add(-2 * time.Minute),
			LastSeen:     enrichNow,
			Count:        1,
		},
	}
}

// TestEnrich_ServiceResolvesThroughItsSelector is the #366 headline:
// the signal that most needs a bundle — endpoints_empty, critical —
// used to produce 198B of resolver complaint, because both resolve
// paths accepted only pod-owning kinds and a Service is not one. It
// names one through its selector, and that workload's pods and logs
// are the answer to "why are there no endpoints".
func TestEnrich_ServiceResolvesThroughItsSelector(t *testing.T) {
	t.Parallel()
	cs := fake.NewClientset(enrichFixtureObjects()...)
	e := testEnricher(newMetrics(), cs, enrichLogFixture())

	b := e.Incident(context.Background(), serviceSignal())

	head, _, _ := strings.Cut(b, "\n")
	if !strings.Contains(head, "workload=Deployment/prod/api") {
		t.Errorf("Service should enrich as the workload behind it, head = %s", head)
	}
	for _, want := range []string{"section=spec", "section=delta", "section=edges", "section=logs"} {
		if !strings.Contains(b, want) {
			t.Errorf("Service bundle missing %q:\n%s", want, b)
		}
	}
	if strings.Contains(b, "enrichment_error") {
		t.Errorf("Service enrichment should not fail:\n%s", b)
	}
	if got := testutil.ToFloat64(e.metrics.enrichments.WithLabelValues("ok")); got != 1 {
		t.Errorf("enrichments{outcome=ok} = %v, want 1", got)
	}
}

// TestEnrich_ServiceTakesTheScopedPathEvenWithALiveGraph: a Service
// IS in the live topology graph, so the live path would resolve it
// and then have nothing to read — the informer set has no Service
// index and no pod templates to evaluate a selector against. Handing
// over is what keeps the answer right; the live path is an
// optimization of cost, never of correctness.
func TestEnrich_ServiceTakesTheScopedPathEvenWithALiveGraph(t *testing.T) {
	t.Parallel()
	cs := fake.NewClientset(enrichFixtureObjects()...)
	e := testEnricher(newMetrics(), cs, enrichLogFixture())
	e.snapshot = liveSnapshotOf(t, enrichPod(), enrichReplicaSet(), enrichNode(), enrichService())

	b := e.Incident(context.Background(), serviceSignal())

	head, _, _ := strings.Cut(b, "\n")
	if !strings.Contains(head, "workload=Deployment/prod/api") {
		t.Errorf("live graph present, Service still resolves to its backend, head = %s", head)
	}
	// The edges section is the tell: the live path can only ever skip
	// it, so a computed one proves the scoped fallback ran.
	if !strings.Contains(b, "section=edges") || strings.Contains(b, "overflow section=edges") {
		t.Errorf("Service should have taken the scoped path (computed edges):\n%s", b)
	}
}

// TestEnrich_ANodeIsNotAWorkloadButItsRadiusIsStillTheIncident is the
// other half of #366. There is no workload object to GET and no
// workload edges to validate — but the pods on that node ARE what the
// incident is about, so the bundle keeps its radius and reports the
// missing sections as absent rather than broken.
func TestEnrich_ANodeIsNotAWorkloadButItsRadiusIsStillTheIncident(t *testing.T) {
	t.Parallel()
	cs := fake.NewClientset(enrichFixtureObjects()...)
	e := testEnricher(newMetrics(), cs, enrichLogFixture())
	e.snapshot = liveSnapshotOf(t, enrichPod(), enrichReplicaSet(), enrichNode())

	b := e.Incident(context.Background(), nodeSignal())

	if !strings.Contains(b, "section=radius") {
		t.Errorf("a Node bundle is its radius — the pods on it:\n%s", b)
	}
	if strings.Contains(b, "enrichment_error") {
		t.Errorf("absent sections are not failures:\n%s", b)
	}
	if strings.Contains(b, "unsupported workload kind") {
		t.Errorf("the bundle must not describe the resolver (#366):\n%s", b)
	}
	// The overflow trailers still name a command that exists: a
	// `--workload=Node//node-1` would not.
	for _, want := range []string{
		`overflow section=spec cmd="lookout triage delta --only=nodes"`,
		`overflow section=edges cmd="lookout triage delta --only=nodes"`,
	} {
		if !strings.Contains(b, want) {
			t.Errorf("Node bundle missing %q:\n%s", want, b)
		}
	}
}

// TestEnrich_ANodeWithoutALiveGraphIsSkippedEntirely: the scoped path
// can only produce a workload bundle, and a Node names no workload.
// Nothing is the right answer — the resolver's complaint would spend
// the inject's enrichment budget describing the enricher on every
// node signal forever (#366).
func TestEnrich_ANodeWithoutALiveGraphIsSkippedEntirely(t *testing.T) {
	t.Parallel()
	cs := fake.NewClientset(enrichFixtureObjects()...)
	// A skip is decided before the read, so the List must never
	// happen: the cheapest enrichment is the one not attempted.
	cs.PrependReactor("list", "*", func(a k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("a skipped enrichment must not List (asked for %s)", a.GetResource().Resource)
	})
	e := testEnricher(newMetrics(), cs, enrichLogFixture())

	if b := e.Incident(context.Background(), nodeSignal()); b != "" {
		t.Errorf("want no bundle at all, got:\n%s", b)
	}
	// Counted apart from failed: "we chose not to" and "we tried and
	// could not" are different operational facts.
	if got := testutil.ToFloat64(e.metrics.enrichments.WithLabelValues("skipped")); got != 1 {
		t.Errorf("enrichments{outcome=skipped} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(e.metrics.enrichments.WithLabelValues("failed")); got != 0 {
		t.Errorf("enrichments{outcome=failed} = %v, want 0", got)
	}
}

// TestEnrich_AnUnresolvableServiceStillFailsHonestly draws the line
// the skip does not cross: a Service that could have had a backend
// and does not is a real read that came back empty, and an operator
// can act on that. Only the structurally impossible target is
// silent.
func TestEnrich_AnUnresolvableServiceStillFailsHonestly(t *testing.T) {
	t.Parallel()
	cs := fake.NewClientset(enrichFixtureObjects()...)
	e := testEnricher(newMetrics(), cs, enrichLogFixture())
	sig := serviceSignal()
	sig.Name = "not-a-service"

	b := e.Incident(context.Background(), sig)

	if !strings.Contains(b, "enrichment_error stage=resolve") {
		t.Errorf("a Service that resolves to nothing is an honest failure:\n%s", b)
	}
	if !strings.Contains(b, "no single workload behind this Service") {
		t.Errorf("the trailer should say why:\n%s", b)
	}
}
