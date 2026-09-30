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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// TestEnrich_ANodeIsEnrichedAsTheNode is issue #376: the bundle for a
// node_notready signal is about the node — its spec (kubelet,
// allocatable vs capacity), its abnormal conditions, and the pods on
// it — on both read paths, rather than a radius with every other
// section trailed away (live) or nothing at all (no graph).
func TestEnrich_ANodeIsEnrichedAsTheNode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		live bool
	}{{"live graph", true}, {"no graph", false}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cs := fake.NewClientset(nodeFixtureObjects()...)
			e := testEnricher(newMetrics(), cs, enrichLogFixture())
			if tc.live {
				e.snapshot = liveSnapshotOf(t, enrichPod(), enrichReplicaSet(), notReadyNode())
			}

			b := e.Incident(context.Background(), nodeSignal())

			head, _, _ := strings.Cut(b, "\n")
			for _, want := range []string{"kind_of_object=Node", "name=node-1", "node=node-1", "pods=1", "sections=spec,delta,radius"} {
				if !strings.Contains(head, want) {
					t.Errorf("head missing %q: %s", want, head)
				}
			}
			if strings.Contains(head, "workload=") {
				t.Errorf("a Node has no workload form: %s", head)
			}
			for _, want := range []string{
				"kubelet=v1.33.4-gke.1245000",
				"allocatable=cpu:3920m,memory:12698Mi,pods:110",
				"kind=node.notready severity=critical kind_of_object=Node name=node-1 reason=KubeletStopped",
				"kind=pod.crashloop",
				"kind=radius.neighbor severity=info namespace=prod kind_of_object=Pod name=" + enrichPod().Name + " section=radius",
			} {
				if !strings.Contains(b, want) {
					t.Errorf("Node bundle missing %q:\n%s", want, b)
				}
			}
			// No edges and no logs: nothing to validate and nothing on
			// the API server to read, so no trailer promises either.
			for _, bad := range []string{"section=edges", "section=logs", "enrichment_error", "radius.missing", "elsewhere"} {
				if strings.Contains(b, bad) {
					t.Errorf("Node bundle must not carry %q:\n%s", bad, b)
				}
			}
			if got := testutil.ToFloat64(e.metrics.enrichments.WithLabelValues("ok")); got != 1 {
				t.Errorf("enrichments{outcome=ok} = %v, want 1", got)
			}
		})
	}
}

// TestEnrich_ANodeWithoutAGraphReadsOnlyItsOwnPods: the scoped path's
// pod read is one List narrowed to the node, never a cluster-wide
// sweep, and the node itself is read by List (the watcher's grant)
// rather than GET.
func TestEnrich_ANodeWithoutAGraphReadsOnlyItsOwnPods(t *testing.T) {
	t.Parallel()
	cs := fake.NewClientset(nodeFixtureObjects()...)
	e := testEnricher(newMetrics(), cs, enrichLogFixture())

	e.Incident(context.Background(), nodeSignal())

	selectors := map[string]string{}
	for _, a := range cs.Actions() {
		if a.GetVerb() == "get" {
			t.Errorf("a Node bundle reads by List only, got a GET of %s", a.GetResource().Resource)
		}
		if l, ok := a.(k8stesting.ListAction); ok {
			selectors[a.GetResource().Resource] = l.GetListRestrictions().Fields.String()
		}
	}
	if got := selectors["pods"]; got != "spec.nodeName=node-1" {
		t.Errorf("pods List field selector = %q, want spec.nodeName=node-1", got)
	}
	if got := selectors["nodes"]; got != "metadata.name=node-1" {
		t.Errorf("nodes List field selector = %q, want metadata.name=node-1", got)
	}
}

// TestEnrich_ANodeBundleSurvivesEitherReadFailing: the node and its
// pods are separate reads, and losing one still ships the other.
// Only losing both is a resolve failure.
func TestEnrich_ANodeBundleSurvivesEitherReadFailing(t *testing.T) {
	t.Parallel()
	deny := func(resources ...string) *fake.Clientset {
		cs := fake.NewClientset(nodeFixtureObjects()...)
		for _, r := range resources {
			cs.PrependReactor("list", r, func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, fmt.Errorf("%s is forbidden", r)
			})
		}
		return cs
	}
	for _, tc := range []struct {
		name    string
		denied  []string
		want    []string
		outcome string
	}{
		{"node denied", []string{"nodes"}, []string{"section=radius", "kind=pod.crashloop", "enrichment_error stage=spec"}, "partial"},
		{"pods denied", []string{"pods"}, []string{"section=spec", "kind=node.notready", "enrichment_error stage=radius"}, "partial"},
		{"both denied", []string{"nodes", "pods"}, []string{"enrichment_error stage=resolve", "nodes is forbidden"}, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testEnricher(newMetrics(), deny(tc.denied...), enrichLogFixture())
			b := e.Incident(context.Background(), nodeSignal())
			for _, want := range tc.want {
				if !strings.Contains(b, want) {
					t.Errorf("bundle missing %q:\n%s", want, b)
				}
			}
			if got := testutil.ToFloat64(e.metrics.enrichments.WithLabelValues(tc.outcome)); got != 1 {
				t.Errorf("enrichments{outcome=%s} = %v, want 1", tc.outcome, got)
			}
		})
	}
}

// notReadyNode is node-1 as a node_notready signal finds it: Ready
// False, with the status fields a Node spec section reads.
func notReadyNode() *corev1.Node {
	n := enrichNode()
	n.Status = corev1.NodeStatus{
		NodeInfo: corev1.NodeSystemInfo{KubeletVersion: "v1.33.4-gke.1245000", ContainerRuntimeVersion: "containerd://2.0.6"},
		Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("3920m"),
			corev1.ResourceMemory: resource.MustParse("12698Mi"),
			corev1.ResourcePods:   resource.MustParse("110"),
		},
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("4"),
			corev1.ResourceMemory: resource.MustParse("16Gi"),
			corev1.ResourcePods:   resource.MustParse("110"),
		},
		Conditions: []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionFalse, Reason: "KubeletStopped",
			Message:            "kubelet stopped posting node status",
			LastTransitionTime: metav1.Time{Time: enrichNow.Add(-3 * time.Minute)},
		}},
	}
	return n
}

// nodeFixtureObjects is the enrichment fixture with node-1 NotReady,
// plus a pod on another node that a node-1 bundle must not read.
func nodeFixtureObjects() []runtime.Object {
	var out []runtime.Object
	for _, o := range enrichFixtureObjects() {
		if n, ok := o.(*corev1.Node); ok && n.Name == "node-1" {
			o = notReadyNode()
		}
		out = append(out, o)
	}
	elsewhere := enrichPod()
	elsewhere.Name = "api-7c9d8-elsewhere"
	elsewhere.Spec.NodeName = "node-2"
	return append(out, elsewhere)
}

// TestEnrich_ANonWorkloadWithoutALiveGraphIsSkippedEntirely: the
// scoped path can only produce a workload bundle, and a kind that
// names none (a PersistentVolumeClaim here) gets nothing. The
// resolver's complaint would spend the inject's enrichment budget
// describing the enricher on every such signal forever (#366).
func TestEnrich_ANonWorkloadWithoutALiveGraphIsSkippedEntirely(t *testing.T) {
	t.Parallel()
	cs := fake.NewClientset(enrichFixtureObjects()...)
	// A skip is decided before the read, so the List must never
	// happen: the cheapest enrichment is the one not attempted.
	cs.PrependReactor("list", "*", func(a k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("a skipped enrichment must not List (asked for %s)", a.GetResource().Resource)
	})
	e := testEnricher(newMetrics(), cs, enrichLogFixture())
	sig := nodeSignal()
	sig.KindOfObject, sig.Namespace, sig.Name = "PersistentVolumeClaim", enrichNS, "data"

	if b := e.Incident(context.Background(), sig); b != "" {
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
