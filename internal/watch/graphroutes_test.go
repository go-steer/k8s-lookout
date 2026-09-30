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
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/sources"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

func testService(ns, name string, selector map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID("svc-" + name)},
		Spec:       corev1.ServiceSpec{Selector: selector},
	}
}

func testSlice(ns, name, svc string, pods ...string) *discoveryv1.EndpointSlice {
	eps := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: name, UID: types.UID("eps-" + name),
		Labels:          map[string]string{discoveryv1.LabelServiceName: svc},
		OwnerReferences: []metav1.OwnerReference{{Kind: "Service", Name: svc, UID: types.UID("svc-" + svc)}},
	}}
	for _, p := range pods {
		eps.Endpoints = append(eps.Endpoints, discoveryv1.Endpoint{
			Addresses: []string{"10.0.0.1"},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: ns, Name: p},
		})
	}
	return eps
}

// TestProbeRoutingAccess: the routing layer degrades per kind — a
// missing grant drops that kind with a line naming it and the fix,
// never a startup failure — while a probe that cannot be evaluated
// still refuses to start (§11).
func TestProbeRoutingAccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	got, lines, err := probeRoutingAccess(ctx, allowAll())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, routingKinds) {
		t.Errorf("all granted: kinds = %v, want %v", got, routingKinds)
	}
	if len(lines) != 1 || !strings.Contains(lines[0], "recording the routing layer (Service, EndpointSlice, Ingress, NetworkPolicy)") {
		t.Errorf("all granted: lines = %q", lines)
	}

	// Ingress watch denied (the pre-#507 manifest shape: list only).
	got, lines, err = probeRoutingAccess(ctx, grantReviewer{allow: func(r sources.Requirement) bool {
		return r.Resource != "ingresses" || r.Verb != "watch"
	}})
	if err != nil {
		t.Fatalf("a denial must degrade, not fail: %v", err)
	}
	want := []graph.NodeKind{graph.KindService, graph.KindEndpointSlice, graph.KindNetworkPolicy}
	if !slices.Equal(got, want) {
		t.Errorf("ingress denied: kinds = %v, want %v", got, want)
	}
	joined := strings.Join(lines, "\n")
	for _, w := range []string{"not recording Ingress", "ingresses", "unrecorded=", "deploy/12-clusterrole-watcher.yaml", "#507"} {
		if !strings.Contains(joined, w) {
			t.Errorf("ingress denied: lines missing %q:\n%s", w, joined)
		}
	}

	// Everything denied: no kinds, one line per kind, no "recording" line.
	got, lines, err = probeRoutingAccess(ctx, grantReviewer{allow: func(sources.Requirement) bool { return false }})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 || len(lines) != len(routingKinds) {
		t.Errorf("all denied: kinds = %v, lines = %q", got, lines)
	}

	if _, _, err := probeRoutingAccess(ctx, erroringReviewer{}); err == nil {
		t.Error("an unevaluable probe must be fatal")
	}
}

// TestRoutingUnchanged pins the EndpointSlice store-growth filter:
// readiness flips (the bulk of slice updates) log nothing, anything the
// graph derives from a slice still does, and no other kind is filtered.
func TestRoutingUnchanged(t *testing.T) {
	t.Parallel()
	base := testSlice("shop", "web-abc", "web", "web-1", "web-2")

	notReady := base.DeepCopy()
	f := false
	notReady.Endpoints[1].Conditions.Ready = &f
	notReady.ResourceVersion = "2"
	if !routingUnchanged(base, notReady) {
		t.Error("a readiness-only update must be filtered")
	}

	member := testSlice("shop", "web-abc", "web", "web-1", "web-3")
	if routingUnchanged(base, member) {
		t.Error("a membership change must pass")
	}
	grown := testSlice("shop", "web-abc", "web", "web-1", "web-2", "web-3")
	if routingUnchanged(base, grown) {
		t.Error("a new endpoint must pass")
	}
	relabeled := base.DeepCopy()
	relabeled.Labels[discoveryv1.LabelServiceName] = "web-canary"
	if routingUnchanged(base, relabeled) {
		t.Error("a label change must pass")
	}
	reowned := base.DeepCopy()
	reowned.OwnerReferences[0].UID = "svc-web-recreated"
	if routingUnchanged(base, reowned) {
		t.Error("an owner change must pass")
	}

	svc := testService("shop", "web", map[string]string{"app": "web"})
	if routingUnchanged(svc, svc.DeepCopy()) {
		t.Error("only EndpointSlices are filtered")
	}
	if routingUnchanged(base, svc) {
		t.Error("a mismatched pair is never filtered")
	}
}

// TestGraphAncestors_ServiceNeverResolves: with a store the feed does
// hold Services, but a Service incident must key exactly as it does
// without one — per-incident, no namespace key.
func TestGraphAncestors_ServiceNeverResolves(t *testing.T) {
	t.Parallel()
	g := buildFeedGraph(t,
		testService("shop", "web", map[string]string{"app": "web"}),
	)
	if keys := g.Ancestors(engine.ObjectRef{Kind: "Service", Namespace: "shop", Name: "web"}); len(keys) != 0 {
		t.Errorf("Service resolved to %v, want no keys (correlation must not depend on --store)", keys)
	}
}

// TestGraphHistory_RoutingLayer is #507 end to end: a feed given the
// routing kinds records a Service and its EndpointSlice, history
// answers Selects/RoutesTo the same shape live does, and the snapshot
// names only CronJob unrecorded.
func TestGraphHistory_RoutingLayer(t *testing.T) {
	t.Parallel()
	st, err := store.Open(filepath.Join(t.TempDir(), "lookout.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()

	pod := testPod("shop", "web-1", "gke-a", "web-7b9d", "")
	pod.Labels = map[string]string{"app": "web"}
	client := fake.NewSimpleClientset(testNode("gke-a"), testRS("shop", "web-7b9d", "web"), pod)
	factory := informers.NewSharedInformerFactory(client, 0)
	feed := newGraphFeed(sameFactory(factory), st.RecordGraphChange, routingKinds...)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = feed.Run(ctx) }()
	go runGraphHistoryLoop(ctx, feed.snapshot, st, 10*time.Millisecond)

	waitFor(t, "initial snapshot", func() bool {
		_, err := feed.snapshot()
		return err == nil
	})

	// Steady state: the Service and its slice appear after baseline, so
	// they reach history through the delta log.
	if _, err := client.CoreV1().Services("shop").Create(ctx,
		testService("shop", "web", map[string]string{"app": "web"}), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DiscoveryV1().EndpointSlices("shop").Create(ctx,
		testSlice("shop", "web-abc", "web", "web-1"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	hasEdge := func(snap *graph.Snapshot, from graph.NodeID, kind graph.EdgeKind, to graph.NodeID) bool {
		return slices.ContainsFunc(snap.Out(from), func(e graph.Edge) bool { return e.Kind == kind && e.To == to })
	}
	var snap *graph.Snapshot
	waitFor(t, "GraphAt holds Selects and RoutesTo", func() bool {
		st.Flush()
		s, err := st.GraphAt(ctx, time.Now())
		if err != nil {
			return false
		}
		svc, ok1 := s.Lookup(graph.KindService, "shop", "web")
		eps, ok2 := s.Lookup(graph.KindEndpointSlice, "shop", "web-abc")
		p, ok3 := s.Lookup(graph.KindPod, "shop", "web-1")
		if !ok1 || !ok2 || !ok3 {
			return false
		}
		if hasEdge(s, svc, graph.EdgeSelects, p) && hasEdge(s, svc, graph.EdgeRoutesTo, eps) && hasEdge(s, eps, graph.EdgeRoutesTo, p) {
			snap = s
			return true
		}
		return false
	})
	if got := snap.Unrecorded(); !slices.Equal(got, []graph.NodeKind{graph.KindCronJob}) {
		t.Errorf("history Unrecorded() = %v, want [CronJob]", got)
	}
	svc, _ := snap.Lookup(graph.KindService, "shop", "web")
	if ref, _ := snap.Resolve(svc); !ref.Observed {
		t.Error("a recorded Service must be observed, not identity-only")
	}

	// A readiness flip on the slice logs no delta row.
	before, err := st.GraphChanges(ctx, time.Time{}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	flip := testSlice("shop", "web-abc", "web", "web-1")
	f := false
	flip.Endpoints[0].Conditions.Ready = &f
	if _, err := client.DiscoveryV1().EndpointSlices("shop").Update(ctx, flip, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	// Then a pod change that does log, as the fence the flip would have
	// landed ahead of.
	if _, err := client.CoreV1().Pods("shop").Create(ctx,
		testPod("shop", "web-2", "gke-a", "web-7b9d", ""), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var after []store.GraphChange
	waitFor(t, "fence row", func() bool {
		st.Flush()
		after, err = st.GraphChanges(ctx, time.Time{}, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		return slices.ContainsFunc(after, func(r store.GraphChange) bool { return r.Name == "web-2" })
	})
	for _, r := range after[len(before):] {
		if r.Kind == "EndpointSlice" {
			t.Errorf("readiness flip logged a delta row: %+v", r)
		}
	}
}
