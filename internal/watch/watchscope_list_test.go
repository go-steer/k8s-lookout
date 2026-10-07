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
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// The namespace LIST form of --watch-scope=namespace (#407, the 2026-09-17
// decision): one read shard per namespace, one brain. The fixture is the
// documented default tier — a Role in each listed namespace plus cluster-wide
// read of nodes and persistent volumes — over a cluster with a third
// namespace, "other", that nothing may see.

var listNS = []string{"team-a", "team-b"}

// hybridClient is the hybrid-grant fixture: Roles in the listed namespaces,
// the capacity Role in kube-system, cluster-wide nodes and PVs.
func hybridClient(objs ...runtime.Object) *fake.Clientset {
	return scopedClient(append([]string{"kube-system"}, listNS...), []string{"nodes", "persistentvolumes"}, objs...)
}

func listScopeFlags(t *testing.T, extra ...string) *flags {
	t.Helper()
	f, err := parseFlags(append([]string{"--dry-run", "--watch-scope=namespace", "--namespace=" + strings.Join(listNS, ",")}, extra...))
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	return f
}

// TestWatchScopeNamespaces_HybridShardsOneBrain is the list form's acceptance
// test. Under the hybrid grant the node-dependent sources survive auto and
// storm resolves on; every source and the topology graph sync over the
// union of the shards (bounded — a cluster-wide informer would never sync);
// an event in each listed namespace is emitted exactly once; and nothing
// from "other" reaches a signal or the graph.
func TestWatchScopeNamespaces_HybridShardsOneBrain(t *testing.T) {
	client := hybridClient(
		testNode("node-1"),
		testRS("team-a", "web-1", "web"), testPod("team-a", "web-x", "node-1", "web-1", ""),
		testRS("team-b", "api-1", "api"), testPod("team-b", "api-x", "node-1", "api-1", ""),
		testRS("other", "db-1", "db"), testPod("other", "db-x", "node-1", "db-1", ""),
	)
	f := listScopeFlags(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := admitNamespaces(ctx, f, client, newMetrics()); err != nil {
		t.Fatalf("admitNamespaces: %v", err)
	}
	buf, restore := captureLogOutput(t)
	err := resolveAutoDefaults(ctx, f, client)
	restore()
	if err != nil {
		t.Fatalf("resolveAutoDefaults: %v", err)
	}
	for _, name := range []string{"object-state", "topology-drift", "saturation", "capacity"} {
		if !strings.Contains(","+f.sources+",", ","+name+",") {
			t.Errorf("hybrid grant should keep %s; auto resolved %s\n%s", name, f.sources, buf.String())
		}
	}
	if f.storm != stormOn {
		t.Errorf("--storm=auto resolved %q under the node grant, want on\n%s", f.storm, buf.String())
	}

	// saturation resolved on above; it is polled rather than informer-backed
	// (its scope reaches it through scopedPodFetcher), and its kubelet read
	// needs a REST transport the fake clientset does not have.
	f.sources = strings.Replace(f.sources, ",saturation", "", 1)
	bs, err := buildSources(f, "", client, nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	factories := newSharedFactories(client, nil, f.scopeNamespaces())
	if _, ok := factories.Namespaced.(*unionFactory); !ok {
		t.Fatalf("two namespaces built %T, want the union over two shards", factories.Namespaced)
	}
	attachSharedFactories(bs, factories)
	if _, err := sources.Probe(ctx, newAccessReviewer(f, client), bs.registry.All()...); err != nil {
		t.Fatalf("§11 probe refused the resolved set under the hybrid grant: %v", err)
	}
	if err := probeGraphAccess(ctx, newAccessReviewer(f, client)); err != nil {
		t.Fatalf("probeGraphAccess: %v", err)
	}
	// With the routing layer, as --store would ask for: every informer type
	// the sentinel takes from its namespaced factory is now exercised over
	// the union, so an uncovered one panics here (union.go).
	feed := newGraphFeed(factories, nil, graph.KindService, graph.KindEndpointSlice, graph.KindIngress, graph.KindNetworkPolicy)

	var mu sync.Mutex
	var got []engine.Signal
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = feed.Run(runCtx) }()
	runErr := make(chan error, 1)
	go func() {
		runErr <- sources.RunAll(runCtx, bs.registry.All(), func(s engine.Signal) {
			mu.Lock()
			got = append(got, s)
			mu.Unlock()
		})
	}()
	waitFor(t, "every source and the graph to sync over the union", func() bool {
		ok, _ := sources.AllSynced(bs.registry.All())
		_, gerr := feed.graph.Snapshot()
		return ok && gerr == nil
	})

	// The graph is the union: both listed namespaces' pods hang off the one
	// shared node, so a node failure across them correlates as one; the
	// unlisted namespace is not in it at all.
	snap, err := feed.graph.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	for _, ns := range listNS {
		name := map[string]string{"team-a": "web-x", "team-b": "api-x"}[ns]
		if _, ok := snap.Lookup(graph.KindPod, ns, name); !ok {
			t.Errorf("graph is missing %s/%s from its shard", ns, name)
		}
	}
	if _, ok := snap.Lookup(graph.KindPod, "other", "db-x"); ok {
		t.Error("graph holds a pod from an unlisted namespace")
	}
	if _, ok := snap.Lookup(graph.KindNode, "", "node-1"); !ok {
		t.Error("graph is missing the shared node")
	}

	for _, ns := range []string{"other", "team-a", "team-b"} {
		if _, err := client.CoreV1().Events(ns).Create(ctx, warningEvent(ns, "crash"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create event in %s: %v", ns, err)
		}
	}
	countEvents := func(ns string) int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, s := range got {
			if s.Kind == engine.KindK8sEvent && s.Namespace == ns {
				n++
			}
		}
		return n
	}
	waitFor(t, "an event from each listed namespace", func() bool {
		return countEvents("team-a") > 0 && countEvents("team-b") > 0
	})
	time.Sleep(200 * time.Millisecond)
	for _, ns := range listNS {
		if n := countEvents(ns); n != 1 {
			t.Errorf("event in %s emitted %d times, want exactly once — shards must not duplicate", ns, n)
		}
	}
	mu.Lock()
	for _, s := range got {
		if s.Namespace == "other" {
			t.Errorf("signal from an unlisted namespace: kind=%s name=%s", s.Kind, s.Name)
		}
	}
	mu.Unlock()

	stop()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("RunAll: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("sources did not stop after cancel")
	}
}

// TestWatchScopeNamespaces_NodeFailureAcrossNamespacesIsOneStorm pins "keep
// one brain": three pods in three listed namespaces failing on one node are
// one storm session, because the shards feed one graph and one correlator.
// A runner per namespace would have opened three.
func TestWatchScopeNamespaces_NodeFailureAcrossNamespacesIsOneStorm(t *testing.T) {
	nss := []string{"shop", "web", "api"}
	objs := []runtime.Object{testNode("gke-a")}
	for i, ns := range nss {
		objs = append(objs, testRS(ns, "rs", "dep"), testPod(ns, podName(i+1), "gke-a", "rs", ""))
	}
	client := scopedClient(nss, []string{"nodes"}, objs...)
	factories := newSharedFactories(client, nil, nss)
	// With the routing layer, as --store would ask for: every informer type
	// the sentinel takes from its namespaced factory is now exercised over
	// the union, so an uncovered one panics here (union.go).
	feed := newGraphFeed(factories, nil, graph.KindService, graph.KindEndpointSlice, graph.KindIngress, graph.KindNetworkPolicy)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = feed.Run(ctx) }()
	waitFor(t, "the union graph", func() bool {
		_, err := feed.graph.Snapshot()
		return err == nil
	})

	base, injects := newRoutingFakeDaemon(t)
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	correlator, err := engine.NewStormCorrelator(engine.DefaultStormWindow, engine.DefaultStormMin, feed)
	if err != nil {
		t.Fatalf("NewStormCorrelator: %v", err)
	}
	d := &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nss, nil, 0, 1, 0)),
		dedup:    dedup,
		injector: inj,
		metrics:  newMetrics(),
		cluster:  "prod",
		mode:     "per-incident",
		storm:    correlator,
	}
	for i, ns := range nss {
		sig := stormPodSignal(i+1, ns)
		d.DispatchSignal(ctx, sig)
	}
	if got := testutil.ToFloat64(d.metrics.stormsFormed); got != 1 {
		t.Errorf("storms formed = %v, want 1 across the three namespaces", got)
	}
	storms := 0
	for _, in := range *injects {
		if strings.Contains(messageOf(t, in.Body), `"kind":"storm"`) {
			storms++
		}
	}
	if storms != 1 {
		t.Errorf("storm injects = %d, want exactly one", storms)
	}
}

func podName(i int) string { return "pay-" + string(rune('0'+i)) }

// TestAdmitNamespaces_SkipsADeniedNamespace is #407's skip-vs-abort answer:
// a listed namespace without a Role is skipped — one line, a counted gap —
// and the others are watched; only when every namespace is refused does
// startup fail.
func TestAdmitNamespaces_SkipsADeniedNamespace(t *testing.T) {
	client := scopedClient([]string{"team-a"}, nil)
	f := listScopeFlags(t) // team-a,team-b; only team-a has a Role
	m := newMetrics()
	buf, restore := captureLogOutput(t)
	err := admitNamespaces(context.Background(), f, client, m)
	restore()
	if err != nil {
		t.Fatalf("admitNamespaces = %v, want team-b skipped and team-a watched", err)
	}
	if got := f.scopeNamespaces(); len(got) != 1 || got[0] != "team-a" {
		t.Errorf("admitted %v, want [team-a]", got)
	}
	if line := lineWith(buf.String(), `namespace "team-b" NOT watched`); !strings.Contains(line, "list events in namespace team-b") {
		t.Errorf("skip line missing or does not name the refused grant: %q\n%s", line, buf.String())
	}
	if got := testutil.ToFloat64(m.namespaceErrors.WithLabelValues("team-b", "access_denied")); got != 1 {
		t.Errorf("lookout_namespace_errors_total{team-b} = %v, want 1", got)
	}
	// What is probed afterwards is the admitted set only.
	if reqs := sources.Expand(newAccessReviewer(f, client), sources.Requirement{Resource: "pods", Verb: "list"}); len(reqs) != 1 || reqs[0].Namespace != "team-a" {
		t.Errorf("post-admission probe asks %v, want team-a only", reqs)
	}

	none := scopedClient(nil, nil)
	f2 := listScopeFlags(t)
	err = admitNamespaces(context.Background(), f2, none, newMetrics())
	if err == nil || !errors.Is(err, sources.ErrAccessDenied) {
		t.Fatalf("every namespace refused: admitNamespaces = %v, want a settled access denial", err)
	}
}

// TestUnionInformer_SerialisesDelivery: a SharedInformer promises each
// handler one event at a time, and the union keeps the promise across
// shards that deliver from their own goroutines.
func TestUnionInformer_SerialisesDelivery(t *testing.T) {
	var inFlight, maxInFlight int
	var mu sync.Mutex
	h := &serialHandler{h: handlerFunc(func() {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		time.Sleep(time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
	})}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				h.OnAdd(&corev1.Pod{}, false)
			}
		}()
	}
	wg.Wait()
	if maxInFlight != 1 {
		t.Errorf("handler saw %d concurrent deliveries, want 1", maxInFlight)
	}
}

type handlerFunc func()

func (f handlerFunc) OnAdd(any, bool)   { f() }
func (f handlerFunc) OnUpdate(any, any) { f() }
func (f handlerFunc) OnDelete(any)      { f() }

// TestUnionFactory_ListerReadsEveryShard: the cross-namespace sources
// (topology-drift's inventory, compute-class's shares, capacity's pending
// pods) read through listers, and a union lister must answer for every
// shard — List across all of them, Get and per-namespace List routed to the
// right one — and never for a namespace outside the scope.
func TestUnionFactory_ListerReadsEveryShard(t *testing.T) {
	client := hybridClient(
		testPod("team-a", "a", "n", "", ""),
		testPod("team-b", "b", "n", "", ""),
		testPod("other", "o", "n", "", ""),
	)
	sf := newSharedFactories(client, nil, listNS)
	pods := sf.Namespaced.Core().V1().Pods()
	inf := pods.Informer()
	lister := pods.Lister()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sf.Start(ctx.Done())
	waitFor(t, "the union pod informer", inf.HasSynced)

	all, err := lister.List(labels.Everything())
	if err != nil || len(all) != 2 {
		t.Fatalf("union List = %d pods, %v; want the two listed namespaces' pods", len(all), err)
	}
	for ns, name := range map[string]string{"team-a": "a", "team-b": "b"} {
		if _, err := lister.Pods(ns).Get(name); err != nil {
			t.Errorf("Get %s/%s: %v", ns, name, err)
		}
		if l, _ := lister.Pods(ns).List(labels.Everything()); len(l) != 1 {
			t.Errorf("List in %s = %d, want 1", ns, len(l))
		}
	}
	if _, err := lister.Pods("other").Get("o"); err == nil {
		t.Error("union lister answered for an unlisted namespace")
	}
}
