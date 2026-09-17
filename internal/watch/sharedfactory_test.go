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
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
	"github.com/go-steer/k8s-lookout/pkg/sources/capacity"
	"github.com/go-steer/k8s-lookout/pkg/sources/ingress"
	"github.com/go-steer/k8s-lookout/pkg/sources/k8sevents"
	"github.com/go-steer/k8s-lookout/pkg/sources/objectstate"
	"github.com/go-steer/k8s-lookout/pkg/sources/topologydrift"
)

// watchCounter counts LIST+WATCH streams per resource on a fake
// clientset. One informer opens exactly one watch per resource, so the
// count IS the number of caches the process holds for that type.
type watchCounter struct {
	mu sync.Mutex
	n  map[string]int
}

func (w *watchCounter) install(client *fake.Clientset, resources ...string) {
	w.n = map[string]int{}
	for _, res := range resources {
		client.PrependWatchReactor(res, func(action k8stesting.Action) (bool, watch.Interface, error) {
			w.mu.Lock()
			w.n[action.GetResource().Resource]++
			w.mu.Unlock()
			return false, nil, nil // fall through to the default tracker
		})
	}
}

func (w *watchCounter) get(res string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n[res]
}

// TestSharedFactory_OneWatchPerResource pins the property the whole
// §6.3 shared-factory wiring exists for: sources that read the same
// object type share ONE informer, so the process holds one cache and
// opens one watch per type — never one per consumer.
//
// k8s-events, ingress and capacity all watch core/v1 Events, and
// capacity and the §7.4 pod clearance observer both watch Pods. Before
// the factory was made unconditional and these four were moved onto it,
// this cluster ran 3 event watches and 3 pod watches; the assertions
// below fail on that wiring.
func TestSharedFactory_OneWatchPerResource(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	counter := &watchCounter{}
	counter.install(client, "events", "pods", "nodes")

	factory := informers.NewSharedInformerFactory(client, 0)

	evSrc := k8sevents.New(client, 0)
	evSrc.WithFactory(factory)
	ingSrc := ingress.New(client)
	ingSrc.WithFactory(factory)
	capCfg := capacity.DefaultConfig()
	capCfg.PollInterval = time.Hour // no poll ticks during the test
	capSrc := capacity.New(client, nil, capCfg)
	capSrc.WithFactory(factory)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	all := []sources.Source{evSrc, ingSrc, capSrc}
	var wg sync.WaitGroup
	for _, s := range all {
		wg.Add(1)
		go func(s sources.Source) {
			defer wg.Done()
			_ = s.Run(ctx, func(engine.Signal) {})
		}(s)
	}

	// The pod clearance observer is the fourth rider — it blocks until
	// its cache syncs, which is also the gate that its informer is up.
	obs := newPodClearanceObserver(client, factory)
	if err := obs.Start(ctx); err != nil {
		t.Fatalf("pod clearance observer Start: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if ok, _ := sources.AllSynced(all); ok {
			break
		}
		if time.Now().After(deadline) {
			_, pending := sources.AllSynced(all)
			t.Fatalf("sources did not sync within 10s (waiting on %q)", pending)
		}
		time.Sleep(5 * time.Millisecond)
	}

	for _, tc := range []struct {
		resource string
		readers  string
	}{
		{"events", "k8s-events, ingress, capacity"},
		{"pods", "capacity, pod clearance observer"},
		{"nodes", "capacity"},
	} {
		if got := counter.get(tc.resource); got != 1 {
			t.Errorf("%s: %d watches opened, want exactly 1 — readers (%s) must share one informer, not hold a cache each",
				tc.resource, got, tc.readers)
		}
	}

	cancel()
	wg.Wait()
}

// TestSharedFactory_SourcesFallBackWhenUnset proves the seam stays
// optional: a source handed no factory builds its own, so every source
// remains usable outside the sentinel (and its own package tests keep
// driving it standalone).
func TestSharedFactory_SourcesFallBackWhenUnset(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	counter := &watchCounter{}
	counter.install(client, "events")

	evSrc := k8sevents.New(client, 0)
	ingSrc := ingress.New(client)
	// No WithFactory call on either.

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	all := []sources.Source{evSrc, ingSrc}
	var wg sync.WaitGroup
	for _, s := range all {
		wg.Add(1)
		go func(s sources.Source) {
			defer wg.Done()
			_ = s.Run(ctx, func(engine.Signal) {})
		}(s)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if ok, _ := sources.AllSynced(all); ok {
			break
		}
		if time.Now().After(deadline) {
			_, pending := sources.AllSynced(all)
			t.Fatalf("sources did not sync within 10s (waiting on %q)", pending)
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := counter.get("events"); got != 2 {
		t.Errorf("events: %d watches opened, want 2 — a source with no shared factory must build its own", got)
	}

	cancel()
	wg.Wait()
}

// TestK8sEventsResyncKeepsPrivateFactory: resync is a per-factory
// property, so a source asking for a non-zero resync must NOT be folded
// onto a shared factory built with a different one — it would silently
// lose the cadence it asked for.
func TestK8sEventsResyncKeepsPrivateFactory(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	counter := &watchCounter{}
	counter.install(client, "events")

	factory := informers.NewSharedInformerFactory(client, 0)
	shared := k8sevents.New(client, 0)
	shared.WithFactory(factory)
	resyncing := k8sevents.New(client, 30*time.Second)
	resyncing.WithFactory(factory) // must be declined

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	all := []sources.Source{shared, resyncing}
	var wg sync.WaitGroup
	for _, s := range all {
		wg.Add(1)
		go func(s sources.Source) {
			defer wg.Done()
			_ = s.Run(ctx, func(engine.Signal) {})
		}(s)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if ok, _ := sources.AllSynced(all); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sources did not sync within 10s")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := counter.get("events"); got != 2 {
		t.Errorf("events: %d watches opened, want 2 — the resyncing source must keep its own factory", got)
	}

	cancel()
	wg.Wait()
}

// nodeReaders are the four consumers of the shared Nodes informer, each built
// against a split factory pair. Any new one belongs here: the list is the
// answer to "who has to know about the node carve-out".
var nodeReaders = []struct {
	name  string
	build func(*fake.Clientset, sharedFactories) (run func(context.Context) error, synced func() bool)
}{
	{"capacity", func(client *fake.Clientset, f sharedFactories) (func(context.Context) error, func() bool) {
		cfg := capacity.DefaultConfig()
		cfg.PollInterval = time.Hour // no poll ticks during the test
		s := capacity.New(client, nil, cfg)
		s.WithFactory(f.Namespaced)
		s.WithNodeFactory(f.Cluster)
		return runSourceFn(s), syncedFn(s)
	}},
	{"object-state", func(client *fake.Clientset, f sharedFactories) (func(context.Context) error, func() bool) {
		s := objectstate.New(client, objectstate.Config{TickInterval: time.Hour})
		s.WithFactory(f.Namespaced)
		s.WithNodeFactory(f.Cluster)
		return runSourceFn(s), syncedFn(s)
	}},
	{"topology-drift", func(client *fake.Clientset, f sharedFactories) (func(context.Context) error, func() bool) {
		s := topologydrift.New(client, topologydrift.Config{VerifyInterval: time.Hour})
		s.WithFactory(f.Namespaced)
		s.WithNodeFactory(f.Cluster)
		return runSourceFn(s), syncedFn(s)
	}},
	{"graph feed", func(_ *fake.Clientset, f sharedFactories) (func(context.Context) error, func() bool) {
		// The feed takes the pair itself rather than a WithNodeFactory setter,
		// because it is internal and there is no seam to keep compatible.
		feed := newGraphFeed(f, nil)
		return feed.Run, func() bool { _, err := feed.graph.Snapshot(); return err == nil }
	}},
}

func runSourceFn(s sources.Source) func(context.Context) error {
	return func(ctx context.Context) error { return s.Run(ctx, func(engine.Signal) {}) }
}

func syncedFn(s sources.Source) func() bool {
	return func() bool { ok, _ := sources.AllSynced([]sources.Source{s}); return ok }
}

// TestSharedFactories_SplitEveryNodeReaderStartsIt runs each node reader ALONE
// against a split pair, which is the only arrangement that can catch the bug it
// is here for.
//
// When --exclude-namespace splits the factories, the node informer lives on a
// SECOND factory object, and a reader that starts only its namespaced one
// leaves that informer unstarted: WaitForCacheSync then blocks forever rather
// than erroring, so the production symptom is a sentinel that hangs at startup
// with no log line. Run the readers together and the bug hides — the pair is
// shared, so ONE reader remembering to start the cluster factory covers for
// every reader that forgot.
func TestSharedFactories_SplitEveryNodeReaderStartsIt(t *testing.T) {
	t.Parallel()

	for _, tc := range nodeReaders {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := fake.NewSimpleClientset()
			factories := newSharedFactories(client, []string{"kube-system"})
			if !factories.Split() {
				t.Fatal("test precondition: a deny list must split the factories")
			}
			run, synced := tc.build(client, factories)

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = run(ctx)
			}()

			deadline := time.Now().Add(10 * time.Second)
			for !synced() {
				if time.Now().After(deadline) {
					t.Fatalf("%s did not sync within 10s — it started the namespaced factory but not the cluster-scoped one, so its node informer never listed", tc.name)
				}
				time.Sleep(5 * time.Millisecond)
			}

			cancel()
			<-done
		})
	}
}

// TestSharedFactories_SplitKeepsOneNodeWatch is the other half: splitting for
// the node carve-out must not cost a stream. All four readers together still
// hold exactly one node watch and one pod watch between them — the §6.3
// property, unchanged by the deny list.
func TestSharedFactories_SplitKeepsOneNodeWatch(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	counter := &watchCounter{}
	counter.install(client, "pods", "nodes", "events")

	factories := newSharedFactories(client, []string{"kube-system"})
	if !factories.Split() {
		t.Fatal("test precondition: a deny list must split the factories")
	}

	capCfg := capacity.DefaultConfig()
	capCfg.PollInterval = time.Hour // no poll ticks during the test
	capSrc := capacity.New(client, nil, capCfg)
	capSrc.WithFactory(factories.Namespaced)
	capSrc.WithNodeFactory(factories.Cluster)

	objSrc := objectstate.New(client, objectstate.Config{TickInterval: time.Hour})
	objSrc.WithFactory(factories.Namespaced)
	objSrc.WithNodeFactory(factories.Cluster)

	driftSrc := topologydrift.New(client, topologydrift.Config{VerifyInterval: time.Hour})
	driftSrc.WithFactory(factories.Namespaced)
	driftSrc.WithNodeFactory(factories.Cluster)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	all := []sources.Source{capSrc, objSrc, driftSrc}
	var wg sync.WaitGroup
	for _, s := range all {
		wg.Add(1)
		go func(s sources.Source) {
			defer wg.Done()
			_ = s.Run(ctx, func(engine.Signal) {})
		}(s)
	}

	// The graph feed is the fourth node reader, and the one that takes the
	// factory pair rather than a WithNodeFactory setter.
	feed := newGraphFeed(factories, nil)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = feed.Run(ctx)
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if ok, _ := sources.AllSynced(all); ok {
			break
		}
		if time.Now().After(deadline) {
			_, pending := sources.AllSynced(all)
			t.Fatalf("sources did not sync within 10s (waiting on %q) — a split node factory that is never Started makes this hang, not fail", pending)
		}
		time.Sleep(5 * time.Millisecond)
	}

	for _, tc := range []struct {
		resource string
		readers  string
	}{
		{"nodes", "capacity, object-state, topology-drift, graph feed"},
		{"pods", "capacity, object-state, topology-drift, graph feed"},
	} {
		if got := counter.get(tc.resource); got != 1 {
			t.Errorf("%s: %d watches opened, want exactly 1 — splitting the factory for the node carve-out must not cost a stream (readers: %s)",
				tc.resource, got, tc.readers)
		}
	}

	cancel()
	wg.Wait()
}
