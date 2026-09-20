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

package topologydrift

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric/noop"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Interface conformance, asserted at compile time the way the other sources do.
var (
	_ sources.Source         = (*Source)(nil)
	_ sources.SyncReporter   = (*Source)(nil)
	_ sources.AccessDeclarer = (*Source)(nil)
)

// replicaSet builds a ReplicaSet controlled by the named Deployment.
func replicaSet(name, namespace, deployment string) *appsv1.ReplicaSet {
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	if deployment != "" {
		yes := true
		rs.OwnerReferences = []metav1.OwnerReference{{Kind: "Deployment", Name: deployment, Controller: &yes}}
	}
	return rs
}

// runSource starts a source against a fake clientset seeded with objs and
// blocks until it reports synced.
func runSource(t *testing.T, cfg Config, objs ...runtime.Object) (*Source, []engine.Signal) {
	t.Helper()
	client := fake.NewSimpleClientset(objs...)
	s := New(client, cfg)
	s.logf = func(string, ...any) {}

	var (
		mu      sync.Mutex
		emitted []engine.Signal
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Run(ctx, func(sig sources.Signal) {
			mu.Lock()
			defer mu.Unlock()
			emitted = append(emitted, sig)
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v, want nil on cancellation", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Run did not return after cancellation")
		}
	})

	waitFor(t, "the source to sync", s.HasSynced)
	mu.Lock()
	defer mu.Unlock()
	return s, slices.Clone(emitted)
}

func TestSource_Contract(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{})

	if s.Name() != "topology-drift" {
		t.Errorf("Name() = %q, want topology-drift", s.Name())
	}
	if s.Scope() != sources.ScopeCluster {
		t.Errorf("Scope() = %v, want cluster — the node inventory is a cluster-wide watch", s.Scope())
	}
	if s.HasSynced() {
		t.Error("HasSynced() is true before Run")
	}

	want := []sources.Requirement{
		{Resource: "pods", Verb: "list"}, {Resource: "pods", Verb: "watch"},
		{Resource: "nodes", Verb: "list"}, {Resource: "nodes", Verb: "watch"},
		{Group: "apps", Resource: "replicasets", Verb: "list"},
		{Group: "apps", Resource: "replicasets", Verb: "watch"},
		{Group: "apps", Resource: "deployments", Verb: "list"},
		{Group: "apps", Resource: "deployments", Verb: "watch"},
		{Group: "apps", Resource: "statefulsets", Verb: "list"},
		{Group: "apps", Resource: "statefulsets", Verb: "watch"},
		{Resource: "persistentvolumeclaims", Verb: "list"},
		{Resource: "persistentvolumeclaims", Verb: "watch"},
		{Resource: "persistentvolumes", Verb: "list"},
		{Resource: "persistentvolumes", Verb: "watch"},
	}
	if got := s.RequiredAccess(); !slices.Equal(got, want) {
		t.Errorf("RequiredAccess() = %+v, want %+v", got, want)
	}
}

func TestConfig_Normalize(t *testing.T) {
	got := Config{}.normalize()
	if !slices.Equal(got.TopologyKeys, DefaultTopologyKeys) {
		t.Errorf("TopologyKeys = %v, want %v", got.TopologyKeys, DefaultTopologyKeys)
	}
	if got.EligibilitySweepInterval != DefaultEligibilitySweepInterval {
		t.Errorf("EligibilitySweepInterval = %v, want %v", got.EligibilitySweepInterval, DefaultEligibilitySweepInterval)
	}

	custom := Config{
		TopologyKeys:             []leeway.TopologyKey{poolKey},
		EligibilitySweepInterval: time.Minute,
	}.normalize()
	if !slices.Equal(custom.TopologyKeys, []leeway.TopologyKey{poolKey}) || custom.EligibilitySweepInterval != time.Minute {
		t.Errorf("a configured value was overwritten: %+v", custom)
	}
}

func TestSource_RunBuildsCountersAndAQuietClusterStaysQuiet(t *testing.T) {
	// The whole pipeline end to end on a cluster with nothing wrong with it:
	// counters built from a live informer set, and not one signal produced.
	// Emission is wired now, so this asserts the absence of findings rather
	// than the absence of a feature.
	objs := []runtime.Object{
		node("n-a", "us-central1-a"),
		node("n-b", "us-central1-b"),
		replicaSet("web-7c9f", "prod", "web"),
		pod("web-1", "prod", "n-a", ownedBy("ReplicaSet", "web-7c9f")),
		pod("web-2", "prod", "n-a", ownedBy("ReplicaSet", "web-7c9f")),
		pod("web-3", "prod", "n-b", ownedBy("ReplicaSet", "web-7c9f")),
		pod("db-1", "prod", "n-b", ownedBy("StatefulSet", "db")),
		pod("bare", "prod", "n-a"), // no controller: not a subject, not counted
	}
	s, emitted := runSource(t, Config{TopologyKeys: []leeway.TopologyKey{zoneKey}}, objs...)

	if len(emitted) != 0 {
		t.Errorf("Phase 2 emitted %d signals, want 0", len(emitted))
	}
	if got := s.Inventory().Len(); got != 2 {
		t.Errorf("inventory holds %d nodes, want 2", got)
	}
	if got := s.State().Len(); got != 4 {
		t.Errorf("state counts %d pods, want 4 (the bare pod is uncounted)", got)
	}

	web := leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "web"}
	dist := s.State().Snapshot(web)[zoneKey]
	if dist == nil {
		t.Fatalf("no zone distribution for %s; subjects = %v", web, s.State().Subjects())
	}
	if got := dist.ByDomain["us-central1-a"].Running; got != 2 {
		t.Errorf("us-central1-a running = %d, want 2", got)
	}
	if got := dist.ByDomain["us-central1-b"].Running; got != 1 {
		t.Errorf("us-central1-b running = %d, want 1", got)
	}

	if got := s.State().SubjectCounts(); got[leeway.SubjectDeployment] != 1 || got[leeway.SubjectStatefulSet] != 1 {
		t.Errorf("SubjectCounts() = %v, want one Deployment and one StatefulSet", got)
	}
}

func TestSource_RunTracksLiveEvents(t *testing.T) {
	client := fake.NewSimpleClientset(
		node("n-a", "us-central1-a"),
		replicaSet("web-7c9f", "prod", "web"),
	)
	s := New(client, Config{TopologyKeys: []leeway.TopologyKey{zoneKey}, CoalesceWindow: tickWindow})
	s.logf = func(string, ...any) {}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx, func(sources.Signal) {}) }()
	waitFor(t, "the source to sync", s.HasSynced)

	p := pod("web-1", "prod", "n-a", ownedBy("ReplicaSet", "web-7c9f"))
	if _, err := client.CoreV1().Pods("prod").Create(ctx, p, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	web := leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "web"}
	waitFor(t, "the new pod to be counted", func() bool {
		d := s.State().Snapshot(web)[zoneKey]
		return d != nil && d.Total == 1
	})

	if err := client.CoreV1().Pods("prod").Delete(ctx, "web-1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	waitFor(t, "the deleted pod to be uncounted", func() bool { return s.State().Len() == 0 })
}

func TestSource_RunFailsWhenTheCacheNeverSyncs(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{})
	s.logf = func(string, ...any) {}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // stop before the initial list can complete

	err := s.Run(ctx, func(sources.Signal) {})
	if err == nil {
		t.Fatal("Run returned nil after a failed cache sync; a source that is not watching must say so")
	}
	if s.HasSynced() {
		t.Error("HasSynced() is true after a failed sync")
	}
}

func TestSource_RunSweepsOnItsTicker(t *testing.T) {
	// The broad half of §6.3's node-event asymmetry, end to end: a node going
	// NotReady must reach the evaluation queue via the ticker rather than
	// synchronously, so a rolling upgrade cannot fan out per node touched.
	client := fake.NewSimpleClientset(
		node("n-a", "us-central1-a"),
		pod("db-0", "prod", "n-a", ownedBy("StatefulSet", "db")),
	)
	s := New(client, Config{
		TopologyKeys:             []leeway.TopologyKey{zoneKey},
		EligibilitySweepInterval: 10 * time.Millisecond,
		CoalesceWindow:           time.Hour, // hold subjects in the queue to be counted
	})
	s.logf = func(string, ...any) {}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx, func(sources.Signal) {}) }()
	waitFor(t, "the source to sync", s.HasSynced)
	drainQueue(s)

	if _, err := client.CoreV1().Nodes().Update(ctx, node("n-a", "us-central1-a", notReady()), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update node: %v", err)
	}
	waitFor(t, "the sweep to re-enqueue the subject", func() bool { return s.queue.Pending() == 1 })
}

func TestSource_RunFailsWhenTheInstrumentsCannotBeDeclared(t *testing.T) {
	// A source that cannot instrument itself fails loudly at startup rather
	// than running blind: §6.5's mismatch counter is an SLI, and a subsystem
	// whose SLI silently never appears is worse than one that does not start.
	s := New(fake.NewSimpleClientset(), Config{
		Meter: brokenMeter{Meter: noop.NewMeterProvider().Meter(MeterName), fail: metricLastEvent},
	})
	s.logf = func(string, ...any) {}

	err := s.Run(context.Background(), func(sources.Signal) {})
	if err == nil {
		t.Fatal("Run succeeded with an undeclarable instrument")
	}
	if !errors.Is(err, errDeclare) {
		t.Errorf("Run returned %v, want the declaration failure", err)
	}
}

func TestSource_WithFactory(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{})

	t.Run("nil is ignored", func(t *testing.T) {
		s.WithFactory(nil)
		if s.factory != nil {
			t.Error("WithFactory(nil) installed a factory")
		}
	})

	t.Run("the shared factory is used and not shut down", func(t *testing.T) {
		// §6.1: leeway adds no informers of its own to a process that already
		// watches pods and nodes. Shutting the shared factory down would stop
		// every other source's handlers with it, so Run must not.
		client := fake.NewSimpleClientset(
			node("n-a", "us-central1-a"),
			replicaSet("web-7c9f", "prod", "web"),
			pod("web-1", "prod", "n-a", ownedBy("ReplicaSet", "web-7c9f")),
		)
		shared := informers.NewSharedInformerFactory(client, 0)
		src := New(client, Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
		src.logf = func(string, ...any) {}
		src.WithFactory(shared)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- src.Run(ctx, func(sources.Signal) {}) }()
		waitFor(t, "the source to sync on the shared factory", src.HasSynced)

		web := leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "web"}
		if d := src.State().Snapshot(web)[zoneKey]; d == nil || d.Total != 1 {
			t.Errorf("the shared factory did not feed the counters: %+v", d)
		}

		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v, want nil", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return; it may be shutting down a factory it does not own")
		}
		shared.Shutdown()
	})
}

func TestSource_WithMeter(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{})

	s.WithMeter(nil)
	if s.cfg.Meter != nil {
		t.Error("WithMeter(nil) installed a meter")
	}

	// §8.4: the meter belongs to the process that serves the scrape endpoint,
	// so the sentinel hands one down after construction.
	m := noop.NewMeterProvider().Meter(MeterName)
	s.WithMeter(m)
	if s.cfg.Meter != m {
		t.Errorf("cfg.Meter = %v, want the supplied meter", s.cfg.Meter)
	}
}

func TestSource_LoggerDefaultsToTheStandardLog(t *testing.T) {
	if New(fake.NewSimpleClientset(), Config{}).logger() == nil {
		t.Error("logger() returned nil with no override")
	}
}

func TestSource_ReapplyCountsPodsWhoseOwnerLandedLate(t *testing.T) {
	// The cold-start race reapply exists for: a pod applied while the
	// ReplicaSet cache was still empty resolves to no subject and is silently
	// uncounted. Nothing in the pod's own stream fixes that — a settled
	// workload produces no further events — so the post-sync re-apply is the
	// only thing standing between the race and a permanently missing subject.
	client := fake.NewSimpleClientset(replicaSet("web-7c9f", "prod", "web"))
	s := New(client, Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	s.logf = func(string, ...any) {}
	s.inv.Upsert(node("n-a", "us-central1-a"))

	p := pod("web-1", "prod", "n-a", ownedBy("ReplicaSet", "web-7c9f"))
	s.state.OnPodAdd(p) // lister still nil: the owner hop cannot be made
	if got := s.state.Len(); got != 0 {
		t.Fatalf("state counted %d pods before the owner cache existed, want 0", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory := informers.NewSharedInformerFactory(client, 0)
	pods, rs := factory.Core().V1().Pods(), factory.Apps().V1().ReplicaSets()
	// Touch both listers before Start: a factory only builds the informers
	// something has asked for, and one created afterwards never runs.
	podLister, rsLister := pods.Lister(), rs.Lister()
	if _, err := client.CoreV1().Pods("prod").Create(ctx, p, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
	// Shutdown blocks until the informer goroutines exit, so it has to run
	// after the cancel, not before it.
	defer func() { cancel(); factory.Shutdown() }()

	s.mu.Lock()
	s.replicaSets = rsLister
	s.mu.Unlock()

	if err := s.reapply(podLister); err != nil {
		t.Fatalf("reapply: %v", err)
	}
	web := leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "web"}
	if d := s.state.Snapshot(web)[zoneKey]; d == nil || d.Total != 1 {
		t.Errorf("the late-owner pod is still uncounted: %+v", d)
	}
}

func TestSource_OwnerOf(t *testing.T) {
	client := fake.NewSimpleClientset(
		replicaSet("web-7c9f", "prod", "web"),
		replicaSet("orphan", "prod", ""),
	)
	s := New(client, Config{})

	t.Run("before Run there is no lister, so nothing resolves", func(t *testing.T) {
		if _, ok := s.ownerOf("prod", "ReplicaSet", "web-7c9f"); ok {
			t.Error("resolved an owner with no ReplicaSet cache")
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory := informers.NewSharedInformerFactory(client, 0)
	rsLister := factory.Apps().V1().ReplicaSets().Lister() // before Start; see above
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
	// Shutdown blocks until the informer goroutines exit, so it has to run
	// after the cancel, not before it.
	defer func() { cancel(); factory.Shutdown() }()
	s.mu.Lock()
	s.replicaSets = rsLister
	s.mu.Unlock()

	t.Run("a ReplicaSet resolves to its controlling Deployment", func(t *testing.T) {
		owner, ok := s.ownerOf("prod", "ReplicaSet", "web-7c9f")
		if !ok || owner == nil || owner.Kind != "Deployment" || owner.Name != "web" {
			t.Errorf("ownerOf = (%+v, %v), want the web Deployment", owner, ok)
		}
	})
	t.Run("a ReplicaSet with no controller is found but ownerless", func(t *testing.T) {
		owner, ok := s.ownerOf("prod", "ReplicaSet", "orphan")
		if !ok || owner != nil {
			t.Errorf("ownerOf = (%+v, %v), want (nil, true)", owner, ok)
		}
	})
	t.Run("an absent ReplicaSet is not found", func(t *testing.T) {
		if _, ok := s.ownerOf("prod", "ReplicaSet", "gone"); ok {
			t.Error("resolved a ReplicaSet that is not in the cache")
		}
	})
	t.Run("other kinds are never looked up", func(t *testing.T) {
		// resolveSubject only ever asks about ReplicaSets; anything else
		// reaching here would be a caller bug, not a cache miss.
		if _, ok := s.ownerOf("prod", "StatefulSet", "db"); ok {
			t.Error("ownerOf answered for a kind it does not resolve")
		}
	})
}

func TestSource_SweepDefersBroadNodeChanges(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	s.logf = func(string, ...any) {}
	s.inv.Upsert(node("n-a", "us-central1-a"))
	s.state.OnPodAdd(pod("db-1", "prod", "n-a", ownedBy("StatefulSet", "db")))
	drainQueue(s)

	t.Run("nothing pending means no work", func(t *testing.T) {
		s.sweep()
		if got := s.queue.Pending(); got != 0 {
			t.Errorf("sweep enqueued %d subjects with no eligibility change", got)
		}
	})

	t.Run("a narrow change does not arm the sweep", func(t *testing.T) {
		// A relabel is applied immediately by State.OnNodeUpsert because it
		// moves counts; it can also move the eligible set, in which case the
		// sweep is armed too. A pure status heartbeat must arm nothing.
		s.noteChange(Change{})
		s.sweep()
		if got := s.queue.Pending(); got != 0 {
			t.Errorf("sweep enqueued %d subjects for a no-op change", got)
		}
	})

	t.Run("an eligibility change enqueues every subject, once", func(t *testing.T) {
		s.noteChange(Change{EligibilityChanged: true})
		s.noteChange(Change{EligibilityChanged: true}) // a rolling upgrade, many nodes
		s.sweep()
		if got := s.queue.Pending(); got != 1 {
			t.Errorf("sweep enqueued %d subjects, want the one tracked subject", got)
		}
		// And the flag is consumed: the next tick is quiet again.
		drainQueue(s)
		s.sweep()
		if got := s.queue.Pending(); got != 0 {
			t.Errorf("the sweep flag was not consumed: %d subjects enqueued", got)
		}
	})
}

// drainQueue empties the coalescer without running it.
func drainQueue(s *Source) {
	s.queue.mu.Lock()
	defer s.queue.mu.Unlock()
	s.queue.pending = map[leeway.SubjectRef]*pendingEntry{}
	s.queue.timers = nil
}

func TestSource_NodeEventsArmTheSweepThroughTheHandler(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	s.logf = func(string, ...any) {}
	ctx := context.Background()

	s.onNode(ctx, node("n-a", "us-central1-a"))
	if !s.sweepArmed() {
		t.Error("a new node did not arm the sweep")
	}
	s.mu.Lock()
	s.sweepPending = false
	s.mu.Unlock()

	// A heartbeat-only update changes no retained fact, so nothing is armed.
	s.onNode(ctx, node("n-a", "us-central1-a"))
	if s.sweepArmed() {
		t.Error("an unchanged node armed the sweep")
	}

	s.onNode(ctx, node("n-a", "us-central1-a", notReady()))
	if !s.sweepArmed() {
		t.Error("a node going NotReady did not arm the sweep")
	}
	s.mu.Lock()
	s.sweepPending = false
	s.mu.Unlock()

	s.onNodeDelete(ctx, node("n-a", "us-central1-a", notReady()))
	if !s.sweepArmed() {
		t.Error("a node removal did not arm the sweep")
	}
	if got := s.inv.Len(); got != 0 {
		t.Errorf("inventory holds %d nodes after the delete, want 0", got)
	}
}

func (s *Source) sweepArmed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sweepPending
}

func TestSource_HandlersIgnoreForeignObjects(t *testing.T) {
	// An informer handler is the wrong place to panic. A type it cannot use is
	// dropped; a tombstone is unwrapped by the State handlers themselves.
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	ctx := context.Background()
	s.onPod(ctx, &corev1.Node{})
	s.onNode(ctx, &corev1.Pod{})
	if s.state.Len() != 0 || s.inv.Len() != 0 {
		t.Error("a foreign object changed the indexes")
	}
}

func TestSource_MetricProjections(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	s.inv.Upsert(node("n-a", "us-central1-a"))
	s.inv.Upsert(node("n-b", "us-central1-a"))
	s.inv.Upsert(node("n-c", "us-central1-b", cordoned()))
	s.state.OnPodAdd(pod("db-0", "prod", "n-a", ownedBy("StatefulSet", "db")))
	s.state.OnPodAdd(pod("db-1", "prod", "n-c", ownedBy("StatefulSet", "db"), terminating()))

	t.Run("domainNodes reports usable nodes, not all nodes", func(t *testing.T) {
		got := s.domainNodes()[zoneKey]
		if got["us-central1-a"] != 2 {
			t.Errorf("us-central1-a = %d usable, want 2", got["us-central1-a"])
		}
		if got["us-central1-b"] != 0 {
			t.Errorf("us-central1-b = %d usable, want 0 — its only node is cordoned", got["us-central1-b"])
		}
	})

	t.Run("domainObjects yields one row per non-zero state", func(t *testing.T) {
		type row struct {
			domain leeway.Domain
			state  leeway.CountState
			n      int64
		}
		var rows []row
		s.domainObjects(PerDomainGate{All: true}, func(sub leeway.SubjectRef, key leeway.TopologyKey, d leeway.Domain, st leeway.CountState, n int64) {
			if sub.Name != "db" || key != zoneKey {
				t.Errorf("unexpected row for %s on %s", sub, key)
			}
			rows = append(rows, row{d, st, n})
		})
		want := []row{
			{"us-central1-a", leeway.StateRunning, 1},
			{"us-central1-b", leeway.StateTerminating, 1},
		}
		slices.SortFunc(rows, func(a, b row) int { return int(a.state) - int(b.state) })
		if !slices.Equal(rows, want) {
			t.Errorf("rows = %+v, want %+v", rows, want)
		}
	})
}

func TestSource_EvaluateIsWiredToTheQueue(t *testing.T) {
	// What has to hold is that the delta path reaches evaluate at all. This
	// source has no pod lister — it is driven straight through State — so the
	// evaluation finds no representative and resolves nothing, which is the
	// point: the wiring is separable from what hangs off it.
	s := New(fake.NewSimpleClientset(), Config{
		TopologyKeys: []leeway.TopologyKey{zoneKey}, CoalesceWindow: tickWindow,
	})
	if err := s.startMetrics(); err != nil {
		t.Fatalf("startMetrics: %v", err)
	}
	defer func() { _ = s.metrics.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.queue.Run(ctx)

	s.inv.Upsert(node("n-a", "us-central1-a"))
	s.state.OnPodAdd(pod("db-0", "prod", "n-a", ownedBy("StatefulSet", "db")))

	waitFor(t, "the pod event to reach an evaluation", func() bool {
		_, evaluated := s.queue.stats()
		return evaluated == 1
	})
}

// TestSource_RunVerifiesOnItsTicker is §6.5 end to end: the auditor is wired
// into Run, it reads the informer's pod cache, and what it finds it repairs.
func TestSource_RunVerifiesOnItsTicker(t *testing.T) {
	client := fake.NewSimpleClientset(
		node("n-a", "us-central1-a"),
		pod("db-0", "prod", "n-a", ownedBy("StatefulSet", "db")),
	)
	s := New(client, Config{
		TopologyKeys:   []leeway.TopologyKey{zoneKey},
		VerifyInterval: 10 * time.Millisecond,
		VerifyShards:   1,
	})
	var (
		mu     sync.Mutex
		logged []string
	)
	s.logf = func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		logged = append(logged, fmt.Sprintf(format, args...))
	}

	if got := s.Verifier().Shards(); got != 1 {
		t.Fatalf("Verifier().Shards() = %d, want the configured 1", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx, func(sources.Signal) {}) }()
	waitFor(t, "the source to sync", s.HasSynced)

	// A pod the cluster does not have, counted as though a delete had been
	// missed. Nothing short of the verifier can get rid of it.
	s.State().OnPodAdd(pod("db-1", "prod", "n-a", ownedBy("StatefulSet", "db")))
	if got := s.State().Len(); got != 2 {
		t.Fatalf("Len = %d after the injected pod, want 2", got)
	}

	waitFor(t, "the verifier to drop the phantom pod", func() bool { return s.State().Len() == 1 })

	mu.Lock()
	defer mu.Unlock()
	if !slices.ContainsFunc(logged, func(l string) bool {
		return strings.Contains(l, "verify shard 0/1: 1 of 1 subject(s) drifted")
	}) {
		t.Errorf("no shard summary line:\n%s", strings.Join(logged, "\n"))
	}
}

// TestSource_CachedPodsBeforeRunIsAnError. An empty answer here would tell the
// verifier the cluster has no pods, and it would "repair" every counter to zero.
func TestSource_CachedPodsBeforeRunIsAnError(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{})
	pods, err := s.cachedPods()
	if err == nil {
		t.Fatalf("cachedPods before Run returned %d pods and no error", len(pods))
	}
	if !strings.Contains(err.Error(), "pod cache not ready") {
		t.Errorf("error = %v", err)
	}
}

// evalConfig is a config whose evaluation queue actually drains inside a test's
// patience.
//
// Setting CoalesceWindow alone is not enough, and the way it fails is worth
// recording: §6.4 widens a subject's wait to RolloutCoalesceWindow the moment it
// is enqueued a second time while still pending, and start-up produces exactly
// that whenever the pod's event is delivered before its node's — the pod is
// counted as unknown on every axis, then re-counted by remapNode when the node
// lands. Two enqueues, one subject, and the default 15s widening. Which way
// that race falls is not deterministic, so a test that leaves the rollout
// window at its default passes or hangs depending on informer scheduling.
func evalConfig(keys ...leeway.TopologyKey) Config {
	return Config{
		TopologyKeys:          keys,
		CoalesceWindow:        tickWindow,
		RolloutCoalesceWindow: tickWindow,
		MaxCoalesceDelay:      tickWindow,
	}
}

// spreadPod is a Deployment-owned pod carrying a zone spread constraint, which
// is the shape every test below infers from.
func spreadPod(name string, created time.Time) *corev1.Pod {
	p := pod(name, "prod", "n-a", ownedBy("ReplicaSet", "web-7c9f"))
	p.Labels = map[string]string{"app": "api"}
	p.CreationTimestamp = metav1.NewTime(created)
	p.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{
		zoneSpread(1, corev1.DoNotSchedule),
	}
	return p
}

// TestSource_EvaluateResolvesIntentFromTheRepresentative is Phase 3's assembly
// end to end: a pod event reaches evaluate, evaluate finds a pod for the
// subject through State's hint, and the intent inferred from it lands where the
// intent_info scrape reads.
func TestSource_EvaluateResolvesIntentFromTheRepresentative(t *testing.T) {
	s, _ := runSource(t,
		evalConfig(zoneKey),
		node("n-a", "us-central1-a"),
		replicaSet("web-7c9f", "prod", "web"),
		spreadPod("web-1", time.Now()),
	)

	waitFor(t, "the subject's intent to be resolved", func() bool {
		return s.state.IntentsOf(webSubject) != nil
	})

	in := s.state.IntentsOf(webSubject)[zoneKey]
	if in == nil {
		t.Fatalf("no zone intent: %+v", s.state.IntentsOf(webSubject))
	}
	if in.Source != leeway.SourceTopologySpreadConstraint {
		t.Errorf("Source = %v, want the spread constraint", in.Source)
	}
	if in.MaxSkew == nil || *in.MaxSkew != 1 {
		t.Errorf("MaxSkew = %v, want 1", in.MaxSkew)
	}

	var seen int
	s.state.EachIntent(func(leeway.SubjectRef, leeway.TopologyKey, *leeway.Intent) { seen++ })
	if seen != 1 {
		t.Errorf("EachIntent yielded %d rows, want the one intent_info series", seen)
	}
}

// TestSource_SubjectPodRecoversFromAStaleHint covers the miss path. The hint is
// allowed to be wrong — that is what makes it cheap — so what has to hold is
// that a wrong one costs one list and then repairs itself.
func TestSource_SubjectPodRecoversFromAStaleHint(t *testing.T) {
	s, _ := runSource(t,
		evalConfig(zoneKey),
		node("n-a", "us-central1-a"),
		replicaSet("web-7c9f", "prod", "web"),
		spreadPod("web-1", time.Now()),
	)
	waitFor(t, "the subject to be tracked", func() bool {
		_, ok := s.state.Representative(webSubject)
		return ok
	})

	s.state.SetRepresentative(webSubject, types.UID("prod/web-gone"), "web-gone")
	if got := s.subjectPod(webSubject); got == nil || got.Name != "web-1" {
		t.Fatalf("subjectPod = %v, want the pod the fallback list found", got)
	}
	if got, _ := s.state.Representative(webSubject); got != "web-1" {
		t.Errorf("Representative = %q after the miss, want the re-elected web-1", got)
	}

	// A subject with no pods at all in the cache is a miss the fallback cannot
	// fix, and it must say so rather than guess.
	other := leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "absent"}
	if got := s.subjectPod(other); got != nil {
		t.Errorf("subjectPod(absent) = %v, want nil", got)
	}
}

// TestSource_ElectRepresentativePicksTheNewest: during a rollout the pod being
// replaced still carries the template the workload has already abandoned, so
// reading it would describe intent that is on its way out.
func TestSource_ElectRepresentativePicksTheNewest(t *testing.T) {
	old := spreadPod("web-old", time.Now().Add(-time.Hour))
	fresh := spreadPod("web-new", time.Now())
	// Same name prefix, different template: only the newer pod spreads on the
	// pool axis, so which one was read is visible in the result.
	fresh.Spec.TopologySpreadConstraints[0].TopologyKey = string(poolKey)

	s, _ := runSource(t,
		evalConfig(zoneKey, poolKey),
		node("n-a", "us-central1-a"),
		replicaSet("web-7c9f", "prod", "web"),
		old, fresh,
	)
	waitFor(t, "the subject to be tracked", func() bool {
		return len(s.state.Subjects()) == 1
	})

	// Clear the hint so election has to run, rather than testing whichever pod
	// the informer happened to deliver last.
	s.state.SetRepresentative(webSubject, types.UID("prod/web-gone"), "web-gone")
	got := s.subjectPod(webSubject)
	if got == nil || got.Name != "web-new" {
		t.Fatalf("elected %v, want web-new", got)
	}
}

// TestNewerPod covers the tiebreak directly, because the case it exists for is
// the one a fixture cannot reliably produce: creation timestamps have
// one-second resolution, so a scale-out puts a whole ReplicaSet on the same
// value and the election has to stay reproducible anyway.
func TestNewerPod(t *testing.T) {
	now := time.Now()
	a := spreadPod("web-a", now)
	b := spreadPod("web-b", now)
	if !newerPod(a, b) || newerPod(b, a) {
		t.Errorf("same timestamp: want the name to break the tie, got %v/%v", newerPod(a, b), newerPod(b, a))
	}

	older := spreadPod("web-z", now.Add(-time.Minute))
	if newerPod(older, a) || !newerPod(a, older) {
		t.Error("an older pod won on a name comparison the timestamp should have settled")
	}
}

// TestSource_SubjectPodBeforeRun. The informer handlers are registered before
// the listers are, so an evaluation can be dispatched with no cache to read.
func TestSource_SubjectPodBeforeRun(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{})
	if got := s.subjectPod(webSubject); got != nil {
		t.Errorf("subjectPod before Run = %v, want nil", got)
	}
}
