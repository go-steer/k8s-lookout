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
	"fmt"
	"log"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the source's stable config- and schema-facing name.
const Name = "topology-drift"

// DefaultEligibilitySweepInterval bounds how often a broad node change — a
// node going NotReady, a taint appearing, allocatable shrinking — causes every
// subject to be re-evaluated.
//
// §6.3 calls this the cheap half of the node-event asymmetry, and the interval
// is what makes it cheap: a rolling node upgrade touches thousands of nodes,
// and enqueuing 20,000 subjects per node touched is the one way this source
// could take a cluster down. The narrow half — a node's topology labels
// actually moving — is applied immediately, because it changes counts.
const DefaultEligibilitySweepInterval = 5 * time.Minute

// DefaultTopologyKeys are the axes tracked when none are configured (§10.2).
var DefaultTopologyKeys = []leeway.TopologyKey{
	corev1.LabelTopologyZone,
	corev1.LabelTopologyRegion,
}

// Config configures the source. Zero-valued fields take the shipped defaults.
type Config struct {
	// TopologyKeys are the axes counted, in order. Empty takes
	// DefaultTopologyKeys.
	TopologyKeys []leeway.TopologyKey

	// CoalesceWindow, RolloutCoalesceWindow and MaxCoalesceDelay configure
	// §6.4's evaluation queue.
	CoalesceWindow        time.Duration
	RolloutCoalesceWindow time.Duration
	MaxCoalesceDelay      time.Duration

	// EligibilitySweepInterval bounds the broad-node-change sweep.
	EligibilitySweepInterval time.Duration

	// PerDomainSeries exports the per-subject, per-domain counts. See
	// metricsOptions.PerDomainSeries for why this is off by default.
	PerDomainSeries bool

	// Meter is where the §8.4 instruments are declared. Nil means no-op, so
	// the source is usable in tests and in a process with no telemetry.
	Meter metric.Meter
}

func (c Config) normalize() Config {
	if len(c.TopologyKeys) == 0 {
		c.TopologyKeys = DefaultTopologyKeys
	}
	if c.EligibilitySweepInterval <= 0 {
		c.EligibilitySweepInterval = DefaultEligibilitySweepInterval
	}
	return c
}

// Source implements sources.Source for the topology-drift row of §3.
//
// **Phase 2 emits nothing.** Run takes an emit callback to satisfy the
// interface and never calls it. That is the whole reason this phase can ship
// default-on ahead of the detection half: the source maintains its counters
// against live traffic, exports them as metrics, and cannot produce a finding —
// so a bug here is a wrong number on a dashboard nobody alerts on, not a page.
// Intent inference lands in Phase 3 and findings in Phase 4.
type Source struct {
	client kubernetes.Interface
	cfg    Config

	// factory, when set via WithFactory, is the externally owned shared
	// informer factory (§6.1: leeway adds no informers of its own to a
	// process that already watches pods and nodes).
	factory informers.SharedInformerFactory

	inv     *Inventory
	state   *State
	queue   *coalescer
	metrics *instruments

	mu sync.Mutex
	// armed flips true after every informer cache syncs and the initial
	// re-apply has run.
	armed bool
	// replicaSets resolves the pod → ReplicaSet → Deployment hop. Set in Run.
	replicaSets appslisters.ReplicaSetLister
	// sweepPending records that some node's eligibility moved since the last
	// sweep. See DefaultEligibilitySweepInterval.
	sweepPending bool

	// logf overrides log.Printf for testing. nil = log.Printf.
	logf func(format string, args ...any)
}

// logger returns the configured log sink.
func (s *Source) logger() func(string, ...any) {
	if s.logf != nil {
		return s.logf
	}
	return log.Printf
}

// New constructs the source.
func New(client kubernetes.Interface, cfg Config) *Source {
	cfg = cfg.normalize()
	s := &Source{client: client, cfg: cfg}
	s.inv = NewInventory(cfg.TopologyKeys)
	s.queue = newCoalescer(CoalesceOptions{
		Window:        cfg.CoalesceWindow,
		RolloutWindow: cfg.RolloutCoalesceWindow,
		MaxDelay:      cfg.MaxCoalesceDelay,
		Evaluate:      s.evaluate,
	})
	s.state = NewState(StateOptions{
		Inventory: s.inv,
		Owners:    s.ownerOf,
		Enqueue:   s.queue.Enqueue,
	})
	return s
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: the node inventory of §6.2 is built from a
// cluster-wide node watch, so a namespace-tier deployment gets the loud §11
// startup failure rather than a distribution over one visible domain.
func (s *Source) Scope() sources.Scope { return sources.ScopeCluster }

// WithFactory directs Run to register its handlers on an externally owned
// shared factory. Call before Run; nil is ignored.
func (s *Source) WithFactory(f informers.SharedInformerFactory) {
	if f != nil {
		s.factory = f
	}
}

// WithMeter directs Run to declare its §8.4 instruments against an externally
// owned meter. Call before Run; nil is ignored.
//
// Separate from Config.Meter for the same reason WithFactory is separate from
// the client: the meter belongs to the process that serves the scrape endpoint,
// not to the configuration an operator writes, and the sentinel builds it one
// layer above the code that decides which sources to construct.
func (s *Source) WithMeter(m metric.Meter) {
	if m != nil {
		s.cfg.Meter = m
	}
}

// RequiredAccess implements sources.AccessDeclarer (§11).
func (s *Source) RequiredAccess() []sources.Requirement {
	var reqs []sources.Requirement
	for _, r := range []struct{ group, resource string }{
		{"", "pods"},
		{"", "nodes"},
		{"apps", "replicasets"},
	} {
		for _, verb := range []string{"list", "watch"} {
			reqs = append(reqs, sources.Requirement{Group: r.group, Resource: r.resource, Verb: verb})
		}
	}
	return reqs
}

// HasSynced implements sources.SyncReporter. Reporting unsynced until the
// initial re-apply has run is the honest answer: until then the counters are a
// partial cluster, and a reader cannot tell a half-built distribution from a
// genuinely lopsided one.
func (s *Source) HasSynced() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.armed
}

// State exposes the counters for the §6.5 verifier and for tests.
func (s *Source) State() *State { return s.state }

// Inventory exposes the node-side state for the §6.5 verifier and for tests.
func (s *Source) Inventory() *Inventory { return s.inv }

// ownerOf implements OwnerLookup against the ReplicaSet cache. Before Run
// populates the lister — and for any kind other than ReplicaSet, which the
// resolver never asks about — it reports no owner, so the pod goes uncounted
// and is retried on its next event.
func (s *Source) ownerOf(namespace, kind, name string) (*metav1.OwnerReference, bool) {
	if kind != "ReplicaSet" {
		return nil, false
	}
	s.mu.Lock()
	lister := s.replicaSets
	s.mu.Unlock()
	if lister == nil {
		return nil, false
	}
	rs, err := lister.ReplicaSets(namespace).Get(name)
	if err != nil {
		return nil, false
	}
	return controllerOf(rs.OwnerReferences), true
}

// evaluate is the coalesced per-subject callback.
//
// Phase 2 has nothing to evaluate — intent inference is Phase 3 — so this only
// times the dispatch. The queue still runs, and deliberately: it is the piece
// whose behaviour under a 200-pod rollout has to be right before anything
// expensive hangs off it, and running it now means the same property tests that
// exercise the counters exercise it too.
func (s *Source) evaluate(ctx context.Context, sub leeway.SubjectRef) {
	start := time.Now()
	defer func() { s.metrics.recordEvaluation(ctx, sub.Kind, time.Since(start)) }()

	// Phase 3 hangs intent inference and scoring here. Until then the recorded
	// duration is real but tiny, which is the truthful reading: dispatch costs
	// what it costs and evaluation costs nothing yet.
}

// Run implements sources.Source. emit is never called; see the Source doc.
func (s *Source) Run(ctx context.Context, _ func(sources.Signal)) error {
	factory, owned := s.factory, false
	if factory == nil {
		factory = informers.NewSharedInformerFactory(s.client, 0)
		owned = true
	}

	podInformer := factory.Core().V1().Pods()
	nodeInformer := factory.Core().V1().Nodes()
	rsInformer := factory.Apps().V1().ReplicaSets()

	// The ReplicaSet cache is read, not watched: the resolver asks it for one
	// object at a time and nothing here reacts to a ReplicaSet changing.
	// Touching the Lister before Start is what gets the informer built and
	// started with the rest.
	s.mu.Lock()
	s.replicaSets = rsInformer.Lister()
	s.mu.Unlock()

	podH, err := podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.onPod(ctx, obj) },
		UpdateFunc: func(_, obj any) { s.onPod(ctx, obj) },
		DeleteFunc: func(obj any) { s.onPodDelete(ctx, obj) },
	})
	if err != nil {
		return fmt.Errorf("topologydrift: register pod handler: %w", err)
	}
	nodeH, err := nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.onNode(ctx, obj) },
		UpdateFunc: func(_, obj any) { s.onNode(ctx, obj) },
		DeleteFunc: func(obj any) { s.onNodeDelete(ctx, obj) },
	})
	if err != nil {
		return fmt.Errorf("topologydrift: register node handler: %w", err)
	}

	if err := s.startMetrics(); err != nil {
		return err
	}
	defer func() { _ = s.metrics.Close() }()

	factory.Start(ctx.Done())
	if owned {
		defer factory.Shutdown()
	}

	if !cache.WaitForCacheSync(ctx.Done(),
		podH.HasSynced, nodeH.HasSynced, rsInformer.Informer().HasSynced) {
		return fmt.Errorf("topologydrift: cache sync failed (informer stopped before initial list completed)")
	}

	if err := s.reapply(podInformer.Lister()); err != nil {
		return err
	}
	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()

	go s.queue.Run(ctx)

	ticker := time.NewTicker(s.cfg.EligibilitySweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.sweep()
		}
	}
}

// startMetrics declares the §8.4 instruments against the configured meter.
func (s *Source) startMetrics() error {
	in, err := newInstruments(metricsOptions{
		Meter:           s.cfg.Meter,
		PerDomainSeries: s.cfg.PerDomainSeries,
		SubjectCounts:   s.state.SubjectCounts,
		DomainNodes:     s.domainNodes,
		DomainObjects:   s.domainObjects,
	})
	if err != nil {
		return err
	}
	s.metrics = in
	return nil
}

// reapply rebuilds the counters from the synced pod cache.
//
// The handlers registered above already replayed the cache as adds, so most of
// this is a no-op — but not all of it, and the part that is not is the point.
// A pod whose ReplicaSet had not yet landed in its own cache resolved to no
// subject and went uncounted; there is no ordering guarantee between two
// informers' initial lists. Re-applying once both are synced closes that
// window, and OnPodAdd is idempotent by construction precisely so this is safe
// to do. Steady state has no equivalent race worth handling here — a pod is
// created after its ReplicaSet and gets several more events within seconds —
// and §6.5's verifier is the backstop for whatever slips through anyway.
func (s *Source) reapply(lister corelisters.PodLister) error {
	pods, err := lister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("topologydrift: initial pod list: %w", err)
	}
	for _, p := range pods {
		s.state.OnPodAdd(p)
	}
	s.logger()("topology-drift: counters built from %d pods across %d nodes: %d subjects tracked",
		len(pods), s.inv.Len(), s.state.Len())
	return nil
}

// sweep re-enqueues every subject after a broad node change. It is a no-op
// when nothing eligibility-relevant has happened since the last tick, which is
// the common case.
func (s *Source) sweep() {
	s.mu.Lock()
	pending := s.sweepPending
	s.sweepPending = false
	s.mu.Unlock()
	if !pending {
		return
	}
	subs := s.state.Subjects()
	for _, sub := range subs {
		s.queue.Enqueue(sub)
	}
	s.logger()("topology-drift: node eligibility changed, re-enqueuing %d subjects (inventory generation %d)",
		len(subs), s.inv.Generation())
}

func (s *Source) onPod(ctx context.Context, obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	s.state.OnPodAdd(pod)
	s.metrics.recordEvent(ctx, resourcePod, time.Now())
}

func (s *Source) onPodDelete(ctx context.Context, obj any) {
	s.state.OnPodDelete(obj)
	s.metrics.recordEvent(ctx, resourcePod, time.Now())
}

func (s *Source) onNode(ctx context.Context, obj any) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return
	}
	s.noteChange(s.state.OnNodeUpsert(node))
	s.metrics.recordEvent(ctx, resourceNode, time.Now())
}

func (s *Source) onNodeDelete(ctx context.Context, obj any) {
	s.noteChange(s.state.OnNodeDelete(obj))
	s.metrics.recordEvent(ctx, resourceNode, time.Now())
}

// noteChange applies the §6.3 node-event asymmetry. The narrow half is already
// done by the time we get here — State.OnNodeUpsert re-maps the affected pods
// and enqueues their subjects synchronously, because those are counts and
// counts cannot wait. The broad half only changes which domains a subject
// *could* have used, which nothing reads until the next evaluation, so it is
// deferred to the sweep.
func (s *Source) noteChange(ch Change) {
	if !ch.EligibilityChanged {
		return
	}
	s.mu.Lock()
	s.sweepPending = true
	s.mu.Unlock()
}

// domainNodes reports usable nodes per domain, per axis, for
// lookout.leeway.domain_ready_nodes.
func (s *Source) domainNodes() map[leeway.TopologyKey]map[leeway.Domain]int64 {
	keys := s.inv.Keys()
	out := make(map[leeway.TopologyKey]map[leeway.Domain]int64, len(keys))
	for _, key := range keys {
		byDomain := make(map[leeway.Domain]int64)
		for domain, st := range s.inv.Stats(key) {
			byDomain[domain] = st.Usable
		}
		out[key] = byDomain
	}
	return out
}

// domainObjects walks every non-zero per-domain count for
// lookout.leeway.domain_objects.
//
// Snapshots subject by subject rather than holding the index lock across the
// whole walk: a scrape must not be able to stall the delta path. The cost is
// that two subjects in one scrape can be a few microseconds apart, which for a
// gauge of a moving count is not a cost at all.
func (s *Source) domainObjects(yield countObserver) {
	for _, sub := range s.state.Subjects() {
		for key, dist := range s.state.Snapshot(sub) {
			for _, domain := range dist.Domains() {
				c := dist.ByDomain[domain]
				for _, sc := range []struct {
					state leeway.CountState
					n     int64
				}{
					{leeway.StateRunning, c.Running},
					{leeway.StatePending, c.Pending},
					{leeway.StateUnschedulable, c.Unschedulable},
					{leeway.StateTerminating, c.Terminating},
				} {
					if sc.n != 0 {
						yield(sub, key, domain, sc.state, sc.n)
					}
				}
			}
		}
	}
}
