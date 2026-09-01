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

// Package objectstate is the object-state signal source (DESIGN.md
// §7.2 row 2): leading indicators from STATE TRANSITIONS, observed by
// shared informers on Pods, Nodes, Deployments, EndpointSlices, and
// PodDisruptionBudgets. Where k8s-events reacts to what the control
// plane already reported, object-state fires on the transition itself
// — a node flipping NotReady, a kubelet pressure condition setting
// in, a burst of evictions on one node, a rollout approaching (not
// yet exceeding) its progress deadline, a Service's ready-endpoint
// count hitting zero, a PDB gridlocking, a pod's restart count
// climbing — each of which precedes the corresponding event, when one
// exists at all.
//
// Transition discipline: every emitted kind requires per-object
// previous-state memory (in-memory, TTL-bounded, rebuilt from the
// informer cache on restart). The initial LIST populates that memory
// without emitting — the source arms only after all caches sync — so
// a sentinel restart never re-fires transitions it cannot have
// observed. Creations are not transitions: a Node created NotReady, an
// EndpointSlice created empty, or a PDB created at
// disruptionsAllowed=0 emits nothing. The one deliberate exception is
// objectstate.progress_deadline, which is a deadline countdown rather
// than a transition: a deployment already stuck at startup fires after
// arming (the engine's persisted dedup absorbs the repeat).
//
// This source also absorbed internal/watch's minimal pod observer: its
// pod informer feeds the shared PodClearance state machine, and its
// node informer the NodeClearance one, so the §7.4 recovery tracker
// uses ClearanceObserver() instead of second informers when the
// source is enabled — and node-scoped incidents (node_notready and
// the NodeNotReady reactive family) resolve like pod-scoped ones.
package objectstate

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the stable source name (§7.2 table) used in the signal
// schema and the `--sources` flag.
const Name = "object-state"

// kindPrefix namespaces this source's signal kinds (§7.3). The prefix
// drops the hyphen because kind segments are dot-separated tokens
// (`rollout.stall`, `saturation.forecast`); the hyphenated form stays
// the source/config name.
const kindPrefix = "objectstate."

// Signal kinds emitted by this source. APPEND-ONLY: kinds are part of
// the signal schema playbooks and fleet consumers match on — never
// rename or reuse
// one. The dedup/fingerprint reason for each is the kind suffix (the
// part after "objectstate.").
const (
	// KindNodeNotReady: a Node's Ready condition transitioned
	// True→False (Unknown counts as not ready — the node controller
	// lost contact). Critical: workloads on that node are next.
	KindNodeNotReady = kindPrefix + "node_notready"
	// KindNodeFlapping: a Node's Ready condition changed
	// Config.FlapTransitions times within Config.FlapWindow.
	KindNodeFlapping = kindPrefix + "node_flapping"
	// KindProgressDeadline: a Deployment rollout has made no progress
	// for more than Config.ProgressDeadlineFraction of its
	// progressDeadlineSeconds with unready replicas — fired BEFORE
	// the control plane's ProgressDeadlineExceeded event.
	KindProgressDeadline = kindPrefix + "progress_deadline"
	// KindEndpointsEmpty: an EndpointSlice-backed Service's ready-
	// endpoint count transitioned >0 → 0 (a Service created with no
	// ready endpoints does not fire).
	KindEndpointsEmpty = kindPrefix + "endpoints_empty"
	// KindPDBGridlocked: a PodDisruptionBudget's disruptionsAllowed
	// transitioned >0 → 0 while pods behind it exist — node drains
	// and voluntary disruptions will now stall. This is the
	// TRANSITION observation; `triage delta`'s point-in-time scan of
	// the same condition is the read-path counterpart.
	KindPDBGridlocked = kindPrefix + "pdb_gridlocked"
	// KindRestartBurst: a pod's summed container restart count grew
	// by at least Config.RestartBurstThreshold within
	// Config.RestartBurstWindow — the cheap leading edge of a crash
	// loop, ahead of the kubelet's BackOff events.
	KindRestartBurst = kindPrefix + "restart_burst"
	// KindNodePressure: one of a Node's kubelet pressure conditions
	// (MemoryPressure, DiskPressure, PIDPressure) transitioned
	// False→True. Warning at onset; ONE escalation to critical per
	// pressure episode when the sweep sees pressure still active past
	// Config.PressureSustainWindow, or immediately when an eviction
	// burst fires on the same node while pressure is active (the
	// kubelet is already shedding load). One incident per node
	// (UID=node UID, reason "node_pressure"); the message names the
	// condition(s).
	KindNodePressure = kindPrefix + "node_pressure"
	// KindEvictionBurst: Config.EvictionBurstThreshold pod evictions
	// on ONE node within Config.EvictionBurstWindow, folded into a
	// single node-scoped signal instead of N pod-scoped ones.
	// Deliberately "burst", mirroring restart_burst — kind=storm is
	// §7.5's separate cross-incident mechanism. §7.5 coordination:
	// the per-pod Evicted k8s-events still flow through the
	// k8s-events source and, with --storm on, form a node-keyed
	// storm; this signal is the storm-off fallback and, being
	// node-scoped, itself joins/seeds the node storm (internal/watch
	// graphfeed makes Node the ancestor) — no engine changes needed.
	KindEvictionBurst = kindPrefix + "eviction_burst"
)

// kindSeverity is the default §7.7 severity per kind. Per-class
// overrides belong to the severity-routing config when it lands
// (§7.7); until then these defaults are the routing input.
var kindSeverity = map[string]engine.Severity{
	KindNodeNotReady:     engine.SeverityCritical,
	KindNodeFlapping:     engine.SeverityWarning,
	KindProgressDeadline: engine.SeverityWarning,
	KindEndpointsEmpty:   engine.SeverityCritical,
	KindPDBGridlocked:    engine.SeverityWarning,
	KindRestartBurst:     engine.SeverityWarning,
	KindNodePressure:     engine.SeverityWarning,
	KindEvictionBurst:    engine.SeverityWarning,
}

// reasonOf derives the dedup/fingerprint reason from a kind: the
// suffix after the source prefix (e.g. "node_notready").
func reasonOf(kind string) string { return strings.TrimPrefix(kind, kindPrefix) }

// Config are the source's thresholds. Zero values take the defaults
// (DefaultConfig); the §7.7 severity/threshold config file will feed
// this struct when it lands.
type Config struct {
	// ProgressDeadlineFraction of progressDeadlineSeconds after which
	// a still-incomplete rollout fires KindProgressDeadline.
	// Default 0.8.
	ProgressDeadlineFraction float64
	// FlapTransitions is how many Ready-condition changes within
	// FlapWindow make a node "flapping". Default 3 (down, up, down —
	// a second outage inside the window).
	FlapTransitions int
	// FlapWindow is the sliding window for FlapTransitions.
	// Default 10m.
	FlapWindow time.Duration
	// RestartBurstThreshold is the minimum observed restart-count
	// growth within RestartBurstWindow that fires KindRestartBurst.
	// Default 3.
	RestartBurstThreshold int
	// RestartBurstWindow is the sliding window for restart growth.
	// Default 10m.
	RestartBurstWindow time.Duration
	// PressureSustainWindow is how long a node pressure condition
	// must stay True before the warning-level KindNodePressure
	// escalates to critical. Default 5m.
	PressureSustainWindow time.Duration
	// EvictionBurstThreshold is how many pod evictions on one node
	// within EvictionBurstWindow fire KindEvictionBurst. Default 3.
	EvictionBurstThreshold int
	// EvictionBurstWindow is the sliding window for per-node
	// eviction counting. Default 10m.
	EvictionBurstWindow time.Duration
	// TickInterval drives the deadline sweep (progress-deadline math
	// needs a clock, not an informer update — a stuck rollout stops
	// producing updates) and the state-TTL prune. Default 30s.
	TickInterval time.Duration
	// StateTTL bounds the per-object transition memory: entries not
	// refreshed by any informer activity within this window are
	// dropped (safety net behind DeleteFunc; a dropped entry costs
	// one unobservable transition, never a leak). Default 24h.
	StateTTL time.Duration
}

// DefaultConfig returns the shipped thresholds.
func DefaultConfig() Config {
	return Config{
		ProgressDeadlineFraction: 0.8,
		FlapTransitions:          3,
		FlapWindow:               10 * time.Minute,
		RestartBurstThreshold:    3,
		RestartBurstWindow:       10 * time.Minute,
		PressureSustainWindow:    5 * time.Minute,
		EvictionBurstThreshold:   3,
		EvictionBurstWindow:      10 * time.Minute,
		TickInterval:             30 * time.Second,
		StateTTL:                 24 * time.Hour,
	}
}

// normalize fills zero fields with defaults.
func (c Config) normalize() Config {
	d := DefaultConfig()
	if c.ProgressDeadlineFraction <= 0 || c.ProgressDeadlineFraction > 1 {
		c.ProgressDeadlineFraction = d.ProgressDeadlineFraction
	}
	if c.FlapTransitions <= 0 {
		c.FlapTransitions = d.FlapTransitions
	}
	if c.FlapWindow <= 0 {
		c.FlapWindow = d.FlapWindow
	}
	if c.RestartBurstThreshold <= 0 {
		c.RestartBurstThreshold = d.RestartBurstThreshold
	}
	if c.RestartBurstWindow <= 0 {
		c.RestartBurstWindow = d.RestartBurstWindow
	}
	if c.PressureSustainWindow <= 0 {
		c.PressureSustainWindow = d.PressureSustainWindow
	}
	if c.EvictionBurstThreshold <= 0 {
		c.EvictionBurstThreshold = d.EvictionBurstThreshold
	}
	if c.EvictionBurstWindow <= 0 {
		c.EvictionBurstWindow = d.EvictionBurstWindow
	}
	if c.TickInterval <= 0 {
		c.TickInterval = d.TickInterval
	}
	if c.StateTTL <= 0 {
		c.StateTTL = d.StateTTL
	}
	return c
}

// pressureLevel is the fired-level latch for a node pressure episode
// (the capacity source's pending-aged pattern): one warning per
// episode, one critical escalation per episode; monotonic within the
// episode, reset when every pressure condition returns False.
type pressureLevel int

const (
	pressureNone pressureLevel = iota
	pressureWarn
	pressureCritical
)

// pressureConditions are the kubelet pressure conditions
// KindNodePressure watches. Absent counts as False.
var pressureConditions = []corev1.NodeConditionType{
	corev1.NodeMemoryPressure,
	corev1.NodeDiskPressure,
	corev1.NodePIDPressure,
}

// nodeState is the per-node transition memory.
type nodeState struct {
	name  string
	ready bool
	// transitions are the timestamps of recent Ready-condition
	// changes (either direction), pruned to Config.FlapWindow.
	transitions []time.Time
	// pressure is the last observed status of each kubelet pressure
	// condition (absent = false).
	pressure map[corev1.NodeConditionType]bool
	// pressureSince is when the current pressure episode began (the
	// instant any pressure condition was observed True from an
	// all-False state). Zero when no episode is active.
	pressureSince time.Time
	// pressureFired is the per-episode emission latch. It advances
	// past pressureNone only for episodes whose ONSET was observed
	// while armed — a pre-existing episode (baseline, or pre-arm
	// onset) never warned, so it never escalates either (§7.2: no
	// boot-time replay).
	pressureFired pressureLevel
	lastSeen      time.Time
}

// evictionState is the per-node eviction-burst memory, keyed by node
// NAME (evicted pods carry only spec.nodeName; a same-name node
// replacement inherits the bucket, which is also what clearance
// judges).
type evictionState struct {
	// times are the observed eviction transitions, pruned to
	// Config.EvictionBurstWindow.
	times []time.Time
	// last is the most recent eviction regardless of pruning — the
	// clearance predicate's input.
	last time.Time
	// fired latches one KindEvictionBurst per episode; the sweep
	// resets it when the window drains.
	fired    bool
	lastSeen time.Time
}

// deploymentState is the per-deployment sweep subject: the last
// observed object plus the fired-generation gate.
type deploymentState struct {
	dep *appsv1.Deployment
	// firedGeneration gates KindProgressDeadline to once per rollout
	// generation — the signal is "this rollout is about to blow its
	// deadline", said once; re-emitting every sweep would only feed
	// the dedup suppressor (and its log line).
	firedGeneration int64
	lastSeen        time.Time
}

// serviceKey identifies the Service an EndpointSlice belongs to.
type serviceKey struct {
	namespace string
	name      string
}

// serviceState aggregates ready-endpoint counts across a Service's
// EndpointSlices (a Service may shard endpoints over many slices —
// the transition that matters is the per-SERVICE total hitting 0).
type serviceState struct {
	slices   map[string]int // slice name → ready endpoint count
	uid      string         // Service UID from the slice's owner ref, best-effort
	lastSeen time.Time
}

func (s *serviceState) total() int {
	t := 0
	for _, n := range s.slices {
		t += n
	}
	return t
}

// pdbState is the per-PDB transition memory.
type pdbState struct {
	allowed  int32
	lastSeen time.Time
}

// restartState is the per-pod restart-growth memory (also the
// per-pod eviction dedupe: informer update replays of an already-
// counted Evicted pod must not double-count).
type restartState struct {
	total int32
	// bumps are observed restart-count increments, pruned to
	// Config.RestartBurstWindow.
	bumps []restartBump
	// evicted latches whether this pod's transition to
	// Failed/Evicted was already observed (counted at most once).
	evicted  bool
	lastSeen time.Time
}

type restartBump struct {
	at time.Time
	n  int
}

// Source implements sources.Source for the object-state row of §7.2.
type Source struct {
	client kubernetes.Interface
	cfg    Config
	// pc is the shared §7.4 pod-clearance state machine, fed by this
	// source's pod informer. See ClearanceObserver.
	pc *PodClearance
	// nc is the §7.4 node-clearance state machine, fed by this
	// source's node informer (M2 drill observation 2: node-scoped
	// incidents need a clearance observer too, or a node-anchored
	// storm can never fully resolve). See ClearanceObserver.
	nc *NodeClearance
	// factory, when set via WithFactory, is the externally owned
	// shared informer factory Run registers on instead of creating
	// its own — §6.3's "one informer set serves the sentinel sources
	// and the graph". Nil (the default) preserves the shipped
	// behavior exactly: Run builds a private factory.
	factory informers.SharedInformerFactory

	mu sync.Mutex
	// armed flips true after every informer cache syncs. Handlers
	// always record state; they emit only when armed, so the initial
	// LIST rebuilds transition memory without firing (§7.2 restart
	// discipline).
	armed bool
	emit  func(engine.Signal)

	nodes       map[types.UID]*nodeState
	deployments map[types.UID]*deploymentState
	services    map[serviceKey]*serviceState
	pdbs        map[types.UID]*pdbState
	restarts    map[types.UID]*restartState
	// evictions is the per-node eviction-burst memory, keyed by node
	// name (see evictionState).
	evictions map[string]*evictionState

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
}

// New constructs the source. Zero-valued cfg fields take the shipped
// defaults.
func New(client kubernetes.Interface, cfg Config) *Source {
	return &Source{
		client:      client,
		cfg:         cfg.normalize(),
		pc:          NewPodClearance(),
		nc:          NewNodeClearance(),
		nodes:       make(map[types.UID]*nodeState),
		deployments: make(map[types.UID]*deploymentState),
		services:    make(map[serviceKey]*serviceState),
		pdbs:        make(map[types.UID]*pdbState),
		restarts:    make(map[types.UID]*restartState),
		evictions:   make(map[string]*evictionState),
	}
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: nodes are cluster-scoped, so the
// source needs cluster RBAC (§11 — namespace-tier deployments get the
// loud startup failure, never a silent gap).
func (s *Source) Scope() sources.Scope { return sources.ScopeCluster }

// WithFactory directs Run to register its informers on an externally
// owned shared factory (DESIGN.md §6.3: one informer set serves the
// sentinel sources AND the graph — the sentinel passes the same
// factory to this source and to the storm-correlation graph feed, so
// pods/nodes are watched once, not twice). Call before Run; nil is
// ignored. SharedInformerFactory.Start and Shutdown are idempotent,
// so both parties may drive the shared lifecycle safely.
func (s *Source) WithFactory(f informers.SharedInformerFactory) {
	if f != nil {
		s.factory = f
	}
}

// ClearanceObserver returns the §7.4 clearance predicate backed by
// this source's informers: eviction_burst incidents are judged by the
// source's own eviction memory (the symptom is eviction ACTIVITY, not
// node readiness — only the source observes it), other node-scoped
// incidents by the node informer's NodeClearance, pod-scoped ones by
// the pod informer's PodClearance. The eviction judge precedes nc
// because both claim Node-scoped incidents; nc and pc are disjoint.
// The recovery tracker uses it instead of internal/watch's standalone
// pod observer when the source is enabled — the same informers, no
// duplicates, and pod judging behavior identical to before.
func (s *Source) ClearanceObserver() engine.ClearanceObserver {
	return composedClearance{evictionClearance{s}, s.nc, s.pc}
}

// evictionClearance judges Node-scoped eviction_burst incidents from
// the Source's per-node eviction memory (§7.4: the source that
// observes the symptom observes its absence): cleared when no
// eviction was recorded for the node within EvictionBurstWindow,
// StableSince = the instant the window drained. When the node is gone
// AND no eviction memory remains it declines, so NodeClearance's
// gone-node logic supplies the object_deleted / same-name-replacement
// verdict.
type evictionClearance struct{ s *Source }

func (e evictionClearance) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	if !strings.EqualFold(inc.Ref.KindOfObject, "Node") || inc.Key.Reason != reasonOf(KindEvictionBurst) {
		return engine.Clearance{}, false
	}
	s := e.s
	now := s.clock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.armed {
		// Caches not synced: cannot judge yet (mirrors the
		// SetSynced gate of the clearance state machines).
		return engine.Clearance{}, false
	}
	ev := s.evictions[inc.Ref.Name]
	if ev == nil {
		for _, st := range s.nodes {
			if st.name == inc.Ref.Name {
				// Live node, no eviction memory: symptom absent
				// (nothing recorded to vouch a later instant from).
				return engine.Clearance{Cleared: true, Resolution: engine.ResolutionRecovered}, true
			}
		}
		return engine.Clearance{}, false // node gone — nc decides
	}
	drained := ev.last.Add(s.cfg.EvictionBurstWindow)
	return engine.Clearance{
		Cleared:     !now.Before(drained),
		StableSince: drained,
		Resolution:  engine.ResolutionRecovered,
	}, true
}

// composedClearance asks each observer in order; the first that can
// judge the incident wins (the same rule the tracker applies across
// registered observers).
type composedClearance []engine.ClearanceObserver

func (c composedClearance) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	for _, o := range c {
		if verdict, ok := o.Clearance(inc); ok {
			return verdict, true
		}
	}
	return engine.Clearance{}, false
}

// RequiredAccess implements sources.AccessDeclarer (§11): list+watch
// on each informer target. Matches deploy/12-clusterrole-watcher.yaml.
func (s *Source) RequiredAccess() []sources.Requirement {
	var reqs []sources.Requirement
	for _, r := range []struct{ group, resource string }{
		{"", "pods"},
		{"", "nodes"},
		{"apps", "deployments"},
		{"discovery.k8s.io", "endpointslices"},
		{"policy", "poddisruptionbudgets"},
	} {
		for _, verb := range []string{"list", "watch"} {
			reqs = append(reqs, sources.Requirement{Group: r.group, Resource: r.resource, Verb: verb})
		}
	}
	return reqs
}

func (s *Source) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// arm enables emission — called once all caches are synced.
func (s *Source) arm() {
	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()
}

// HasSynced implements sources.SyncReporter — the sentinel's /readyz
// probe is not ready until every source with a barrier has crossed it.
func (s *Source) HasSynced() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.armed
}

// send delivers signals to the pipeline. Never called under s.mu (the
// emit callback dispatches synchronously into filter/dedup/inject).
func (s *Source) send(sigs []engine.Signal) {
	if len(sigs) == 0 {
		return
	}
	s.mu.Lock()
	emit := s.emit
	s.mu.Unlock()
	if emit == nil {
		return // not running (unit tests drive handlers directly)
	}
	for _, sig := range sigs {
		emit(sig)
	}
}

// newSignal composes one object-state Signal. Deployment identity and
// fingerprint stay empty for the pipeline to stamp (§7.2).
func newSignal(kind, objKind, namespace, name, uid, node, msg string, ts time.Time) engine.Signal {
	return engine.Signal{
		Kind:     kind,
		Source:   engine.SourceSentinel,
		Severity: kindSeverity[kind],
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: uid, Reason: reasonOf(kind)},
			Namespace:    namespace,
			KindOfObject: objKind,
			Name:         name,
			Node:         node,
			Message:      msg,
			FirstSeen:    ts,
			LastSeen:     ts,
			Count:        1,
		},
	}
}

// Run implements sources.Source: starts the five informers, arms
// after every cache syncs, then drives the sweep ticker until ctx is
// cancelled.
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()

	factory := s.factory
	if factory == nil {
		factory = informers.NewSharedInformerFactory(s.client, 0)
	}

	podH, err := factory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.asPod(obj, s.onPod) },
		UpdateFunc: func(_, obj any) { s.asPod(obj, s.onPod) },
		DeleteFunc: func(obj any) { s.asPod(tombstoneObj(obj), s.onPodDelete) },
	})
	if err != nil {
		return fmt.Errorf("object-state: register pod handler: %w", err)
	}
	s.pc.SetSynced(podH.HasSynced)

	nodeH, err := factory.Core().V1().Nodes().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.asNode(obj, s.onNode) },
		UpdateFunc: func(_, obj any) { s.asNode(obj, s.onNode) },
		DeleteFunc: func(obj any) { s.asNode(tombstoneObj(obj), s.onNodeDelete) },
	})
	if err != nil {
		return fmt.Errorf("object-state: register node handler: %w", err)
	}
	s.nc.SetSynced(nodeH.HasSynced)
	depH, err := factory.Apps().V1().Deployments().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.asDeployment(obj, s.onDeployment) },
		UpdateFunc: func(_, obj any) { s.asDeployment(obj, s.onDeployment) },
		DeleteFunc: func(obj any) { s.asDeployment(tombstoneObj(obj), s.onDeploymentDelete) },
	})
	if err != nil {
		return fmt.Errorf("object-state: register deployment handler: %w", err)
	}
	sliceH, err := factory.Discovery().V1().EndpointSlices().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.asSlice(obj, s.onSlice) },
		UpdateFunc: func(_, obj any) { s.asSlice(obj, s.onSlice) },
		DeleteFunc: func(obj any) { s.asSlice(tombstoneObj(obj), s.onSliceDelete) },
	})
	if err != nil {
		return fmt.Errorf("object-state: register endpointslice handler: %w", err)
	}
	pdbH, err := factory.Policy().V1().PodDisruptionBudgets().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.asPDB(obj, s.onPDB) },
		UpdateFunc: func(_, obj any) { s.asPDB(obj, s.onPDB) },
		DeleteFunc: func(obj any) { s.asPDB(tombstoneObj(obj), s.onPDBDelete) },
	})
	if err != nil {
		return fmt.Errorf("object-state: register pdb handler: %w", err)
	}

	factory.Start(ctx.Done())
	// Shutdown blocks until every handler goroutine exits, upholding
	// the Source contract that emit is never called after Run returns.
	defer factory.Shutdown()

	// Arm-after-sync (§7.2): the initial LIST above rebuilt the
	// transition memory silently; only from here do state changes
	// count as transitions.
	if !cache.WaitForCacheSync(ctx.Done(), podH.HasSynced, nodeH.HasSynced, depH.HasSynced, sliceH.HasSynced, pdbH.HasSynced) {
		return fmt.Errorf("object-state: cache sync failed (informer stopped before initial list completed)")
	}
	s.arm()

	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.send(s.sweep(s.clock()))
		}
	}
}

// tombstoneObj unwraps cache.DeletedFinalStateUnknown tombstones.
func tombstoneObj(obj any) any {
	if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		return t.Obj
	}
	return obj
}

func (s *Source) asPod(obj any, fn func(*corev1.Pod)) {
	if p, ok := obj.(*corev1.Pod); ok {
		fn(p)
	}
}

func (s *Source) asNode(obj any, fn func(*corev1.Node)) {
	if n, ok := obj.(*corev1.Node); ok {
		fn(n)
	}
}

func (s *Source) asDeployment(obj any, fn func(*appsv1.Deployment)) {
	if d, ok := obj.(*appsv1.Deployment); ok {
		fn(d)
	}
}

func (s *Source) asSlice(obj any, fn func(*discoveryv1.EndpointSlice)) {
	if sl, ok := obj.(*discoveryv1.EndpointSlice); ok {
		fn(sl)
	}
}

func (s *Source) asPDB(obj any, fn func(*policyv1.PodDisruptionBudget)) {
	if p, ok := obj.(*policyv1.PodDisruptionBudget); ok {
		fn(p)
	}
}

// ---- Nodes: Ready flips, flap detection, pressure onset ----

// nodeReady reads the Ready condition. Unknown counts as NOT ready:
// the node controller losing contact is exactly the outage the signal
// is for. ok=false when the condition is absent (nothing to judge).
func nodeReady(n *corev1.Node) (ready bool, msg string, ok bool) {
	for _, cond := range n.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			detail := cond.Message
			if detail == "" {
				detail = cond.Reason
			}
			return cond.Status == corev1.ConditionTrue, detail, true
		}
	}
	return false, "", false
}

// nodePressure reads the kubelet pressure conditions. Absent counts
// as False (a condition the kubelet never reported is not pressure).
func nodePressure(n *corev1.Node) map[corev1.NodeConditionType]bool {
	p := make(map[corev1.NodeConditionType]bool, len(pressureConditions))
	for _, cond := range n.Status.Conditions {
		for _, want := range pressureConditions {
			if cond.Type == want {
				p[cond.Type] = cond.Status == corev1.ConditionTrue
			}
		}
	}
	return p
}

// anyPressure reports whether any pressure condition is True.
func anyPressure(p map[corev1.NodeConditionType]bool) bool {
	for _, v := range p {
		if v {
			return true
		}
	}
	return false
}

// activePressureNames lists the True pressure conditions in the fixed
// pressureConditions order (deterministic messages).
func activePressureNames(p map[corev1.NodeConditionType]bool) []string {
	var names []string
	for _, c := range pressureConditions {
		if p[c] {
			names = append(names, string(c))
		}
	}
	return names
}

func (s *Source) onNode(n *corev1.Node) {
	// Clearance duties first (§7.4 node observer, mirroring onPod).
	s.nc.Upsert(n)

	ready, detail, ok := nodeReady(n)
	if !ok {
		return
	}
	pressure := nodePressure(n)
	now := s.clock()

	s.mu.Lock()
	st, seen := s.nodes[n.UID]
	if !seen {
		// First observation (initial LIST, or a node created mid-
		// flight): record, never fire — a creation is not a
		// transition. A node first seen WITH pressure active is
		// history: the episode is recorded (pressureSince) but the
		// fired latch stays pressureNone, so it neither warns nor
		// escalates (§7.2: no boot-time replay).
		st = &nodeState{name: n.Name, ready: ready, pressure: pressure, lastSeen: now}
		if anyPressure(pressure) {
			st.pressureSince = now
		}
		s.nodes[n.UID] = st
		s.mu.Unlock()
		return
	}
	st.name = n.Name
	st.lastSeen = now
	var out []engine.Signal
	if ready != st.ready {
		st.ready = ready
		st.transitions = append(st.transitions, now)
		st.transitions = pruneTimes(st.transitions, now.Add(-s.cfg.FlapWindow))
		if s.armed {
			if !ready {
				out = append(out, newSignal(KindNodeNotReady, "Node", "", n.Name, string(n.UID), n.Name,
					fmt.Sprintf("node Ready condition went True→False: %s", detail), now))
			}
			if len(st.transitions) >= s.cfg.FlapTransitions {
				out = append(out, newSignal(KindNodeFlapping, "Node", "", n.Name, string(n.UID), n.Name,
					fmt.Sprintf("node Ready condition changed %d times within %s (flapping)", len(st.transitions), s.cfg.FlapWindow), now))
				st.transitions = nil // re-count toward the next burst
			}
		}
	}
	// Pressure episode tracking, independent of Ready (a node can be
	// Ready and under memory pressure — which is why NodeClearance
	// judges node_pressure incidents by the pressure conditions, not
	// Ready-ness).
	wasActive := anyPressure(st.pressure)
	st.pressure = pressure
	switch active := anyPressure(pressure); {
	case active && !wasActive:
		// Episode onset: some pressure condition flipped False→True.
		st.pressureSince = now
		st.pressureFired = pressureNone
		if s.armed {
			st.pressureFired = pressureWarn
			out = append(out, newSignal(KindNodePressure, "Node", "", n.Name, string(n.UID), n.Name,
				fmt.Sprintf("node pressure condition went False→True: %s", strings.Join(activePressureNames(pressure), ", ")), now))
		}
	case !active && wasActive:
		// Episode over: every pressure condition back to False. The
		// §7.4 resolve is NodeClearance's business; here only the
		// latch resets so a NEW episode warns again.
		st.pressureSince = time.Time{}
		st.pressureFired = pressureNone
	}
	s.mu.Unlock()
	s.send(out)
}

func (s *Source) onNodeDelete(n *corev1.Node) {
	s.nc.Delete(n)
	s.mu.Lock()
	delete(s.nodes, n.UID)
	// The node's eviction bucket goes with it: a same-name
	// replacement starts a fresh count, and a gone-node
	// eviction_burst incident falls through to NodeClearance's
	// object_deleted verdict.
	delete(s.evictions, n.Name)
	s.mu.Unlock()
}

// pruneTimes drops timestamps at or before cutoff, preserving order.
func pruneTimes(ts []time.Time, cutoff time.Time) []time.Time {
	out := ts[:0]
	for _, t := range ts {
		if t.After(cutoff) {
			out = append(out, t)
		}
	}
	return out
}

// ---- Deployments: progress-deadline approaching ----

func (s *Source) onDeployment(d *appsv1.Deployment) {
	now := s.clock()
	s.mu.Lock()
	st, ok := s.deployments[d.UID]
	if !ok {
		st = &deploymentState{}
		s.deployments[d.UID] = st
	}
	st.dep = d
	st.lastSeen = now
	if deploymentComplete(d) {
		// A finished rollout clears the gate; the generation can only
		// stall again after a spec change bumps it, but keeping the
		// invariant explicit costs nothing.
		st.firedGeneration = 0
	}
	s.mu.Unlock()
	// Evaluate immediately too — a status update can itself reveal a
	// rollout already past the threshold (e.g. sentinel started
	// mid-stall and the deployment just got its first status touch).
	s.send(s.sweep(now))
}

func (s *Source) onDeploymentDelete(d *appsv1.Deployment) {
	s.mu.Lock()
	delete(s.deployments, d.UID)
	s.mu.Unlock()
}

// deploymentComplete is the standard rollout-done predicate: the
// controller has observed the current generation and every replica is
// updated and available.
func deploymentComplete(d *appsv1.Deployment) bool {
	if d.Status.ObservedGeneration < d.Generation {
		return false
	}
	replicas := int32(1)
	if d.Spec.Replicas != nil {
		replicas = *d.Spec.Replicas
	}
	return d.Status.UpdatedReplicas == replicas &&
		d.Status.Replicas == replicas &&
		d.Status.AvailableReplicas == replicas
}

// assessProgress decides whether an in-flight rollout has consumed
// more than fraction of its progress deadline. Pure — all clock input
// is now.
//
// The deadline clock is the Progressing condition's LastUpdateTime,
// which the deployment controller advances whenever the rollout makes
// progress — the same clock it checks against progressDeadlineSeconds
// before emitting ProgressDeadlineExceeded. Firing at
// fraction*deadline on the same clock is what makes this signal
// LEADING: same math, earlier threshold.
func assessProgress(d *appsv1.Deployment, fraction float64, now time.Time) (fire bool, msg string) {
	if d.Spec.Paused {
		return false, "" // the controller suspends the deadline check while paused
	}
	deadline := int32(600) // k8s API default for progressDeadlineSeconds
	if d.Spec.ProgressDeadlineSeconds != nil {
		deadline = *d.Spec.ProgressDeadlineSeconds
	}
	if deadline == math.MaxInt32 {
		return false, "" // deadline explicitly disabled
	}
	if d.Status.ObservedGeneration < d.Generation {
		// The controller hasn't reconciled this generation yet; the
		// Progressing condition still describes the PREVIOUS rollout
		// and would read as stale elapsed time.
		return false, ""
	}
	if deploymentComplete(d) {
		return false, ""
	}
	var progressing *appsv1.DeploymentCondition
	for i := range d.Status.Conditions {
		if d.Status.Conditions[i].Type == appsv1.DeploymentProgressing {
			progressing = &d.Status.Conditions[i]
		}
	}
	if progressing == nil {
		return false, ""
	}
	if progressing.Reason == "ProgressDeadlineExceeded" {
		// Too late to lead: the control plane already declared it,
		// and the k8s-events source owns the reactive signal.
		return false, ""
	}
	if d.Status.Replicas == d.Status.UpdatedReplicas &&
		progressing.Reason == "NewReplicaSetAvailable" {
		// Every replica belongs to the current template and the last
		// thing the controller concluded is that the rollout finished:
		// there is only one active ReplicaSet, so no rollout is in
		// flight and this condition's timestamp measures the LAST one.
		//
		// This is the deployment controller's own isCompleteDeployment
		// predicate (syncRolloutStatus), and it skips its deadline
		// check on the same terms. Without it, the transient in the
		// middle of a SCALE fires: Spec.Replicas moves first, so for a
		// second or so the deployment reads as incomplete while the
		// condition still carries the timestamp of a rollout that
		// finished minutes ago — and the threshold is 7.5 minutes at
		// the defaults, which every settled workload is past. Any HPA
		// scale would have produced a warning that a rollout is
		// stalling when none is happening (issue #365).
		//
		// A real stall keeps its own clock: the controller records
		// progress the moment the new ReplicaSet gets a pod, which
		// flips the reason to ReplicaSetUpdated with a fresh
		// LastUpdateTime, and Replicas != UpdatedReplicas throughout a
		// rollout that never gets that far.
		return false, ""
	}
	elapsed := now.Sub(progressing.LastUpdateTime.Time)
	budget := time.Duration(deadline) * time.Second
	if elapsed < time.Duration(fraction*float64(budget)) {
		return false, ""
	}
	replicas := int32(1)
	if d.Spec.Replicas != nil {
		replicas = *d.Spec.Replicas
	}
	return true, fmt.Sprintf(
		"rollout has made no progress for %s — %d%% of progressDeadlineSeconds=%ds; ProgressDeadlineExceeded in ~%s (updated=%d/%d available=%d)",
		elapsed.Truncate(time.Second), int(100*elapsed/budget), deadline,
		(budget - elapsed).Truncate(time.Second),
		d.Status.UpdatedReplicas, replicas, d.Status.AvailableReplicas)
}

// sweep is the ticker body: evaluates progress deadlines and
// sustained node pressure, maintains the eviction windows, and prunes
// TTL-expired transition memory. Returns the signals to emit (the
// caller sends them outside the lock).
func (s *Source) sweep(now time.Time) []engine.Signal {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []engine.Signal
	if s.armed {
		for _, st := range s.deployments {
			d := st.dep
			if d == nil || st.firedGeneration == d.Generation {
				continue
			}
			if fire, msg := assessProgress(d, s.cfg.ProgressDeadlineFraction, now); fire {
				st.firedGeneration = d.Generation
				out = append(out, newSignal(KindProgressDeadline, "Deployment", d.Namespace, d.Name, string(d.UID), "", msg, now))
			}
		}
		// Sustained-pressure escalation: one critical per episode
		// (level latch), and only for episodes that WARNED — an
		// episode whose onset predates arming never emitted, so it
		// never escalates either.
		for uid, st := range s.nodes {
			if st.pressureFired != pressureWarn || !anyPressure(st.pressure) {
				continue
			}
			if held := now.Sub(st.pressureSince); held >= s.cfg.PressureSustainWindow {
				st.pressureFired = pressureCritical
				sig := newSignal(KindNodePressure, "Node", "", st.name, string(uid), st.name,
					fmt.Sprintf("node pressure (%s) sustained for %s (threshold %s)",
						strings.Join(activePressureNames(st.pressure), ", "), held.Truncate(time.Second), s.cfg.PressureSustainWindow), now)
				sig.Severity = engine.SeverityCritical
				out = append(out, sig)
			}
		}
	}
	// Eviction windows: prune, and re-arm the burst latch once the
	// window drains (the next burst is a new episode).
	for _, ev := range s.evictions {
		ev.times = pruneTimes(ev.times, now.Add(-s.cfg.EvictionBurstWindow))
		if len(ev.times) == 0 {
			ev.fired = false
		}
	}
	// TTL prune (safety net behind DeleteFunc — a missed delete must
	// not leak state forever).
	cutoff := now.Add(-s.cfg.StateTTL)
	for uid, st := range s.nodes {
		if st.lastSeen.Before(cutoff) {
			delete(s.nodes, uid)
		}
	}
	for uid, st := range s.deployments {
		if st.lastSeen.Before(cutoff) {
			delete(s.deployments, uid)
		}
	}
	for key, st := range s.services {
		if st.lastSeen.Before(cutoff) {
			delete(s.services, key)
		}
	}
	for uid, st := range s.pdbs {
		if st.lastSeen.Before(cutoff) {
			delete(s.pdbs, uid)
		}
	}
	for uid, st := range s.restarts {
		if st.lastSeen.Before(cutoff) {
			delete(s.restarts, uid)
		}
	}
	for name, ev := range s.evictions {
		if ev.lastSeen.Before(cutoff) {
			delete(s.evictions, name)
		}
	}
	return out
}

// ---- EndpointSlices: per-Service ready count → 0 ----

// readyEndpoints counts a slice's ready endpoints. A nil Ready
// condition means unknown, which consumers must interpret as ready
// (discovery/v1 API contract).
func readyEndpoints(sl *discoveryv1.EndpointSlice) int {
	n := 0
	for _, ep := range sl.Endpoints {
		if ep.Conditions.Ready == nil || *ep.Conditions.Ready {
			n++
		}
	}
	return n
}

// sliceServiceUID pulls the owning Service's UID off the slice.
func sliceServiceUID(sl *discoveryv1.EndpointSlice) string {
	for _, ref := range sl.OwnerReferences {
		if ref.Kind == "Service" {
			return string(ref.UID)
		}
	}
	return ""
}

func (s *Source) onSlice(sl *discoveryv1.EndpointSlice) {
	svcName := sl.Labels[discoveryv1.LabelServiceName]
	if svcName == "" {
		return // unmanaged slice — not an EndpointSlice-backed Service
	}
	now := s.clock()
	key := serviceKey{sl.Namespace, svcName}

	s.mu.Lock()
	st, ok := s.services[key]
	if !ok {
		st = &serviceState{slices: make(map[string]int)}
		s.services[key] = st
	}
	if st.uid == "" {
		st.uid = sliceServiceUID(sl)
	}
	st.lastSeen = now
	prev := st.total()
	hadSlice := len(st.slices) > 0
	st.slices[sl.Name] = readyEndpoints(sl)
	cur := st.total()
	var out []engine.Signal
	// Transition only: a Service first observed at 0 (created empty,
	// or its first slice arriving empty) does not fire.
	if s.armed && hadSlice && prev > 0 && cur == 0 {
		out = append(out, s.emptySignal(key, st, prev, now))
	}
	s.mu.Unlock()
	s.send(out)
}

func (s *Source) onSliceDelete(sl *discoveryv1.EndpointSlice) {
	svcName := sl.Labels[discoveryv1.LabelServiceName]
	if svcName == "" {
		return
	}
	now := s.clock()
	key := serviceKey{sl.Namespace, svcName}

	s.mu.Lock()
	st, ok := s.services[key]
	if !ok {
		s.mu.Unlock()
		return
	}
	prev := st.total()
	delete(st.slices, sl.Name)
	var out []engine.Signal
	if len(st.slices) == 0 {
		// Last slice gone — the Service itself is (being) deleted;
		// that is object removal, not an endpoints outage.
		delete(s.services, key)
	} else if s.armed && prev > 0 && st.total() == 0 {
		out = append(out, s.emptySignal(key, st, prev, now))
	}
	s.mu.Unlock()
	s.send(out)
}

// emptySignal composes KindEndpointsEmpty for a service. Called under
// s.mu. Falls back to a synthesized stable UID when the slice carried
// no Service owner ref (dedup still needs a per-object key).
func (s *Source) emptySignal(key serviceKey, st *serviceState, prev int, now time.Time) engine.Signal {
	uid := st.uid
	if uid == "" {
		uid = "service:" + key.namespace + "/" + key.name
	}
	return newSignal(KindEndpointsEmpty, "Service", key.namespace, key.name, uid, "",
		fmt.Sprintf("service has 0 ready endpoints (was %d) across %d EndpointSlice(s)", prev, len(st.slices)), now)
}

// ---- PodDisruptionBudgets: disruptionsAllowed → 0 ----

func (s *Source) onPDB(p *policyv1.PodDisruptionBudget) {
	now := s.clock()
	allowed := p.Status.DisruptionsAllowed

	s.mu.Lock()
	st, seen := s.pdbs[p.UID]
	if !seen {
		s.pdbs[p.UID] = &pdbState{allowed: allowed, lastSeen: now}
		s.mu.Unlock()
		return
	}
	prev := st.allowed
	st.allowed = allowed
	st.lastSeen = now
	var out []engine.Signal
	// The TRANSITION >0 → 0, and only while pods behind the PDB exist
	// (expectedPods==0 means the selector matches nothing — an empty
	// PDB blocks no one).
	if s.armed && prev > 0 && allowed == 0 && p.Status.ExpectedPods > 0 {
		out = append(out, newSignal(KindPDBGridlocked, "PodDisruptionBudget", p.Namespace, p.Name, string(p.UID), "",
			fmt.Sprintf("disruptionsAllowed dropped to 0 (expected=%d healthy=%d desired=%d): voluntary disruptions and node drains will stall",
				p.Status.ExpectedPods, p.Status.CurrentHealthy, p.Status.DesiredHealthy), now))
	}
	s.mu.Unlock()
	s.send(out)
}

func (s *Source) onPDBDelete(p *policyv1.PodDisruptionBudget) {
	s.mu.Lock()
	delete(s.pdbs, p.UID)
	s.mu.Unlock()
}

// ---- Pods: clearance mirror + restart-burst + eviction-burst ----

// podEvicted reports the kubelet's eviction verdict: an evicted pod
// surfaces on the pod informer as phase=Failed with status.reason
// "Evicted" — no event informer needed.
func podEvicted(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodFailed && p.Status.Reason == "Evicted"
}

func (s *Source) onPod(p *corev1.Pod) {
	// Clearance duties first (absorbed pod observer, §7.4).
	s.pc.Upsert(p)

	now := s.clock()
	total, _ := containerRestarts(p)
	evicted := podEvicted(p)

	s.mu.Lock()
	st, seen := s.restarts[p.UID]
	if !seen {
		// Baseline only: a pod first observed with a high restart
		// count — or already Evicted (initial LIST) — is history,
		// not an observed transition.
		s.restarts[p.UID] = &restartState{total: total, evicted: evicted, lastSeen: now}
		s.mu.Unlock()
		return
	}
	st.lastSeen = now
	var out []engine.Signal
	if total > st.total {
		st.bumps = append(st.bumps, restartBump{at: now, n: int(total - st.total)})
		cutoff := now.Add(-s.cfg.RestartBurstWindow)
		kept := st.bumps[:0]
		grown := 0
		for _, b := range st.bumps {
			if b.at.After(cutoff) {
				kept = append(kept, b)
				grown += b.n
			}
		}
		st.bumps = kept
		if s.armed && grown >= s.cfg.RestartBurstThreshold {
			out = append(out, newSignal(KindRestartBurst, "Pod", p.Namespace, p.Name, string(p.UID), p.Spec.NodeName,
				fmt.Sprintf("container restart count grew by %d within %s (total=%d)", grown, s.cfg.RestartBurstWindow, total), now))
			st.bumps = nil // re-count toward the next burst
		}
	}
	st.total = total
	if evicted && !st.evicted {
		// TRANSITION to Failed/Evicted, counted once per pod UID
		// (update replays of the terminal state hit the latch).
		st.evicted = true
		if p.Spec.NodeName != "" {
			out = append(out, s.recordEvictionLocked(p.Spec.NodeName, now)...)
		}
	}
	s.mu.Unlock()
	s.send(out)
}

// recordEvictionLocked buckets one observed eviction on nodeName
// (called under s.mu) and, at Config.EvictionBurstThreshold within
// Config.EvictionBurstWindow, fires ONE node-scoped
// KindEvictionBurst — instead of N pod-scoped signals. §7.5
// coordination: with --storm on, the per-pod Evicted k8s-events
// (engine filter allow-list) still form a node-keyed storm; this
// signal is the storm-off fallback, and being node-scoped it
// joins/seeds that same node storm (graphfeed makes Node the
// ancestor), so no engine changes are needed to avoid double
// aggregation. An eviction burst while the node's pressure episode is
// active also escalates that episode's KindNodePressure to critical
// (the kubelet is shedding load — pressure is no longer speculative).
func (s *Source) recordEvictionLocked(nodeName string, now time.Time) []engine.Signal {
	ev, ok := s.evictions[nodeName]
	if !ok {
		ev = &evictionState{}
		s.evictions[nodeName] = ev
	}
	ev.times = pruneTimes(append(ev.times, now), now.Add(-s.cfg.EvictionBurstWindow))
	ev.last = now
	ev.lastSeen = now
	if !s.armed || ev.fired || len(ev.times) < s.cfg.EvictionBurstThreshold {
		return nil
	}
	ev.fired = true // one burst per episode; the sweep re-arms on drain

	// Node identity: evicted pods carry only spec.nodeName; recover
	// the UID from the node informer's memory when it's there.
	uid := "node:" + nodeName
	var nst *nodeState
	for u, st := range s.nodes {
		if st.name == nodeName {
			uid, nst = string(u), st
			break
		}
	}
	out := []engine.Signal{newSignal(KindEvictionBurst, "Node", "", nodeName, uid, nodeName,
		fmt.Sprintf("%d pod evictions on the node within %s", len(ev.times), s.cfg.EvictionBurstWindow), now)}
	if nst != nil && nst.pressureFired == pressureWarn && anyPressure(nst.pressure) {
		nst.pressureFired = pressureCritical
		sig := newSignal(KindNodePressure, "Node", "", nst.name, uid, nst.name,
			fmt.Sprintf("node pressure (%s) paired with eviction activity: %d pod evictions within %s",
				strings.Join(activePressureNames(nst.pressure), ", "), len(ev.times), s.cfg.EvictionBurstWindow), now)
		sig.Severity = engine.SeverityCritical
		out = append(out, sig)
	}
	return out
}

func (s *Source) onPodDelete(p *corev1.Pod) {
	s.pc.Delete(p)
	s.mu.Lock()
	delete(s.restarts, p.UID)
	s.mu.Unlock()
}
