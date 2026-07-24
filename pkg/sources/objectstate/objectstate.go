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
// — a node flipping NotReady, a rollout approaching (not yet
// exceeding) its progress deadline, a Service's ready-endpoint count
// hitting zero, a PDB gridlocking, a pod's restart count climbing —
// each of which precedes the corresponding event, when one exists at
// all.
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
// pod informer feeds the shared PodClearance state machine, so the
// §7.4 recovery tracker uses ClearanceObserver() instead of a second
// pod informer when the source is enabled.
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
// the signal schema playbooks and AX match on — never rename or reuse
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
	if c.TickInterval <= 0 {
		c.TickInterval = d.TickInterval
	}
	if c.StateTTL <= 0 {
		c.StateTTL = d.StateTTL
	}
	return c
}

// nodeState is the per-node transition memory.
type nodeState struct {
	ready bool
	// transitions are the timestamps of recent Ready-condition
	// changes (either direction), pruned to Config.FlapWindow.
	transitions []time.Time
	lastSeen    time.Time
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

// restartState is the per-pod restart-growth memory.
type restartState struct {
	total int32
	// bumps are observed restart-count increments, pruned to
	// Config.RestartBurstWindow.
	bumps    []restartBump
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
		nodes:       make(map[types.UID]*nodeState),
		deployments: make(map[types.UID]*deploymentState),
		services:    make(map[serviceKey]*serviceState),
		pdbs:        make(map[types.UID]*pdbState),
		restarts:    make(map[types.UID]*restartState),
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
// this source's pod informer. The recovery tracker uses it instead of
// internal/watch's standalone pod observer when the source is enabled
// — one pod informer, identical judging behavior.
func (s *Source) ClearanceObserver() engine.ClearanceObserver { return s.pc }

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

// ---- Nodes: Ready flips and flap detection ----

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

func (s *Source) onNode(n *corev1.Node) {
	ready, detail, ok := nodeReady(n)
	if !ok {
		return
	}
	now := s.clock()

	s.mu.Lock()
	st, seen := s.nodes[n.UID]
	if !seen {
		// First observation (initial LIST, or a node created mid-
		// flight): record, never fire — a creation is not a
		// transition.
		s.nodes[n.UID] = &nodeState{ready: ready, lastSeen: now}
		s.mu.Unlock()
		return
	}
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
	s.mu.Unlock()
	s.send(out)
}

func (s *Source) onNodeDelete(n *corev1.Node) {
	s.mu.Lock()
	delete(s.nodes, n.UID)
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

// sweep is the ticker body: evaluates progress deadlines and prunes
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

// ---- Pods: clearance mirror + restart-burst ----

func (s *Source) onPod(p *corev1.Pod) {
	// Clearance duties first (absorbed pod observer, §7.4).
	s.pc.Upsert(p)

	now := s.clock()
	total, _ := containerRestarts(p)

	s.mu.Lock()
	st, seen := s.restarts[p.UID]
	if !seen {
		// Baseline only: a pod first observed with a high restart
		// count is history, not observed growth.
		s.restarts[p.UID] = &restartState{total: total, lastSeen: now}
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
	s.mu.Unlock()
	s.send(out)
}

func (s *Source) onPodDelete(p *corev1.Pod) {
	s.pc.Delete(p)
	s.mu.Lock()
	delete(s.restarts, p.UID)
	s.mu.Unlock()
}
