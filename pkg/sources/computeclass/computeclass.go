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

// Package computeclass is leeway's preference half (§7.7 of
// docs/leeway-design.md): the source that answers "which priority of its
// compute class is this workload actually running on, and for how much of the
// time".
//
// # The condition it exists for
//
// A GKE custom compute class is an ORDERED list of provisioning priorities.
// GKE tries priority 0, and on insufficient capacity falls down the list. The
// pod reaches Running and the Deployment reports its full replica count either
// way, so nothing in the Kubernetes API says anything went wrong — the estate
// is silently on its second or third choice of hardware, and stays there.
// Nothing here is a spread problem: §7.2–7.4's arithmetic is about domains that
// are peers with an even desired distribution, and priorities are ranked with
// all the mass desired at rank 0.
//
// # Rank, not index
//
// The trap this source is built around is that the number GKE writes on a node
// is not the number to score. `ccc_priority_index` is a position in
// spec.priorities. When a class sets `priorityScore`, preference is that score
// — higher is better, the OPPOSITE direction to list position — and several
// rules may share one. Spike S2 measured a class whose LEAST preferred rule
// sits at list position 0 and whose positions 1 and 2 are peers, so on that
// class an index-based reading is wrong in both directions at once. pkg/leeway
// owns the derivation (rank.go); this package owns the plumbing.
//
// # Where the rank comes from
//
// The node annotation is primary and inference is a cross-check, not the other
// way round. GKE stamps `ccc_priority_index` itself, so it is ground truth
// where it exists — but it is undocumented, it is ABSENT for the first 33–44
// seconds of every node's life (S1), and three of its four value shapes are
// not integers at all (`ccc_no_rule_matching`, `ccc_scale_up_anyway`, absent).
// So this source also matches the node's own attributes against the class's
// priority rules and reports the two answers disagreeing as an SLI. The
// matcher FAILS CLOSED: a rule with a field pkg/leeway does not model is
// excluded from inference and counted under `unsupported_rules`, because a
// matcher that ignores fields it does not understand matches rules it should
// not, and corrupts the cross-check into agreement with nothing.
//
// # Time, not gauges
//
// The question is not how many pods are at rank 2 right now but what fraction
// of the time the estate runs at rank 2. A ninety-second burst of rank-3 pods
// during a scale-up and three weeks parked on the spot fallback look identical
// to a gauge scraped every thirty seconds, so the primary series is
// pod-seconds (§7.7.3) and the gauges are supporting.
//
// # Signals
//
// The four `leeway.rank_*` kinds of §7.7.4, judged by pkg/leeway and held by
// §8.2's dwell machine at one episode per rule per axis. Two of the rules are
// gated on policy fields this source decodes from the class rather than on
// anything a node says: `whenUnsatisfiable` must DECLARE DoNotScaleUp before a
// Pending pod counts as wedged, and `activeMigration.optimizeRulePriority` must
// be set before a failure to migrate back is a failure at all.
//
// The shares they are judged over are windowed, not cumulative. The tracker's
// counters run for the life of the axis, and a share taken off those totals
// would report a bad week in March forever and a bad two hours right now not at
// all — so the judge diffs two readings and scores the difference (judge.go).
//
// The §7.7.4 row that is not here is the Tier C one: mean achieved rank rising
// against its own EWMA baseline. It wants §7.5's estimator, which is
// domain-share shaped and persisted against topology subjects, and pointing it
// at a rank mix is its own change.
//
// # Availability
//
// The ComputeClass CRD is optional and GKE-specific, so the CRs are watched
// unstructured through a dynamic informer (the `gateway` precedent) and the
// binary carries no GKE types. A cluster without the CRD never auto-enables
// the source; an explicit `--sources=…,compute-class` on such a cluster fails
// loudly at startup (§11) rather than watching an empty stream.
package computeclass

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel/metric"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the stable source name used in the signal schema and `--sources`.
//
// Hyphenated like `topology-drift`, and for the same reason: the source names
// are what an operator types, and the Go package directory is not. §3's layout
// writes it `pkg/sources/compute-class/`; the repo's existing convention wins.
const Name = "compute-class"

// The GKE ComputeClass CR. Cluster-scoped, short name `cc`, all verified on a
// live cluster (§13, spike S3).
var (
	computeClassGV  = schema.GroupVersion{Group: "cloud.google.com", Version: "v1"}
	computeClassGVR = computeClassGV.WithResource("computeclasses")
)

// Config tunes the source. The zero value takes every shipped default, which
// are the label and annotation keys verified on a live GKE cluster.
type Config struct {
	// ClassLabel is the node label naming the compute class a node belongs
	// to. It is also what class-pinned pods carry as a nodeSelector.
	ClassLabel string

	// PriorityIndexAnnotation is the node annotation GKE stamps the
	// provisioned priority into. Unprefixed, which is unusual for a GKE
	// annotation and is one of several reasons it is treated as undocumented.
	PriorityIndexAnnotation string

	// Profile is the label table node attributes are extracted through.
	Profile leeway.NodeProfileConfig

	// Infer turns the inference cross-check on. Nil means on. Off keeps the
	// annotation path and stops reporting disagreement, which is the escape
	// hatch if GKE ships a rule field that makes the matcher useless before
	// pkg/leeway can model it.
	Infer *bool

	// FlushInterval advances the pod-second buckets even when nothing enters
	// or leaves a rank, so a cluster parked on its fallback still accrues
	// time. Scrapes flush too; this bounds the loss if nothing ever scrapes.
	FlushInterval time.Duration

	// Window is how much history a rank share is a share of (§7.7.4).
	//
	// Bounded rather than cumulative because the counters are monotonic: a
	// share taken off the totals answers "what fraction of this class's whole
	// recorded life ran at rank 2", which means a bad week in March goes on
	// reporting in June and a bad two hours right now reports nothing.
	Window time.Duration

	// AlertInterval is how often §8.2's machine runs. It is also the sampling
	// rate of the window ring, so it bounds how finely the share can move.
	AlertInterval time.Duration

	// Dwell is §8.2's hysteresis. The zero value takes leeway's defaults.
	Dwell leeway.Dwell

	// ReconcileGrace bounds how long a persisted episode waits for a verdict
	// before it is discarded (§9.3 step 6).
	ReconcileGrace time.Duration

	// Rank0ShareFloor and LastRankShareCeiling are §7.7.4's two share rules.
	// Nil takes the shipped default, which is OFF for the floor and 0.9 for
	// the ceiling; zero is a setting and turns the rule off, which is why they
	// are pointers. See §7.7.5's "not all fallback is bad".
	Rank0ShareFloor      *float64
	LastRankShareCeiling *float64

	// UnusedTierFor is how long a preference tier must have been idle. Zero
	// takes the §7.7.4 default of thirty days.
	UnusedTierFor time.Duration

	// MigrationGrace is how long after capacity returns a stuck migration is
	// still just a slow one. Zero takes the default, which is spike S1's
	// measured 4 m 17 s with room.
	MigrationGrace time.Duration

	// TierCSignals routes the Tier C kinds to sinks as well as to metrics.
	// Off by default (§8.3): an unused preference tier is a cost observation,
	// and it is the operator's call whether it is worth waking up for.
	TierCSignals bool

	// Cluster names this cluster in the persisted episode rows.
	Cluster string

	// Meter is where the §8.4 instruments are declared. Nil means no-op, so a
	// source constructed without telemetry still runs.
	Meter metric.Meter
}

// DefaultConfig returns the shipped defaults.
func DefaultConfig() Config {
	return Config{
		ClassLabel:              "cloud.google.com/compute-class",
		PriorityIndexAnnotation: "ccc_priority_index",
		Profile:                 leeway.DefaultNodeProfileConfig(),
		FlushInterval:           DefaultFlushInterval,
		Window:                  DefaultWindow,
		AlertInterval:           DefaultAlertInterval,
		Dwell:                   leeway.DefaultDwell(),
		ReconcileGrace:          DefaultReconcileGrace,
	}
}

// DefaultFlushInterval is how often a quiescent rank advances.
const DefaultFlushInterval = 30 * time.Second

// DefaultWindow is how much history a rank share is a share of.
//
// An hour, which is long enough that a rolling replacement of an estate does
// not read as a fallback and short enough that yesterday's recovery does not
// hold today's finding open. It composes with the dwell rather than duplicating
// it: the window decides whether the condition is true now, the dwell decides
// whether it has been true long enough to say.
const DefaultWindow = time.Hour

// DefaultAlertInterval is how often §8.2's machine runs, and how finely the
// window is sampled.
const DefaultAlertInterval = time.Minute

// DefaultReconcileGrace is how long a persisted episode waits for a verdict.
//
// Longer than topologydrift's equivalent would need to be, because the window
// this source judges over is itself empty at startup: an episode restored from
// the store meets nothing but abstentions until MinWindow has elapsed, and
// discarding it before then would turn every restart into a lost dwell that the
// persistence exists to prevent.
const DefaultReconcileGrace = 15 * time.Minute

// normalize fills the zero fields from the defaults.
func (c Config) normalize() Config {
	d := DefaultConfig()
	if c.ClassLabel == "" {
		c.ClassLabel = d.ClassLabel
	}
	if c.PriorityIndexAnnotation == "" {
		c.PriorityIndexAnnotation = d.PriorityIndexAnnotation
	}
	if c.Profile.InstanceTypeLabel == "" && c.Profile.MachineFamilyLabel == "" {
		c.Profile = d.Profile
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = d.FlushInterval
	}
	if c.Window <= 0 {
		c.Window = d.Window
	}
	if c.AlertInterval <= 0 {
		c.AlertInterval = d.AlertInterval
	}
	if c.ReconcileGrace <= 0 {
		c.ReconcileGrace = d.ReconcileGrace
	}
	// Dwell is deliberately not filled here: leeway.AlertState.Advance
	// normalizes it itself, and a partial Dwell means §8.2's 3× resolve rule
	// rather than the shipped resolve time. Copying the defaults in would
	// silently overrule that.
	return c
}

// infer reports whether the cross-check is on.
func (c Config) infer() bool { return c.Infer == nil || *c.Infer }

// New constructs the source. dyn is the dynamic client the ComputeClass
// informer reads through; client supplies discovery for the CRD gate and the
// node and pod informers when no factory is supplied.
func New(client kubernetes.Interface, dyn dynamic.Interface, cfg Config) (*Source, error) {
	cfg = cfg.normalize()
	extractor, err := leeway.NewProfileExtractor(cfg.Profile)
	if err != nil {
		return nil, fmt.Errorf("%s: node profile config: %w", Name, err)
	}
	resolver := leeway.NewRankResolver()
	resolver.Infer = cfg.infer()
	return &Source{
		client:         client,
		dyn:            dyn,
		cfg:            cfg,
		extractor:      extractor,
		resolver:       resolver,
		tracker:        leeway.NewRankTracker(),
		classes:        map[string]*leeway.ComputeClass{},
		decodeFails:    map[string]int64{},
		nodes:          map[string]*nodeState{},
		pods:           map[podRef]*podState{},
		podsByNode:     map[string]map[podRef]struct{}{},
		pendingByClass: map[string]map[podRef]struct{}{},
		transitions:    map[transitionKey]int64{},
		history:        map[leeway.AxisKey]*axisHistory{},
		alerts:         newRankAlerts(cfg.Dwell),
	}, nil
}

// AlertStore is §9.1's persistence seam, satisfied by *store.Store.
//
// The same three methods topologydrift declares, and deliberately the same
// table: the grain is identical — one subject, one independent episode per axis
// of judgement — and the two key columns are free text. What keeps the two
// sources out of each other's rows is the subject KIND, which each filters on
// at load; see rankAlerts.Load.
type AlertStore interface {
	// LeewayAlertStates returns one cluster's persisted episodes.
	LeewayAlertStates(ctx context.Context, cluster string) ([]leeway.AlertRecord, error)
	// PutLeewayAlertState writes one episode.
	PutLeewayAlertState(ctx context.Context, rec leeway.AlertRecord) error
	// DeleteLeewayAlertState ends one episode.
	DeleteLeewayAlertState(ctx context.Context, cluster, subjectKey, topologyKey string) error
}

// WithStore gives the source somewhere to persist its dwell timers.
//
// Optional, and running without one is a supported deployment rather than a
// degraded mode — it is what `watch` does with no `--store`. §9.2's rule is
// that a missing history costs one dwell, never the monitoring.
func (s *Source) WithStore(st AlertStore, cluster string) {
	if st == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store = st
	s.cfg.Cluster = cluster
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: ComputeClasses are cluster-scoped and the
// rank of a pod is a property of the node it landed on, so a namespace-tier
// deployment gets the loud §11 startup failure rather than rank shares
// computed over the fraction of the estate it can see.
func (s *Source) Scope() sources.Scope { return sources.ScopeCluster }

// RequiredAccess implements sources.AccessDeclarer (§11): list+watch on
// ComputeClasses. Nodes and pods are already granted for every cluster-tier
// source and are not re-declared here.
//
// Like the gateway source's Gateway-API rules, this grant is inert on a
// cluster without the CRD — RBAC naming an absent group is legal and ignored —
// so the SSAR probe passes wherever the ClusterRole is applied, and the
// CRD-presence gate in auto.go is what actually decides auto-enable.
func (s *Source) RequiredAccess() []sources.Requirement { return RequiredAccess() }

// RequiredAccess is the same declaration without a Source. New can fail (a bad
// profile config), so the auto-resolution probe in internal/watch cannot build
// a throwaway source the way it does for every other candidate — and a probe
// that swallowed that error would silently probe nothing. The grant does not
// depend on configuration, so it does not need an instance.
func RequiredAccess() []sources.Requirement {
	return []sources.Requirement{
		{Group: computeClassGV.Group, Resource: computeClassGVR.Resource, Verb: "list"},
		{Group: computeClassGV.Group, Resource: computeClassGVR.Resource, Verb: "watch"},
	}
}

// WithFactory directs Run to take its Pod informer from an externally owned
// shared factory. Call before Run; nil is ignored.
func (s *Source) WithFactory(f informers.SharedInformerFactory) {
	if f != nil {
		s.factory = f
	}
}

// WithNodeFactory directs Run to take the Node informer from a different
// factory than the pod one, for the same reason topologydrift does: a
// namespace deny list is applied as a field selector, and `metadata.namespace`
// is not selectable on a cluster-scoped resource.
func (s *Source) WithNodeFactory(f informers.SharedInformerFactory) {
	if f != nil {
		s.nodeFactory = f
	}
}

// WithMeter directs Run to declare its instruments against an externally owned
// meter. Call before Run.
func (s *Source) WithMeter(m metric.Meter) {
	if m != nil {
		s.cfg.Meter = m
	}
}

// ComputeClassesServed reports whether the cluster serves the ComputeClass CR.
// It is the discovery gate internal/watch/auto.go gives resolveSourcesAuto, so
// a non-GKE cluster (or a GKE cluster on a version without custom compute
// classes) skips the source instead of enabling a watch that would fail at
// startup.
func ComputeClassesServed(client kubernetes.Interface) bool {
	resources, err := client.Discovery().ServerResourcesForGroupVersion(computeClassGV.String())
	if err != nil || resources == nil {
		return false
	}
	for _, r := range resources.APIResources {
		if r.Name == computeClassGVR.Resource {
			return true
		}
	}
	return false
}

// HasSynced implements sources.SyncReporter.
func (s *Source) HasSynced() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.synced
}

func (s *Source) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *Source) logPrintf(format string, args ...any) {
	if s.logf != nil {
		s.logf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// Run implements sources.Source.
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()

	if !ComputeClassesServed(s.client) {
		return fmt.Errorf("%s: %s is not served — this source reads GKE custom compute classes, so drop %q from --sources on a cluster without them",
			Name, computeClassGVR, Name)
	}

	factory := s.factory
	nodeFactory := s.nodeFactory
	owned := false
	if factory == nil {
		factory = informers.NewSharedInformerFactory(s.client, 0)
		owned = true
	}
	if nodeFactory == nil {
		nodeFactory = factory
	}

	var synced []cache.InformerSynced

	podInformer := factory.Core().V1().Pods().Informer()
	h, err := podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.onPod(obj) },
		UpdateFunc: func(_, obj any) { s.onPod(obj) },
		DeleteFunc: func(obj any) { s.onPodDelete(obj) },
	})
	if err != nil {
		return fmt.Errorf("%s: register Pod handler: %w", Name, err)
	}
	synced = append(synced, h.HasSynced)

	nodeInformer := nodeFactory.Core().V1().Nodes().Informer()
	h, err = nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.onNode(obj) },
		UpdateFunc: func(_, obj any) { s.onNode(obj) },
		DeleteFunc: func(obj any) { s.onNodeDelete(obj) },
	})
	if err != nil {
		return fmt.Errorf("%s: register Node handler: %w", Name, err)
	}
	synced = append(synced, h.HasSynced)

	// The CRD watch is its own dynamicinformer factory: different client type,
	// it cannot merge into the typed one. NFR-5 budgets exactly this — one
	// additional cluster-wide watch stream, on a resource with a handful of
	// objects.
	dynFactory := dynamicinformer.NewDynamicSharedInformerFactory(s.dyn, 0)
	classInformer := dynFactory.ForResource(computeClassGVR).Informer()
	h, err = classInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.onClass(obj) },
		UpdateFunc: func(_, obj any) { s.onClass(obj) },
		DeleteFunc: func(obj any) { s.onClassDelete(obj) },
	})
	if err != nil {
		return fmt.Errorf("%s: register ComputeClass handler: %w", Name, err)
	}
	synced = append(synced, h.HasSynced)

	if err := s.startMetrics(); err != nil {
		return err
	}
	defer func() {
		if err := s.closeMetrics(); err != nil {
			s.logPrintf("%s: unregister metrics: %v", Name, err)
		}
	}()

	factory.Start(ctx.Done())
	if nodeFactory != factory {
		nodeFactory.Start(ctx.Done())
	}
	dynFactory.Start(ctx.Done())
	if owned {
		defer factory.Shutdown()
	}
	defer dynFactory.Shutdown()

	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		return ctx.Err()
	}
	s.mu.Lock()
	s.synced = true
	s.mu.Unlock()

	// One reconcile after the barrier. The three caches sync independently, so
	// a node handled before its class arrived resolved against an axis that
	// did not exist yet; without this it would stay unplaced until GKE next
	// touched it, which for a stable node is never.
	s.reconcile(s.clock())
	s.loadAlerts(ctx)

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()
	alertTick := time.NewTicker(s.cfg.AlertInterval)
	defer alertTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.tracker.Flush(s.clock())
		case <-alertTick.C:
			s.runAlertPass(ctx)
		}
	}
}

// loadAlerts seats §9.1's persisted episodes, after the barrier.
//
// After, not before: Load holds the records aside until a verdict turns up for
// them, and a verdict cannot exist until the caches have something in them. A
// store that errors is logged and ignored — §9.2's rule is that an unreadable
// history costs one dwell rather than the monitoring.
func (s *Source) loadAlerts(ctx context.Context) {
	s.mu.Lock()
	st, cluster := s.store, s.cfg.Cluster
	s.mu.Unlock()
	if st == nil {
		return
	}
	recs, err := st.LeewayAlertStates(ctx, cluster)
	if err != nil {
		s.logPrintf("%s: read persisted alert state: %v", Name, err)
		return
	}
	if n := s.alerts.Load(recs, s.clock()); n > 0 {
		s.logPrintf("%s: restored %d open rank episode(s)", Name, n)
	}
}

// runAlertPass judges every axis, advances §8.2's machine and emits what
// crossed its dwell.
//
// Persist first, emit second, and only TransitionFiring emits. Both rules are
// topologydrift's and hold here for the same reasons: a finding on the wire the
// store does not know about becomes a duplicate at the next restart, and
// Resolved is answered by the clearance observer rather than by a second signal
// — a source that emitted its own would produce two.
func (s *Source) runAlertPass(ctx context.Context) {
	now := s.clock()
	js := s.judgeAll(now)
	byAxis := make(map[leeway.AxisKey]*rankJudgement, len(js))
	for i := range js {
		byAxis[js[i].axis] = &js[i]
	}

	for _, o := range s.alerts.pass(js, now, s.cfg.ReconcileGrace) {
		s.persistOutcome(ctx, o, now)
		if o.Transition == leeway.TransitionFiring {
			s.emitFinding(byAxis[o.Key.Axis], o, now)
		}
	}
}

// emitFinding builds, routes and sends the §8.5 payload for one episode.
func (s *Source) emitFinding(j *rankJudgement, o rankOutcome, now time.Time) {
	s.mu.Lock()
	emit := s.emit
	s.mu.Unlock()
	if emit == nil || j == nil || o.Verdict == nil {
		return
	}
	f, delivery := s.findingFor(j, *o.Verdict, o.State)
	if !delivery.Signal {
		// Metrics only (§8.3). Not logged: the suppressed kind is the one that
		// fires on every class with a reserved tier, and a line per episode
		// would be a log nobody reads about the decision that keeps the source
		// quiet. The alert_state series already shows the episode.
		return
	}
	emit(signalFor(f, o.Key, now))
}

// persistOutcome writes one episode's move through to the store.
//
// Only a move is written, and a Gone episode is deleted rather than stored in
// PhaseOK — which is what makes a recurrence read as new rather than as the
// resumption of something the table never let go of.
func (s *Source) persistOutcome(ctx context.Context, o rankOutcome, now time.Time) {
	s.mu.Lock()
	st, cluster := s.store, s.cfg.Cluster
	s.mu.Unlock()
	if st == nil {
		return
	}

	subject, state := o.Key.subjectKey(), o.Key.stateKey()
	if o.Gone {
		if err := st.DeleteLeewayAlertState(ctx, cluster, subject, state); err != nil {
			s.logPrintf("%s: delete alert state for %s/%s: %v", Name, subject, state, err)
		}
		return
	}
	if o.Transition == leeway.TransitionNone {
		return
	}
	if err := st.PutLeewayAlertState(ctx, leeway.AlertRecord{
		Cluster:     cluster,
		SubjectKey:  subject,
		TopologyKey: state,
		AlertState:  o.State,
		UpdatedAt:   now,
	}); err != nil {
		s.logPrintf("%s: persist alert state for %s/%s: %v", Name, subject, state, err)
	}
}

// onPod / onNode / onClass adapt informer events to the state machine. The
// unstructured cast is the only place a cache.DeletedFinalStateUnknown
// tombstone can reach us, so each delete path unwraps one.

func (s *Source) onPod(obj any) {
	if pod, ok := obj.(*corev1.Pod); ok {
		s.UpsertPod(pod, s.clock())
	}
}

func (s *Source) onPodDelete(obj any) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	if pod, ok := obj.(*corev1.Pod); ok {
		s.DeletePod(pod, s.clock())
	}
}

func (s *Source) onNode(obj any) {
	if node, ok := obj.(*corev1.Node); ok {
		s.UpsertNode(node, s.clock())
	}
}

func (s *Source) onNodeDelete(obj any) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	if node, ok := obj.(*corev1.Node); ok {
		s.DeleteNode(node.Name, s.clock())
	}
}

func (s *Source) onClass(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	spec, _, _ := unstructured.NestedMap(u.Object, "spec")
	s.UpsertClass(u.GetName(), spec, s.clock())
}

func (s *Source) onClassDelete(obj any) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	s.DeleteClass(u.GetName(), s.clock())
}
