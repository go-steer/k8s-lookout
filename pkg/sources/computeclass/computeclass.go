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
// None yet, deliberately. This phase ships the axes, the accounting and the
// §7.7.3 metric set; the four `leeway.rank_*` kinds of §7.7.4 are the next
// one, and they are gated on policy fields (`whenUnsatisfiable`,
// `activeMigration.optimizeRulePriority`) this source already decodes. The
// same staging the topology half took: a source that exports its numbers and
// self-verifies before it is allowed to page anybody.
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
		FlushInterval:           30 * time.Second,
	}
}

// DefaultFlushInterval is how often a quiescent rank advances.
const DefaultFlushInterval = 30 * time.Second

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
		client:      client,
		dyn:         dyn,
		cfg:         cfg,
		extractor:   extractor,
		resolver:    resolver,
		tracker:     leeway.NewRankTracker(),
		classes:     map[string]*leeway.ComputeClass{},
		decodeFails: map[string]int64{},
		nodes:       map[string]*nodeState{},
		pods:        map[podRef]*podState{},
		podsByNode:  map[string]map[podRef]struct{}{},
		transitions: map[transitionKey]int64{},
	}, nil
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
//
// emit is accepted and unused: this phase exports metrics only, and the §7.7.4
// kinds are the next one. Taking the parameter now keeps the signature honest
// against the interface rather than inventing a second one later.
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	_ = emit

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

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.tracker.Flush(s.clock())
		}
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
