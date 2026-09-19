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
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
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

	// VerifyInterval and VerifyShards configure §6.5's verifier, taking
	// DefaultVerifyInterval and DefaultVerifyShards when zero.
	//
	// Deliberately not flags. There is no operational decision here to
	// delegate: the defaults put a full pass at one hour, and an operator who
	// lengthened that would be choosing to detect a counter bug more slowly in
	// exchange for nothing measurable. They exist so tests can run a pass
	// without waiting five minutes.
	VerifyInterval time.Duration
	VerifyShards   int

	// ClusterDefaultConstraints is the cluster's PodTopologySpread
	// defaultConstraints (FR-9), and it is a pointer on purpose: nil is "we
	// have never been told", an empty slice is "an operator asserts there are
	// none", and collapsing those two is the silent-assumption bug spike S4
	// exists to stop. See ClusterDefaultIntents.
	ClusterDefaultConstraints *[]corev1.TopologySpreadConstraint

	// PerDomainSeries exports the per-subject, per-domain counts for *every*
	// tracked subject, not only the drifting ones. See PerDomainGate for why
	// that is a deliberate opt-in and PerDomainSeriesMinDrift for what a
	// deployment gets without it.
	PerDomainSeries bool

	// PerDomainSeriesMinDrift is §8.4's cardinality floor on the per-domain
	// series. Zero takes DefaultPerDomainSeriesMinDrift; negative admits every
	// scored subject.
	PerDomainSeriesMinDrift float64

	// Thresholds are §7.4's scoring thresholds. Nil takes
	// leeway.DefaultThresholds.
	//
	// A pointer rather than a value, because these are five fields that must
	// be set together: a caller that wanted a stricter drift threshold and set
	// the struct's one field would otherwise get a MinReplicasForScoring of
	// zero along with it, and score every single-replica workload in the
	// cluster. Take DefaultThresholds and amend it.
	Thresholds *leeway.Thresholds

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
	if c.PerDomainSeriesMinDrift == 0 {
		c.PerDomainSeriesMinDrift = DefaultPerDomainSeriesMinDrift
	}
	if c.Thresholds == nil {
		t := leeway.DefaultThresholds()
		c.Thresholds = &t
	}
	return c
}

// perDomainGate is §8.4's gate as the metrics layer takes it. A negative floor
// is the configured way to say "every scored subject", which the gate spells
// as a zero floor — drift is never negative, so passing it through unchanged
// would work by accident rather than by contract.
func (c Config) perDomainGate() PerDomainGate {
	g := PerDomainGate{All: c.PerDomainSeries, MinDrift: c.PerDomainSeriesMinDrift}
	if g.MinDrift < 0 {
		g.MinDrift = 0
	}
	return g
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

	// nodeFactory, when set via WithNodeFactory, is where the Node informer
	// comes from. Unset means factory.
	nodeFactory informers.SharedInformerFactory

	// dyn, when set via WithDynamic, is the dynamic client the optional
	// LeewayPolicy informers are built on. Unset means no policy watch at all,
	// which is the same observable state as a cluster without the CRDs.
	dyn dynamic.Interface

	inv     *Inventory
	state   *State
	queue   *coalescer
	metrics *instruments
	verify  *Verifier
	// policies holds the LeewayPolicy objects the informers deliver. Always
	// non-nil so that policyFor needs no second nil check; an empty store is
	// the normal deployment.
	policies *PolicyStore

	mu sync.Mutex
	// armed flips true after every informer cache syncs and the initial
	// re-apply has run.
	armed bool
	// replicaSets resolves the pod → ReplicaSet → Deployment hop. Set in Run.
	replicaSets appslisters.ReplicaSetLister
	// pods is the verifier's view of the cache. Set in Run.
	pods corelisters.PodLister
	// claims and volumes resolve FR-8's pod → PVC → PV hop. Set in Run.
	claims  corelisters.PersistentVolumeClaimLister
	volumes corelisters.PersistentVolumeLister
	// sweepPending records that some node's eligibility moved since the last
	// sweep. See DefaultEligibilitySweepInterval.
	sweepPending bool
	// warnedTies deduplicates the ambiguous-policy warning. Keyed by the set of
	// competing policies rather than by subject: the operator fixes a pair of
	// policies, not each of the subjects they both match, and keying it this
	// way bounds the map by the number of policies (tens) instead of by the
	// number of subjects (tens of thousands).
	warnedTies map[string]bool

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
	s.policies = NewPolicyStore()
	s.warnedTies = map[string]bool{}
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
		Pinned:    VolumePins(s.boundVolume, cfg.TopologyKeys),
	})
	s.verify = NewVerifier(VerifyOptions{
		State:      s.state,
		Pods:       s.cachedPods,
		Interval:   cfg.VerifyInterval,
		Shards:     cfg.VerifyShards,
		OnMismatch: s.onMismatch,
		Logf:       func(f string, a ...any) { s.logger()(f, a...) },
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

// WithNodeFactory directs Run to take the Node informer from a different
// factory than the namespaced ones. Call before Run; nil is ignored, and
// unset means "the same factory as everything else", which is the caller's
// normal case.
//
// It exists because a namespace deny list is applied as a field selector on the
// factory, and `metadata.namespace` is not selectable on a cluster-scoped
// resource — the API server rejects the node LIST outright rather than ignoring
// the term. A caller that scopes its namespaced watches therefore has to hand
// the node watch a factory that carries no selector.
func (s *Source) WithNodeFactory(f informers.SharedInformerFactory) {
	if f != nil {
		s.nodeFactory = f
	}
}

// WithDynamic gives Run a dynamic client to build the optional LeewayPolicy
// informers on. Call before Run; nil is ignored.
//
// Separate from New's signature, unlike the gateway source which takes its
// dynamic client as a constructor parameter, because there the CRD is the
// entire subject and a source without it has nothing to watch. Here the policy
// watch is an optional override on a source that works fully without it, so a
// caller that does not care — every unit test, and any embedder that has no
// dynamic client — should not have to name it.
func (s *Source) WithDynamic(dyn dynamic.Interface) {
	if dyn != nil {
		s.dyn = dyn
	}
}

// warnAmbiguous logs a policy tie once per competing set. Never called under
// s.mu by its caller; it takes the lock itself.
func (s *Source) warnAmbiguous(sub leeway.SubjectRef, m PolicyMatch) {
	tie := strings.Join(m.Competing, ",")
	s.mu.Lock()
	seen := s.warnedTies[tie]
	s.warnedTies[tie] = true
	s.mu.Unlock()
	if seen {
		return
	}
	s.logger()("topologydrift: %d policies match the same subject (e.g. %s) and there is no defensible ordering between their selectors — applying %q; narrow one of %s to make this deterministic",
		len(m.Competing), sub, m.Policy.ref(), tie)
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
		// FR-8: a pod bound to a volume that cannot follow it is drift nobody
		// can act on, and saying so needs the claim's binding and the volume's
		// node affinity.
		{"", "persistentvolumeclaims"},
		{"", "persistentvolumes"},
	} {
		for _, verb := range []string{"list", "watch"} {
			reqs = append(reqs, sources.Requirement{Group: r.group, Resource: r.resource, Verb: verb})
		}
	}
	// FR-10's policies are deliberately absent. §11's access check is a
	// coverage contract — it fails startup when a grant the source needs to do
	// its job is missing — and the source does its whole job without ever
	// reading a policy. Declaring them would turn an optional override into a
	// startup prerequisite on every cluster, including the ones that never
	// install the CRD, which is exactly the coupling keeping the CRD out of
	// deploy/kustomization.yaml avoids. A missing grant is instead handled
	// where it happens: the discovery gate in Run logs and continues.
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

// Verifier exposes §6.5's auditor, for tests.
func (s *Source) Verifier() *Verifier { return s.verify }

// cachedPods implements PodSnapshot against the pod informer's cache. Before
// Run has a lister it reports an error rather than an empty cluster: a verifier
// handed zero pods would conclude that every tracked subject is a leak and
// "repair" the entire state to nothing.
func (s *Source) cachedPods() ([]*corev1.Pod, error) {
	s.mu.Lock()
	lister := s.pods
	s.mu.Unlock()
	if lister == nil {
		return nil, fmt.Errorf("topologydrift: pod cache not ready")
	}
	return lister.List(labels.Everything())
}

// onMismatch records the §6.5 SLI. Nothing here is attached to a request, so
// the background context is the honest one.
func (s *Source) onMismatch(kind leeway.SubjectKind) {
	s.metrics.recordMismatch(context.Background(), kind)
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

// boundVolume implements VolumeLookup against the PVC and PV caches.
//
// Before Run populates the listers it reports nothing bound, which reads as
// unpinned — the same "retried on the next event" shape as ownerOf, and for
// FR-8 a benign one: the first thing Run does after the caches sync is reapply,
// which re-counts every pod with the listers in place.
func (s *Source) boundVolume(namespace, claim string) (*corev1.PersistentVolume, bool) {
	s.mu.Lock()
	claims, volumes := s.claims, s.volumes
	s.mu.Unlock()
	if claims == nil || volumes == nil {
		return nil, false
	}
	pvc, err := claims.PersistentVolumeClaims(namespace).Get(claim)
	if err != nil {
		return nil, false
	}
	// An unbound claim has no VolumeName, and a claim that is Pending because
	// its class provisions on first consumer will get one the moment the pod is
	// scheduled — which is also the moment the pod starts occupying a domain.
	if pvc.Spec.VolumeName == "" {
		return nil, false
	}
	pv, err := volumes.Get(pvc.Spec.VolumeName)
	if err != nil {
		return nil, false
	}
	return pv, true
}

// policyCRDsServed asks discovery which of the two policy kinds the cluster
// serves. A discovery error — which is what an uninstalled CRD looks like,
// since the whole group is absent — reads as "neither", because that is the
// same observable state and the same correct behaviour.
func (s *Source) policyCRDsServed() (namespaced, clusterScoped bool) {
	resources, err := s.client.Discovery().ServerResourcesForGroupVersion(policyGV.String())
	if err != nil || resources == nil {
		return false, false
	}
	for _, r := range resources.APIResources {
		switch r.Name {
		case policyGVR.Resource:
			namespaced = true
		case clusterPolicyGVR.Resource:
			clusterScoped = true
		}
	}
	return namespaced, clusterScoped
}

// startPolicyWatch registers informers for whichever policy CRDs the cluster
// serves and returns their sync barriers, or nothing at all.
//
// **Absent CRDs log and continue.** This is the opposite of the gateway
// source, which returns an error when its CRD is missing, and the difference is
// which way the silence points. There, the source was named explicitly or by a
// discovery gate that would have skipped it, so an empty watch would be a
// coverage lie. Here the source is default-on and complete without any policy:
// refusing to start would make an optional override a prerequisite for a
// cluster-wide watcher, and the operator who installs the CRD later gets it at
// the next restart, which the manifest says out loud.
//
// The gate is evaluated once. Watching for the CRD itself to appear would mean
// a second watch on apiextensions — a grant we would then need everywhere — to
// save a restart on a one-off installation step.
func (s *Source) startPolicyWatch(ctx context.Context) ([]cache.InformerSynced, error) {
	if s.dyn == nil {
		return nil, nil
	}
	namespaced, clusterScoped := s.policyCRDsServed()
	if !namespaced && !clusterScoped {
		s.logger()("topologydrift: %s not installed — placement intent is inferred only (apply deploy/crds/leewaypolicies.yaml and restart to declare it)", policyGV)
		return nil, nil
	}

	factory := dynamicinformer.NewDynamicSharedInformerFactory(s.dyn, 0)
	var synced []cache.InformerSynced
	for _, w := range []struct {
		gvr           schema.GroupVersionResource
		serve         bool
		clusterScoped bool
	}{
		{policyGVR, namespaced, false},
		{clusterPolicyGVR, clusterScoped, true},
	} {
		if !w.serve {
			s.logger()("topologydrift: %s not served — %s policies ignored", w.gvr, w.gvr.Resource)
			continue
		}
		scoped := w.clusterScoped
		inf := factory.ForResource(w.gvr).Informer()
		h, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { s.onPolicy(obj, scoped) },
			UpdateFunc: func(_, obj any) { s.onPolicy(obj, scoped) },
			DeleteFunc: func(obj any) { s.onPolicyDelete(obj) },
		})
		if err != nil {
			return nil, fmt.Errorf("topologydrift: register %s handler: %w", w.gvr.Resource, err)
		}
		synced = append(synced, h.HasSynced)
	}
	factory.Start(ctx.Done())
	return synced, nil
}

// onPolicy decodes and stores one policy object.
//
// A policy that does not decode is dropped with a log line and the previous
// version of it, if any, is left in place. Replacing it with nothing would mean
// a typo in one field silently reverts a subject to inferred intent — the
// operator sees their policy stop applying and nothing tells them why. The
// structural schema rejects most of this at admission; what reaches here is
// what a schema cannot express, such as an all-zero expectedDistribution.
func (s *Source) onPolicy(obj any, clusterScoped bool) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	p, err := DecodePolicy(u, clusterScoped)
	if err != nil {
		s.logger()("topologydrift: ignoring %s %s/%s: %v", u.GetKind(), u.GetNamespace(), u.GetName(), err)
		return
	}
	s.policies.Upsert(p)
}

// onPolicyDelete removes a policy, resolving the tombstone the informer
// delivers when it missed the delete itself.
func (s *Source) onPolicyDelete(obj any) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	s.policies.Delete(u.GetNamespace(), u.GetName())
}

// evaluate is the coalesced per-subject callback.
//
// It resolves intent and eligibility, scores the subject on every axis and
// stores both results; it emits nothing. The §8.2 state machine and the
// findings that come out of it are the next increment, and keeping them out of
// this one means the whole measurement stack — inference, apportionment and the
// §7.3 scores — reaches a real cluster and a real dashboard before anything can
// page on it. A wrong number here is a wrong number on a graph.
func (s *Source) evaluate(ctx context.Context, sub leeway.SubjectRef) {
	start := time.Now()
	defer func() { s.metrics.recordEvaluation(ctx, sub.Kind, time.Since(start)) }()

	pod := s.subjectPod(sub)
	if pod == nil {
		// No pod to read means no evidence, and no evidence is not the same as
		// no intent: clearing what the last evaluation resolved would drop a
		// subject's intent_info every time its pods turned over faster than the
		// cache. The stale answer is the better one, and it is bounded — the
		// subject is forgotten outright once its last pod stops counting.
		return
	}
	res := Resolve(pod, s.inv, ResolveConfig{
		ClusterDefaults: s.cfg.ClusterDefaultConstraints,
		Policy:          s.policyFor(sub, pod),
	})
	s.state.SetIntents(sub, res.Intents)

	// Snapshot and then score outside the index lock. Scoring is O(domains)
	// arithmetic and holding the lock across it would put every pod event in
	// the cluster behind whichever subject the queue happened to reach.
	s.state.SetEvaluations(sub, ScoreSubject(res, s.state.Snapshot(sub), *s.cfg.Thresholds))
}

// policyFor picks the LeewayPolicy governing sub, or nil when none does — the
// normal case, since the CRD is optional and most clusters never install it.
//
// The selector is matched against the *pod's* labels, not the workload's. The
// source resolves subjects from pods and never reads the owning object, so a
// label that exists only on the Deployment is not available to match; the pod
// carries the template's labels plus the controller's own, which is a superset
// of what a workload selector would offer anyway.
func (s *Source) policyFor(sub leeway.SubjectRef, pod *corev1.Pod) *Policy {
	m := s.policies.For(sub, pod.Labels)
	if m.Ambiguous {
		// Reported, not resolved: there is no honest ordering between two
		// selectors, so the operator has to be told which one won rather than
		// left to discover it from the intent it produced. Rate-limited,
		// because this runs on every coalesced evaluation of a matched subject
		// and the condition persists until someone edits a policy.
		s.warnAmbiguous(sub, m)
	}
	return m.Policy
}

// subjectPod returns an admitted pod belonging to sub, for inference to read.
//
// The fast path is State's representative hint: one map get and one lister get,
// which is what keeps evaluation independent of cluster size. The hint is a
// hint, so a miss falls through to listing the subject's namespace and electing
// a new one — plain O(pods in namespace), taken rarely and never cached into a
// per-pod index.
//
// That the fallback is un-indexed is the decision, not an omission. A
// subject-keyed pod index has to run resolveSubject to place a pod, which reads
// the ReplicaSet cache — so a pod's index entry would depend on a *different*
// informer's state, and an informer re-indexes an object only when that object
// changes. With the resync period at zero, an entry computed before the
// ReplicaSet landed is never repaired. A hint that can be wrong and is checked
// on every read has no such failure mode.
//
// Misses also cluster in exactly the case where paying for them is wrong: a
// domain outage turns over every pod at once, and §7.6 suppresses workload
// findings for the duration and raises one domain finding instead. Skipping an
// evaluation whose output would be suppressed anyway is the right answer, so
// the fallback is allowed to be slow because it is allowed to be rare.
func (s *Source) subjectPod(sub leeway.SubjectRef) *corev1.Pod {
	s.mu.Lock()
	lister := s.pods
	s.mu.Unlock()
	if lister == nil {
		return nil
	}
	if name, ok := s.state.Representative(sub); ok {
		if pod, err := lister.Pods(sub.Namespace).Get(name); err == nil {
			return pod
		}
	}
	return s.electRepresentative(lister, sub)
}

// electRepresentative finds a pod for sub the hard way and records it.
//
// It picks the most recently created pod, breaking ties on name. Newest because
// during a rollout it is the one carrying the current template — inference that
// reads the pod being replaced describes intent the workload has already
// abandoned; deterministic because two evaluations of an unchanged subject that
// disagree would flap `intent_info` on nothing but map iteration order.
func (s *Source) electRepresentative(lister corelisters.PodLister, sub leeway.SubjectRef) *corev1.Pod {
	pods, err := lister.Pods(sub.Namespace).List(labels.Everything())
	if err != nil {
		return nil
	}
	var best *corev1.Pod
	for _, pod := range pods {
		got, ok := resolveSubject(pod, s.ownerOf)
		if !ok || got != sub {
			continue
		}
		if best == nil || newerPod(pod, best) {
			best = pod
		}
	}
	if best == nil {
		return nil
	}
	s.state.SetRepresentative(sub, best.UID, best.Name)
	return best
}

// newerPod reports whether a should be preferred over b as a representative.
func newerPod(a, b *corev1.Pod) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.After(b.CreationTimestamp.Time)
	}
	// Creation timestamps have one-second resolution, so a scale-out puts a
	// whole ReplicaSet on the same value. The name is the tiebreak that makes
	// the choice reproducible.
	return a.Name < b.Name
}

// Run implements sources.Source. emit is never called; see the Source doc.
func (s *Source) Run(ctx context.Context, _ func(sources.Signal)) error {
	factory, owned := s.factory, false
	if factory == nil {
		factory = informers.NewSharedInformerFactory(s.client, 0)
		owned = true
	}

	nodeFactory := s.nodeFactory
	if nodeFactory == nil {
		nodeFactory = factory
	}

	podInformer := factory.Core().V1().Pods()
	nodeInformer := nodeFactory.Core().V1().Nodes()
	rsInformer := factory.Apps().V1().ReplicaSets()
	pvcInformer := factory.Core().V1().PersistentVolumeClaims()
	// PersistentVolumes are cluster-scoped, so they go where the nodes go —
	// see WithNodeFactory for why a scoped caller's factory cannot list them.
	pvInformer := nodeFactory.Core().V1().PersistentVolumes()

	// The ReplicaSet, PVC and PV caches are read, not watched: the resolver and
	// the FR-8 pin predicate ask them for one object at a time and nothing here
	// reacts to any of the three changing. Touching the Lister before Start is
	// what gets the informer built and started with the rest.
	//
	// No handler on the volumes is a decision, not an omission. A pod whose
	// claim binds later is not a pod whose placement we have already got wrong:
	// until the claim binds the pod is unscheduled, so it holds no domain and
	// contributes to no distribution, and the event that schedules it is a pod
	// event we already act on. Watching the volumes instead would make a pod's
	// Pinned answer depend on a second informer's arrival order — the same trap
	// that ruled out a subject-keyed pod index in subjectPod.
	s.mu.Lock()
	s.replicaSets = rsInformer.Lister()
	s.pods = podInformer.Lister()
	s.claims = pvcInformer.Lister()
	s.volumes = pvInformer.Lister()
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

	// §13 S4's fourth mitigation, and the cheapest one: say once, out loud,
	// whose numbers the fleet is about to be scored against. An operator who
	// never configured anything should not have to read the source to find out
	// that a maxSkew is ours rather than theirs.
	s.logger()("topologydrift: %s", DescribeClusterDefaults(s.cfg.ClusterDefaultConstraints))

	policySynced, err := s.startPolicyWatch(ctx)
	if err != nil {
		return err
	}

	if err := s.startMetrics(); err != nil {
		return err
	}
	defer func() { _ = s.metrics.Close() }()

	factory.Start(ctx.Done())
	if nodeFactory != factory {
		// Start is idempotent per informer, so starting the same object twice
		// would be harmless — but these are two objects whenever the caller
		// scoped its namespaced watches, and the node informer lives only on
		// the second one.
		nodeFactory.Start(ctx.Done())
	}
	if owned {
		defer factory.Shutdown()
	}

	// The policy barriers join the rest rather than being awaited separately:
	// arming before they sync would run the first re-apply against inferred
	// intent and then quietly correct it, so every subject a policy governs
	// would publish one wrong intent_info sample at every restart.
	synced := append([]cache.InformerSynced{
		podH.HasSynced, nodeH.HasSynced, rsInformer.Informer().HasSynced,
		pvcInformer.Informer().HasSynced, pvInformer.Informer().HasSynced,
	}, policySynced...)
	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
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

	// §6.5. One shard per tick, on its own timer rather than folded into the
	// sweep: the sweep is conditional on a node having changed, and a counter
	// bug does not wait for one.
	verifyTick := time.NewTicker(s.verify.Interval())
	defer verifyTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.sweep()
		case <-verifyTick.C:
			s.runVerifyPass()
		}
	}
}

// runVerifyPass checks one shard and logs only when it found something. A
// component that logs a line every five minutes forever to say nothing happened
// is a component whose one important line gets scrolled past.
func (s *Source) runVerifyPass() {
	rep := s.verify.Tick()
	if rep.Err != nil || rep.Drifted > 0 {
		// Both are already logged in detail by the verifier itself; this is the
		// line that ties those per-subject reports to a shard.
		s.logger()("topology-drift: verify shard %d/%d: %d of %d subject(s) drifted",
			rep.Shard, s.verify.Shards(), rep.Drifted, rep.Subjects)
	}
}

// startMetrics declares the §8.4 instruments against the configured meter.
func (s *Source) startMetrics() error {
	in, err := newInstruments(metricsOptions{
		Meter:         s.cfg.Meter,
		PerDomain:     s.cfg.perDomainGate(),
		SubjectCounts: s.state.SubjectCounts,
		DomainNodes:   s.domainNodes,
		DomainObjects: s.domainObjects,
		Evaluations:   s.state.EachEvaluation,
		Intents:       s.state.EachIntent,
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
func (s *Source) domainObjects(gate PerDomainGate, yield countObserver) {
	for _, sub := range s.state.Subjects() {
		if !gate.Admits(s.state.DriftOf(sub)) {
			continue
		}
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
