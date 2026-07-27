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

// Package rollout is the rollout signal source (DESIGN.md §7.2 row 3):
// as-it-happens leading indicators from Deployments and StatefulSets
// with in-progress rollouts. The normative example: "new RS 0/1 ready
// for 4 min, old RS healthy — probable bad deploy", fired WELL BEFORE
// progressDeadlineSeconds.
//
// Distinct from objectstate.progress_deadline on purpose: that signal
// is deadline-clock-based (fraction of progressDeadlineSeconds
// consumed on the controller's own Progressing clock); this one is
// EVIDENCE-based — the new revision's pods are failing or not-ready
// while the old revision stays healthy, which is what a bad deploy
// looks like from the outside long before any deadline math notices.
// A rollout with progressDeadlineSeconds=600 (the default) stalls
// here after --rollout-observe (default 3m) of zero progress; the
// deadline signal would wait ~8 more minutes.
//
// Detection: a Deployment rollout is in progress when its newest
// ReplicaSet (highest deployment.kubernetes.io/revision) has
// desired > 0 while an older ReplicaSet still holds desired > 0
// replicas; a StatefulSet's when status.updateRevision !=
// status.currentRevision. The stall fires once per revision (dedup
// reason rollout_stall, uid = workload UID) when the new revision has
// made NO ready-count progress for the observe window while the old
// revision's ready ratio stays >= Config.OldReadyRatio. Any new-pod
// becoming ready resets the window — a slow-but-progressing rollout
// never fires.
//
// Arm-after-sync (same discipline as objectstate): the initial LIST
// populates workload/pod state without emitting; the source arms only
// after all caches sync. A rollout already stalled when the sentinel
// starts fires --rollout-observe after arming — for an as-it-happens
// source that is a fresh observation, not a replayed transition.
//
// The source also implements engine.ClearanceObserver for its own
// incidents (§7.4: each source that can observe a symptom can observe
// its absence): a rollout_stall clears when the rollout COMPLETES —
// which covers both the fix (a good revision rolled out) and the
// rollback (reverting re-promotes the old, already-ready ReplicaSet,
// completing immediately). A deleted workload clears as
// object_deleted, never as a fix.
package rollout

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the stable source name (§7.2 table) used in the signal
// schema and the `--sources` flag.
const Name = "rollout"

// KindStall is the one kind this source emits (§7.3). APPEND-ONLY:
// kinds are part of the signal schema playbooks and fleet consumers match on.
const KindStall = "rollout.stall"

// ReasonStall is the dedup/fingerprint reason (the kind suffix, same
// convention as objectstate). Maps to itself under CanonicalReason.
const ReasonStall = "rollout_stall"

// revisionAnnotation is the deployment controller's revision stamp on
// ReplicaSets — the ordering key for "newest RS".
const revisionAnnotation = "deployment.kubernetes.io/revision"

// revisionHashLabel is the StatefulSet controller's revision label on
// pods (apps/v1 ControllerRevision hash).
const revisionHashLabel = appsv1.ControllerRevisionHashLabelKey

// Config are the source's thresholds. Zero values take the defaults.
type Config struct {
	// Observe is how long the new revision must make zero ready-count
	// progress (while the old revision stays healthy) before
	// KindStall fires. The `--rollout-observe` flag. Default 3m.
	Observe time.Duration
	// OldReadyRatio is the minimum old-revision ready ratio for the
	// stall verdict: below it the problem is not "new pods failing
	// while old healthy" — it is the cluster, and other signals own
	// that. Default 0.9.
	OldReadyRatio float64
	// TickInterval drives the stall sweep (a stalled rollout stops
	// producing informer updates — the clock has to notice) and the
	// state-TTL prune. Default 15s.
	TickInterval time.Duration
	// StateTTL bounds per-object memory (safety net behind
	// DeleteFunc). Default 24h.
	StateTTL time.Duration
}

// DefaultConfig returns the shipped thresholds.
func DefaultConfig() Config {
	return Config{
		Observe:       3 * time.Minute,
		OldReadyRatio: 0.9,
		TickInterval:  15 * time.Second,
		StateTTL:      24 * time.Hour,
	}
}

// normalize fills zero fields with defaults.
func (c Config) normalize() Config {
	d := DefaultConfig()
	if c.Observe <= 0 {
		c.Observe = d.Observe
	}
	if c.OldReadyRatio <= 0 || c.OldReadyRatio > 1 {
		c.OldReadyRatio = d.OldReadyRatio
	}
	if c.TickInterval <= 0 {
		c.TickInterval = d.TickInterval
	}
	if c.StateTTL <= 0 {
		c.StateTTL = d.StateTTL
	}
	return c
}

// podInfo is the minimal per-pod state the stall evidence needs.
type podInfo struct {
	namespace string
	name      string
	// ownerUID is the controlling owner's UID (ReplicaSet for
	// Deployment pods, StatefulSet for STS pods).
	ownerUID types.UID
	// revisionHash is the controller-revision-hash label (STS pods).
	revisionHash  string
	ready         bool
	waitingReason string
	lastSeen      time.Time
}

// objEntry wraps a stored workload/ReplicaSet object with the
// TTL-prune stamp.
type objEntry[T any] struct {
	obj      T
	lastSeen time.Time
}

// track is the per-workload rollout progress memory.
type track struct {
	// revision identifies the in-progress target revision (new RS
	// name for Deployments, updateRevision for StatefulSets).
	revision string
	// since is when this revision was first observed in progress.
	since time.Time
	// maxNewReady is the best new-revision ready count seen for this
	// revision; any increase is progress and resets the window.
	maxNewReady int32
	// lastProgressAt is when the new revision last made progress
	// (revision start, or a ready-count increase).
	lastProgressAt time.Time
	// fired gates KindStall to once per revision.
	fired map[string]bool
	// complete is whether the LAST evaluation observed the workload
	// rollout-complete; completedAt stamps the not-complete→complete
	// TRANSITION and is the clearance StableSince. The transition
	// gate matters (M3 drill observation 4): a deployment that was
	// complete long before an incident must not lend that old
	// timestamp to the incident's clearance — the §7.4 stability
	// window has to count from when completion was OBSERVED after
	// the stall, or resolved records fire instantly with inverted
	// cleared_after/observed_stable_for durations.
	complete    bool
	completedAt time.Time
	lastSeen    time.Time
}

// Source implements sources.Source (and engine.ClearanceObserver) for
// the rollout row of §7.2.
type Source struct {
	client kubernetes.Interface
	cfg    Config
	// factory, when set via WithFactory, is the externally owned
	// shared informer factory (§6.3: one informer set serves the
	// sentinel sources and the graph).
	factory informers.SharedInformerFactory

	mu sync.Mutex
	// armed flips true after every informer cache syncs (§7.2
	// restart discipline — see the package comment).
	armed bool
	emit  func(engine.Signal)

	deployments  map[types.UID]*objEntry[*appsv1.Deployment]
	replicasets  map[types.UID]*objEntry[*appsv1.ReplicaSet]
	statefulsets map[types.UID]*objEntry[*appsv1.StatefulSet]
	pods         map[types.UID]*podInfo
	tracks       map[types.UID]*track

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
}

// New constructs the source. Zero-valued cfg fields take the shipped
// defaults.
func New(client kubernetes.Interface, cfg Config) *Source {
	return &Source{
		client:       client,
		cfg:          cfg.normalize(),
		deployments:  make(map[types.UID]*objEntry[*appsv1.Deployment]),
		replicasets:  make(map[types.UID]*objEntry[*appsv1.ReplicaSet]),
		statefulsets: make(map[types.UID]*objEntry[*appsv1.StatefulSet]),
		pods:         make(map[types.UID]*podInfo),
		tracks:       make(map[types.UID]*track),
	}
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: the informers list workloads
// cluster-wide (one shared informer set, §6.3), so the source needs
// cluster RBAC — a namespace-tier deployment gets the loud §11
// startup failure.
func (s *Source) Scope() sources.Scope { return sources.ScopeCluster }

// WithFactory directs Run to register its informers on an externally
// owned shared factory (§6.3). Call before Run; nil is ignored.
func (s *Source) WithFactory(f informers.SharedInformerFactory) {
	if f != nil {
		s.factory = f
	}
}

// ClearanceObserver returns the §7.4 clearance predicate for
// rollout_stall incidents, backed by this source's informers.
func (s *Source) ClearanceObserver() engine.ClearanceObserver { return s }

// RequiredAccess implements sources.AccessDeclarer (§11): list+watch
// on each informer target. Matches deploy/12-clusterrole-watcher.yaml.
func (s *Source) RequiredAccess() []sources.Requirement {
	var reqs []sources.Requirement
	for _, r := range []struct{ group, resource string }{
		{"", "pods"},
		{"apps", "deployments"},
		{"apps", "replicasets"},
		{"apps", "statefulsets"},
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

// send delivers signals to the pipeline. Never called under s.mu.
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

// Run implements sources.Source: starts the four informers, arms
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

	depH, err := factory.Apps().V1().Deployments().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.asDeployment(obj, s.onDeployment) },
		UpdateFunc: func(_, obj any) { s.asDeployment(obj, s.onDeployment) },
		DeleteFunc: func(obj any) { s.asDeployment(tombstoneObj(obj), s.onDeploymentDelete) },
	})
	if err != nil {
		return fmt.Errorf("rollout: register deployment handler: %w", err)
	}
	rsH, err := factory.Apps().V1().ReplicaSets().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.asReplicaSet(obj, s.onReplicaSet) },
		UpdateFunc: func(_, obj any) { s.asReplicaSet(obj, s.onReplicaSet) },
		DeleteFunc: func(obj any) { s.asReplicaSet(tombstoneObj(obj), s.onReplicaSetDelete) },
	})
	if err != nil {
		return fmt.Errorf("rollout: register replicaset handler: %w", err)
	}
	stsH, err := factory.Apps().V1().StatefulSets().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.asStatefulSet(obj, s.onStatefulSet) },
		UpdateFunc: func(_, obj any) { s.asStatefulSet(obj, s.onStatefulSet) },
		DeleteFunc: func(obj any) { s.asStatefulSet(tombstoneObj(obj), s.onStatefulSetDelete) },
	})
	if err != nil {
		return fmt.Errorf("rollout: register statefulset handler: %w", err)
	}
	podH, err := factory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.asPod(obj, s.onPod) },
		UpdateFunc: func(_, obj any) { s.asPod(obj, s.onPod) },
		DeleteFunc: func(obj any) { s.asPod(tombstoneObj(obj), s.onPodDelete) },
	})
	if err != nil {
		return fmt.Errorf("rollout: register pod handler: %w", err)
	}

	factory.Start(ctx.Done())
	// Shutdown blocks until every handler goroutine exits, upholding
	// the Source contract that emit is never called after Run returns.
	defer factory.Shutdown()

	if !cache.WaitForCacheSync(ctx.Done(), depH.HasSynced, rsH.HasSynced, stsH.HasSynced, podH.HasSynced) {
		return fmt.Errorf("rollout: cache sync failed (informer stopped before initial list completed)")
	}
	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()

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

func (s *Source) asDeployment(obj any, fn func(*appsv1.Deployment)) {
	if d, ok := obj.(*appsv1.Deployment); ok {
		fn(d)
	}
}

func (s *Source) asReplicaSet(obj any, fn func(*appsv1.ReplicaSet)) {
	if rs, ok := obj.(*appsv1.ReplicaSet); ok {
		fn(rs)
	}
}

func (s *Source) asStatefulSet(obj any, fn func(*appsv1.StatefulSet)) {
	if sts, ok := obj.(*appsv1.StatefulSet); ok {
		fn(sts)
	}
}

func (s *Source) asPod(obj any, fn func(*corev1.Pod)) {
	if p, ok := obj.(*corev1.Pod); ok {
		fn(p)
	}
}

// ---- informer handlers: record state, evaluate on workload/RS touches ----

func (s *Source) onDeployment(d *appsv1.Deployment) {
	now := s.clock()
	s.mu.Lock()
	s.deployments[d.UID] = &objEntry[*appsv1.Deployment]{obj: d, lastSeen: now}
	s.mu.Unlock()
	s.send(s.sweep(now))
}

func (s *Source) onDeploymentDelete(d *appsv1.Deployment) {
	s.mu.Lock()
	delete(s.deployments, d.UID)
	delete(s.tracks, d.UID)
	s.mu.Unlock()
}

func (s *Source) onReplicaSet(rs *appsv1.ReplicaSet) {
	now := s.clock()
	s.mu.Lock()
	s.replicasets[rs.UID] = &objEntry[*appsv1.ReplicaSet]{obj: rs, lastSeen: now}
	s.mu.Unlock()
	s.send(s.sweep(now))
}

func (s *Source) onReplicaSetDelete(rs *appsv1.ReplicaSet) {
	s.mu.Lock()
	delete(s.replicasets, rs.UID)
	s.mu.Unlock()
}

func (s *Source) onStatefulSet(sts *appsv1.StatefulSet) {
	now := s.clock()
	s.mu.Lock()
	s.statefulsets[sts.UID] = &objEntry[*appsv1.StatefulSet]{obj: sts, lastSeen: now}
	s.mu.Unlock()
	s.send(s.sweep(now))
}

func (s *Source) onStatefulSetDelete(sts *appsv1.StatefulSet) {
	s.mu.Lock()
	delete(s.statefulsets, sts.UID)
	delete(s.tracks, sts.UID)
	s.mu.Unlock()
}

func (s *Source) onPod(p *corev1.Pod) {
	now := s.clock()
	info := &podInfo{
		namespace:     p.Namespace,
		name:          p.Name,
		revisionHash:  p.Labels[revisionHashLabel],
		ready:         podReady(p),
		waitingReason: waitingReason(p),
		lastSeen:      now,
	}
	for _, ref := range p.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			info.ownerUID = ref.UID
			break
		}
	}
	s.mu.Lock()
	s.pods[p.UID] = info
	s.mu.Unlock()
	// No sweep here on purpose: pod churn is the high-frequency
	// stream, and a pod update never STARTS a stall by itself — the
	// window only elapses on the ticker (and workload/RS updates).
}

func (s *Source) onPodDelete(p *corev1.Pod) {
	s.mu.Lock()
	delete(s.pods, p.UID)
	s.mu.Unlock()
}

func podReady(p *corev1.Pod) bool {
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// waitingReason returns the first waiting-container reason (init
// containers included — a stuck init IS the rollout failing).
func waitingReason(p *corev1.Pod) string {
	for _, cs := range append(append([]corev1.ContainerStatus{}, p.Status.InitContainerStatuses...), p.Status.ContainerStatuses...) {
		if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
			return cs.State.Waiting.Reason
		}
	}
	return ""
}

// ---- sweep: the stall verdict ----

// sweep evaluates every workload's rollout state and prunes
// TTL-expired memory. Returns the signals to emit (the caller sends
// them outside the lock).
func (s *Source) sweep(now time.Time) []engine.Signal {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []engine.Signal
	for _, de := range s.deployments {
		if sig := s.evalDeployment(de.obj, now); sig != nil {
			out = append(out, *sig)
		}
	}
	for _, se := range s.statefulsets {
		if sig := s.evalStatefulSet(se.obj, now); sig != nil {
			out = append(out, *sig)
		}
	}
	// TTL prune (safety net behind DeleteFunc).
	cutoff := now.Add(-s.cfg.StateTTL)
	for uid, e := range s.deployments {
		if e.lastSeen.Before(cutoff) {
			delete(s.deployments, uid)
			delete(s.tracks, uid)
		}
	}
	for uid, e := range s.replicasets {
		if e.lastSeen.Before(cutoff) {
			delete(s.replicasets, uid)
		}
	}
	for uid, e := range s.statefulsets {
		if e.lastSeen.Before(cutoff) {
			delete(s.statefulsets, uid)
			delete(s.tracks, uid)
		}
	}
	for uid, p := range s.pods {
		if p.lastSeen.Before(cutoff) {
			delete(s.pods, uid)
		}
	}
	for uid, tr := range s.tracks {
		if tr.lastSeen.Before(cutoff) {
			delete(s.tracks, uid)
		}
	}
	return out
}

// markComplete records one completion observation: the FIRST sweep
// that sees the rollout complete after a not-complete period stamps
// completedAt (the clearance StableSince); repeated complete sweeps
// leave it alone so the §7.4 stability window keeps counting from
// the transition. Called under s.mu.
func (tr *track) markComplete(now time.Time) {
	if !tr.complete {
		tr.complete = true
		tr.completedAt = now
	}
	tr.revision = ""
}

// trackFor returns (creating if needed) the workload's progress
// memory. Called under s.mu.
func (s *Source) trackFor(uid types.UID, now time.Time) *track {
	tr, ok := s.tracks[uid]
	if !ok {
		tr = &track{fired: make(map[string]bool)}
		s.tracks[uid] = tr
	}
	tr.lastSeen = now
	return tr
}

// observeRevision updates a workload's progress memory for the
// current target revision and reports whether the observe window has
// elapsed with zero progress. Called under s.mu.
func (s *Source) observeRevision(tr *track, revision string, newReady int32, now time.Time) (stalled bool) {
	if tr.revision != revision {
		tr.revision = revision
		tr.since = now
		tr.maxNewReady = newReady
		tr.lastProgressAt = now
		return false
	}
	if newReady > tr.maxNewReady {
		// Progress: a new-revision pod became ready. The window
		// restarts — slow-but-progressing rollouts never fire.
		tr.maxNewReady = newReady
		tr.lastProgressAt = now
		return false
	}
	return now.Sub(tr.lastProgressAt) >= s.cfg.Observe
}

// int32OrOne dereferences a replicas pointer with the API's default.
func int32OrOne(p *int32) int32 {
	if p != nil {
		return *p
	}
	return 1
}

// deploymentComplete is the standard rollout-done predicate.
func deploymentComplete(d *appsv1.Deployment) bool {
	if d.Status.ObservedGeneration < d.Generation {
		return false
	}
	replicas := int32OrOne(d.Spec.Replicas)
	return d.Status.UpdatedReplicas == replicas &&
		d.Status.Replicas == replicas &&
		d.Status.AvailableReplicas == replicas
}

// stsComplete is the StatefulSet rollout-done predicate.
func stsComplete(sts *appsv1.StatefulSet) bool {
	if sts.Status.ObservedGeneration < sts.Generation {
		return false
	}
	if sts.Status.UpdateRevision != "" && sts.Status.UpdateRevision != sts.Status.CurrentRevision {
		return false
	}
	return sts.Status.ReadyReplicas == int32OrOne(sts.Spec.Replicas)
}

// rsRevision parses the deployment controller's revision annotation
// (-1 when absent, sorting unannotated RSes oldest).
func rsRevision(rs *appsv1.ReplicaSet) int64 {
	n, err := strconv.ParseInt(rs.Annotations[revisionAnnotation], 10, 64)
	if err != nil {
		return -1
	}
	return n
}

// evalDeployment applies the stall verdict to one Deployment. Called
// under s.mu; returns the signal to emit, if any.
func (s *Source) evalDeployment(d *appsv1.Deployment, now time.Time) *engine.Signal {
	// Owned ReplicaSets, newest (highest revision) vs old.
	var newest *appsv1.ReplicaSet
	var newestRev int64 = -1
	var owned []*appsv1.ReplicaSet
	for _, e := range s.replicasets {
		rs := e.obj
		if !ownedBy(rs.OwnerReferences, d.UID) {
			continue
		}
		owned = append(owned, rs)
		if rev := rsRevision(rs); rev > newestRev || (rev == newestRev && newest != nil && rs.Name > newest.Name) {
			newestRev, newest = rev, rs
		}
	}
	tr := s.trackFor(d.UID, now)
	if deploymentComplete(d) {
		tr.markComplete(now)
		return nil
	}
	tr.complete = false
	if d.Spec.Paused || newest == nil {
		tr.revision = ""
		return nil
	}
	newDesired := int32OrOne(newest.Spec.Replicas)
	if newDesired == 0 {
		tr.revision = ""
		return nil
	}
	// Old revisions still holding replicas: without one there is no
	// "old healthy" baseline (initial deploy) — that failure mode
	// belongs to objectstate.progress_deadline and k8s-events.
	var oldDesired, oldReady int32
	for _, rs := range owned {
		if rs.UID == newest.UID {
			continue
		}
		oldDesired += int32OrOne(rs.Spec.Replicas)
		oldReady += rs.Status.ReadyReplicas
	}
	if oldDesired == 0 {
		tr.revision = ""
		return nil
	}
	newReady := newest.Status.ReadyReplicas
	stalled := s.observeRevision(tr, newest.Name, newReady, now)
	if newReady >= newDesired {
		return nil // new revision fully ready; completion is imminent
	}
	if !s.armed || !stalled || tr.fired[newest.Name] {
		return nil
	}
	if float64(oldReady) < s.cfg.OldReadyRatio*float64(oldDesired) {
		return nil // old revision unhealthy too — not a bad-deploy shape
	}
	tr.fired[newest.Name] = true
	waiting := s.topWaitingReason(func(p *podInfo) bool { return p.ownerUID == newest.UID })
	sig := s.newStall("Deployment", d.Namespace, d.Name, string(d.UID),
		fmt.Sprintf("new ReplicaSet %s", newest.Name),
		newReady, newDesired, oldReady, oldDesired, now.Sub(tr.since), waiting, now)
	return &sig
}

// evalStatefulSet applies the stall verdict to one StatefulSet.
// Called under s.mu; returns the signal to emit, if any.
func (s *Source) evalStatefulSet(sts *appsv1.StatefulSet, now time.Time) *engine.Signal {
	tr := s.trackFor(sts.UID, now)
	if stsComplete(sts) {
		tr.markComplete(now)
		return nil
	}
	tr.complete = false
	update, current := sts.Status.UpdateRevision, sts.Status.CurrentRevision
	if update == "" || update == current {
		tr.revision = ""
		return nil // not a revision rollout (scale-up, initial create)
	}
	replicas := int32OrOne(sts.Spec.Replicas)
	newDesired := replicas
	if ru := sts.Spec.UpdateStrategy.RollingUpdate; ru != nil && ru.Partition != nil {
		newDesired = replicas - *ru.Partition
	}
	if newDesired <= 0 {
		tr.revision = ""
		return nil
	}
	// Count new/old revision pods from the pod mirror (the STS
	// controller labels each pod with its ControllerRevision hash).
	var newReady, oldReady, oldCount int32
	for _, p := range s.pods {
		if p.ownerUID != sts.UID {
			continue
		}
		switch p.revisionHash {
		case update:
			if p.ready {
				newReady++
			}
		case current:
			oldCount++
			if p.ready {
				oldReady++
			}
		}
	}
	stalled := s.observeRevision(tr, update, newReady, now)
	if newReady >= newDesired {
		return nil
	}
	if !s.armed || !stalled || tr.fired[update] {
		return nil
	}
	// The old-healthy gate judges the pods still on the current
	// revision (an STS replaces one ordinal at a time). No old pods
	// left → no baseline → not this source's verdict.
	if oldCount == 0 || float64(oldReady) < s.cfg.OldReadyRatio*float64(oldCount) {
		return nil
	}
	tr.fired[update] = true
	waiting := s.topWaitingReason(func(p *podInfo) bool { return p.ownerUID == sts.UID && p.revisionHash == update })
	sig := s.newStall("StatefulSet", sts.Namespace, sts.Name, string(sts.UID),
		fmt.Sprintf("update revision %s", update),
		newReady, newDesired, oldReady, oldCount, now.Sub(tr.since), waiting, now)
	return &sig
}

// ownedBy reports whether refs contain a controller reference to uid.
func ownedBy(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.Controller != nil && *ref.Controller && ref.UID == uid {
			return true
		}
	}
	return false
}

// topWaitingReason tallies waiting reasons across the pods matching
// sel and returns the most frequent (ties broken lexicographically
// for determinism). Called under s.mu.
func (s *Source) topWaitingReason(sel func(*podInfo) bool) string {
	counts := make(map[string]int)
	for _, p := range s.pods {
		if sel(p) && p.waitingReason != "" {
			counts[p.waitingReason]++
		}
	}
	if len(counts) == 0 {
		return ""
	}
	reasons := make([]string, 0, len(counts))
	for r := range counts {
		reasons = append(reasons, r)
	}
	sort.Slice(reasons, func(i, j int) bool {
		if counts[reasons[i]] != counts[reasons[j]] {
			return counts[reasons[i]] > counts[reasons[j]]
		}
		return reasons[i] < reasons[j]
	})
	return reasons[0]
}

// newStall composes the rollout.stall Signal with the evidence
// fields. Deployment identity and fingerprint stay empty for the
// pipeline to stamp (§7.2).
func (s *Source) newStall(objKind, namespace, name, uid, revDesc string, newReady, newDesired, oldReady, oldDesired int32, elapsed time.Duration, waiting string, ts time.Time) engine.Signal {
	msg := fmt.Sprintf(
		"rollout stalled: %s new_ready=%d/%d old_ready=%d/%d elapsed=%s",
		revDesc, newReady, newDesired, oldReady, oldDesired, elapsed.Truncate(time.Second))
	if waiting != "" {
		msg += fmt.Sprintf(" top_waiting_reason=%s", waiting)
	}
	msg += " — new-revision pods failing while the old revision stays healthy (probable bad deploy, fired ahead of progressDeadlineSeconds)"
	return engine.Signal{
		Kind:     KindStall,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityWarning,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: uid, Reason: ReasonStall},
			Namespace:    namespace,
			KindOfObject: objKind,
			Name:         name,
			Message:      msg,
			FirstSeen:    ts,
			LastSeen:     ts,
			Count:        1,
		},
	}
}

// ---- §7.4 clearance ----

// Clearance implements engine.ClearanceObserver for rollout_stall
// incidents: cleared when the rollout COMPLETED (a fixed revision
// rolled out, or a rollback re-promoted the old, already-ready
// revision — both end in completion) — resolution recovered; or when
// the workload itself is gone — resolution object_deleted. ok=false
// for incidents that are not rollout stalls, or before the informer
// caches synced (cannot judge against an empty mirror).
//
// StableSince is the sweep that OBSERVED the completion transition
// (track.markComplete), never an earlier completion from before the
// stall: the recovery tracker counts the §7.4 stability window from
// it, so a rollback debounces --recovery-stable-for like every other
// observer's clearance, and the resolved record's cleared_after /
// observed_stable_for split at clearance time, not fire time.
func (s *Source) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	if engine.CanonicalReason(inc.Key.Reason) != ReasonStall {
		return engine.Clearance{}, false
	}
	kind := inc.Ref.KindOfObject
	if !strings.EqualFold(kind, "Deployment") && !strings.EqualFold(kind, "StatefulSet") {
		return engine.Clearance{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.armed {
		return engine.Clearance{}, false
	}
	uid := types.UID(inc.Key.UID)
	var complete, exists bool
	if strings.EqualFold(kind, "Deployment") {
		if e, ok := s.deployments[uid]; ok {
			exists, complete = true, deploymentComplete(e.obj)
		}
	} else {
		if e, ok := s.statefulsets[uid]; ok {
			exists, complete = true, stsComplete(e.obj)
		}
	}
	if !exists {
		return engine.Clearance{Cleared: true, Resolution: engine.ResolutionObjectDeleted}, true
	}
	var stableSince time.Time
	if tr, ok := s.tracks[uid]; ok {
		stableSince = tr.completedAt
	}
	return engine.Clearance{
		Cleared:     complete,
		StableSince: stableSince,
		Resolution:  engine.ResolutionRecovered,
	}, true
}
