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
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// podClearanceObserver is the k8s-events world's clearance side of
// §7.4: for pod-scoped incidents (CrashLoopBackOff, ImagePullBackOff,
// Unhealthy, FailedMount, …) it answers "is the symptom absent?" via
// a pod informer:
//
//   - live pod: cleared when the pod is Ready; the vouched-stable-
//     since instant is the later of the Ready transition and the
//     newest container start, so a container restart between tracker
//     ticks restarts the stability window even if the pod reads Ready
//     at every tick (restart-count stability, §7.4).
//   - deleted pod, controller has a Ready replacement: cleared with
//     resolution=recovered (owner-based — the incident is about the
//     workload, and the workload is healthy again).
//   - deleted pod, controller gone too (no pods left under the
//     owner): cleared with resolution=object_deleted — the incident
//     closes, explicitly distinguishable from a fix (§9.3).
//
// This is deliberately NOT the §7.2 object-state source: it emits no
// signals, opens no incidents, and keeps only the minimal pod state
// the clearance predicate needs. When the object-state source lands
// (next M2 change) its pod informer can absorb this observer.
type podClearanceObserver struct {
	client kubernetes.Interface
	synced cache.InformerSynced

	mu sync.Mutex
	// pods mirrors the recovery-relevant slice of live pod status,
	// keyed by UID (the incident key).
	pods map[types.UID]*podState
	// byOwner indexes live pods by controller owner so a deleted
	// pod's incident can be judged against its replacement.
	byOwner map[ownerKey]map[types.UID]struct{}
	// byName indexes live pods by namespace/name — the fallback for
	// judging a restored incident whose pod was replaced under the
	// same name (StatefulSet) while the sentinel was down.
	byName map[nsName]types.UID
	// tombstones remembers recently deleted pods (owner, deletion
	// time) so gone-pod incidents can be judged owner-based. TTL-
	// swept; see tombstoneTTL.
	tombstones map[types.UID]*podTombstone

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
}

// ownerKey identifies a pod's controller within a namespace.
type ownerKey struct {
	namespace string
	kind      string
	name      string
}

type nsName struct {
	namespace string
	name      string
}

// podState is the minimal live-pod status the predicate needs.
type podState struct {
	namespace string
	name      string
	owner     *ownerKey // nil for bare pods
	ready     bool
	// stableSince is the later of the PodReady transition and the
	// newest container start (or the observer-clocked instant the
	// restart count last changed). Meaningful when ready.
	stableSince time.Time
	// restarts is the summed container restart count, kept to detect
	// bumps between informer updates.
	restarts int32
}

type podTombstone struct {
	namespace string
	name      string
	owner     *ownerKey
	deletedAt time.Time
}

// tombstoneTTL bounds the tombstone map: an incident whose resolution
// takes longer than this after the pod vanished falls back to the
// no-tombstone path (owner unknown), which still closes it. Generous
// relative to any sane --recovery-stable-for.
const tombstoneTTL = 2 * time.Hour

func newPodClearanceObserver(client kubernetes.Interface) *podClearanceObserver {
	return &podClearanceObserver{
		client:     client,
		pods:       make(map[types.UID]*podState),
		byOwner:    make(map[ownerKey]map[types.UID]struct{}),
		byName:     make(map[nsName]types.UID),
		tombstones: make(map[types.UID]*podTombstone),
	}
}

func (o *podClearanceObserver) clock() time.Time {
	if o.now != nil {
		return o.now()
	}
	return time.Now()
}

// Start launches the pod informer and blocks until its initial list
// completes (so the first tracker tick judges against real state, not
// an empty cache). The informer stops when ctx is cancelled.
func (o *podClearanceObserver) Start(ctx context.Context) error {
	factory := informers.NewSharedInformerFactory(o.client, 0)
	informer := factory.Core().V1().Pods().Informer()
	handler, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if pod, ok := obj.(*corev1.Pod); ok {
				o.upsert(pod)
			}
		},
		UpdateFunc: func(_, newObj any) {
			if pod, ok := newObj.(*corev1.Pod); ok {
				o.upsert(pod)
			}
		},
		DeleteFunc: func(obj any) {
			if ts, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = ts.Obj
			}
			if pod, ok := obj.(*corev1.Pod); ok {
				o.delete(pod)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("recovery pod observer: register handler: %w", err)
	}
	o.synced = handler.HasSynced
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), handler.HasSynced) {
		return fmt.Errorf("recovery pod observer: cache sync failed (informer stopped before initial list completed)")
	}
	return nil
}

// upsert records/refreshes a live pod's recovery-relevant state.
func (o *podClearanceObserver) upsert(pod *corev1.Pod) {
	now := o.clock()
	uid := pod.UID
	ready, readySince := podReadiness(pod)
	restarts, newestStart := containerRestarts(pod)

	o.mu.Lock()
	defer o.mu.Unlock()

	stableSince := readySince
	if newestStart.After(stableSince) {
		stableSince = newestStart
	}
	if prev, ok := o.pods[uid]; ok {
		// Restart bump between updates with no fresher container
		// start visible (e.g. status lag): stamp observer-now so the
		// stability window restarts regardless.
		if restarts != prev.restarts && !stableSince.After(prev.stableSince) {
			stableSince = now
		}
		o.removeOwnerIndex(prev, uid)
	}
	owner := controllerOwner(pod)
	o.pods[uid] = &podState{
		namespace:   pod.Namespace,
		name:        pod.Name,
		owner:       owner,
		ready:       ready,
		stableSince: stableSince,
		restarts:    restarts,
	}
	if owner != nil {
		set, ok := o.byOwner[*owner]
		if !ok {
			set = make(map[types.UID]struct{})
			o.byOwner[*owner] = set
		}
		set[uid] = struct{}{}
	}
	o.byName[nsName{pod.Namespace, pod.Name}] = uid
	delete(o.tombstones, uid) // resurrection (informer replay) beats tombstone
}

// delete moves a pod to the tombstone map and prunes expired
// tombstones (cheap TTL sweep on the rare delete path).
func (o *podClearanceObserver) delete(pod *corev1.Pod) {
	now := o.clock()
	o.mu.Lock()
	defer o.mu.Unlock()
	uid := pod.UID
	if prev, ok := o.pods[uid]; ok {
		o.removeOwnerIndex(prev, uid)
		if o.byName[nsName{prev.namespace, prev.name}] == uid {
			delete(o.byName, nsName{prev.namespace, prev.name})
		}
		delete(o.pods, uid)
	}
	o.tombstones[uid] = &podTombstone{
		namespace: pod.Namespace,
		name:      pod.Name,
		owner:     controllerOwner(pod),
		deletedAt: now,
	}
	for tsUID, ts := range o.tombstones {
		if now.Sub(ts.deletedAt) > tombstoneTTL {
			delete(o.tombstones, tsUID)
		}
	}
}

// removeOwnerIndex is called under lock.
func (o *podClearanceObserver) removeOwnerIndex(ps *podState, uid types.UID) {
	if ps.owner == nil {
		return
	}
	if set, ok := o.byOwner[*ps.owner]; ok {
		delete(set, uid)
		if len(set) == 0 {
			delete(o.byOwner, *ps.owner)
		}
	}
}

// Clearance implements engine.ClearanceObserver for pod-scoped
// incidents. ok=false when the incident isn't pod-scoped or the
// informer hasn't synced yet (can't judge — the tracker keeps
// waiting).
func (o *podClearanceObserver) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	if !strings.EqualFold(inc.Ref.KindOfObject, "Pod") {
		return engine.Clearance{}, false
	}
	if o.synced == nil || !o.synced() {
		return engine.Clearance{}, false
	}
	uid := types.UID(inc.Key.UID)

	o.mu.Lock()
	defer o.mu.Unlock()

	// Live pod: Ready == symptom absent; stableSince carries the
	// restart-stability vouching.
	if ps, ok := o.pods[uid]; ok {
		return engine.Clearance{
			Cleared:     ps.ready,
			StableSince: ps.stableSince,
			Resolution:  engine.ResolutionRecovered,
		}, true
	}

	// Pod gone. Resolve the owner: tombstone first, then the
	// persisted ControllerRef ("Kind/name") for incidents restored
	// across a sentinel restart.
	var owner *ownerKey
	deletedAt := time.Time{}
	if ts, ok := o.tombstones[uid]; ok {
		owner = ts.owner
		deletedAt = ts.deletedAt
	} else if k, n, ok := splitControllerRef(inc.Ref.ControllerRef); ok {
		owner = &ownerKey{namespace: inc.Ref.Namespace, kind: k, name: n}
	}

	if owner != nil {
		if set, ok := o.byOwner[*owner]; ok && len(set) > 0 {
			// Replacement pods exist under the same controller: the
			// incident clears only if one is Ready — a crash-looping
			// replacement IS the symptom persisting.
			best := time.Time{}
			anyReady := false
			for sibUID := range set {
				sib := o.pods[sibUID]
				if sib == nil || !sib.ready {
					continue
				}
				if !anyReady || sib.stableSince.Before(best) {
					best = sib.stableSince // oldest-stable ready sibling vouches longest
					anyReady = true
				}
			}
			return engine.Clearance{
				Cleared:     anyReady,
				StableSince: best,
				Resolution:  engine.ResolutionRecovered,
			}, true
		}
		// Owner has no pods at all: either the controller is gone or
		// a replacement hasn't been scheduled yet. Report cleared as
		// object_deleted and let the tracker's stability window
		// arbitrate — a slow replacement appearing mid-window flips
		// the verdict (crashing replacement → symptomatic; Ready
		// replacement → recovered) before anything is emitted.
		return engine.Clearance{
			Cleared:     true,
			StableSince: deletedAt,
			Resolution:  engine.ResolutionObjectDeleted,
		}, true
	}

	// No owner known. A same-name replacement (StatefulSet pods keep
	// their name across recreation) counts as the workload.
	if repUID, ok := o.byName[nsName{inc.Ref.Namespace, inc.Ref.Name}]; ok {
		if rep := o.pods[repUID]; rep != nil {
			return engine.Clearance{
				Cleared:     rep.ready,
				StableSince: rep.stableSince,
				Resolution:  engine.ResolutionRecovered,
			}, true
		}
	}

	// Bare pod (or unknowable owner) gone with no replacement:
	// closed as object_deleted after the window.
	return engine.Clearance{
		Cleared:     true,
		StableSince: deletedAt,
		Resolution:  engine.ResolutionObjectDeleted,
	}, true
}

// podReadiness returns whether the PodReady condition is True and its
// last transition time.
func podReadiness(pod *corev1.Pod) (bool, time.Time) {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue, cond.LastTransitionTime.Time
		}
	}
	return false, time.Time{}
}

// containerRestarts sums container restart counts and returns the
// newest running-container start time — the "when did this pod last
// restart" proxy available from status alone.
func containerRestarts(pod *corev1.Pod) (int32, time.Time) {
	var total int32
	var newest time.Time
	for _, cs := range pod.Status.ContainerStatuses {
		total += cs.RestartCount
		if cs.State.Running != nil && cs.State.Running.StartedAt.After(newest) {
			newest = cs.State.Running.StartedAt.Time
		}
	}
	return total, newest
}

// controllerOwner extracts the controlling owner reference, nil for
// bare pods.
func controllerOwner(pod *corev1.Pod) *ownerKey {
	for _, ref := range pod.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			return &ownerKey{namespace: pod.Namespace, kind: ref.Kind, name: ref.Name}
		}
	}
	return nil
}

// splitControllerRef parses the payload-shaped "Kind/name" controller
// ref (e.g. "ReplicaSet/checkout-svc-7b9d").
func splitControllerRef(s string) (kind, name string, ok bool) {
	i := strings.IndexByte(s, '/')
	if i <= 0 || i == len(s)-1 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}
