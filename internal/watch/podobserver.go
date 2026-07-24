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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources/objectstate"
)

// podClearanceObserver is the STANDALONE fallback for the §7.4 pod
// clearance predicate: a minimal pod informer feeding the shared
// objectstate.PodClearance state machine.
//
// The object-state source (pkg/sources/objectstate) absorbed the
// clearance logic — when `--sources` enables it, its pod informer
// feeds the same state machine and setupRecovery wires the tracker to
// the source's ClearanceObserver instead of constructing this type.
// This wrapper exists so the DEFAULT deployment (--sources=k8s-events,
// the frozen M0 surface) keeps recovery injects working with exactly
// the M2 PR #31 behavior and RBAC posture (pods list/watch only,
// disabled loudly when missing). It emits no signals and opens no
// incidents.
type podClearanceObserver struct {
	client kubernetes.Interface
	// state is the shared clearance state machine; the behavior
	// contract lives there and is pinned by this package's tests.
	state *objectstate.PodClearance
}

func newPodClearanceObserver(client kubernetes.Interface) *podClearanceObserver {
	return &podClearanceObserver{client: client, state: objectstate.NewPodClearance()}
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
				o.state.Upsert(pod)
			}
		},
		UpdateFunc: func(_, newObj any) {
			if pod, ok := newObj.(*corev1.Pod); ok {
				o.state.Upsert(pod)
			}
		},
		DeleteFunc: func(obj any) {
			if ts, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = ts.Obj
			}
			if pod, ok := obj.(*corev1.Pod); ok {
				o.state.Delete(pod)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("recovery pod observer: register handler: %w", err)
	}
	o.state.SetSynced(handler.HasSynced)
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), handler.HasSynced) {
		return fmt.Errorf("recovery pod observer: cache sync failed (informer stopped before initial list completed)")
	}
	return nil
}

// Clearance implements engine.ClearanceObserver by delegating to the
// shared state machine.
func (o *podClearanceObserver) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	return o.state.Clearance(inc)
}
