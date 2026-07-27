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

// Package k8sevents is the first signal source (DESIGN.md §7.2): the
// core/v1 Event informer that was internal/watch's watcher — the M0
// k8s-event-watcher — refactored behind the pkg/sources.Source
// interface with semantics unchanged. It emits kind=k8s-event Signals
// per the existing Reason allow-list; filter/dedup/inject stay in the
// shared pipeline downstream.
package k8sevents

import (
	"context"
	"fmt"
	"log"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the stable source name (§7.2 table) used in the signal
// schema and config.
const Name = "k8s-events"

// Source wires a client-go informer for core/v1.Events into the
// sentinel pipeline. On Add (new event object) and Update (event
// count bump), the handler converts the *corev1.Event to a Signal and
// hands it to emit.
//
// The pipeline decides whether to filter/dedup/inject — the source
// itself is just the informer boilerplate plus the Event → Signal
// conversion.
type Source struct {
	client       kubernetes.Interface
	resyncPeriod time.Duration
}

// New constructs the source. resyncPeriod == 0 disables the periodic
// resync (informer only fires on real API events); non-zero values
// re-fire every registered event through the handler at that cadence
// — usually not what you want, so callers pass 0.
func New(client kubernetes.Interface, resyncPeriod time.Duration) *Source {
	return &Source{client: client, resyncPeriod: resyncPeriod}
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source. The Event informer lists and
// watches events across all namespaces (namespace filtering happens
// downstream in the engine filter), so the source needs cluster-wide
// RBAC.
func (s *Source) Scope() sources.Scope { return sources.ScopeCluster }

// RequiredAccess implements sources.AccessDeclarer (§11): the
// informer's initial List plus the Watch it maintains. Matches the
// shipped ClusterRole in deploy/12-clusterrole-watcher.yaml.
func (s *Source) RequiredAccess() []sources.Requirement {
	return []sources.Requirement{
		{Resource: "events", Verb: "list"},
		{Resource: "events", Verb: "watch"},
	}
}

// Run implements sources.Source: starts the informer + handler
// goroutines and blocks until ctx is cancelled. Returns any startup
// error (e.g., initial list failure); shutdown-path errors are logged
// but not returned so callers can distinguish "startup failed,
// restart me" from "clean shutdown."
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	factory := informers.NewSharedInformerFactory(s.client, s.resyncPeriod)
	eventInformer := factory.Core().V1().Events().Informer()

	handler, err := eventInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			ev, ok := obj.(*corev1.Event)
			if !ok {
				log.Printf("k8s-events: unexpected object type on Add: %T", obj)
				return
			}
			emit(toSignal(ev))
		},
		UpdateFunc: func(_, newObj any) {
			// Update fires when the k8s API bumps the Event's
			// Count / LastTimestamp (kubelet reports a repeat).
			// We treat each update as another observation so
			// persistent failures continue to feed the dedup
			// window's LastSeen bump.
			ev, ok := newObj.(*corev1.Event)
			if !ok {
				log.Printf("k8s-events: unexpected object type on Update: %T", newObj)
				return
			}
			emit(toSignal(ev))
		},
		// No DeleteFunc — event deletion is not a signal we care
		// about; the underlying incident may or may not be
		// resolved and we don't want to trigger investigations
		// on tombstones.
	})
	if err != nil {
		return fmt.Errorf("k8s-events: register event handler: %w", err)
	}
	// Route client-go internal errors ("unknown object type in
	// cache" on shutdown ctx.Done races, reflector list failures)
	// through our logger. APPEND, never replace: runtime.ErrorHandlers
	// is package-global process state shared by every client-go
	// consumer in this binary — replacing the slice silently discarded
	// the default handlers (and any handler another source installed),
	// a real bug once two informer-backed sources run in one process.
	runtime.ErrorHandlers = append(runtime.ErrorHandlers,
		func(_ context.Context, err error, _ string, _ ...any) {
			log.Printf("k8s-events: informer error: %v", err)
		},
	)

	factory.Start(ctx.Done())
	// WaitForCacheSync blocks until the initial list is done —
	// without this, the first N events after startup would
	// arrive without their prior Count/LastTimestamp, breaking
	// the dedup logic.
	if !cache.WaitForCacheSync(ctx.Done(), handler.HasSynced) {
		return fmt.Errorf("k8s-events: cache sync failed (informer stopped before initial list completed)")
	}
	<-ctx.Done()
	return nil
}

// toSignal converts a *corev1.Event to the pipeline Signal: the
// frozen kind=k8s-event with the TriageEvent core populated from the
// event. Severity is critical — the §7.7 default for k8s-event, i.e.
// today's per-incident routing. Cluster/Zone/Fingerprint are left
// empty for the pipeline to stamp (the source doesn't know the
// deployment's identity).
func toSignal(ev *corev1.Event) engine.Signal {
	return engine.Signal{
		Kind:        engine.KindK8sEvent,
		Source:      engine.SourceSentinel,
		Severity:    engine.SeverityCritical,
		TriageEvent: toTriageEvent(ev),
	}
}

// toTriageEvent flattens a *corev1.Event to the internal payload
// shape. Timestamps prefer LastTimestamp (kubelet-set); fall back
// to EventTime / CreationTimestamp per k8s API convention.
func toTriageEvent(ev *corev1.Event) engine.TriageEvent {
	first := ev.FirstTimestamp.Time
	if first.IsZero() {
		first = ev.EventTime.Time
	}
	if first.IsZero() {
		first = ev.CreationTimestamp.Time
	}
	last := ev.LastTimestamp.Time
	if last.IsZero() {
		last = ev.EventTime.Time
	}
	if last.IsZero() {
		last = ev.CreationTimestamp.Time
	}

	// The event references its target via InvolvedObject.
	// InvolvedObject.UID is what we key dedup on.
	uid := string(ev.InvolvedObject.UID)

	// ControllerRef: for a Pod, the parent ReplicaSet /
	// Deployment / StatefulSet is on OwnerReferences. Populating
	// this requires an additional Pod GET which we don't have
	// in-hand here. Left empty; the recipe includes RBAC for
	// pod GET so the agent can enrich via MCP if needed.
	controllerRef := ""

	return engine.TriageEvent{
		Key: engine.EventKey{
			UID:    uid,
			Reason: ev.Reason,
		},
		Namespace:     ev.InvolvedObject.Namespace,
		KindOfObject:  ev.InvolvedObject.Kind,
		Name:          ev.InvolvedObject.Name,
		Container:     ev.InvolvedObject.FieldPath,
		Message:       truncateMessage(ev.Message),
		FirstSeen:     first,
		LastSeen:      last,
		ControllerRef: controllerRef,
		Node:          nodeFromSource(ev),
		Labels:        labelsFromMeta(ev.ObjectMeta),
		Count:         int(ev.Count),
		Type:          ev.Type,
	}
}

// truncateMessage caps the payload's message field. K8s event
// messages are supposed to be small but we've seen kubelet emit
// multi-KB stack traces; playbook skills don't need more than a
// few hundred bytes to categorize.
//
// The truncation marker still says "k8s-event-watcher": it appears
// inside message text that shipped payloads carry, so it stays
// byte-identical through the source refactor.
func truncateMessage(msg string) string {
	const max = 2048
	if len(msg) <= max {
		return msg
	}
	return msg[:max] + "... [truncated by k8s-event-watcher]"
}

// nodeFromSource pulls the node name out of an Event's Source or
// ReportingController fields, whichever the API server populated.
func nodeFromSource(ev *corev1.Event) string {
	if ev.Source.Host != "" {
		return ev.Source.Host
	}
	if ev.ReportingInstance != "" {
		return ev.ReportingInstance
	}
	return ""
}

// labelsFromMeta returns a shallow copy of the event's own labels
// (not the involved object's — that would require an extra API
// call). Empty when no labels are set.
func labelsFromMeta(m metav1.ObjectMeta) map[string]string {
	if len(m.Labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(m.Labels))
	for k, v := range m.Labels {
		out[k] = v
	}
	return out
}
