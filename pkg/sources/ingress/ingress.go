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

// Package ingress is the GCLB/Ingress programming-failure signal
// source (post-M5 roadmap C.5, issue #135 half 1): the ingress-gce
// controller's failure events — Sync errors, Translate errors, and
// NEG attach/sync failures. When GCLB programming fails, the Ingress
// object itself keeps looking fine (spec accepted, old VIP still in
// status); these events are the only in-cluster evidence that the
// load balancer behind it is not being programmed.
//
// The source OWNS these reasons with its own Event informer filter —
// the exact precedent of the capacity source owning the
// cluster-autoscaler reasons (see pkg/sources/capacity's package
// comment): the k8s-events source's default allow-list is
// deliberately unchanged (its `--reason` surface is frozen deployment
// config, and ingress-gce reasons only mean something when an
// operator opted into ingress watching), so enabling `ingress` — not
// editing `--reason` — is what turns them on. Beyond that standing
// argument, a default allow-list entry is DISQUALIFIED here by a
// reason collision: ingress-gce uses reason `Sync` for BOTH Normal
// housekeeping events ("Scheduled for sync", "ForwardingRule …
// deleted") and Warning failures ("Error syncing to GCP: …"), and the
// k8s-events reactive path carries no event type — allow-listing
// `Sync` would inject an incident on every routine sync. This source
// gates on Type == Warning, which only a source-owned filter can do.
//
// Three kinds (§7.3, APPEND-ONLY), all WARNING — the watchboard
// posture: programming failures repeat on every controller requeue
// while broken (the digest batches them), and a transient
// AttachFailed during a rollout is normal endpoint churn; false
// criticals erode trust:
//
//   - ingress.sync_failed — reason `Sync` (Warning) on an Ingress:
//     "Error syncing to GCP: …" from the ingress-gce sync loop.
//   - ingress.translate_failed — reason `Translate` (Warning) on an
//     Ingress: "Translation failed: …" — the spec cannot even be
//     turned into GCLB resources (bad backend ref, invalid config).
//   - ingress.neg_failed — the NEG controller failure family
//     (SyncNetworkEndpointGroupFailed, AttachFailed, DetachFailed,
//     RetryFailed; Warning) on a Service: endpoints are not reaching
//     the load balancer even though the Ingress looks synced.
//
// Reasons per upstream ingress-gce (constants in its
// pkg/events/events.go; emission in pkg/controller, pkg/loadbalancers,
// and the NEG controllers). `GarbageCollection` failures are
// deliberately out of scope — issue #135 names Sync/Translate/NEG.
//
// Arming: the Event informer arms only after its cache syncs — the
// initial LIST replays up to an hour (the event TTL) of stale events,
// and a pre-existing failure is history, not observation. Nothing is
// lost by not replaying: ingress-gce requeues failing objects, so a
// still-broken Ingress re-fires on its next sync attempt.
//
// No §7.4 clearance observer: like the capacity source's event kinds,
// these are event-reactive incidents that resolve through the normal
// paths.
package ingress

import (
	"context"
	"fmt"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the stable source name (§7.2 table) used in the signal
// schema and the `--sources` flag.
const Name = "ingress"

// kindPrefix namespaces this source's signal kinds (§7.3).
const kindPrefix = "ingress."

// Signal kinds emitted by this source. APPEND-ONLY: kinds are part of
// the signal schema playbooks and fleet consumers match on — never
// rename or reuse one. The dedup/fingerprint reason for each is the
// kind suffix.
const (
	// KindSyncFailed: a Warning `Sync` event on an Ingress — the
	// ingress-gce sync loop failed to program GCP ("Error syncing to
	// GCP: …"). Warning: repeats every requeue while broken.
	KindSyncFailed = kindPrefix + "sync_failed"
	// KindTranslateFailed: a Warning `Translate` event on an Ingress —
	// the spec could not be translated into GCLB resources
	// ("Translation failed: …"), typically a config error the operator
	// can fix.
	KindTranslateFailed = kindPrefix + "translate_failed"
	// KindNEGFailed: a Warning NEG-controller failure on a Service
	// (SyncNetworkEndpointGroupFailed, AttachFailed, DetachFailed,
	// RetryFailed) — endpoints are not reaching the load balancer.
	KindNEGFailed = kindPrefix + "neg_failed"
)

// eventKinds maps involved-object kind → ingress-gce Warning reason →
// signal kind. The reason table is OWNED by this source (see the
// package comment for why the k8s-events default allow-list is
// deliberately unchanged, and why `Sync` in particular can never be
// allow-listed there). Reasons per upstream ingress-gce
// pkg/events/events.go: Sync/Translate land on the Ingress; the NEG
// failure family lands on the Service whose endpoints feed the NEG.
var eventKinds = map[string]map[string]string{
	"Ingress": {
		"Sync":      KindSyncFailed,
		"Translate": KindTranslateFailed,
	},
	"Service": {
		"SyncNetworkEndpointGroupFailed": KindNEGFailed,
		"AttachFailed":                   KindNEGFailed,
		"DetachFailed":                   KindNEGFailed,
		"RetryFailed":                    KindNEGFailed,
	},
}

// Source implements sources.Source for the ingress row of §7.2.
type Source struct {
	client kubernetes.Interface

	mu   sync.Mutex
	emit func(engine.Signal)
	// armed flips true after the informer cache syncs: the handler
	// records nothing and emits nothing before that (the initial LIST
	// is history, not observation — see the package comment).
	armed bool
}

// New constructs the source. The source is pure client-go — no cloud
// provider: ingress-gce writes ordinary Kubernetes Events, so a
// vanilla events grant is all it reads.
func New(client kubernetes.Interface) *Source {
	return &Source{client: client}
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: the Event informer watches
// cluster-wide (an Ingress can live in any namespace), so the source
// needs cluster RBAC (§11).
func (s *Source) Scope() sources.Scope { return sources.ScopeCluster }

// RequiredAccess implements sources.AccessDeclarer (§11): the Event
// informer's list+watch — the same grant the k8s-events source rides
// (deploy/12-clusterrole-watcher.yaml), no new rule.
func (s *Source) RequiredAccess() []sources.Requirement {
	return []sources.Requirement{
		{Resource: "events", Verb: "list"},
		{Resource: "events", Verb: "watch"},
	}
}

// arm enables event emission — called once the informer cache syncs.
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

// send hands a signal to the pipeline under the source lock's
// protection of s.emit (Run sets it before the informer starts).
func (s *Source) send(sig engine.Signal) {
	s.mu.Lock()
	emit := s.emit
	s.mu.Unlock()
	if emit != nil {
		emit(sig)
	}
}

// onEvent is the Event informer handler. Gating, in order: armed (the
// initial LIST replays up to an hour of stale events — drop them),
// Type == Warning (the load-bearing gate: ingress-gce reuses reason
// `Sync` for Normal housekeeping, see the package comment), then the
// involved-object kind + reason table.
func (s *Source) onEvent(ev *corev1.Event) {
	s.mu.Lock()
	armed := s.armed
	s.mu.Unlock()
	if !armed {
		return
	}
	if ev.Type != corev1.EventTypeWarning {
		return
	}
	kind, ok := eventKinds[ev.InvolvedObject.Kind][ev.Reason]
	if !ok {
		return
	}
	s.send(eventSignal(kind, ev))
}

// eventSignal converts a gated ingress-gce event to its Signal. The
// dedup/fingerprint reason is the kind suffix (so all four NEG
// reasons collapse into one neg_failed incident per Service); the raw
// event reason stays visible in the message.
func eventSignal(kind string, ev *corev1.Event) engine.Signal {
	msg := ev.Message
	if ev.Reason != "" {
		msg = ev.Reason + ": " + ev.Message
	}
	first := ev.FirstTimestamp.Time
	if first.IsZero() {
		first = ev.EventTime.Time
	}
	last := ev.LastTimestamp.Time
	if last.IsZero() {
		last = first
	}
	return engine.Signal{
		Kind:     kind,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityWarning,
		TriageEvent: engine.TriageEvent{
			Key: engine.EventKey{
				UID:    string(ev.InvolvedObject.UID),
				Reason: strings.TrimPrefix(kind, kindPrefix),
			},
			Namespace:    ev.InvolvedObject.Namespace,
			KindOfObject: ev.InvolvedObject.Kind,
			Name:         ev.InvolvedObject.Name,
			Message:      truncate(msg),
			FirstSeen:    first,
			LastSeen:     last,
			Count:        max(int(ev.Count), 1),
		},
	}
}

// truncate caps message payloads, same bound as the k8s-events
// source.
func truncate(msg string) string {
	const max = 2048
	if len(msg) <= max {
		return msg
	}
	return msg[:max] + "... [truncated by ingress source]"
}

// Run implements sources.Source: starts the Event informer, arms
// after its cache syncs, then blocks until ctx is cancelled. The
// whole source is the informer — there is no poll loop.
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()

	factory := informers.NewSharedInformerFactory(s.client, 0)
	h, err := factory.Core().V1().Events().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if ev, ok := obj.(*corev1.Event); ok {
				s.onEvent(ev)
			}
		},
		UpdateFunc: func(_, newObj any) {
			// Count/LastTimestamp bump = another observation; the
			// engine dedup decides what is a repeat.
			if ev, ok := newObj.(*corev1.Event); ok {
				s.onEvent(ev)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("ingress: register event handler: %w", err)
	}

	factory.Start(ctx.Done())
	// Shutdown blocks until the handler goroutines exit, upholding the
	// Source contract that emit is never called after Run returns.
	defer factory.Shutdown()

	if !cache.WaitForCacheSync(ctx.Done(), h.HasSynced) {
		return fmt.Errorf("ingress: cache sync failed (informer stopped before initial list completed)")
	}
	s.arm()

	<-ctx.Done()
	return nil
}
