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

// Package capacity is the capacity signal source (DESIGN.md §7.2 row
// 7, §10.1): cluster-autoscaler signals from STRUCTURED sources —
// never the CA text log. Four sub-sources feed one Source, in the
// §10.1 ascending-quality order plus the resident pending-pod trend:
//
//  1. Kubernetes Events — NotTriggerScaleUp (per-nodegroup rejection
//     reasons parsed from the event message), TriggeredScaleUp, and
//     the ScaleDown family. The real-time trigger. The capacity
//     source OWNS these reasons with its own Event informer filter:
//     the k8s-events source's default allow-list is deliberately
//     unchanged (its `--reason` surface is frozen deployment config,
//     and CA reasons only mean something when an operator opted into
//     capacity watching), so enabling `capacity` — not editing
//     `--reason` — is what turns them on. The cost is a second
//     events watch; the win is that neither source's contract moves.
//  2. The `cluster-autoscaler-status` ConfigMap (kube-system),
//     polled every Config.PollInterval: per-nodegroup health and
//     backoff, and the cloudProviderTarget vs registered vs ready
//     gap — "asked for a node, didn't get one". Both the legacy text
//     format and the CA ≥ 1.30 yaml format are understood (status.go).
//  3. Provider scale decisions (pkg/cloud CapacityAPI — on GKE, the
//     cluster-autoscaler-visibility Cloud Logging records): the
//     authoritative structured WHY — GCE_STOCKOUT vs
//     GCE_QUOTA_EXCEEDED vs IP exhaustion, whose remedies are
//     disjoint (§10.1). GKE-only; with no provider the sub-source is
//     off with the §2 explicit log line, and the portable
//     sub-sources still fire on every scaleup failure — they just
//     can't always name the structured why.
//  4. Pending-pod aging — pods Pending+Unschedulable for longer than
//     Config.PendingAge. This is the resident TRENDING counterpart
//     of `triage delta`'s point-in-time pending-pod scan: the
//     sentinel owns the wall clock, so it observes the aging itself.
//
// Sub-sources 1–2 are upstream cluster-autoscaler behavior and work
// on any CA-running cluster (§2 portability); only sub-source 3 goes
// through the cloud-provider boundary. This package imports pkg/cloud
// (the boundary), never a cloud SDK (AGENTS.md hard rule).
//
// Arming: the Event informer arms only after its cache syncs — the
// initial LIST replays up to an hour (the event TTL) of stale CA
// events, and the poll + aging sub-sources already cover current
// state, so pre-existing events are recorded nowhere and re-fired
// never. Pending-pod aging is the deliberate exception (same posture
// as objectstate.progress_deadline): it is a countdown over a
// CURRENT condition, not an edge, so a pod already old at startup
// fires after arming and the engine's persisted dedup absorbs any
// repeat.
package capacity

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the stable source name (§7.2 table) used in the signal
// schema and the `--sources` flag.
const Name = "capacity"

// kindPrefix namespaces this source's signal kinds (§7.3).
const kindPrefix = "capacity."

// Signal kinds emitted by this source. APPEND-ONLY: kinds are part of
// the signal schema playbooks and fleet consumers match on — never
// rename or reuse
// one. The dedup/fingerprint reason for each is the kind suffix.
const (
	// KindPending: a NotTriggerScaleUp event — the autoscaler looked
	// at a pending pod and declined, with per-nodegroup rejection
	// reasons parsed from the event message. Warning: the pod is not
	// coming up on its own, but the reasons often name a config fix.
	KindPending = kindPrefix + "pending"
	// KindScaleUp: a TriggeredScaleUp event — the autoscaler asked
	// the cloud for nodes. Info: this is the system working; it
	// matters as stored context when a later gap or stockout says
	// the ask was never answered.
	KindScaleUp = kindPrefix + "scaleup"
	// KindScaleDown: the ScaleDown event family (ScaleDown,
	// ScaleDownEmpty, ScaleDownFailed). Info for the routine
	// variants; warning for ScaleDownFailed.
	KindScaleDown = kindPrefix + "scaledown"
	// KindScaleUpGap: the cluster-autoscaler-status ConfigMap shows
	// a nodegroup whose cloudProviderTarget exceeds its ready count,
	// sustained beyond Config.GapSustain — "asked for a node, didn't
	// get one" (§10.1 source 2). Warning; critical when the
	// nodegroup is additionally in scale-up Backoff with a recorded
	// error (the yaml status format carries errorCode/errorMessage).
	KindScaleUpGap = kindPrefix + "scaleup_gap"
	// KindStockout: a provider scale decision names GCE_STOCKOUT —
	// the zone/machine-type has no capacity (§7.3, §10.1 source 3).
	// Critical, and remedy-disjoint from quota: reroute the pool to
	// another zone or machine type.
	KindStockout = kindPrefix + "stockout"
	// KindQuotaBlocked: a provider scale decision names
	// GCE_QUOTA_EXCEEDED. Critical, and remedy-disjoint from
	// stockout: file a quota increase (§10.3).
	KindQuotaBlocked = kindPrefix + "quota_blocked"
	// KindIPExhausted: a provider scale decision names an
	// IP-exhaustion reason — new nodes/pods cannot get addresses.
	// Critical.
	KindIPExhausted = kindPrefix + "ip_exhausted"
	// KindPendingAged: a pod has been Pending+Unschedulable for
	// longer than Config.PendingAge (§7.3 capacity.pending-aged).
	// Warning at the configured age, critical once the pod has been
	// stuck past CriticalPendingAge.
	KindPendingAged = kindPrefix + "pending-aged"
)

// CriticalPendingAge is the design-fixed escalation threshold for
// KindPendingAged: a pod unschedulable for 15 minutes is a paging
// matter regardless of deployment policy. When Config.PendingAge is
// set higher, escalation follows it (warning can never outrank
// critical's threshold).
const CriticalPendingAge = 15 * time.Minute

// Config are the source's knobs. Zero values take the shipped
// defaults (DefaultConfig).
type Config struct {
	// PollInterval is the cadence of the ConfigMap poll, the
	// provider scale-decision poll, and the pending-age sweep — the
	// --capacity-poll flag. Default 60s.
	PollInterval time.Duration
	// PendingAge is how long a pod must be Pending+Unschedulable
	// before capacity.pending-aged fires at warning — the
	// --pending-age flag. Default 5m. Distinct from `triage delta`'s
	// point-in-time scan: this is the resident trending version.
	PendingAge time.Duration
	// GapSustain is how long a nodegroup's target>ready gap must
	// persist across status polls before capacity.scaleup_gap fires.
	// Default 3m — long enough for a healthy node boot to close the
	// gap, short enough to beat the pending pods it strands.
	GapSustain time.Duration
	// StatusNamespace/StatusName locate the cluster-autoscaler
	// status ConfigMap. Defaults: kube-system /
	// cluster-autoscaler-status (upstream CA's own defaults).
	StatusNamespace string
	StatusName      string
}

// DefaultConfig returns the shipped knobs.
func DefaultConfig() Config {
	return Config{
		PollInterval:    60 * time.Second,
		PendingAge:      5 * time.Minute,
		GapSustain:      3 * time.Minute,
		StatusNamespace: "kube-system",
		StatusName:      "cluster-autoscaler-status",
	}
}

// normalize fills zero fields with defaults.
func (c Config) normalize() Config {
	d := DefaultConfig()
	if c.PollInterval <= 0 {
		c.PollInterval = d.PollInterval
	}
	if c.PendingAge <= 0 {
		c.PendingAge = d.PendingAge
	}
	if c.GapSustain <= 0 {
		c.GapSustain = d.GapSustain
	}
	if c.StatusNamespace == "" {
		c.StatusNamespace = d.StatusNamespace
	}
	if c.StatusName == "" {
		c.StatusName = d.StatusName
	}
	return c
}

// criticalPendingAge is the effective escalation threshold (see
// CriticalPendingAge).
func (c Config) criticalPendingAge() time.Duration {
	if c.PendingAge > CriticalPendingAge {
		return c.PendingAge
	}
	return CriticalPendingAge
}

// Source implements sources.Source for the capacity row of §7.2.
type Source struct {
	client kubernetes.Interface
	// decisions is the provider's CapacityAPI (§10.1 source 3), nil
	// when the provider lacks the capability — then unavailable
	// carries the §2 explicit reason logged at Run.
	decisions   cloud.CapacityAPI
	unavailable string
	cfg         Config

	mu   sync.Mutex
	emit func(engine.Signal)
	// armed flips true after the informer caches sync: event
	// handlers record nothing and emit nothing before that (the
	// initial LIST is history, not observation); the pending-pod
	// tracker records from the initial LIST but only fires from the
	// sweep, which runs post-arm by construction.
	armed bool
	// pending tracks Unschedulable pods for the aging sub-source,
	// keyed by pod UID.
	pending map[string]*pendingEntry
	// gaps tracks per-nodegroup scale-up gap episodes, keyed by
	// nodegroup name.
	gaps map[string]*gapEntry
	// statusMissing dedupes the "no status ConfigMap" log line.
	statusMissing bool
	// lastDecisions is the high-water mark of the provider
	// scale-decision poll window.
	lastDecisions time.Time

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
	// logf overrides log.Printf for testing. nil = log.Printf.
	logf func(format string, args ...any)
}

// New constructs the source. provider is the pkg/cloud boundary —
// pass cloud.NoProvider (never nil) on vanilla clusters; the
// sub-source degrades explicitly per §2. Zero-valued cfg fields take
// the shipped defaults.
func New(client kubernetes.Interface, provider cloud.Provider, cfg Config) *Source {
	s := &Source{
		client:  client,
		cfg:     cfg.normalize(),
		pending: make(map[string]*pendingEntry),
		gaps:    make(map[string]*gapEntry),
	}
	if provider == nil {
		provider = cloud.NoProvider
	}
	if api, ok := provider.Capacity(); ok {
		s.decisions = api
	} else {
		s.unavailable = cloud.Unavailable(provider, cloud.CapabilityCapacity).Marker()
	}
	return s
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: the Event and Pod informers watch
// cluster-wide and the status ConfigMap lives in kube-system, so the
// source needs cluster RBAC (§11).
func (s *Source) Scope() sources.Scope { return sources.ScopeCluster }

// RequiredAccess implements sources.AccessDeclarer (§11): the two
// informers' list+watch, plus `get` on the status ConfigMap — that
// one is namespace- AND name-scoped, so a deployment can grant it
// with a kube-system Role pinned to cluster-autoscaler-status
// (deploy/14-role-watcher-capacity.yaml) instead of widening the
// ClusterRole; the probe verifies exactly that shape.
func (s *Source) RequiredAccess() []sources.Requirement {
	return []sources.Requirement{
		{Resource: "events", Verb: "list"},
		{Resource: "events", Verb: "watch"},
		{Resource: "pods", Verb: "list"},
		{Resource: "pods", Verb: "watch"},
		{Resource: "configmaps", Verb: "get", Namespace: s.cfg.StatusNamespace, Name: s.cfg.StatusName},
	}
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

// arm enables event emission — called once the informer caches sync.
func (s *Source) arm() {
	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()
}

// send hands a signal to the pipeline under the source lock's
// protection of s.emit (Run sets it before the informers start).
func (s *Source) send(sig engine.Signal) {
	s.mu.Lock()
	emit := s.emit
	s.mu.Unlock()
	if emit != nil {
		emit(sig)
	}
}

// Run implements sources.Source: states the provider posture once
// (§2 — absent cloud is reported, never silent), starts the Event and
// Pod informers, arms after their caches sync, then drives the poll
// ticker (status ConfigMap, provider decisions, pending-age sweep)
// until ctx is cancelled. The first status poll failing is NOT fatal
// (clusters without a cluster autoscaler are legal deployments — the
// absence is logged once); informer sync failure is.
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()

	if s.decisions == nil {
		s.logPrintf("capacity: provider scale-decision sub-source (§10.1 source 3) disabled: %s — Events + status-ConfigMap sub-sources still fire on scaleup failures, without the structured why", s.unavailable)
	} else {
		s.logPrintf("capacity: provider scale-decision sub-source enabled (poll %s)", s.cfg.PollInterval)
	}

	factory := informers.NewSharedInformerFactory(s.client, 0)
	eventInformer := factory.Core().V1().Events().Informer()
	eventH, err := eventInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
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
		return fmt.Errorf("capacity: register event handler: %w", err)
	}

	podInformer := factory.Core().V1().Pods().Informer()
	podH, err := podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if p, ok := obj.(*corev1.Pod); ok {
				s.trackPod(p)
			}
		},
		UpdateFunc: func(_, newObj any) {
			if p, ok := newObj.(*corev1.Pod); ok {
				s.trackPod(p)
			}
		},
		DeleteFunc: func(obj any) {
			if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = d.Obj
			}
			if p, ok := obj.(*corev1.Pod); ok {
				s.forgetPod(p)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("capacity: register pod handler: %w", err)
	}

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), eventH.HasSynced, podH.HasSynced) {
		return fmt.Errorf("capacity: cache sync failed (informer stopped before initial list completed)")
	}
	s.arm()

	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.poll(ctx)
		}
	}
}

// poll runs one tick of the polled sub-sources. Errors are logged and
// retried next tick — a transient API error must not kill a resident
// watcher; a PERSISTENTLY failing poll logs every tick, which is the
// §11 loud-not-silent posture for a running source.
func (s *Source) poll(ctx context.Context) {
	now := s.clock()
	if err := s.pollStatus(ctx, now); err != nil {
		s.logPrintf("capacity: status ConfigMap poll failed (retry in %s): %v", s.cfg.PollInterval, err)
	}
	if s.decisions != nil {
		if err := s.pollDecisions(ctx, now); err != nil {
			s.logPrintf("capacity: provider scale-decision poll failed (retry in %s): %v", s.cfg.PollInterval, err)
		}
	}
	s.sweepPending(now)
}
