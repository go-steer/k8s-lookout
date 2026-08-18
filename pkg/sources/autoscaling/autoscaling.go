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

// Package autoscaling is the HPA-saturation signal source (post-M5
// roadmap B.2, issue #131). The HPA split, stated explicitly:
//
//   - THRASH is a read-path check: `triage events` reconstructs the
//     replica history from SuccessfulRescale events and emits
//     event.hpa_thrash (pkg/checks/events/hpa.go). A one-shot scan can
//     recover an oscillation; a resident process adds nothing there.
//   - PINNED and METRICS-DEAD are resident leading indicators: both
//     are SUSTAINED states, invisible to a point-in-time scan without
//     history and invisible to the k8s-events source without watching
//     the HPA controller's chatty Normal events. They live here.
//
// Two kinds (§7.3, APPEND-ONLY):
//
//   - autoscaling.hpa_pinned — an HPA has sat at spec.maxReplicas with
//     its metric still over target past Config.PinnedSustain: the
//     autoscaler is out of headroom, and the next load increase has
//     nowhere to go. Warning once per episode; escalates to critical
//     once past Config.PinnedCritSustain (the capacity pending-aged
//     level-latch pattern — one warning, one escalation per episode).
//   - autoscaling.hpa_metrics_dead — the HPA's metrics pipeline is
//     broken (ScalingActive=False with a FailedGet* reason: resource,
//     pods, object, external, container-resource metric fetch
//     failures) past Config.MetricsDeadSustain: autoscaling is
//     silently dead. Warning-only — it is a leading indicator; the
//     load consequence, if it comes, pages on its own.
//
// Why CONDITION-based, not event-based: the HPA controller narrates
// both states as Events (FailedGetResourceMetric warnings and the
// "unable to compute replica count" family), but detecting them from
// status conditions needs no events RBAC beyond this source's own HPA
// list/watch, no allow-list additions, and no coupling to the
// controller's event reason/message strings — conditions are API,
// event text is not. ScalingLimited=True/TooManyReplicas is the
// controller's OWN verdict that it capped the desired count at max,
// robust across every metric type; comparing currentMetrics to
// targets by hand is kept only as a secondary predicate (and for the
// message's "cpu at 87% vs target 60%" clause).
//
// The pinned predicate deliberately requires the CAP, not just
// current==max: an HPA sized minReplicas==maxReplicas, or one whose
// metric is satisfied exactly at max, reports ScalingLimited=False
// (or a non-TooManyReplicas reason) and must NOT fire. Likewise
// ScalingActive=False with reason ScalingDisabled (spec.replicas==0
// on the target) is a deliberate operator state, not a dead pipeline.
//
// Timing model: informer updates only RECORD state (the HPA
// controller refreshes status every ~15s, so updates flow steadily);
// the poll ticker JUDGES the sustained windows post-arm. Episode
// semantics: a pinned episode ends when the HPA is no longer at max
// or the cap lifts; a metrics-dead episode ends when the FailedGet*
// state ends. Ending an episode resets the fired latch, so a NEW
// episode fires again. An HPA already pinned at startup counts its
// window from arming (countdown posture — the compound predicate has
// no single trustworthy transition timestamp); a pipeline already
// dead at startup counts from the ScalingActive condition's
// lastTransitionTime (one condition, its own timestamp), and the
// engine's persisted dedup absorbs restart repeats.
//
// §7.4 clearance (each source that can observe a symptom observes its
// absence):
//
//   - hpa_pinned clears as recovered when the HPA is below max or no
//     longer ScalingLimited; StableSince is when that was OBSERVED
//     (M3 observation 4: never a historical status timestamp).
//   - hpa_metrics_dead clears as recovered when ScalingActive is back
//     to True (not merely a reason change away from FailedGet* —
//     False/ScalingDisabled is not a live pipeline); StableSince is
//     the observed False→True transition.
//   - HPA deleted → object_deleted.
package autoscaling

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the stable source name (§7.2 table) used in the signal
// schema and the `--sources` flag.
const Name = "autoscaling"

// kindPrefix namespaces this source's kinds (§7.3).
const kindPrefix = "autoscaling."

// The kinds this source emits (§7.3). APPEND-ONLY.
const (
	KindPinned      = kindPrefix + "hpa_pinned"
	KindMetricsDead = kindPrefix + "hpa_metrics_dead"
)

// The dedup/fingerprint reasons (kind suffixes, the objectstate
// convention). Both map to themselves under CanonicalReason.
const (
	ReasonPinned      = "hpa_pinned"
	ReasonMetricsDead = "hpa_metrics_dead"
)

// hpaKind is the KindOfObject every signal and clearance verdict of
// this source carries.
const hpaKind = "HorizontalPodAutoscaler"

// reasonScalingDisabled is the ScalingActive=False reason the
// controller sets when the scale target is at spec.replicas==0 — a
// deliberate operator state that must NOT read as a dead pipeline.
const reasonScalingDisabled = "ScalingDisabled"

// failedGetPrefix matches the ScalingActive=False reason family that
// means the metrics PIPELINE is broken: FailedGetResourceMetric,
// FailedGetPodsMetric, FailedGetObjectMetric, FailedGetExternalMetric,
// FailedGetContainerResourceMetric.
const failedGetPrefix = "FailedGet"

// reasonTooManyReplicas is the ScalingLimited=True reason meaning the
// controller capped the desired replica count at spec.maxReplicas —
// the condition-based pinned verdict.
const reasonTooManyReplicas = "TooManyReplicas"

// Config are the source's thresholds. Zero values take the defaults.
type Config struct {
	// PinnedSustain is how long an HPA must sit pinned at max (capped,
	// metric over target) before hpa_pinned fires at warning.
	// Default 10m — long enough to outlive a traffic spike the HPA is
	// still absorbing, short enough to lead the outage.
	PinnedSustain time.Duration
	// PinnedCritSustain is the escalation threshold: pinned this long
	// escalates the episode to critical (once). Default 30m; never
	// effectively below PinnedSustain.
	PinnedCritSustain time.Duration
	// MetricsDeadSustain is how long ScalingActive must report a
	// FailedGet* reason before hpa_metrics_dead fires (warning-only).
	// Default 15m — metrics-server restarts and apiserver blips heal
	// well inside it.
	MetricsDeadSustain time.Duration
	// TickInterval drives the sustain sweep (the windows are judged by
	// the clock, not by update arrival). Default 30s.
	TickInterval time.Duration
}

// DefaultConfig returns the shipped thresholds.
func DefaultConfig() Config {
	return Config{
		PinnedSustain:      10 * time.Minute,
		PinnedCritSustain:  30 * time.Minute,
		MetricsDeadSustain: 15 * time.Minute,
		TickInterval:       30 * time.Second,
	}
}

// normalize fills zero fields with defaults.
func (c Config) normalize() Config {
	d := DefaultConfig()
	if c.PinnedSustain <= 0 {
		c.PinnedSustain = d.PinnedSustain
	}
	if c.PinnedCritSustain <= 0 {
		c.PinnedCritSustain = d.PinnedCritSustain
	}
	if c.MetricsDeadSustain <= 0 {
		c.MetricsDeadSustain = d.MetricsDeadSustain
	}
	if c.TickInterval <= 0 {
		c.TickInterval = d.TickInterval
	}
	return c
}

// pinnedCritSustain is the effective escalation threshold: a config
// that raised PinnedSustain past PinnedCritSustain escalates at the
// warning threshold, never before it.
func (c Config) pinnedCritSustain() time.Duration {
	if c.PinnedSustain > c.PinnedCritSustain {
		return c.PinnedSustain
	}
	return c.PinnedCritSustain
}

// level is the per-episode fired latch: one warning, one critical
// escalation per episode (the capacity pending-aged pattern).
type level int

const (
	levelNone level = iota
	levelWarn
	levelCritical
)

func (l level) severity() engine.Severity {
	if l == levelCritical {
		return engine.SeverityCritical
	}
	return engine.SeverityWarning
}

// hpaEntry is the per-HPA state mirror plus episode memory.
type hpaEntry struct {
	namespace string
	name      string
	// targetKind/targetName mirror spec.scaleTargetRef — the message
	// names the workload, not just the HPA.
	targetKind  string
	targetName  string
	maxReplicas int32

	// ---- pinned episode ----
	pinned bool
	// pinnedSince is when the compound predicate was OBSERVED turning
	// true (countdown posture for HPAs already pinned at startup —
	// see the package comment).
	pinnedSince time.Time
	pinnedFired level
	// pinnedDetail is the metric-vs-target clause for the message
	// ("cpu at 87% vs target 60%"); may be empty when only the
	// condition carried the verdict.
	pinnedDetail string
	// pinnedClearedAt stamps the pinned→not-pinned OBSERVATION — the
	// clearance StableSince.
	pinnedClearedAt time.Time

	// ---- metrics-dead episode ----
	metricsDead bool
	// metricsDeadSince anchors the sustain window: the ScalingActive
	// condition's lastTransitionTime when it predates the observation
	// (a pipeline already dead at startup), else the observation.
	metricsDeadSince  time.Time
	metricsDeadFired  bool
	metricsDeadReason string
	metricsDeadMsg    string

	// scalingActive mirrors ScalingActive==True; metricsAliveAt stamps
	// the False→True OBSERVATION — the clearance StableSince.
	scalingActive  bool
	metricsAliveAt time.Time
}

// Source implements sources.Source (and engine.ClearanceObserver) for
// the autoscaling row of the post-M5 roadmap.
type Source struct {
	client kubernetes.Interface
	cfg    Config

	mu sync.Mutex
	// armed flips true after the informer cache syncs; the sweep (the
	// only emitter) runs post-arm by construction, so handlers record
	// freely from the initial LIST.
	armed   bool
	armedAt time.Time
	emit    func(engine.Signal)

	hpas map[types.UID]*hpaEntry

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
}

// New constructs the source. Zero-valued cfg fields take the shipped
// defaults.
func New(client kubernetes.Interface, cfg Config) *Source {
	return &Source{
		client: client,
		cfg:    cfg.normalize(),
		hpas:   make(map[types.UID]*hpaEntry),
	}
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: the informer lists HPAs
// cluster-wide.
func (s *Source) Scope() sources.Scope { return sources.ScopeCluster }

// ClearanceObserver returns the §7.4 clearance predicate for this
// source's incidents, backed by its informer mirror.
func (s *Source) ClearanceObserver() engine.ClearanceObserver { return s }

// RequiredAccess implements sources.AccessDeclarer (§11): list+watch
// on the one informer target. Matches deploy/12-clusterrole-watcher.yaml.
func (s *Source) RequiredAccess() []sources.Requirement {
	var reqs []sources.Requirement
	for _, verb := range []string{"list", "watch"} {
		reqs = append(reqs, sources.Requirement{Group: "autoscaling", Resource: "horizontalpodautoscalers", Verb: verb})
	}
	return reqs
}

func (s *Source) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// HasSynced implements sources.SyncReporter — the sentinel's /readyz
// probe is not ready until every source with a barrier has crossed it.
func (s *Source) HasSynced() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.armed
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

// Run implements sources.Source: starts the HPA informer on a private
// factory, arms after the cache syncs, then drives the sustain sweep
// until ctx is cancelled. The factory is private on purpose — the
// §6.3 shared factory carries the pods/nodes/workloads informers the
// graph and other sources share; nothing else watches HPAs.
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()

	factory := informers.NewSharedInformerFactory(s.client, 0)
	h, err := factory.Autoscaling().V2().HorizontalPodAutoscalers().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.asHPA(obj, s.onHPA) },
		UpdateFunc: func(_, obj any) { s.asHPA(obj, s.onHPA) },
		DeleteFunc: func(obj any) { s.asHPA(tombstoneObj(obj), s.onHPADelete) },
	})
	if err != nil {
		return fmt.Errorf("autoscaling: register hpa handler: %w", err)
	}

	factory.Start(ctx.Done())
	// Shutdown blocks until every handler goroutine exits, upholding
	// the Source contract that emit is never called after Run returns.
	defer factory.Shutdown()

	if !cache.WaitForCacheSync(ctx.Done(), h.HasSynced) {
		return fmt.Errorf("autoscaling: cache sync failed (informer stopped before initial list completed)")
	}
	s.mu.Lock()
	s.armed = true
	s.armedAt = s.clock()
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

func (s *Source) asHPA(obj any, fn func(*autoscalingv2.HorizontalPodAutoscaler)) {
	if h, ok := obj.(*autoscalingv2.HorizontalPodAutoscaler); ok {
		fn(h)
	}
}

// condition returns the named status condition, nil when absent.
func condition(h *autoscalingv2.HorizontalPodAutoscaler, t autoscalingv2.HorizontalPodAutoscalerConditionType) *autoscalingv2.HorizontalPodAutoscalerCondition {
	for i := range h.Status.Conditions {
		if h.Status.Conditions[i].Type == t {
			return &h.Status.Conditions[i]
		}
	}
	return nil
}

// pinnedState evaluates the pinned predicate: at max AND capped.
// Capped is the controller's own ScalingLimited=True/TooManyReplicas
// verdict (primary — robust across metric types); the secondary arm
// (desired>=max with a current metric over target) covers status
// shapes without the condition. detail is the metric-vs-target
// clause for the message, "" when no metric pair was comparable.
func pinnedState(h *autoscalingv2.HorizontalPodAutoscaler) (pinned bool, detail string) {
	max := h.Spec.MaxReplicas
	if max <= 0 || h.Status.CurrentReplicas != max {
		return false, ""
	}
	over, clause := metricOverTarget(h)
	limited := condition(h, autoscalingv2.ScalingLimited)
	capped := limited != nil && limited.Status == corev1.ConditionTrue && limited.Reason == reasonTooManyReplicas
	if !capped && (h.Status.DesiredReplicas < max || !over) {
		return false, ""
	}
	return true, clause
}

// metricsDeadState evaluates the metrics-dead predicate:
// ScalingActive=False with a FailedGet* reason. ScalingDisabled (and
// every other non-FailedGet reason, e.g. InvalidSelector) does NOT
// count — only the metric-fetch family means the pipeline is dead.
func metricsDeadState(h *autoscalingv2.HorizontalPodAutoscaler) (dead bool, cond *autoscalingv2.HorizontalPodAutoscalerCondition) {
	c := condition(h, autoscalingv2.ScalingActive)
	if c == nil || c.Status != corev1.ConditionFalse {
		return false, c
	}
	if c.Reason == reasonScalingDisabled || !strings.HasPrefix(c.Reason, failedGetPrefix) {
		return false, c
	}
	return true, c
}

// ---- informer handlers ----

func (s *Source) onHPA(h *autoscalingv2.HorizontalPodAutoscaler) {
	now := s.clock()
	pinnedNow, detail := pinnedState(h)
	deadNow, activeCond := metricsDeadState(h)
	active := activeCond != nil && activeCond.Status == corev1.ConditionTrue

	s.mu.Lock()
	defer s.mu.Unlock()
	e, known := s.hpas[h.UID]
	if !known {
		e = &hpaEntry{}
		s.hpas[h.UID] = e
	}
	e.namespace, e.name = h.Namespace, h.Name
	e.targetKind, e.targetName = h.Spec.ScaleTargetRef.Kind, h.Spec.ScaleTargetRef.Name
	e.maxReplicas = h.Spec.MaxReplicas

	// Pinned episode edges. Recording is not emission — the sweep
	// judges the windows post-arm.
	if pinnedNow {
		if !e.pinned {
			e.pinned = true
			e.pinnedSince = now
			e.pinnedFired = levelNone
		}
		if detail != "" {
			e.pinnedDetail = detail
		}
	} else if e.pinned {
		e.pinned = false
		e.pinnedFired = levelNone
		e.pinnedDetail = ""
		e.pinnedClearedAt = now
	}

	// Metrics-dead episode edges.
	if deadNow {
		if !e.metricsDead {
			e.metricsDead = true
			e.metricsDeadFired = false
			e.metricsDeadSince = now
			if ltt := activeCond.LastTransitionTime.Time; !ltt.IsZero() && ltt.Before(now) {
				// A pipeline already dead when first observed counts
				// from the condition's own transition (one condition,
				// one trustworthy timestamp); persisted dedup absorbs
				// restart repeats.
				e.metricsDeadSince = ltt
			}
		}
		e.metricsDeadReason = activeCond.Reason
		e.metricsDeadMsg = activeCond.Message
	} else if e.metricsDead {
		e.metricsDead = false
		e.metricsDeadFired = false
	}
	if known && active && !e.scalingActive {
		e.metricsAliveAt = now // the clearance StableSince (observed)
	}
	e.scalingActive = active
}

func (s *Source) onHPADelete(h *autoscalingv2.HorizontalPodAutoscaler) {
	s.mu.Lock()
	delete(s.hpas, h.UID)
	s.mu.Unlock()
}

// ---- the sustain sweep ----

// sweep judges every tracked episode's window. Returns the signals to
// emit (the caller sends them outside the lock). Fired latches keep
// each episode to one warning (and, for pinned, one escalation).
func (s *Source) sweep(now time.Time) []engine.Signal {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.armed {
		return nil
	}
	var out []engine.Signal
	for uid, e := range s.hpas {
		if e.pinned {
			age := now.Sub(e.pinnedSince)
			lvl := levelNone
			switch {
			case age >= s.cfg.pinnedCritSustain():
				lvl = levelCritical
			case age >= s.cfg.PinnedSustain:
				lvl = levelWarn
			}
			if lvl != levelNone && lvl > e.pinnedFired {
				e.pinnedFired = lvl
				out = append(out, pinnedSignal(uid, e, age, now, lvl))
			}
		}
		if e.metricsDead && !e.metricsDeadFired && now.Sub(e.metricsDeadSince) >= s.cfg.MetricsDeadSustain {
			e.metricsDeadFired = true
			out = append(out, metricsDeadSignal(uid, e, now.Sub(e.metricsDeadSince), now))
		}
	}
	return out
}

// scaleTarget renders spec.scaleTargetRef for messages ("Deployment
// web"), falling back to the HPA's own name.
func (e *hpaEntry) scaleTarget() string {
	if e.targetName == "" {
		return "HPA " + e.name
	}
	if e.targetKind == "" {
		return e.targetName
	}
	return e.targetKind + " " + e.targetName
}

func pinnedSignal(uid types.UID, e *hpaEntry, age time.Duration, now time.Time, lvl level) engine.Signal {
	msg := fmt.Sprintf("%s pinned at maxReplicas=%d for %s", e.scaleTarget(), e.maxReplicas, age.Truncate(time.Second))
	if e.pinnedDetail != "" {
		msg += ", " + e.pinnedDetail
	} else {
		msg += ", metric still over target (ScalingLimited=TooManyReplicas)"
	}
	msg += " — the autoscaler is out of headroom; the next load increase has nowhere to go"
	return engine.Signal{
		Kind:     KindPinned,
		Source:   engine.SourceSentinel,
		Severity: lvl.severity(),
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: string(uid), Reason: ReasonPinned},
			Namespace:    e.namespace,
			KindOfObject: hpaKind,
			Name:         e.name,
			Message:      msg,
			FirstSeen:    e.pinnedSince,
			LastSeen:     now,
			Count:        1,
		},
	}
}

func metricsDeadSignal(uid types.UID, e *hpaEntry, age time.Duration, now time.Time) engine.Signal {
	msg := fmt.Sprintf("HPA metrics pipeline dead for %s (%s", age.Truncate(time.Second), e.metricsDeadReason)
	if e.metricsDeadMsg != "" {
		msg += ": " + e.metricsDeadMsg
	}
	msg += fmt.Sprintf(") — %s is not autoscaling and nothing else will say so", e.scaleTarget())
	return engine.Signal{
		Kind:     KindMetricsDead,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityWarning,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: string(uid), Reason: ReasonMetricsDead},
			Namespace:    e.namespace,
			KindOfObject: hpaKind,
			Name:         e.name,
			Message:      msg,
			FirstSeen:    e.metricsDeadSince,
			LastSeen:     now,
			Count:        1,
		},
	}
}

// ---- metric-vs-target comparison ----

// metricOverTarget reports whether any current metric exceeds its
// spec target, with the human clause for the first over-target pair
// ("cpu at 87% vs target 60%"). Pairs are matched by (source type,
// metric identifier); incomparable pairs (mixed representations,
// status not yet populated) are skipped — the condition-based
// predicate does not depend on this succeeding.
func metricOverTarget(h *autoscalingv2.HorizontalPodAutoscaler) (bool, string) {
	current := make(map[string]autoscalingv2.MetricValueStatus, len(h.Status.CurrentMetrics))
	for _, st := range h.Status.CurrentMetrics {
		if id, v, ok := statusValue(st); ok {
			current[id] = v
		}
	}
	for _, spec := range h.Spec.Metrics {
		id, display, tgt, ok := specTarget(spec)
		if !ok {
			continue
		}
		cur, ok := current[id]
		if !ok {
			continue
		}
		if over, clause := overClause(display, cur, tgt); over {
			return true, clause
		}
	}
	return false, ""
}

func metricID(t autoscalingv2.MetricSourceType, name string) string {
	return string(t) + "/" + name
}

// specTarget extracts one metric spec's identity and target.
func specTarget(m autoscalingv2.MetricSpec) (id, display string, tgt autoscalingv2.MetricTarget, ok bool) {
	switch m.Type {
	case autoscalingv2.ResourceMetricSourceType:
		if m.Resource != nil {
			n := string(m.Resource.Name)
			return metricID(m.Type, n), n, m.Resource.Target, true
		}
	case autoscalingv2.ContainerResourceMetricSourceType:
		if m.ContainerResource != nil {
			n := string(m.ContainerResource.Name) + "/" + m.ContainerResource.Container
			return metricID(m.Type, n), n, m.ContainerResource.Target, true
		}
	case autoscalingv2.PodsMetricSourceType:
		if m.Pods != nil {
			return metricID(m.Type, m.Pods.Metric.Name), m.Pods.Metric.Name, m.Pods.Target, true
		}
	case autoscalingv2.ObjectMetricSourceType:
		if m.Object != nil {
			return metricID(m.Type, m.Object.Metric.Name), m.Object.Metric.Name, m.Object.Target, true
		}
	case autoscalingv2.ExternalMetricSourceType:
		if m.External != nil {
			return metricID(m.Type, m.External.Metric.Name), m.External.Metric.Name, m.External.Target, true
		}
	}
	return "", "", autoscalingv2.MetricTarget{}, false
}

// statusValue extracts one metric status's identity and current value.
func statusValue(m autoscalingv2.MetricStatus) (id string, v autoscalingv2.MetricValueStatus, ok bool) {
	switch m.Type {
	case autoscalingv2.ResourceMetricSourceType:
		if m.Resource != nil {
			return metricID(m.Type, string(m.Resource.Name)), m.Resource.Current, true
		}
	case autoscalingv2.ContainerResourceMetricSourceType:
		if m.ContainerResource != nil {
			n := string(m.ContainerResource.Name) + "/" + m.ContainerResource.Container
			return metricID(m.Type, n), m.ContainerResource.Current, true
		}
	case autoscalingv2.PodsMetricSourceType:
		if m.Pods != nil {
			return metricID(m.Type, m.Pods.Metric.Name), m.Pods.Current, true
		}
	case autoscalingv2.ObjectMetricSourceType:
		if m.Object != nil {
			return metricID(m.Type, m.Object.Metric.Name), m.Object.Current, true
		}
	case autoscalingv2.ExternalMetricSourceType:
		if m.External != nil {
			return metricID(m.Type, m.External.Metric.Name), m.External.Current, true
		}
	}
	return "", autoscalingv2.MetricValueStatus{}, false
}

// overClause compares one current/target pair in whichever
// representation both sides carry.
func overClause(display string, cur autoscalingv2.MetricValueStatus, tgt autoscalingv2.MetricTarget) (bool, string) {
	switch {
	case tgt.AverageUtilization != nil && cur.AverageUtilization != nil:
		return *cur.AverageUtilization > *tgt.AverageUtilization,
			fmt.Sprintf("%s at %d%% vs target %d%%", display, *cur.AverageUtilization, *tgt.AverageUtilization)
	case tgt.AverageValue != nil && cur.AverageValue != nil:
		return cur.AverageValue.Cmp(*tgt.AverageValue) > 0,
			fmt.Sprintf("%s at %s vs target %s", display, cur.AverageValue, tgt.AverageValue)
	case tgt.Value != nil && cur.Value != nil:
		return cur.Value.Cmp(*tgt.Value) > 0,
			fmt.Sprintf("%s at %s vs target %s", display, cur.Value, tgt.Value)
	}
	return false, ""
}

// ---- §7.4 clearance ----

// Clearance implements engine.ClearanceObserver for this source's
// incidents. See the package comment for the semantics per kind.
// ok=false for other sources' incidents, or before the cache synced
// (cannot judge against an empty mirror).
func (s *Source) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	reason := engine.CanonicalReason(inc.Key.Reason)
	if reason != ReasonPinned && reason != ReasonMetricsDead {
		return engine.Clearance{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.armed {
		return engine.Clearance{}, false
	}
	e, ok := s.hpas[types.UID(inc.Key.UID)]
	if !ok {
		return engine.Clearance{Cleared: true, Resolution: engine.ResolutionObjectDeleted}, true
	}
	if reason == ReasonPinned {
		if !e.pinned {
			// Below max or no longer ScalingLimited. StableSince is the
			// observed lift; zero for incidents restored from a previous
			// sentinel's snapshot whose lift predates this process
			// ("absent as of this observation only").
			return engine.Clearance{Cleared: true, StableSince: e.pinnedClearedAt, Resolution: engine.ResolutionRecovered}, true
		}
		return engine.Clearance{Cleared: false, Resolution: engine.ResolutionRecovered}, true
	}
	if e.scalingActive {
		// ScalingActive returned True — the pipeline computed a replica
		// count again. Merely leaving the FailedGet* reason (e.g. into
		// ScalingDisabled) ends the EPISODE but does not clear: a
		// disabled target says nothing about the pipeline.
		return engine.Clearance{Cleared: true, StableSince: e.metricsAliveAt, Resolution: engine.ResolutionRecovered}, true
	}
	return engine.Clearance{Cleared: false, Resolution: engine.ResolutionRecovered}, true
}
