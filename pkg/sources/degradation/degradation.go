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

// Package degradation is the degradation signal source (DESIGN.md
// §7.2 row 5): leading indicators from TRENDS on EndpointSlice ready
// ratios and from probe flaps below the `Unhealthy` threshold —
// "payment-backend capacity 5/5 → 3/5 over 10 min".
//
// Boundary with object-state (deliberate, both directions):
//
//   - objectstate.endpoints_empty is the >0 → 0 TRANSITION — the
//     outage. degradation.capacity is the decline BEFORE it: a ready
//     RATIO trending down across a window. This source never fires
//     when the ratio reaches 0; the empty transition is object-state's
//     critical, and double-paging the same instant helps no one.
//   - k8s-events' `Unhealthy` reason (gated by --unhealthy-min-count
//     consecutive failures) is the sustained probe failure.
//     degradation.probe_flap is the pod that keeps dipping BELOW that
//     threshold: each dip recovers before the consecutive-failure gate
//     is reached, so the reactive path stays silent while the workload
//     serves through a flapping readiness gate. Flip counting encodes
//     the "never breached" clause structurally: a pod that goes
//     NotReady and STAYS there produces exactly one transition — it
//     can never reach the flip threshold, and its sustained failure is
//     the k8s-events source's signal to make.
//
// Firing predicate for degradation.capacity (exact, per §7.2):
// samples of (ready, total) are recorded per Service across its
// EndpointSlices — on every slice update (coalesced to one sample per
// Config.SampleFloor so a near-simultaneous burst of slice writes is
// ONE step) and on a low-frequency tick (so a stalled stream still
// ages the window). Over the retained window W (Config.Window):
//
//	r0 = ready ratio at window start, rn = ready ratio now
//	downSteps = count of consecutive-sample pairs whose ratio DECREASED
//
// Fire iff: rn > 0  AND  (r0 - rn) >= Config.Drop  AND
// downSteps >= Config.MinDownSteps (default 2). A single blip — one
// downward step, however deep — never fires; a decline that lands
// exactly at 0 never fires here (see boundary above). Severity is
// warning, escalated to critical when rn <= CriticalRatio (0.5). The
// signal fires ONCE per decline episode: the latch clears only when
// the ratio recovers to the window-start level r0 (or to 1.0), which
// is also the §7.4 clearance predicate.
//
// Transition discipline: like object-state, informer handlers record
// state from the initial LIST without emitting; the source arms only
// after all caches sync, so a restart never re-fires a decline it did
// not observe.
package degradation

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the stable source name (§7.2 table) used in the signal
// schema and the `--sources` flag.
const Name = "degradation"

// kindPrefix namespaces this source's signal kinds (§7.3).
const kindPrefix = "degradation."

// Signal kinds emitted by this source. APPEND-ONLY: kinds are part of
// the signal schema playbooks and fleet consumers match on — never
// rename or reuse
// one. The dedup/fingerprint reason for each is the kind suffix.
const (
	// KindCapacity: a Service's ready-endpoint ratio declined across
	// the window per the package-level predicate — capacity eroding
	// before the outage. Warning; critical when the surviving ratio
	// is <= CriticalRatio.
	KindCapacity = kindPrefix + "capacity"
	// KindProbeFlap: a pod's readiness gate flipped
	// Config.FlapCount times within the window without ever
	// sustaining failure long enough for the reactive `Unhealthy`
	// path (see package comment).
	KindProbeFlap = kindPrefix + "probe_flap"
)

// CriticalRatio is the surviving-capacity ratio at or below which a
// capacity decline escalates from warning to critical.
const CriticalRatio = 0.5

// reasonOf derives the dedup/fingerprint reason from a kind.
func reasonOf(kind string) string { return strings.TrimPrefix(kind, kindPrefix) }

// Config are the source's thresholds. Zero values take the defaults.
type Config struct {
	// Window is the trend window for both the capacity ratio series
	// and probe-flap counting (--degradation-window). Default 15m.
	Window time.Duration
	// Drop is the minimum ready-ratio decline from window start that
	// fires KindCapacity (--degradation-drop). Default 0.3.
	Drop float64
	// MinDownSteps is the minimum count of distinct downward steps
	// within the window — the no-single-blip gate. Default 2.
	MinDownSteps int
	// FlapCount is the minimum readiness-gate flips (transitions in
	// either direction) within Window that fire KindProbeFlap.
	// Default 4 (down-up-down-up: two full dips).
	FlapCount int
	// SampleFloor coalesces ratio samples: a slice update arriving
	// within SampleFloor of the previous sample replaces it instead
	// of appending, so a burst of near-simultaneous writes is one
	// step. Default 10s.
	SampleFloor time.Duration
	// TickInterval drives the low-frequency sampling tick (a service
	// whose slices stop updating still needs its window to age) and
	// the state-TTL prune. Default 60s.
	TickInterval time.Duration
	// StateTTL bounds per-object state: entries with no informer
	// activity within this window are dropped. Default 24h.
	StateTTL time.Duration
}

// DefaultConfig returns the shipped thresholds.
func DefaultConfig() Config {
	return Config{
		Window:       15 * time.Minute,
		Drop:         0.3,
		MinDownSteps: 2,
		FlapCount:    4,
		SampleFloor:  10 * time.Second,
		TickInterval: 60 * time.Second,
		StateTTL:     24 * time.Hour,
	}
}

// normalize fills zero fields with defaults.
func (c Config) normalize() Config {
	d := DefaultConfig()
	if c.Window <= 0 {
		c.Window = d.Window
	}
	if c.Drop <= 0 || c.Drop > 1 {
		c.Drop = d.Drop
	}
	if c.MinDownSteps <= 0 {
		c.MinDownSteps = d.MinDownSteps
	}
	if c.FlapCount <= 0 {
		c.FlapCount = d.FlapCount
	}
	if c.SampleFloor <= 0 {
		c.SampleFloor = d.SampleFloor
	}
	if c.TickInterval <= 0 {
		c.TickInterval = d.TickInterval
	}
	if c.StateTTL <= 0 {
		c.StateTTL = d.StateTTL
	}
	return c
}

// serviceKey identifies the Service an EndpointSlice belongs to.
type serviceKey struct {
	namespace string
	name      string
}

// sliceCounts is one slice's contribution to a Service's totals.
type sliceCounts struct {
	ready int
	total int
}

// sample is one point of the per-Service ready-ratio time series.
type sample struct {
	at    time.Time
	ready int
	total int
}

func (s sample) ratio() float64 {
	if s.total == 0 {
		return 0
	}
	return float64(s.ready) / float64(s.total)
}

// svcState is the per-Service trend memory: a window-pruned ring of
// (ready, total) samples plus the single-fire latch.
type svcState struct {
	slices map[string]sliceCounts
	uid    string // Service UID from the slice's owner ref, best-effort
	// samples is the ratio series, pruned to Config.Window, coalesced
	// to Config.SampleFloor resolution.
	samples []sample
	// fired latches after KindCapacity emits; it clears only when the
	// ratio recovers to baseline (or 1.0) — one signal per decline
	// episode, and the §7.4 clearance predicate.
	fired bool
	// baseline is the window-start ratio recorded when fired.
	baseline float64
	// recoveredSince is when a fired decline was last observed
	// recovered — the clearance observer's StableSince.
	recoveredSince time.Time
	lastSeen       time.Time
}

func (st *svcState) totals() (ready, total int) {
	for _, c := range st.slices {
		ready += c.ready
		total += c.total
	}
	return ready, total
}

// podFlapState is the per-pod readiness flip memory.
type podFlapState struct {
	namespace string
	name      string
	node      string
	ready     bool
	// flips are readiness-gate transitions (either direction), pruned
	// to Config.Window. Reset after firing (re-count to next burst).
	flips      []time.Time
	lastFlipAt time.Time
	lastSeen   time.Time
}

// Source implements sources.Source for the degradation row of §7.2.
type Source struct {
	client kubernetes.Interface
	cfg    Config
	// factory, when set via WithFactory, is the externally owned
	// shared informer factory (§6.3: one informer set serves the
	// sentinel sources and the graph). Nil = Run builds a private one.
	factory informers.SharedInformerFactory

	mu    sync.Mutex
	armed bool
	emit  func(engine.Signal)

	services map[serviceKey]*svcState
	pods     map[types.UID]*podFlapState

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
}

// New constructs the source. Zero-valued cfg fields take the shipped
// defaults.
func New(client kubernetes.Interface, cfg Config) *Source {
	return &Source{
		client:   client,
		cfg:      cfg.normalize(),
		services: make(map[serviceKey]*svcState),
		pods:     make(map[types.UID]*podFlapState),
	}
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: the informers list pods and
// endpointslices cluster-wide, so the source needs cluster RBAC (§11).
func (s *Source) Scope() sources.Scope { return sources.ScopeCluster }

// WithFactory directs Run to register its informers on an externally
// owned shared factory (§6.3). Call before Run; nil is ignored.
func (s *Source) WithFactory(f informers.SharedInformerFactory) {
	if f != nil {
		s.factory = f
	}
}

// RequiredAccess implements sources.AccessDeclarer (§11): list+watch
// on each informer target. Both grants already exist in
// deploy/12-clusterrole-watcher.yaml (pods for object-state/recovery,
// endpointslices for object-state) — this source adds NO new RBAC.
func (s *Source) RequiredAccess() []sources.Requirement {
	var reqs []sources.Requirement
	for _, r := range []struct{ group, resource string }{
		{"", "pods"},
		{"discovery.k8s.io", "endpointslices"},
	} {
		for _, verb := range []string{"list", "watch"} {
			reqs = append(reqs, sources.Requirement{Group: r.group, Resource: r.resource, Verb: verb})
		}
	}
	return reqs
}

// ClearanceObserver returns the §7.4 clearance predicate for this
// source's kinds: a capacity incident clears when the Service's ready
// ratio is back at (or above) the window-start baseline recorded at
// fire time, or at 1.0; a probe-flap incident clears when the pod is
// Ready with no flips left in the window.
func (s *Source) ClearanceObserver() engine.ClearanceObserver { return clearance{s} }

func (s *Source) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// arm enables emission — called once all caches are synced.
func (s *Source) arm() {
	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()
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

// Run implements sources.Source: starts the two informers, arms after
// caches sync, then drives the sampling tick until ctx is cancelled.
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()

	factory := s.factory
	if factory == nil {
		factory = informers.NewSharedInformerFactory(s.client, 0)
	}

	sliceH, err := factory.Discovery().V1().EndpointSlices().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.asSlice(obj, s.onSlice) },
		UpdateFunc: func(_, obj any) { s.asSlice(obj, s.onSlice) },
		DeleteFunc: func(obj any) { s.asSlice(tombstoneObj(obj), s.onSliceDelete) },
	})
	if err != nil {
		return fmt.Errorf("degradation: register endpointslice handler: %w", err)
	}
	podH, err := factory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.asPod(obj, s.onPod) },
		UpdateFunc: func(_, obj any) { s.asPod(obj, s.onPod) },
		DeleteFunc: func(obj any) { s.asPod(tombstoneObj(obj), s.onPodDelete) },
	})
	if err != nil {
		return fmt.Errorf("degradation: register pod handler: %w", err)
	}

	factory.Start(ctx.Done())
	// Shutdown blocks until every handler goroutine exits, upholding
	// the Source contract that emit is never called after Run returns.
	defer factory.Shutdown()

	// Arm-after-sync (§7.2 restart discipline): the initial LIST above
	// seeded the ratio series and flip memory silently; only from here
	// do declines count as observed.
	if !cache.WaitForCacheSync(ctx.Done(), sliceH.HasSynced, podH.HasSynced) {
		return fmt.Errorf("degradation: cache sync failed (informer stopped before initial list completed)")
	}
	s.arm()

	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.send(s.tick(s.clock()))
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

func (s *Source) asSlice(obj any, fn func(*discoveryv1.EndpointSlice)) {
	if sl, ok := obj.(*discoveryv1.EndpointSlice); ok {
		fn(sl)
	}
}

func (s *Source) asPod(obj any, fn func(*corev1.Pod)) {
	if p, ok := obj.(*corev1.Pod); ok {
		fn(p)
	}
}

// ---- Capacity: per-Service ready-ratio trend ----

// sliceCountsOf counts a slice's ready and total endpoints. A nil
// Ready condition means unknown, which consumers must interpret as
// ready (discovery/v1 API contract).
func sliceCountsOf(sl *discoveryv1.EndpointSlice) sliceCounts {
	c := sliceCounts{total: len(sl.Endpoints)}
	for _, ep := range sl.Endpoints {
		if ep.Conditions.Ready == nil || *ep.Conditions.Ready {
			c.ready++
		}
	}
	return c
}

// sliceServiceUID pulls the owning Service's UID off the slice.
func sliceServiceUID(sl *discoveryv1.EndpointSlice) string {
	for _, ref := range sl.OwnerReferences {
		if ref.Kind == "Service" {
			return string(ref.UID)
		}
	}
	return ""
}

func (s *Source) onSlice(sl *discoveryv1.EndpointSlice) {
	svcName := sl.Labels[discoveryv1.LabelServiceName]
	if svcName == "" {
		return // unmanaged slice — not an EndpointSlice-backed Service
	}
	now := s.clock()
	key := serviceKey{sl.Namespace, svcName}

	s.mu.Lock()
	st, ok := s.services[key]
	if !ok {
		st = &svcState{slices: make(map[string]sliceCounts)}
		s.services[key] = st
	}
	if st.uid == "" {
		st.uid = sliceServiceUID(sl)
	}
	st.lastSeen = now
	st.slices[sl.Name] = sliceCountsOf(sl)
	out := s.recordAndEvaluate(key, st, now)
	s.mu.Unlock()
	s.send(out)
}

func (s *Source) onSliceDelete(sl *discoveryv1.EndpointSlice) {
	svcName := sl.Labels[discoveryv1.LabelServiceName]
	if svcName == "" {
		return
	}
	now := s.clock()
	key := serviceKey{sl.Namespace, svcName}

	s.mu.Lock()
	st, ok := s.services[key]
	if !ok {
		s.mu.Unlock()
		return
	}
	delete(st.slices, sl.Name)
	var out []engine.Signal
	if len(st.slices) == 0 {
		// Last slice gone — the Service itself is (being) deleted;
		// object removal is not a capacity decline.
		delete(s.services, key)
	} else {
		st.lastSeen = now
		out = s.recordAndEvaluate(key, st, now)
	}
	s.mu.Unlock()
	s.send(out)
}

// recordAndEvaluate appends the current totals to the ratio series
// (coalescing within SampleFloor), prunes the window, and applies the
// firing predicate. Called under s.mu.
func (s *Source) recordAndEvaluate(key serviceKey, st *svcState, now time.Time) []engine.Signal {
	ready, total := st.totals()
	if n := len(st.samples); n > 0 && now.Sub(st.samples[n-1].at) < s.cfg.SampleFloor {
		// Coalesce: a burst of slice updates within the floor is ONE
		// step — the no-single-blip gate must not be defeated by the
		// EndpointSlice controller sharding one change over several
		// writes.
		st.samples[n-1] = sample{at: st.samples[n-1].at, ready: ready, total: total}
	} else {
		st.samples = append(st.samples, sample{at: now, ready: ready, total: total})
	}
	st.samples = pruneSamples(st.samples, now.Add(-s.cfg.Window))
	return s.evaluate(key, st, now)
}

// pruneSamples drops samples older than cutoff, preserving order.
func pruneSamples(in []sample, cutoff time.Time) []sample {
	out := in[:0]
	for _, sm := range in {
		if !sm.at.Before(cutoff) {
			out = append(out, sm)
		}
	}
	return out
}

// evaluate applies the package-level predicate to one service's
// series. Called under s.mu. Also maintains the recovery side of the
// single-fire latch.
func (s *Source) evaluate(key serviceKey, st *svcState, now time.Time) []engine.Signal {
	n := len(st.samples)
	if n == 0 {
		return nil
	}
	cur := st.samples[n-1]
	rn := cur.ratio()

	if st.fired {
		// Clearance side of the latch: recovered to the baseline the
		// decline was measured from, or to full capacity.
		if cur.total > 0 && (rn >= st.baseline || rn >= 1.0) {
			st.fired = false
			st.recoveredSince = now
		}
		return nil
	}
	if !s.armed || n < 3 {
		// Fewer than 3 samples cannot hold 2 downward steps.
		return nil
	}
	if cur.total == 0 || rn <= 0 {
		// Ratio reached 0 (or the service has no endpoints at all):
		// that is objectstate.endpoints_empty's critical, never ours.
		return nil
	}
	first := st.samples[0]
	r0 := first.ratio()
	if r0-rn < s.cfg.Drop {
		return nil
	}
	downSteps := 0
	for i := 1; i < n; i++ {
		if st.samples[i].ratio() < st.samples[i-1].ratio() {
			downSteps++
		}
	}
	if downSteps < s.cfg.MinDownSteps {
		return nil
	}

	st.fired = true
	st.baseline = r0
	st.recoveredSince = time.Time{}

	severity := engine.SeverityWarning
	if rn <= CriticalRatio {
		severity = engine.SeverityCritical
	}
	uid := st.uid
	if uid == "" {
		uid = "service:" + key.namespace + "/" + key.name
	}
	msg := fmt.Sprintf(
		"service ready capacity declined %d/%d → %d/%d over %s (ratio %.2f→%.2f, drop %.2f ≥ %.2f, downward steps %d ≥ %d, window %s); timeline: %s",
		first.ready, first.total, cur.ready, cur.total,
		cur.at.Sub(first.at).Truncate(time.Second),
		r0, rn, r0-rn, s.cfg.Drop, downSteps, s.cfg.MinDownSteps, s.cfg.Window,
		timeline(st.samples))
	sig := engine.Signal{
		Kind:     KindCapacity,
		Source:   engine.SourceSentinel,
		Severity: severity,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: uid, Reason: reasonOf(KindCapacity)},
			Namespace:    key.namespace,
			KindOfObject: "Service",
			Name:         key.name,
			Message:      msg,
			FirstSeen:    first.at,
			LastSeen:     now,
			Count:        1,
		},
	}
	return []engine.Signal{sig}
}

// timeline renders the per-step evidence compactly: only samples that
// changed the counts, capped to the 8 most recent steps.
func timeline(samples []sample) string {
	var steps []string
	for i, sm := range samples {
		if i > 0 && sm.ready == samples[i-1].ready && sm.total == samples[i-1].total {
			continue
		}
		steps = append(steps, fmt.Sprintf("%d/%d", sm.ready, sm.total))
	}
	if len(steps) > 8 {
		steps = append([]string{"…"}, steps[len(steps)-8:]...)
	}
	return strings.Join(steps, " → ")
}

// ---- Probe flaps: readiness-gate flips below the Unhealthy gate ----

// podReady reads the PodReady condition. ok=false when absent.
func podReady(p *corev1.Pod) (ready bool, ok bool) {
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue, true
		}
	}
	return false, false
}

func (s *Source) onPod(p *corev1.Pod) {
	ready, ok := podReady(p)
	if !ok {
		return
	}
	now := s.clock()

	s.mu.Lock()
	st, seen := s.pods[p.UID]
	if !seen {
		// First observation (initial LIST or new pod): baseline only —
		// a creation is not a flip.
		s.pods[p.UID] = &podFlapState{
			namespace: p.Namespace, name: p.Name, node: p.Spec.NodeName,
			ready: ready, lastSeen: now,
		}
		s.mu.Unlock()
		return
	}
	st.lastSeen = now
	var out []engine.Signal
	if ready != st.ready {
		st.ready = ready
		st.flips = pruneTimes(append(st.flips, now), now.Add(-s.cfg.Window))
		st.lastFlipAt = now
		if s.armed && len(st.flips) >= s.cfg.FlapCount {
			out = append(out, engine.Signal{
				Kind:     KindProbeFlap,
				Source:   engine.SourceSentinel,
				Severity: engine.SeverityWarning,
				TriageEvent: engine.TriageEvent{
					Key:          engine.EventKey{UID: string(p.UID), Reason: reasonOf(KindProbeFlap)},
					Namespace:    p.Namespace,
					KindOfObject: "Pod",
					Name:         p.Name,
					Node:         p.Spec.NodeName,
					Message: fmt.Sprintf(
						"pod readiness gate flipped %d times within %s without sustained failure (probe flapping below the Unhealthy threshold — the reactive path stays silent)",
						len(st.flips), s.cfg.Window),
					FirstSeen: st.flips[0],
					LastSeen:  now,
					Count:     1,
				},
			})
			st.flips = nil // re-count toward the next burst
		}
	}
	s.mu.Unlock()
	s.send(out)
}

func (s *Source) onPodDelete(p *corev1.Pod) {
	s.mu.Lock()
	delete(s.pods, p.UID)
	s.mu.Unlock()
}

// pruneTimes drops timestamps at or before cutoff, preserving order.
func pruneTimes(ts []time.Time, cutoff time.Time) []time.Time {
	out := ts[:0]
	for _, t := range ts {
		if t.After(cutoff) {
			out = append(out, t)
		}
	}
	return out
}

// tick is the ticker body: appends a fresh sample per service (so a
// service whose slices stopped updating still ages its window and can
// recover/clear), evaluates, and prunes TTL-expired state. Returns
// the signals to emit (sent outside the lock by the caller).
func (s *Source) tick(now time.Time) []engine.Signal {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []engine.Signal
	for key, st := range s.services {
		out = append(out, s.recordAndEvaluate(key, st, now)...)
	}
	cutoff := now.Add(-s.cfg.StateTTL)
	for key, st := range s.services {
		if st.lastSeen.Before(cutoff) {
			delete(s.services, key)
		}
	}
	for uid, st := range s.pods {
		if st.lastSeen.Before(cutoff) {
			delete(s.pods, uid)
		}
	}
	return out
}

// ---- §7.4 clearance ----

// clearance implements engine.ClearanceObserver over the source's
// live state. It judges ONLY this source's own kinds (by dedup
// reason), so it composes with the pod-scoped PodClearance observer:
// register it first and probe_flap incidents get the flap-aware
// verdict instead of the generic "pod is Ready" one (a flapping pod
// reads Ready most of the time).
type clearance struct{ s *Source }

func (c clearance) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	s := c.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.armed {
		return engine.Clearance{}, false // cannot judge before sync
	}
	switch inc.Key.Reason {
	case reasonOf(KindCapacity):
		if !strings.EqualFold(inc.Ref.KindOfObject, "Service") {
			return engine.Clearance{}, false
		}
		st, ok := s.services[serviceKey{inc.Ref.Namespace, inc.Ref.Name}]
		if !ok {
			// Service gone: incident over, nothing was fixed.
			return engine.Clearance{Cleared: true, Resolution: engine.ResolutionObjectDeleted}, true
		}
		if st.fired {
			return engine.Clearance{}, true // still degraded
		}
		return engine.Clearance{
			Cleared:     true,
			StableSince: st.recoveredSince,
			Resolution:  engine.ResolutionRecovered,
		}, true
	case reasonOf(KindProbeFlap):
		if !strings.EqualFold(inc.Ref.KindOfObject, "Pod") {
			return engine.Clearance{}, false
		}
		st, ok := s.pods[types.UID(inc.Key.UID)]
		if !ok {
			return engine.Clearance{Cleared: true, Resolution: engine.ResolutionObjectDeleted}, true
		}
		now := s.clock()
		flips := 0
		for _, t := range st.flips {
			if t.After(now.Add(-s.cfg.Window)) {
				flips++
			}
		}
		if st.ready && flips == 0 {
			return engine.Clearance{
				Cleared:     true,
				StableSince: st.lastFlipAt,
				Resolution:  engine.ResolutionRecovered,
			}, true
		}
		return engine.Clearance{}, true // still flapping (or not ready)
	}
	return engine.Clearance{}, false // not this source's incident
}
