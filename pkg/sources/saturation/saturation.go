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

// Package saturation is the saturation signal source (DESIGN.md §7.2
// row 4): trend leading indicators from continuously sampled resource
// usage — "pod hits memory limit in ~14 min" (slope → ETA), "PVC full
// in ~3 h". This is where v2 top-analyzer's regression math lives: a
// resident process owns the time series a one-shot binary never had.
//
// Dimensions:
//
//   - container CPU and memory usage via metrics.k8s.io, judged
//     against the container's resource LIMITS (no limit → no ceiling
//     → no forecast);
//   - PVC usage via the kubelet stats summary (nodes/proxy get). The
//     endpoint is standard kubelet, portable per §2 — but if it is
//     unreachable (managed platforms that block node proxying), the
//     source logs ONE loud line and keeps running with the PVC
//     dimension skipped; it quietly resumes if the endpoint comes
//     back. metrics.k8s.io is different: it is the source's primary
//     dimension, so an initial fetch failure is a loud startup error
//     per §11 (install metrics-server or disable the source); later
//     transient failures skip the cycle with a throttled log.
//
// Forecast math (§13 trend testing): per (object, resource) the
// source keeps a ring buffer of samples pruned to Config.Window
// (default 90m), fits a least-squares line, and projects
// ETA = (limit - current) / slope. It emits `saturation.forecast`
// with Forecast{ETA, "linear-90m-window"} ONLY when the projection is
// trustworthy AND urgent:
//
//   - NO forecast when the buffer holds < Config.MinSamples samples
//     or spans < Window/2 (insufficient window);
//   - NO forecast when the slope is non-positive;
//   - NO forecast when the object has no limit;
//   - severity warning when ETA < Config.WarnETA (default 60m),
//     critical when ETA < Config.CritETA (default 15m); an ETA beyond
//     WarnETA is recorded, never emitted.
//
// Hysteresis (documented per the M3 scope): each target latches the
// highest severity it fired. The same severity never re-fires;
// escalation (warning → critical) fires once more. The latch releases
// — and the §7.4 clearance reports the symptom absent — when the ETA
// recedes beyond 2×WarnETA, or when the slope stays non-positive for
// a full re-observation period (Window/2 — the same span the fit
// needed to be trusted in the first place). After release, a fresh
// approach to the threshold fires again.
package saturation

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the stable source name (§7.2 table) used in the signal
// schema and the `--sources` flag.
const Name = "saturation"

// KindForecast is the one kind this source emits (§7.3). APPEND-ONLY.
const KindForecast = "saturation.forecast"

// Resource dimensions. The dedup/fingerprint reason is
// "forecast_<resource>" so the same object's memory and CPU forecasts
// are distinct incidents (and distinct §8 fingerprint classes for
// fleet rollup).
const (
	ResourceCPU    = "cpu"
	ResourceMemory = "memory"
	ResourcePVC    = "pvc"
)

// reasonPrefix builds the per-dimension dedup reason.
const reasonPrefix = "forecast_"

// ReasonFor returns the dedup/fingerprint reason for a resource
// dimension (e.g. "forecast_memory"). Self-canonical under
// engine.CanonicalReason.
func ReasonFor(resource string) string { return reasonPrefix + resource }

// resourceOf inverts ReasonFor; ok=false for non-saturation reasons.
func resourceOf(reason string) (string, bool) {
	r, found := strings.CutPrefix(reason, reasonPrefix)
	if !found {
		return "", false
	}
	switch r {
	case ResourceCPU, ResourceMemory, ResourcePVC:
		return r, true
	}
	return "", false
}

// ContainerSample is one container usage observation from the pod
// fetcher. Units: CPU in millicores, memory in bytes. Limit 0 means
// no limit configured (no forecast possible).
type ContainerSample struct {
	Namespace string
	Pod       string
	PodUID    string
	Container string
	Node      string
	Resource  string // ResourceCPU | ResourceMemory
	Used      float64
	Limit     float64
}

// VolumeSample is one PVC usage observation from the volume fetcher.
type VolumeSample struct {
	Namespace     string
	ClaimName     string
	UsedBytes     float64
	CapacityBytes float64
}

// PodUsageFetcher supplies container CPU/memory usage. The real
// implementation reads metrics.k8s.io + pod specs (limits); tests
// substitute synthetic series with known slopes (§13).
type PodUsageFetcher interface {
	FetchPodUsage(ctx context.Context) ([]ContainerSample, error)
}

// VolumeUsageFetcher supplies PVC usage. The real implementation
// reads each node's kubelet stats summary via nodes/proxy.
type VolumeUsageFetcher interface {
	FetchVolumeUsage(ctx context.Context) ([]VolumeSample, error)
}

// Config are the source's sampling and threshold knobs. Zero values
// take the defaults.
type Config struct {
	// Interval between samples (`--saturation-interval`). Default 30s.
	Interval time.Duration
	// Window is the regression window (`--saturation-window`): samples
	// older than this are pruned; a forecast needs a span of at least
	// Window/2. Default 90m — the §8 "linear-90m-window" basis.
	Window time.Duration
	// WarnETA (`--saturation-warn`): forecasts with ETA below it emit
	// at severity warning. Default 60m.
	WarnETA time.Duration
	// CritETA: forecasts with ETA below it emit at severity critical.
	// Default 15m (deliberately not a flag in this change — the
	// warn/crit ratio is part of the hysteresis geometry).
	CritETA time.Duration
	// MinSamples is the minimum buffer size for a fit. Default 8.
	MinSamples int
	// StaleAfter drops a target's series when no fresh sample arrives
	// within it (pod gone, PVC deleted) — the clearance then reports
	// object_deleted. Default 10m.
	StaleAfter time.Duration
}

// DefaultConfig returns the shipped knobs.
func DefaultConfig() Config {
	return Config{
		Interval:   30 * time.Second,
		Window:     90 * time.Minute,
		WarnETA:    60 * time.Minute,
		CritETA:    15 * time.Minute,
		MinSamples: 8,
		StaleAfter: 10 * time.Minute,
	}
}

// normalize fills zero fields with defaults.
func (c Config) normalize() Config {
	d := DefaultConfig()
	if c.Interval <= 0 {
		c.Interval = d.Interval
	}
	if c.Window <= 0 {
		c.Window = d.Window
	}
	if c.WarnETA <= 0 {
		c.WarnETA = d.WarnETA
	}
	if c.CritETA <= 0 || c.CritETA >= c.WarnETA {
		c.CritETA = d.CritETA
	}
	if c.MinSamples <= 0 {
		c.MinSamples = d.MinSamples
	}
	if c.StaleAfter <= 0 {
		c.StaleAfter = d.StaleAfter
	}
	return c
}

// minSpan is the smallest sample span a fit is trusted over.
func (c Config) minSpan() time.Duration { return c.Window / 2 }

// reobserve is the §7.4 clearance re-observation period: how long the
// slope must stay non-positive before the symptom counts as absent.
func (c Config) reobserve() time.Duration { return c.Window / 2 }

// clearETA is the recede threshold: an ETA beyond it releases the
// hysteresis latch and clears the incident.
func (c Config) clearETA() time.Duration { return 2 * c.WarnETA }

// targetKey identifies one (object, resource) series. uid carries the
// container for pod dimensions ("<pod-uid>/<container>") and the
// synthesized "pvc:<ns>/<name>" for PVCs — exactly the Signal UID, so
// the dedup key and the clearance lookup agree.
type targetKey struct {
	uid      string
	resource string
}

// sample is one (time, value) observation.
type sample struct {
	t time.Time
	v float64
}

// series is the per-target ring buffer + hysteresis state.
type series struct {
	// identity for signal composition.
	kindOfObject string
	namespace    string
	name         string
	container    string
	node         string

	samples []sample
	limit   float64

	// firedSeverity is the hysteresis latch: "" (none) | warning |
	// critical. Released on recede / sustained non-positive slope.
	firedSeverity engine.Severity
	// nonPosSince is when the fitted slope last turned non-positive
	// (zero while positive).
	nonPosSince time.Time
	// recededSince is when the ETA last receded beyond clearETA
	// (zero while urgent or unknown).
	recededSince time.Time
	lastSample   time.Time
}

// Source implements sources.Source (and engine.ClearanceObserver) for
// the saturation row of §7.2.
type Source struct {
	cfg     Config
	pods    PodUsageFetcher
	volumes VolumeUsageFetcher

	mu     sync.Mutex
	emit   func(engine.Signal)
	series map[targetKey]*series
	// fetched flips true after the first successful pod-usage fetch;
	// Clearance declines to judge before it (an empty series map at
	// startup must not read as "everything deleted").
	fetched bool
	// kubeletDown tracks the PVC dimension's one-time loud log.
	kubeletDown bool
	// podFetchFailing throttles the transient metrics.k8s.io log.
	podFetchFailing bool

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
	// logf overrides log.Printf for testing. nil = log.Printf.
	logf func(format string, args ...any)
}

// New constructs the source. volumes may be nil to disable the PVC
// dimension outright (tests; the shipped wiring always passes one).
func New(cfg Config, pods PodUsageFetcher, volumes VolumeUsageFetcher) *Source {
	return &Source{
		cfg:     cfg.normalize(),
		pods:    pods,
		volumes: volumes,
		series:  make(map[targetKey]*series),
	}
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: the kubelet stats summary rides
// nodes/proxy, a cluster-scoped subresource — namespace-tier
// deployments get the loud §11 startup failure.
func (s *Source) Scope() sources.Scope { return sources.ScopeCluster }

// ClearanceObserver returns the §7.4 clearance predicate for
// saturation.forecast incidents. Register it BEFORE any pod-scoped
// observer: forecast incidents carry KindOfObject=Pod, and a generic
// pod-readiness judge would wrongly clear a leaking-but-Ready pod.
func (s *Source) ClearanceObserver() engine.ClearanceObserver { return s }

// RequiredAccess implements sources.AccessDeclarer (§11): the metrics
// API for the container dimensions, core pods list for limits, nodes
// list + nodes/proxy get for the kubelet stats summary. Matches
// deploy/12-clusterrole-watcher.yaml.
func (s *Source) RequiredAccess() []sources.Requirement {
	return []sources.Requirement{
		{Group: "metrics.k8s.io", Resource: "pods", Verb: "get"},
		{Group: "metrics.k8s.io", Resource: "pods", Verb: "list"},
		{Resource: "pods", Verb: "list"},
		{Resource: "nodes", Verb: "list"},
		{Resource: "nodes", Subresource: "proxy", Verb: "get"},
	}
}

func (s *Source) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *Source) printf(format string, args ...any) {
	if s.logf != nil {
		s.logf(format, args...)
		return
	}
	log.Printf(format, args...)
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
		return // not running (unit tests drive sampleOnce directly)
	}
	for _, sig := range sigs {
		emit(sig)
	}
}

// Run implements sources.Source: verifies metrics.k8s.io answers
// (fail loudly at startup, §11 — an unreachable metrics API would
// otherwise be a silently empty trend source), then drives the
// sampling loop until ctx is cancelled.
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()

	// First sample synchronously: a dead metrics API at startup is a
	// config/platform error the operator must see, not a silent gap.
	first, err := s.pods.FetchPodUsage(ctx)
	if err != nil {
		return fmt.Errorf("saturation: metrics.k8s.io unavailable: %w (install metrics-server, or disable the source)", err)
	}
	now := s.clock()
	s.send(s.ingestPods(first, now))
	s.send(s.sampleVolumes(ctx, now))
	s.finishCycle(now)

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.send(s.sampleOnce(ctx, s.clock()))
		}
	}
}

// sampleOnce runs one full sampling cycle and returns the signals to
// emit. Exported to the package's tests, which drive it with a fake
// clock and synthetic fetchers.
func (s *Source) sampleOnce(ctx context.Context, now time.Time) []engine.Signal {
	var out []engine.Signal
	pods, err := s.pods.FetchPodUsage(ctx)
	if err != nil {
		// Transient (startup already proved the API exists): skip the
		// cycle, log the edge only.
		s.mu.Lock()
		firstFailure := !s.podFetchFailing
		s.podFetchFailing = true
		s.mu.Unlock()
		if firstFailure {
			s.printf("saturation: metrics.k8s.io fetch failed (%v) — skipping container samples until it recovers", err)
		}
	} else {
		s.mu.Lock()
		if s.podFetchFailing {
			s.podFetchFailing = false
			s.mu.Unlock()
			s.printf("saturation: metrics.k8s.io recovered")
		} else {
			s.mu.Unlock()
		}
		out = append(out, s.ingestPods(pods, now)...)
	}
	out = append(out, s.sampleVolumes(ctx, now)...)
	s.finishCycle(now)
	return out
}

// sampleVolumes fetches and ingests the PVC dimension, honoring the
// one-time-loud-log unreachable contract.
func (s *Source) sampleVolumes(ctx context.Context, now time.Time) []engine.Signal {
	if s.volumes == nil {
		return nil
	}
	vols, err := s.volumes.FetchVolumeUsage(ctx)
	if err != nil {
		s.mu.Lock()
		firstFailure := !s.kubeletDown
		s.kubeletDown = true
		s.mu.Unlock()
		if firstFailure {
			s.printf("saturation: kubelet stats summary unreachable (%v) — SKIPPING the PVC dimension; container CPU/memory forecasting continues (portable posture per DESIGN.md §2; the dimension resumes automatically if nodes/proxy becomes reachable)", err)
		}
		return nil
	}
	s.mu.Lock()
	wasDown := s.kubeletDown
	s.kubeletDown = false
	s.mu.Unlock()
	if wasDown {
		s.printf("saturation: kubelet stats summary reachable again — PVC dimension resumed")
	}
	return s.ingestVolumes(vols, now)
}

// finishCycle marks the first successful cycle and prunes stale
// series (targets gone).
func (s *Source) finishCycle(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fetched = true
	for key, ser := range s.series {
		if now.Sub(ser.lastSample) > s.cfg.StaleAfter {
			delete(s.series, key)
		}
	}
}

// ingestPods records container samples and returns any forecasts.
func (s *Source) ingestPods(samples []ContainerSample, now time.Time) []engine.Signal {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []engine.Signal
	for _, cs := range samples {
		key := targetKey{uid: cs.PodUID + "/" + cs.Container, resource: cs.Resource}
		ser := s.seriesFor(key)
		ser.kindOfObject = "Pod"
		ser.namespace, ser.name, ser.container, ser.node = cs.Namespace, cs.Pod, cs.Container, cs.Node
		if sig := s.record(key, ser, cs.Used, cs.Limit, now); sig != nil {
			out = append(out, *sig)
		}
	}
	return out
}

// ingestVolumes records PVC samples and returns any forecasts.
func (s *Source) ingestVolumes(samples []VolumeSample, now time.Time) []engine.Signal {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []engine.Signal
	for _, vs := range samples {
		key := targetKey{uid: "pvc:" + vs.Namespace + "/" + vs.ClaimName, resource: ResourcePVC}
		ser := s.seriesFor(key)
		ser.kindOfObject = "PersistentVolumeClaim"
		ser.namespace, ser.name = vs.Namespace, vs.ClaimName
		if sig := s.record(key, ser, vs.UsedBytes, vs.CapacityBytes, now); sig != nil {
			out = append(out, *sig)
		}
	}
	return out
}

// seriesFor returns (creating if needed) the target's series. Called
// under s.mu.
func (s *Source) seriesFor(key targetKey) *series {
	ser, ok := s.series[key]
	if !ok {
		ser = &series{}
		s.series[key] = ser
	}
	return ser
}

// record appends one observation and evaluates the forecast + the
// hysteresis latch. Called under s.mu; returns a signal to emit, if
// any.
func (s *Source) record(key targetKey, ser *series, value, limit float64, now time.Time) *engine.Signal {
	ser.limit = limit
	ser.lastSample = now
	ser.samples = append(ser.samples, sample{t: now, v: value})
	cutoff := now.Add(-s.cfg.Window)
	kept := ser.samples[:0]
	for _, p := range ser.samples {
		if p.t.After(cutoff) {
			kept = append(kept, p)
		}
	}
	ser.samples = kept

	// The three no-forecast gates (§13): no limit, insufficient
	// window (count or span), non-positive slope.
	if limit <= 0 {
		return nil
	}
	n := len(ser.samples)
	if n < s.cfg.MinSamples || ser.samples[n-1].t.Sub(ser.samples[0].t) < s.cfg.minSpan() {
		return nil
	}
	slope := leastSquaresSlope(ser.samples) // units per second
	if slope <= 0 {
		if ser.nonPosSince.IsZero() {
			ser.nonPosSince = now
		}
		ser.recededSince = time.Time{}
		if ser.firedSeverity != "" && now.Sub(ser.nonPosSince) >= s.cfg.reobserve() {
			ser.firedSeverity = "" // latch released; clearance agrees
		}
		return nil
	}
	ser.nonPosSince = time.Time{}

	current := ser.samples[n-1].v
	headroom := ser.limit - current
	var eta time.Duration
	if headroom <= 0 {
		eta = 0 // already at/over the limit
	} else {
		eta = time.Duration(headroom / slope * float64(time.Second))
	}
	if eta > s.cfg.clearETA() {
		// Receded beyond 2×warn: release the latch so a future
		// approach re-fires; clearance reports the symptom absent.
		if ser.recededSince.IsZero() {
			ser.recededSince = now
		}
		ser.firedSeverity = ""
		return nil
	}
	ser.recededSince = time.Time{}

	var sev engine.Severity
	switch {
	case eta < s.cfg.CritETA:
		sev = engine.SeverityCritical
	case eta < s.cfg.WarnETA:
		sev = engine.SeverityWarning
	default:
		// Between warn and 2×warn: no threshold crossed. The latch
		// HOLDS here (hysteresis band) — an ETA oscillating around
		// the warn line neither re-fires nor clears.
		return nil
	}
	if severityRank(sev) <= severityRank(ser.firedSeverity) {
		return nil // same severity never re-fires; only escalation does
	}
	ser.firedSeverity = sev
	sig := s.newForecast(key, ser, sev, current, slope, eta, now)
	return &sig
}

// severityRank orders the hysteresis latch: none < warning < critical.
func severityRank(sev engine.Severity) int {
	switch sev {
	case engine.SeverityCritical:
		return 2
	case engine.SeverityWarning:
		return 1
	}
	return 0
}

// leastSquaresSlope fits value = a + b*t over the samples and returns
// b in units per second. The fit itself lives in the exported
// LeastSquaresSlope (regression.go) — the §10.2 seam the quota
// source reuses.
func leastSquaresSlope(pts []sample) float64 {
	times := make([]time.Time, len(pts))
	values := make([]float64, len(pts))
	for i, p := range pts {
		times[i], values[i] = p.t, p.v
	}
	return LeastSquaresSlope(times, values)
}

// newForecast composes the saturation.forecast Signal with the §8
// forecast attachment and the evidence fields (current, limit,
// slope_per_min). Called under s.mu.
func (s *Source) newForecast(key targetKey, ser *series, sev engine.Severity, current, slope float64, eta time.Duration, now time.Time) engine.Signal {
	slopePerMin := slope * 60
	msg := fmt.Sprintf(
		"%s saturation forecast for %s: current=%s limit=%s slope_per_min=%s — limit reached in ~%s at the observed trend (%d samples over %s)",
		key.resource, ser.name,
		formatValue(key.resource, current), formatValue(key.resource, ser.limit),
		formatValue(key.resource, slopePerMin),
		eta.Truncate(time.Second),
		len(ser.samples), s.cfg.Window)
	return engine.Signal{
		Kind:     KindForecast,
		Source:   engine.SourceSentinel,
		Severity: sev,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: key.uid, Reason: ReasonFor(key.resource)},
			Namespace:    ser.namespace,
			KindOfObject: ser.kindOfObject,
			Name:         ser.name,
			Container:    ser.container,
			Node:         ser.node,
			Message:      msg,
			FirstSeen:    now,
			LastSeen:     now,
			Count:        1,
		},
		Forecast: &engine.Forecast{
			ETA:             now.Add(eta),
			ConfidenceBasis: fmt.Sprintf("linear-%dm-window", int(s.cfg.Window.Minutes())),
		},
	}
}

// formatValue renders a value in the dimension's natural unit:
// millicores for CPU, IEC bytes for memory/PVC.
func formatValue(resource string, v float64) string {
	if resource == ResourceCPU {
		return fmt.Sprintf("%.0fm", v)
	}
	abs := v
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= 1<<30:
		return fmt.Sprintf("%.1fGiB", v/(1<<30))
	case abs >= 1<<20:
		return fmt.Sprintf("%.1fMiB", v/(1<<20))
	case abs >= 1<<10:
		return fmt.Sprintf("%.1fKiB", v/(1<<10))
	}
	return fmt.Sprintf("%.0fB", v)
}

// ---- §7.4 clearance ----

// Clearance implements engine.ClearanceObserver for
// saturation.forecast incidents. It CLAIMS every forecast_* incident
// (ok=true) even while symptomatic, so a generic pod observer never
// judges them — a leaking pod is Ready right up to the OOM kill.
//
// Cleared when (package comment, hysteresis contract):
//   - the target's series is gone after a successful cycle (pod/PVC
//     deleted, samples stopped) → object_deleted;
//   - the ETA receded beyond 2×WarnETA → recovered, stable since the
//     recede;
//   - the slope stayed non-positive for a full re-observation period
//     (Window/2) → recovered, stable since the slope turned.
func (s *Source) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	resource, ok := resourceOf(engine.CanonicalReason(inc.Key.Reason))
	if !ok {
		return engine.Clearance{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.fetched {
		return engine.Clearance{}, false // no cycle yet — cannot judge
	}
	ser, exists := s.series[targetKey{uid: inc.Key.UID, resource: resource}]
	if !exists {
		return engine.Clearance{Cleared: true, Resolution: engine.ResolutionObjectDeleted}, true
	}
	if !ser.recededSince.IsZero() {
		return engine.Clearance{Cleared: true, StableSince: ser.recededSince, Resolution: engine.ResolutionRecovered}, true
	}
	if !ser.nonPosSince.IsZero() && s.clock().Sub(ser.nonPosSince) >= s.cfg.reobserve() {
		return engine.Clearance{Cleared: true, StableSince: ser.nonPosSince, Resolution: engine.ResolutionRecovered}, true
	}
	return engine.Clearance{Cleared: false}, true
}
