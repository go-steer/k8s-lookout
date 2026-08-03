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

package capacity

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources/saturation"
)

// Sub-source 5: the cluster bin-packing forecast (issue #131, roadmap
// B.3). Every poll tick samples, per scheduling domain, the ratio
//
//	sum(requests of pods BOUND to the domain's schedulable nodes)
//	--------------------------------------------------------------
//	sum(allocatable of the domain's schedulable nodes)
//
// for CPU (millicores) and memory (bytes) independently, fits a
// least-squares line over Config.ForecastWindow (the same exported
// regression + ETA seams saturation/quota/tokenburn share —
// saturation.LeastSquaresSlope, saturation.ETAFromSeconds with its
// #80 overflow clamp), and emits capacity.cluster_forecast when the
// domain projects to ratio 1.0 — full — within ForecastWarnETA
// (warning) or ForecastCritETA (critical).
//
// Scheduling domain per node: the GKE nodepool label when present,
// else the stable zone topology label, else the legacy-beta zone
// label (same fallback order as pkg/checks/state's volume checks),
// else the whole cluster as one domain.
//
// Deliberate accounting choices (kept simple on purpose):
//
//   - Only SCHEDULABLE nodes count: Ready condition True and not
//     spec.unschedulable. A cordoned or NotReady node contributes
//     neither allocatable nor its pods' requests — both sides of the
//     ratio describe the pool the scheduler can actually pack into.
//   - Only BOUND, non-terminal pods count: spec.nodeName set and
//     phase not Succeeded/Failed. Pending pods are the reactive
//     sub-sources' business; this one is about the headroom left
//     before they appear.
//   - Requests are the sum of app containers' requests. Init
//     containers (transient) and pod overhead are ignored — a
//     conservative simplification that slightly undercounts.
//
// Gates, severity, and hysteresis are saturation's geometry
// (saturation.go record()): no fit below ForecastMinSamples or a
// span under ForecastWindow/2; positive slope only; the latch
// releases when the ETA recedes beyond 2×ForecastWarnETA or the
// slope stays non-positive for ForecastWindow/2, and the §7.4
// clearance (Clearance below) reports the symptom absent on the same
// state.

// Resource dimensions of the bin-packing ratio, tracked as
// independent series per domain. Both dimensions share the domain's
// dedup key (UID "nodegroup:<domain>", reason "cluster_forecast") —
// a domain filling up is ONE incident however many dimensions
// observe it; whichever fires first opens the session and the other
// attaches as a followup.
const (
	forecastResourceCPU    = "cpu"
	forecastResourceMemory = "memory"
)

// forecastResources enumerates the dimensions (iteration order for
// deterministic emission).
var forecastResources = [...]string{forecastResourceCPU, forecastResourceMemory}

// reasonClusterForecast is the dedup/fingerprint reason (kind
// suffix, self-canonical under engine.CanonicalReason).
var reasonClusterForecast = strings.TrimPrefix(KindClusterForecast, kindPrefix)

// Node scheduling-domain labels: GKE nodepool first (the unit the
// autoscaler scales), then the stable zone topology label, then the
// legacy-beta zone key older clusters still stamp (the same
// stable/legacy pair pkg/checks/state's volume checks read).
const (
	nodepoolLabel = "cloud.google.com/gke-nodepool"
	zoneLabel     = "topology.kubernetes.io/zone"
	zoneLabelBeta = "failure-domain.beta.kubernetes.io/zone"
)

// clusterDomain is the fallback scheduling domain when a node
// carries none of the domain labels.
const clusterDomain = "cluster"

// domainKey identifies one (scheduling domain, resource) ratio
// series.
type domainKey struct {
	domain   string
	resource string
}

// ratioSample is one (time, ratio) observation; 1.0 = full.
type ratioSample struct {
	t time.Time
	v float64
}

// domainSeries is the per-(domain, resource) ring buffer +
// hysteresis state — the same shape as saturation's series, local on
// purpose (the exported seams are the regression + ETA math; the
// loop is re-implemented per source).
type domainSeries struct {
	samples []ratioSample
	// firedSeverity is the hysteresis latch: "" (none) | warning |
	// critical. Released on recede / sustained non-positive slope.
	firedSeverity engine.Severity
	// nonPosSince is when the fitted slope last turned non-positive
	// (zero while positive).
	nonPosSince time.Time
	// recededSince is when the ETA last receded beyond
	// forecastClearETA (zero while urgent or unknown).
	recededSince time.Time
}

// nodeDomain returns the node's scheduling domain.
func nodeDomain(n *corev1.Node) string {
	if v := n.Labels[nodepoolLabel]; v != "" {
		return v
	}
	if v := n.Labels[zoneLabel]; v != "" {
		return v
	}
	if v := n.Labels[zoneLabelBeta]; v != "" {
		return v
	}
	return clusterDomain
}

// nodeSchedulable reports whether the node counts toward the
// domain's packable capacity: Ready and not cordoned (package-doc
// accounting choice).
func nodeSchedulable(n *corev1.Node) bool {
	if n.Spec.Unschedulable {
		return false
	}
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// pollForecast runs one bin-packing sample from the informer caches.
// No-op until Run wired the listers (unit tests drive sampleCluster
// directly).
func (s *Source) pollForecast(now time.Time) {
	s.mu.Lock()
	podLister, nodeLister := s.podLister, s.nodeLister
	s.mu.Unlock()
	if podLister == nil || nodeLister == nil {
		return
	}
	pods, err := podLister.List(labels.Everything())
	if err != nil {
		s.logPrintf("capacity: cluster-forecast pod list failed (retry in %s): %v", s.cfg.PollInterval, err)
		return
	}
	nodes, err := nodeLister.List(labels.Everything())
	if err != nil {
		s.logPrintf("capacity: cluster-forecast node list failed (retry in %s): %v", s.cfg.PollInterval, err)
		return
	}
	for _, sig := range s.sampleCluster(pods, nodes, now) {
		s.send(sig)
	}
}

// domainSums accumulates one domain's requests and allocatable per
// dimension.
type domainSums struct {
	reqCPU, reqMem     float64
	allocCPU, allocMem float64
}

// sampleCluster records one bin-packing observation per (domain,
// resource) and returns the forecasts to emit. Domains absent from
// this snapshot (nodepool scaled away) drop their series, so the
// §7.4 clearance reports object_deleted.
func (s *Source) sampleCluster(pods []*corev1.Pod, nodes []*corev1.Node, now time.Time) []engine.Signal {
	domainOf := make(map[string]string, len(nodes)) // node name → domain (schedulable nodes only)
	sums := make(map[string]*domainSums)
	for _, n := range nodes {
		if !nodeSchedulable(n) {
			continue
		}
		d := nodeDomain(n)
		domainOf[n.Name] = d
		agg := sums[d]
		if agg == nil {
			agg = &domainSums{}
			sums[d] = agg
		}
		agg.allocCPU += float64(n.Status.Allocatable.Cpu().MilliValue())
		agg.allocMem += float64(n.Status.Allocatable.Memory().Value())
	}
	for _, p := range pods {
		if p.Spec.NodeName == "" || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		agg, ok := sums[domainOf[p.Spec.NodeName]]
		if !ok {
			continue // node unknown or excluded: neither its capacity nor its load counts
		}
		for i := range p.Spec.Containers {
			req := p.Spec.Containers[i].Resources.Requests
			agg.reqCPU += float64(req.Cpu().MilliValue())
			agg.reqMem += float64(req.Memory().Value())
		}
	}

	var out []engine.Signal
	s.mu.Lock()
	seen := make(map[domainKey]bool, 2*len(sums))
	for d, agg := range sums {
		for _, res := range forecastResources {
			var ratio, alloc float64
			switch res {
			case forecastResourceCPU:
				ratio, alloc = agg.reqCPU, agg.allocCPU
			case forecastResourceMemory:
				ratio, alloc = agg.reqMem, agg.allocMem
			}
			if alloc <= 0 {
				continue // no allocatable in this dimension: no ratio to trend
			}
			ratio /= alloc
			key := domainKey{domain: d, resource: res}
			seen[key] = true
			if sig := s.recordDomain(key, ratio, now); sig != nil {
				out = append(out, *sig)
			}
		}
	}
	for key := range s.domains {
		if !seen[key] {
			delete(s.domains, key)
		}
	}
	s.forecastSampled = true
	s.mu.Unlock()
	return out
}

// recordDomain appends one ratio observation and evaluates the
// forecast + the hysteresis latch (saturation's record() geometry
// with the fixed limit 1.0). Called under s.mu; returns a signal to
// emit, if any.
func (s *Source) recordDomain(key domainKey, ratio float64, now time.Time) *engine.Signal {
	ser := s.domains[key]
	if ser == nil {
		ser = &domainSeries{}
		s.domains[key] = ser
	}
	ser.samples = append(ser.samples, ratioSample{t: now, v: ratio})
	cutoff := now.Add(-s.cfg.ForecastWindow)
	kept := ser.samples[:0]
	for _, p := range ser.samples {
		if p.t.After(cutoff) {
			kept = append(kept, p)
		}
	}
	ser.samples = kept

	// The no-forecast gates (§13): insufficient window (count or
	// span), non-positive slope.
	n := len(ser.samples)
	if n < s.cfg.ForecastMinSamples || ser.samples[n-1].t.Sub(ser.samples[0].t) < s.cfg.forecastMinSpan() {
		return nil
	}
	times := make([]time.Time, n)
	values := make([]float64, n)
	for i, p := range ser.samples {
		times[i], values[i] = p.t, p.v
	}
	slope := saturation.LeastSquaresSlope(times, values) // ratio units per second
	current := ser.samples[n-1].v
	headroom := 1.0 - current
	var (
		eta    time.Duration
		hasETA bool
	)
	if slope > 0 {
		if headroom <= 0 {
			eta, hasETA = 0, true // already at/over allocatable (requests can exceed it transiently)
		} else {
			// hasETA=false here is the overflow clamp (issue #80): a
			// projection beyond the representable horizon is no
			// projection, same as a non-positive slope.
			eta, hasETA = saturation.ETAFromSeconds(headroom / slope)
		}
	}
	if !hasETA {
		if ser.nonPosSince.IsZero() {
			ser.nonPosSince = now
		}
		ser.recededSince = time.Time{}
		if ser.firedSeverity != "" && now.Sub(ser.nonPosSince) >= s.cfg.forecastReobserve() {
			ser.firedSeverity = "" // latch released; clearance agrees
		}
		return nil
	}
	ser.nonPosSince = time.Time{}
	if eta > s.cfg.forecastClearETA() {
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
	case eta <= s.cfg.ForecastCritETA:
		sev = engine.SeverityCritical
	case eta <= s.cfg.ForecastWarnETA:
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
	sig := s.newClusterForecast(key, sev, current, eta, now)
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

// newClusterForecast composes the capacity.cluster_forecast Signal
// with the §8 forecast attachment. UID is the nodegroup convention
// the decision sub-source established ("nodegroup:<domain>"), so a
// later stockout/scaleup_gap on the same nodegroup lands in the same
// dedup family's session. Called under s.mu.
func (s *Source) newClusterForecast(key domainKey, sev engine.Severity, ratio float64, eta time.Duration, now time.Time) engine.Signal {
	label := forecastWindowLabel(s.cfg.ForecastWindow)
	msg := fmt.Sprintf(
		"scheduling domain %s: %s requests at %.1f%% of allocatable, full in ~%s at the observed trend (linear-%s window)",
		key.domain, key.resource, ratio*100, formatForecastETA(eta), label)
	return engine.Signal{
		Kind:     KindClusterForecast,
		Source:   engine.SourceSentinel,
		Severity: sev,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "nodegroup:" + key.domain, Reason: reasonClusterForecast},
			KindOfObject: "NodeGroup",
			Name:         key.domain,
			Message:      truncate(msg),
			FirstSeen:    now,
			LastSeen:     now,
			Count:        1,
		},
		Forecast: &engine.Forecast{
			ETA:             now.Add(eta),
			ConfidenceBasis: "linear-" + label + "-window",
		},
	}
}

// forecastWindowLabel renders the window for the §8 confidence basis
// ("3h" for whole hours, minutes otherwise — quota's "7d" analog on
// cluster timescales).
func forecastWindowLabel(w time.Duration) string {
	if h := w / time.Hour; h >= 1 && w == h*time.Hour {
		return fmt.Sprintf("%dh", int(h))
	}
	return fmt.Sprintf("%dm", int(w.Minutes()))
}

// formatForecastETA renders the projection on cluster timescales
// (hours + minutes, minutes under an hour).
func formatForecastETA(eta time.Duration) string {
	if eta >= time.Hour {
		h := int(eta.Hours())
		m := int(eta.Minutes()) % 60
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh%dm", h, m)
	}
	return eta.Truncate(time.Minute).String()
}

// ---- §7.4 clearance ----

// ClearanceObserver returns the §7.4 clearance predicate for
// capacity.cluster_forecast incidents. Register it BEFORE any
// pod-scoped observer (internal/watch wiring order); it claims ONLY
// cluster_forecast incidents, so the reactive capacity kinds keep
// their existing (uncovered or family-joined) behavior.
func (s *Source) ClearanceObserver() engine.ClearanceObserver { return s }

// Clearance implements engine.ClearanceObserver for
// capacity.cluster_forecast incidents. It CLAIMS every
// cluster_forecast incident (ok=true) even while symptomatic.
//
// Cleared when (package comment, hysteresis contract):
//   - the domain has no series left after a sample cycle (nodepool
//     scaled away / relabeled) → object_deleted;
//   - every remaining dimension's projection is absent: the ETA
//     receded beyond 2×ForecastWarnETA, or the slope stayed
//     non-positive for a full re-observation period
//     (ForecastWindow/2) → recovered, stable since the LAST
//     dimension's recede/turn (both dimensions ride one incident, so
//     the domain is only clear once the last one is).
func (s *Source) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	if engine.CanonicalReason(inc.Key.Reason) != reasonClusterForecast {
		return engine.Clearance{}, false
	}
	domain := strings.TrimPrefix(inc.Key.UID, "nodegroup:")
	now := s.clock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.forecastSampled {
		return engine.Clearance{}, false // no sample cycle yet — cannot judge
	}
	var (
		exists bool
		stable time.Time
	)
	for _, res := range forecastResources {
		ser, ok := s.domains[domainKey{domain: domain, resource: res}]
		if !ok {
			continue
		}
		exists = true
		cleared, since := s.domainSeriesClear(ser, now)
		if !cleared {
			return engine.Clearance{Cleared: false}, true
		}
		if since.After(stable) {
			stable = since
		}
	}
	if !exists {
		return engine.Clearance{Cleared: true, Resolution: engine.ResolutionObjectDeleted}, true
	}
	return engine.Clearance{Cleared: true, StableSince: stable, Resolution: engine.ResolutionRecovered}, true
}

// domainSeriesClear judges one dimension's series: absent-symptom
// when the ETA receded past the clear threshold or the slope stayed
// non-positive for the re-observation period (saturation's per-series
// clearance verdict). Called under s.mu.
func (s *Source) domainSeriesClear(ser *domainSeries, now time.Time) (bool, time.Time) {
	if !ser.recededSince.IsZero() {
		return true, ser.recededSince
	}
	if !ser.nonPosSince.IsZero() && now.Sub(ser.nonPosSince) >= s.cfg.forecastReobserve() {
		return true, ser.nonPosSince
	}
	return false, time.Time{}
}
