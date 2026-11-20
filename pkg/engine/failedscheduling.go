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

package engine

import (
	"log"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultFailedSchedulingMinCount is the default per-Event repeat count
	// required before an unruled FailedScheduling event passes the gate (#835).
	// 1 disables the count backstop.
	DefaultFailedSchedulingMinCount = 5
	// DefaultFailedSchedulingMinSpan is the default minimum wall-clock span
	// (LastSeen - FirstSeen) required before an unruled FailedScheduling event
	// passes the count backstop (#835). 0 disables the span backstop.
	DefaultFailedSchedulingMinSpan = 2 * time.Minute
	// DefaultScaleUpHold is the default duration a TriggeredScaleUp verdict
	// holds FailedScheduling events for the pod (matching cluster-autoscaler's
	// default max-node-provision-time, 15m). 0 disables the hold.
	DefaultScaleUpHold = 15 * time.Minute
	// FailedSchedulingStaleAfter is the sighting age past which a
	// FailedScheduling event is held as stale (the scheduler flushes its
	// unschedulable queue every ~5m, so an active stuck pod receives fresher
	// sightings; stale holds protect against replay of already-resolved pods).
	FailedSchedulingStaleAfter = 15 * time.Minute
	// NoNodesAvailableMessage is kube-scheduler's exact ErrNoNodesAvailable
	// text, emitted only when the cluster currently has zero nodes. Used by
	// the Phase 2 Autopilot scale-to-zero hold (#835).
	NoNodesAvailableMessage = "no nodes available to schedule pods"

	// failedSchedulingLogEvery is how long one pod's FailedScheduling gate
	// decision is suppressed from re-logging while unchanged (#835 / kube-agents #4528).
	failedSchedulingLogEvery = FailedSchedulingStaleAfter
	// maxFailedSchedulingLogEntries caps the per-pod decision log memo so a
	// huge Pending fleet cannot grow memory unboundedly. When full, live
	// entries are never evicted; uncached pods log on each sighting until the
	// next expired sweep makes room.
	maxFailedSchedulingLogEntries = 50_000
	// failedSchedulingLogSweepEvery bounds expired-entry sweeps when the log
	// memo is full to at most once per minute.
	failedSchedulingLogSweepEvery = time.Minute
)

// ScaleUpVerdict is the cluster autoscaler's latest per-pod ruling, aligned
// with pkg/leeway/scaleup.go (#532, #835).
type ScaleUpVerdict uint8

const (
	// ScaleUpUnheard means the autoscaler has recorded no verdict for the pod.
	ScaleUpUnheard ScaleUpVerdict = iota
	// ScaleUpProvisioning means TriggeredScaleUp is the latest verdict.
	ScaleUpProvisioning
	// ScaleUpDeclined means NotTriggerScaleUp or FailedScaleUp is the latest
	// verdict.
	ScaleUpDeclined
)

// String renders a verdict for logs and tests.
func (v ScaleUpVerdict) String() string {
	switch v {
	case ScaleUpProvisioning:
		return "provisioning"
	case ScaleUpDeclined:
		return "declined"
	default:
		return "unheard"
	}
}

// ScaleUpMark is a pod's latest autoscaler verdict and the clamped event
// timestamp it was recorded at.
type ScaleUpMark struct {
	Verdict ScaleUpVerdict
	At      time.Time
}

// ScaleUpVerdictLookup reads per-pod autoscaler verdicts recorded by the
// capacity source. Kept as a narrow interface so pkg/engine never imports
// pkg/sources/capacity or pkg/cloud.
type ScaleUpVerdictLookup interface {
	LookupScaleUpVerdict(namespace, uid string) ScaleUpMark
}

// PodSchedulingState classifies a Pod from the capacity source's synced
// podLister for the fail-open pre-check of the FailedScheduling gate (#835).
type PodSchedulingState uint8

const (
	// PodSchedulingUnknown means the lookup cannot judge (e.g. cache not
	// synced yet or lister unavailable). Treated fail-open: never holds
	// failedscheduling_not_pending without positive evidence.
	PodSchedulingUnknown PodSchedulingState = iota
	// PodSchedulingNotPending is positive evidence from the synced pod cache
	// that the pod is no longer pending: bound (spec.nodeName != ""),
	// phase != Pending, PodScheduled=True, absent from the synced cache, or
	// UID mismatch. Held as failedscheduling_not_pending.
	PodSchedulingNotPending
	// PodSchedulingOtherFailure means the pod is Pending with
	// PodScheduled=False and Reason != Unschedulable (e.g. SchedulerError).
	// Neither capacity.pending-aged nor autoscaler verdicts cover these pods,
	// so the gate passes them immediately (fail open).
	PodSchedulingOtherFailure
	// PodSchedulingUnschedulable means the pod is Pending with
	// PodScheduled=False Reason=Unschedulable, OR Pending with no
	// PodScheduled=False condition yet (informer cache lag where the Event
	// arrived before the condition patch). Continues into the verdict/count/span
	// gate.
	PodSchedulingUnschedulable
)

// PodStateLookup performs an O(1) point lookup on the capacity source's synced
// pod cache. Kept as a narrow interface so pkg/engine never imports
// pkg/sources/capacity or k8s.io/client-go.
type PodStateLookup interface {
	LookupPodSchedulingState(namespace, name, uid string) PodSchedulingState
}

// FailedSchedulingConfig configures the FailedScheduling verdict + count/span
// gate (#835). All fields are zero-safe: Enabled=false leaves the gate off
// (restoring event-#1 pass-through, e.g. when the capacity source is disabled).
type FailedSchedulingConfig struct {
	// Enabled activates the gate when at least one knob below is active.
	// Must be false whenever the capacity source is not enabled.
	Enabled bool
	// MinCount is the count backstop for unruled FailedScheduling events.
	// Default 5; 1 disables the count backstop.
	MinCount int
	// MinSpan is the minimum LastSeen - FirstSeen duration for unruled
	// FailedScheduling events past MinCount. Default 2m; 0 disables.
	MinSpan time.Duration
	// ScaleUpHold holds FailedScheduling while TriggeredScaleUp is within
	// this duration of the event's LastSeen. Default 15m; 0 disables.
	ScaleUpHold time.Duration
	// AutopilotScaleToZeroHold enables Phase 2 holding of exact
	// NoNodesAvailableMessage on Autopilot clusters with no autoscaler
	// verdict. Default true.
	AutopilotScaleToZeroHold bool
	// Autopilot reports whether pkg/cloud.Provider's ClusterConfig identified
	// this cluster as GKE Autopilot. False when no provider is configured or
	// ClusterConfig is unavailable (fail open).
	Autopilot bool
	// PodState is the capacity source's synced podLister point lookup.
	PodState PodStateLookup
	// Verdicts is the capacity source's per-pod autoscaler verdict memo.
	Verdicts ScaleUpVerdictLookup
	// Logf overrides log.Printf for per-pod gate decision logging (tests).
	Logf func(format string, args ...any)
	// Now overrides time.Now (tests).
	Now func() time.Time
}

// DefaultFailedSchedulingConfig returns the shipped knob defaults with
// Enabled=false (callers enable it explicitly once capacity is wired).
func DefaultFailedSchedulingConfig() FailedSchedulingConfig {
	return FailedSchedulingConfig{
		MinCount:                 DefaultFailedSchedulingMinCount,
		MinSpan:                  DefaultFailedSchedulingMinSpan,
		ScaleUpHold:              DefaultScaleUpHold,
		AutopilotScaleToZeroHold: true,
	}
}

// Active reports whether the FailedScheduling gate should run.
// Disabling all four operator knobs (MinCount<=1, MinSpan<=0, ScaleUpHold<=0,
// AutopilotScaleToZeroHold=false) disables the entire gate — including
// failedscheduling_not_pending and failedscheduling_stale — so the pre-#835
// event-#1 behavior stays reachable by flag (docs/cli-stability-policy.md).
func (c FailedSchedulingConfig) Active() bool {
	if !c.Enabled {
		return false
	}
	return c.MinCount > 1 || c.MinSpan > 0 || c.ScaleUpHold > 0 || c.AutopilotScaleToZeroHold
}

// WithFailedScheduling returns a copy of FilterConfig with the FailedScheduling
// gate configuration attached.
func (c FilterConfig) WithFailedScheduling(fs FailedSchedulingConfig) FilterConfig {
	c.failedScheduling = fs
	return c
}

// SetFailedScheduling updates the filter's FailedScheduling gate configuration
// during sentinel startup (before sources start dispatching).
func (f *Filter) SetFailedScheduling(fs FailedSchedulingConfig) {
	if f == nil {
		return
	}
	f.cfg.failedScheduling = fs
}

func (f *Filter) clock() time.Time {
	if f.cfg.failedScheduling.Now != nil {
		return f.cfg.failedScheduling.Now()
	}
	return time.Now()
}

func (f *Filter) logf(format string, args ...any) {
	if f.cfg.failedScheduling.Logf != nil {
		f.cfg.failedScheduling.Logf(format, args...)
		return
	}
	log.Printf(format, args...)
}

type notedLine struct {
	format string
	args   []any
}

type fsLogEntry struct {
	gate string
	at   time.Time
}

type fsLogMemo struct {
	mu        sync.Mutex
	entries   map[string]fsLogEntry
	lastSweep time.Time
}

func (m *fsLogMemo) shouldLog(key, gate string, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = make(map[string]fsLogEntry)
	}
	if prev, ok := m.entries[key]; ok {
		if now.Sub(prev.at) <= failedSchedulingLogEvery {
			if prev.gate == gate {
				return false
			}
		} else {
			delete(m.entries, key)
		}
	}
	if _, exists := m.entries[key]; !exists && len(m.entries) >= maxFailedSchedulingLogEntries {
		if now.Sub(m.lastSweep) >= failedSchedulingLogSweepEvery {
			for k, e := range m.entries {
				if now.Sub(e.at) > failedSchedulingLogEvery {
					delete(m.entries, k)
				}
			}
			m.lastSweep = now
		}
		if len(m.entries) >= maxFailedSchedulingLogEntries {
			// Memo full even after sweeping expired entries: never evict live
			// entries and suppress overflow logging to prevent log floods;
			// lookout_events_filtered_total still counts every sighting.
			return false
		}
	}
	m.entries[key] = fsLogEntry{gate: gate, at: now}
	return true
}

func (f *Filter) logFailedSchedulingDecision(sig Signal, gate string, now time.Time, lines []notedLine) {
	if len(lines) == 0 {
		return
	}
	if sig.Key.UID != "" && sig.Namespace != "" {
		key := strings.Join([]string{sig.Project, sig.Region, sig.Zone, sig.Cluster, sig.Namespace, sig.Key.UID}, "\x00")
		if !f.fsLog.shouldLog(key, gate, now) {
			return
		}
	}
	for _, line := range lines {
		f.logf(line.format, line.args...)
	}
}

// decideFailedScheduling evaluates the #835 FailedScheduling gate:
//
//  1. Fail-open pod-state point lookup on capacity's synced podLister:
//     - bound / phase != Pending / PodScheduled=True / absent / UID mismatch
//     → hold failedscheduling_not_pending (blocks late finishSeries flushes).
//     - Pending + PodScheduled=False with Reason != Unschedulable
//     (e.g. SchedulerError) → pass immediately (fail open).
//     - Pending + Unschedulable, or Pending with no PodScheduled=False yet
//     (cache lag) → continue.
//  2. Stale sighting (> 15m old) → hold failedscheduling_stale.
//  3. Autoscaler verdict lookup:
//     - NotTriggerScaleUp / FailedScaleUp sighted before this event (1s
//     truncation: sig.LastSeen.Truncate(1s) > verdict.At.Truncate(1s)) →
//     pass immediately.
//     - TriggeredScaleUp within --scaleup-hold of sig.LastSeen → hold
//     scaleup_hold.
//  4. Phase 2 Autopilot scale-to-zero hold: Autopilot && exact trimmed message
//     "no nodes available to schedule pods" && no verdict → hold
//     autopilot_scale_to_zero.
//  5. Count backstop (0 fails open) → hold failedscheduling_min_count.
//  6. Span backstop (zero timestamps fail open) → hold
//     failedscheduling_min_span.
func (f *Filter) decideFailedScheduling(sig Signal) (accepted bool, gate string) {
	cfg := f.cfg.failedScheduling
	now := f.clock()
	var lines []notedLine
	note := func(format string, args ...any) {
		lines = append(lines, notedLine{format: format, args: args})
	}
	defer func() { f.logFailedSchedulingDecision(sig, gate, now, lines) }()

	// U0: Fail-open pod-state point lookup on capacity's synced podLister.
	if cfg.PodState != nil && (sig.KindOfObject == "" || sig.KindOfObject == "Pod") {
		switch cfg.PodState.LookupPodSchedulingState(sig.Namespace, sig.Name, sig.Key.UID) {
		case PodSchedulingNotPending:
			note("held %s pod=%s/%s (count=%d): pod is no longer Pending in synced pod cache (bound, deleted, or UID changed)",
				sig.Key.Reason, sig.Namespace, sig.Name, sig.Count)
			return false, GateFailedSchedulingNotPending
		case PodSchedulingOtherFailure:
			note("pass %s pod=%s/%s (count=%d): pod is Pending with non-Unschedulable PodScheduled=False condition (e.g. SchedulerError); failing open",
				sig.Key.Reason, sig.Namespace, sig.Name, sig.Count)
			return true, ""
		case PodSchedulingUnknown:
			// Capacity pod cache is not yet synced/armed (so capacity.pending-aged
			// is not sweeping yet) or lister errored: fail open rather than
			// holding without the pending-aged safety net.
			note("pass %s pod=%s/%s (count=%d): capacity pod cache unavailable or not synced yet; failing open",
				sig.Key.Reason, sig.Namespace, sig.Name, sig.Count)
			return true, ""
		}
	}

	// B: Stale sighting check (> 15m old). Zero LastSeen fails open.
	if !sig.LastSeen.IsZero() && now.Sub(sig.LastSeen) > FailedSchedulingStaleAfter {
		note("held %s pod=%s/%s (count=%d): last sighting %s ago (> %s), pod sighting is stale",
			sig.Key.Reason, sig.Namespace, sig.Name, sig.Count, now.Sub(sig.LastSeen).Round(time.Second), FailedSchedulingStaleAfter)
		return false, GateFailedSchedulingStale
	}

	// C & D: Per-pod autoscaler verdict lookup.
	var mark ScaleUpMark
	if cfg.Verdicts != nil {
		mark = cfg.Verdicts.LookupScaleUpVerdict(sig.Namespace, sig.Key.UID)
	}
	switch mark.Verdict {
	case ScaleUpDeclined:
		// Truncate both timestamps to 1s so microsecond events.k8s.io/v1
		// sightings in the verdict's own second are not mistaken for a later
		// post-verdict retry (kube-agents/filter.go:659-670, 736).
		if !sig.LastSeen.IsZero() && !sig.LastSeen.Truncate(time.Second).After(mark.At.Truncate(time.Second)) {
			note("%s pod=%s/%s (count=%d): autoscaler declined %s ago, %s after this sighting; scheduler has not retried since, falling back to count/span",
				sig.Key.Reason, sig.Namespace, sig.Name, sig.Count, now.Sub(mark.At).Round(time.Second), mark.At.Sub(sig.LastSeen).Round(time.Second))
			break
		}
		note("pass %s pod=%s/%s (count=%d): autoscaler declined scale-up %s ago, short-circuiting count/span backstop",
			sig.Key.Reason, sig.Namespace, sig.Name, sig.Count, now.Sub(mark.At).Round(time.Second))
		return true, ""
	case ScaleUpProvisioning:
		if cfg.ScaleUpHold > 0 {
			sighting := sig.LastSeen
			if sighting.IsZero() {
				sighting = now
			}
			sinceMark := sighting.Sub(mark.At)
			if sinceMark <= cfg.ScaleUpHold {
				note("held %s pod=%s/%s (count=%d): autoscaler triggered scale-up %s ago (%s before sighting), holding up to %s",
					sig.Key.Reason, sig.Namespace, sig.Name, sig.Count, now.Sub(mark.At).Round(time.Second), max(sinceMark, 0).Round(time.Second), cfg.ScaleUpHold)
				return false, GateScaleUpHold
			}
			note("%s pod=%s/%s (count=%d): scale-up triggered %s ago was %s old at sighting, past %s hold; falling back to count/span",
				sig.Key.Reason, sig.Namespace, sig.Name, sig.Count, now.Sub(mark.At).Round(time.Second), sinceMark.Round(time.Second), cfg.ScaleUpHold)
		}
	}

	// E: Phase 2 Autopilot scale-to-zero hold.
	if cfg.AutopilotScaleToZeroHold && cfg.Autopilot && mark.Verdict == ScaleUpUnheard &&
		strings.TrimSpace(sig.Message) == NoNodesAvailableMessage {
		note("held %s pod=%s/%s (count=%d): %q on Autopilot cluster %q with no autoscaler verdict — scaled to zero nodes",
			sig.Key.Reason, sig.Namespace, sig.Name, sig.Count, strings.TrimSpace(sig.Message), sig.Cluster)
		return false, GateAutopilotScaleToZero
	}

	// F: Count backstop (Count <= 0 fails open per #833).
	if cfg.MinCount > 1 && belowMinCount(sig.Count, cfg.MinCount) {
		note("held %s pod=%s/%s (count=%d < %d, verdict=%s)",
			sig.Key.Reason, sig.Namespace, sig.Name, sig.Count, cfg.MinCount, mark.Verdict)
		return false, GateFailedSchedulingMinCount
	}

	// G: Span backstop (zero timestamps fail open).
	if cfg.MinSpan > 0 && !eventSpanReached(sig.TriageEvent, cfg.MinSpan) {
		note("held %s pod=%s/%s (count=%d, lasted %s < %s, verdict=%s)",
			sig.Key.Reason, sig.Namespace, sig.Name, sig.Count, sig.LastSeen.Sub(sig.FirstSeen).Round(time.Second), cfg.MinSpan, mark.Verdict)
		return false, GateFailedSchedulingMinSpan
	}

	return true, ""
}

// belowMinCount reports whether count is positively below min. Non-positive
// counts fail open so emitters that omit Event.Count are never silenced (#833).
func belowMinCount(count, min int) bool {
	return count > 0 && count < min
}

// eventSpanReached reports whether the event's own sightings span at least
// minSpan. Missing timestamps fail open.
func eventSpanReached(ev TriageEvent, minSpan time.Duration) bool {
	if ev.FirstSeen.IsZero() || ev.LastSeen.IsZero() {
		return true
	}
	return ev.LastSeen.Sub(ev.FirstSeen) >= minSpan
}
