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
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/go-steer/leeway/pkg/leeway"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

const (
	// ScaleUpReporter is the Event reporter name cluster-autoscaler and GKE
	// Node Auto-Provisioning (Autopilot/NAP) stamp on TriggeredScaleUp,
	// NotTriggerScaleUp, and FailedScaleUp events (source.component on the
	// legacy recorder, falling back to reportingController). Verified live on
	// GKE 1.37 Standard (autoscale-arcade, shipmind-stage) and Autopilot
	// measurements (#835 item (f)).
	//
	// Security note: source.component / reportingController is author-written,
	// not authenticated by kube-apiserver. Filtering on it stops accidental
	// collisions and casual wrong-reporter events; keying on ev.Namespace
	// (and rejecting ev.InvolvedObject.Namespace != ev.Namespace) confines any
	// forged event to the namespace where its author holds create-events RBAC,
	// and capacity.pending-aged (which reads Pod status conditions, never
	// Events) bounds any same-namespace forged mute at max(15m, --pending-age).
	ScaleUpReporter = "cluster-autoscaler"

	// defaultScaleUpVerdictCap bounds per-cluster verdict entries (#835).
	defaultScaleUpVerdictCap = 4096
	// defaultScaleUpVerdictTTL is the floor TTL when Config.VerdictTTL is unset:
	// DefaultScaleUpHold (15m) + FailedSchedulingStaleAfter (15m) = 30m.
	defaultScaleUpVerdictTTL = engine.DefaultScaleUpHold + engine.FailedSchedulingStaleAfter

	// rejectedVerdictLogEvery rate-limits per-(namespace, reporter, reason,
	// cause) rejected-verdict log lines so a tenant flooding forged Events
	// cannot flood sentinel logs.
	rejectedVerdictLogEvery = 15 * time.Minute
	maxRejectedLogEntries   = 1024

	verdictKeySep = "/"
)

func verdictToLeewayReason(v engine.ScaleUpVerdict) string {
	switch v {
	case engine.ScaleUpProvisioning:
		return leeway.ReasonTriggeredScaleUp
	case engine.ScaleUpDeclined:
		return leeway.ReasonNotTriggerScaleUp
	default:
		return ""
	}
}

func leewayToEngineVerdict(v leeway.ScaleUpVerdict) engine.ScaleUpVerdict {
	switch v {
	case leeway.ScaleUpProvisioning:
		return engine.ScaleUpProvisioning
	case leeway.ScaleUpDeclined:
		return engine.ScaleUpDeclined
	default:
		return engine.ScaleUpUnheard
	}
}

func eventReporter(ev *corev1.Event) string {
	if ev.Source.Component != "" {
		return ev.Source.Component
	}
	return ev.ReportingController
}

func scaleUpEventTime(ev *corev1.Event) time.Time {
	at := ev.LastTimestamp.Time
	if ev.Series != nil && ev.Series.LastObservedTime.After(at) {
		at = ev.Series.LastObservedTime.Time
	}
	if at.IsZero() {
		at = ev.EventTime.Time
	}
	if at.IsZero() {
		at = ev.CreationTimestamp.Time
	}
	return at
}

func verdictKey(namespace, uid string) string {
	if namespace == "" || uid == "" {
		return ""
	}
	return namespace + verdictKeySep + uid
}

func verdictNamespace(key string) string {
	ns, _, _ := strings.Cut(key, verdictKeySep)
	return ns
}

// rejectedLogMemo bounds rejected-verdict log volume to at most one line per
// (namespace, reporter, reason, cause) per 15m (capped at 1024 live entries;
// overflow suppresses logging while the metric counter still counts every
// distinct event).
type rejectedLogMemo struct {
	mu      sync.Mutex
	entries map[string]time.Time
}

func (m *rejectedLogMemo) allow(key string, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = make(map[string]time.Time)
	}
	if last, ok := m.entries[key]; ok {
		if now.Sub(last) <= rejectedVerdictLogEvery {
			return false
		}
		delete(m.entries, key)
	}
	if len(m.entries) >= maxRejectedLogEntries {
		for k, at := range m.entries {
			if now.Sub(at) > rejectedVerdictLogEvery {
				delete(m.entries, k)
			}
		}
		if len(m.entries) >= maxRejectedLogEntries {
			return false
		}
	}
	m.entries[key] = now
	return true
}

type verdictMemo struct {
	mu      sync.Mutex
	entries map[string]engine.ScaleUpMark
	ttl     time.Duration
	max     int
	clock   func() time.Time
}

func newVerdictMemo(ttl time.Duration, maxEntries int, clock func() time.Time) *verdictMemo {
	if ttl <= 0 {
		ttl = defaultScaleUpVerdictTTL
	}
	if maxEntries <= 0 {
		maxEntries = defaultScaleUpVerdictCap
	}
	return &verdictMemo{
		entries: make(map[string]engine.ScaleUpMark),
		ttl:     ttl,
		max:     maxEntries,
		clock:   clock,
	}
}

func (m *verdictMemo) now() time.Time {
	if m.clock != nil {
		return m.clock()
	}
	return time.Now()
}

// Record stores an autoscaler verdict for (namespace, uid) at event timestamp
// at, clamping zero or future stamps to now. Verdict precedence — including
// FailedScaleUp as declined and same-second trigger-wins ties — delegates
// directly to leeway.LatestScaleUp over 1s-truncated timestamps so the two
// subsystems cannot drift (#532, #835).
func (m *verdictMemo) Record(namespace, uid string, reason string, at time.Time) {
	key := verdictKey(namespace, uid)
	if m == nil || key == "" || !leeway.IsScaleUpReason(reason) {
		return
	}
	now := m.now()
	if at.IsZero() || at.After(now) {
		at = now
	}
	verdict := leewayToEngineVerdict(leeway.LatestScaleUp([]leeway.AutoscalerEvent{{Reason: reason, At: at}}, "", time.Time{}))
	if verdict == engine.ScaleUpUnheard {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if prev, ok := m.lookupLocked(key, now); ok {
		prevSec := prev.At.Truncate(time.Second)
		atSec := at.Truncate(time.Second)
		if prevSec.After(atSec) {
			return
		}
		if prevSec.Equal(atSec) {
			winner := leewayToEngineVerdict(leeway.LatestScaleUp([]leeway.AutoscalerEvent{
				{Reason: verdictToLeewayReason(prev.Verdict), At: prevSec},
				{Reason: reason, At: atSec},
			}, "", time.Time{}))
			if winner == prev.Verdict && (prev.Verdict != verdict || prev.At.After(at)) {
				return
			}
			verdict = winner
		}
	} else {
		m.evictIfFullLocked(now)
	}
	m.entries[key] = engine.ScaleUpMark{Verdict: verdict, At: at}
}

func (m *verdictMemo) Lookup(namespace, uid string) engine.ScaleUpMark {
	key := verdictKey(namespace, uid)
	if m == nil || key == "" {
		return engine.ScaleUpMark{}
	}
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	mark, ok := m.lookupLocked(key, now)
	if !ok {
		return engine.ScaleUpMark{}
	}
	return mark
}

func (m *verdictMemo) Snapshot() map[string]engine.ScaleUpMark {
	if m == nil {
		return nil
	}
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]engine.ScaleUpMark, len(m.entries))
	for k, v := range m.entries {
		if now.Sub(v.At) > m.ttl {
			delete(m.entries, k)
			continue
		}
		out[k] = v
	}
	return out
}

func (m *verdictMemo) lookupLocked(key string, now time.Time) (engine.ScaleUpMark, bool) {
	mark, ok := m.entries[key]
	if !ok {
		return engine.ScaleUpMark{}, false
	}
	if now.Sub(mark.At) > m.ttl {
		delete(m.entries, key)
		return engine.ScaleUpMark{}, false
	}
	return mark, true
}

func (m *verdictMemo) evictIfFullLocked(now time.Time) {
	if len(m.entries) < m.max {
		return
	}
	for k, v := range m.entries {
		if now.Sub(v.At) > m.ttl {
			delete(m.entries, k)
		}
	}
	if len(m.entries) < m.max {
		return
	}
	delete(m.entries, m.evicteeLocked())
}

func (m *verdictMemo) evicteeLocked() string {
	type nsOldest struct {
		key   string
		at    time.Time
		count int
	}
	byNS := make(map[string]*nsOldest)
	for k, v := range m.entries {
		ns := verdictNamespace(k)
		g, ok := byNS[ns]
		if !ok {
			byNS[ns] = &nsOldest{key: k, at: v.At, count: 1}
			continue
		}
		g.count++
		if v.At.Before(g.at) || (v.At.Equal(g.at) && k < g.key) {
			g.key, g.at = k, v.At
		}
	}
	var chosenNS string
	var chosen *nsOldest
	for ns, g := range byNS {
		if chosen == nil ||
			g.count > chosen.count ||
			(g.count == chosen.count && g.at.Before(chosen.at)) ||
			(g.count == chosen.count && g.at.Equal(chosen.at) && ns < chosenNS) {
			chosenNS, chosen = ns, g
		}
	}
	return chosen.key
}

func (s *Source) rejectVerdict(ev *corev1.Event, reporter, cause, format string, args ...any) {
	s.rejectedVerdicts.Add(1)
	s.mu.Lock()
	hook := s.onVerdictRejected
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	logKey := strings.Join([]string{ev.Namespace, reporter, ev.Reason, cause}, "\x00")
	if s.rejectedLogs.allow(logKey, s.clock()) {
		s.logPrintf(format, args...)
	}
}

func (s *Source) recordScaleUpVerdict(ev *corev1.Event) {
	if !leeway.IsScaleUpReason(ev.Reason) {
		return
	}
	if ev.InvolvedObject.Kind != "Pod" {
		return
	}
	reporter := eventReporter(ev)
	if reporter != ScaleUpReporter {
		s.rejectVerdict(ev, reporter, "reporter",
			"capacity: ignoring %s verdict for pod %s/%s (uid=%s) in event namespace %q from unexpected reporter %q (want %q)",
			ev.Reason, ev.InvolvedObject.Namespace, ev.InvolvedObject.Name, ev.InvolvedObject.UID, ev.Namespace, reporter, ScaleUpReporter)
		return
	}
	// ev.Namespace (metadata.namespace) is the only RBAC-authenticated
	// coordinate: for events.k8s.io/v1 (eventTime set), involvedObject.namespace
	// is author-written and not forced to match ev.Namespace. Reject any
	// cross-namespace claim and key exclusively on ev.Namespace so a tenant
	// cannot forge or evict verdicts for another namespace's pods (#835).
	if ev.Namespace == "" || ev.InvolvedObject.Namespace != ev.Namespace {
		s.rejectVerdict(ev, reporter, "cross_namespace",
			"capacity: ignoring %s verdict in namespace %q claiming cross-namespace pod %s/%s (uid=%s)",
			ev.Reason, ev.Namespace, ev.InvolvedObject.Namespace, ev.InvolvedObject.Name, ev.InvolvedObject.UID)
		return
	}
	s.verdicts.Record(ev.Namespace, string(ev.InvolvedObject.UID), ev.Reason, scaleUpEventTime(ev))
}

// LookupScaleUpVerdict implements engine.ScaleUpVerdictLookup (O(1) lookup by
// pod namespace and UID).
func (s *Source) LookupScaleUpVerdict(namespace, uid string) engine.ScaleUpMark {
	if s == nil {
		return engine.ScaleUpMark{}
	}
	return s.verdicts.Lookup(namespace, uid)
}

// ScaleUpVerdicts returns a snapshot copy of all live per-pod autoscaler
// verdicts keyed by "namespace/uid" — the verdict sibling of Unschedulables().
func (s *Source) ScaleUpVerdicts() map[string]engine.ScaleUpMark {
	if s == nil {
		return nil
	}
	return s.verdicts.Snapshot()
}

// RejectedVerdicts returns the number of autoscaler verdict events ignored
// because their reporter was not ScaleUpReporter ("cluster-autoscaler") or
// because involvedObject.namespace did not match metadata.namespace.
func (s *Source) RejectedVerdicts() int64 {
	if s == nil {
		return 0
	}
	return s.rejectedVerdicts.Load()
}

// WithRejectedVerdictHook registers an optional callback invoked whenever a
// verdict event is rejected (wired to Prometheus metrics by internal/watch).
// Call before Run.
func (s *Source) WithRejectedVerdictHook(fn func()) {
	s.mu.Lock()
	s.onVerdictRejected = fn
	s.mu.Unlock()
}

// LookupPodSchedulingState implements engine.PodStateLookup via an O(1) point
// lookup on the capacity source's synced podLister (#835).
func (s *Source) LookupPodSchedulingState(namespace, name, uid string) engine.PodSchedulingState {
	if s == nil || namespace == "" || name == "" {
		return engine.PodSchedulingUnknown
	}
	s.mu.Lock()
	lister := s.podLister
	armed := s.armed
	s.mu.Unlock()
	if lister == nil || !armed {
		return engine.PodSchedulingUnknown
	}
	pod, err := lister.Pods(namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return engine.PodSchedulingNotPending
		}
		return engine.PodSchedulingUnknown
	}
	if uid != "" && string(pod.UID) != uid {
		return engine.PodSchedulingNotPending
	}
	if pod.Spec.NodeName != "" || pod.Status.Phase != corev1.PodPending {
		return engine.PodSchedulingNotPending
	}
	for i := range pod.Status.Conditions {
		c := &pod.Status.Conditions[i]
		if c.Type != corev1.PodScheduled {
			continue
		}
		if c.Status == corev1.ConditionTrue {
			return engine.PodSchedulingNotPending
		}
		if c.Status == corev1.ConditionFalse {
			if c.Reason == corev1.PodReasonUnschedulable {
				return engine.PodSchedulingUnschedulable
			}
			return engine.PodSchedulingOtherFailure
		}
	}
	return engine.PodSchedulingUnschedulable
}
