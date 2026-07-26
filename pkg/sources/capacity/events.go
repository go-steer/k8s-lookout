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
	"unicode"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// Sub-source 1 (§10.1): cluster-autoscaler Kubernetes Events — the
// real-time trigger. The reason allow-list below is OWNED by this
// source (see the package comment for why the k8s-events source's
// default allow-list is deliberately unchanged).

// caEventKinds maps the CA event reasons this source watches to the
// signal kind each becomes. Reasons per upstream cluster-autoscaler
// (clusterstate/utils + core/scale_up.go/scale_down.go event
// emission): NotTriggerScaleUp and TriggeredScaleUp land on the
// pending Pod; the ScaleDown family lands on the Node being drained.
var caEventKinds = map[string]string{
	"NotTriggerScaleUp": KindPending,
	"TriggeredScaleUp":  KindScaleUp,
	"ScaleDown":         KindScaleDown,
	"ScaleDownEmpty":    KindScaleDown,
	"ScaleDownFailed":   KindScaleDown,
}

// eventSeverity is the §7.7 default severity per CA event reason.
// NotTriggerScaleUp warns (a pod is stuck and the autoscaler said
// why); TriggeredScaleUp and routine scale-downs are info context;
// ScaleDownFailed warns (a node the autoscaler wants gone is stuck).
func eventSeverity(reason string) engine.Severity {
	switch reason {
	case "NotTriggerScaleUp", "ScaleDownFailed":
		return engine.SeverityWarning
	default:
		return engine.SeverityInfo
	}
}

// onEvent is the Event informer handler: filter to the CA allow-list,
// drop pre-arm history (package comment), convert, emit.
func (s *Source) onEvent(ev *corev1.Event) {
	kind, ok := caEventKinds[ev.Reason]
	if !ok {
		return
	}
	s.mu.Lock()
	armed := s.armed
	s.mu.Unlock()
	if !armed {
		return
	}
	s.send(s.eventSignal(kind, ev))
}

// eventSignal converts an allow-listed CA event to its Signal. The
// dedup/fingerprint reason is the kind suffix; the raw event reason
// stays visible in the message. For NotTriggerScaleUp the message is
// normalized around the parsed per-nodegroup rejection reasons.
func (s *Source) eventSignal(kind string, ev *corev1.Event) engine.Signal {
	msg := ev.Message
	if kind == KindPending {
		if rej := parseNoScaleUpReasons(ev.Message); len(rej) > 0 {
			msg = fmt.Sprintf("autoscaler did not trigger scale-up for this pod; nodegroup rejections: %s (raw: %s)", formatRejections(rej), ev.Message)
		}
	} else if ev.Reason != "" {
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
		Severity: eventSeverity(ev.Reason),
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

// rejection is one parsed NotTriggerScaleUp rejection: how many
// nodegroups declined, and the autoscaler's verbatim reason.
type rejection struct {
	Count  int
	Reason string
}

// parseNoScaleUpReasons parses the real NotTriggerScaleUp message
// shapes the cluster-autoscaler emits:
//
//	pod didn't trigger scale-up: 2 max node group size reached, 1 not ready for scale-up
//	pod didn't trigger scale-up (it wouldn't fit if a new node is added): 3 Insufficient cpu
//	pod didn't trigger scale-up: 1 node(s) had taint {dedicated: gpu}, that the pod didn't tolerate
//
// i.e. an optional parenthetical after the fixed prefix, then a
// comma-separated list of "<nodegroup-count> <reason>" entries. A
// reason may itself contain ", " (the taint form above), so the
// splitter only starts a new entry when the segment leads with a
// count; anything else is a continuation of the previous reason.
// Messages that don't match (including the bare old-CA form with no
// reason list) return nil and the raw message is kept as-is.
func parseNoScaleUpReasons(msg string) []rejection {
	const prefix = "pod didn't trigger scale-up"
	msg = strings.TrimSpace(msg)
	if !strings.HasPrefix(msg, prefix) {
		return nil
	}
	rest := strings.TrimSpace(strings.TrimPrefix(msg, prefix))
	if strings.HasPrefix(rest, "(") {
		if end := strings.Index(rest, ")"); end >= 0 {
			rest = strings.TrimSpace(rest[end+1:])
		}
	}
	rest = strings.TrimSpace(strings.TrimPrefix(rest, ":"))
	if rest == "" {
		return nil
	}
	var out []rejection
	for _, seg := range strings.Split(rest, ",") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		if n, reason, ok := leadingCount(seg); ok {
			out = append(out, rejection{Count: n, Reason: reason})
		} else if len(out) > 0 {
			// Continuation of a reason that contains a comma.
			out[len(out)-1].Reason += ", " + seg
		} else {
			return nil // no leading-count entry at all: not the list form
		}
	}
	return out
}

// leadingCount splits "2 max node group size reached" into (2, rest).
func leadingCount(seg string) (int, string, bool) {
	i := 0
	for i < len(seg) && unicode.IsDigit(rune(seg[i])) {
		i++
	}
	if i == 0 || i == len(seg) || seg[i] != ' ' {
		return 0, "", false
	}
	n := 0
	for _, r := range seg[:i] {
		n = n*10 + int(r-'0')
	}
	return n, strings.TrimSpace(seg[i:]), true
}

// formatRejections renders parsed rejections compactly:
// `2× "max node group size reached", 1× "not ready for scale-up"`.
func formatRejections(rej []rejection) string {
	parts := make([]string, 0, len(rej))
	for _, r := range rej {
		parts = append(parts, fmt.Sprintf("%d× %q", r.Count, r.Reason))
	}
	return strings.Join(parts, ", ")
}

// truncate caps message payloads, same bound as the k8s-events
// source.
func truncate(msg string) string {
	const max = 2048
	if len(msg) <= max {
		return msg
	}
	return msg[:max] + "... [truncated by capacity source]"
}
