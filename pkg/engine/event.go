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

// Package engine implements the watch-path signal pipeline (DESIGN.md
// §7): the Signal type carried between stages (§8 schema), the frozen
// cross-cluster Fingerprint, and the reason/namespace filter and
// rolling-window dedup cache that decide which observed signals
// become incidents. Signal sources live in pkg/sources; this package
// deliberately carries no k8s.io/api types.
package engine

import "time"

// EventKey uniquely identifies an incident for dedup purposes: the
// (involvedObject.uid, reason) pair. Same pod + same failure mode =
// same incident, regardless of how many event objects the k8s API
// emits about it.
type EventKey struct {
	UID    string
	Reason string
}

// TriageEvent is the per-object core every Signal shares, embedded in
// Signal (see signal.go): the object reference, reason/message,
// counters, and context. Derived from *corev1.Event by the k8s-events
// source (pkg/sources/k8sevents) but carries no k8s.io/api types
// itself so unit tests can construct it without a fake clientset. The
// name is kept from M0 — it is the frozen field set behind the
// shipped k8s-event inject payload.
type TriageEvent struct {
	Key           EventKey
	Namespace     string
	KindOfObject  string
	Name          string
	Container     string
	Message       string
	FirstSeen     time.Time
	LastSeen      time.Time
	ControllerRef string
	Node          string
	Labels        map[string]string
	// Count is the k8s Event's own repeat-count field (how many times
	// the source recorded this same event). The sidecar's own dedup
	// counter is separate — see dedup.go.
	Count int
	// Type is the k8s Event.Type ("Normal" or "Warning"), populated by
	// the k8s-events source. Empty for synthetic signals from other
	// sources — they observe state transitions and forecasts, not
	// Events, and inventing a type would be dishonest. Rides the wire
	// as the payload's "type" field (pkg/inject.Payload.Type).
	Type string
}

// CanonicalKey returns the event's pipeline key: the EventKey with
// Reason replaced by its message-aware canonical class
// (CanonicalReasonForEvent). This is THE dedup/binding/tracking key —
// the dispatcher computes it once per signal and threads it through
// dedup, storm bookkeeping, triage regression state, and the recovery
// tracker, so every stage agrees on which incident an event belongs
// to even when kubelet spelled the reason generically ("BackOff",
// "Failed"). The wire payload keeps the ORIGINAL Key.Reason —
// canonicalization never rewrites what went over the wire.
func (t TriageEvent) CanonicalKey() EventKey {
	k := t.Key
	k.Reason = CanonicalReasonForEvent(k.Reason, t.Message)
	return k
}
