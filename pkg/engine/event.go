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

// Package engine implements the watch-path signal pipeline: the
// Event.Reason / namespace filter and the rolling-window dedup cache
// that decide which observed Kubernetes events become incidents.
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

// TriageEvent is the internal representation the filter + dedup +
// injector layers pass around. Derived from *corev1.Event by watcher.go
// but carries no k8s.io/api types itself so unit tests can construct
// it without a fake clientset.
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
}
