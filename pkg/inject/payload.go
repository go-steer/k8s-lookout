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

package inject

import "time"

// Payload is the JSON body POSTed to
// /sessions/<sid>/inject.message. Field names and casing mirror the
// design doc's "Inject payload shape" section verbatim so playbook
// skills can pattern-match against them.
type Payload struct {
	Kind         string         `json:"kind"`
	Reason       string         `json:"reason"`
	Namespace    string         `json:"namespace"`
	KindOfObject string         `json:"kind_of_object"`
	Name         string         `json:"name"`
	Container    string         `json:"container,omitempty"`
	UID          string         `json:"uid"`
	Message      string         `json:"message"`
	Count        int            `json:"count"`
	FirstSeen    time.Time      `json:"first_seen"`
	LastSeen     time.Time      `json:"last_seen"`
	Cluster      string         `json:"cluster"`
	Context      PayloadContext `json:"context"`
}

// PayloadContext is the nested "context" object on Payload.
type PayloadContext struct {
	ControllerRef string            `json:"controller_ref,omitempty"`
	Node          string            `json:"node,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
}

// Kind* are the constants we stamp on every payload's "kind"
// field. Skills match against this to distinguish k8s-triggered
// injects from other signal sources (Cloud Monitoring, PagerDuty,
// etc.) that would use different constants when they ship.
const (
	KindEvent    = "k8s-event"
	KindFollowup = "k8s-event-followup"
)
