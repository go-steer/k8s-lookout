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

package audit

import (
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// kindLivenessPreempts: the probe claim that no_readiness_probe and
// no_liveness_probe (workloads.go) do not cover — both probes exist,
// and the liveness one makes the readiness one pointless.
const kindLivenessPreempts = "audit.liveness_preempts_readiness"

const reasonLivenessFiresFirst = "LivenessFiresFirst"

// selfHealthSidecars are injected containers whose probes and hooks are
// the mesh's or the platform's business, not the workload owner's. A
// finding naming istio-proxy would send the owner to a manifest they do
// not write. (A policy knob of the same name once pkg/policy exists.)
var selfHealthSidecars = map[string]bool{
	"istio-proxy":         true,
	"cloud-sql-proxy":     true,
	"gke-metadata-server": true,
}

// Kubelet defaults for the probe fields that decide a deadline.
const (
	probeDefaultPeriodSeconds    = 10
	probeDefaultFailureThreshold = 3
)

// livenessPreemptsReadiness: a liveness probe that fails no later than
// its readiness twin, on the SAME handler. When both probes test the
// same thing the cluster has two responses to one signal — stop routing
// (recoverable) and restart (not) — and whichever deadline is shorter
// wins. Liveness shorter or equal means readiness can never pull the
// pod from the Service before the kubelet kills it; the readiness probe
// is decoration.
//
// Deliberately narrow: a stricter liveness probe on a DIFFERENT handler
// is not flagged, because a local liveness endpoint beside a
// dependency-aware readiness one is the correct shape and the manifest
// cannot tell it from a wrong one. Deadline = initialDelaySeconds +
// failureThreshold × periodSeconds, with kubelet defaults; approximate
// on both sides in the same way, and only ever compared to each other.
func livenessPreemptsReadiness(spec corev1.PodSpec) []emit.Finding {
	var names, detail []string
	strict := false
	for _, c := range spec.Containers {
		if selfHealthSidecars[c.Name] || c.LivenessProbe == nil || c.ReadinessProbe == nil {
			continue
		}
		kind, handler := probeHandler(c.LivenessProbe)
		if kind == "" {
			continue
		}
		rkind, rhandler := probeHandler(c.ReadinessProbe)
		if kind != rkind || handler != rhandler {
			continue
		}
		live, ready := probeDeadline(c.LivenessProbe), probeDeadline(c.ReadinessProbe)
		if live > ready {
			continue
		}
		if live < ready {
			strict = true
		}
		names = append(names, c.Name)
		detail = append(detail, fmt.Sprintf("%s: %s is both probes; liveness fails at ~%ds, readiness at ~%ds", c.Name, kind, live, ready))
	}
	if len(names) == 0 {
		return nil
	}
	// A tie — one handler, the same timings on both probes, which is
	// the shape a great many manifests have — is info: the claim holds
	// (readiness never wins the race) but the fix is a timing tweak,
	// and a fleet of them should not read as a fleet of warnings.
	// Liveness strictly earlier is the misconfiguration proper.
	severity := emit.SeverityInfo
	if strict {
		severity = emit.SeverityWarning
	}
	return []emit.Finding{{
		Kind:     kindLivenessPreempts,
		Severity: severity,
		Reason:   reasonLivenessFiresFirst,
		Message: fmt.Sprintf("liveness and readiness probe the same handler and liveness gives up first on %d of %d containers: the kubelet restarts the container before readiness can take it out of the Service, so every slow start or dependency blip is a restart with traffic still routed to it — %s",
			len(names), len(spec.Containers), strings.Join(detail, "; ")),
		Details: []emit.Field{
			{Key: "containers", Value: itoa(len(names))},
			{Key: "container_names", Value: cappedList(names)},
			{Key: "total_containers", Value: itoa(len(spec.Containers))},
		},
	}}
}

// probeHandler names the handler kind and a canonical rendering of it,
// so two probes that differ only in field order compare equal.
func probeHandler(p *corev1.Probe) (string, string) {
	switch {
	case p.HTTPGet != nil:
		return "httpGet", canonicalJSON(p.HTTPGet)
	case p.TCPSocket != nil:
		return "tcpSocket", canonicalJSON(p.TCPSocket)
	case p.Exec != nil:
		return "exec", canonicalJSON(p.Exec)
	case p.GRPC != nil:
		return "grpc", canonicalJSON(p.GRPC)
	}
	return "", ""
}

func canonicalJSON(v any) string {
	b, err := json.Marshal(v) // encoding/json sorts struct fields deterministically by declaration
	if err != nil {
		return ""
	}
	return string(b)
}

func probeDeadline(p *corev1.Probe) int {
	period, threshold := int(p.PeriodSeconds), int(p.FailureThreshold)
	if period == 0 {
		period = probeDefaultPeriodSeconds
	}
	if threshold == 0 {
		threshold = probeDefaultFailureThreshold
	}
	return int(p.InitialDelaySeconds) + threshold*period
}
