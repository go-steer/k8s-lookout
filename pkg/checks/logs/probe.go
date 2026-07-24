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

package logs

import "regexp"

// Probe-noise stripping (DESIGN.md §5): in a healthy pod the single
// largest log population is often the kubelet's own health checks
// echoed by the app's access log — pure nominal state, stripped
// before clustering. The count of stripped lines is still reported
// (one `log.probe_noise` finding), never silently swallowed, and
// --keep-probes disables stripping entirely.
//
// ProbePatterns is a package-level var so embedders (and future
// config) can extend it; each entry documents what it matches.
var ProbePatterns = []*regexp.Regexp{
	// The kubelet's User-Agent, present in any access-log format
	// that logs UA — the canonical probe marker.
	regexp.MustCompile(`kube-probe/`),
	// GET/HEAD requests against conventional health endpoints
	// (/healthz, /readyz, /livez, /health, /health/live, /ready,
	// /live, /alive, /ping, /liveness, /readiness — with an
	// optional Prometheus-style /-/ prefix, which also brings
	// /-/healthy), in plain or quoted access-log lines.
	regexp.MustCompile(`(?i)\b(GET|HEAD)\s+"?(?:/-)?/(healthz|readyz|livez|healthy|health|ready|live|alive|ping|liveness|readiness)([/?\s"']|$)`),
	// The same endpoints as the path field of a structured
	// (JSON/logfmt) access log.
	regexp.MustCompile(`(?i)"(?:path|uri|url|route|request_?path)"\s*:\s*"(?:/-)?/(healthz|readyz|livez|healthy|health|ready|live|alive|ping|liveness|readiness)([/?"]|$)`),
	// Bare mentions of the k8s health endpoints; these path names
	// exist for probes and essentially never appear in real
	// payload traffic.
	regexp.MustCompile(`(?i)\b(healthz|readyz|livez)\b`),
}

// isProbeNoise reports whether a line matches any probe pattern.
func isProbeNoise(line string) bool {
	for _, re := range ProbePatterns {
		if re.MatchString(line) {
			return true
		}
	}
	return false
}
