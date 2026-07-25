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
	"fmt"
	"strings"
)

// Route is the §7.7 per-severity routing decision for a NEW incident
// in per-incident mode. It is deliberately not consulted in shared
// mode: `--mode=shared` predates severity routing and keeps its
// frozen contract — ALL severities route to `--target-session`.
type Route int

const (
	// RoutePerIncident opens a dedicated incident session with full
	// enrichment (§7.6, next change) — today's watcher behavior. The
	// §7.7 default for `critical`, and the fallback for any severity
	// the router does not recognize: an unclassifiable signal fails
	// TOWARD paging, never toward silence.
	RoutePerIncident Route = iota
	// RouteWatchboard batches the signal into the shared watchboard
	// session's rolling digest (§7.7 default for `warning`).
	RouteWatchboard
	// RouteStore is the §7.7 `info` policy: stored only (§9.1),
	// surfaced by read-path queries and digests. With --store set the
	// dispatcher persists the signal in the raw-occurrence store
	// (pkg/store, route=info-stored); without it the signal is
	// counted in metrics and dropped with a log. Info signals are
	// NEVER silently ignored either way.
	RouteStore
)

// String names the route for logs and metric labels.
func (r Route) String() string {
	switch r {
	case RouteWatchboard:
		return "watchboard"
	case RouteStore:
		return "store"
	default:
		return "per-incident"
	}
}

// RouteFor maps a severity class to its §7.7 routing policy:
//
//	critical → per-incident session (today's behavior), full enrichment
//	warning  → shared watchboard session, batched (rolling digest inject)
//	info     → stored only (§9.1; persisted when --store is set, else counted + dropped)
//
// Anything else (including the zero value) routes per-incident — see
// RoutePerIncident's fail-toward-paging rationale.
func RouteFor(sev Severity) Route {
	switch sev {
	case SeverityWarning:
		return RouteWatchboard
	case SeverityInfo:
		return RouteStore
	default:
		return RoutePerIncident
	}
}

// RoutingPolicy resolves a Signal's effective severity: the per-kind
// severity defaults are stamped by the sources (§7.2 — the source
// knows what its kinds mean), and deployment config may override any
// kind via the `--severity` flag (§7.7 "overridable in config").
type RoutingPolicy struct {
	overrides map[string]Severity
}

// NewRoutingPolicy builds a policy from parsed per-kind overrides
// (nil/empty = source defaults everywhere).
func NewRoutingPolicy(overrides map[string]Severity) *RoutingPolicy {
	return &RoutingPolicy{overrides: overrides}
}

// Classify returns sig's effective severity: the config override for
// its kind if one is set, else the source-stamped severity, else
// critical (a source that forgot to classify fails toward paging,
// mirroring RouteFor's posture for unknown classes).
func (p *RoutingPolicy) Classify(sig Signal) Severity {
	if p != nil {
		if sev, ok := p.overrides[sig.Kind]; ok {
			return sev
		}
	}
	if sig.Severity.Valid() {
		return sig.Severity
	}
	return SeverityCritical
}

// ParseSeverityOverrides parses the `--severity` flag's values into a
// per-kind override map. Each entry is one flag occurrence and may
// carry several comma-separated `kind=level` pairs (the flag is
// additive: repeats accumulate). Levels are the three §7.7 classes.
// A kind may appear at most once across all occurrences — a repeated
// kind is a config ambiguity and is rejected rather than resolved by
// position.
func ParseSeverityOverrides(entries []string) (map[string]Severity, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	out := make(map[string]Severity)
	for _, entry := range entries {
		for _, pair := range strings.Split(entry, ",") {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			kind, level, ok := strings.Cut(pair, "=")
			kind = strings.TrimSpace(kind)
			sev := Severity(strings.TrimSpace(level))
			if !ok || kind == "" {
				return nil, fmt.Errorf("invalid override %q: want kind=level (e.g. objectstate.restart_burst=info)", pair)
			}
			if !sev.Valid() {
				return nil, fmt.Errorf("invalid override %q: level must be one of %s, %s, %s",
					pair, SeverityCritical, SeverityWarning, SeverityInfo)
			}
			if prev, dup := out[kind]; dup {
				return nil, fmt.Errorf("kind %q overridden twice (%s and %s): each kind may appear at most once", kind, prev, sev)
			}
			out[kind] = sev
		}
	}
	return out, nil
}
