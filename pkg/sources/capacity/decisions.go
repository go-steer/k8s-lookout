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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// Sub-source 3 (§10.1): provider scale-decision records — on GKE, the
// cluster-autoscaler-visibility Cloud Logging stream, reached through
// the pkg/cloud boundary (this package never imports a cloud SDK).
// Only the reasons whose remedies the design names get a kind:
// stockout → reroute zone/machine-type, quota → file an increase
// (§10.3), IP exhaustion → grow the ranges. Every other decision
// record is context the portable sub-sources already surface
// (NotTriggerScaleUp events carry the full rejection list), so
// unmatched records are deliberately not re-emitted here.

// decisionKind maps a cloud.ScaleDecision.Reason to the signal kind
// it fires, or "" for records this sub-source does not escalate.
// Matching is by the provider-normalized tokens the boundary
// documents (GCE_STOCKOUT, GCE_QUOTA_EXCEEDED, IP_SPACE_EXHAUSTED)
// with conservative substring fallbacks for provider variants
// (e.g. GCE_IP_SPACE_EXHAUSTED, ZONE_RESOURCE_POOL_EXHAUSTED).
func decisionKind(reason string) string {
	r := strings.ToUpper(reason)
	switch {
	case r == "GCE_STOCKOUT", strings.Contains(r, "STOCKOUT"), strings.Contains(r, "RESOURCE_POOL_EXHAUSTED"):
		return KindStockout
	case strings.Contains(r, "QUOTA"):
		return KindQuotaBlocked
	case strings.Contains(r, "IP_SPACE_EXHAUSTED"):
		return KindIPExhausted
	default:
		return ""
	}
}

// pollDecisions fetches the provider's scale decisions since the last
// poll and emits the remedy-bearing ones. The window high-water mark
// only advances on success, so a transient API failure re-polls the
// same window instead of dropping it.
func (s *Source) pollDecisions(ctx context.Context, now time.Time) error {
	s.mu.Lock()
	if s.lastDecisions.IsZero() {
		// First poll: look back one interval. Persisted before the
		// query so a failing first poll retries this same window.
		s.lastDecisions = now.Add(-s.cfg.PollInterval)
	}
	start := s.lastDecisions
	s.mu.Unlock()
	decisions, err := s.decisions.ScaleDecisions(ctx, cloud.TimeWindow{Start: start, End: now})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.lastDecisions = now
	s.mu.Unlock()

	for _, d := range decisions {
		kind := decisionKind(d.Reason)
		if kind == "" {
			continue
		}
		s.send(decisionSignal(kind, d))
	}
	return nil
}

func decisionSignal(kind string, d cloud.ScaleDecision) engine.Signal {
	msg := fmt.Sprintf("autoscaler %s decision for nodegroup %s: %s", d.Decision, d.NodeGroup, d.Reason)
	if d.Message != "" {
		msg += " — " + d.Message
	}
	at := d.Time
	if at.IsZero() {
		at = time.Now()
	}
	return engine.Signal{
		Kind:     kind,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key: engine.EventKey{
				UID:    "nodegroup:" + d.NodeGroup,
				Reason: strings.TrimPrefix(kind, kindPrefix),
			},
			KindOfObject: "NodeGroup",
			Name:         d.NodeGroup,
			Message:      truncate(msg),
			FirstSeen:    at,
			LastSeen:     at,
			Count:        1,
		},
	}
}
