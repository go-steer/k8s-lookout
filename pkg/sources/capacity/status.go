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
	"regexp"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// Sub-source 2 (§10.1): the cluster-autoscaler-status ConfigMap,
// polled. Per-nodegroup health/backoff plus the cloudProviderTarget
// vs registered vs ready gap — "asked for a node, didn't get one".
//
// The ConfigMap's `status` key changed shape in cluster-autoscaler
// 1.30: before, a human-oriented text block ("NodeGroups:\n  Name: …\n
// Health: Healthy (ready=1 … cloudProviderTarget=2 …)"); since, a
// yaml document (upstream api types: ClusterAutoscalerStatus). Both
// are tolerated: yaml is detected by its `autoscalerStatus:` top-level
// key, everything else goes through the legacy text parser.

// nodeGroupStatus is the normalized per-nodegroup view both parsers
// produce.
type nodeGroupStatus struct {
	Name         string
	HealthStatus string // "Healthy" / "Unhealthy" / "" when absent
	Ready        int
	Registered   int
	Target       int // cloudProviderTarget
	// ScaleUp backoff, with the yaml format's error detail when
	// present (the legacy text format does not carry error codes).
	Backoff      bool
	BackoffError string // "errorCode: errorMessage", "" if none
}

// gapEntry is the per-nodegroup gap-episode memory between polls.
type gapEntry struct {
	since time.Time // first poll that observed target > ready
	fired level     // episode latch: none → warn → critical
}

// pollStatus reads and judges the status ConfigMap once. A missing
// ConfigMap is legal (no cluster autoscaler installed) — logged once,
// polled again next tick in case CA arrives later.
func (s *Source) pollStatus(ctx context.Context, now time.Time) error {
	cm, err := s.client.CoreV1().ConfigMaps(s.cfg.StatusNamespace).Get(ctx, s.cfg.StatusName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		s.mu.Lock()
		logged := s.statusMissing
		s.statusMissing = true
		s.mu.Unlock()
		if !logged {
			s.logPrintf("capacity: ConfigMap %s/%s not found — no cluster autoscaler on this cluster? status sub-source idle until it appears", s.cfg.StatusNamespace, s.cfg.StatusName)
		}
		return nil
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.statusMissing = false
	s.mu.Unlock()

	groups, err := parseStatus(cm.Data["status"])
	if err != nil {
		return fmt.Errorf("parse %s/%s: %w", s.cfg.StatusNamespace, s.cfg.StatusName, err)
	}
	s.judgeStatus(groups, now)
	return nil
}

// judgeStatus applies the sustained-gap rule: a nodegroup whose
// cloudProviderTarget exceeds its ready count opens an episode; if
// the gap survives GapSustain it fires capacity.scaleup_gap once per
// level — warning, escalated to critical when the nodegroup is in
// scale-up Backoff with a recorded error. A closed gap (target
// reached, or target withdrawn) retires the episode so a future gap
// fires fresh.
func (s *Source) judgeStatus(groups []nodeGroupStatus, now time.Time) {
	var out []engine.Signal
	s.mu.Lock()
	seen := map[string]bool{}
	for _, g := range groups {
		seen[g.Name] = true
		if g.Target <= g.Ready {
			delete(s.gaps, g.Name)
			continue
		}
		e, ok := s.gaps[g.Name]
		if !ok {
			e = &gapEntry{since: now}
			s.gaps[g.Name] = e
		}
		if now.Sub(e.since) < s.cfg.GapSustain {
			continue
		}
		lvl := levelWarn
		if g.Backoff && g.BackoffError != "" {
			lvl = levelCritical
		}
		if lvl <= e.fired {
			continue // this episode already reported at this level
		}
		e.fired = lvl
		out = append(out, gapSignal(g, e.since, now, lvl))
	}
	// Nodegroups gone from the status retire their episodes.
	for name := range s.gaps {
		if !seen[name] {
			delete(s.gaps, name)
		}
	}
	s.mu.Unlock()
	for _, sig := range out {
		s.send(sig)
	}
}

func gapSignal(g nodeGroupStatus, since, now time.Time, lvl level) engine.Signal {
	msg := fmt.Sprintf(
		"nodegroup %s: cloudProviderTarget=%d registered=%d ready=%d — asked for %d node(s) and did not get them (gap sustained %s)",
		g.Name, g.Target, g.Registered, g.Ready, g.Target-g.Ready,
		now.Sub(since).Truncate(time.Second))
	if g.HealthStatus != "" && g.HealthStatus != "Healthy" {
		msg += "; health=" + g.HealthStatus
	}
	if g.Backoff {
		msg += "; scale-up in backoff"
		if g.BackoffError != "" {
			msg += " (" + g.BackoffError + ")"
		}
	}
	return engine.Signal{
		Kind:     KindScaleUpGap,
		Source:   engine.SourceSentinel,
		Severity: lvl.severity(),
		TriageEvent: engine.TriageEvent{
			Key: engine.EventKey{
				UID:    "nodegroup:" + g.Name,
				Reason: strings.TrimPrefix(KindScaleUpGap, kindPrefix),
			},
			KindOfObject: "NodeGroup",
			Name:         g.Name,
			Message:      truncate(msg),
			FirstSeen:    since,
			LastSeen:     now,
			Count:        1,
		},
	}
}

// level is the two-step episode latch (shared by the gap and
// pending-age judges).
type level int

const (
	levelNone level = iota
	levelWarn
	levelCritical
)

func (l level) severity() engine.Severity {
	if l == levelCritical {
		return engine.SeverityCritical
	}
	return engine.SeverityWarning
}

// parseStatus detects the format and dispatches.
func parseStatus(status string) ([]nodeGroupStatus, error) {
	if strings.TrimSpace(status) == "" {
		return nil, fmt.Errorf("status ConfigMap has no %q key or it is empty", "status")
	}
	if isYAMLStatus(status) {
		return parseYAMLStatus(status)
	}
	return parseLegacyStatus(status)
}

// isYAMLStatus detects the CA ≥ 1.30 yaml document by its
// `autoscalerStatus:` top-level key — a token the legacy text format
// never contains.
func isYAMLStatus(status string) bool {
	for _, line := range strings.Split(status, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "autoscalerStatus:") {
			return true
		}
	}
	return false
}

// ---- yaml format (cluster-autoscaler ≥ 1.30) ----

// yamlStatus mirrors the fields we consume of the upstream
// ClusterAutoscalerStatus api type (sigs.k8s.io/yaml honors the json
// tags). Unknown fields are ignored, so additive upstream changes
// don't break the poll.
type yamlStatus struct {
	AutoscalerStatus string          `json:"autoscalerStatus"`
	NodeGroups       []yamlNodeGroup `json:"nodeGroups"`
}

type yamlNodeGroup struct {
	Name   string `json:"name"`
	Health struct {
		Status     string `json:"status"`
		NodeCounts struct {
			Registered struct {
				Total int `json:"total"`
				Ready int `json:"ready"`
			} `json:"registered"`
		} `json:"nodeCounts"`
		CloudProviderTarget int `json:"cloudProviderTarget"`
	} `json:"health"`
	ScaleUp struct {
		Status      string `json:"status"`
		BackoffInfo struct {
			ErrorCode    string `json:"errorCode"`
			ErrorMessage string `json:"errorMessage"`
		} `json:"backoffInfo"`
	} `json:"scaleUp"`
}

func parseYAMLStatus(status string) ([]nodeGroupStatus, error) {
	var doc yamlStatus
	if err := yaml.Unmarshal([]byte(status), &doc); err != nil {
		return nil, fmt.Errorf("yaml status format: %w", err)
	}
	out := make([]nodeGroupStatus, 0, len(doc.NodeGroups))
	for _, ng := range doc.NodeGroups {
		g := nodeGroupStatus{
			Name:         ng.Name,
			HealthStatus: ng.Health.Status,
			Ready:        ng.Health.NodeCounts.Registered.Ready,
			Registered:   ng.Health.NodeCounts.Registered.Total,
			Target:       ng.Health.CloudProviderTarget,
			Backoff:      ng.ScaleUp.Status == "Backoff",
		}
		if code, msg := ng.ScaleUp.BackoffInfo.ErrorCode, ng.ScaleUp.BackoffInfo.ErrorMessage; code != "" || msg != "" {
			g.BackoffError = strings.TrimSuffix(strings.TrimPrefix(code+": "+msg, ": "), ": ")
		}
		out = append(out, g)
	}
	return out, nil
}

// ---- legacy text format (cluster-autoscaler < 1.30) ----

// legacyKV extracts key=value integers ("ready=1", "cloudProviderTarget=2").
var legacyKV = regexp.MustCompile(`([A-Za-z]+)=(\d+)`)

// parseLegacyStatus parses the pre-1.30 human-oriented block:
//
//	NodeGroups:
//	  Name:        https://…/instanceGroups/mig-a
//	    Health:      Healthy (ready=1 unready=0 … registered=1 … cloudProviderTarget=2 (minSize=0, maxSize=3))
//	    ScaleUp:     Backoff (ready=1 cloudProviderTarget=2)
//
// Only the NodeGroups section is consumed (the Cluster-wide block has
// no per-nodegroup answer to give). Nodegroup names arrive as full
// provider URLs; the basename is the stable operator-facing name.
func parseLegacyStatus(status string) ([]nodeGroupStatus, error) {
	var (
		out        []nodeGroupStatus
		cur        *nodeGroupStatus
		inGroups   bool
		sawSection bool
	)
	flush := func() {
		if cur != nil {
			out = append(out, *cur)
			cur = nil
		}
	}
	for _, raw := range strings.Split(status, "\n") {
		line := strings.TrimSpace(raw)
		switch {
		case line == "NodeGroups:":
			inGroups, sawSection = true, true
		case !inGroups:
			continue
		case strings.HasPrefix(line, "Name:"):
			flush()
			name := strings.TrimSpace(strings.TrimPrefix(line, "Name:"))
			if i := strings.LastIndex(name, "/"); i >= 0 {
				name = name[i+1:]
			}
			cur = &nodeGroupStatus{Name: name}
		case cur != nil && strings.HasPrefix(line, "Health:"):
			rest := strings.TrimSpace(strings.TrimPrefix(line, "Health:"))
			cur.HealthStatus = firstWord(rest)
			for _, kv := range legacyKV.FindAllStringSubmatch(rest, -1) {
				n, _ := strconv.Atoi(kv[2])
				switch kv[1] {
				case "ready":
					cur.Ready = n
				case "registered":
					cur.Registered = n
				case "cloudProviderTarget":
					cur.Target = n
				}
			}
		case cur != nil && strings.HasPrefix(line, "ScaleUp:"):
			rest := strings.TrimSpace(strings.TrimPrefix(line, "ScaleUp:"))
			cur.Backoff = firstWord(rest) == "Backoff"
		}
	}
	flush()
	if !sawSection {
		return nil, fmt.Errorf("legacy status format: no NodeGroups section found")
	}
	return out, nil
}

func firstWord(s string) string {
	if i := strings.IndexAny(s, " \t("); i >= 0 {
		return s[:i]
	}
	return s
}
