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

//go:build gke || allproviders

package gke

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// CapacityAPI (§10.1 source 3): GKE cluster-autoscaler visibility
// logs — structured decision records from Cloud Logging, never the
// CA text log. The Logging dependency stays behind a small client
// interface (EntryLister) per §13: tests run against recorded JSON
// fixtures (testdata/, authored from the documented event format —
// see capacity_test.go); the real logadmin-backed lister lives in
// logadmin.go and is exercised only against a live project, which CI
// never does.

// visibilityLogID is the log ID GKE writes cluster-autoscaler
// visibility events to (URL-encoded into the logName).
const visibilityLogID = "container.googleapis.com%2Fcluster-autoscaler-visibility"

// LogEntry is the boundary shape of one Cloud Logging entry: the
// timestamp plus the raw jsonPayload. Deliberately minimal so
// fixtures are plain JSON files.
type LogEntry struct {
	Timestamp time.Time
	// Payload is the entry's jsonPayload, verbatim.
	Payload json.RawMessage
}

// EntryLister is the small client interface (§13) between the
// capacity API and Cloud Logging: list the entries matching filter,
// oldest first. Implemented by the logadmin-backed lister
// (logadmin.go) and by test fixtures.
type EntryLister interface {
	ListEntries(ctx context.Context, filter string) ([]LogEntry, error)
}

// capacityAPI implements cloud.CapacityAPI over an EntryLister. The
// real lister is built lazily on first use (constructing a Logging
// client needs credentials; provider construction must stay cheap
// and offline-safe).
type capacityAPI struct {
	project  string
	location string
	cluster  string

	mu        sync.Mutex
	lister    EntryLister
	newLister func(ctx context.Context, project string) (EntryLister, error)
}

func (c *capacityAPI) getLister(ctx context.Context) (EntryLister, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lister == nil {
		l, err := c.newLister(ctx, c.project)
		if err != nil {
			return nil, fmt.Errorf("gke: cloud logging client: %w", err)
		}
		c.lister = l
	}
	return c.lister, nil
}

// ScaleDecisions implements cloud.CapacityAPI: query the visibility
// log over the window and flatten the decision records to
// cloud.ScaleDecision.
func (c *capacityAPI) ScaleDecisions(ctx context.Context, w cloud.TimeWindow) ([]cloud.ScaleDecision, error) {
	lister, err := c.getLister(ctx)
	if err != nil {
		return nil, err
	}
	entries, err := lister.ListEntries(ctx, c.filter(w))
	if err != nil {
		return nil, fmt.Errorf("gke: list cluster-autoscaler-visibility entries: %w", err)
	}
	var out []cloud.ScaleDecision
	for _, e := range entries {
		ds, err := parseVisibilityEntry(e)
		if err != nil {
			// One malformed record must not blind the window; the
			// caller's poll loop logs the aggregate error path, so
			// here we skip — the fixture contract test pins the
			// documented shapes we do parse.
			continue
		}
		out = append(out, ds...)
	}
	return out, nil
}

// filter builds the Cloud Logging filter for the visibility stream,
// scoped to this cluster and the query window.
func (c *capacityAPI) filter(w cloud.TimeWindow) string {
	var b strings.Builder
	fmt.Fprintf(&b, "logName=%q", "projects/"+c.project+"/logs/"+visibilityLogID)
	b.WriteString(` AND resource.type="k8s_cluster"`)
	if c.cluster != "" {
		fmt.Fprintf(&b, ` AND resource.labels.cluster_name=%q`, c.cluster)
	}
	if c.location != "" {
		fmt.Fprintf(&b, ` AND resource.labels.location=%q`, c.location)
	}
	fmt.Fprintf(&b, ` AND timestamp>=%q AND timestamp<%q`,
		w.Start.UTC().Format(time.RFC3339), w.End.UTC().Format(time.RFC3339))
	return b.String()
}

// ---- visibility event payload (documented format) ----
//
// Shapes per the GKE "cluster autoscaler visibility events"
// documentation: a decision record carries scaleUp / noScaleUp /
// scaleDown / noScaleDown branches; eventResult records carry
// resultInfo with per-result errorMsg. Only the fields consumed here
// are declared; unknown fields are ignored (additive upstream changes
// don't break parsing).

type visibilityPayload struct {
	Decision   *visibilityDecision `json:"decision"`
	ResultInfo *visibilityResults  `json:"resultInfo"`
}

type visibilityDecision struct {
	EventID   string `json:"eventId"`
	NoScaleUp *struct {
		SkippedMigs []struct {
			Mig    visibilityMig    `json:"mig"`
			Reason visibilityReason `json:"reason"`
		} `json:"skippedMigs"`
		UnhandledPodGroups []struct {
			RejectedMigs []struct {
				Mig    visibilityMig    `json:"mig"`
				Reason visibilityReason `json:"reason"`
			} `json:"rejectedMigs"`
		} `json:"unhandledPodGroups"`
		Reason *visibilityReason `json:"reason"`
	} `json:"noScaleUp"`
	ScaleUp *struct {
		IncreasedMigs []struct {
			Mig            visibilityMig `json:"mig"`
			RequestedNodes int           `json:"requestedNodes"`
		} `json:"increasedMigs"`
	} `json:"scaleUp"`
}

type visibilityResults struct {
	Results []struct {
		EventID  string `json:"eventId"`
		ErrorMsg *struct {
			MessageID  string   `json:"messageId"`
			Parameters []string `json:"parameters"`
		} `json:"errorMsg"`
	} `json:"results"`
}

type visibilityMig struct {
	Name     string `json:"name"`
	Nodepool string `json:"nodepool"`
	Zone     string `json:"zone"`
}

type visibilityReason struct {
	MessageID  string   `json:"messageId"`
	Parameters []string `json:"parameters"`
}

// errorMessageReasons maps documented eventResult errorMsg messageIds
// to the boundary's machine-matchable reason tokens (§10.1: the
// stockout/quota distinction drives disjoint remedies).
var errorMessageReasons = map[string]string{
	"scale.up.error.out.of.resources":   "GCE_STOCKOUT",
	"scale.up.error.quota.exceeded":     "GCE_QUOTA_EXCEEDED",
	"scale.up.error.ip.space.exhausted": "IP_SPACE_EXHAUSTED",
}

// parseVisibilityEntry flattens one log entry to zero or more
// ScaleDecisions:
//
//   - decision.noScaleUp → one record per rejected/skipped MIG (the
//     per-MIG reasons of §10.1), plus one MIG-less record for a
//     top-level reason with no MIG detail;
//   - decision.scaleUp → one record per increased MIG (context for a
//     later eventResult error);
//   - eventResult errorMsg → one record per named MIG parameter (or
//     one MIG-less record), with the messageId normalized to the
//     boundary token (GCE_STOCKOUT / GCE_QUOTA_EXCEEDED /
//     IP_SPACE_EXHAUSTED) — parameters that already carry a GCE_*
//     token pass through verbatim.
func parseVisibilityEntry(e LogEntry) ([]cloud.ScaleDecision, error) {
	var p visibilityPayload
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return nil, err
	}
	var out []cloud.ScaleDecision

	if d := p.Decision; d != nil {
		if n := d.NoScaleUp; n != nil {
			for _, sk := range n.SkippedMigs {
				out = append(out, noScaleUpDecision(e.Timestamp, sk.Mig, sk.Reason))
			}
			for _, g := range n.UnhandledPodGroups {
				for _, rej := range g.RejectedMigs {
					out = append(out, noScaleUpDecision(e.Timestamp, rej.Mig, rej.Reason))
				}
			}
			if len(out) == 0 && n.Reason != nil {
				out = append(out, noScaleUpDecision(e.Timestamp, visibilityMig{}, *n.Reason))
			}
		}
		if u := d.ScaleUp; u != nil {
			for _, inc := range u.IncreasedMigs {
				out = append(out, cloud.ScaleDecision{
					Time:      e.Timestamp,
					Decision:  "scaleUp",
					NodeGroup: inc.Mig.Name,
					Reason:    "TRIGGERED",
					Message:   fmt.Sprintf("requested %d node(s) in nodepool %s zone %s", inc.RequestedNodes, inc.Mig.Nodepool, inc.Mig.Zone),
				})
			}
		}
	}

	if r := p.ResultInfo; r != nil {
		for _, res := range r.Results {
			if res.ErrorMsg == nil {
				continue
			}
			reason := errorMessageReasons[res.ErrorMsg.MessageID]
			if reason == "" {
				reason = res.ErrorMsg.MessageID
			}
			migs := gceTokenFreeMigs(res.ErrorMsg.Parameters)
			if len(migs) == 0 {
				migs = []string{""}
			}
			for _, mig := range migs {
				out = append(out, cloud.ScaleDecision{
					Time:      e.Timestamp,
					Decision:  "scaleUp",
					NodeGroup: mig,
					Reason:    reason,
					Message:   fmt.Sprintf("scale-up result error %s (parameters: %s)", res.ErrorMsg.MessageID, strings.Join(res.ErrorMsg.Parameters, ", ")),
				})
			}
		}
	}
	return out, nil
}

// noScaleUpDecision renders one per-MIG rejection. The reason is the
// first ALL-CAPS parameter when present (that is where GCE_STOCKOUT /
// GCE_QUOTA_EXCEEDED surface in rejection records), else the
// messageId.
func noScaleUpDecision(at time.Time, mig visibilityMig, r visibilityReason) cloud.ScaleDecision {
	reason := r.MessageID
	for _, param := range r.Parameters {
		if isReasonToken(param) {
			reason = param
			break
		}
	}
	return cloud.ScaleDecision{
		Time:      at,
		Decision:  "noScaleUp",
		NodeGroup: mig.Name,
		Reason:    reason,
		Message:   fmt.Sprintf("%s (parameters: %s)", r.MessageID, strings.Join(r.Parameters, ", ")),
	}
}

// isReasonToken reports whether s looks like a machine-matchable
// reason constant (GCE_STOCKOUT-style: ALL_CAPS_WITH_UNDERSCORES).
func isReasonToken(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

// gceTokenFreeMigs returns the parameters that name MIGs (i.e. are
// not reason tokens) — eventResult errors carry the failing MIG names
// in parameters.
func gceTokenFreeMigs(params []string) []string {
	var out []string
	for _, p := range params {
		if p != "" && !isReasonToken(p) {
			out = append(out, p)
		}
	}
	return out
}
