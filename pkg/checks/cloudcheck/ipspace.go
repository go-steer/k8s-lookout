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

package cloudcheck

// `cloud ipspace` (DESIGN.md §5): pod/service/node CIDR utilization
// per subnet, point-in-time; consumption RATE lives in the capacity
// source (§10). Zero nominal state applies as in `triage top`: only
// ranges at/above the warning line become findings (--all dumps
// every range, info below the threshold) — with one exception: a
// range whose usage the cloud APIs cannot see is ALWAYS reported as
// an explicit info finding (reason=UsageNotCloudVisible), because
// "we cannot know" rendered as silence-or-0% is exactly the quiet
// degradation §2 bans.
//
// Thresholds are fixed at warning ≥80% / critical ≥95% (§5 row),
// deliberately not flags: IP space is incompressible — like memory
// in `triage top`, at 100% the next allocation (a node's pod block,
// a node IP) fails outright, so the judgment is the point of the
// command, not a tunable.

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

const (
	// ipWarnPct / ipCritPct are the §5-mandated judgment lines.
	ipWarnPct = 80.0
	ipCritPct = 95.0
)

// IPSpaceCommand builds the `cloud ipspace` declaration around deps.
func IPSpaceCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "cloud ipspace",
		MCPName: "k8s_cloud_ipspace",
		Summary: "Pod/Service/node CIDR utilization per subnet, judged: warning at 80%, critical at 95% — IP space is incompressible, an exhausted range fails the next node or pod block outright. Consumption rate/ETA lives in the sentinel's capacity source.",
		Flags: []emit.FlagSpec{
			{Name: "all", Type: emit.FlagBool, Default: "false",
				Help: "exploratory dump: emit every range regardless of utilization (info severity below 80%), sorted by pct descending"},
		},
		Output: append([]checks.OutputField{
			{Name: "cidr", Doc: "the range's CIDR block"},
			{Name: "purpose", Doc: "what the range allocates: pods, services, or nodes"},
			{Name: "used", Doc: "allocated addresses (for GKE pod ranges: addresses of the per-node blocks already carved out — the granularity at which the range actually exhausts)"},
			{Name: "capacity", Doc: "usable addresses in the range"},
			{Name: "pct", Doc: "used as a percent of capacity, one decimal"},
		}, unavailableFields(cloud.CapabilityIPSpace)...),
		Examples: []string{
			"lookout cloud ipspace",
			"lookout cloud ipspace --all",
			"lookout cloud ipspace --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runIPSpace(ctx, deps, inv)
		},
	}
}

func runIPSpace(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if err := rejectClusterScope(inv, "cloud ipspace"); err != nil {
		return 0, err
	}
	all := inv.Flags.Bool("all")

	provider, err := deps.provider(ctx)
	if err != nil {
		return 0, err
	}
	api, ok := provider.IPSpace()
	if !ok {
		return emitUnavailable(inv, provider, cloud.CapabilityIPSpace, "cloud ipspace")
	}
	ranges, err := api.SubnetUtilization(ctx)
	if err != nil {
		return 0, fmt.Errorf("subnet utilization: %w", err)
	}

	type rated struct {
		r   cloud.SubnetUtilization
		pct float64
	}
	var known, unknown []rated
	for _, r := range ranges {
		if r.Used < 0 || r.Capacity <= 0 {
			unknown = append(unknown, rated{r: r})
			continue
		}
		known = append(known, rated{r: r, pct: float64(r.Used) / float64(r.Capacity) * 100})
	}
	sort.Slice(known, func(i, j int) bool {
		if known[i].pct != known[j].pct {
			return known[i].pct > known[j].pct
		}
		if known[i].r.Subnet != known[j].r.Subnet {
			return known[i].r.Subnet < known[j].r.Subnet
		}
		return known[i].r.Purpose < known[j].r.Purpose
	})
	sort.Slice(unknown, func(i, j int) bool {
		if unknown[i].r.Subnet != unknown[j].r.Subnet {
			return unknown[i].r.Subnet < unknown[j].r.Subnet
		}
		return unknown[i].r.Purpose < unknown[j].r.Purpose
	})

	for _, e := range known {
		if e.pct < ipWarnPct && !all {
			continue
		}
		if err := inv.Out.Emit(rangeFinding(e.r, e.pct)); err != nil {
			return 0, err
		}
	}
	for _, e := range unknown {
		if err := inv.Out.Emit(unknownRangeFinding(e.r)); err != nil {
			return 0, err
		}
	}
	return len(ranges), nil
}

func rangeFinding(r cloud.SubnetUtilization, pct float64) emit.Finding {
	f := emit.Finding{
		Kind:         "ipspace.range",
		KindOfObject: "Subnetwork",
		Name:         r.Subnet,
		Details: []emit.Field{
			{Key: "cidr", Value: r.CIDR},
			{Key: "purpose", Value: r.Purpose},
			{Key: "used", Value: strconv.FormatInt(r.Used, 10)},
			{Key: "capacity", Value: strconv.FormatInt(r.Capacity, 10)},
			{Key: "pct", Value: fmtPct(pct)},
		},
	}
	switch {
	case pct >= ipCritPct:
		f.Severity = emit.SeverityCritical
		f.Reason = "IPRangeNearExhaustion"
		f.Message = fmt.Sprintf("%s range at %s%% of %s — the next allocation fails at 100%%: IP space is incompressible", r.Purpose, fmtPct(pct), r.CIDR)
	case pct >= ipWarnPct:
		f.Severity = emit.SeverityWarning
		f.Reason = "IPRangeHighUtilization"
		f.Message = fmt.Sprintf("%s range at %s%% of %s — critical from %.0f%%; plan the secondary range before the slope arrives", r.Purpose, fmtPct(pct), r.CIDR, ipCritPct)
	default:
		// --all row below the line: numbers only, no reason, no
		// message (zero nominal state applies to fields too).
		f.Severity = emit.SeverityInfo
	}
	return f
}

// unknownRangeFinding is the §2 explicit record for a range the
// cloud APIs cannot rate (GKE service ClusterIPs are allocated by
// the Kubernetes API server, invisible to GCP).
func unknownRangeFinding(r cloud.SubnetUtilization) emit.Finding {
	f := emit.Finding{
		Kind:         "ipspace.range",
		Severity:     emit.SeverityInfo,
		KindOfObject: "Subnetwork",
		Name:         r.Subnet,
		Reason:       "UsageNotCloudVisible",
		Message:      fmt.Sprintf("%s range %s: usage is not visible to the cloud APIs (allocated cluster-side) — capacity reported, utilization unknown", r.Purpose, r.CIDR),
		Details: []emit.Field{
			{Key: "cidr", Value: r.CIDR},
			{Key: "purpose", Value: r.Purpose},
		},
	}
	if r.Capacity > 0 {
		f.Details = append(f.Details, emit.Field{Key: "capacity", Value: strconv.FormatInt(r.Capacity, 10)})
	}
	return f
}
