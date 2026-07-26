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

// `cloud orphans` (DESIGN.md §5): the one-command orphan sweep that
// absorbed v2's disk-orphan-scout and lb-ghost-buster — same sweep
// shape, `--only=disks,lbs` toggles.
//
//   - disks: unattached billing-active disks older than --min-age
//     (default 24h; a freshly detached disk mid-migration is not an
//     orphan). Age is measured from the last detach when the
//     provider records one, else creation. A disk the provider
//     cannot date is REPORTED with age unknown rather than silently
//     dropped — hiding a possibly-idle disk because a timestamp is
//     missing would be the quiet-degradation failure mode §2 bans.
//   - lbs: forwarding rules whose backends resolve to zero
//     endpoints — billed, routing nothing.

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// defaultOrphanMinAge is the --min-age default: long enough that
// routine detach/reattach churn (upgrades, migrations) never shows
// up as waste.
const defaultOrphanMinAge = 24 * time.Hour

// OrphansCommand builds the `cloud orphans` declaration around deps.
func OrphansCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "cloud orphans",
		MCPName: "k8s_cloud_orphans",
		Summary: "Billing-active cloud leftovers: unattached GCE disks older than --min-age and forwarding rules/LBs routing to zero endpoints — cost and hygiene sweep, not an incident read.",
		Flags: []emit.FlagSpec{
			{Name: "only", Type: emit.FlagString, Default: "disks,lbs",
				Help: "resource classes to sweep, comma-separated: disks, lbs"},
			{Name: "min-age", Type: emit.FlagDuration, Default: defaultOrphanMinAge.String(),
				Help: "report a disk only when unattached at least this long (age from last detach, else creation); disks with no datable age are always reported"},
		},
		Output: append([]checks.OutputField{
			{Name: "zone", Doc: "orphan.disk: the disk's zone"},
			{Name: "size_gb", Doc: "orphan.disk: provisioned size in GB (billed whether used or not)"},
			{Name: "disk_type", Doc: "orphan.disk: disk type short name (pd-ssd bills ~4x pd-standard idle)"},
			{Name: "unused_since", Doc: "orphan.disk: last detach (or creation, if never attached), RFC3339; omitted when the provider cannot date it"},
			{Name: "unused_for", Doc: "orphan.disk: how long the disk has been unattached; \"unknown\" when undatable"},
			{Name: "region", Doc: "orphan.lb: the forwarding rule's region (\"global\" for global rules)"},
			{Name: "why", Doc: "orphan.lb: the provider's orphan judgment (e.g. which backend resolved empty)"},
		}, unavailableFields(cloud.CapabilityOrphans)...),
		Examples: []string{
			"lookout cloud orphans",
			"lookout cloud orphans --only=disks --min-age=72h",
			"lookout cloud orphans --only=lbs --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runOrphans(ctx, deps, inv)
		},
	}
}

func runOrphans(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if err := rejectClusterScope(inv, "cloud orphans"); err != nil {
		return 0, err
	}
	minAge := inv.Flags.Duration("min-age")
	if minAge < 0 {
		return 0, emit.UsageErrorf("--min-age must not be negative, got %s", minAge)
	}
	wantDisks, wantLBs, err := parseOnly(inv.Flags.String("only"))
	if err != nil {
		return 0, err
	}

	provider, err := deps.provider(ctx)
	if err != nil {
		return 0, err
	}
	api, ok := provider.Orphans()
	if !ok {
		return emitUnavailable(inv, provider, cloud.CapabilityOrphans, "cloud orphans")
	}

	scanned := 0
	if wantDisks {
		disks, err := api.OrphanDisks(ctx)
		if err != nil {
			return 0, fmt.Errorf("disk sweep: %w", err)
		}
		scanned += len(disks)
		now := deps.now()
		var old []cloud.OrphanDisk
		for _, d := range disks {
			// Zero UnusedSince = undatable — always reported (see
			// package comment); otherwise apply --min-age.
			if d.UnusedSince.IsZero() || now.Sub(d.UnusedSince) >= minAge {
				old = append(old, d)
			}
		}
		sort.Slice(old, func(i, j int) bool {
			if old[i].SizeGB != old[j].SizeGB {
				return old[i].SizeGB > old[j].SizeGB // biggest bill first
			}
			return old[i].Name < old[j].Name
		})
		for _, d := range old {
			if err := inv.Out.Emit(diskFinding(d, now)); err != nil {
				return 0, err
			}
		}
	}
	if wantLBs {
		lbs, err := api.OrphanLoadBalancers(ctx)
		if err != nil {
			return 0, fmt.Errorf("load-balancer sweep: %w", err)
		}
		scanned += len(lbs)
		sort.Slice(lbs, func(i, j int) bool { return lbs[i].Name < lbs[j].Name })
		for _, lb := range lbs {
			if err := inv.Out.Emit(lbFinding(lb)); err != nil {
				return 0, err
			}
		}
	}
	return scanned, nil
}

// parseOnly validates the --only class list.
func parseOnly(only string) (disks, lbs bool, err error) {
	for _, c := range strings.Split(only, ",") {
		switch strings.TrimSpace(c) {
		case "disks":
			disks = true
		case "lbs":
			lbs = true
		case "":
		default:
			return false, false, emit.UsageErrorf("--only accepts disks,lbs — unknown class %q", strings.TrimSpace(c))
		}
	}
	if !disks && !lbs {
		return false, false, emit.UsageErrorf("--only selected nothing: pass disks, lbs, or both")
	}
	return disks, lbs, nil
}

func diskFinding(d cloud.OrphanDisk, now time.Time) emit.Finding {
	desc := fmt.Sprintf("%dGB", d.SizeGB)
	if d.Type != "" {
		desc += " " + d.Type
	}
	age := "unknown"
	if !d.UnusedSince.IsZero() {
		age = now.Sub(d.UnusedSince).Truncate(time.Minute).String()
	}
	f := emit.Finding{
		Kind:         "orphan.disk",
		Severity:     emit.SeverityWarning,
		KindOfObject: "Disk",
		Name:         d.Name,
		Reason:       "UnattachedDisk",
		Message:      fmt.Sprintf("unattached billing-active disk: %s idle for %s — billed until deleted", desc, age),
		Details: []emit.Field{
			{Key: "zone", Value: d.Zone},
			{Key: "size_gb", Value: strconv.FormatInt(d.SizeGB, 10)},
		},
	}
	if d.Type != "" {
		f.Details = append(f.Details, emit.Field{Key: "disk_type", Value: d.Type})
	}
	if !d.UnusedSince.IsZero() {
		f.Details = append(f.Details, emit.Field{Key: "unused_since", Value: fmtTime(d.UnusedSince)})
	}
	f.Details = append(f.Details, emit.Field{Key: "unused_for", Value: age})
	return f
}

func lbFinding(lb cloud.OrphanLoadBalancer) emit.Finding {
	msg := "forwarding rule routes to zero endpoints — billed and serving nothing"
	if lb.Reason != "" {
		msg += ": " + lb.Reason
	}
	f := emit.Finding{
		Kind:         "orphan.lb",
		Severity:     emit.SeverityWarning,
		KindOfObject: "ForwardingRule",
		Name:         lb.Name,
		Reason:       "NoBackendEndpoints",
		Message:      msg,
		Details: []emit.Field{
			{Key: "region", Value: lb.Region},
		},
	}
	if lb.Reason != "" {
		f.Details = append(f.Details, emit.Field{Key: "why", Value: lb.Reason})
	}
	return f
}
