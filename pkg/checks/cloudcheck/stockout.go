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

// `cloud stockout` (DESIGN.md §5): ZONE_RESOURCE_POOL_EXHAUSTED
// extraction over the --since window (default 24h), one finding per
// (zone, machine type) pair with the event count and first/last
// seen. Point-in-time read; the resident version with history lives
// in the capacity source (§10).
//
// # The reroute heuristic (deliberately simple, event-derived)
//
// The remedy for a stockout is rerouting the pool to a sibling zone
// (§10.1); the suggestion here is derived ONLY from the events in
// the window, so it never claims knowledge it does not have:
// a reroute candidate for (zone Z, machine type M) is another zone
// in Z's region that appears somewhere in the window's event set
// (so the log window itself proves the zone is in use) but has NO
// stockout for M. No events from a zone means no evidence either
// way — the zone is not suggested. Zone→region derivation is the
// GCE convention (zone = region + "-<letter>"). Richer suggestions
// (stockout-proneness across weeks) are §9.2 distilled-history
// territory, not a one-shot read's.

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

// defaultStockoutSince is the lookback when --since is 0 (§4.2:
// 0 means the command default).
const defaultStockoutSince = 24 * time.Hour

// StockoutCommand builds the `cloud stockout` declaration around
// deps. The default registry gets production wiring; tests inject a
// fake provider.
func StockoutCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "cloud stockout",
		MCPName: "k8s_cloud_stockout",
		Summary: "GCE capacity stockouts (ZONE_RESOURCE_POOL_EXHAUSTED) per zone/machine-type over --since (default 24h), with event-derived reroute candidates — the cloud-side why behind pods stuck Pending on failed scale-ups.",
		Flags:   []emit.FlagSpec{},
		Kinds: []checks.KindField{
			checks.Kind("stockout.zone", "the cloud had no capacity for a machine type in this zone during the window — the reason a scale-up failed and pods stayed Pending", emit.SeverityWarning),
			checks.CloudUnavailableKind(),
		},
		Output: append([]checks.OutputField{
			{Name: "machine_type", Doc: "the exhausted machine type (omitted when the log record does not name one)"},
			{Name: "events", Doc: "stockout events for this zone/machine-type pair in the window"},
			{Name: "first_seen", Doc: "earliest event in the window (RFC3339)"},
			{Name: "last_seen", Doc: "latest event in the window (RFC3339)"},
			{Name: "reroute", Doc: "same-region zones active in the window with no stockout for this machine type, comma-separated; omitted when the window offers no clean candidate"},
			{Name: "window", Doc: "summary-line note: the lookback the events cover"},
		}, unavailableFields(cloud.CapabilityStockout)...),
		Examples: []string{
			"lookout cloud stockout",
			"lookout cloud stockout --since=6h",
			"lookout cloud stockout --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runStockout(ctx, deps, inv)
		},
	}
}

// stockoutGroup aggregates one (zone, machine type) pair.
type stockoutGroup struct {
	zone        string
	machineType string
	count       int
	first, last time.Time
}

func runStockout(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if err := rejectClusterScope(inv, "cloud stockout"); err != nil {
		return 0, err
	}
	since := inv.Scope.Since
	if since == 0 {
		since = defaultStockoutSince
	}

	provider, err := deps.provider(ctx)
	if err != nil {
		return 0, err
	}
	api, ok := provider.Stockouts()
	if !ok {
		return emitUnavailable(inv, provider, cloud.CapabilityStockout, "cloud stockout")
	}

	now := deps.now()
	events, err := api.Stockouts(ctx, cloud.TimeWindow{Start: now.Add(-since), End: now})
	if err != nil {
		return 0, fmt.Errorf("stockout extraction: %w", err)
	}

	groups := map[[2]string]*stockoutGroup{}
	zonesSeen := map[string]bool{}            // every zone with any event
	exhausted := map[string]map[string]bool{} // machine type → zones stocked out for it
	for _, e := range events {
		zonesSeen[e.Zone] = true
		if exhausted[e.MachineType] == nil {
			exhausted[e.MachineType] = map[string]bool{}
		}
		exhausted[e.MachineType][e.Zone] = true
		key := [2]string{e.Zone, e.MachineType}
		g := groups[key]
		if g == nil {
			g = &stockoutGroup{zone: e.Zone, machineType: e.MachineType, first: e.Time, last: e.Time}
			groups[key] = g
		}
		g.count++
		if e.Time.Before(g.first) {
			g.first = e.Time
		}
		if e.Time.After(g.last) {
			g.last = e.Time
		}
	}

	sorted := make([]*stockoutGroup, 0, len(groups))
	for _, g := range groups {
		sorted = append(sorted, g)
	}
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		if !a.last.Equal(b.last) {
			return a.last.After(b.last) // most recent exhaustion first
		}
		if a.zone != b.zone {
			return a.zone < b.zone
		}
		return a.machineType < b.machineType
	})

	for _, g := range sorted {
		if err := inv.Out.Emit(stockoutFinding(g, since, rerouteCandidates(g, zonesSeen, exhausted))); err != nil {
			return 0, err
		}
	}
	if err := inv.Out.Note("window", since.String()); err != nil {
		return 0, err
	}
	return len(events), nil
}

// rerouteCandidates applies the package-comment heuristic: sibling
// zones (same region) active in the window with no stockout for
// this machine type, sorted.
func rerouteCandidates(g *stockoutGroup, zonesSeen map[string]bool, exhausted map[string]map[string]bool) []string {
	region := zoneRegion(g.zone)
	if region == "" {
		return nil
	}
	var out []string
	for z := range zonesSeen {
		if z == g.zone || zoneRegion(z) != region {
			continue
		}
		if exhausted[g.machineType][z] {
			continue
		}
		out = append(out, z)
	}
	sort.Strings(out)
	return out
}

// zoneRegion derives the region from a GCE zone name ("us-east1-b"
// → "us-east1"): everything before the final "-<suffix>". Returns ""
// for names without a dash (no region derivable).
func zoneRegion(zone string) string {
	i := strings.LastIndexByte(zone, '-')
	if i <= 0 {
		return ""
	}
	return zone[:i]
}

func stockoutFinding(g *stockoutGroup, window time.Duration, reroute []string) emit.Finding {
	target := g.machineType
	if target == "" {
		target = "unspecified machine type"
	}
	msg := fmt.Sprintf("GCE stockout: %s exhausted in %s ×%d in the last %s", target, g.zone, g.count, window)
	if len(reroute) > 0 {
		msg += " — reroute candidates (same region, no stockout for this type in window): " + strings.Join(reroute, ",")
	} else {
		msg += " — no same-region zone observed clean for this type in the window"
	}
	f := emit.Finding{
		Kind:         "stockout.zone",
		Severity:     emit.SeverityWarning,
		KindOfObject: "Zone",
		Name:         g.zone,
		Reason:       "ZoneResourcePoolExhausted",
		Message:      msg,
	}
	if g.machineType != "" {
		f.Details = append(f.Details, emit.Field{Key: "machine_type", Value: g.machineType})
	}
	f.Details = append(f.Details,
		emit.Field{Key: "events", Value: strconv.Itoa(g.count)},
		emit.Field{Key: "first_seen", Value: fmtTime(g.first)},
		emit.Field{Key: "last_seen", Value: fmtTime(g.last)},
	)
	if len(reroute) > 0 {
		f.Details = append(f.Details, emit.Field{Key: "reroute", Value: strings.Join(reroute, ",")})
	}
	return f
}
