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
// shape, `--only=disks,lbs,addresses` toggles.
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
//   - addresses (#231, the fleet-audit `idle-address` slug): reserved
//     external static IPs that nothing uses. The claim turns on a
//     state the provider reports (RESERVED, no users), not on an
//     inference, which is what keeps it inside the §5 bar the
//     stale-object sweeper failed. Internal static addresses are out
//     of scope: they are not billed idle, so they are not waste.
//     --min-age applies to the reservation time — the provider
//     records no "last released" time, so an address freed an hour
//     ago from a two-year-old reservation is reported, and one
//     reserved an hour ago for a load balancer about to be created is
//     not. The same undatable rule as disks: reported, age unknown.
//   - nodepools (#557, the fleet-audit `idle-nodepool` slug): node
//     pools with at least one node and no workload pod on any of them.
//     OPT-IN — not in the default --only — because unlike the three
//     classes above it is judged on one observation rather than on a
//     terminal provider state, so a batch pool between runs reads as
//     idle. It needs the cluster-config capability rather than the
//     orphans one, plus a Node and a Pod List; the claim, its
//     exclusions and its degradations are spelled out in nodepools.go.
//
// Each class needs one provider capability (orphans for disks, lbs and
// addresses; cluster-config for nodepools). When NO selected class can
// run, the command degrades exactly as every cloud command does: one
// cloud.unavailable per missing capability and the unavailable= note.
// When only some can, the missing capability is reported the same way
// and the rest are still swept.
//
// A class the provider refuses (cloud.ErrPermissionDenied — on GKE a
// 403, e.g. no compute.addresses.list) degrades alone: one
// cloud.unavailable finding naming the class and permission, an
// unavailable= summary note, and the other classes still swept, exit
// 0. Any other sweep error stays a runtime error (exit 1).

import (
	"context"
	"fmt"
	"slices"
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
		Summary: "Billing-active cloud leftovers: unattached GCE disks and reserved-but-unused external static IPs older than --min-age, and forwarding rules/LBs routing to zero endpoints; opt-in --only=nodepools adds node pools with nodes but no workload pods — cost and hygiene sweep, not an incident read.",
		Flags: []emit.FlagSpec{
			{Name: "only", Type: emit.FlagString, Default: "disks,lbs,addresses",
				Help: "resource classes to sweep, comma-separated: disks, lbs, addresses, nodepools (nodepools is opt-in: never in the default, judged on one observation, and reads cluster Nodes and Pods)"},
			{Name: "min-age", Type: emit.FlagDuration, Default: defaultOrphanMinAge.String(),
				Help: "report a disk only when unattached at least this long (age from last detach, else creation), and an address only when reserved at least this long; disks and addresses with no datable age are always reported"},
		},
		Kinds: []checks.KindField{
			checks.Kind("orphan.disk", "a GCE disk has been unattached for at least --min-age and is still billing", emit.SeverityWarning),
			checks.Kind("orphan.lb", "a forwarding rule or load balancer routes to zero endpoints and is still billing", emit.SeverityWarning),
			checks.Kind("orphan.address", "an external static IP has been reserved for at least --min-age, nothing uses it, and it is still billing", emit.SeverityWarning),
			checks.Kind(kindIdleNodePool, "--only=nodepools: a node pool has at least one node and no workload pod scheduled on any of them (DaemonSet-owned and mirror/static pods do not count), judged on this one observation — info, because a batch pool between runs looks the same; the reason names the autoscaler case and so the remedy", emit.SeverityInfo),
			checks.CloudUnavailableKind(),
			checks.UnreadKind(),
		},
		Output: append([]checks.OutputField{
			{Name: "zone", Doc: "orphan.disk: the disk's zone"},
			{Name: "size_gb", Doc: "orphan.disk: provisioned size in GB (billed whether used or not)"},
			{Name: "disk_type", Doc: "orphan.disk: disk type short name (pd-ssd bills ~4x pd-standard idle)"},
			{Name: "unused_since", Doc: "orphan.disk: last detach (or creation, if never attached), RFC3339; omitted when the provider cannot date it"},
			{Name: "unused_for", Doc: "orphan.disk: how long the disk has been unattached; \"unknown\" when undatable"},
			{Name: "region", Doc: "orphan.lb, orphan.address: the forwarding rule's or address's region (\"global\" for global ones)"},
			{Name: "why", Doc: "orphan.lb: the provider's orphan judgment (e.g. which backend resolved empty)"},
			{Name: "address", Doc: "orphan.address: the reserved IP"},
			{Name: "network_tier", Doc: "orphan.address: the address's network tier (PREMIUM or STANDARD); omitted when the provider records none"},
			{Name: "reserved_since", Doc: "orphan.address: when the address was reserved, RFC3339 — the provider records no release time, so this bounds the idle time from above; omitted when undatable"},
			{Name: "reserved_for", Doc: "orphan.address: how long ago the address was reserved; \"unknown\" when undatable"},
			{Name: "node_count", Doc: "orphan.nodepool: the pool's current nodes, counted from the cluster's Nodes by their cloud.google.com/gke-nodepool label (the provider record carries no live count); never 0 — a pool at zero nodes costs nothing and is not reported"},
			{Name: "excluded_pods", Doc: "orphan.nodepool: running DaemonSet-owned and mirror (static) pods on the pool's nodes — present, but not workload; 0 when the nodes run nothing at all"},
			{Name: "machine_type", Doc: "orphan.nodepool: the pool's machine type, what each idle node bills as; omitted when the provider record names none"},
			{Name: "autoscaling", Doc: "orphan.nodepool: enabled or disabled — whether the autoscaler can shrink the pool at all"},
			{Name: "min_node_count", Doc: "orphan.nodepool: the autoscaler's per-zone minimum; a non-zero floor is what keeps an idle autoscaled pool up (reason IdleMinNodeCount). Omitted when autoscaling is disabled or the pool uses total limits"},
			{Name: "max_node_count", Doc: "orphan.nodepool: the autoscaler's per-zone maximum; omitted when autoscaling is disabled or the pool uses total limits"},
			{Name: "total_min_node_count", Doc: "orphan.nodepool: the autoscaler's pool-wide minimum, reported instead of min_node_count when the pool sets total limits"},
			{Name: "total_max_node_count", Doc: "orphan.nodepool: the autoscaler's pool-wide maximum, reported instead of max_node_count when the pool sets total limits"},
			{Name: "autoprovisioned", Doc: "orphan.nodepool: true when node auto-provisioning created the pool (and deletes it once empty); omitted otherwise"},
			{Name: "nodepools_skipped", Doc: "summary-line note: why --only=nodepools swept no pools although the provider answered (an Autopilot cluster: node pools are provider-managed and billed per pod)"},
			{Name: "class", Doc: "cloud.unavailable (reason=PermissionDenied): the --only class whose sweep the provider refused (disks, lbs, addresses, nodepools); the other selected classes are still swept, and the summary's unavailable= note lists each refused class with its permission"},
			{Name: "permission", Doc: "cloud.unavailable (reason=PermissionDenied): the provider permission the refused sweep needed (e.g. compute.addresses.list, container.clusters.get); omitted when the provider did not name it"},
			{Name: "capability", Doc: "cloud.unavailable: the provider capability a selected class needed (orphans for disks, lbs and addresses; cluster-config for nodepools)"},
			{Name: "provider", Doc: "cloud.unavailable: the provider that was asked"},
			{Name: "unavailable", Doc: "summary-line note (§2 marker): why the cloud read could not be served — the capability reason when no selected class could run, else one class: cause fragment per class that could not (a missing capability, a refused permission, or a forbidden Node/Pod List for nodepools)"},
		}, checks.UnreadFields()...),
		Examples: []string{
			"lookout cloud orphans",
			"lookout cloud orphans --only=disks --min-age=72h",
			"lookout cloud orphans --only=lbs --format=json",
			"lookout cloud orphans --only=addresses",
			"lookout cloud orphans --only=nodepools",
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
	want, err := parseOnly(inv.Flags.String("only"))
	if err != nil {
		return 0, err
	}

	provider, err := deps.provider(ctx)
	if err != nil {
		return 0, err
	}
	api, hasOrphans := provider.Orphans()
	cc, hasConfig := provider.ClusterConfig()

	now := deps.now()
	plain := func(fn func() (int, error)) func() (int, string, error) {
		return func() (int, string, error) {
			n, err := fn()
			return n, "", err
		}
	}
	sweeps := []struct {
		class, label string
		on           bool
		capability   cloud.Capability
		available    bool
		run          func() (int, string, error)
	}{
		{"disks", "disk sweep", want.disks, cloud.CapabilityOrphans, hasOrphans,
			plain(func() (int, error) { return sweepDisks(ctx, api, inv, now, minAge) })},
		{"lbs", "load-balancer sweep", want.lbs, cloud.CapabilityOrphans, hasOrphans,
			plain(func() (int, error) { return sweepLBs(ctx, api, inv) })},
		{"addresses", "address sweep", want.addresses, cloud.CapabilityOrphans, hasOrphans,
			plain(func() (int, error) { return sweepAddresses(ctx, api, inv, now, minAge) })},
		{"nodepools", "node-pool sweep", want.nodepools, cloud.CapabilityClusterConfig, hasConfig,
			func() (int, string, error) { return sweepNodePools(ctx, cc, deps, inv) }},
	}

	// Missing capabilities first: one cloud.unavailable per capability,
	// however many selected classes needed it.
	var missing []cloud.Capability
	runnable := 0
	for _, s := range sweeps {
		switch {
		case !s.on:
		case s.available:
			runnable++
		case !slices.Contains(missing, s.capability):
			missing = append(missing, s.capability)
		}
	}
	var refused, reasons []string
	for _, c := range missing {
		reason, err := emitCapabilityUnavailable(inv, provider, c, "cloud orphans")
		if err != nil {
			return 0, err
		}
		if !slices.Contains(reasons, reason) {
			reasons = append(reasons, reason)
		}
	}
	if runnable == 0 {
		// Nothing selected could run: the standard §2 record.
		if err := inv.Out.Note("unavailable", strings.Join(reasons, "; ")); err != nil {
			return 0, err
		}
		return 0, nil
	}

	scanned := 0
	for _, s := range sweeps {
		if !s.on {
			continue
		}
		if !s.available {
			refused = append(refused, s.class+": "+cloud.Unavailable(provider, s.capability).Reason)
			continue
		}
		n, degraded, err := s.run()
		if degraded != "" {
			refused = append(refused, degraded)
		}
		if permission, denied := cloud.PermissionDenied(err); denied {
			// A refused class degrades alone (#231, the #546 rule:
			// a forbidden read is reported, not fatal) — the default
			// --only gained addresses, and an identity without
			// compute.addresses.list must keep getting its disk and
			// lb answer from the same invocation.
			note, err := emitClassDenied(inv, provider, s.class, permission)
			if err != nil {
				return 0, err
			}
			refused = append(refused, note)
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("%s: %w", s.label, err)
		}
		scanned += n
	}
	if len(refused) > 0 {
		if err := inv.Out.Note("unavailable", strings.Join(refused, "; ")); err != nil {
			return 0, err
		}
	}
	return scanned, nil
}

// emitClassDenied is the per-class twin of emitUnavailable: one
// cloud.unavailable finding naming the refused class and, when the
// provider knows it, the permission to grant. It returns the
// summary-note fragment for the class.
func emitClassDenied(inv emit.Invocation, p cloud.Provider, class, permission string) (string, error) {
	need := "permission denied"
	if permission != "" {
		need = "needs " + permission
	}
	details := []emit.Field{
		{Key: "capability", Value: string(cloud.CapabilityOrphans)},
		{Key: "provider", Value: p.Name()},
		{Key: "class", Value: class},
	}
	if permission != "" {
		details = append(details, emit.Field{Key: "permission", Value: permission})
	}
	if err := inv.Out.Emit(emit.Finding{
		Kind:     "cloud.unavailable",
		Severity: emit.SeverityInfo,
		Reason:   "PermissionDenied",
		Message:  fmt.Sprintf("cloud orphans could not sweep %s: the cloud identity was refused (%s) — no %s were examined; the other selected classes were still swept", class, need, class),
		Details:  details,
	}); err != nil {
		return "", err
	}
	return class + ": " + need, nil
}

// sweepDisks reports unattached disks at least minAge idle; it
// returns how many unattached disks it examined.
func sweepDisks(ctx context.Context, api cloud.OrphanAPI, inv emit.Invocation, now time.Time, minAge time.Duration) (int, error) {
	disks, err := api.OrphanDisks(ctx)
	if err != nil {
		return 0, err
	}
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
	return len(disks), nil
}

// sweepLBs reports forwarding rules routing to zero endpoints; it
// returns how many orphaned rules the provider judged.
func sweepLBs(ctx context.Context, api cloud.OrphanAPI, inv emit.Invocation) (int, error) {
	lbs, err := api.OrphanLoadBalancers(ctx)
	if err != nil {
		return 0, err
	}
	sort.Slice(lbs, func(i, j int) bool { return lbs[i].Name < lbs[j].Name })
	for _, lb := range lbs {
		if err := inv.Out.Emit(lbFinding(lb)); err != nil {
			return 0, err
		}
	}
	return len(lbs), nil
}

// sweepAddresses reports idle external static IPs reserved at least
// minAge; it returns how many idle addresses it examined.
func sweepAddresses(ctx context.Context, api cloud.OrphanAPI, inv emit.Invocation, now time.Time, minAge time.Duration) (int, error) {
	addrs, err := api.OrphanAddresses(ctx)
	if err != nil {
		return 0, err
	}
	var old []cloud.OrphanAddress
	for _, a := range addrs {
		// Zero ReservedSince = undatable — always reported, as for
		// disks; otherwise apply --min-age.
		if a.ReservedSince.IsZero() || now.Sub(a.ReservedSince) >= minAge {
			old = append(old, a)
		}
	}
	sort.Slice(old, func(i, j int) bool {
		if old[i].Region != old[j].Region {
			return old[i].Region < old[j].Region
		}
		return old[i].Name < old[j].Name
	})
	for _, a := range old {
		if err := inv.Out.Emit(addressFinding(a, now)); err != nil {
			return 0, err
		}
	}
	return len(addrs), nil
}

// orphanClasses is the parsed --only selection.
type orphanClasses struct {
	disks, lbs, addresses, nodepools bool
}

// parseOnly validates the --only class list.
func parseOnly(only string) (orphanClasses, error) {
	var want orphanClasses
	for _, c := range strings.Split(only, ",") {
		switch strings.TrimSpace(c) {
		case "disks":
			want.disks = true
		case "lbs":
			want.lbs = true
		case "addresses":
			want.addresses = true
		case "nodepools":
			want.nodepools = true
		case "":
		default:
			return orphanClasses{}, emit.UsageErrorf("--only accepts disks,lbs,addresses,nodepools — unknown class %q", strings.TrimSpace(c))
		}
	}
	if !want.disks && !want.lbs && !want.addresses && !want.nodepools {
		return orphanClasses{}, emit.UsageErrorf("--only selected nothing: pass one or more of disks, lbs, addresses, nodepools")
	}
	return want, nil
}

func addressFinding(a cloud.OrphanAddress, now time.Time) emit.Finding {
	age, when := "unknown", "reservation time unknown"
	if !a.ReservedSince.IsZero() {
		age = now.Sub(a.ReservedSince).Truncate(time.Minute).String()
		when = "reserved " + age + " ago"
	}
	f := emit.Finding{
		Kind:         "orphan.address",
		Severity:     emit.SeverityWarning,
		KindOfObject: "Address",
		Name:         a.Name,
		Reason:       "ReservedUnusedAddress",
		Message:      fmt.Sprintf("external static IP %s is reserved and attached to nothing (%s) — billed until released", a.Address, when),
		Details: []emit.Field{
			{Key: "region", Value: a.Region},
			{Key: "address", Value: a.Address},
		},
	}
	if a.Tier != "" {
		f.Details = append(f.Details, emit.Field{Key: "network_tier", Value: a.Tier})
	}
	if !a.ReservedSince.IsZero() {
		f.Details = append(f.Details, emit.Field{Key: "reserved_since", Value: fmtTime(a.ReservedSince)})
	}
	f.Details = append(f.Details, emit.Field{Key: "reserved_for", Value: age})
	return f
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
