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

// AddressDNSRecords (#716): the address class's Cloud DNS cross-check.
// A reserved external address with no users is billed and attached to
// nothing, yet it can still be what DNS points at — the wildcard
// target of a whole domain, bound by name by a Gateway that is not
// deployed right now, or an address used by DNS alone. Releasing it
// would hand those names' traffic to whoever is next assigned the IP.
//
// The check lists the project's Cloud DNS managed zones (public and
// private alike — a private record pointing at the address breaks
// just as surely) and every record set in each, and matches:
//
//   - A/AAAA record data, including routing-policy data: geo and
//     weighted-round-robin items, their health-checked external
//     endpoints, and primary-backup targets;
//   - CNAMEs whose target reaches a matching record through the same
//     zones' records, up to maxCNAMEHops, falling back to the closest
//     wildcard ("*.example.com.") when no exact name exists. The
//     approximation errs toward "referenced", the safe side for a
//     release recommendation.
//
// Addresses compare as parsed IPs, so an AAAA written in another
// textual form still matches. DNS hosted outside Cloud DNS — or in
// another project's zones — is invisible here; the command says so.
//
// One Compute-style rule for errors: a 403 refusal becomes
// cloud.ErrPermissionDenied naming the permission
// (classifyPermission); a disabled API becomes a short apiDisabledError
// rather than the API's paragraph. A zone deleted between the list
// and its record read (404) holds nothing any more and is skipped.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"sort"
	"strings"

	dns "google.golang.org/api/dns/v1"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// The permissions the check's two list calls need (roles/dns.reader
// grants both).
const (
	permDNSZonesList   = "dns.managedZones.list"
	permDNSRecordsList = "dns.resourceRecordSets.list"
)

// maxCNAMEHops bounds a CNAME chain followed through the listed
// records (a loop is cut by the visited set before this).
const maxCNAMEHops = 8

// orphanDNSAPI is the §13 small client interface over the two Cloud
// DNS list calls the check needs. Production is cloudDNSClient; tests
// replay recorded responses.
type orphanDNSAPI interface {
	// ListManagedZones returns every managed zone in the project.
	ListManagedZones(ctx context.Context) ([]*dns.ManagedZone, error)
	// ListRecordSets returns every record set in one managed zone.
	ListRecordSets(ctx context.Context, zone string) ([]*dns.ResourceRecordSet, error)
}

// AddressDNSRecords implements cloud.OrphanAPI per the file comment.
func (o *orphanAPI) AddressDNSRecords(ctx context.Context, addresses []string) ([]cloud.DNSRecord, error) {
	want := map[netip.Addr]string{}
	for _, a := range addresses {
		if ip, err := netip.ParseAddr(strings.TrimSpace(a)); err == nil {
			want[ip.Unmap()] = a
		}
	}
	if len(want) == 0 {
		return nil, nil
	}
	if o.dns == nil {
		return nil, errors.New("no Cloud DNS client configured")
	}
	zones, err := o.dns.ListManagedZones(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing Cloud DNS managed zones: %w", dnsFailure(err, permDNSZonesList))
	}
	sort.Slice(zones, func(i, j int) bool { return zoneName(zones[i]) < zoneName(zones[j]) })
	idx := newDNSIndex(want)
	for _, z := range zones {
		name := zoneName(z)
		if name == "" {
			continue
		}
		sets, err := o.dns.ListRecordSets(ctx, name)
		if isStatus(err, http.StatusNotFound) {
			continue // deleted since the zone list: it holds nothing now
		}
		if err != nil {
			return nil, fmt.Errorf("listing record sets in Cloud DNS zone %s: %w", name, dnsFailure(err, permDNSRecordsList))
		}
		for _, rs := range sets {
			idx.add(name, rs)
		}
	}
	return idx.records(), nil
}

func zoneName(z *dns.ManagedZone) string {
	if z == nil {
		return ""
	}
	return z.Name
}

// apiDisabledError is a 403 that means the API is off in the project:
// a short, actionable message in place of the API's paragraph, with
// the original kept for errors.As.
type apiDisabledError struct {
	api string
	err error
}

func (e *apiDisabledError) Error() string { return "the " + e.api + " is not enabled in the project" }
func (e *apiDisabledError) Unwrap() error { return e.err }

// dnsFailure classifies a Cloud DNS call's error: a disabled API, a
// permission refusal naming permission, or anything else unchanged.
func dnsFailure(err error, permission string) error {
	if apiDisabled(err) {
		return &apiDisabledError{api: "Cloud DNS API (dns.googleapis.com)", err: err}
	}
	return classifyPermission(err, permission)
}

// dnsIndex accumulates the record sets of every zone, then resolves
// which names reach the wanted addresses.
type dnsIndex struct {
	want map[netip.Addr]string
	// hits maps a canonical owner name to the wanted addresses its
	// A/AAAA data names.
	hits map[string][]netip.Addr
	// targets maps a canonical owner name to its CNAME targets
	// (several when split-horizon zones disagree).
	targets map[string][]string
	// names is every owner name the zones hold, of any type: a name
	// that exists is never answered by a wildcard.
	names  map[string]bool
	direct []cloud.DNSRecord
	cnames []cnameRecord
}

type cnameRecord struct {
	name, zone, target string
}

func newDNSIndex(want map[netip.Addr]string) *dnsIndex {
	return &dnsIndex{want: want, hits: map[string][]netip.Addr{}, targets: map[string][]string{}, names: map[string]bool{}}
}

// canonName lower-cases a DNS name and makes it fully qualified, so
// names from record data and owner names compare equal.
func canonName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s != "" && !strings.HasSuffix(s, ".") {
		s += "."
	}
	return s
}

func (x *dnsIndex) add(zone string, rs *dns.ResourceRecordSet) {
	if rs == nil || rs.Name == "" {
		return
	}
	owner := canonName(rs.Name)
	x.names[owner] = true
	switch typ := strings.ToUpper(rs.Type); typ {
	case "A", "AAAA":
		for _, data := range recordData(rs) {
			ip, err := netip.ParseAddr(strings.TrimSpace(data))
			if err != nil {
				continue
			}
			ip = ip.Unmap()
			if addr, ok := x.want[ip]; ok {
				x.direct = append(x.direct, cloud.DNSRecord{Name: rs.Name, Type: typ, Zone: zone, Address: addr})
				x.hits[owner] = append(x.hits[owner], ip)
			}
		}
	case "CNAME":
		for _, data := range recordData(rs) {
			target := canonName(data)
			if target == "" {
				continue
			}
			x.cnames = append(x.cnames, cnameRecord{name: rs.Name, zone: zone, target: target})
			x.targets[owner] = append(x.targets[owner], target)
		}
	}
}

// recordData is every value a record set can resolve to: its plain
// data, and its routing policy's items and health-checked external
// endpoints.
func recordData(rs *dns.ResourceRecordSet) []string {
	out := append([]string(nil), rs.Rrdatas...)
	p := rs.RoutingPolicy
	if p == nil {
		return out
	}
	targets := func(t *dns.RRSetRoutingPolicyHealthCheckTargets) {
		if t != nil {
			out = append(out, t.ExternalEndpoints...)
		}
	}
	geo := func(g *dns.RRSetRoutingPolicyGeoPolicy) {
		if g == nil {
			return
		}
		for _, it := range g.Items {
			if it != nil {
				out = append(out, it.Rrdatas...)
				targets(it.HealthCheckedTargets)
			}
		}
	}
	geo(p.Geo)
	if p.Wrr != nil {
		for _, it := range p.Wrr.Items {
			if it != nil {
				out = append(out, it.Rrdatas...)
				targets(it.HealthCheckedTargets)
			}
		}
	}
	if p.PrimaryBackup != nil {
		targets(p.PrimaryBackup.PrimaryTargets)
		geo(p.PrimaryBackup.BackupGeoTargets)
	}
	return out
}

// lookup is the index key a query for name lands on: the name itself
// when the zones hold it (any type), else the closest enclosing
// wildcard, else the name (which then reaches nothing).
func (x *dnsIndex) lookup(name string) string {
	if x.known(name) {
		return name
	}
	for n := name; ; {
		_, parent, ok := strings.Cut(n, ".")
		if !ok || parent == "" {
			return name
		}
		if w := "*." + parent; x.known(w) {
			return w
		}
		n = parent
	}
}

func (x *dnsIndex) known(name string) bool { return x.names[name] }

// reach is every wanted address name resolves to through the indexed
// records.
func (x *dnsIndex) reach(name string, seen map[string]bool, hops int) []netip.Addr {
	key := x.lookup(name)
	if hops > maxCNAMEHops || seen[key] {
		return nil
	}
	seen[key] = true
	out := append([]netip.Addr(nil), x.hits[key]...)
	for _, t := range x.targets[key] {
		out = append(out, x.reach(t, seen, hops+1)...)
	}
	return out
}

// records is the direct matches plus every CNAME whose chain reaches
// a wanted address, deduplicated and ordered by address, zone, name
// and type.
func (x *dnsIndex) records() []cloud.DNSRecord {
	out := append([]cloud.DNSRecord(nil), x.direct...)
	for _, c := range x.cnames {
		for _, ip := range x.reach(c.target, map[string]bool{}, 1) {
			out = append(out, cloud.DNSRecord{Name: c.name, Type: "CNAME", Zone: c.zone, Address: x.want[ip]})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Address != b.Address {
			return a.Address < b.Address
		}
		if a.Zone != b.Zone {
			return a.Zone < b.Zone
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.Type < b.Type
	})
	dedup := out[:0]
	for i, r := range out {
		if i == 0 || r != out[i-1] {
			dedup = append(dedup, r)
		}
	}
	return dedup
}

// cloudDNSClient is the production orphanDNSAPI over the Cloud DNS
// REST API (google.golang.org/api/dns/v1 — the same module as the
// Compute client, so no new dependency).
type cloudDNSClient struct {
	project string
	svc     func(ctx context.Context) (*dns.Service, error)
}

func newCloudDNSClient(project string) *cloudDNSClient {
	return &cloudDNSClient{
		project: project,
		svc: lazyClient(func(ctx context.Context) (*dns.Service, error) {
			return dns.NewService(ctx, clientOptions(ctx)...)
		}),
	}
}

func (c *cloudDNSClient) ListManagedZones(ctx context.Context) ([]*dns.ManagedZone, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	var out []*dns.ManagedZone
	err = svc.ManagedZones.List(c.project).Pages(ctx, func(page *dns.ManagedZonesListResponse) error {
		out = append(out, page.ManagedZones...)
		return nil
	})
	return out, err
}

func (c *cloudDNSClient) ListRecordSets(ctx context.Context, zone string) ([]*dns.ResourceRecordSet, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, err
	}
	var out []*dns.ResourceRecordSet
	err = svc.ResourceRecordSets.List(c.project, zone).Pages(ctx, func(page *dns.ResourceRecordSetsListResponse) error {
		out = append(out, page.Rrsets...)
		return nil
	})
	return out, err
}
