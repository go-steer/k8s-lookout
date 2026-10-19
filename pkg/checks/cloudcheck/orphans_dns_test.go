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

package cloudcheck_test

// #716: an idle reserved address that DNS still points at is not a
// release candidate — releasing it dangles every name resolving to it.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/cloudcheck"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// dnsFixture is orphanFixture with records in two zones: a public
// wildcard A record and a private CNAME both reaching spare-egress-ip,
// and a record for just-reserved, which --min-age filters out before
// the DNS check is asked anything.
func dnsFixture() *fakeOrphans {
	api := orphanFixture()
	api.dnsRecords = []cloud.DNSRecord{
		{Name: "*.example.test.", Type: "A", Zone: "public-zone", Address: "35.196.1.2"},
		{Name: "api.internal.example.test.", Type: "CNAME", Zone: "private-zone", Address: "35.196.1.2"},
		{Name: "next.example.test.", Type: "A", Zone: "public-zone", Address: "35.196.1.9"},
	}
	return api
}

// addressRecords runs the command and returns its orphan.address
// records by name, plus the summary line.
func addressRecords(t *testing.T, api *fakeOrphans, args ...string) (map[string]map[string]string, map[string]string) {
	t.Helper()
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd, args...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0; stderr: %s", res.Code, res.Stderr)
	}
	byName := map[string]map[string]string{}
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "orphan.address" {
			byName[r["name"]] = r
		}
	}
	return byName, summaryLine(t, res.Stdout)
}

func TestOrphansAddressReferencedByDNS(t *testing.T) {
	api := dnsFixture()
	addrs, sum := addressRecords(t, api, "--only=addresses")

	// Asked about exactly the addresses about to be reported.
	slices.Sort(api.dnsAsked)
	if !slices.Equal(api.dnsAsked, []string{"34.120.10.20", "35.196.1.2"}) {
		t.Errorf("DNS check asked about %v, want the two reported addresses only", api.dnsAsked)
	}
	if len(addrs) != 2 {
		t.Fatalf("address records = %v, want old-ingress-ip and spare-egress-ip", addrs)
	}

	spare := addrs["spare-egress-ip"]
	if spare["severity"] != emit.SeverityInfo || spare["reason"] != "ReferencedByDNS" || spare["dns_check"] != "referenced" {
		t.Errorf("spare-egress-ip = %v, want info ReferencedByDNS dns_check=referenced", spare)
	}
	// Records from both zones are named, with their zone.
	for _, want := range []string{"*.example.test. A in zone public-zone", "api.internal.example.test. CNAME in zone private-zone"} {
		if !strings.Contains(spare["dns_records"], want) || !strings.Contains(spare["message"], want) {
			t.Errorf("spare-egress-ip dns_records/message missing %q: %v", want, spare)
		}
	}
	for _, bad := range []string{"billed until released", "next.example.test."} {
		if strings.Contains(spare["message"], bad) {
			t.Errorf("spare-egress-ip message carries %q: %s", bad, spare["message"])
		}
	}
	if !strings.Contains(spare["message"], "not a cleanup candidate") {
		t.Errorf("spare-egress-ip message = %q, want it to say it is not a cleanup candidate", spare["message"])
	}

	// No record for old-ingress-ip: the release recommendation stands,
	// scoped to what was checked.
	old := addrs["old-ingress-ip"]
	if old["severity"] != emit.SeverityWarning || old["reason"] != "ReservedUnusedAddress" || old["dns_check"] != "none_found" || old["dns_records"] != "" {
		t.Errorf("old-ingress-ip = %v, want warning ReservedUnusedAddress dns_check=none_found", old)
	}
	if !strings.Contains(old["message"], "billed until released") || !strings.Contains(old["message"], "DNS hosted elsewhere is not checked") {
		t.Errorf("old-ingress-ip message = %q", old["message"])
	}
	if sum["unavailable"] != "" || sum["scanned"] != "3" {
		t.Errorf("summary = %v, want scanned=3 and no unavailable note", sum)
	}
}

func TestOrphansAddressDNSGolden(t *testing.T) {
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: dnsFixture()}))
	res := checktest.Run(t, cmd, "--only=addresses")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/orphans_dns.golden", res.Stdout)
}

func TestOrphansAddressDNSContract(t *testing.T) {
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: dnsFixture()}))
	checktest.VerifyContract(t, cmd)
}

// TestOrphansAddressDNSUnavailable: a DNS check that cannot run keeps
// every address a warning, says why on each record and in the
// unavailable= note, softens the advice — and costs no other class.
func TestOrphansAddressDNSUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name, why string
		err       error
	}{
		{"permission denied", "needs dns.managedZones.list", denied("dns.managedZones.list")},
		{"refused, permission unnamed", "permission denied", &cloud.PermissionDeniedError{Err: errors.New("googleapi: Error 403: Forbidden")}},
		{"API disabled", "listing Cloud DNS managed zones: the Cloud DNS API (dns.googleapis.com) is not enabled in the project",
			errors.New("listing Cloud DNS managed zones: the Cloud DNS API (dns.googleapis.com) is not enabled in the project")},
		{"provider error", "listing record sets in Cloud DNS zone public-zone: googleapi: Error 500: Internal Error",
			errors.New("listing record sets in Cloud DNS zone public-zone: googleapi: Error 500: Internal Error")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := dnsFixture()
			api.dnsErr = tc.err
			cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
			res := checktest.Run(t, cmd)
			if res.Code != emit.ExitData {
				t.Fatalf("exit %d, want 0; stderr: %s", res.Code, res.Stderr)
			}
			kinds := map[string]int{}
			for _, r := range findingLines(t, res.Stdout) {
				kinds[r["kind"]]++
				if r["kind"] != "orphan.address" {
					continue
				}
				if r["severity"] != emit.SeverityWarning || r["reason"] != "ReservedUnusedAddress" ||
					r["dns_check"] != "unavailable" || r["dns_unavailable"] != tc.why || r["dns_records"] != "" {
					t.Errorf("%s = %v, want warning ReservedUnusedAddress dns_check=unavailable dns_unavailable=%q", r["name"], r, tc.why)
				}
				if !strings.Contains(r["message"], "verify no DNS record points at it before releasing") ||
					!strings.Contains(r["message"], tc.why) {
					t.Errorf("%s message = %q, want the softened advice naming why", r["name"], r["message"])
				}
			}
			if kinds["orphan.address"] != 2 || kinds["orphan.disk"] != 3 || kinds["orphan.lb"] != 2 || kinds["cloud.unavailable"] != 0 {
				t.Errorf("kinds = %v, want every class swept and both addresses kept", kinds)
			}
			want := "addresses: DNS check not run (" + tc.why + ")"
			if sum := summaryLine(t, res.Stdout); sum["unavailable"] != want {
				t.Errorf("unavailable = %q, want %q", sum["unavailable"], want)
			}
		})
	}
}

// TestOrphansAddressDNSSkippedWhenNothingReported: no address past
// --min-age, no DNS read (and no note claiming one failed).
func TestOrphansAddressDNSSkippedWhenNothingReported(t *testing.T) {
	api := dnsFixture()
	api.dnsErr = denied("dns.managedZones.list")
	addrs, sum := addressRecords(t, api, "--only=addresses", "--min-age=100000h")
	if len(addrs) != 1 || addrs["old-ingress-ip"] == nil || addrs["old-ingress-ip"]["dns_check"] != "unavailable" || sum["unavailable"] == "" {
		t.Fatalf("records = %v, summary = %v; want only the undatable old-ingress-ip, DNS-checked", addrs, sum)
	}
	api = dnsFixture()
	api.addrs = api.addrs[:2] // drop the undatable one
	addrs, sum = addressRecords(t, api, "--only=addresses", "--min-age=100000h")
	if len(addrs) != 0 || api.dnsCalled || sum["unavailable"] != "" {
		t.Errorf("records=%v dnsCalled=%v unavailable=%q, want no records, no DNS read, no note", addrs, api.dnsCalled, sum["unavailable"])
	}
}

// TestOrphansAddressDNSRecordsCapped: an address many names point at
// lists the first five and counts the rest.
func TestOrphansAddressDNSRecordsCapped(t *testing.T) {
	api := dnsFixture()
	api.dnsRecords = nil
	for i := range 8 {
		api.dnsRecords = append(api.dnsRecords, cloud.DNSRecord{Name: fmt.Sprintf("host%d.example.test.", i), Type: "A", Zone: "public-zone", Address: "35.196.1.2"})
	}
	addrs, _ := addressRecords(t, api, "--only=addresses")
	recs := addrs["spare-egress-ip"]["dns_records"]
	if !strings.Contains(recs, "host4.example.test.") || strings.Contains(recs, "host5.example.test.") || !strings.HasSuffix(recs, ", and 3 more") {
		t.Errorf("dns_records = %q, want hosts 0-4 then \"and 3 more\"", recs)
	}
}

// TestOrphansAddressDNSBudget: a DNS check still running when its
// share of --timeout ends degrades like any other failure, so the
// addresses are still reported with the caveat and exit 0; one that
// outlives the invocation itself is the --timeout runtime error.
func TestOrphansAddressDNSBudget(t *testing.T) {
	api := &slowDNS{fakeOrphans: dnsFixture(), honorContext: true}
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd, "--only=addresses", "--timeout=200ms")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0; stdout: %s stderr: %s", res.Code, res.Stdout, res.Stderr)
	}
	if n := strings.Count(res.Stdout, `dns_unavailable="ran out of its share of --timeout"`); n != 2 {
		t.Errorf("budget-expired records = %d, want 2:\n%s", n, res.Stdout)
	}

	api = &slowDNS{fakeOrphans: dnsFixture()}
	cmd = cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res = checktest.Run(t, cmd, "--only=addresses", "--timeout=50ms")
	if res.Code != emit.ExitRuntime {
		t.Fatalf("exit %d, want %d (runtime); stdout: %s", res.Code, emit.ExitRuntime, res.Stdout)
	}
	if strings.Contains(res.Stdout, "dns_check=unavailable") {
		t.Errorf("a timed-out invocation degraded the DNS check:\n%s", res.Stdout)
	}
}

// slowDNS blocks the DNS check: until its context ends when
// honorContext, else past any test --timeout.
type slowDNS struct {
	*fakeOrphans
	honorContext bool
}

func (s *slowDNS) AddressDNSRecords(ctx context.Context, _ []string) ([]cloud.DNSRecord, error) {
	if s.honorContext {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	time.Sleep(150 * time.Millisecond)
	return nil, errors.New("too late")
}
