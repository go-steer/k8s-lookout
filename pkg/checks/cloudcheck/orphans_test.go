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

import (
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/cloudcheck"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// orphanFixture: one big 3-day-idle pd-ssd, one small week-idle
// pd-standard, one FRESH detach (2h — below the 24h default), one
// undatable disk, two orphaned forwarding rules, and three idle
// addresses: one reserved months ago, one reserved an hour ago (below
// the 24h default — a load balancer about to claim it), and one the
// provider cannot date.
func orphanFixture() *fakeOrphans {
	return &fakeOrphans{
		addrs: []cloud.OrphanAddress{
			{Name: "spare-egress-ip", Region: "us-east1", Address: "35.196.1.2", Tier: "STANDARD", ReservedSince: fixedNow.Add(-30 * 24 * time.Hour)},
			{Name: "just-reserved", Region: "us-east1", Address: "35.196.1.9", Tier: "PREMIUM", ReservedSince: fixedNow.Add(-1 * time.Hour)},
			{Name: "old-ingress-ip", Region: "global", Address: "34.120.10.20"},
		},
		disks: []cloud.OrphanDisk{
			{Name: "small-old", Zone: "us-east1-b", SizeGB: 10, Type: "pd-standard", UnusedSince: fixedNow.Add(-7 * 24 * time.Hour)},
			{Name: "big-idle", Zone: "us-east1-c", SizeGB: 500, Type: "pd-ssd", UnusedSince: fixedNow.Add(-72 * time.Hour)},
			{Name: "fresh-detach", Zone: "us-east1-b", SizeGB: 200, Type: "pd-ssd", UnusedSince: fixedNow.Add(-2 * time.Hour)},
			{Name: "undatable", Zone: "us-east1-b", SizeGB: 50, Type: "pd-balanced"},
		},
		lbs: []cloud.OrphanLoadBalancer{
			{Name: "web-rule", Region: "us-east1", Reason: "backend service web-bs has 0 endpoints across all groups"},
			{Name: "api-rule", Region: "global", Reason: "url map api-um routes to no backend service"},
		},
	}
}

func TestOrphansSweep(t *testing.T) {
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: orphanFixture()}))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)

	var disks, lbs, addrs []map[string]string
	for _, r := range recs {
		switch r["kind"] {
		case "orphan.disk":
			disks = append(disks, r)
		case "orphan.lb":
			lbs = append(lbs, r)
		case "orphan.address":
			addrs = append(addrs, r)
		default:
			t.Errorf("unexpected kind %q: %v", r["kind"], r)
		}
	}

	// fresh-detach (2h < 24h min-age) is filtered; the other three
	// appear, biggest bill first.
	if len(disks) != 3 {
		t.Fatalf("disk findings = %v, want 3 (fresh detach filtered)", disks)
	}
	if disks[0]["name"] != "big-idle" || disks[1]["name"] != "undatable" || disks[2]["name"] != "small-old" {
		t.Errorf("disk order = %s,%s,%s, want big-idle,undatable,small-old (size descending)",
			disks[0]["name"], disks[1]["name"], disks[2]["name"])
	}
	big := disks[0]
	if big["severity"] != emit.SeverityWarning || big["reason"] != "UnattachedDisk" ||
		big["size_gb"] != "500" || big["disk_type"] != "pd-ssd" || big["zone"] != "us-east1-c" ||
		big["unused_for"] != "72h0m0s" || big["unused_since"] != "2026-07-22T12:00:00Z" {
		t.Errorf("big-idle = %v, want warning UnattachedDisk 500GB pd-ssd 72h idle", big)
	}
	// The undatable disk is reported, explicitly unknown — never
	// silently dropped.
	und := disks[1]
	if und["unused_for"] != "unknown" || und["unused_since"] != "" {
		t.Errorf("undatable = %v, want unused_for=unknown and no unused_since", und)
	}

	if len(lbs) != 2 || lbs[0]["name"] != "api-rule" || lbs[1]["name"] != "web-rule" {
		t.Fatalf("lb findings = %v, want api-rule,web-rule (name order)", lbs)
	}
	if lbs[0]["severity"] != emit.SeverityWarning || lbs[0]["reason"] != "NoBackendEndpoints" ||
		lbs[0]["region"] != "global" || !strings.Contains(lbs[0]["why"], "no backend service") {
		t.Errorf("api-rule = %v, want warning NoBackendEndpoints global with the provider judgment in why", lbs[0])
	}

	// just-reserved (1h < 24h min-age) is filtered; the undatable
	// global address is reported with its age unknown. Region, then
	// name order.
	if len(addrs) != 2 || addrs[0]["name"] != "old-ingress-ip" || addrs[1]["name"] != "spare-egress-ip" {
		t.Fatalf("address findings = %v, want old-ingress-ip,spare-egress-ip (fresh reservation filtered)", addrs)
	}
	spare := addrs[1]
	if spare["severity"] != emit.SeverityWarning || spare["reason"] != "ReservedUnusedAddress" ||
		spare["kind_of_object"] != "Address" || spare["region"] != "us-east1" || spare["address"] != "35.196.1.2" ||
		spare["network_tier"] != "STANDARD" || spare["reserved_for"] != "720h0m0s" || spare["reserved_since"] != "2026-06-25T12:00:00Z" {
		t.Errorf("spare-egress-ip = %v, want warning ReservedUnusedAddress us-east1 STANDARD reserved 720h", spare)
	}
	if und := addrs[0]; und["reserved_for"] != "unknown" || und["reserved_since"] != "" || und["network_tier"] != "" {
		t.Errorf("old-ingress-ip = %v, want reserved_for=unknown, no reserved_since, no network_tier", und)
	}

	// scanned = 4 disks examined + 2 orphaned rules + 3 idle
	// addresses examined.
	if sum := summaryLine(t, res.Stdout); sum["scanned"] != "9" {
		t.Errorf("scanned = %s, want 9", sum["scanned"])
	}
}

func TestOrphansOnlyToggles(t *testing.T) {
	api := orphanFixture()
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd, "--only=disks")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !api.disksCalled || api.lbsCalled || api.addrsCalled {
		t.Errorf("--only=disks called disks=%v lbs=%v addresses=%v, want the lb and address sweeps skipped entirely", api.disksCalled, api.lbsCalled, api.addrsCalled)
	}
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "orphan.lb" {
			t.Errorf("lb finding under --only=disks: %v", r)
		}
	}
	if sum := summaryLine(t, res.Stdout); sum["scanned"] != "4" {
		t.Errorf("scanned = %s, want 4 (disks only)", sum["scanned"])
	}

	api = orphanFixture()
	cmd = cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res = checktest.Run(t, cmd, "--only=lbs")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if api.disksCalled || !api.lbsCalled || api.addrsCalled {
		t.Errorf("--only=lbs called disks=%v lbs=%v addresses=%v, want the disk and address sweeps skipped", api.disksCalled, api.lbsCalled, api.addrsCalled)
	}

	// --only=addresses sweeps addresses alone; scanned counts the
	// three idle addresses examined, the filtered fresh one included.
	api = orphanFixture()
	cmd = cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res = checktest.Run(t, cmd, "--only=addresses")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if api.disksCalled || api.lbsCalled || !api.addrsCalled {
		t.Errorf("--only=addresses called disks=%v lbs=%v addresses=%v, want only the address sweep", api.disksCalled, api.lbsCalled, api.addrsCalled)
	}
	if sum := summaryLine(t, res.Stdout); sum["scanned"] != "3" {
		t.Errorf("scanned = %s, want 3 (addresses only)", sum["scanned"])
	}

	// The pre-#231 default stays reachable by naming it.
	api = orphanFixture()
	cmd = cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res = checktest.Run(t, cmd, "--only=disks,lbs")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !api.disksCalled || !api.lbsCalled || api.addrsCalled {
		t.Errorf("--only=disks,lbs called disks=%v lbs=%v addresses=%v, want the address sweep skipped", api.disksCalled, api.lbsCalled, api.addrsCalled)
	}

	res = checktest.Run(t, cmd, "--only=vms")
	if res.Code != emit.ExitUsage || !strings.Contains(res.Stderr, `unknown class "vms"`) {
		t.Errorf("--only=vms: exit %d stderr %q, want usage error", res.Code, res.Stderr)
	}
}

func TestOrphansMinAge(t *testing.T) {
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: orphanFixture()}))
	res := checktest.Run(t, cmd, "--only=disks", "--min-age=1h")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	names := map[string]bool{}
	for _, r := range findingLines(t, res.Stdout) {
		names[r["name"]] = true
	}
	if !names["fresh-detach"] || len(names) != 4 {
		t.Errorf("--min-age=1h names = %v, want all 4 disks including fresh-detach", names)
	}
}

func TestOrphansUnavailable(t *testing.T) {
	cmd := cloudcheck.OrphansCommand(testDeps(cloud.NoProvider))
	assertUnavailable(t, checktest.Run(t, cmd), "orphans")
}

func TestOrphansContract(t *testing.T) {
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: orphanFixture()}))
	checktest.VerifyContract(t, cmd)
}

func TestOrphansGolden(t *testing.T) {
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: orphanFixture()}))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/orphans.golden", res.Stdout)
}
