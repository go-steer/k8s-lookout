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
	"context"
	"errors"
	"fmt"
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

// denied builds the provider's wrapped permission refusal, as the
// GKE implementation returns it (a 403 classified at the provider
// seam, then wrapped with the call's context).
func denied(permission string) error {
	return fmt.Errorf("listing: %w", &cloud.PermissionDeniedError{
		Permission: permission,
		Err:        errors.New("googleapi: Error 403: Required '" + permission + "' permission for 'projects/p', forbidden"),
	})
}

// TestOrphansAddressesDenied is the #231 regression: the default
// --only gained addresses, so an identity that ran the old default
// fine but lacks compute.addresses.list must still get exit 0, its
// disk and lb findings, and one explicit unavailable for addresses
// naming the permission — not exit 1 from the same invocation.
func TestOrphansAddressesDenied(t *testing.T) {
	api := orphanFixture()
	api.addrsErr = denied("compute.addresses.list")
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0 (a refused class degrades, not fails); stderr: %s", res.Code, res.Stderr)
	}
	kinds := map[string]int{}
	var unavail []map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		kinds[r["kind"]]++
		if r["kind"] == "cloud.unavailable" {
			unavail = append(unavail, r)
		}
	}
	if kinds["orphan.disk"] != 3 || kinds["orphan.lb"] != 2 || kinds["orphan.address"] != 0 {
		t.Errorf("kinds = %v, want 3 orphan.disk, 2 orphan.lb, no orphan.address", kinds)
	}
	if len(unavail) != 1 {
		t.Fatalf("cloud.unavailable findings = %v, want exactly one (addresses)", unavail)
	}
	u := unavail[0]
	if u["reason"] != "PermissionDenied" || u["severity"] != emit.SeverityInfo ||
		u["capability"] != string(cloud.CapabilityOrphans) || u["provider"] != cloud.NoProviderName ||
		u["class"] != "addresses" || u["permission"] != "compute.addresses.list" ||
		!strings.Contains(u["message"], "compute.addresses.list") {
		t.Errorf("unavailable = %v, want PermissionDenied class=addresses permission=compute.addresses.list", u)
	}
	sum := summaryLine(t, res.Stdout)
	// scanned = 4 disks + 2 rules; the refused class examined nothing.
	if sum["scanned"] != "6" || sum["unavailable"] != "addresses: needs compute.addresses.list" {
		t.Errorf("summary = %v, want scanned=6 unavailable=\"addresses: needs compute.addresses.list\"", sum)
	}
}

// TestOrphansAllDenied: every class refused is still an answer —
// exit 0, three unavailables, nothing scanned. An unnamed permission
// is reported as such rather than guessed.
func TestOrphansAllDenied(t *testing.T) {
	api := orphanFixture()
	api.disksErr = denied("compute.disks.list")
	api.lbsErr = fmt.Errorf("resolving forwarding rule %q: %w", "web-rule", cloud.ErrPermissionDenied)
	api.addrsErr = denied("compute.addresses.list")
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0; stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 3 {
		t.Fatalf("records = %v, want three cloud.unavailable", recs)
	}
	want := []struct{ class, permission string }{
		{"disks", "compute.disks.list"},
		{"addresses", "compute.addresses.list"},
		{"lbs", ""}, // lbs runs last
	}
	for i, r := range recs {
		if r["kind"] != "cloud.unavailable" || r["reason"] != "PermissionDenied" ||
			r["class"] != want[i].class || r["permission"] != want[i].permission {
			t.Errorf("record %d = %v, want cloud.unavailable class=%s permission=%q", i, r, want[i].class, want[i].permission)
		}
	}
	sum := summaryLine(t, res.Stdout)
	wantNote := "disks: needs compute.disks.list; addresses: needs compute.addresses.list; lbs: permission denied"
	if sum["scanned"] != "0" || sum["unavailable"] != wantNote {
		t.Errorf("summary = %v, want scanned=0 unavailable=%q", sum, wantNote)
	}
}

// lbListErr is a class-level lb failure as the GKE provider returns
// it: the forwarding-rule list itself failed. (One rule's failure is
// RuleUnresolved instead — TestOrphansUnresolvedRules.)
const lbListErr = `listing forwarding rules: googleapi: Error 500: Internal error encountered., backendError`

// TestOrphansSweepFailedDegradesClass (#612): a non-permission
// provider error in the lb sweep costs that class one
// cloud.unavailable (reason=SweepFailed, class=lbs, the provider's
// error) — the disk and address findings still come, and the exit
// code stays 0.
func TestOrphansSweepFailedDegradesClass(t *testing.T) {
	api := orphanFixture()
	api.lbsErr = errors.New(lbListErr)
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0 (a failed class degrades, not fails); stderr: %s", res.Code, res.Stderr)
	}
	if !api.addrsCalled {
		t.Fatal("address sweep never ran after the lb sweep failed")
	}
	kinds := map[string]int{}
	var unavail []map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		kinds[r["kind"]]++
		if r["kind"] == "cloud.unavailable" {
			unavail = append(unavail, r)
		}
	}
	if kinds["orphan.disk"] != 3 || kinds["orphan.lb"] != 0 || kinds["orphan.address"] != 2 {
		t.Errorf("kinds = %v, want 3 orphan.disk, no orphan.lb, 2 orphan.address", kinds)
	}
	if len(unavail) != 1 {
		t.Fatalf("cloud.unavailable findings = %v, want exactly one (lbs)", unavail)
	}
	u := unavail[0]
	if u["reason"] != "SweepFailed" || u["severity"] != emit.SeverityInfo ||
		u["capability"] != string(cloud.CapabilityOrphans) || u["provider"] != cloud.NoProviderName ||
		u["class"] != "lbs" || u["permission"] != "" ||
		!strings.Contains(u["error"], "listing forwarding rules") ||
		!strings.Contains(u["error"], "Error 500") {
		t.Errorf("unavailable = %v, want SweepFailed class=lbs carrying the provider's 500", u)
	}
	sum := summaryLine(t, res.Stdout)
	// scanned = 4 disks + 3 idle addresses; the failed class adds none.
	if sum["scanned"] != "7" || sum["unavailable"] != "lbs: sweep failed" {
		t.Errorf("summary = %v, want scanned=7 unavailable=\"lbs: sweep failed\"", sum)
	}
}

// TestOrphansSweepFailedErrorBounded: a long provider error is cut
// on the record rather than dumped.
func TestOrphansSweepFailedErrorBounded(t *testing.T) {
	api := orphanFixture()
	api.disksErr = errors.New("listing disks: " + strings.Repeat("x", 1000))
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd, "--only=disks")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d; stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 1 || recs[0]["class"] != "disks" {
		t.Fatalf("records = %v, want one class=disks unavailable", recs)
	}
	if n := len([]rune(recs[0]["error"])); n > 301 {
		t.Errorf("error is %d runes, want at most 300 plus the ellipsis", n)
	}
}

// TestOrphansSweepFailedErrorSanitized: the provider error is masked
// before it is cut, so a credential straddling the cut (here a PEM
// block whose END line falls past it) never reaches stdout.
func TestOrphansSweepFailedErrorSanitized(t *testing.T) {
	const keyBody = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7"
	pem := "-----BEGIN PRIVATE KEY-----\n" + strings.Repeat(keyBody, 8) + "\n-----END PRIVATE KEY-----"
	api := orphanFixture()
	api.lbsErr = errors.New("resolving forwarding rule \"r\": " + strings.Repeat("y", 200) + pem)
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd, "--only=lbs")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d; stderr: %s", res.Code, res.Stderr)
	}
	if strings.Contains(res.Stdout, keyBody) || strings.Contains(res.Stdout, "BEGIN PRIVATE KEY") {
		t.Errorf("stdout leaks the key material:\n%s", res.Stdout)
	}
}

// TestOrphansMixedDeniedAndFailed: a refusal and a failure in the
// same invocation each get their own record and note fragment.
func TestOrphansMixedDeniedAndFailed(t *testing.T) {
	api := orphanFixture()
	api.lbsErr = errors.New(lbListErr)
	api.addrsErr = denied("compute.addresses.list")
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d; stderr: %s", res.Code, res.Stderr)
	}
	sum := summaryLine(t, res.Stdout)
	want := "addresses: needs compute.addresses.list; lbs: sweep failed"
	if sum["scanned"] != "4" || sum["unavailable"] != want {
		t.Errorf("summary = %v, want scanned=4 unavailable=%q", sum, want)
	}
}

// ctxOrphans blocks the lb sweep until the invocation's context
// ends, then fails with the context's error — a call cut off by
// --timeout.
type ctxOrphans struct{ *fakeOrphans }

func (c ctxOrphans) OrphanLoadBalancers(ctx context.Context) (cloud.LoadBalancerSweep, error) {
	<-ctx.Done()
	return cloud.LoadBalancerSweep{}, fmt.Errorf("resolving forwarding rule %q: %w", "web-rule", ctx.Err())
}

// TestOrphansTimeoutStaysFatal: an expired --timeout is not one
// class's failure — every later class would fail the same way — so it
// stays a runtime error (exit 1), as before.
func TestOrphansTimeoutStaysFatal(t *testing.T) {
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: ctxOrphans{orphanFixture()}}))
	res := checktest.Run(t, cmd, "--only=lbs", "--timeout=1ns")
	if res.Code != emit.ExitRuntime {
		t.Fatalf("exit %d, want %d; stdout: %s", res.Code, emit.ExitRuntime, res.Stdout)
	}
	if !strings.Contains(res.Stderr, "timed out") || strings.Contains(res.Stdout, "SweepFailed") {
		t.Errorf("stdout = %q, stderr = %q; want the timeout reported, not a SweepFailed record", res.Stdout, res.Stderr)
	}
}

// TestOrphansSweepFailedContract: the SweepFailed record's fields
// (error included) are in the output glossary.
func TestOrphansSweepFailedContract(t *testing.T) {
	api := orphanFixture()
	api.lbsErr = errors.New(lbListErr)
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	checktest.VerifyContract(t, cmd)
}

// TestOrphansDeniedContract: the degraded path's fields (class,
// permission, the unavailable= note) are in the output glossary.
func TestOrphansDeniedContract(t *testing.T) {
	api := orphanFixture()
	api.addrsErr = denied("compute.addresses.list")
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	checktest.VerifyContract(t, cmd)
}

// TestOrphansUnresolvedRules: rules the provider could not judge each
// get one cloud.unavailable (reason=RuleUnresolved) naming the rule
// and carrying its error — a refusal also names the permission — while
// the judged orphans and every other class are still reported, the
// summary counts them, and the exit code stays 0.
func TestOrphansUnresolvedRules(t *testing.T) {
	api := orphanFixture()
	api.unresolved = []cloud.UnresolvedLoadBalancer{
		{Name: "shared-ingress", Region: "global", Err: &cloud.PermissionDeniedError{
			Permission: "compute.backendServices.get", Err: errors.New("googleapi: Error 403: Required 'compute.backendServices.get' permission")}},
		{Name: "churned-ingress", Region: "global", Err: errors.New("googleapi: Error 404: The resource 'k8s1-web-bs' was not found, notFound")},
	}
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d; stderr: %s", res.Code, res.Stderr)
	}
	kinds := map[string]int{}
	unresolved := map[string]map[string]string{}
	for _, r := range findingLines(t, res.Stdout) {
		kinds[r["kind"]]++
		if r["reason"] == "RuleUnresolved" {
			unresolved[r["name"]] = r
		}
	}
	if kinds["orphan.disk"] != 3 || kinds["orphan.lb"] != 2 || kinds["orphan.address"] != 2 || kinds["cloud.unavailable"] != 2 {
		t.Errorf("kinds = %v, want 3 disks, 2 lbs, 2 addresses and 2 unresolved rules", kinds)
	}
	refused := unresolved["shared-ingress"]
	if refused["kind_of_object"] != "ForwardingRule" || refused["class"] != "lbs" || refused["region"] != "global" ||
		refused["permission"] != "compute.backendServices.get" || !strings.Contains(refused["error"], "Error 403") {
		t.Errorf("shared-ingress = %v, want a RuleUnresolved ForwardingRule record with the permission and the 403", refused)
	}
	churned := unresolved["churned-ingress"]
	if churned["permission"] != "" || !strings.Contains(churned["error"], "Error 404") || churned["severity"] != emit.SeverityInfo {
		t.Errorf("churned-ingress = %v, want an info RuleUnresolved record with the 404 and no permission", churned)
	}
	sum := summaryLine(t, res.Stdout)
	// scanned = 4 disks + 2 orphaned rules + 3 idle addresses.
	if sum["scanned"] != "9" || sum["unavailable"] != "lbs: 2 rules unresolved" {
		t.Errorf("summary = %v, want scanned=9 unavailable=\"lbs: 2 rules unresolved\"", sum)
	}
}

// TestOrphansUnresolvedRulesCapped: a systemic failure (every rule
// behind one unreadable project) is capped at 20 records; the summary
// still counts them all.
func TestOrphansUnresolvedRulesCapped(t *testing.T) {
	api := orphanFixture()
	for i := range 25 {
		api.unresolved = append(api.unresolved, cloud.UnresolvedLoadBalancer{
			Name: fmt.Sprintf("rule-%02d", i), Region: "global", Err: errors.New("googleapi: Error 500: backendError")})
	}
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd, "--only=lbs")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d; stderr: %s", res.Code, res.Stderr)
	}
	n := 0
	for _, r := range findingLines(t, res.Stdout) {
		if r["reason"] == "RuleUnresolved" {
			n++
		}
	}
	sum := summaryLine(t, res.Stdout)
	if n != 20 || sum["unavailable"] != "lbs: 25 rules unresolved (20 listed)" {
		t.Errorf("records = %d, summary = %v; want 20 records and \"lbs: 25 rules unresolved (20 listed)\"", n, sum)
	}
}

// TestOrphansUnresolvedContract: the RuleUnresolved fields are in the
// output glossary.
func TestOrphansUnresolvedContract(t *testing.T) {
	api := orphanFixture()
	api.unresolved = []cloud.UnresolvedLoadBalancer{{Name: "r", Region: "global", Err: &cloud.PermissionDeniedError{
		Permission: "compute.backendServices.get", Err: errors.New("googleapi: Error 403")}}}
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	checktest.VerifyContract(t, cmd)
}

// TestOrphansSlowLBsCannotStarveTheRest: lbs runs last and on a
// budget, so an lb sweep that would run past --timeout degrades to
// SweepFailed — the disks and addresses are reported, the summary is
// written, the exit code stays 0.
func TestOrphansSlowLBsCannotStarveTheRest(t *testing.T) {
	api := ctxOrphans{orphanFixture()}
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd, "--timeout=500ms")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0; stderr: %s", res.Code, res.Stderr)
	}
	kinds := map[string]int{}
	var failed map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		kinds[r["kind"]]++
		if r["reason"] == "SweepFailed" {
			failed = r
		}
	}
	if kinds["orphan.disk"] != 3 || kinds["orphan.address"] != 2 {
		t.Errorf("kinds = %v, want the disks and addresses reported", kinds)
	}
	if failed["class"] != "lbs" || !strings.Contains(failed["error"], "share of --timeout") {
		t.Errorf("SweepFailed = %v, want class=lbs naming its --timeout share", failed)
	}
	if sum := summaryLine(t, res.Stdout); sum["unavailable"] != "lbs: sweep failed" {
		t.Errorf("summary = %v, want unavailable=\"lbs: sweep failed\"", sum)
	}
}

// identityOrphanProvider wraps an orphanProvider with cloud.Identity so
// tests can exercise cluster-zone disk scoping vs --project-wide.
type identityOrphanProvider struct {
	orphanProvider
	project, location, region string
}

func (p identityOrphanProvider) Project() string  { return p.project }
func (p identityOrphanProvider) Location() string { return p.location }
func (p identityOrphanProvider) Region() string   { return p.region }

func TestOrphansDiskScopeClusterZonesAndProjectWide(t *testing.T) {
	fixture := func() *fakeOrphans {
		f := orphanFixture()
		// Add a disk in another region so regional and zonal filtering both drop it.
		f.disks = append(f.disks, cloud.OrphanDisk{
			Name:        "other-region-disk",
			Zone:        "europe-west1-b",
			SizeGB:      300,
			Type:        "pd-ssd",
			UnusedSince: fixedNow.Add(-96 * time.Hour),
		})
		return f
	}

	t.Run("default zonal cluster filters to cluster zone", func(t *testing.T) {
		p := identityOrphanProvider{
			orphanProvider: orphanProvider{Provider: cloud.NoProvider, api: fixture()},
			project:        "p",
			location:       "us-east1-b",
			region:         "us-east1",
		}
		cmd := cloudcheck.OrphansCommand(testDeps(p))
		res := checktest.Run(t, cmd, "--only=disks")
		if res.Code != emit.ExitData {
			t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
		}
		var names []string
		for _, r := range findingLines(t, res.Stdout) {
			names = append(names, r["name"])
		}
		// Only us-east1-b disks older than 24h (or undatable): undatable and small-old.
		// big-idle (us-east1-c) and other-region-disk (europe-west1-b) are filtered out.
		if got := strings.Join(names, ","); got != "undatable,small-old" {
			t.Errorf("disks = %q, want undatable,small-old", got)
		}
		sum := summaryLine(t, res.Stdout)
		if sum["scope"] != "cluster-zones" || sum["scanned"] != "3" {
			t.Errorf("summary = %v, want scope=cluster-zones scanned=3", sum)
		}
	})

	t.Run("default regional cluster filters to region prefix", func(t *testing.T) {
		p := identityOrphanProvider{
			orphanProvider: orphanProvider{Provider: cloud.NoProvider, api: fixture()},
			project:        "p",
			location:       "us-east1",
			region:         "us-east1",
		}
		cmd := cloudcheck.OrphansCommand(testDeps(p))
		res := checktest.Run(t, cmd, "--only=disks")
		if res.Code != emit.ExitData {
			t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
		}
		var names []string
		for _, r := range findingLines(t, res.Stdout) {
			names = append(names, r["name"])
		}
		// us-east1-b and us-east1-c disks are kept; europe-west1-b is filtered out.
		if got := strings.Join(names, ","); got != "big-idle,undatable,small-old" {
			t.Errorf("disks = %q, want big-idle,undatable,small-old", got)
		}
		sum := summaryLine(t, res.Stdout)
		if sum["scope"] != "cluster-zones" || sum["scanned"] != "4" {
			t.Errorf("summary = %v, want scope=cluster-zones scanned=4", sum)
		}
	})

	t.Run("project-wide keeps all disks across zones", func(t *testing.T) {
		p := identityOrphanProvider{
			orphanProvider: orphanProvider{Provider: cloud.NoProvider, api: fixture()},
			project:        "p",
			location:       "us-east1-b",
			region:         "us-east1",
		}
		cmd := cloudcheck.OrphansCommand(testDeps(p))
		res := checktest.Run(t, cmd, "--only=disks", "--project-wide")
		if res.Code != emit.ExitData {
			t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
		}
		var names []string
		for _, r := range findingLines(t, res.Stdout) {
			names = append(names, r["name"])
		}
		if got := strings.Join(names, ","); got != "big-idle,other-region-disk,undatable,small-old" {
			t.Errorf("disks = %q, want big-idle,other-region-disk,undatable,small-old", got)
		}
		sum := summaryLine(t, res.Stdout)
		if sum["scope"] != "project" || sum["scanned"] != "5" {
			t.Errorf("summary = %v, want scope=project scanned=5", sum)
		}
	})
}
