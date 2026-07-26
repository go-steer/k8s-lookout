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

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/cloudcheck"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// ipspaceFixture: a critical pod range (96.9%), a warning node range
// (85.5%), a healthy node range (7%), and a services range whose
// usage the cloud cannot see.
func ipspaceFixture() *fakeIPSpace {
	return &fakeIPSpace{ranges: []cloud.SubnetUtilization{
		{Subnet: "prod-subnet", CIDR: "10.8.0.0/14", Purpose: "pods", Used: 253952, Capacity: 262144}, // 96.9%
		{Subnet: "prod-subnet", CIDR: "10.0.0.0/24", Purpose: "nodes", Used: 216, Capacity: 252},      // 85.7%
		{Subnet: "prod-subnet", CIDR: "10.12.0.0/20", Purpose: "services", Used: -1, Capacity: 4096},  // unknown
		{Subnet: "batch-subnet", CIDR: "10.1.0.0/24", Purpose: "nodes", Used: 18, Capacity: 252},      // 7.1%
	}}
}

func TestIPSpaceThresholds(t *testing.T) {
	cmd := cloudcheck.IPSpaceCommand(testDeps(ipspaceProvider{Provider: cloud.NoProvider, api: ipspaceFixture()}))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	// pods critical + nodes warning + the explicit unknown services
	// row; the healthy batch-subnet row is silent (zero nominal
	// state).
	if len(recs) != 3 {
		t.Fatalf("findings = %d, want 3:\n%s", len(recs), res.Stdout)
	}

	pods := recs[0]
	if pods["purpose"] != "pods" || pods["severity"] != emit.SeverityCritical || pods["reason"] != "IPRangeNearExhaustion" ||
		pods["pct"] != "96.9" || pods["name"] != "prod-subnet" || pods["kind_of_object"] != "Subnetwork" {
		t.Errorf("pods row = %v, want critical IPRangeNearExhaustion 96.9%%", pods)
	}
	if !strings.Contains(pods["message"], "incompressible") {
		t.Errorf("pods message %q must carry the why", pods["message"])
	}

	nodes := recs[1]
	if nodes["purpose"] != "nodes" || nodes["severity"] != emit.SeverityWarning || nodes["reason"] != "IPRangeHighUtilization" ||
		nodes["pct"] != "85.7" || nodes["used"] != "216" || nodes["capacity"] != "252" {
		t.Errorf("nodes row = %v, want warning 85.7%% 216/252", nodes)
	}

	// The unknown-usage services range is ALWAYS explicit — §2 bans
	// rendering "cannot know" as silence or 0%.
	svc := recs[2]
	if svc["purpose"] != "services" || svc["severity"] != emit.SeverityInfo || svc["reason"] != "UsageNotCloudVisible" {
		t.Errorf("services row = %v, want explicit info UsageNotCloudVisible", svc)
	}
	if svc["pct"] != "" || svc["used"] != "" {
		t.Errorf("services row carries fake numbers: %v", svc)
	}
	if svc["capacity"] != "4096" {
		t.Errorf("services capacity = %q, want 4096 (what IS known is reported)", svc["capacity"])
	}

	if sum := summaryLine(t, res.Stdout); sum["scanned"] != "4" {
		t.Errorf("scanned = %s, want 4 ranges", sum["scanned"])
	}
}

func TestIPSpaceAllDump(t *testing.T) {
	cmd := cloudcheck.IPSpaceCommand(testDeps(ipspaceProvider{Provider: cloud.NoProvider, api: ipspaceFixture()}))
	res := checktest.Run(t, cmd, "--all")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 4 {
		t.Fatalf("--all findings = %d, want all 4 ranges", len(recs))
	}
	// The healthy row: numbers only, info severity, no judgment
	// fields.
	healthy := recs[2]
	if healthy["name"] != "batch-subnet" || healthy["severity"] != emit.SeverityInfo ||
		healthy["reason"] != "" || healthy["message"] != "" || healthy["pct"] != "7.1" {
		t.Errorf("healthy --all row = %v, want bare info numbers pct=7.1", healthy)
	}
	// Known rows stay pct-descending; the unknown row stays last.
	if recs[0]["pct"] != "96.9" || recs[1]["pct"] != "85.7" || recs[3]["reason"] != "UsageNotCloudVisible" {
		t.Errorf("--all order wrong:\n%s", res.Stdout)
	}
}

func TestIPSpaceUnavailable(t *testing.T) {
	cmd := cloudcheck.IPSpaceCommand(testDeps(cloud.NoProvider))
	assertUnavailable(t, checktest.Run(t, cmd), "ipspace")
}

func TestIPSpaceContract(t *testing.T) {
	cmd := cloudcheck.IPSpaceCommand(testDeps(ipspaceProvider{Provider: cloud.NoProvider, api: ipspaceFixture()}))
	checktest.VerifyContract(t, cmd, "--all")
}

func TestIPSpaceGolden(t *testing.T) {
	cmd := cloudcheck.IPSpaceCommand(testDeps(ipspaceProvider{Provider: cloud.NoProvider, api: ipspaceFixture()}))
	res := checktest.Run(t, cmd, "--all")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checkGolden(t, "ipspace.golden", res.Stdout)
}
