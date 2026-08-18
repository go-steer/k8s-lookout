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

// quotaFixture: an exhausted quota (100%), a critical one (98%), a
// warning one (87.5%), two healthy ones, and a zero-limit
// (unentitled) quota that is scanned but never listed.
func quotaFixture() *fakeQuota {
	return &fakeQuota{quotas: []cloud.QuotaUsage{
		{Name: "CPUS", Scope: "us-east1", Usage: 588, Limit: 600},            // 98% critical
		{Name: "IN_USE_ADDRESSES", Scope: "us-east1", Usage: 8, Limit: 8},    // 100% exhausted
		{Name: "SSD_TOTAL_GB", Scope: "us-east1", Usage: 3500, Limit: 4000},  // 87.5% warning
		{Name: "NETWORKS", Scope: "global", Usage: 3, Limit: 15},             // 20% silent
		{Name: "CPUS_ALL_REGIONS", Scope: "global", Usage: 620, Limit: 1200}, // 51.7% silent
		{Name: "NVIDIA_H100_GPUS", Scope: "us-east1", Usage: 0, Limit: 0},    // unratable
	}}
}

func TestQuotaRankingAndSeverities(t *testing.T) {
	cmd := cloudcheck.QuotaCommand(testDeps(quotaProvider{Provider: cloud.NoProvider, api: quotaFixture()}))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 3 {
		t.Fatalf("findings = %d, want 3 at/above the 80%% default:\n%s", len(recs), res.Stdout)
	}

	// Nearest-to-exhaustion first.
	if recs[0]["name"] != "IN_USE_ADDRESSES" || recs[1]["name"] != "CPUS" || recs[2]["name"] != "SSD_TOTAL_GB" {
		t.Fatalf("order = %s,%s,%s, want IN_USE_ADDRESSES,CPUS,SSD_TOTAL_GB",
			recs[0]["name"], recs[1]["name"], recs[2]["name"])
	}

	full := recs[0]
	if full["severity"] != emit.SeverityCritical || full["reason"] != "QuotaExhausted" || full["pct"] != "100" ||
		!strings.Contains(full["message"], "GCE_QUOTA_EXCEEDED") {
		t.Errorf("exhausted quota = %v, want critical QuotaExhausted naming the failure mode", full)
	}
	crit := recs[1]
	if crit["severity"] != emit.SeverityCritical || crit["reason"] != "QuotaNearLimit" || crit["pct"] != "98" ||
		crit["usage"] != "588" || crit["limit"] != "600" || crit["scope"] != "us-east1" {
		t.Errorf("98%% quota = %v, want critical QuotaNearLimit 588/600 us-east1", crit)
	}
	warn := recs[2]
	if warn["severity"] != emit.SeverityWarning || warn["reason"] != "QuotaNearLimit" || warn["pct"] != "87.5" {
		t.Errorf("87.5%% quota = %v, want warning QuotaNearLimit", warn)
	}

	// scanned counts EVERY quota examined, including the healthy and
	// the unratable ones.
	if sum := summaryLine(t, res.Stdout); sum["scanned"] != "6" {
		t.Errorf("scanned = %s, want 6", sum["scanned"])
	}
}

func TestQuotaWarnFlagAndAll(t *testing.T) {
	cmd := cloudcheck.QuotaCommand(testDeps(quotaProvider{Provider: cloud.NoProvider, api: quotaFixture()}))

	res := checktest.Run(t, cmd, "--quota-warn=50")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if recs := findingLines(t, res.Stdout); len(recs) != 4 || recs[3]["name"] != "CPUS_ALL_REGIONS" ||
		recs[3]["severity"] != emit.SeverityWarning {
		t.Errorf("--quota-warn=50 rows = %v, want CPUS_ALL_REGIONS (51.7%%) pulled in as warning", recs)
	}

	res = checktest.Run(t, cmd, "--all")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	// 5 ratable quotas; the zero-limit one never appears even under
	// --all.
	if len(recs) != 5 {
		t.Fatalf("--all rows = %d, want 5 ratable quotas", len(recs))
	}
	last := recs[4]
	if last["name"] != "NETWORKS" || last["severity"] != emit.SeverityInfo || last["reason"] != "" || last["message"] != "" {
		t.Errorf("below-threshold --all row = %v, want bare info numbers (zero nominal state)", last)
	}
	for _, r := range recs {
		if r["name"] == "NVIDIA_H100_GPUS" {
			t.Errorf("zero-limit quota listed: %v", r)
		}
	}

	res = checktest.Run(t, cmd, "--quota-warn=0")
	if res.Code != emit.ExitUsage {
		t.Errorf("--quota-warn=0: exit %d, want usage error", res.Code)
	}
}

func TestQuotaUnavailable(t *testing.T) {
	cmd := cloudcheck.QuotaCommand(testDeps(cloud.NoProvider))
	assertUnavailable(t, checktest.Run(t, cmd), "quota")
}

func TestQuotaContract(t *testing.T) {
	cmd := cloudcheck.QuotaCommand(testDeps(quotaProvider{Provider: cloud.NoProvider, api: quotaFixture()}))
	checktest.VerifyContract(t, cmd, "--all")
}

func TestQuotaGolden(t *testing.T) {
	cmd := cloudcheck.QuotaCommand(testDeps(quotaProvider{Provider: cloud.NoProvider, api: quotaFixture()}))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/quota.golden", res.Stdout)
}
