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

func stockoutAt(minsAgo int, zone, mt string) cloud.Stockout {
	return cloud.Stockout{
		Time:        fixedNow.Add(-time.Duration(minsAgo) * time.Minute),
		Zone:        zone,
		MachineType: mt,
		Message:     "ZONE_RESOURCE_POOL_EXHAUSTED",
	}
}

// stockoutFixture: n2-standard-16 exhausted in us-east1-b (3
// events) and us-east1-c (1); e2-medium exhausted only in
// us-east1-b; one europe event proves cross-region zones are never
// suggested for us-east1.
func stockoutFixture() *fakeStockouts {
	return &fakeStockouts{events: []cloud.Stockout{
		stockoutAt(30, "us-east1-b", "n2-standard-16"),
		stockoutAt(90, "us-east1-b", "n2-standard-16"),
		stockoutAt(600, "us-east1-b", "n2-standard-16"),
		stockoutAt(45, "us-east1-c", "n2-standard-16"),
		stockoutAt(200, "us-east1-b", "e2-medium"),
		stockoutAt(100, "europe-west1-d", "e2-medium"),
	}}
}

func TestStockoutGroupingAndReroute(t *testing.T) {
	api := stockoutFixture()
	cmd := cloudcheck.StockoutCommand(testDeps(stockoutProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 4 {
		t.Fatalf("findings = %d, want 4 zone/machine-type groups:\n%s", len(recs), res.Stdout)
	}
	type key struct{ zone, mt string }
	byKey := map[key]map[string]string{}
	for _, r := range recs {
		if r["kind"] != "stockout.zone" || r["severity"] != emit.SeverityWarning || r["reason"] != "ZoneResourcePoolExhausted" {
			t.Errorf("finding = %v, want warning stockout.zone/ZoneResourcePoolExhausted", r)
		}
		if r["kind_of_object"] != "Zone" {
			t.Errorf("kind_of_object = %q, want Zone", r["kind_of_object"])
		}
		byKey[key{r["name"], r["machine_type"]}] = r
	}

	b := byKey[key{"us-east1-b", "n2-standard-16"}]
	if b == nil || b["events"] != "3" {
		t.Fatalf("us-east1-b/n2 group = %v, want events=3", b)
	}
	if b["first_seen"] != "2026-07-25T02:00:00Z" || b["last_seen"] != "2026-07-25T11:30:00Z" {
		t.Errorf("first/last = %s/%s, want 02:00:00Z/11:30:00Z", b["first_seen"], b["last_seen"])
	}
	// n2 is exhausted in BOTH observed us-east1 zones — no clean
	// sibling, so no reroute field and the message says so.
	if b["reroute"] != "" {
		t.Errorf("us-east1-b/n2 reroute = %q, want none (both zones exhausted)", b["reroute"])
	}
	if !strings.Contains(b["message"], "no same-region zone observed clean") {
		t.Errorf("message %q must state the no-candidate case explicitly", b["message"])
	}

	// e2-medium in us-east1-b: us-east1-c is observed (n2 events)
	// and has no e2 stockout → the reroute candidate. europe-west1-d
	// is observed but cross-region → excluded.
	e := byKey[key{"us-east1-b", "e2-medium"}]
	if e == nil || e["reroute"] != "us-east1-c" {
		t.Fatalf("us-east1-b/e2 = %v, want reroute=us-east1-c", e)
	}
	if strings.Contains(e["reroute"], "europe") {
		t.Errorf("cross-region zone suggested: %q", e["reroute"])
	}

	// Ordering: most recent exhaustion first.
	if recs[0]["name"] != "us-east1-b" || recs[0]["machine_type"] != "n2-standard-16" {
		t.Errorf("first finding = %v, want the 30m-ago us-east1-b/n2 group", recs[0])
	}

	// Summary: scanned counts EVENTS, window note carries the
	// default 24h.
	sum := summaryLine(t, res.Stdout)
	if sum["scanned"] != "6" || sum["window"] != "24h0m0s" {
		t.Errorf("summary = %v, want scanned=6 window=24h0m0s", sum)
	}
}

func TestStockoutSinceWindow(t *testing.T) {
	api := stockoutFixture()
	cmd := cloudcheck.StockoutCommand(testDeps(stockoutProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd, "--since=6h")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !api.window.End.Equal(fixedNow) || !api.window.Start.Equal(fixedNow.Add(-6*time.Hour)) {
		t.Errorf("window = %+v, want [now-6h, now)", api.window)
	}
	if sum := summaryLine(t, res.Stdout); sum["window"] != "6h0m0s" {
		t.Errorf("summary window = %q, want 6h0m0s", sum["window"])
	}
}

func TestStockoutMachineTypeUnknown(t *testing.T) {
	api := &fakeStockouts{events: []cloud.Stockout{stockoutAt(10, "us-east1-b", "")}}
	cmd := cloudcheck.StockoutCommand(testDeps(stockoutProvider{Provider: cloud.NoProvider, api: api}))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 1 || recs[0]["machine_type"] != "" {
		t.Fatalf("recs = %v, want one finding with machine_type omitted (zero nominal state)", recs)
	}
	if !strings.Contains(recs[0]["message"], "unspecified machine type") {
		t.Errorf("message %q must name the unknown-type case", recs[0]["message"])
	}
}

func TestStockoutEmptyWindow(t *testing.T) {
	cmd := cloudcheck.StockoutCommand(testDeps(stockoutProvider{Provider: cloud.NoProvider, api: &fakeStockouts{}}))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	sum := summaryLine(t, res.Stdout)
	if sum["scanned"] != "0" || sum["findings"] != "0" || sum["window"] != "24h0m0s" {
		t.Errorf("summary = %v, want an explicit empty scan with the window note", sum)
	}
}

func TestStockoutUnavailable(t *testing.T) {
	cmd := cloudcheck.StockoutCommand(testDeps(cloud.NoProvider))
	assertUnavailable(t, checktest.Run(t, cmd), "stockout")
}

func TestStockoutContract(t *testing.T) {
	cmd := cloudcheck.StockoutCommand(testDeps(stockoutProvider{Provider: cloud.NoProvider, api: stockoutFixture()}))
	checktest.VerifyContract(t, cmd)
}

func TestStockoutGolden(t *testing.T) {
	cmd := cloudcheck.StockoutCommand(testDeps(stockoutProvider{Provider: cloud.NoProvider, api: stockoutFixture()}))
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checkGolden(t, "stockout.golden", res.Stdout)
}
