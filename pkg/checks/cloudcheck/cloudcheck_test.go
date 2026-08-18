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

// §13 conventions: capability-backed fake provider (cloud.NoProvider
// plus exactly the one capability under test — the same embedding
// trick as `triage top`'s history tests), canned capability results,
// goldens per command, VerifyContract in both formats, and the §2
// unavailable path exercised per command.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/cloudcheck"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

var fixedNow = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

func testDeps(p cloud.Provider) cloudcheck.Deps {
	return cloudcheck.Deps{
		Provider: func(context.Context) (cloud.Provider, error) { return p, nil },
		Now:      func() time.Time { return fixedNow },
	}
}

// Capability-backed fake providers: NoProvider plus one working
// capability each.

type stockoutProvider struct {
	cloud.Provider
	api cloud.StockoutAPI
}

func (p stockoutProvider) Stockouts() (cloud.StockoutAPI, bool) { return p.api, true }

type orphanProvider struct {
	cloud.Provider
	api cloud.OrphanAPI
}

func (p orphanProvider) Orphans() (cloud.OrphanAPI, bool) { return p.api, true }

type ipspaceProvider struct {
	cloud.Provider
	api cloud.IPSpaceAPI
}

func (p ipspaceProvider) IPSpace() (cloud.IPSpaceAPI, bool) { return p.api, true }

type quotaProvider struct {
	cloud.Provider
	api cloud.QuotaAPI
}

func (p quotaProvider) Quota() (cloud.QuotaAPI, bool) { return p.api, true }

// Fake capability implementations serving canned results.

type fakeStockouts struct {
	events []cloud.Stockout
	window cloud.TimeWindow // records the window asked for
}

func (f *fakeStockouts) Stockouts(_ context.Context, w cloud.TimeWindow) ([]cloud.Stockout, error) {
	f.window = w
	return f.events, nil
}

type fakeOrphans struct {
	disks []cloud.OrphanDisk
	lbs   []cloud.OrphanLoadBalancer

	disksCalled, lbsCalled bool
}

func (f *fakeOrphans) OrphanDisks(context.Context) ([]cloud.OrphanDisk, error) {
	f.disksCalled = true
	return f.disks, nil
}

func (f *fakeOrphans) OrphanLoadBalancers(context.Context) ([]cloud.OrphanLoadBalancer, error) {
	f.lbsCalled = true
	return f.lbs, nil
}

type fakeIPSpace struct {
	ranges []cloud.SubnetUtilization
}

func (f *fakeIPSpace) SubnetUtilization(context.Context) ([]cloud.SubnetUtilization, error) {
	return f.ranges, nil
}

type fakeQuota struct {
	quotas []cloud.QuotaUsage
}

func (f *fakeQuota) Quotas(context.Context) ([]cloud.QuotaUsage, error) { return f.quotas, nil }
func (f *fakeQuota) History(context.Context, string, string, cloud.TimeWindow) (cloud.QuotaHistory, error) {
	return cloud.QuotaHistory{}, nil
}

// Shared logfmt parsing helpers (same shapes as the other command
// test suites).

func parseLine(t *testing.T, line string) map[string]string {
	t.Helper()
	out := map[string]string{}
	rest := line
	for rest != "" {
		eq := strings.IndexByte(rest, '=')
		if eq <= 0 {
			t.Fatalf("bad logfmt line %q", line)
		}
		key := rest[:eq]
		rest = rest[eq+1:]
		var val string
		if strings.HasPrefix(rest, `"`) {
			end := len(rest)
			for i := 1; i < len(rest); i++ {
				if rest[i] == '"' && rest[i-1] != '\\' {
					end = i + 1
					break
				}
			}
			val = strings.ReplaceAll(rest[1:end-1], `\"`, `"`)
			rest = rest[end:]
		} else if sp := strings.IndexByte(rest, ' '); sp >= 0 {
			val, rest = rest[:sp], rest[sp:]
		} else {
			val, rest = rest, ""
		}
		out[key] = val
		rest = strings.TrimPrefix(rest, " ")
	}
	return out
}

func findingLines(t *testing.T, stdout string) []map[string]string {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	var out []map[string]string
	for _, l := range lines[:len(lines)-1] {
		out = append(out, parseLine(t, l))
	}
	return out
}

func summaryLine(t *testing.T, stdout string) map[string]string {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	return parseLine(t, lines[len(lines)-1])
}

// assertUnavailable is the per-command §2 degradation check: exit 0,
// scanned=0, exactly one explicit cloud.unavailable finding, and the
// summary marker.
func assertUnavailable(t *testing.T, res checktest.Result, capability string) {
	t.Helper()
	if res.Code != emit.ExitData {
		t.Fatalf("unavailable path must exit 0 (explicit, not broken), got %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 1 {
		t.Fatalf("want exactly the cloud.unavailable finding, got %d records: %v", len(recs), recs)
	}
	r := recs[0]
	if r["kind"] != "cloud.unavailable" || r["reason"] != "CapabilityUnavailable" ||
		r["capability"] != capability || r["provider"] != cloud.NoProviderName ||
		!strings.Contains(r["message"], cloud.NoProviderReason) {
		t.Errorf("unavailable finding = %v, want capability=%s provider=%s with the reason in the message", r, capability, cloud.NoProviderName)
	}
	sum := summaryLine(t, res.Stdout)
	if sum["scanned"] != "0" || sum["unavailable"] != cloud.NoProviderReason {
		t.Errorf("summary = %v, want scanned=0 unavailable=%q", sum, cloud.NoProviderReason)
	}
}

// TestClusterScopeRejected: the k8s scoping flags are a usage error
// on every cloud command, not a silent no-op.
func TestClusterScopeRejected(t *testing.T) {
	deps := testDeps(cloud.NoProvider)
	for _, tc := range []struct {
		cmdName string
		args    []string
	}{
		{"cloud stockout", []string{"--namespace=prod"}},
		{"cloud orphans", []string{"-A"}},
		{"cloud ipspace", []string{"--workload=Deployment/prod/api"}},
		{"cloud quota", []string{"--namespace=prod"}},
	} {
		cmd := commandByName(t, deps, tc.cmdName)
		res := checktest.Run(t, cmd, tc.args...)
		if res.Code != emit.ExitUsage || !strings.Contains(res.Stderr, "do not apply") {
			t.Errorf("%s %v: exit %d stderr %q, want usage error", tc.cmdName, tc.args, res.Code, res.Stderr)
		}
	}
}

func commandByName(t *testing.T, deps cloudcheck.Deps, name string) checks.Command {
	t.Helper()
	switch name {
	case "cloud stockout":
		return cloudcheck.StockoutCommand(deps)
	case "cloud orphans":
		return cloudcheck.OrphansCommand(deps)
	case "cloud ipspace":
		return cloudcheck.IPSpaceCommand(deps)
	case "cloud quota":
		return cloudcheck.QuotaCommand(deps)
	}
	t.Fatalf("unknown command %q", name)
	return checks.Command{}
}
