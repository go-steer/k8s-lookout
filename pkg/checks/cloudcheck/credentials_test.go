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

	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// errNoToken is how the provider reports lookout's OWN identity being
// refused a token (#614); the raw cause carries the token endpoint's
// URL and server prose, none of which may reach stdout.
var errNoToken = fmt.Errorf("listing: %w", &cloud.CredentialsUnavailableError{
	Fix: "grant lookout's KSA roles/iam.workloadIdentityUser on its GSA",
	Err: errors.New(`regions.list: Get "https://compute.googleapis.com/compute/v1/projects/p/regions": credentials: cannot fetch token: compute: Received 403 Unable to generate access token`),
})

type noTokenQuota struct{}

func (noTokenQuota) Quotas(context.Context) ([]cloud.QuotaUsage, error) { return nil, errNoToken }
func (noTokenQuota) History(context.Context, string, string, cloud.TimeWindow) (cloud.QuotaHistory, error) {
	return cloud.QuotaHistory{}, errNoToken
}

type noTokenIPSpace struct{}

func (noTokenIPSpace) SubnetUtilization(context.Context) ([]cloud.SubnetUtilization, error) {
	return nil, errNoToken
}

type noTokenStockouts struct{}

func (noTokenStockouts) Stockouts(context.Context, cloud.TimeWindow) ([]cloud.Stockout, error) {
	return nil, errNoToken
}

// noTokenOrphans refuses every class and counts the sweeps asked for.
type noTokenOrphans struct{ calls int }

func (f *noTokenOrphans) OrphanDisks(context.Context) ([]cloud.OrphanDisk, error) {
	f.calls++
	return nil, errNoToken
}

func (f *noTokenOrphans) OrphanLoadBalancers(context.Context) (cloud.LoadBalancerSweep, error) {
	f.calls++
	return cloud.LoadBalancerSweep{}, errNoToken
}

func (f *noTokenOrphans) OrphanAddresses(context.Context) ([]cloud.OrphanAddress, error) {
	f.calls++
	return nil, errNoToken
}

// assertCredentialsUnavailable: exit 0, exactly one
// CredentialsUnavailable record naming what could not run, the
// unavailable= note, and no raw error text on stdout.
func assertCredentialsUnavailable(t *testing.T, res checktest.Result, capability, what string) {
	t.Helper()
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0 (stderr: %s)", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 1 {
		t.Fatalf("want exactly the CredentialsUnavailable record, got %d: %v", len(recs), recs)
	}
	r := recs[0]
	wantMsg := cloud.CredentialsUnavailableMessage(what, "grant lookout's KSA roles/iam.workloadIdentityUser on its GSA")
	if r["kind"] != "cloud.unavailable" || r["severity"] != emit.SeverityInfo || r["reason"] != cloud.ReasonCredentialsUnavailable ||
		r["capability"] != capability || r["provider"] != cloud.NoProviderName || r["message"] != wantMsg {
		t.Errorf("record = %v\nwant reason=%s capability=%s message=%q", r, cloud.ReasonCredentialsUnavailable, capability, wantMsg)
	}
	if sum := summaryLine(t, res.Stdout); sum["unavailable"] != cloud.CredentialsUnavailableNote {
		t.Errorf("summary = %v, want unavailable=%q", sum, cloud.CredentialsUnavailableNote)
	}
	for _, raw := range []string{"https://", "cannot fetch token", "Unable to generate"} {
		if strings.Contains(res.Stdout, raw) {
			t.Errorf("stdout carries raw error text %q: %s", raw, res.Stdout)
		}
	}
}

func TestCredentialsUnavailable_CloudCommands(t *testing.T) {
	for _, tc := range []struct {
		cmdName    string
		provider   cloud.Provider
		capability cloud.Capability
	}{
		{"cloud quota", quotaProvider{Provider: cloud.NoProvider, api: noTokenQuota{}}, cloud.CapabilityQuota},
		{"cloud ipspace", ipspaceProvider{Provider: cloud.NoProvider, api: noTokenIPSpace{}}, cloud.CapabilityIPSpace},
		{"cloud stockout", stockoutProvider{Provider: cloud.NoProvider, api: noTokenStockouts{}}, cloud.CapabilityStockout},
	} {
		t.Run(tc.cmdName, func(t *testing.T) {
			res := checktest.Run(t, commandByName(t, testDeps(tc.provider), tc.cmdName))
			assertCredentialsUnavailable(t, res, string(tc.capability), tc.cmdName)
		})
	}
}

// TestCredentialsUnavailable_OrphansStopsSweep: a refused token is
// not one class's problem — the first refusal ends the sweep with one
// record, instead of a per-class PermissionDenied-style line for each.
func TestCredentialsUnavailable_OrphansStopsSweep(t *testing.T) {
	api := &noTokenOrphans{}
	res := checktest.Run(t, commandByName(t, testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}), "cloud orphans"), "--only=disks,lbs,addresses")
	assertCredentialsUnavailable(t, res, string(cloud.CapabilityOrphans), "cloud orphans")
	if api.calls != 1 {
		t.Errorf("sweeps asked for = %d, want 1 (stop at the first refused token)", api.calls)
	}
}

// noSweepFailed: a refused token wrapped through the per-class
// providerFailure (#612) must still read as CredentialsUnavailable —
// never also as SweepFailed or RuleUnresolved.
func noSweepFailed(t *testing.T, stdout string) {
	t.Helper()
	for _, r := range findingLines(t, stdout) {
		if r["reason"] == "SweepFailed" || r["reason"] == "RuleUnresolved" {
			t.Errorf("refused token also reported as %s: %v", r["reason"], r)
		}
	}
}

// TestCredentialsUnavailable_LbsClass: the lbs class's refusal (from
// the rule list or from any rule's chain — the provider returns either
// as the class error) is one CredentialsUnavailable record: the rules
// the provider had judged unresolved before it are not reported.
func TestCredentialsUnavailable_LbsClass(t *testing.T) {
	api := orphanFixture()
	api.disks, api.addrs = nil, nil
	api.unresolved = []cloud.UnresolvedLoadBalancer{{Name: "web-rule", Region: "us-east1", Err: errors.New("boom")}}
	api.lbsErr = errNoToken
	res := checktest.Run(t, commandByName(t, testDeps(orphanProvider{Provider: cloud.NoProvider, api: api}), "cloud orphans"), "--only=disks,addresses,lbs")
	assertCredentialsUnavailable(t, res, string(cloud.CapabilityOrphans), "cloud orphans")
	noSweepFailed(t, res.Stdout)
}

// TestCredentialsUnavailable_NodePoolsStopsSweep: a refusal on the
// nodepools class (cluster-config) ends the sweep there — lbs, which
// runs after it, is never asked — with one record naming the
// capability that was refused.
func TestCredentialsUnavailable_NodePoolsStopsSweep(t *testing.T) {
	orphans := orphanFixture()
	orphans.disks, orphans.addrs = nil, nil
	cfg := poolsFixture()
	cfg.err = errNoToken
	p := fullOrphanProvider{Provider: cloud.NoProvider, orphans: orphans, config: cfg}
	res := checktest.Run(t, commandByName(t, nodePoolDeps(p, fake.NewClientset(clusterFixture()...)), "cloud orphans"), "--only=disks,addresses,nodepools,lbs")
	assertCredentialsUnavailable(t, res, string(cloud.CapabilityClusterConfig), "cloud orphans")
	noSweepFailed(t, res.Stdout)
	if orphans.lbsCalled {
		t.Error("lbs swept after the refused token; the sweep must stop at the first refusal")
	}
}

// blockingQuota and blockingOrphans hold every call until the
// invocation's context ends, then answer with a refused token — the
// shape of a --timeout expiring mid token fetch.
type blockingQuota struct{}

func (blockingQuota) Quotas(ctx context.Context) ([]cloud.QuotaUsage, error) {
	<-ctx.Done()
	return nil, errNoToken
}

func (blockingQuota) History(context.Context, string, string, cloud.TimeWindow) (cloud.QuotaHistory, error) {
	return cloud.QuotaHistory{}, nil
}

type blockingOrphans struct{}

func (blockingOrphans) OrphanDisks(ctx context.Context) ([]cloud.OrphanDisk, error) {
	<-ctx.Done()
	return nil, errNoToken
}

func (blockingOrphans) OrphanLoadBalancers(context.Context) (cloud.LoadBalancerSweep, error) {
	return cloud.LoadBalancerSweep{}, nil
}

func (blockingOrphans) OrphanAddresses(context.Context) ([]cloud.OrphanAddress, error) {
	return nil, nil
}

// TestCredentialsUnavailable_TimeoutStaysRuntime: once --timeout has
// expired nothing degrades — the §4.2 contract's exit 1, never an
// exit-0 record naming a grant to fix.
func TestCredentialsUnavailable_TimeoutStaysRuntime(t *testing.T) {
	for _, tc := range []struct {
		cmdName string
		p       cloud.Provider
		args    []string
	}{
		{"cloud quota", quotaProvider{Provider: cloud.NoProvider, api: blockingQuota{}}, nil},
		{"cloud orphans", orphanProvider{Provider: cloud.NoProvider, api: blockingOrphans{}}, []string{"--only=disks"}},
	} {
		t.Run(tc.cmdName, func(t *testing.T) {
			res := checktest.Run(t, commandByName(t, testDeps(tc.p), tc.cmdName), append(tc.args, "--timeout=50ms")...)
			if res.Code != emit.ExitRuntime {
				t.Fatalf("exit %d, want %d (stdout: %s)", res.Code, emit.ExitRuntime, res.Stdout)
			}
			if strings.Contains(res.Stdout, cloud.ReasonCredentialsUnavailable) {
				t.Errorf("timed-out run degraded as CredentialsUnavailable: %s", res.Stdout)
			}
		})
	}
}
