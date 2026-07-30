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

package stab_test

// `stab drift --identity` (§5 identity query pack, #128): the audit
// enrichment against a fake AuditAPI, and the §2 explicit-unavailable
// degradation on providers without the capability.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/stab"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// auditProvider is cloud.NoProvider with the audit capability grafted
// on (§13: embed the sentinel, override one getter).
type auditProvider struct {
	cloud.Provider
	api cloud.AuditAPI
}

func (p auditProvider) Audit() (cloud.AuditAPI, bool) { return p.api, p.api != nil }

// fakeAuditAPI serves canned writes and records the refs/windows it
// was asked for.
type fakeAuditAPI struct {
	writes map[string][]cloud.ObjectWrite // key: ns/name
	err    error
	refs   []cloud.AuditRef
	wins   []cloud.TimeWindow
}

func (f *fakeAuditAPI) ObjectWrites(_ context.Context, ref cloud.AuditRef, w cloud.TimeWindow) ([]cloud.ObjectWrite, error) {
	f.refs = append(f.refs, ref)
	f.wins = append(f.wins, w)
	if f.err != nil {
		return nil, f.err
	}
	return f.writes[ref.Namespace+"/"+ref.Name], nil
}

// findingWith returns the first finding line whose key equals value.
func findingWith(t *testing.T, stdout, key, value string) map[string]string {
	t.Helper()
	for _, f := range findingLines(t, stdout) {
		if f[key] == value {
			return f
		}
	}
	t.Fatalf("no finding with %s=%s in:\n%s", key, value, stdout)
	return nil
}

func driftDepsWithProvider(p cloud.Provider) stab.Deps {
	deps := testDeps(driftMixed()...)
	deps.Provider = func(context.Context) (cloud.Provider, error) { return p, nil }
	return deps
}

// kubectlWriteTime is the fixture anchor: driftMixed's kubectl-edit
// entry is stamped ago(3h20m) from the pinned clock.
var kubectlWriteTime = fixedNow.Add(-(3*time.Hour + 20*time.Minute))

func TestDriftIdentityEnrichment(t *testing.T) {
	api := &fakeAuditAPI{writes: map[string][]cloud.ObjectWrite{
		"prod/api": {
			// Newest first, like the backend returns. The nearest to
			// the anchor is alice; argo's SSA write is also inside
			// the window and lands in other_principals.
			{Time: kubectlWriteTime.Add(2 * time.Minute), Principal: "argo-sa@proj.iam.gserviceaccount.com", Method: "io.k8s.apps.v1.deployments.patch"},
			{Time: kubectlWriteTime.Add(30 * time.Second), Principal: "alice@example.com", Method: "io.k8s.apps.v1.deployments.patch", UserAgent: "kubectl/v1.31.0 (linux/amd64)"},
		},
	}}
	res := checktest.Run(t, stab.DriftCommand(driftDepsWithProvider(auditProvider{Provider: cloud.NoProvider, api: api})), "--identity")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}

	kubectl := findingWith(t, res.Stdout, "manager", "kubectl-edit")
	if kubectl["principal"] != "alice@example.com" {
		t.Errorf("principal = %q, want the write nearest the managedFields time", kubectl["principal"])
	}
	if !strings.HasPrefix(kubectl["principal_agent"], "kubectl/v1.31.0") {
		t.Errorf("principal_agent = %q", kubectl["principal_agent"])
	}
	if kubectl["other_principals"] != "argo-sa@proj.iam.gserviceaccount.com" {
		t.Errorf("other_principals = %q", kubectl["other_principals"])
	}

	// helm-legacy's entry has no managedFields time: explicit
	// sentinel, never silence.
	helm := findingWith(t, res.Stdout, "manager", "helm-legacy")
	if helm["principal"] != "no-write-time-anchor" {
		t.Errorf("anchorless principal = %q", helm["principal"])
	}

	// The one anchored query: apps/v1 deployments prod/api, window
	// centered on the managedFields time.
	if len(api.refs) != 1 {
		t.Fatalf("audit queries = %d, want 1 (anchorless findings must not query)", len(api.refs))
	}
	ref := api.refs[0]
	if ref.APIGroup != "apps" || ref.Version != "v1" || ref.Resource != "deployments" || ref.Namespace != "prod" || ref.Name != "api" {
		t.Errorf("ref = %+v", ref)
	}
	w := api.wins[0]
	if !w.Start.Equal(kubectlWriteTime.Add(-15*time.Minute)) || !w.End.Equal(kubectlWriteTime.Add(15*time.Minute)) {
		t.Errorf("window = %v..%v, want ±15m around %v", w.Start, w.End, kubectlWriteTime)
	}
}

func TestDriftIdentityNoWritesInWindow(t *testing.T) {
	api := &fakeAuditAPI{writes: map[string][]cloud.ObjectWrite{}}
	res := checktest.Run(t, stab.DriftCommand(driftDepsWithProvider(auditProvider{Provider: cloud.NoProvider, api: api})), "--identity")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	kubectl := findingWith(t, res.Stdout, "manager", "kubectl-edit")
	if kubectl["principal"] != "none-in-audit-window" {
		t.Errorf("principal = %q, want the empty-window sentinel", kubectl["principal"])
	}
}

func TestDriftIdentityUnavailable(t *testing.T) {
	res := checktest.Run(t, stab.DriftCommand(driftDepsWithProvider(cloud.NoProvider)), "--identity")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	// Findings still emit — the portable read owes nothing to the
	// cloud — and none carries a principal.
	kubectl := findingWith(t, res.Stdout, "manager", "kubectl-edit")
	if _, ok := kubectl["principal"]; ok {
		t.Error("principal present without an audit capability")
	}
	// The summary carries the §2 marker.
	summary := summaryLine(t, res.Stdout)
	if !strings.Contains(summary["identity"], "no cloud provider configured") {
		t.Errorf("summary identity note = %q, want the unavailable marker", summary["identity"])
	}
}

func TestDriftIdentityBackendError(t *testing.T) {
	api := &fakeAuditAPI{err: errors.New("logging backend exploded")}
	res := checktest.Run(t, stab.DriftCommand(driftDepsWithProvider(auditProvider{Provider: cloud.NoProvider, api: api})), "--identity")
	if res.Code == emit.ExitData {
		t.Fatal("backend error swallowed; want scan failure")
	}
	if !strings.Contains(res.Stderr, "logging backend exploded") {
		t.Errorf("stderr = %q", res.Stderr)
	}
}

func TestDriftIdentityOffByDefault(t *testing.T) {
	// Without --identity the provider seam must never be touched:
	// a Deps.Provider that panics proves it.
	deps := testDeps(driftMixed()...)
	deps.Provider = func(context.Context) (cloud.Provider, error) {
		panic("provider resolved without --identity")
	}
	res := checktest.Run(t, stab.DriftCommand(deps))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
}
