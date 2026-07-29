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

package watch

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/bundle"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
)

// frozenBundleSections is the bundle section contract: the section
// identifiers, comma-joined in the fixed emission (and truncation)
// order, exactly as the bundle.target head finding's `sections`
// detail declares them on BOTH surfaces. SCHEMA-STABLE: agents and
// playbook skills parse one bundle shape whether it arrived via
// `lookout bundle` (CLI/MCP) or inside a §7.6 enrichment inject —
// treat a failing pin as a breaking schema change, never as a test
// to update.
const frozenBundleSections = "spec,delta,edges,radius,logs"

// TestBundleSectionContractFrozen is the issue #86 guardian: the
// section identifiers live in TWO unexported constant sets —
// pkg/checks/bundle/bundle.go ("sections, in emission order") and
// internal/watch/enrich.go (enrichSection*, "they mirror
// pkg/checks/bundle's sections — the enrichment bundle IS a bundle,
// so agents parse one shape") — with nothing else tying them
// together. This test drives the SAME broken-workload fixture through
// both surfaces and compares the observable section sequences:
//
//   - the `sections=` field of each bundle.target head line (the
//     declared contract string agents parse), and
//   - the first-appearance order of `section=` values in each body
//     (the actual emission order, so a stage reorder that leaves the
//     head join untouched still fails here).
//
// All four sequences must be byte-identical to frozenBundleSections.
// If this fails, one half of the contract moved: fix the side that
// drifted (or, for a deliberate schema change, move BOTH constant
// sets together and renegotiate the frozen shape per repo-map.md
// "Frozen contracts").
func TestBundleSectionContractFrozen(t *testing.T) {
	t.Parallel()

	// Surface 1: the CLI/MCP `lookout bundle` command
	// (pkg/checks/bundle), on the enrichment suite's fixture cluster.
	cmd := bundle.New(bundle.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) {
			return fake.NewClientset(enrichFixtureObjects()...), nil
		},
		Logs: enrichLogFixture(),
		Now:  func() time.Time { return enrichNow },
	})
	res := checktest.Run(t, cmd, "--workload=Deployment/prod/api")
	cliHead := headSectionsOf(t, res.Stdout, "lookout bundle stdout")
	cliBody := bodySectionOrderOf(res.Stdout)

	// Surface 2: the §7.6 enrichment bundle riding the incident
	// inject (internal/watch enrich path), same fixture.
	base, injects := newRoutingFakeDaemon(t)
	e := testEnricher(nil, fake.NewClientset(enrichFixtureObjects()...), enrichLogFixture())
	d := newEnrichedDispatcher(t, base, e)
	d.DispatchSignal(context.Background(), crashSignal())
	if len(*injects) != 1 {
		t.Fatalf("enrichment drill produced %d injects, want 1", len(*injects))
	}
	enrichBundle := bundleOf(t, (*injects)[0].Body)
	enrichHead := headSectionsOf(t, enrichBundle, "enrichment bundle")
	enrichBody := bodySectionOrderOf(enrichBundle)

	for _, got := range []struct{ surface, sections string }{
		{"lookout bundle head `sections=` (pkg/checks/bundle/bundle.go section constants)", cliHead},
		{"lookout bundle body section= emission order (pkg/checks/bundle)", cliBody},
		{"enrichment head `sections=` (internal/watch/enrich.go enrichSection* constants)", enrichHead},
		{"enrichment body section= emission order (internal/watch/enrich.go stage order)", enrichBody},
	} {
		if got.sections != frozenBundleSections {
			t.Errorf("bundle section contract drifted on %s:\n got: %s\nwant: %s\nthe identifiers/order are declared twice (pkg/checks/bundle/bundle.go and internal/watch/enrich.go) and agents parse ONE shape across both surfaces — move both together or revert",
				got.surface, got.sections, frozenBundleSections)
		}
	}
	if cliHead != enrichHead {
		t.Errorf("the two surfaces declare DIFFERENT section contracts: lookout bundle says %q, enrichment says %q — pkg/checks/bundle/bundle.go and internal/watch/enrich.go forked", cliHead, enrichHead)
	}
}

// headSectionsOf extracts the `sections=` value from the
// kind=bundle.target head line of a rendered bundle.
func headSectionsOf(t *testing.T, out, surface string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, "kind=bundle.target") {
			continue
		}
		for _, tok := range strings.Fields(line) {
			if v, ok := strings.CutPrefix(tok, "sections="); ok {
				return v
			}
		}
		t.Fatalf("%s: bundle.target head line has no sections= field:\n%s", surface, line)
	}
	t.Fatalf("%s: no kind=bundle.target head line found:\n%s", surface, out)
	return ""
}

// bodySectionOrderOf returns the first-appearance order of section=
// values across a rendered bundle's lines, comma-joined. (CutPrefix
// on "section=" cannot match the head's "sections=" field.)
func bodySectionOrderOf(out string) string {
	var order []string
	seen := map[string]bool{}
	for _, line := range strings.Split(out, "\n") {
		for _, tok := range strings.Fields(line) {
			v, ok := strings.CutPrefix(tok, "section=")
			if !ok || seen[v] {
				continue
			}
			seen[v] = true
			order = append(order, v)
		}
	}
	return strings.Join(order, ",")
}
