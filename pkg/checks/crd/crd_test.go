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

package crd_test

// Tests for the CRD seam itself, separate from any one detector: the
// three answers discovery can give (served, absent, denied), the
// per-invocation cache, the degradation envelope, and the unstructured
// readers. Every detector built on this package inherits whatever is
// asserted here, so the degradation shape in particular is pinned
// exactly rather than approximately.

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/crd"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

var testGroup = crd.Group{
	Name:      "Widget API",
	GV:        schema.GroupVersion{Group: "widgets.example.com", Version: "v1"},
	Resources: []string{"widgets", "widgetclasses"},
	Install:   "install the widget operator",
}

// discoveryServing returns a discovery client that serves exactly the
// named resources of the test group.
func discoveryServing(resources ...string) discovery.DiscoveryInterface {
	cs := fake.NewClientset()
	list := &metav1.APIResourceList{GroupVersion: testGroup.GV.String()}
	for _, r := range resources {
		list.APIResources = append(list.APIResources, metav1.APIResource{Name: r})
	}
	cs.Resources = []*metav1.APIResourceList{list}
	return cs.Discovery()
}

func TestResolveFullyServed(t *testing.T) {
	a := crd.NewResolver(discoveryServing("widgets", "widgetclasses")).Resolve(testGroup)
	if !a.Any() {
		t.Fatal("group should be available")
	}
	if !a.Serves("widgets") || !a.Serves("widgetclasses") {
		t.Errorf("both resources should be served: %+v", a.Served)
	}
	if len(a.Missing) != 0 {
		t.Errorf("nothing should be missing: %v", a.Missing)
	}
	if a.Reason != "" {
		t.Errorf("reason should be empty when everything is served: %q", a.Reason)
	}
}

// A partially served group is normal — an older install of an API
// serves some of its kinds and not others — and the detector degrades
// to what it has rather than refusing outright.
func TestResolvePartiallyServed(t *testing.T) {
	a := crd.NewResolver(discoveryServing("widgets")).Resolve(testGroup)
	if !a.Any() {
		t.Fatal("a partially served group is still available")
	}
	if a.Serves("widgetclasses") {
		t.Error("widgetclasses is not served")
	}
	if got := strings.Join(a.Missing, ","); got != "widgetclasses" {
		t.Errorf("missing = %q, want widgetclasses", got)
	}
}

// The distinction that matters most: a group nobody installed and a
// group discovery could not reach must not read the same, because one
// is a fact about the cluster and the other is a fact about our
// credentials.
func TestResolveDistinguishesAbsentFromFailed(t *testing.T) {
	absent := crd.NewResolver(fake.NewClientset().Discovery()).Resolve(testGroup)
	if absent.Any() {
		t.Fatal("an unserved group is not available")
	}
	if !strings.Contains(absent.Reason, "is not installed") {
		t.Errorf("absent reason = %q, want an is-not-installed phrasing", absent.Reason)
	}

	cs := fake.NewClientset()
	cs.PrependReactor("get", "resource", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("widgets.example.com is forbidden")
	})
	denied := crd.NewResolver(cs.Discovery()).Resolve(testGroup)
	if denied.Any() {
		t.Fatal("a denied group is not available")
	}
	if !strings.Contains(denied.Reason, "discovery for") || !strings.Contains(denied.Reason, "forbidden") {
		t.Errorf("denied reason = %q, want the discovery error carried through", denied.Reason)
	}
}

// A group served with none of the resources we want is "not
// installed" as far as the detector is concerned — the CRDs of some
// other operator sharing the group name do not help.
func TestResolveGroupServesNoneOfWhatWeWant(t *testing.T) {
	a := crd.NewResolver(discoveryServing("gadgets")).Resolve(testGroup)
	if a.Any() {
		t.Fatal("none of the wanted resources are served")
	}
	if !strings.Contains(a.Reason, "serves none of") {
		t.Errorf("reason = %q", a.Reason)
	}
}

func TestResolveNilDiscovery(t *testing.T) {
	a := crd.NewResolver(nil).Resolve(testGroup)
	if a.Any() {
		t.Fatal("no discovery client means nothing is known to be served")
	}
	if !strings.Contains(a.Reason, "no discovery client") {
		t.Errorf("reason = %q", a.Reason)
	}
}

// The cache is what makes a composition affordable: N CRD-gated checks
// sharing one Resolver pay one round trip per group, not N.
func TestResolveCachesPerGroupVersion(t *testing.T) {
	cs := fake.NewClientset()
	cs.Resources = []*metav1.APIResourceList{{
		GroupVersion: testGroup.GV.String(),
		APIResources: []metav1.APIResource{{Name: "widgets"}, {Name: "widgetclasses"}},
	}}
	calls := 0
	cs.PrependReactor("get", "resource", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		return false, nil, nil
	})
	r := crd.NewResolver(cs.Discovery())
	for i := 0; i < 5; i++ {
		if !r.Resolve(testGroup).Any() {
			t.Fatal("group should be available on every call")
		}
	}
	if calls != 1 {
		t.Errorf("discovery called %d times, want 1", calls)
	}
}

// A fresh Resolver does not inherit the last one's answer. This is the
// property that keeps the long-lived MCP server honest: install the
// CRDs and the very next tool call sees them.
func TestResolverIsNotProcessWide(t *testing.T) {
	if crd.NewResolver(fake.NewClientset().Discovery()).Resolve(testGroup).Any() {
		t.Fatal("group is absent from the first cluster")
	}
	if !crd.NewResolver(discoveryServing("widgets")).Resolve(testGroup).Any() {
		t.Fatal("a new resolver over a cluster that serves the group must see it")
	}
}

// unavailableCommand is the degradation path on its own, so the shape
// every CRD detector inherits can be asserted exactly once.
func unavailableCommand(disc discovery.DiscoveryInterface) checks.Command {
	return checks.Command{
		Name:    "demo unavailable",
		MCPName: "k8s_demo_unavailable",
		Summary: "Test-only command exercising the CRD degradation path.",
		Output:  crd.UnavailableFields(),
		Run: func(_ context.Context, inv emit.Invocation) (int, error) {
			return crd.EmitUnavailable(inv, crd.NewResolver(disc).Resolve(testGroup))
		},
	}
}

// The contract, mirroring cloud.unavailable: exit 0, scanned=0, one
// info finding naming the group, and a summary note. A cluster without
// the CRDs must never look like a cluster that passed the check.
func TestEmitUnavailableEnvelope(t *testing.T) {
	res := checktest.Run(t, unavailableCommand(fake.NewClientset().Discovery()))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0 — an absent CRD is a degradation, not an error; stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want one finding and one summary line, got %d: %q", len(lines), res.Stdout)
	}
	want := `kind=crd.unavailable severity=info reason=APIGroupNotServed message="Widget API is not installed: the widgets.example.com/v1 API group is not served by this cluster — install the widget operator" api_group=widgets.example.com/v1 resources=widgets,widgetclasses`
	if lines[0] != want {
		t.Errorf("finding\n got: %s\nwant: %s", lines[0], want)
	}
	if !strings.HasPrefix(lines[1], "scanned=0 findings=1 ") {
		t.Errorf("summary should report nothing scanned: %s", lines[1])
	}
	if !strings.Contains(lines[1], `unavailable="Widget API is not installed`) {
		t.Errorf("summary should carry the unavailable note: %s", lines[1])
	}
}

func TestUnavailablePassesContract(t *testing.T) {
	checktest.VerifyContract(t, unavailableCommand(fake.NewClientset().Discovery()))
}

// A partially served group returns real answers, so narrowing coverage
// silently would be the coverage lie §11 forbids.
func TestPartialNote(t *testing.T) {
	res := checktest.Run(t, checks.Command{
		Name:    "demo partial",
		MCPName: "k8s_demo_partial",
		Summary: "Test-only command exercising the partial-coverage note.",
		Output:  crd.UnavailableFields(),
		Run: func(_ context.Context, inv emit.Invocation) (int, error) {
			a := crd.NewResolver(discoveryServing("widgets")).Resolve(testGroup)
			return 1, crd.PartialNote(inv, a)
		},
	})
	if !strings.Contains(res.Stdout, "not_served=widgetclasses") {
		t.Errorf("summary should name what could not be read: %q", res.Stdout)
	}
}

// A fully served group adds no note, and an entirely absent one
// already has EmitUnavailable to speak for it.
func TestPartialNoteSilentCases(t *testing.T) {
	for _, tc := range []struct {
		name string
		disc discovery.DiscoveryInterface
	}{
		{"fully served", discoveryServing("widgets", "widgetclasses")},
		{"nothing served", fake.NewClientset().Discovery()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, checks.Command{
				Name:    "demo partial",
				MCPName: "k8s_demo_partial",
				Summary: "Test-only command exercising the partial-coverage note.",
				Output:  crd.UnavailableFields(),
				Run: func(_ context.Context, inv emit.Invocation) (int, error) {
					return 0, crd.PartialNote(inv, crd.NewResolver(tc.disc).Resolve(testGroup))
				},
			})
			if strings.Contains(res.Stdout, "not_served=") {
				t.Errorf("no note expected: %q", res.Stdout)
			}
		})
	}
}

// The readers exist so a detector can walk a half-written status
// without a presence check at every hop. A controller that has not
// reconciled yet has written none of these fields, and that is normal.
func TestUnstructuredReadersOnAbsentFields(t *testing.T) {
	empty := map[string]any{}
	if got := crd.Str(empty, "spec", "name"); got != "" {
		t.Errorf("Str = %q, want empty", got)
	}
	if _, ok := crd.Int(empty, "spec", "port"); ok {
		t.Error("Int should report absence")
	}
	if got := crd.Slice(empty, "spec", "rules"); got != nil {
		t.Errorf("Slice = %v, want nil", got)
	}
	if got := crd.Map(empty, "spec", "ref"); got != nil {
		t.Errorf("Map = %v, want nil", got)
	}
	if got := crd.Conditions(empty, "status", "conditions"); len(got) != 0 {
		t.Errorf("Conditions = %v, want none", got)
	}
	// Chaining through an absent map is the point: no hop needs a
	// presence check of its own.
	if got := crd.Str(crd.Map(empty, "spec", "ref"), "name"); got != "" {
		t.Errorf("chained Str = %q, want empty", got)
	}
}

// Wrong-typed fields read as absent rather than panicking: a CRD is
// whatever the cluster admitted, and we are not the validator.
func TestUnstructuredReadersOnWrongTypes(t *testing.T) {
	obj := map[string]any{"spec": map[string]any{
		"name":  int64(7),
		"port":  "eighty",
		"rules": "not-a-list",
	}}
	if got := crd.Str(obj, "spec", "name"); got != "" {
		t.Errorf("Str over a number = %q, want empty", got)
	}
	if _, ok := crd.Int(obj, "spec", "port"); ok {
		t.Error("Int over a string should report absence")
	}
	if got := crd.Slice(obj, "spec", "rules"); got != nil {
		t.Errorf("Slice over a string = %v, want nil", got)
	}
}

func TestConditions(t *testing.T) {
	obj := map[string]any{"status": map[string]any{"conditions": []any{
		map[string]any{"type": "Accepted", "status": "True"},
		map[string]any{"type": "Programmed", "status": "False", "reason": "Pending", "message": "no address"},
		"junk", // dropped, not fatal
	}}}
	conds := crd.Conditions(obj, "status", "conditions")
	if len(conds) != 2 {
		t.Fatalf("got %d conditions, want 2: %+v", len(conds), conds)
	}
	acc, ok := crd.FindCondition(conds, "Accepted")
	if !ok || !acc.True() {
		t.Errorf("Accepted = %+v, ok=%v", acc, ok)
	}
	prog, ok := crd.FindCondition(conds, "Programmed")
	if !ok || prog.True() {
		t.Fatalf("Programmed = %+v, ok=%v", prog, ok)
	}
	if prog.Reason != "Pending" || prog.Message != "no address" {
		t.Errorf("Programmed detail lost: %+v", prog)
	}
	// Absent is not False: "no controller has looked at this" and "a
	// controller looked and said no" are different, and only the second
	// is a finding.
	if _, ok := crd.FindCondition(conds, "ResolvedRefs"); ok {
		t.Error("an unwritten condition must report absent")
	}
}

// Guard against the fake discovery client drifting: the seam's
// absent-group detection depends on recognising this error.
func TestFakeDiscoveryStillReturnsNotFound(t *testing.T) {
	_, err := (&fakediscovery.FakeDiscovery{Fake: &k8stesting.Fake{}}).
		ServerResourcesForGroupVersion(testGroup.GV.String())
	if err == nil {
		t.Fatal("expected an error for an unserved group-version")
	}
}
