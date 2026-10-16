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

package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"k8s.io/client-go/rest"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

func TestParseScopes(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string // Scope.String() of each
		kube bool
		path string
	}{
		{in: "my-proj", want: []string{"my-proj"}},
		{in: "my-proj/us-central1", want: []string{"my-proj/us-central1"}},
		{in: "fleet:host", want: []string{"fleet:host"}},
		{in: "folders/123", want: []string{"folders/123"}},
		{in: "organizations/456", want: []string{"organizations/456"}},
		{in: " a , b/us-east1,fleet:h ,folders/1,organizations/2", want: []string{"a", "b/us-east1", "fleet:h", "folders/1", "organizations/2"}},
		{in: "a,a,folders/1,folders/1", want: []string{"a", "folders/1"}},
		{in: "kubeconfig", kube: true},
		{in: "  kubeconfig  ", kube: true},
		{in: "kubeconfig:/etc/lookout/fleet.yaml", kube: true, path: "/etc/lookout/fleet.yaml"},
		{in: "kubeconfig: /etc/k.yaml ", kube: true, path: "/etc/k.yaml"},
		{in: "kubeconfigs", want: []string{"kubeconfigs"}},
		// kubeconfig:<path> takes the rest of the value verbatim, as it
		// always has: a path may contain a comma.
		{in: "kubeconfig:/x,my-proj", kube: true, path: "/x,my-proj"},
	} {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseScopes(tc.in)
			if err != nil {
				t.Fatalf("ParseScopes(%q): %v", tc.in, err)
			}
			if got.Kubeconfig != tc.kube || got.KubeconfigPath != tc.path {
				t.Errorf("kubeconfig = (%t, %q), want (%t, %q)", got.Kubeconfig, got.KubeconfigPath, tc.kube, tc.path)
			}
			var names []string
			for _, s := range got.Scopes {
				names = append(names, s.String())
			}
			if !reflect.DeepEqual(names, tc.want) {
				t.Errorf("scopes = %v, want %v", names, tc.want)
			}
		})
	}
}

func TestParseScopesKinds(t *testing.T) {
	got, err := ParseScopes("p/l,fleet:h,folders/1,organizations/2")
	if err != nil {
		t.Fatal(err)
	}
	want := []cloud.Scope{
		{Kind: cloud.ScopeProject, Project: "p", Location: "l"},
		{Kind: cloud.ScopeFleet, Project: "h"},
		{Kind: cloud.ScopeFolder, ID: "1"},
		{Kind: cloud.ScopeOrganization, ID: "2"},
	}
	if !reflect.DeepEqual(got.Scopes, want) {
		t.Errorf("got %+v", got.Scopes)
	}
}

func TestParseScopesErrors(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", "empty"},
		{" , ", "names no scope"},
		{"my-proj,kubeconfig", "cannot be combined"},
		{"folders/abc", "numeric ID"},
		{"organizations/", "numeric ID"},
		{"fleet:", "fleet:<host-project>"},
		{"fleet:a/b", "fleet:<host-project>"},
		{"a/b/c", "is not a project"},
		{"/us-central1", "is not a project"},
	} {
		t.Run(tc.in, func(t *testing.T) {
			_, err := ParseScopes(tc.in)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("ParseScopes(%q) err = %v, want it to contain %q", tc.in, err, tc.want)
			}
		})
	}
}

func TestParseClusters(t *testing.T) {
	refs, err := ParseClusters("prod-us=abc.us-central1.gke.goog, prod-eu=def.europe-west1.gke.goog")
	if err != nil {
		t.Fatalf("ParseClusters: %v", err)
	}
	want := []cloud.ClusterRef{
		{Name: "prod-us", Endpoint: "abc.us-central1.gke.goog"},
		{Name: "prod-eu", Endpoint: "def.europe-west1.gke.goog"},
	}
	if !reflect.DeepEqual(refs, want) {
		t.Errorf("refs = %+v", refs)
	}
	refs, err = ParseClusters("uid123.us-central1.gke.goog")
	if err != nil || len(refs) != 1 || refs[0].Name != "uid123" {
		t.Errorf("a bare endpoint should derive its first-label name, got %+v (%v)", refs, err)
	}
}

func TestParseClustersErrors(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"empty", "", "empty"},
		{"no endpoint", "prod-us=", "no endpoint"},
		{"no name", "=abc.gke.goog", "no cluster name"},
		{"duplicate", "a=x.gke.goog,a=y.gke.goog", "duplicate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseClusters(tc.in); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("ParseClusters(%q) err = %v, want it to contain %q", tc.in, err, tc.want)
			}
		})
	}
}

func writeKubeconfig(t *testing.T) string {
	t.Helper()
	cfg := `apiVersion: v1
kind: Config
current-context: prod-eu
clusters:
- name: eu
  cluster:
    server: https://eu.example.test:443
- name: us
  cluster:
    server: https://us.example.test:443
contexts:
- name: prod-eu
  context:
    cluster: eu
    user: alice
- name: prod-us
  context:
    cluster: us
    user: alice
users:
- name: alice
  user:
    token: t0ken
`
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// A kubeconfig fleet needs no cloud provider and no build tag: the
// untagged build resolves the NoProvider sentinel.
func TestResolveKubeconfigNeedsNoProvider(t *testing.T) {
	path := writeKubeconfig(t)
	src, err := Resolve(context.Background(), Spec{ClustersFrom: "kubeconfig:" + path})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !strings.Contains(src.Describe(), path) {
		t.Errorf("Describe() = %q, want it to name the kubeconfig", src.Describe())
	}
	res, err := src.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	refs := res.Clusters
	if len(refs) != 2 || refs[0].Name != "prod-eu" || refs[1].Name != "prod-us" {
		t.Fatalf("refs = %+v, want the two contexts sorted by name", refs)
	}
	// A kubeconfig knows no project, location or region, and the refs
	// must not invent them: a wrong failure domain goes into the §8
	// fingerprint hash.
	if refs[0].Project != "" || refs[0].Location != "" || refs[0].Region != "" {
		t.Errorf("ref %+v carries cloud identity a kubeconfig cannot know", refs[0])
	}
	if refs[0].Endpoint != "https://eu.example.test:443" {
		t.Errorf("ref endpoint = %q, want the context's server", refs[0].Endpoint)
	}
}

// Without a Fleet-capable provider, the error names both ways out.
func TestResolveNoCloudFleetNamesBothWaysOut(t *testing.T) {
	for _, spec := range []Spec{{Clusters: "a=x.gke.goog"}, {ClustersFrom: "folders/1"}} {
		_, err := Resolve(context.Background(), spec)
		if err == nil {
			t.Fatalf("Resolve(%+v) with no Fleet-capable provider returned no error", spec)
		}
		for _, want := range []string{"-tags gke", "--clusters-from=kubeconfig"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not mention %q", err, want)
			}
		}
	}
}

func TestResolveRefusesBothFlags(t *testing.T) {
	_, err := Resolve(context.Background(), Spec{Clusters: "a=x", ClustersFrom: "p"})
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("err = %v", err)
	}
}

// fakeScoped answers Discover per scope.
type fakeScoped struct {
	byScope map[string]cloud.Discovery
	errs    map[string]error
}

func (f fakeScoped) DiscoverClusters(context.Context) ([]cloud.ClusterRef, error) { return nil, nil }
func (f fakeScoped) RESTConfig(context.Context, cloud.ClusterRef) (*rest.Config, error) {
	return &rest.Config{}, nil
}
func (f fakeScoped) Discover(_ context.Context, s cloud.Scope) (cloud.Discovery, error) {
	return f.byScope[s.String()], f.errs[s.String()]
}

func ref(project, location, name string) cloud.ClusterRef {
	return cloud.ClusterRef{Name: name, Project: project, Location: location, Endpoint: name + ".gke.goog"}
}

// TestCloudSourceMergesScopes: overlapping scopes yield each cluster
// once; one scope failing is reported, not fatal; the notes say what
// each scope did, fallback and skips included.
func TestCloudSourceMergesScopes(t *testing.T) {
	scopes, err := ParseScopes("proj-a,folders/1,fleet:host")
	if err != nil {
		t.Fatal(err)
	}
	fake := fakeScoped{
		byScope: map[string]cloud.Discovery{
			"proj-a": {Via: "container", Clusters: []cloud.ClusterRef{ref("proj-a", "us-central1", "prod")}},
			"folders/1": {
				Via:      "resourcemanager",
				Fallback: "Cloud Asset Inventory could not answer: API disabled",
				Skipped:  map[string]int{"projects with the Kubernetes Engine API off": 3},
				Clusters: []cloud.ClusterRef{ref("proj-a", "us-central1", "prod"), ref("proj-b", "europe-west1", "prod")},
			},
		},
		errs: map[string]error{"fleet:host": errors.New("permission denied")},
	}
	src := cloudSource{fleet: fake, scoped: fake, name: "gke", scopes: scopes.Scopes}
	res, err := src.Discover(context.Background())
	if err != nil {
		t.Fatalf("one scope failing among three is not an error: %v", err)
	}
	want := []cloud.ClusterRef{ref("proj-a", "us-central1", "prod"), ref("proj-b", "europe-west1", "prod")}
	if !reflect.DeepEqual(res.Clusters, want) {
		t.Errorf("clusters = %+v, want %+v", res.Clusters, want)
	}
	notes := strings.Join(res.Notes(), "\n")
	for _, want := range []string{
		`scope "proj-a": 1 cluster(s) via container`,
		`scope "folders/1": 2 cluster(s) via resourcemanager, 1 already found by an earlier scope (fallback: "Cloud Asset Inventory could not answer: API disabled"); skipped 3 projects with the Kubernetes Engine API off`,
		`scope "fleet:host" FAILED, its clusters are not served: "permission denied"`,
	} {
		if !strings.Contains(notes, want) {
			t.Errorf("notes missing %q:\n%s", want, notes)
		}
	}
}

func TestCloudSourceEveryScopeFailingIsAnError(t *testing.T) {
	scopes, _ := ParseScopes("a,b")
	fake := fakeScoped{errs: map[string]error{"a": errors.New("no a"), "b": errors.New("no b")}}
	src := cloudSource{fleet: fake, scoped: fake, name: "gke", scopes: scopes.Scopes}
	_, err := src.Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "no a") || !strings.Contains(err.Error(), "no b") {
		t.Errorf("err = %v, want both scopes' errors", err)
	}
}

// TestNotesQuoteOutsideText: a scope's name or an API message cannot
// break a log line in two.
func TestNotesQuoteOutsideText(t *testing.T) {
	res := Result{Scopes: []ScopeResult{{Scope: "x\nforged line", Err: errors.New("bad\nthing")}}}
	for _, n := range res.Notes() {
		if strings.Contains(n, "\n") {
			t.Errorf("note %q contains a raw newline", n)
		}
	}
}

func TestStaticClustersNeedNoDiscovery(t *testing.T) {
	refs, _ := ParseClusters("a=x.gke.goog")
	src := cloudSource{fleet: fakeScoped{}, name: "gke", static: refs}
	res, err := src.Discover(context.Background())
	if err != nil || !reflect.DeepEqual(res.Clusters, refs) || len(res.Scopes) != 0 {
		t.Errorf("got %+v, %v", res, err)
	}
}
