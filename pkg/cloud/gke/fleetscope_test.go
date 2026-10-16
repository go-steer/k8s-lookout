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

//go:build gke || allproviders

package gke

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	container "google.golang.org/api/container/v1"
	gkehub "google.golang.org/api/gkehub/v1"
	"google.golang.org/api/googleapi"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// perProjectLister answers clusters.list per parent and records which
// parents were asked for.
type perProjectLister struct {
	mu       sync.Mutex
	clusters map[string][]*container.Cluster // project → clusters
	errs     map[string]error                // project → error
	asked    []string
}

func (l *perProjectLister) ListClusters(_ context.Context, parent string) ([]*container.Cluster, error) {
	project := strings.Split(parent, "/")[1]
	l.mu.Lock()
	l.asked = append(l.asked, project)
	l.mu.Unlock()
	if err := l.errs[project]; err != nil {
		return nil, err
	}
	return l.clusters[project], nil
}

func (l *perProjectLister) askedSorted() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := slices.Clone(l.asked)
	slices.Sort(out)
	return out
}

type fakeHub struct {
	members []*gkehub.Membership
	parent  string
	err     error
}

func (h *fakeHub) ListMemberships(_ context.Context, parent string) ([]*gkehub.Membership, error) {
	h.parent = parent
	return h.members, h.err
}

type fakeAsset struct {
	names []string
	err   error
	scope string
}

func (a *fakeAsset) SearchClusters(_ context.Context, scope string) ([]string, error) {
	a.scope = scope
	return a.names, a.err
}

type fakeWalker struct {
	projects map[string][]string
	folders  map[string][]string
	errs     map[string]error
}

func (w *fakeWalker) ListProjects(_ context.Context, parent string) ([]string, error) {
	if err := w.errs[parent]; err != nil {
		return nil, err
	}
	return w.projects[parent], nil
}

func (w *fakeWalker) ListFolders(_ context.Context, parent string) ([]string, error) {
	if err := w.errs[parent]; err != nil {
		return nil, err
	}
	return w.folders[parent], nil
}

func gkeMember(link string) *gkehub.Membership {
	return &gkehub.Membership{Endpoint: &gkehub.MembershipEndpoint{GkeCluster: &gkehub.GkeCluster{ResourceLink: link}}}
}

func forbidden(reason, msg string) error {
	return &googleapi.Error{Code: http.StatusForbidden, Message: msg, Errors: []googleapi.ErrorItem{{Reason: reason, Message: msg}}}
}

func names(refs []cloud.ClusterRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.Project+"/"+r.Location+"/"+r.Name)
	}
	return out
}

func TestDiscoverScopeProject(t *testing.T) {
	lister := &fakeFleetLister{clusters: []*container.Cluster{withDNS("a", "us-central1", "a.gke.goog")}}
	disc, err := discoverScope(context.Background(), cloud.Scope{Kind: cloud.ScopeProject, Project: "p1", Location: "us-central1"}, scopeDeps{clusters: lister})
	if err != nil {
		t.Fatal(err)
	}
	if disc.Via != viaContainer || len(disc.Clusters) != 1 || disc.Fallback != "" {
		t.Errorf("got %+v", disc)
	}
	if lister.parent != "projects/p1/locations/us-central1" {
		t.Errorf("parent = %q", lister.parent)
	}
}

// TestDiscoverScopeFleet: a fleet's members span projects; only GKE
// members whose cluster still exists count, and a project's clusters
// that are not members are left out.
func TestDiscoverScopeFleet(t *testing.T) {
	hub := &fakeHub{members: []*gkehub.Membership{
		gkeMember("//container.googleapis.com/projects/proj-a/locations/us-central1/clusters/prod"),
		gkeMember("//container.googleapis.com/projects/proj-b/zones/us-east1-b/clusters/prod"),
		{Endpoint: &gkehub.MembershipEndpoint{}}, // attached, not GKE
		{Endpoint: &gkehub.MembershipEndpoint{GkeCluster: &gkehub.GkeCluster{
			ResourceLink:   "//container.googleapis.com/projects/proj-a/locations/us-west1/clusters/gone",
			ClusterMissing: true,
		}}},
		gkeMember("not-a-link"),
	}}
	lister := &perProjectLister{clusters: map[string][]*container.Cluster{
		"proj-a": {withDNS("prod", "us-central1", "a.gke.goog"), withDNS("not-a-member", "us-central1", "x.gke.goog")},
		"proj-b": {withDNS("prod", "us-east1-b", "b.gke.goog")},
	}}
	disc, err := discoverScope(context.Background(), cloud.Scope{Kind: cloud.ScopeFleet, Project: "host"}, scopeDeps{clusters: lister, hub: hub})
	if err != nil {
		t.Fatal(err)
	}
	if hub.parent != "projects/host/locations/-" {
		t.Errorf("hub parent = %q", hub.parent)
	}
	want := []string{"proj-a/us-central1/prod", "proj-b/us-east1-b/prod"}
	if got := names(disc.Clusters); !reflect.DeepEqual(got, want) {
		t.Errorf("clusters = %v, want %v", got, want)
	}
	if disc.Via != viaHub {
		t.Errorf("via = %q", disc.Via)
	}
	wantSkip := map[string]int{skipNotGKE: 2, skipClusterGone: 1}
	if !reflect.DeepEqual(disc.Skipped, wantSkip) {
		t.Errorf("skipped = %v, want %v", disc.Skipped, wantSkip)
	}
	if got := lister.askedSorted(); !reflect.DeepEqual(got, []string{"proj-a", "proj-b"}) {
		t.Errorf("listed projects %v", got)
	}
}

func TestDiscoverScopeFleetRefused(t *testing.T) {
	hub := &fakeHub{err: forbidden("forbidden", "Permission 'gkehub.memberships.list' denied")}
	_, err := discoverScope(context.Background(), cloud.Scope{Kind: cloud.ScopeFleet, Project: "host"}, scopeDeps{hub: hub})
	var pd *cloud.PermissionDeniedError
	if !errors.As(err, &pd) || pd.Permission != "gkehub.memberships.list" {
		t.Errorf("err = %v, want a permission refusal naming gkehub.memberships.list", err)
	}
}

// TestDiscoverScopeFolderViaAsset: one search names the projects that
// hold clusters, and only those are listed.
func TestDiscoverScopeFolderViaAsset(t *testing.T) {
	asset := &fakeAsset{names: []string{
		"//container.googleapis.com/projects/p2/locations/us-central1/clusters/b",
		"//container.googleapis.com/projects/p1/locations/europe-west1/clusters/a",
		"//container.googleapis.com/projects/p2/locations/us-east1/clusters/c",
	}}
	lister := &perProjectLister{clusters: map[string][]*container.Cluster{
		"p1": {withDNS("a", "europe-west1", "a.gke.goog")},
		"p2": {withDNS("b", "us-central1", "b.gke.goog"), withDNS("c", "us-east1", "c.gke.goog")},
	}}
	disc, err := discoverScope(context.Background(), cloud.Scope{Kind: cloud.ScopeFolder, ID: "123"}, scopeDeps{clusters: lister, asset: asset})
	if err != nil {
		t.Fatal(err)
	}
	if asset.scope != "folders/123" || disc.Via != viaAsset || disc.Fallback != "" {
		t.Errorf("scope %q via %q fallback %q", asset.scope, disc.Via, disc.Fallback)
	}
	want := []string{"p1/europe-west1/a", "p2/us-central1/b", "p2/us-east1/c"}
	if got := names(disc.Clusters); !reflect.DeepEqual(got, want) {
		t.Errorf("clusters = %v, want %v", got, want)
	}
	if got := lister.askedSorted(); !reflect.DeepEqual(got, []string{"p1", "p2"}) {
		t.Errorf("listed projects %v, want exactly the two that hold clusters", got)
	}
}

// TestDiscoverScopeOrganizationFallback: Cloud Asset Inventory off, so
// the folder tree is walked instead, and that is reported. A subfolder
// that cannot be read and a project with the API off are counted, not
// fatal.
func TestDiscoverScopeOrganizationFallback(t *testing.T) {
	asset := &fakeAsset{err: forbidden("SERVICE_DISABLED", "Cloud Asset API has not been used in project 1 before or it is disabled.")}
	walker := &fakeWalker{
		projects: map[string][]string{"organizations/9": {"p1"}, "folders/1": {"p2"}},
		folders:  map[string][]string{"organizations/9": {"folders/1"}, "folders/1": {"folders/2"}},
		errs:     map[string]error{"folders/2": forbidden("forbidden", "denied")},
	}
	lister := &perProjectLister{
		clusters: map[string][]*container.Cluster{"p2": {withDNS("b", "us-central1", "b.gke.goog")}},
		errs:     map[string]error{"p1": forbidden("SERVICE_DISABLED", "Kubernetes Engine API has not been used in project p1")},
	}
	disc, err := discoverScope(context.Background(), cloud.Scope{Kind: cloud.ScopeOrganization, ID: "9"}, scopeDeps{clusters: lister, asset: asset, walker: walker})
	if err != nil {
		t.Fatal(err)
	}
	if disc.Via != viaResourceManager {
		t.Errorf("via = %q, want %q", disc.Via, viaResourceManager)
	}
	if !strings.Contains(disc.Fallback, "Cloud Asset API has not been used") {
		t.Errorf("fallback %q does not say why", disc.Fallback)
	}
	if got := names(disc.Clusters); !reflect.DeepEqual(got, []string{"p2/us-central1/b"}) {
		t.Errorf("clusters = %v", got)
	}
	wantSkip := map[string]int{skipFolderNoRead: 1, skipAPIDisabled: 1}
	if !reflect.DeepEqual(disc.Skipped, wantSkip) {
		t.Errorf("skipped = %v, want %v", disc.Skipped, wantSkip)
	}
}

func TestDiscoverScopeAssetOutageIsNotPaperedOver(t *testing.T) {
	asset := &fakeAsset{err: &googleapi.Error{Code: http.StatusInternalServerError, Message: "backend error"}}
	walker := &fakeWalker{}
	_, err := discoverScope(context.Background(), cloud.Scope{Kind: cloud.ScopeFolder, ID: "1"}, scopeDeps{asset: asset, walker: walker})
	if err == nil || !strings.Contains(err.Error(), "backend error") {
		t.Errorf("err = %v, want the asset error, not a fallback", err)
	}
}

func TestDiscoverScopeUnreadableParentIsAnError(t *testing.T) {
	asset := &fakeAsset{err: forbidden("forbidden", "no search")}
	walker := &fakeWalker{errs: map[string]error{"folders/1": forbidden("forbidden", "Permission 'resourcemanager.projects.list' denied")}}
	_, err := discoverScope(context.Background(), cloud.Scope{Kind: cloud.ScopeFolder, ID: "1"}, scopeDeps{asset: asset, walker: walker})
	var pd *cloud.PermissionDeniedError
	if !errors.As(err, &pd) {
		t.Errorf("err = %v, want a permission refusal", err)
	}
}

// TestListInProjectsFailures: refusals and a disabled API are counted;
// another failure is counted too, and is the error only when nothing
// was found anywhere.
func TestListInProjectsFailures(t *testing.T) {
	boom := &googleapi.Error{Code: http.StatusInternalServerError, Message: "boom"}
	lister := &perProjectLister{
		clusters: map[string][]*container.Cluster{"ok": {withDNS("a", "us-central1", "a.gke.goog")}},
		errs: map[string]error{
			"off":    forbidden("SERVICE_DISABLED", "disabled"),
			"denied": forbidden("forbidden", "denied"),
			"gone":   &googleapi.Error{Code: http.StatusNotFound},
			"broken": boom,
		},
	}
	var disc cloud.Discovery
	refs, err := listInProjects(context.Background(), []string{"ok", "off", "denied", "gone", "broken"}, lister, &disc)
	if err != nil || len(refs) != 1 {
		t.Fatalf("refs %v err %v: a partial failure with results is not an error", refs, err)
	}
	want := map[string]int{skipAPIDisabled: 1, skipNoAccess: 2, skipListFailed: 1}
	if !reflect.DeepEqual(disc.Skipped, want) {
		t.Errorf("skipped = %v, want %v", disc.Skipped, want)
	}
	var none cloud.Discovery
	if _, err := listInProjects(context.Background(), []string{"broken"}, lister, &none); !errors.Is(err, boom) {
		t.Errorf("all failed: err = %v, want boom", err)
	}
}

func TestParseClusterName(t *testing.T) {
	for _, tc := range []struct {
		in                      string
		project, location, name string
		ok                      bool
	}{
		{"//container.googleapis.com/projects/p/locations/us-central1/clusters/c", "p", "us-central1", "c", true},
		{"//container.googleapis.com/projects/p/zones/us-east1-b/clusters/c", "p", "us-east1-b", "c", true},
		{"//container.googleapis.com/projects/p/locations/us-central1/clusters/", "", "", "", false},
		{"//compute.googleapis.com/projects/p/zones/z/instances/i", "", "", "", false},
		{"projects/p/locations/l/clusters/c", "", "", "", false},
		{"", "", "", "", false},
	} {
		p, l, n, ok := parseClusterName(tc.in)
		if p != tc.project || l != tc.location || n != tc.name || ok != tc.ok {
			t.Errorf("parseClusterName(%q) = %q %q %q %t", tc.in, p, l, n, ok)
		}
	}
}
