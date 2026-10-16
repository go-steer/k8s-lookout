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

package mcpserver

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"golang.org/x/oauth2"
	"k8s.io/client-go/rest"

	"github.com/go-steer/k8s-lookout/internal/fleet"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// fakeFleet is a fleet.Source whose discoveries are scripted: by the
// caller's token when there is one, else in sequence.
type fakeFleet struct {
	mu        sync.Mutex
	byToken   map[string][]cloud.ClusterRef
	sequence  [][]cloud.ClusterRef
	errAfter  int // Discover fails from this call on (0: never)
	discovers int
	configs   int
	cfgCtx    []context.Context
}

func (f *fakeFleet) Describe() string { return "fake fleet" }

func (f *fakeFleet) Discover(ctx context.Context) (fleet.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.discovers++
	if f.errAfter > 0 && f.discovers >= f.errAfter {
		return fleet.Result{}, errors.New("discovery down")
	}
	if ts, ok := kube.CallerTokenFrom(ctx); ok {
		tok, _ := ts.Token()
		return fleet.Result{Clusters: f.byToken[tok.AccessToken]}, nil
	}
	i := min(f.discovers-1, len(f.sequence)-1)
	return fleet.Result{Clusters: f.sequence[i], Scopes: []fleet.ScopeResult{{Scope: "p", Via: "container", Clusters: len(f.sequence[i])}}}, nil
}

func (f *fakeFleet) RESTConfig(ctx context.Context, ref cloud.ClusterRef) (*rest.Config, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.configs++
	f.cfgCtx = append(f.cfgCtx, ctx)
	return &rest.Config{Host: "https://" + ref.Endpoint}, nil
}

func gref(project, location, name string) cloud.ClusterRef {
	return cloud.ClusterRef{Name: name, Project: project, Location: location, Endpoint: name + "." + location + ".gke.goog"}
}

func withToken(ctx context.Context, tok string) context.Context {
	return kube.WithCallerToken(ctx, oauth2.StaticTokenSource(&oauth2.Token{AccessToken: tok}))
}

// TestNameClusters: bare where unique, name@project where the name
// repeats across projects, name@project/location where it repeats
// within one; the canonical name always resolves.
func TestNameClusters(t *testing.T) {
	set := nameClusters([]cloud.ClusterRef{
		gref("proj-a", "us-central1", "prod"),
		gref("proj-b", "us-east1", "prod"),
		gref("proj-a", "us-central1", "dev"),
		gref("proj-c", "us-central1", "api"),
		gref("proj-c", "us-east1", "api"),
		{Name: "kind-local", Endpoint: "https://127.0.0.1:6443"}, // a kubeconfig context
	})
	var got []string
	for _, e := range set.entries {
		got = append(got, e.name+" = "+e.canonical)
	}
	want := []string{
		"api@proj-c/us-central1 = proj-c/us-central1/api",
		"api@proj-c/us-east1 = proj-c/us-east1/api",
		"dev = proj-a/us-central1/dev",
		"kind-local = kind-local",
		"prod@proj-a = proj-a/us-central1/prod",
		"prod@proj-b = proj-b/us-east1/prod",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("names:\n%v\nwant:\n%v", got, want)
	}
	for _, name := range []string{"dev", "proj-a/us-central1/dev", "prod@proj-b", "proj-b/us-east1/prod", "api@proj-c/us-east1"} {
		if _, ok := set.byName[name]; !ok {
			t.Errorf("%q does not resolve", name)
		}
	}
	for _, name := range []string{"prod", "api", "api@proj-c"} {
		if _, ok := set.byName[name]; ok {
			t.Errorf("ambiguous %q resolves", name)
		}
	}
}

func newTestClusters(src *fakeFleet, perCaller bool) (*Clusters, *time.Time) {
	c := NewClusters(src, perCaller, time.Hour)
	c.access = nil // no API server behind the fakes; tests that need access set it
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }
	return c, &now
}

// TestResolveUnknownRediscoversAtMostOncePerMinute: a new cluster is
// found by rediscovering on a miss; a typo repeated within the minute
// does not rediscover again.
func TestResolveUnknownRediscoversAtMostOncePerMinute(t *testing.T) {
	src := &fakeFleet{sequence: [][]cloud.ClusterRef{
		{gref("p", "l", "old")},
		{gref("p", "l", "old"), gref("p", "l", "new")},
	}}
	c, now := newTestClusters(src, false)
	if _, _, err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if e, err := c.Resolve(t.Context(), "", "new"); err != nil || e.canonical != "p/l/new" {
		t.Fatalf("new cluster: %+v, %v", e, err)
	}
	if src.discovers != 2 {
		t.Fatalf("discovers = %d, want 2 (startup, then the miss)", src.discovers)
	}
	*now = now.Add(10 * time.Second)
	if _, err := c.Resolve(t.Context(), "", "typo"); !errors.Is(err, errUnknownCluster) {
		t.Fatalf("typo: %v", err)
	}
	if _, err := c.Resolve(t.Context(), "", "typo"); !errors.Is(err, errUnknownCluster) {
		t.Fatalf("typo again: %v", err)
	}
	if src.discovers != 2 {
		t.Errorf("discovers = %d, want 2: misses within a minute of the last rediscovery must not rediscover", src.discovers)
	}
	*now = now.Add(time.Minute)
	_, _ = c.Resolve(t.Context(), "", "typo")
	if src.discovers != 3 {
		t.Errorf("discovers = %d, want 3 a minute later", src.discovers)
	}
}

// TestPerCallerSets: in Google-token mode each caller's set is their
// own discovery, so one caller cannot name another's cluster.
func TestPerCallerSets(t *testing.T) {
	src := &fakeFleet{byToken: map[string][]cloud.ClusterRef{
		"tok-a": {gref("p", "l", "alpha"), gref("p", "l", "shared")},
		"tok-b": {gref("p", "l", "shared")},
	}}
	c, _ := newTestClusters(src, true)
	if _, n, err := c.Start(t.Context()); err != nil || n != 0 {
		t.Fatalf("Start in per-caller mode discovers nothing: %d, %v", n, err)
	}
	a, b := withToken(t.Context(), "tok-a"), withToken(t.Context(), "tok-b")
	if _, err := c.Resolve(a, "a@x", "alpha"); err != nil {
		t.Fatalf("caller a: %v", err)
	}
	if _, err := c.Resolve(b, "b@x", "alpha"); !errors.Is(err, errUnknownCluster) {
		t.Errorf("caller b resolved caller a's cluster: %v", err)
	}
	if _, err := c.Resolve(b, "b@x", "shared"); err != nil {
		t.Errorf("caller b: %v", err)
	}
	entries, _, err := c.List(b, "b@x", "")
	if err != nil || len(entries) != 1 {
		t.Errorf("caller b's list: %+v, %v", entries, err)
	}
}

// TestCallerSetIsCachedForItsTTL: a caller's set is reused, then
// rediscovered once it is older than the TTL.
func TestCallerSetIsCachedForItsTTL(t *testing.T) {
	src := &fakeFleet{byToken: map[string][]cloud.ClusterRef{"tok": {gref("p", "l", "c")}}}
	c, now := newTestClusters(src, true)
	ctx := withToken(t.Context(), "tok")
	for range 3 {
		if _, err := c.Resolve(ctx, "a@x", "c"); err != nil {
			t.Fatal(err)
		}
	}
	if src.discovers != 1 {
		t.Errorf("discovers = %d, want 1 within the TTL", src.discovers)
	}
	*now = now.Add(defaultCallerSetTTL + time.Second)
	_, _ = c.Resolve(ctx, "a@x", "c")
	if src.discovers != 2 {
		t.Errorf("discovers = %d, want 2 after the TTL", src.discovers)
	}
}

// TestFailedRefreshKeepsServing: a stale shared set whose refresh fails
// is still served; one bad discovery does not take every cluster away.
func TestFailedRefreshKeepsServing(t *testing.T) {
	src := &fakeFleet{sequence: [][]cloud.ClusterRef{{gref("p", "l", "c")}}, errAfter: 2}
	c, now := newTestClusters(src, false)
	if _, _, err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(2 * time.Hour)
	if _, err := c.Resolve(t.Context(), "", "c"); err != nil {
		t.Errorf("a failed refresh dropped the set: %v", err)
	}
}

func TestStartFailsWithNoClusters(t *testing.T) {
	c, _ := newTestClusters(&fakeFleet{sequence: [][]cloud.ClusterRef{{}}}, false)
	if _, _, err := c.Start(t.Context()); err == nil {
		t.Error("an empty fleet started")
	}
	c, _ = newTestClusters(&fakeFleet{sequence: [][]cloud.ClusterRef{{}}, errAfter: 1}, false)
	if _, _, err := c.Start(t.Context()); err == nil {
		t.Error("a failed discovery started")
	}
}

// TestConfigCachedAndDetached: with the server's identity a cluster's
// config is built once, from a context the request's end cannot cancel
// (an ADC token source keeps it for every refresh). Per caller, it is
// built per call.
func TestConfigCachedAndDetached(t *testing.T) {
	src := &fakeFleet{sequence: [][]cloud.ClusterRef{{gref("p", "l", "c")}}}
	c, _ := newTestClusters(src, false)
	if _, _, err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	e, _ := c.Resolve(t.Context(), "", "c")
	ctx, cancel := context.WithCancel(t.Context())
	if _, err := c.Config(ctx, e); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := c.Config(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if src.configs != 1 {
		t.Errorf("configs = %d, want 1 (cached)", src.configs)
	}
	if src.cfgCtx[0].Done() != nil || src.cfgCtx[0].Err() != nil {
		t.Error("the cached config was built from a cancellable context")
	}

	per := &fakeFleet{byToken: map[string][]cloud.ClusterRef{"tok": {gref("p", "l", "c")}}}
	pc, _ := newTestClusters(per, true)
	cctx := withToken(t.Context(), "tok")
	pe, _ := pc.Resolve(cctx, "a@x", "c")
	_, _ = pc.Config(cctx, pe)
	_, _ = pc.Config(cctx, pe)
	if per.configs != 2 {
		t.Errorf("per-caller configs = %d, want 2 (one per call)", per.configs)
	}
}

func TestIsGKEEndpoint(t *testing.T) {
	for endpoint, want := range map[string]bool{
		"gke-abc-123.us-central1.gke.goog":         true,
		"https://gke-abc-123.us-central1.gke.goog": true,
		"https://abc.gke.goog:443":                 true,
		".gke.goog":                                false,
		"https://eks.example.com":                  false,
		"https://evil.gke.goog.example.com":        false,
		"https://10.0.0.1":                         false,
		"":                                         false,
	} {
		if got := IsGKEEndpoint(endpoint); got != want {
			t.Errorf("IsGKEEndpoint(%q) = %t, want %t", endpoint, got, want)
		}
	}
}

// TestPerCallerConfigRefusesNonGKE: in Google-token mode a caller's
// token is never sent to an API server that is not a GKE control plane,
// whatever the fleet source yielded.
func TestPerCallerConfigRefusesNonGKE(t *testing.T) {
	src := &fakeFleet{byToken: map[string][]cloud.ClusterRef{"tok": {{Name: "eks", Endpoint: "https://eks.example.com"}}}}
	c, _ := newTestClusters(src, true)
	ctx := withToken(t.Context(), "tok")
	e, err := c.Resolve(ctx, "a@x", "eks")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Config(ctx, e); err == nil {
		t.Error("a caller's token would have gone to a non-GKE API server")
	}
	if src.configs != 0 {
		t.Errorf("RESTConfig was called %d times", src.configs)
	}
}

// gatedFleet blocks every discovery until released, counting them.
type gatedFleet struct {
	fakeFleet
	gate    chan struct{}
	started chan struct{}
}

func (g *gatedFleet) Discover(ctx context.Context) (fleet.Result, error) {
	g.started <- struct{}{}
	<-g.gate
	return g.fakeFleet.Discover(ctx)
}

// TestConcurrentCallsShareOneDiscovery: an agent's parallel first calls
// wait for one discovery rather than each starting their own.
func TestConcurrentCallsShareOneDiscovery(t *testing.T) {
	src := &gatedFleet{
		fakeFleet: fakeFleet{byToken: map[string][]cloud.ClusterRef{"tok": {gref("p", "l", "c")}}},
		gate:      make(chan struct{}),
		started:   make(chan struct{}, 8),
	}
	c := NewClusters(src, true, time.Hour)
	c.access = nil
	ctx := withToken(t.Context(), "tok")
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Go(func() {
			_, err := c.Resolve(ctx, "a@x", "c")
			errs <- err
		})
	}
	<-src.started
	time.Sleep(50 * time.Millisecond) // let the others reach the wait
	close(src.gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	if src.discovers != 1 {
		t.Errorf("discovers = %d, want 1 shared by four concurrent calls", src.discovers)
	}
}

// TestConfigKeyedByEndpoint: a cluster recreated under its old name has
// a new endpoint, and gets a new config.
func TestConfigKeyedByEndpoint(t *testing.T) {
	old, recreated := gref("p", "l", "c"), gref("p", "l", "c")
	recreated.Endpoint = "new." + old.Endpoint
	src := &fakeFleet{sequence: [][]cloud.ClusterRef{{old}}}
	c, _ := newTestClusters(src, false)
	_, _ = c.Config(t.Context(), clusterEntry{ref: old, canonical: "p/l/c"})
	cfg, _ := c.Config(t.Context(), clusterEntry{ref: recreated, canonical: "p/l/c"})
	if src.configs != 2 || cfg.Host != "https://"+recreated.Endpoint {
		t.Errorf("configs = %d, host = %s: the old endpoint's config was reused", src.configs, cfg.Host)
	}
}

func TestListFilter(t *testing.T) {
	src := &fakeFleet{sequence: [][]cloud.ClusterRef{{gref("proj-a", "us-central1", "prod"), gref("proj-b", "europe-west1", "dev")}}}
	c, _ := newTestClusters(src, false)
	if _, _, err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	for filter, want := range map[string]int{"": 2, "PROD": 1, "europe": 1, "proj-b": 1, "nothing": 0} {
		got, _, err := c.List(t.Context(), "", filter)
		if err != nil || len(got) != want {
			t.Errorf("filter %q: %d entries (%v), want %d", filter, len(got), err, want)
		}
	}
}
