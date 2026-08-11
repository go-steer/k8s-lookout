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
	"io"
	"net/http"
	"strings"
	"testing"

	"golang.org/x/oauth2"
	container "google.golang.org/api/container/v1"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// fakeFleetLister replays a fixed cluster list and records the parent
// it was asked for.
type fakeFleetLister struct {
	clusters []*container.Cluster
	parent   string
	err      error
}

func (f *fakeFleetLister) ListClusters(_ context.Context, parent string) ([]*container.Cluster, error) {
	f.parent = parent
	return f.clusters, f.err
}

func withDNS(name, location, endpoint string) *container.Cluster {
	return &container.Cluster{
		Name:     name,
		Location: location,
		ControlPlaneEndpointsConfig: &container.ControlPlaneEndpointsConfig{
			DnsEndpointConfig: &container.DNSEndpointConfig{Endpoint: endpoint},
		},
	}
}

func TestDiscoverClusters(t *testing.T) {
	lister := &fakeFleetLister{clusters: []*container.Cluster{
		withDNS("prod-us", "us-central1", "aaa.us-central1.gke.goog"),
		{Name: "legacy", Location: "us-east1"}, // no DNS endpoint configured
	}}

	got, err := discoverClusters(context.Background(), "prod-project", "", lister)
	if err != nil {
		t.Fatalf("discoverClusters: %v", err)
	}

	// Empty location discovers across all locations.
	if lister.parent != "projects/prod-project/locations/-" {
		t.Errorf("parent = %q, want the all-locations wildcard", lister.parent)
	}
	if len(got) != 2 {
		t.Fatalf("refs = %+v, want 2", got)
	}
	if got[0] != (cloud.ClusterRef{Name: "prod-us", Project: "prod-project", Location: "us-central1", Endpoint: "aaa.us-central1.gke.goog"}) {
		t.Errorf("ref[0] = %+v", got[0])
	}
	// The DNS-less cluster still surfaces; RESTConfig fails loudly for it.
	if got[1].Endpoint != "" {
		t.Errorf("ref[1].Endpoint = %q, want empty for a cluster with no DNS endpoint", got[1].Endpoint)
	}
}

func TestDiscoverClustersPinnedLocation(t *testing.T) {
	lister := &fakeFleetLister{}
	if _, err := discoverClusters(context.Background(), "p", "us-central1", lister); err != nil {
		t.Fatalf("discoverClusters: %v", err)
	}
	if lister.parent != "projects/p/locations/us-central1" {
		t.Errorf("parent = %q, want the pinned location", lister.parent)
	}
}

func TestDiscoverClustersNoProject(t *testing.T) {
	_, err := discoverClusters(context.Background(), "", "", &fakeFleetLister{})
	if err == nil {
		t.Fatal("discoverClusters with no project: want a fail-loudly error, got nil")
	}
}

// recordingRT captures the request it sees and returns a bare 200.
type recordingRT struct{ auth string }

func (r *recordingRT) RoundTrip(req *http.Request) (*http.Response, error) {
	r.auth = req.Header.Get("Authorization")
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

func TestRESTConfigADCOverDNSEndpoint(t *testing.T) {
	newTS := func(context.Context, ...string) (oauth2.TokenSource, error) {
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "tok-123", TokenType: "Bearer"}), nil
	}
	ref := cloud.ClusterRef{Name: "prod-us", Endpoint: "aaa.us-central1.gke.goog"}

	cfg, err := restConfig(context.Background(), ref, newTS)
	if err != nil {
		t.Fatalf("restConfig: %v", err)
	}

	if cfg.Host != "https://aaa.us-central1.gke.goog" {
		t.Errorf("Host = %q, want the https DNS endpoint", cfg.Host)
	}
	// The DNS endpoint has a public cert — no CA material is pinned.
	if cfg.CAData != nil || cfg.CAFile != "" {
		t.Errorf("CA material set (%v / %q); the *.gke.goog endpoint needs none", cfg.CAData, cfg.CAFile)
	}
	if cfg.WrapTransport == nil {
		t.Fatal("WrapTransport is nil; the ADC bearer token would never be attached")
	}

	// The wrapped transport must inject the ADC bearer token.
	rec := &recordingRT{}
	rt := cfg.WrapTransport(rec)
	req, _ := http.NewRequest(http.MethodGet, cfg.Host+"/api", nil)
	if _, err := rt.RoundTrip(req); err != nil {
		t.Fatalf("wrapped RoundTrip: %v", err)
	}
	if rec.auth != "Bearer tok-123" {
		t.Errorf("Authorization = %q, want the ADC bearer token", rec.auth)
	}
}

func TestRESTConfigNoEndpoint(t *testing.T) {
	newTS := func(context.Context, ...string) (oauth2.TokenSource, error) {
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "x"}), nil
	}
	_, err := restConfig(context.Background(), cloud.ClusterRef{Name: "no-dns"}, newTS)
	if err == nil {
		t.Fatal("restConfig with no endpoint: want a fail-loudly error, got nil")
	}
}
