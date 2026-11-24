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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/client-go/rest"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

func TestParseScopesSupportsAutoAndProjectResourcePaths(t *testing.T) {
	got, err := ParseScopes("auto")
	if err != nil {
		t.Fatalf("ParseScopes(auto): %v", err)
	}
	if !got.Auto || len(got.Scopes) != 0 || got.Kubeconfig {
		t.Fatalf("ParseScopes(auto) = %+v, want Auto=true", got)
	}

	if _, err := ParseScopes("auto,my-proj"); err == nil {
		t.Error("ParseScopes accepted auto combined with another scope")
	}

	resGot, err := ParseScopes("projects/proj-a,projects/proj-b/locations/us-east1")
	if err != nil {
		t.Fatalf("ParseScopes(projects/...): %v", err)
	}
	if len(resGot.Scopes) != 2 ||
		resGot.Scopes[0] != (cloud.Scope{Kind: cloud.ScopeProject, Project: "proj-a"}) ||
		resGot.Scopes[1] != (cloud.Scope{Kind: cloud.ScopeProject, Project: "proj-b", Location: "us-east1"}) {
		t.Errorf("ParseScopes(projects/...) = %+v", resGot.Scopes)
	}
}

func TestCredentialProxyDiscoveryRESTConfigAndThreeTierResolve(t *testing.T) {
	var seenPaths []string
	var seenAuth []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPaths = append(seenPaths, r.URL.Path)
		seenAuth = append(seenAuth, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/gcp/container.googleapis.com/v1/projects/proj-a/locations/-/clusters":
			// Private cluster with NO dnsEndpointConfig still works via /v1/k8s/.
			_, _ = io.WriteString(w, `{"clusters":[{"name":"private-prod","location":"us-central1"}]}`)
		case "/v1/gcp/container.googleapis.com/v1/projects/proj-tier1/locations/us-east1/clusters/c1":
			_, _ = io.WriteString(w, `{"name":"c1","location":"us-east1"}`)
		case "/v1/gcp/container.googleapis.com/v1/projects/proj-tier2/locations/europe-west1/clusters/c2":
			// Simulate proxy only allowing project/location list, refusing single GET.
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error":"GET cluster forbidden"}`)
		case "/v1/gcp/container.googleapis.com/v1/projects/proj-tier2/locations/europe-west1/clusters":
			_, _ = io.WriteString(w, `{"clusters":[{"name":"c2","location":"europe-west1"}]}`)
		case "/v1/gcp/container.googleapis.com/v1/projects/proj-tier3/locations/asia-east1/clusters/c3",
			"/v1/gcp/container.googleapis.com/v1/projects/proj-tier3/locations/asia-east1/clusters":
			// Simulate K8s-only proxy with /v1/gcp/ completely disabled.
			w.WriteHeader(http.StatusNotFound)
		case "/v1/k8s/proj-tier3/asia-east1/c3/api/v1/pods":
			_, _ = io.WriteString(w, `{"kind":"PodList","apiVersion":"v1","items":[]}`)
		case "/v1/gcp/container.googleapis.com/v1/projects/proj-google404/locations/us-central1/clusters/gone":
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":{"code":404,"status":"NOT_FOUND","message":"not found"}}`)
		case "/v1/gcp/container.googleapis.com/v1/projects/proj-acl403/locations/us-central1/clusters/denied",
			"/v1/gcp/container.googleapis.com/v1/projects/proj-acl403/locations/us-central1/clusters":
			w.WriteHeader(http.StatusNotFound)
		case "/v1/k8s/proj-acl403/us-central1/denied/api/v1/pods":
			// Plain-text 403 from credential proxy ACL (NOT kube-apiserver RBAC Status).
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, "denied by proxy ACL")
		case "/v1/k8s/proj-a/us-central1/private-prod/version":
			_, _ = io.WriteString(w, `{"gitVersion":"v1.31.0"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	tokFile := filepath.Join(t.TempDir(), "tok")
	if err := os.WriteFile(tokFile, []byte("proxy-tok-123\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(cloud.CredentialProxyURLEnv, srv.URL)
	t.Setenv(cloud.CredentialProxyTokenFileEnv, tokFile)

	src, err := Resolve(t.Context(), Spec{ClustersFrom: "proj-a"})
	if err != nil {
		t.Fatalf("Resolve(proj-a) via CREDENTIAL_PROXY_URL: %v", err)
	}
	res, err := src.Discover(t.Context())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(res.Clusters) != 1 || res.Clusters[0].Name != "private-prod" || res.Clusters[0].Project != "proj-a" {
		t.Fatalf("Discover clusters = %+v, want [private-prod]", res.Clusters)
	}

	// RESTConfig routes to /v1/k8s/<p>/<l>/<c> even when Endpoint is empty.
	cfg, err := src.RESTConfig(t.Context(), res.Clusters[0])
	if err != nil {
		t.Fatalf("RESTConfig: %v", err)
	}
	if want := srv.URL + "/v1/k8s/proj-a/us-central1/private-prod"; cfg.Host != want {
		t.Errorf("cfg.Host = %q, want %q", cfg.Host, want)
	}
	rt, err := rest.TransportFor(cfg)
	if err != nil {
		t.Fatalf("TransportFor: %v", err)
	}
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, cfg.Host+"/version", nil)
	resp, err := (&http.Client{Transport: rt}).Do(req)
	if err != nil {
		t.Fatalf("GET /version: %v", err)
	}
	_ = resp.Body.Close()

	// Verify all 3 tiers of SingleClusterSource.ResolveCluster.
	single, ok := src.(SingleClusterSource)
	if !ok {
		t.Fatal("credentialProxySource does not implement SingleClusterSource")
	}

	r1, err := single.ResolveCluster(t.Context(), cloud.ClusterID{Project: "proj-tier1", Location: "us-east1", Cluster: "c1"})
	if err != nil || r1.Name != "c1" || r1.Project != "proj-tier1" {
		t.Fatalf("Tier 1 ResolveCluster = (%+v, %v)", r1, err)
	}

	r2, err := single.ResolveCluster(t.Context(), cloud.ClusterID{Project: "proj-tier2", Location: "europe-west1", Cluster: "c2"})
	if err != nil || r2.Name != "c2" || r2.Project != "proj-tier2" {
		t.Fatalf("Tier 2 ResolveCluster = (%+v, %v)", r2, err)
	}

	r3, err := single.ResolveCluster(t.Context(), cloud.ClusterID{Project: "proj-tier3", Location: "asia-east1", Cluster: "c3"})
	if err != nil || r3.Name != "c3" || r3.Project != "proj-tier3" {
		t.Fatalf("Tier 3 ResolveCluster = (%+v, %v)", r3, err)
	}

	// Non-existent cluster fails cleanly with cloud.ErrClusterNotFound, and an
	// authoritative Google API 404 short-circuits without probing /v1/k8s/.
	if _, err := single.ResolveCluster(t.Context(), cloud.ClusterID{Project: "proj-google404", Location: "us-central1", Cluster: "gone"}); !errors.Is(err, cloud.ErrClusterNotFound) {
		t.Errorf("Google 404 ResolveCluster err = %v, want cloud.ErrClusterNotFound", err)
	}
	for _, p := range seenPaths {
		if strings.HasPrefix(p, "/v1/k8s/proj-google404/") {
			t.Errorf("Google API 404 unexpectedly fell through to K8s probe %q", p)
		}
	}

	// Plain-text 403 from proxy ACL must NOT fabricate a cluster or report ErrClusterNotFound.
	if _, err := single.ResolveCluster(t.Context(), cloud.ClusterID{Project: "proj-acl403", Location: "us-central1", Cluster: "denied"}); err == nil || errors.Is(err, cloud.ErrClusterNotFound) {
		t.Errorf("proxy ACL 403 ResolveCluster err = %v, want non-NotFound runtime error", err)
	}

	for i, auth := range seenAuth {
		if auth != "Bearer proxy-tok-123" {
			t.Errorf("request[%d] (%s) Authorization = %q, want Bearer proxy-tok-123", i, seenPaths[i], auth)
		}
	}
}

func TestAutoScopePrecedenceAndTolerantHostProjectFailure(t *testing.T) {
	// 1. Existing KUBECONFIG file wins over CREDENTIAL_PROXY_URL when auto is used.
	kcPath := filepath.Join(t.TempDir(), "kubeconfig")
	const minKC = "apiVersion: v1\nkind: Config\nclusters:\n- name: c\n  cluster:\n    server: https://127.0.0.1:6443\ncontexts:\n- name: c\n  context:\n    cluster: c\n"
	if err := os.WriteFile(kcPath, []byte(minKC), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", kcPath)
	t.Setenv(cloud.CredentialProxyURLEnv, "")
	t.Setenv(cloud.CoreAgentGoogleAuthEnv, "")
	t.Setenv(cloud.GoogleAuthEnv, "")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "ignored-proj")

	src, err := Resolve(t.Context(), Spec{ClustersFrom: "auto"})
	if err != nil {
		t.Fatalf("Resolve(auto) with KUBECONFIG: %v", err)
	}
	if !strings.Contains(src.Describe(), "kubeconfig") {
		t.Errorf("Describe() = %q, want kubeconfig source when KUBECONFIG is set", src.Describe())
	}

	// 2. Without KUBECONFIG, CREDENTIAL_PROXY_URL + failing host GOOGLE_CLOUD_PROJECT
	// returns 0 clusters with ScopeResult.Err recorded rather than failing Discover.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Container API disabled on host project", http.StatusForbidden)
	}))
	defer srv.Close()

	t.Setenv("KUBECONFIG", "")
	t.Setenv(cloud.CredentialProxyURLEnv, srv.URL)
	t.Setenv(cloud.CredentialProxyTokenFileEnv, "")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "vertex-host-proj")

	autoProxySrc, err := Resolve(t.Context(), Spec{ClustersFrom: "auto"})
	if err != nil {
		t.Fatalf("Resolve(auto) with proxy: %v", err)
	}
	res, err := autoProxySrc.Discover(t.Context())
	if err != nil {
		t.Fatalf("auto Discover failed on host project error: %v", err)
	}
	if len(res.Clusters) != 0 || len(res.Scopes) != 1 || res.Scopes[0].Err == nil {
		t.Errorf("auto Discover result = %+v, want 0 clusters + 1 recorded ScopeResult.Err", res)
	}

	// 3. Explicit --clusters via CREDENTIAL_PROXY_URL resolves GKE paths (including
	// domain-scoped projects and dotted cluster names) without dot-truncation or network at Resolve.
	staticSrc, err := Resolve(t.Context(), Spec{Clusters: "projects/google.com:corp-proj/locations/us-central1/clusters/api.v1,gke_proj-b_us-east1_stage"})
	if err != nil {
		t.Fatalf("Resolve(--clusters) with GKE refs: %v", err)
	}
	staticRes, err := staticSrc.Discover(t.Context())
	if err != nil || len(staticRes.Clusters) != 2 {
		t.Fatalf("static Discover = (%+v, %v), want 2 clusters", staticRes, err)
	}
	if staticRes.Clusters[0].Project != "google.com:corp-proj" || staticRes.Clusters[0].Name != "api.v1" {
		t.Errorf("dotted GKE ref parsed as %+v, want project=google.com:corp-proj name=api.v1", staticRes.Clusters[0])
	}
}
