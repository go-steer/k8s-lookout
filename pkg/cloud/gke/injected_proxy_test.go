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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	container "google.golang.org/api/container/v1"
	"google.golang.org/api/option"
	"k8s.io/client-go/rest"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// tripwireMetadataServer fails the test if any code path touches the GCE
// metadata server when injected Google auth or CREDENTIAL_PROXY_URL is active.
func tripwireMetadataServer(t *testing.T) *atomic.Int32 {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		t.Errorf("unexpected GCE metadata request: %s %s", r.Method, r.URL.Path)
		http.Error(w, "no metadata server in distroless task", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(srv.URL, "http://"))
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "no-kubeconfig"))
	t.Setenv("CLOUDSDK_COMPUTE_REGION", "")
	t.Setenv("CLOUDSDK_COMPUTE_ZONE", "")
	t.Setenv("CLOUDSDK_CONTAINER_CLUSTER", "")
	return &hits
}

func TestInjectedGoogleAuthSkipsMetadataAndSendsPlaceholderBearer(t *testing.T) {
	metaHits := tripwireMetadataServer(t)
	t.Setenv(cloud.GoogleAuthEnv, "")
	t.Setenv(cloud.CoreAgentGoogleAuthEnv, "injected")
	t.Setenv(cloud.CredentialProxyURLEnv, "")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "sub-proj")

	// Even with a zero kube.Selection, gke.New must NOT consult GCE metadata
	// (e.g., instance/attributes/cluster-name) when injected auth is active.
	prov, err := New(t.Context(), cloud.Config{})
	if err != nil {
		t.Fatalf("gke.New: %v", err)
	}
	t.Cleanup(func() { _ = prov.Close() })
	if metaHits.Load() != 0 {
		t.Fatalf("metadata hits = %d, want 0", metaHits.Load())
	}

	// Selecting a GKE resource path on kube.Target populates full cluster identity.
	selCtx := kube.WithSelection(t.Context(), kube.Selection{
		Target: &kube.Target{Name: "projects/other-proj/locations/us-central1/clusters/prod"},
	})
	provTarget, err := New(selCtx, cloud.Config{})
	if err != nil {
		t.Fatalf("gke.New with target: %v", err)
	}
	t.Cleanup(func() { _ = provTarget.Close() })
	gp := provTarget.(*Provider)
	if gp.project != "other-proj" || gp.location != "us-central1" || gp.cluster != "prod" {
		t.Errorf("resolved identity = (%q, %q, %q), want (other-proj, us-central1, prod)", gp.project, gp.location, gp.cluster)
	}

	// Outbound Google API calls via clientOptions send Bearer placeholder.
	var gotGKEAuth string
	gkeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotGKEAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"name":"prod","location":"us-central1","controlPlaneEndpointsConfig":{"dnsEndpointConfig":{"endpoint":"gke-123.us-central1.gke.goog"}}}`)
	}))
	defer gkeSrv.Close()

	svc, err := container.NewService(t.Context(), append(clientOptions(t.Context()), option.WithEndpoint(gkeSrv.URL))...)
	if err != nil {
		t.Fatalf("container.NewService: %v", err)
	}
	if _, err := svc.Projects.Locations.Clusters.Get("projects/other-proj/locations/us-central1/clusters/prod").Context(t.Context()).Do(); err != nil {
		t.Fatalf("Clusters.Get: %v", err)
	}
	if gotGKEAuth != "Bearer "+cloud.InjectedBearerToken {
		t.Errorf("GKE API Authorization = %q, want %q", gotGKEAuth, "Bearer "+cloud.InjectedBearerToken)
	}

	// Provider.RESTConfig builds a Kubernetes client config that sends Bearer placeholder
	// without touching ADC or GCE metadata.
	var gotK8sAuth string
	k8sSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotK8sAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Success"}`)
	}))
	defer k8sSrv.Close()

	cfg, err := gp.RESTConfig(t.Context(), cloud.ClusterRef{
		Name:     "prod",
		Project:  "other-proj",
		Location: "us-central1",
		Endpoint: "gke-123.us-central1.gke.goog",
	})
	if err != nil {
		t.Fatalf("RESTConfig: %v", err)
	}
	cfg.Host = k8sSrv.URL
	cfg.TLSClientConfig = rest.TLSClientConfig{Insecure: true}
	rt, err := rest.TransportFor(cfg)
	if err != nil {
		t.Fatalf("TransportFor: %v", err)
	}
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, k8sSrv.URL+"/version", nil)
	resp, err := (&http.Client{Transport: rt}).Do(req)
	if err != nil {
		t.Fatalf("k8s request: %v", err)
	}
	_ = resp.Body.Close()
	if gotK8sAuth != "Bearer "+cloud.InjectedBearerToken {
		t.Errorf("K8s API Authorization = %q, want %q", gotK8sAuth, "Bearer "+cloud.InjectedBearerToken)
	}
	if metaHits.Load() != 0 {
		t.Errorf("metadata hits after GKE+K8s calls = %d, want 0", metaHits.Load())
	}
}

func TestCredentialProxyRoutesGKERESTAndFailsGRPCGracefully(t *testing.T) {
	metaHits := tripwireMetadataServer(t)

	var seenPaths []string
	var seenAuth []string
	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPaths = append(seenPaths, r.URL.Path)
		seenAuth = append(seenAuth, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/gcp/container.googleapis.com/v1/projects/proj-a/locations/-/clusters":
			_, _ = io.WriteString(w, `{"clusters":[{"name":"prod","location":"us-central1"}]}`)
		case "/v1/gcp/container.googleapis.com/v1/projects/proj-b/locations/us-east1/clusters/stage":
			_, _ = io.WriteString(w, `{"name":"stage","location":"us-east1","currentMasterVersion":"1.31.2-gke.100"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer proxySrv.Close()

	tokPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokPath, []byte("proxy-secret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(cloud.CredentialProxyURLEnv, proxySrv.URL)
	t.Setenv(cloud.CredentialProxyTokenFileEnv, tokPath)
	t.Setenv(cloud.GoogleAuthEnv, "")
	t.Setenv(cloud.CoreAgentGoogleAuthEnv, "")

	prov, err := New(t.Context(), cloud.Config{Project: "proj-a", SkipMetadata: true})
	if err != nil {
		t.Fatalf("gke.New: %v", err)
	}
	t.Cleanup(func() { _ = prov.Close() })
	gp := prov.(*Provider)

	refs, err := gp.DiscoverClusters(t.Context())
	if err != nil {
		t.Fatalf("DiscoverClusters via proxy: %v", err)
	}
	if len(refs) != 1 || refs[0].Name != "prod" || refs[0].Project != "proj-a" {
		t.Fatalf("DiscoverClusters = %+v, want [prod in proj-a]", refs)
	}

	ref, err := gp.LookupCluster(t.Context(), "proj-b", "us-east1", "stage")
	if err != nil {
		t.Fatalf("LookupCluster via proxy: %v", err)
	}
	if ref.Name != "stage" || ref.Project != "proj-b" || ref.Location != "us-east1" {
		t.Errorf("LookupCluster = %+v", ref)
	}

	for i, auth := range seenAuth {
		if auth != "Bearer proxy-secret-token" {
			t.Errorf("request[%d] (%s) Authorization = %q, want proxy token", i, seenPaths[i], auth)
		}
	}

	// gRPC-only Google APIs (e.g. Cloud Logging) must not fail option validation
	// with WithHTTPClient incompatible with gRPC; they return CredentialsUnavailableError.
	gpCluster, err := New(t.Context(), cloud.Config{Project: "proj-a", Location: "us-central1", Cluster: "prod"})
	if err != nil {
		t.Fatalf("gke.New with cluster: %v", err)
	}
	t.Cleanup(func() { _ = gpCluster.Close() })
	capAPI, ok := gpCluster.Capacity()
	if !ok {
		t.Fatal("Capacity() = false, want true when project/location/cluster are pinned")
	}
	if _, err := capAPI.ScaleDecisions(t.Context(), cloud.TimeWindow{}); err == nil {
		t.Fatal("gRPC Capacity unexpectedly succeeded via HTTP-only CREDENTIAL_PROXY_URL")
	} else {
		var unavail *cloud.CredentialsUnavailableError
		if !errors.As(err, &unavail) {
			t.Errorf("gRPC Capacity error = %T (%v), want *cloud.CredentialsUnavailableError", err, err)
		}
	}
	if metaHits.Load() != 0 {
		t.Errorf("metadata hits = %d, want 0", metaHits.Load())
	}
}
