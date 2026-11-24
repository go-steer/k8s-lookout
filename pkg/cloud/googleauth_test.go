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

package cloud

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseClusterRef(t *testing.T) {
	valid := []struct {
		in   string
		want ClusterID
	}{
		{
			in:   "projects/my-proj/locations/us-central1/clusters/prod",
			want: ClusterID{Project: "my-proj", Location: "us-central1", Cluster: "prod"},
		},
		{
			in:   "/projects/my-proj/zones/us-central1-a/clusters/zonal-1/",
			want: ClusterID{Project: "my-proj", Location: "us-central1-a", Cluster: "zonal-1"},
		},
		{
			in:   "//container.googleapis.com/projects/my-proj/locations/europe-west1/clusters/eu-api",
			want: ClusterID{Project: "my-proj", Location: "europe-west1", Cluster: "eu-api"},
		},
		{
			in:   "https://container.googleapis.com/v1/projects/my-proj/locations/us-east1/clusters/east",
			want: ClusterID{Project: "my-proj", Location: "us-east1", Cluster: "east"},
		},
		{
			in:   "gke_my-proj_us-central1_prod_cluster",
			want: ClusterID{Project: "my-proj", Location: "us-central1", Cluster: "prod_cluster"},
		},
		{
			in:   "my-proj/us-central1/prod",
			want: ClusterID{Project: "my-proj", Location: "us-central1", Cluster: "prod"},
		},
		{
			in:   "google.com:corp-proj/us-central1/prod",
			want: ClusterID{Project: "google.com:corp-proj", Location: "us-central1", Cluster: "prod"},
		},
	}
	for _, tc := range valid {
		t.Run(tc.in, func(t *testing.T) {
			got, ok := ParseClusterRef(tc.in)
			if !ok {
				t.Fatalf("ParseClusterRef(%q) = false, want %+v", tc.in, tc.want)
			}
			if got != tc.want {
				t.Errorf("ParseClusterRef(%q) = %+v, want %+v", tc.in, got, tc.want)
			}
			if got.ResourcePath() != "projects/"+tc.want.Project+"/locations/"+tc.want.Location+"/clusters/"+tc.want.Cluster {
				t.Errorf("ResourcePath() = %q", got.ResourcePath())
			}
			if got.ContextName() != "gke_"+tc.want.Project+"_"+tc.want.Location+"_"+tc.want.Cluster {
				t.Errorf("ContextName() = %q", got.ContextName())
			}
			if got.Triple() != tc.want.Project+"/"+tc.want.Location+"/"+tc.want.Cluster {
				t.Errorf("Triple() = %q", got.Triple())
			}
		})
	}

	invalid := []string{
		"",
		"prod",
		"prod@my-proj",
		"prod@my-proj/us-central1",
		"my-proj/us-central1",
		"projects/my-proj/locations/us-central1",
		"projects/../locations/us-central1/clusters/prod",
		"projects/my-proj/locations/../clusters/prod",
		"projects/my-proj/locations/us-central1/clusters/..",
		"my-proj/us-central1/prod=bad",
		"auto/us-central1/prod",
		"kubeconfig/us-central1/prod",
		"fleet/us-central1/prod",
		"folders/123/prod",
		"organizations/456/prod",
		"projects/-bad/locations/us-central1/clusters/prod",
		"gke_only_two",
		"gke_my-proj__prod",
	}
	for _, in := range invalid {
		t.Run("invalid_"+in, func(t *testing.T) {
			if got, ok := ParseClusterRef(in); ok {
				t.Errorf("ParseClusterRef(%q) = (%+v, true), want false", in, got)
			}
		})
	}
}

func TestGoogleAuthResolutionAndValidation(t *testing.T) {
	t.Setenv(GoogleAuthEnv, "")
	t.Setenv(CoreAgentGoogleAuthEnv, "")
	t.Setenv(MastGoogleAuthEnv, "")

	if IsInjectedGoogleAuth(context.Background()) {
		t.Fatal("default environment unexpectedly enabled injected Google auth")
	}

	// Upstream Substrate env switches enable injected mode automatically.
	t.Setenv(CoreAgentGoogleAuthEnv, "injected")
	if !IsInjectedGoogleAuth(context.Background()) {
		t.Fatal("CORE_AGENT_GOOGLE_AUTH=injected did not enable injected Google auth")
	}

	// Explicit --google-auth=adc overrides upstream *_GOOGLE_AUTH=injected.
	adcCtx := WithGoogleAuth(context.Background(), "adc")
	if IsInjectedGoogleAuth(adcCtx) {
		t.Fatal("WithGoogleAuth(adc) did not override CORE_AGENT_GOOGLE_AUTH=injected")
	}

	// LOOKOUT_GOOGLE_AUTH=adc also overrides CORE_AGENT_GOOGLE_AUTH=injected.
	t.Setenv(GoogleAuthEnv, "adc")
	if IsInjectedGoogleAuth(context.Background()) {
		t.Fatal("LOOKOUT_GOOGLE_AUTH=adc did not override CORE_AGENT_GOOGLE_AUTH=injected")
	}

	// Explicit --google-auth=injected overrides LOOKOUT_GOOGLE_AUTH=adc.
	injCtx := WithGoogleAuth(context.Background(), "injected")
	if !IsInjectedGoogleAuth(injCtx) {
		t.Fatal("WithGoogleAuth(injected) did not override LOOKOUT_GOOGLE_AUTH=adc")
	}

	// MAST_GOOGLE_AUTH=injected works when LOOKOUT_GOOGLE_AUTH is unset/auto.
	t.Setenv(GoogleAuthEnv, "auto")
	t.Setenv(CoreAgentGoogleAuthEnv, "")
	t.Setenv(MastGoogleAuthEnv, "INJECTED")
	if !IsInjectedGoogleAuth(context.Background()) {
		t.Fatal("MAST_GOOGLE_AUTH=INJECTED with LOOKOUT_GOOGLE_AUTH=auto did not enable injected auth")
	}

	// Invalid values fail ValidateGoogleAuth.
	if err := ValidateGoogleAuth("bogus"); err == nil {
		t.Error("ValidateGoogleAuth accepted bogus flag value")
	}
	t.Setenv(GoogleAuthEnv, "bogus")
	if err := ValidateGoogleAuth("auto"); err == nil {
		t.Error("ValidateGoogleAuth accepted bogus LOOKOUT_GOOGLE_AUTH")
	}

	// InjectedTokenSource yields the static placeholder bearer token.
	tok, err := InjectedTokenSource().Token()
	if err != nil {
		t.Fatalf("InjectedTokenSource: %v", err)
	}
	if tok.AccessToken != InjectedBearerToken {
		t.Errorf("AccessToken = %q, want %q", tok.AccessToken, InjectedBearerToken)
	}
}

func TestCredentialProxyTransportAndTokenRotation(t *testing.T) {
	var gotPath, gotQuery, gotAuth, gotHost string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotAuth = r.Header.Get("Authorization")
		gotHost = r.Host
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	tokFile := filepath.Join(t.TempDir(), "proxy-token")
	if err := os.WriteFile(tokFile, []byte("  tok-v1\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv(CredentialProxyURLEnv, srv.URL+"/")
	t.Setenv(CredentialProxyTokenFileEnv, tokFile)

	proxy, err := CredentialProxyFromEnv()
	if err != nil || proxy == nil {
		t.Fatalf("CredentialProxyFromEnv: proxy=%v err=%v", proxy, err)
	}
	if proxy.URL != srv.URL {
		t.Errorf("proxy.URL = %q, want trailing slash trimmed to %q", proxy.URL, srv.URL)
	}

	k8sHost, err := proxy.K8sHost(ClusterID{Project: "proj-a", Location: "us-central1", Cluster: "prod"})
	if err != nil {
		t.Fatalf("K8sHost: %v", err)
	}
	if want := srv.URL + "/v1/k8s/proj-a/us-central1/prod"; k8sHost != want {
		t.Errorf("K8sHost = %q, want %q", k8sHost, want)
	}
	if _, err := proxy.K8sHost(ClusterID{Project: "../etc", Location: "us-central1", Cluster: "prod"}); err == nil {
		t.Error("K8sHost accepted path traversal project")
	}

	client := &http.Client{Transport: proxy.GCPTransport(nil)}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://container.googleapis.com/v1/projects/proj-a/locations/-/clusters?alt=json", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Any caller/SDK Authorization header is replaced by the proxy token.
	req.Header.Set("Authorization", "Bearer stale-upstream-token")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GCPTransport Do: %v", err)
	}
	_ = resp.Body.Close()

	if want := "/v1/gcp/container.googleapis.com/v1/projects/proj-a/locations/-/clusters"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
	if gotQuery != "alt=json" {
		t.Errorf("query = %q, want alt=json", gotQuery)
	}
	if gotAuth != "Bearer tok-v1" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer tok-v1")
	}
	if !strings.HasSuffix(srv.URL, gotHost) {
		t.Errorf("Host = %q, want proxy host from %q", gotHost, srv.URL)
	}

	// Token rotation on disk is picked up on the very next request.
	if err := os.WriteFile(tokFile, []byte("tok-v2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	req2, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://compute.googleapis.com/compute/v1/projects/proj-a/regions", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("second Do: %v", err)
	}
	_ = resp2.Body.Close()
	if gotAuth != "Bearer tok-v2" {
		t.Errorf("rotated Authorization = %q, want %q", gotAuth, "Bearer tok-v2")
	}

	// An empty token file fails loudly instead of sending an empty Bearer header.
	if err := os.WriteFile(tokFile, []byte("   \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	req3, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://compute.googleapis.com/compute/v1/projects/proj-a/regions", nil)
	if _, err := client.Do(req3); err == nil || !strings.Contains(err.Error(), CredentialProxyTokenFileEnv) {
		t.Errorf("empty token file err = %v, want error naming %s", err, CredentialProxyTokenFileEnv)
	}

	// Invalid CREDENTIAL_PROXY_URL is rejected cleanly.
	t.Setenv(CredentialProxyURLEnv, "ftp://bad.example")
	if _, err := CredentialProxyFromEnv(); err == nil {
		t.Error("CredentialProxyFromEnv accepted non-http(s) URL")
	}
}

func TestCredentialProxyRefusesCrossOriginRedirectsAndNonProxyHosts(t *testing.T) {
	var evilHits int
	evilSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		evilHits++
		w.WriteHeader(http.StatusOK)
	}))
	defer evilSrv.Close()

	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, evilSrv.URL+"/steal", http.StatusFound)
	}))
	defer proxySrv.Close()

	tokFile := filepath.Join(t.TempDir(), "proxy-token")
	if err := os.WriteFile(tokFile, []byte("secret-proxy-tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(CredentialProxyURLEnv, proxySrv.URL)
	t.Setenv(CredentialProxyTokenFileEnv, tokFile)

	proxy, err := CredentialProxyFromEnv()
	if err != nil {
		t.Fatal(err)
	}

	// Both CheckRedirect and WrapRoundTripper/GCPTransport refuse cross-origin targets.
	gcpClient := &http.Client{
		Transport:     proxy.GCPTransport(nil),
		CheckRedirect: proxy.CheckRedirect,
	}
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://container.googleapis.com/v1/projects/p/locations/-/clusters", nil)
	if _, err := gcpClient.Do(req); err == nil {
		t.Error("GCPTransport followed cross-origin redirect from proxy")
	}

	k8sClient := &http.Client{Transport: proxy.WrapRoundTripper(nil)}
	directEvilReq, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, evilSrv.URL+"/api/v1/pods", nil)
	if _, err := k8sClient.Do(directEvilReq); err == nil {
		t.Error("WrapRoundTripper sent proxy token to non-proxy host")
	}
	if evilHits != 0 {
		t.Errorf("evilHits = %d, want 0", evilHits)
	}
}
