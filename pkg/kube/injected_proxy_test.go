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

package kube

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

func TestBuildConfigDoesNotAutoSelectInClusterUnderInjectedOrProxy(t *testing.T) {
	// Simulate running inside a pod/task where KUBERNETES_SERVICE_HOST is set,
	// with no kubeconfig file on disk.
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.96.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "missing-kubeconfig"))

	for _, tc := range []struct {
		name string
		env  map[string]string
	}{
		{
			name: "CORE_AGENT_GOOGLE_AUTH=injected",
			env:  map[string]string{cloud.CoreAgentGoogleAuthEnv: "injected", cloud.CredentialProxyURLEnv: ""},
		},
		{
			name: "CREDENTIAL_PROXY_URL",
			env:  map[string]string{cloud.CoreAgentGoogleAuthEnv: "", cloud.CredentialProxyURLEnv: "http://127.0.0.1:8080"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			opts := OptionsFrom(t.Context())
			if !opts.NoAutoInCluster {
				t.Fatal("OptionsFrom did not set NoAutoInCluster")
			}
			_, err := BuildConfig(opts)
			if err == nil {
				t.Fatal("BuildConfig succeeded without a kubeconfig; expected it NOT to fall back to InClusterConfig")
			}
			if strings.Contains(err.Error(), "in-cluster") {
				t.Errorf("BuildConfig attempted InClusterConfig (%v), want kubeconfig error", err)
			}
		})
	}
}

func TestContextNormalizationMatchesGKEFormsWithoutCrossProjectCollision(t *testing.T) {
	const gkeKubeconfig = `apiVersion: v1
kind: Config
current-context: gke_proj-a_us-central1_prod
clusters:
- name: gke_proj-a_us-central1_prod
  cluster:
    server: https://prod-a.example:6443
- name: gke_proj-b_us-east1_stage
  cluster:
    server: https://stage-b.example:6443
contexts:
- name: gke_proj-a_us-central1_prod
  context:
    cluster: gke_proj-a_us-central1_prod
    user: u
- name: friendly-stage
  context:
    cluster: gke_proj-b_us-east1_stage
    user: u
users:
- name: u
  user:
    token: tok
`
	kcPath := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(kcPath, []byte(gkeKubeconfig), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBERNETES_SERVICE_HOST", "")

	for _, ref := range []string{
		"gke_proj-a_us-central1_prod",
		"projects/proj-a/locations/us-central1/clusters/prod",
		"proj-a/us-central1/prod",
		"prod",
	} {
		cfg, err := BuildConfig(Options{Kubeconfig: kcPath, Context: ref})
		if err != nil {
			t.Fatalf("BuildConfig(%q): %v", ref, err)
		}
		if cfg.Host != "https://prod-a.example:6443" {
			t.Errorf("BuildConfig(%q) host = %q, want prod-a", ref, cfg.Host)
		}
	}

	// Renamed context whose cluster entry carries gke_<p>_<l>_<c> also resolves.
	for _, ref := range []string{
		"friendly-stage",
		"projects/proj-b/locations/us-east1/clusters/stage",
		"proj-b/us-east1/stage",
		"gke_proj-b_us-east1_stage",
		"stage",
	} {
		cfg, err := BuildConfig(Options{Kubeconfig: kcPath, Context: ref})
		if err != nil {
			t.Fatalf("BuildConfig(%q): %v", ref, err)
		}
		if cfg.Host != "https://stage-b.example:6443" {
			t.Errorf("BuildConfig(%q) host = %q, want stage-b", ref, cfg.Host)
		}
	}

	// Asking for the same cluster name in a DIFFERENT project must NOT match proj-a's context.
	if _, err := BuildConfig(Options{Kubeconfig: kcPath, Context: "projects/wrong-proj/locations/us-central1/clusters/prod"}); err == nil {
		t.Error("BuildConfig matched wrong-proj against gke_proj-a_us-central1_prod")
	}
}

func TestResolveContextSelectionAndFleetTargetsViaCredentialProxy(t *testing.T) {
	var gotPath, gotAuth string
	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"major":"1","minor":"31","gitVersion":"v1.31.2"}`)
	}))
	defer proxySrv.Close()

	tokFile := filepath.Join(t.TempDir(), "proxy-tok")
	if err := os.WriteFile(tokFile, []byte("proxy-tok-xyz\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(cloud.CredentialProxyURLEnv, proxySrv.URL)
	t.Setenv(cloud.CredentialProxyTokenFileEnv, tokFile)
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "missing-kubeconfig"))
	t.Setenv("KUBERNETES_SERVICE_HOST", "")

	// Single-cluster --context=<GKE resource path> without any kubeconfig file.
	sel, usedDynamic, err := ResolveContextSelection(t.Context(), "", "projects/proj-a/locations/us-central1/clusters/prod")
	if err != nil {
		t.Fatalf("ResolveContextSelection: %v", err)
	}
	if !usedDynamic {
		t.Error("usedDynamic = false, want true")
	}
	if sel.Target == nil || sel.Target.Project != "proj-a" || sel.Target.Location != "us-central1" || sel.Target.Name != "prod" {
		t.Fatalf("sel.Target = %+v", sel.Target)
	}

	client, err := BuildClient(OptionsFrom(WithSelection(t.Context(), sel)))
	if err != nil {
		t.Fatalf("BuildClient: %v", err)
	}
	ver, err := client.Discovery().ServerVersion()
	if err != nil {
		t.Fatalf("ServerVersion via proxy: %v", err)
	}
	if ver.GitVersion != "v1.31.2" {
		t.Errorf("GitVersion = %q, want v1.31.2", ver.GitVersion)
	}
	if gotPath != "/v1/k8s/proj-a/us-central1/prod/version" {
		t.Errorf("proxy path = %q, want /v1/k8s/proj-a/us-central1/prod/version", gotPath)
	}
	if gotAuth != "Bearer proxy-tok-xyz" {
		t.Errorf("proxy Authorization = %q, want Bearer proxy-tok-xyz", gotAuth)
	}

	// Fan-out --contexts with GKE paths also resolves kubeconfig-free.
	targets, err := ResolveFleetTargetsContext(t.Context(), "", "", "projects/proj-a/locations/us-central1/clusters/prod,proj-b/us-east1/prod")
	if err != nil {
		t.Fatalf("ResolveFleetTargetsContext: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("targets = %+v, want 2", targets)
	}
	// Duplicate bare name "prod" across two projects disambiguates labels cleanly.
	if targets[0].Label == targets[1].Label {
		t.Errorf("duplicate target labels were not disambiguated: %q vs %q", targets[0].Label, targets[1].Label)
	}
}
