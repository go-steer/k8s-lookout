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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// twoContexts is a kubeconfig naming two clusters, with the SECOND as
// current-context, so a test that asks for the first proves the
// override was honored rather than that the default happened to match.
const twoContexts = `
apiVersion: v1
kind: Config
current-context: staging
clusters:
- name: prod
  cluster:
    server: https://prod.example:6443
- name: staging
  cluster:
    server: https://staging.example:6443
contexts:
- name: prod
  context:
    cluster: prod
    user: prod
- name: staging
  context:
    cluster: staging
    user: staging
users:
- name: prod
  user:
    token: prod-token
- name: staging
  user:
    token: staging-token
`

func writeKubeconfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, []byte(twoContexts), 0o600); err != nil {
		t.Fatalf("writing kubeconfig: %v", err)
	}
	return path
}

func TestBuildConfigContextSelectsACluster(t *testing.T) {
	path := writeKubeconfig(t)
	// KUBERNETES_SERVICE_HOST leaks in when the suite runs inside a
	// pod; an explicit --kubeconfig outranks it, but the default case
	// below depends on it being absent.
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBECONFIG", path)

	cases := []struct {
		name       string
		opts       Options
		wantServer string
	}{
		{"explicit kubeconfig, current-context", Options{Kubeconfig: path}, "https://staging.example:6443"},
		{"explicit kubeconfig, overridden", Options{Kubeconfig: path, Context: "prod"}, "https://prod.example:6443"},
		{"default search, overridden", Options{Context: "prod"}, "https://prod.example:6443"},
		{"default search, current-context", Options{}, "https://staging.example:6443"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := BuildConfig(tc.opts)
			if err != nil {
				t.Fatalf("BuildConfig(%+v): %v", tc.opts, err)
			}
			if cfg.Host != tc.wantServer {
				t.Errorf("server = %q, want %q", cfg.Host, tc.wantServer)
			}
		})
	}
}

func TestBuildConfigUnknownContextIsAnError(t *testing.T) {
	path := writeKubeconfig(t)
	t.Setenv("KUBERNETES_SERVICE_HOST", "")

	_, err := BuildConfig(Options{Kubeconfig: path, Context: "nope"})
	if err == nil {
		t.Fatal("BuildConfig with an absent context returned no error")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("error %q does not name the context that was not found", err)
	}
}

// A context is meaningless against a pod's service account, so the
// flag is refused rather than ignored: silently reading a different
// cluster than the one named is the failure mode worth a hard stop.
func TestBuildConfigContextRejectedInCluster(t *testing.T) {
	_, err := BuildConfig(Options{InCluster: true, Context: "prod"})
	if err == nil {
		t.Fatal("BuildConfig(--in-cluster --context) returned no error")
	}
	if !strings.Contains(err.Error(), "in-cluster") {
		t.Errorf("error %q does not explain why the context was refused", err)
	}
}
