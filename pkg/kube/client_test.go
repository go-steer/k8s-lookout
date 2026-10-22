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
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TestBuildConfigRateLimits(t *testing.T) {
	path := writeKubeconfig(t)
	t.Setenv("KUBERNETES_SERVICE_HOST", "")

	def, err := BuildConfig(Options{Kubeconfig: path})
	if err != nil {
		t.Fatalf("BuildConfig defaults: %v", err)
	}
	if def.QPS != DefaultQPS || def.Burst != DefaultBurst {
		t.Errorf("default QPS/Burst = %v/%d, want %v/%d", def.QPS, def.Burst, DefaultQPS, DefaultBurst)
	}

	custom, err := BuildConfig(Options{Kubeconfig: path, QPS: 80, Burst: 160})
	if err != nil {
		t.Fatalf("BuildConfig custom: %v", err)
	}
	if custom.QPS != 80 || custom.Burst != 160 {
		t.Errorf("custom QPS/Burst = %v/%d, want 80/160", custom.QPS, custom.Burst)
	}
}

func TestUnreachableEndpointFailsFast(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")

	// Bind and immediately close a local listener to obtain a port that
	// deterministically refuses connections.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	kc := fmt.Sprintf(`apiVersion: v1
kind: Config
current-context: dead
clusters:
- name: dead
  cluster:
    server: https://%s
    insecure-skip-tls-verify: true
contexts:
- name: dead
  context:
    cluster: dead
    user: u
users:
- name: u
  user:
    token: test-token
`, addr)
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, []byte(kc), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}

	client, err := BuildClient(Options{
		Kubeconfig:  path,
		DialTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BuildClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	for i := range 5 {
		_, err = client.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})
		if err == nil {
			t.Fatalf("call %d succeeded against closed endpoint %s", i, addr)
		}
		if ctx.Err() != nil {
			t.Fatalf("call %d burned context deadline instead of failing fast: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	if elapsed >= 500*time.Millisecond {
		t.Errorf("5 sequential calls to unreachable endpoint took %s, want < 500ms", elapsed)
	}
}
