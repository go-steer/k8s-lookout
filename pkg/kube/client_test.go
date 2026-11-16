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
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
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

func TestBuildConfigProtobufDefaults(t *testing.T) {
	path := writeKubeconfig(t)
	t.Setenv("KUBERNETES_SERVICE_HOST", "")

	cfg, err := BuildConfig(Options{Kubeconfig: path})
	if err != nil {
		t.Fatalf("BuildConfig: %v", err)
	}
	if cfg.AcceptContentTypes != DefaultAcceptContentTypes {
		t.Errorf("AcceptContentTypes = %q, want %q", cfg.AcceptContentTypes, DefaultAcceptContentTypes)
	}
	if cfg.ContentType != DefaultContentType {
		t.Errorf("ContentType = %q, want %q", cfg.ContentType, DefaultContentType)
	}

	// Explicit Target.Config content-type overrides are preserved.
	override := &rest.Config{
		Host: "https://override.example:6443",
		ContentConfig: rest.ContentConfig{
			AcceptContentTypes: "application/json",
			ContentType:        "application/json",
		},
	}
	got, err := BuildConfig(Options{Target: &Target{Name: "override", Config: override}})
	if err != nil {
		t.Fatalf("BuildConfig(Target): %v", err)
	}
	if got.AcceptContentTypes != "application/json" || got.ContentType != "application/json" {
		t.Errorf("Target overrides not preserved: Accept=%q ContentType=%q", got.AcceptContentTypes, got.ContentType)
	}
}

func TestBuildClientFromConfigAppliesProtobufWithoutMutatingCaller(t *testing.T) {
	var gotAccept atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAccept.Store(r.Header.Get("Accept"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"NamespaceList","apiVersion":"v1","items":[]}`))
	}))
	defer srv.Close()

	caller := &rest.Config{Host: srv.URL}
	client, err := BuildClientFromConfig(caller)
	if err != nil {
		t.Fatalf("BuildClientFromConfig: %v", err)
	}
	if caller.AcceptContentTypes != "" || caller.ContentType != "" {
		t.Errorf("BuildClientFromConfig mutated caller config: Accept=%q ContentType=%q", caller.AcceptContentTypes, caller.ContentType)
	}
	if _, err := client.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{}); err != nil {
		t.Fatalf("list namespaces: %v", err)
	}
	if got, _ := gotAccept.Load().(string); got != DefaultAcceptContentTypes {
		t.Errorf("Accept header = %q, want %q", got, DefaultAcceptContentTypes)
	}
}

func TestClientNegotiatesProtobufAndDynamicStaysJSON(t *testing.T) {
	pbSer := protobuf.NewSerializer(scheme.Scheme, scheme.Scheme)

	var (
		gotListAccept  atomic.Value
		gotPostCT      atomic.Value
		gotDynAccept   atomic.Value
		respondAsProto atomic.Bool
	)
	respondAsProto.Store(true)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces":
			gotListAccept.Store(r.Header.Get("Accept"))
			if respondAsProto.Load() {
				var buf bytes.Buffer
				nsList := &corev1.NamespaceList{
					Items: []corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default"}}},
				}
				if err := pbSer.Encode(nsList, &buf); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", DefaultContentType)
				_, _ = w.Write(buf.Bytes())
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"kind":"NamespaceList","apiVersion":"v1","items":[{"metadata":{"name":"json-fallback"}}]}`))

		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/selfsubjectaccessreviews"):
			gotPostCT.Store(r.Header.Get("Content-Type"))
			var buf bytes.Buffer
			resp := &authv1.SelfSubjectAccessReview{Status: authv1.SubjectAccessReviewStatus{Allowed: true}}
			if err := pbSer.Encode(resp, &buf); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", DefaultContentType)
			_, _ = w.Write(buf.Bytes())

		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "computeclasses"):
			gotDynAccept.Store(r.Header.Get("Accept"))
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"apiVersion":"cloud.google.com/v1","kind":"ComputeClassList","items":[]}`))

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	target := &Target{Name: "test", Config: &rest.Config{Host: srv.URL}}
	client, err := BuildClient(Options{Target: target})
	if err != nil {
		t.Fatalf("BuildClient: %v", err)
	}

	// 1. Typed GET negotiates Protobuf and decodes a Protobuf response.
	nsList, err := client.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list namespaces (protobuf): %v", err)
	}
	if len(nsList.Items) != 1 || nsList.Items[0].Name != "default" {
		t.Fatalf("protobuf namespaces = %+v, want [default]", nsList.Items)
	}
	if got, _ := gotListAccept.Load().(string); got != DefaultAcceptContentTypes {
		t.Errorf("typed GET Accept = %q, want %q", got, DefaultAcceptContentTypes)
	}

	// 2. Typed GET transparently falls back to JSON when the server responds with JSON.
	respondAsProto.Store(false)
	nsList, err = client.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list namespaces (json fallback): %v", err)
	}
	if len(nsList.Items) != 1 || nsList.Items[0].Name != "json-fallback" {
		t.Fatalf("json fallback namespaces = %+v, want [json-fallback]", nsList.Items)
	}

	// 3. Typed POST encodes request body as Protobuf.
	ssar, err := client.AuthorizationV1().SelfSubjectAccessReviews().Create(context.Background(), &authv1.SelfSubjectAccessReview{
		Spec: authv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authv1.ResourceAttributes{Verb: "list", Resource: "pods"},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create SSAR: %v", err)
	}
	if !ssar.Status.Allowed {
		t.Errorf("SSAR Allowed = false, want true")
	}
	if got, _ := gotPostCT.Load().(string); got != DefaultContentType {
		t.Errorf("typed POST Content-Type = %q, want %q", got, DefaultContentType)
	}

	// 4. Dynamic client keeps JSON for CRDs even when built from the same Target/BuildConfig.
	dyn, err := BuildDynamicClient(Options{Target: target})
	if err != nil {
		t.Fatalf("BuildDynamicClient: %v", err)
	}
	gvr := schema.GroupVersionResource{Group: "cloud.google.com", Version: "v1", Resource: "computeclasses"}
	if _, err := dyn.Resource(gvr).List(context.Background(), metav1.ListOptions{}); err != nil {
		t.Fatalf("dynamic list: %v", err)
	}
	if got, _ := gotDynAccept.Load().(string); !strings.Contains(got, "application/json") || strings.Contains(got, "protobuf") {
		t.Errorf("dynamic GET Accept = %q, want JSON without protobuf", got)
	}
}
