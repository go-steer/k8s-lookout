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
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"golang.org/x/oauth2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

const callerTok = "ya29.caller-token"

func callerSource() oauth2.TokenSource {
	return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: callerTok})
}

// TestCallerConfigKeepsNoLookoutCredential: every way a resolved config
// can carry lookout's own identity is gone, and only the caller's bearer
// token remains. The cluster address, CA and rate limits are kept.
func TestCallerConfigKeepsNoLookoutCredential(t *testing.T) {
	t.Parallel()
	cfg := &rest.Config{
		Host:            "https://10.0.0.1",
		BearerToken:     "lookout-sa-token",
		BearerTokenFile: "/var/run/secrets/kubernetes.io/serviceaccount/token",
		Username:        "admin",
		Password:        "hunter2",
		AuthProvider:    &clientcmdapi.AuthProviderConfig{Name: "gcp"},
		ExecProvider:    &clientcmdapi.ExecConfig{Command: "gke-gcloud-auth-plugin"},
		Impersonate: rest.ImpersonationConfig{UserName: "system:admin", UID: "1", Groups: []string{"system:masters"},
			Extra: map[string][]string{"k": {"v"}}},
		WrapTransport: func(rt http.RoundTripper) http.RoundTripper { return rt },
		QPS:           50,
		Burst:         100,
		TLSClientConfig: rest.TLSClientConfig{
			CAData:   []byte("ca"),
			CertFile: "/client.crt", KeyFile: "/client.key",
			CertData: []byte("cert"), KeyData: []byte("key"),
		},
	}
	out, err := callerConfig(cfg, callerSource())
	if err != nil {
		t.Fatal(err)
	}
	if out.BearerToken != callerTok {
		t.Errorf("BearerToken = %q, want the caller's", out.BearerToken)
	}
	if out.BearerTokenFile != "" || out.Username != "" || out.Password != "" || out.AuthProvider != nil ||
		out.ExecProvider != nil || !reflect.DeepEqual(out.Impersonate, rest.ImpersonationConfig{}) ||
		out.WrapTransport != nil || out.Transport != nil || out.CertFile != "" || out.KeyFile != "" ||
		out.CertData != nil || out.KeyData != nil {
		t.Errorf("a lookout credential survived: %+v", out)
	}
	if out.Host != "https://10.0.0.1" || string(out.CAData) != "ca" || out.QPS != 50 || out.Burst != 100 {
		t.Errorf("the cluster changed: %+v", out)
	}
}

// TestCallerTokenOnlyOverVerifiedTLS: the caller's token is a
// cloud-platform Google token, so it is never sent over plaintext or to a
// server whose certificate is not verified.
func TestCallerTokenOnlyOverVerifiedTLS(t *testing.T) {
	t.Parallel()
	for _, cfg := range []*rest.Config{
		{Host: "http://10.0.0.1"},
		{Host: "10.0.0.1:443"},
		{Host: "https://10.0.0.1", TLSClientConfig: rest.TLSClientConfig{Insecure: true}},
	} {
		if _, err := callerConfig(cfg, callerSource()); err == nil {
			t.Errorf("sent the caller's token to %+v", cfg)
		}
	}
}

type failingSource struct{}

func (failingSource) Token() (*oauth2.Token, error) { return nil, errors.New("expired") }

// TestCallerTokenErrorIsNotAFallback: a caller token that cannot produce a
// token fails the config, rather than leaving lookout's credential in
// place.
func TestCallerTokenErrorIsNotAFallback(t *testing.T) {
	t.Parallel()
	for _, ts := range []oauth2.TokenSource{failingSource{}, oauth2.StaticTokenSource(&oauth2.Token{})} {
		cfg := &rest.Config{Host: "https://10.0.0.1", BearerToken: "lookout-sa-token"}
		if out, err := callerConfig(cfg, ts); err == nil {
			t.Errorf("%T: no error, config %+v", ts, out)
		}
	}
	kc := writeTokenKubeconfig(t, "https://127.0.0.1:1", "lookout-user-token", nil)
	if _, err := BuildConfig(Options{Kubeconfig: kc, CallerToken: failingSource{}}); err == nil {
		t.Error("BuildConfig with a failing caller token succeeded")
	}
}

func TestOptionsFromCarriesCallerToken(t *testing.T) {
	t.Parallel()
	if o := OptionsFrom(context.Background()); o.CallerToken != nil {
		t.Error("a bare context carried a caller token")
	}
	if ctx := WithCallerToken(context.Background(), nil); ctx != context.Background() {
		t.Error("WithCallerToken(nil) changed the context")
	}
	ctx := WithSelection(WithCallerToken(context.Background(), callerSource()), Selection{Context: "c"})
	o := OptionsFrom(ctx)
	if o.CallerToken == nil || o.Context != "c" {
		t.Errorf("OptionsFrom = %+v, want the selection and the caller token", o)
	}
}

// TestClientSendsCallerToken is the end-to-end property: a client built
// the way every check builds one (BuildClient(OptionsFrom(ctx))) sends the
// caller's token to the API server, and never the kubeconfig user's.
func TestClientSendsCallerToken(t *testing.T) {
	t.Parallel()
	var got atomic.Value
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"NamespaceList","apiVersion":"v1","items":[]}`))
	}))
	defer srv.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	kc := writeTokenKubeconfig(t, srv.URL, "lookout-user-token", ca)

	ctx := WithSelection(WithCallerToken(context.Background(), callerSource()), Selection{Kubeconfig: kc})
	client, err := BuildClient(OptionsFrom(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{}); err != nil {
		t.Fatal(err)
	}
	if h, _ := got.Load().(string); h != "Bearer "+callerTok {
		t.Errorf("API server saw Authorization %q, want the caller's token", h)
	}

	// Without a caller token the same kubeconfig authenticates as its own
	// user, as before.
	client, err = BuildClient(OptionsFrom(WithSelection(context.Background(), Selection{Kubeconfig: kc})))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{}); err != nil {
		t.Fatal(err)
	}
	if h, _ := got.Load().(string); h != "Bearer lookout-user-token" {
		t.Errorf("without a caller token: Authorization %q, want the kubeconfig user's", h)
	}
}

func writeTokenKubeconfig(t *testing.T, server, token string, ca []byte) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("apiVersion: v1\nkind: Config\ncurrent-context: c\nclusters:\n- name: c\n  cluster:\n    server: " + server + "\n")
	if ca != nil {
		caPath := filepath.Join(t.TempDir(), "ca.pem")
		if err := os.WriteFile(caPath, ca, 0o600); err != nil {
			t.Fatal(err)
		}
		b.WriteString("    certificate-authority: " + caPath + "\n")
	}
	b.WriteString("contexts:\n- name: c\n  context:\n    cluster: c\n    user: u\nusers:\n- name: u\n  user:\n    token: " + token + "\n")
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestCallerFiltered: only a person's caller token filters store rows; a
// workload's (an impersonated service account, #701) does not, and no
// caller token filters nothing.
func TestCallerFiltered(t *testing.T) {
	ctx := context.Background()
	if CallerFiltered(ctx) {
		t.Error("no caller token, yet filtered")
	}
	if !CallerFiltered(WithCallerToken(ctx, callerSource())) {
		t.Error("a person's token is not filtered")
	}
	if CallerFiltered(WithWorkloadCaller(WithCallerToken(ctx, callerSource()))) {
		t.Error("a workload's service-account token is filtered")
	}
	if _, ok := CallerTokenFrom(WithWorkloadCaller(WithCallerToken(ctx, callerSource()))); !ok {
		t.Error("marking the caller a workload dropped its token")
	}
}
