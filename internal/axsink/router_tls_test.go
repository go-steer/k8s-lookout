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

package axsink

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// lookoutSPIFFEID is what the podidentity signer would put in the
// sentinel's certificate, and what the stand-in router allows.
const lookoutSPIFFEID = "spiffe://cluster.local/ns/lookout-ax/sa/lookout-ax"

// testCA is a minimal in-memory authority for the two Substrate signers.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

var serial atomic.Int64

func newTestCA(t *testing.T, name string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial.Add(1)),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, key: key}
}

func (c *testCA) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(c.cert)
	return p
}

type leafOptions struct {
	uri        string // a SPIFFE ID, as the podidentity signer issues
	dns        string // a Service name, as the servicedns signer issues
	serverAuth bool   // serverAuth EKU; otherwise clientAuth
}

// issue mints a leaf whose chain is leaf, then this CA.
func (c *testCA) issue(t *testing.T, o leafOptions) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial.Add(1)),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	if o.serverAuth {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	if o.uri != "" {
		u, err := url.Parse(o.uri)
		if err != nil {
			t.Fatal(err)
		}
		tmpl.URIs = []*url.URL{u}
	}
	if o.dns != "" {
		tmpl.DNSNames = []string{o.dns}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der, c.cert.Raw}, PrivateKey: key, Leaf: leaf}
}

// mtlsRouter stands in for Substrate v1.0's router ingress: TLS only, a
// client certificate required and verified against the podidentity CA, its
// URI SAN matched exactly against an allowlist, and a serving certificate
// from the servicedns CA naming atenet-router.ate-system.svc and nothing
// else. Keep-alives are off so every request is a fresh handshake, which is
// what makes rotation observable.
type mtlsRouter struct {
	srv     *httptest.Server
	serving atomic.Pointer[tls.Certificate]

	mu      sync.Mutex
	serials []string // client leaf serial per request, in order
	denied  int      // requests from a certificate not on the allowlist
}

func newMTLSRouter(t *testing.T, podCA *testCA, serving tls.Certificate, inner http.Handler) *mtlsRouter {
	t.Helper()
	r := &mtlsRouter{}
	r.serving.Store(&serving)
	r.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.TLS == nil || len(req.TLS.PeerCertificates) == 0 {
			http.Error(w, "client certificate required", http.StatusForbidden)
			return
		}
		leaf := req.TLS.PeerCertificates[0]
		allowed := len(leaf.URIs) == 1 && leaf.URIs[0].String() == lookoutSPIFFEID
		r.mu.Lock()
		r.serials = append(r.serials, leaf.SerialNumber.String())
		if !allowed {
			r.denied++
		}
		r.mu.Unlock()
		if !allowed {
			http.Error(w, "SPIFFE ID not allowed", http.StatusForbidden)
			return
		}
		inner.ServeHTTP(w, req)
	}))
	r.srv.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12,
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  podCA.pool(),
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return r.serving.Load(), nil
		},
	}
	r.srv.Config.SetKeepAlivesEnabled(false)
	r.srv.StartTLS()
	t.Cleanup(r.srv.Close)
	return r
}

func (r *mtlsRouter) lastSerial() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.serials) == 0 {
		return ""
	}
	return r.serials[len(r.serials)-1]
}

// routerPKI is the two Substrate authorities and the files a pod would
// have mounted from them.
type routerPKI struct {
	podCA, dnsCA *testCA
	bundle       string // podCertificate credentialBundlePath file
	trust        string // clusterTrustBundle projection file
}

func newRouterPKI(t *testing.T) *routerPKI {
	t.Helper()
	dir := t.TempDir()
	p := &routerPKI{
		podCA:  newTestCA(t, "podidentity.podcert.ate.dev"),
		dnsCA:  newTestCA(t, "servicedns.podcert.ate.dev"),
		bundle: filepath.Join(dir, "podidentity", "credential-bundle.pem"),
		trust:  filepath.Join(dir, "servicedns-ca", "trust-bundle.pem"),
	}
	p.issueClient(t, lookoutSPIFFEID)
	writeTrustBundle(t, p.trust, p.dnsCA.cert)
	return p
}

// issueClient writes a fresh client certificate for spiffeID into the
// credential bundle, as the kubelet does on rotation, and returns its
// serial.
func (p *routerPKI) issueClient(t *testing.T, spiffeID string) string {
	t.Helper()
	cert := p.podCA.issue(t, leafOptions{uri: spiffeID})
	writeCredentialBundle(t, p.bundle, cert)
	return cert.Leaf.SerialNumber.String()
}

// routerCert is a servicedns certificate for a Service named dnsName.
func routerCert(t *testing.T, ca *testCA, dnsName string) tls.Certificate {
	t.Helper()
	return ca.issue(t, leafOptions{dns: dnsName, serverAuth: true})
}

// writeCredentialBundle writes cert in the podCertificate
// credentialBundlePath layout — one PKCS#8 PRIVATE KEY block, then the
// chain leaf first — replacing the file atomically as the kubelet's
// atomic writer does.
func writeCredentialBundle(t *testing.T, path string, cert tls.Certificate) {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	out := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	for _, c := range cert.Certificate {
		out = append(out, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c})...)
	}
	writeAtomic(t, path, out)
}

func writeTrustBundle(t *testing.T, path string, cas ...*x509.Certificate) {
	t.Helper()
	var out []byte
	for _, c := range cas {
		out = append(out, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw})...)
	}
	writeAtomic(t, path, out)
}

func writeAtomic(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

func newTestRouterClient(t *testing.T, p *routerPKI, serverName string) *http.Client {
	t.Helper()
	c, closeFn, err := NewRouterHTTPClient(RouterTLS{
		CredentialBundlePath: p.bundle,
		TrustBundlePath:      p.trust,
		ServerName:           serverName,
		ReloadInterval:       10 * time.Millisecond,
		OnReloadError:        func(err error) { t.Logf("reload: %v", err) },
	})
	if err != nil {
		t.Fatalf("NewRouterHTTPClient: %v", err)
	}
	t.Cleanup(func() { _ = closeFn() })
	return c
}

func get(c *http.Client, url string) (int, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url+"/readyz", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("ate-target-actor", "triage/t")
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

var okHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

// The whole sink — readiness wait, open, inject, append — works through a
// router that accepts only mTLS, dialed by IP with the router's .svc name as
// ServerName, presenting the pod certificate from the credential bundle.
func TestRouterMTLS_SinkOpensAndAppendsThroughTheRouter(t *testing.T) {
	p := newRouterPKI(t)
	fake := &fakeRouter{notReady: 1}
	router := newMTLSRouter(t, p.podCA, routerCert(t, p.dnsCA, DefaultRouterServerName), fake)

	client, ax := newFakeAXClient(t)
	sink, err := New(Config{
		Client:       client,
		Template:     testTemplate(),
		RouterURL:    router.srv.URL, // https://127.0.0.1:<port>
		BearerToken:  "tok",
		HTTPClient:   newTestRouterClient(t, p, DefaultRouterServerName),
		StartTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id, err := sink.OpenIncidentKeyed(ctx, crashKey, inject.Payload{Kind: "Pod", Reason: "CrashLoopBackOff", UID: "u1"})
	if err != nil {
		t.Fatalf("OpenIncidentKeyed over mTLS: %v", err)
	}
	if err := sink.Append(ctx, id, inject.ResolvedPayload{Kind: "resolved"}); err != nil {
		t.Fatalf("Append over mTLS: %v", err)
	}
	if len(ax.resumed) != 1 {
		t.Errorf("resumed = %v, want one ResumeTask", ax.resumed)
	}
	task := sink.taskFor(crashKey)
	if got := fake.injects["triage/"+task+" /sessions/attach-1/inject"]; len(got) != 2 {
		t.Fatalf("open + append not delivered through the mTLS router: %v", fake.injects)
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	if router.denied != 0 || len(router.serials) < 4 { // 2 readiness probes, POST /sessions, 2 injects
		t.Errorf("router saw %d requests, %d denied; want >= 4 and 0", len(router.serials), router.denied)
	}
}

// The stand-in is a meaningful router: one-way TLS that trusts the router
// but presents no client certificate is refused, and so is a pod
// certificate whose SPIFFE ID is not on the allowlist.
func TestRouterMTLS_StandInEnforcesClientAuth(t *testing.T) {
	p := newRouterPKI(t)
	router := newMTLSRouter(t, p.podCA, routerCert(t, p.dnsCA, DefaultRouterServerName), okHandler)

	noCert := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    p.dnsCA.pool(),
		ServerName: DefaultRouterServerName,
	}}}
	if code, err := get(noCert, router.srv.URL); err == nil {
		t.Errorf("a client without a certificate got %d, want a failed handshake", code)
	}

	p.issueClient(t, "spiffe://cluster.local/ns/default/sa/default")
	c := newTestRouterClient(t, p, DefaultRouterServerName)
	if code, err := get(c, router.srv.URL); err != nil || code != http.StatusForbidden {
		t.Errorf("unlisted SPIFFE ID: code=%d err=%v, want 403", code, err)
	}
}

// The kubelet rewrites the credential bundle as the pod certificate rotates
// (24h lifetime, refreshed 30 min before expiry). The client presents the
// new certificate on the next handshake, with no restart.
func TestRouterMTLS_ClientCertificateRotates(t *testing.T) {
	p := newRouterPKI(t)
	router := newMTLSRouter(t, p.podCA, routerCert(t, p.dnsCA, DefaultRouterServerName), okHandler)
	c := newTestRouterClient(t, p, DefaultRouterServerName)

	if code, err := get(c, router.srv.URL); err != nil || code != http.StatusOK {
		t.Fatalf("first request: code=%d err=%v", code, err)
	}
	first := router.lastSerial()

	rotated := p.issueClient(t, lookoutSPIFFEID)
	if rotated == first {
		t.Fatal("test bug: the rotated certificate has the same serial")
	}
	eventually(t, "the rotated client certificate", func() bool {
		code, err := get(c, router.srv.URL)
		return err == nil && code == http.StatusOK && router.lastSerial() == rotated
	})
}

// The servicedns CA rotates by canarying a new bundle. Until the projected
// trust bundle carries the new CA, a router serving from it is refused;
// once the file is rewritten, the same client trusts it, with no restart.
func TestRouterMTLS_TrustBundleRotates(t *testing.T) {
	p := newRouterPKI(t)
	router := newMTLSRouter(t, p.podCA, routerCert(t, p.dnsCA, DefaultRouterServerName), okHandler)
	c := newTestRouterClient(t, p, DefaultRouterServerName)
	if code, err := get(c, router.srv.URL); err != nil || code != http.StatusOK {
		t.Fatalf("before rotation: code=%d err=%v", code, err)
	}

	nextCA := newTestCA(t, "servicedns.podcert.ate.dev next")
	next := routerCert(t, nextCA, DefaultRouterServerName)
	router.serving.Store(&next)
	if _, err := get(c, router.srv.URL); err == nil || !strings.Contains(err.Error(), "unknown authority") {
		t.Fatalf("a router certificate from a CA not yet in the trust bundle: err=%v, want unknown authority", err)
	}

	writeTrustBundle(t, p.trust, p.dnsCA.cert, nextCA.cert) // the canary: both live
	eventually(t, "the rotated trust bundle", func() bool {
		code, err := get(c, router.srv.URL)
		return err == nil && code == http.StatusOK
	})
}

// The servicedns CA issues to every Service in the cluster, so chaining to
// it proves little; the server name is what identifies the router.
func TestRouterMTLS_ServerNameIdentifiesTheRouter(t *testing.T) {
	p := newRouterPKI(t)

	t.Run("another Service's certificate from the same CA is refused", func(t *testing.T) {
		router := newMTLSRouter(t, p.podCA, routerCert(t, p.dnsCA, "impostor.default.svc"), okHandler)
		_, err := get(newTestRouterClient(t, p, DefaultRouterServerName), router.srv.URL)
		if err == nil || !strings.Contains(err.Error(), "valid for impostor.default.svc, not "+DefaultRouterServerName) {
			t.Errorf("a certificate for impostor.default.svc: err=%v, want a name mismatch", err)
		}
	})

	router := newMTLSRouter(t, p.podCA, routerCert(t, p.dnsCA, DefaultRouterServerName), okHandler)
	t.Run("the cluster.local name is not on the certificate", func(t *testing.T) {
		_, err := get(newTestRouterClient(t, p, "atenet-router.ate-system.svc.cluster.local"), router.srv.URL)
		if err == nil || !strings.Contains(err.Error(), "not atenet-router.ate-system.svc.cluster.local") {
			t.Errorf("verifying as atenet-router.ate-system.svc.cluster.local: err=%v, want a name mismatch", err)
		}
	})
	t.Run("no server name while dialing an IP is refused", func(t *testing.T) {
		if _, err := get(newTestRouterClient(t, p, ""), router.srv.URL); err == nil || !strings.Contains(err.Error(), "no server name") {
			t.Errorf("a router dialed by IP with no server name: err=%v, want a refusal", err)
		}
	})
}

// A router the sink can never reach (here: a client verifying the wrong
// name) fails the open with the TLS reason, not a bare deadline.
func TestRouterMTLS_ReadinessTimeoutSaysWhy(t *testing.T) {
	p := newRouterPKI(t)
	router := newMTLSRouter(t, p.podCA, routerCert(t, p.dnsCA, DefaultRouterServerName), &fakeRouter{})
	client, _ := newFakeAXClient(t)
	sink, err := New(Config{
		Client:       client,
		Template:     testTemplate(),
		RouterURL:    router.srv.URL,
		BearerToken:  "tok",
		HTTPClient:   newTestRouterClient(t, p, "atenet-router.ate-system.svc.cluster.local"),
		StartTimeout: 1500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = sink.OpenIncidentKeyed(context.Background(), crashKey, inject.Payload{Kind: "Pod", Reason: "CrashLoopBackOff", UID: "u1"})
	if err == nil || !strings.Contains(err.Error(), "not ready") || !strings.Contains(err.Error(), "not atenet-router.ate-system.svc.cluster.local") {
		t.Fatalf("open against an unverifiable router = %v, want a readiness timeout naming the certificate mismatch", err)
	}
}

// A missing mount or an unusable file fails NewRouterHTTPClient, naming the
// file, so lookout watch exits at startup instead of failing every incident.
func TestNewRouterHTTPClient_FailsAtStartupNamingTheFile(t *testing.T) {
	p := newRouterPKI(t)
	dir := t.TempDir()
	missing := filepath.Join(dir, "absent.pem")
	certOnly := filepath.Join(dir, "cert-only.pem")
	writeTrustBundle(t, certOnly, p.podCA.cert) // a chain with no key

	cases := []struct {
		name          string
		bundle, trust string
		want          string
	}{
		{"no bundle path", "", p.trust, "credential bundle"},
		{"no trust path", p.bundle, "", "trust bundle"},
		{"bundle not mounted", missing, p.trust, missing},
		{"trust bundle not mounted", p.bundle, missing, missing},
		{"bundle without a key", certOnly, p.trust, "router client certificate"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hc, closeFn, err := NewRouterHTTPClient(RouterTLS{CredentialBundlePath: c.bundle, TrustBundlePath: c.trust, ServerName: DefaultRouterServerName})
			if err == nil {
				_ = closeFn()
				t.Fatalf("NewRouterHTTPClient = %v, want an error", hc)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not mention %q", err, c.want)
			}
		})
	}
}
