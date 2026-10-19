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

package mcpserver

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/oauth2"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// testCA is an in-test certificate authority: nothing here touches a real
// PKI, a network, or a cloud.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

var serial int64 = 100

func nextSerial() *big.Int { serial++; return big.NewInt(serial) }

func newTestCA(t *testing.T, cn string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          nextSerial(),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue signs a leaf: a server leaf for 127.0.0.1 when uri is "", a
// client leaf whose single URI SAN is uri otherwise (an X509-SVID when uri
// is a SPIFFE ID). It returns the pair and its PEM encodings.
func (ca *testCA) issue(t *testing.T, uri string, server bool) (tls.Certificate, []byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: nextSerial(),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	if server {
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	if uri != "" {
		u, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		tmpl.URIs = []*url.URL{u}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return pair, certPEM, keyPEM
}

func writeFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const (
	idReader       = "spiffe://example.test/ns/agents/sa/reader"
	idWriter       = "spiffe://example.test/ns/agents/sa/writer"
	idImpersonated = "spiffe://example.test/ns/agents/sa/impersonated"
	idBroken       = "spiffe://example.test/ns/agents/sa/broken"
	idStranger     = "spiffe://example.test/ns/other/sa/stranger"

	saReader = "reader@example-project.iam.gserviceaccount.com"
	saBroken = "broken@example-project.iam.gserviceaccount.com"

	// saSentinelToken is what the fake impersonator mints. It must never
	// appear in a response or the access log.
	saSentinelToken = "ya29.sentinel-impersonated-token-0123456789"
)

const workloadRulesYAML = `
rules:
  - name: reader
    identities: [` + idReader + `]
    tools: [k8s_whoami]
  - name: reader-names-the-write-tool
    identities: [` + idReader + `]
    tools: [k8s_write]
  - name: writer
    identities: [` + idWriter + `]
    tools: [k8s_whoami, k8s_write]
    write: true
  - name: impersonated
    identities: [` + idImpersonated + `]
    tools: [k8s_whoami]
    service_account: ` + saReader + `
  - name: broken
    identities: [` + idBroken + `]
    tools: [k8s_whoami]
    service_account: ` + saBroken + `
`

// workloadRegistry serves k8s_whoami, which reports who its invocation
// runs as (the caller token as a fingerprint, the recorded caller name,
// and whether its store rows would be filtered), k8s_write, which changes
// state, and k8s_other, which no rule grants.
func workloadRegistry() *checks.Registry {
	reg := checks.NewRegistry()
	report := func(ctx context.Context, inv emit.Invocation) (int, error) {
		tok := "none"
		if ts, ok := kube.CallerTokenFrom(ctx); ok {
			if t, err := ts.Token(); err == nil {
				tok = tokenFingerprint(t.AccessToken)
			}
		}
		filtered := "false"
		if kube.CallerFiltered(ctx) {
			filtered = "true"
		}
		return 1, inv.Out.Emit(emit.Finding{
			Kind: "fake.whoami", Severity: emit.SeverityInfo, KindOfObject: "Pod", Name: "whoami",
			Reason: "Caller", Message: "token=" + tok + " recorded_by=" + kube.CallerNameFrom(ctx) + " filtered=" + filtered,
		})
	}
	kind := []checks.KindField{checks.Kind("fake.whoami", "synthetic; test scaffolding only", emit.SeverityInfo)}
	reg.Register(checks.Command{Name: "whoami", MCPName: "k8s_whoami", Summary: "Reports the caller; test scaffolding only.", Kinds: kind, Run: report})
	reg.Register(checks.Command{Name: "write", MCPName: "k8s_write", Summary: "Changes state; test scaffolding only.", Writes: true, Kinds: kind, Run: report})
	reg.Register(checks.Command{Name: "other", MCPName: "k8s_other", Summary: "Granted to nobody; test scaffolding only.", Kinds: kind, Run: report})
	return reg
}

type errTokenSource struct{ err error }

func (e errTokenSource) Token() (*oauth2.Token, error) { return nil, e.err }

// fakeImpersonate stands in for IAM Credentials generateAccessToken.
func fakeImpersonate(_ context.Context, sa string, _ ...string) (oauth2.TokenSource, error) {
	switch sa {
	case saReader:
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: saSentinelToken}), nil
	case saBroken:
		return errTokenSource{errors.New("iam: permission denied on generateAccessToken; last token ya29.leaked-in-an-error-0123")}, nil
	}
	return nil, errors.New("unknown service account")
}

type workloadServer struct {
	url   string
	ca    *testCA
	log   *bytes.Buffer
	mu    *sync.Mutex
	plain string // the --listen listener (bearer), when served
}

// startWorkloadServer serves reg with a PKI-profile mTLS listener and the
// given rules, plus a bearer-token --listen listener beside it.
func startWorkloadServer(t *testing.T, rules string, reg *checks.Registry, opts ...Option) workloadServer {
	t.Helper()
	dir := t.TempDir()
	ca := newTestCA(t, "lookout-test-ca")
	_, certPEM, keyPEM := ca.issue(t, "", true)
	w, err := NewWorkloadAuth(WorkloadOptions{
		CertFile:     writeFile(t, dir, "tls.crt", certPEM),
		KeyFile:      writeFile(t, dir, "tls.key", keyPEM),
		ClientCAFile: writeFile(t, dir, "ca.crt", ca.pem),
		RulesFile:    writeFile(t, dir, "rules.yaml", []byte(rules)),
		Registry:     reg,
		MultiCluster: hasClusters(opts),
		Impersonate:  fakeImpersonate,
	})
	if err != nil {
		t.Fatal(err)
	}
	tokenFile := writeFile(t, dir, "token", []byte("plain-listener-bearer-token-0123"))
	bearer, err := LoadAuthToken(tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var logBuf bytes.Buffer
	al := NewAccessLog(lockedWriter{&mu, &logBuf})
	server := New(reg, "test", append([]Option{WithWorkloadAuth(w), WithAccessLog(al)}, opts...)...)
	ctx, cancel := context.WithCancel(context.Background())
	ready, mtlsReady := make(chan string, 1), make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, server, ServeOptions{
			Listen: "127.0.0.1:0", Auth: bearer, Ready: ready,
			MTLSListen: "127.0.0.1:0", Workloads: w, MTLSReady: mtlsReady,
			Stateless: hasClusters(opts),
		})
	}()
	plain, mtlsAddr := <-ready, <-mtlsReady
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	return workloadServer{url: "https://" + mtlsAddr, ca: ca, log: &logBuf, mu: &mu, plain: "http://" + plain}
}

func hasClusters(opts []Option) bool {
	var c config
	for _, o := range opts {
		o(&c)
	}
	return c.clusters != nil
}

// connectAs opens an MCP session over the mTLS listener presenting
// certs (none, for a client without a certificate).
func (s workloadServer) connectAs(t *testing.T, certs ...tls.Certificate) (*mcp.ClientSession, error) {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(s.ca.cert)
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: pool, Certificates: certs, MinVersion: tls.VersionTLS13,
	}}, Timeout: 20 * time.Second}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "workload", Version: "0"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{Endpoint: s.url, HTTPClient: hc, MaxRetries: -1}, nil)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs, nil
}

func (s workloadServer) mustConnectAs(t *testing.T, id string) *mcp.ClientSession {
	t.Helper()
	cert, _, _ := s.ca.issue(t, id, false)
	cs, err := s.connectAs(t, cert)
	if err != nil {
		t.Fatalf("%s: %v", id, err)
	}
	return cs
}

func (s workloadServer) accessLog() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.String()
}

// TestWorkloadAccepted: a workload presenting a certificate the client CA
// issued becomes a caller named by its URI SAN. It reads as lookout (no
// caller token), its rows are not filtered, records it writes name it,
// and the access log names it.
func TestWorkloadAccepted(t *testing.T) {
	s := startWorkloadServer(t, workloadRulesYAML, workloadRegistry())
	cs := s.mustConnectAs(t, idReader)
	res, err := callTool(t, cs, "k8s_whoami", nil)
	if err != nil || res.IsError {
		t.Fatalf("whoami: %v %+v", err, res)
	}
	out := text(t, res)
	for _, want := range []string{"token=none", "recorded_by=" + idReader, "filtered=false"} {
		if !strings.Contains(out, want) {
			t.Errorf("whoami lacks %q:\n%s", want, out)
		}
	}
	if log := s.accessLog(); !strings.Contains(log, "tool=k8s_whoami exit=0") || !strings.Contains(log, "caller="+idReader) {
		t.Errorf("access log does not name the workload:\n%s", log)
	}
}

// TestWorkloadCertificateRefusals: an untrusted certificate, and no
// certificate at all, are refused in the handshake; the mTLS listener
// accepts nothing else, a bearer token included, while the --listen
// listener beside it keeps authenticating its own callers.
func TestWorkloadCertificateRefusals(t *testing.T) {
	s := startWorkloadServer(t, workloadRulesYAML, workloadRegistry())

	rogue := newTestCA(t, "rogue-ca")
	cert, _, _ := rogue.issue(t, idReader, false)
	if _, err := s.connectAs(t, cert); err == nil {
		t.Error("a certificate from an untrusted CA was accepted")
	}
	if _, err := s.connectAs(t); err == nil {
		t.Error("a client without a certificate was accepted")
	}

	// A bearer token on the mTLS listener, with no certificate: refused.
	pool := x509.NewCertPool()
	pool.AddCert(s.ca.cert)
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}}}
	req, _ := http.NewRequest(http.MethodPost, s.url, strings.NewReader(callBody("k8s_whoami")))
	req.Header.Set("Authorization", "Bearer plain-listener-bearer-token-0123")
	if resp, err := hc.Do(req); err == nil {
		_ = resp.Body.Close()
		t.Errorf("a bearer token without a certificate reached the mTLS listener: %d", resp.StatusCode)
	}

	// The --listen listener still serves its bearer callers, and a
	// spoofed placeholder is just a wrong token there.
	for tok, want := range map[string]int{"plain-listener-bearer-token-0123": http.StatusOK, "mtls-verified-caller": http.StatusUnauthorized} {
		req, _ := http.NewRequest(http.MethodPost, s.plain, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"x","version":"0"}}}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("--listen with token %q: %d, want %d", tok, resp.StatusCode, want)
		}
	}
}

// TestWorkloadToolDenied: a workload may call only the tools a rule
// grants it, and a verified workload no rule names may call nothing.
func TestWorkloadToolDenied(t *testing.T) {
	s := startWorkloadServer(t, workloadRulesYAML, workloadRegistry())
	cs := s.mustConnectAs(t, idReader)
	res, err := callTool(t, cs, "k8s_other", nil)
	if err != nil || !res.IsError || !strings.Contains(text(t, res), "is not granted k8s_other") || strings.Contains(text(t, res), "recorded_by=") {
		t.Errorf("an ungranted tool: %v %+v", err, res)
	}
	stranger := s.mustConnectAs(t, idStranger)
	res, err = callTool(t, stranger, "k8s_whoami", nil)
	if err != nil || !res.IsError || !strings.Contains(text(t, res), "--workload-rules") {
		t.Errorf("a workload no rule names: %v %+v", err, res)
	}
	if log := s.accessLog(); !strings.Contains(log, "tool=k8s_other exit=1") || !strings.Contains(log, "caller="+idStranger) {
		t.Errorf("refusals are not in the access log:\n%s", log)
	}
}

// TestWorkloadWriteNeedsExplicitGrant: naming a state-changing tool is
// not enough; the granting rule must also say write: true.
func TestWorkloadWriteNeedsExplicitGrant(t *testing.T) {
	s := startWorkloadServer(t, workloadRulesYAML, workloadRegistry())
	res, err := callTool(t, s.mustConnectAs(t, idReader), "k8s_write", nil)
	if err != nil || !res.IsError || !strings.Contains(text(t, res), "write: true") {
		t.Errorf("a write without the grant: %v %+v", err, res)
	}
	res, err = callTool(t, s.mustConnectAs(t, idWriter), "k8s_write", nil)
	if err != nil || res.IsError || !strings.Contains(text(t, res), "recorded_by="+idWriter) {
		t.Errorf("a granted write: %v %+v", err, res)
	}
}

// TestWorkloadImpersonation: a rule naming a service account makes the
// call read with that account's token (from the impersonator), still as a
// workload (rows unfiltered), and the token reaches no output.
func TestWorkloadImpersonation(t *testing.T) {
	s := startWorkloadServer(t, workloadRulesYAML, workloadRegistry())
	res, err := callTool(t, s.mustConnectAs(t, idImpersonated), "k8s_whoami", nil)
	if err != nil || res.IsError {
		t.Fatalf("impersonated whoami: %v %+v", err, res)
	}
	out := text(t, res)
	if !strings.Contains(out, "token="+tokenFingerprint(saSentinelToken)) || !strings.Contains(out, "filtered=false") {
		t.Errorf("the call did not read as the service account, as a workload:\n%s", out)
	}
	for name, surface := range map[string]string{"response": out, "access log": s.accessLog()} {
		if strings.Contains(surface, saSentinelToken) {
			t.Errorf("the impersonated token reached the %s", name)
		}
	}
}

// TestWorkloadImpersonationFailureIsExplicit: an impersonation that fails
// fails the call with the reason; it never runs as lookout instead.
func TestWorkloadImpersonationFailureIsExplicit(t *testing.T) {
	s := startWorkloadServer(t, workloadRulesYAML, workloadRegistry())
	res, err := callTool(t, s.mustConnectAs(t, idBroken), "k8s_whoami", nil)
	if err != nil || !res.IsError {
		t.Fatalf("a failed impersonation did not fail the call: %v %+v", err, res)
	}
	out := text(t, res)
	if !strings.Contains(out, saBroken) || !strings.Contains(out, "permission denied on generateAccessToken") || strings.Contains(out, "token=none") {
		t.Errorf("the failure is not explicit, or the tool ran anyway:\n%s", out)
	}
	// An error that quotes a token is sanitized before it is returned.
	if strings.Contains(out, "ya29.leaked") || strings.Contains(s.accessLog(), "ya29.") {
		t.Errorf("a token quoted in the impersonation error reached an output:\n%s", out)
	}
}

// TestWorkloadRulesMustAgreeOnIdentity: two rules granting one call but
// naming different service accounts are refused rather than picked from.
func TestWorkloadRulesMustAgreeOnIdentity(t *testing.T) {
	rules := workloadRulesYAML + `
  - name: reader-as-sa
    identities: [` + idReader + `]
    tools: [k8s_whoami]
    service_account: ` + saReader + `
`
	s := startWorkloadServer(t, rules, workloadRegistry())
	res, err := callTool(t, s.mustConnectAs(t, idReader), "k8s_whoami", nil)
	if err != nil || !res.IsError || !strings.Contains(text(t, res), "must agree on one service_account") {
		t.Errorf("disagreeing rules: %v %+v", err, res)
	}
}

// TestWorkloadClusters: on a multi-cluster endpoint a workload may call
// only the clusters its rules allow, and k8s_list_clusters lists only
// those.
func TestWorkloadClusters(t *testing.T) {
	_, dir, regFn := multiClusterServer(t, nil)
	reg := regFn()
	rules := `
rules:
  - name: dev-only
    identities: [` + idReader + `]
    tools: [k8s_echo_target]
    clusters: [dev]
  - name: everywhere
    identities: [` + idWriter + `]
    tools: [all]
    clusters: ["*"]
`
	s := startWorkloadServer(t, rules, reg, WithClusters(dir))
	cs := s.mustConnectAs(t, idReader)
	if res, err := callTool(t, cs, "k8s_echo_target", map[string]any{"cluster": "dev"}); err != nil || res.IsError {
		t.Errorf("an allowed cluster: %v %+v", err, res)
	}
	res, err := callTool(t, cs, "k8s_echo_target", map[string]any{"cluster": "prod@proj-a"})
	if err != nil || !res.IsError || !strings.Contains(text(t, res), "not granted cluster proj-a/us-central1/prod") {
		t.Errorf("a cluster the rules do not allow: %v %+v", err, res)
	}
	res, err = callTool(t, cs, listClustersName, nil)
	if err != nil || res.IsError {
		t.Fatalf("list clusters: %v %+v", err, res)
	}
	if out := text(t, res); !strings.Contains(out, "dev") || strings.Contains(out, "prod") {
		t.Errorf("list clusters shows clusters the workload may not call:\n%s", out)
	}
	res, err = callTool(t, s.mustConnectAs(t, idWriter), "k8s_echo_target", map[string]any{"cluster": "prod@proj-b"})
	if err != nil || res.IsError {
		t.Errorf(`"*": %v %+v`, err, res)
	}
	if log := s.accessLog(); !strings.Contains(log, "caller="+idReader+" cluster=proj-a/us-central1/prod") {
		t.Errorf("access log lacks the refused call's caller and cluster:\n%s", log)
	}
}

// TestWorkloadSPIFFEProfile: under the SPIFFE profile lookout presents its
// own X509-SVID, a caller is its verified SPIFFE ID, and rules can match a
// trust domain and path prefix structurally.
func TestWorkloadSPIFFEProfile(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, "example.test bundle")
	_, svid, svidKey := ca.issue(t, "spiffe://example.test/ns/lookout/sa/lookout", true)
	writeFile(t, dir, "certificates.pem", svid)
	writeFile(t, dir, "private_key.pem", svidKey)
	writeFile(t, dir, "ca_certificates.pem", ca.pem)
	rules := `
rules:
  - name: agents-namespace
    trust_domain: example.test
    path_prefix: /ns/agents
    tools: [k8s_whoami]
`
	reg := workloadRegistry()
	w, err := NewWorkloadAuth(WorkloadOptions{
		SPIFFETrustDomain: "example.test", SPIFFEDir: dir,
		RulesFile: writeFile(t, t.TempDir(), "rules.yaml", []byte(rules)), Registry: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	server := New(reg, "test", WithWorkloadAuth(w))
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, server, ServeOptions{MTLSListen: "127.0.0.1:0", Workloads: w, MTLSReady: ready, Stateless: true})
	}()
	s := workloadServer{url: "https://" + <-ready, ca: ca}
	t.Cleanup(func() { cancel(); <-done })

	res, err := callTool(t, s.mustConnectAs(t, idReader), "k8s_whoami", nil)
	if err != nil || res.IsError || !strings.Contains(text(t, res), "recorded_by="+idReader) {
		t.Errorf("an SVID under the path prefix: %v %+v", err, res)
	}
	res, err = callTool(t, s.mustConnectAs(t, idStranger), "k8s_whoami", nil)
	if err != nil || !res.IsError {
		t.Errorf("an SVID outside the path prefix: %v %+v", err, res)
	}
	other := newTestCA(t, "another bundle")
	cert, _, _ := other.issue(t, idReader, false)
	if _, err := s.connectAs(t, cert); err == nil {
		t.Error("an SVID that does not chain to the bundle was accepted")
	}
}

// TestServeRefusesWorkloadMismatch: the mTLS listener is never put in
// front of a server that does not apply its rules.
func TestServeRefusesWorkloadMismatch(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, "ca")
	_, certPEM, keyPEM := ca.issue(t, "", true)
	reg := workloadRegistry()
	w, err := NewWorkloadAuth(WorkloadOptions{
		CertFile: writeFile(t, dir, "c", certPEM), KeyFile: writeFile(t, dir, "k", keyPEM), ClientCAFile: writeFile(t, dir, "ca", ca.pem),
		RulesFile: writeFile(t, dir, "r", []byte(workloadRulesYAML)), Registry: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = Serve(t.Context(), New(reg, "test"), ServeOptions{MTLSListen: "127.0.0.1:0", Workloads: w})
	if err == nil || !strings.Contains(err.Error(), "WithWorkloadAuth") {
		t.Errorf("Serve = %v", err)
	}
}

// TestWorkloadRulesValidation: a rules file that cannot mean what its
// author meant is refused at startup.
func TestWorkloadRulesValidation(t *testing.T) {
	reg := workloadRegistry()
	for name, tc := range map[string]struct {
		yaml          string
		multi, spiffe bool
		want          string
	}{
		"empty":              {yaml: "rules: []", want: "no rules"},
		"unknown key":        {yaml: "rules:\n- name: a\n  identity: [x]\n  tools: [all]", want: "unknown field"},
		"no name":            {yaml: "rules:\n- identities: [x]\n  tools: [all]", want: "no name"},
		"duplicate":          {yaml: "rules:\n- name: a\n  identities: [x]\n  tools: [all]\n- name: a\n  identities: [y]\n  tools: [all]", want: "declared twice"},
		"matches nobody":     {yaml: "rules:\n- name: a\n  tools: [all]", want: "matches no caller"},
		"no tools":           {yaml: "rules:\n- name: a\n  identities: [x]", want: "grants no tools"},
		"unknown tool":       {yaml: "rules:\n- name: a\n  identities: [x]\n  tools: [k8s_nope]", want: "neither a tool nor a profile"},
		"path without td":    {yaml: "rules:\n- name: a\n  path_prefix: /ns/x\n  tools: [all]", spiffe: true, want: "needs trust_domain"},
		"td under PKI":       {yaml: "rules:\n- name: a\n  trust_domain: example.test\n  tools: [all]", want: "--spiffe-trust-domain"},
		"clusters, single":   {yaml: "rules:\n- name: a\n  identities: [x]\n  tools: [all]\n  clusters: ['*']", want: "multi-cluster endpoint"},
		"no clusters, multi": {yaml: "rules:\n- name: a\n  identities: [x]\n  tools: [all]", multi: true, want: "name the clusters"},
		"bad sa":             {yaml: "rules:\n- name: a\n  identities: [x]\n  tools: [all]\n  service_account: nope", want: "not a service account email"},
	} {
		t.Run(name, func(t *testing.T) {
			path := writeFile(t, t.TempDir(), "rules.yaml", []byte(tc.yaml))
			_, err := loadWorkloadRules(path, reg, tc.multi, tc.spiffe)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
	if _, err := loadWorkloadRules("", reg, false, false); err == nil {
		t.Error("no rules file was accepted")
	}
}

// TestNewWorkloadAuthProfiles: exactly one complete way to verify
// certificates.
func TestNewWorkloadAuthProfiles(t *testing.T) {
	for name, opts := range map[string]WorkloadOptions{
		"none":       {},
		"both":       {CertFile: "c", KeyFile: "k", ClientCAFile: "ca", SPIFFETrustDomain: "example.test"},
		"incomplete": {CertFile: "c", ClientCAFile: "ca"},
	} {
		if _, err := NewWorkloadAuth(opts); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
