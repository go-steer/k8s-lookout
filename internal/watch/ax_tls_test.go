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

package watch

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
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/go-steer/k8s-lookout/internal/axapi"
)

// axGetTask answers GetTask, enough to prove an RPC made it through.
type axGetTask struct{ axapi.UnimplementedAXServer }

func (axGetTask) GetTask(_ context.Context, req *axapi.GetTaskRequest) (*axapi.Task, error) {
	return &axapi.Task{Metadata: &axapi.ObjectMeta{Name: req.GetName()}}, nil
}

// selfSignedCert makes a self-signed certificate for 127.0.0.1 and
// localhost and writes its PEM to a file, which doubles as the CA file.
func selfSignedCert(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ax-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return cert, caFile
}

// serveAX serves axGetTask on a local listener, over TLS when cert is
// non-nil, and returns its address.
func serveAX(t *testing.T, cert *tls.Certificate) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var opts []grpc.ServerOption
	if cert != nil {
		opts = append(opts, grpc.Creds(credentials.NewServerTLSFromCert(cert)))
	}
	srv := grpc.NewServer(opts...)
	axapi.RegisterAXServer(srv, axGetTask{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// getTask dials AX the way the sink does and makes one RPC.
func getTask(t *testing.T, f *flags) error {
	t.Helper()
	conn, err := dialAX(f)
	if err != nil {
		t.Fatalf("dialAX: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = axapi.NewAXClient(conn).GetTask(ctx, &axapi.GetTaskRequest{Name: "lookout-x"})
	return err
}

func TestDialAX_TLSWithCAFile(t *testing.T) {
	cert, caFile := selfSignedCert(t)
	addr := serveAX(t, &cert)
	if err := getTask(t, &flags{axServer: addr, axServerTLS: true, axCAFile: caFile}); err != nil {
		t.Fatalf("TLS with the CA file: %v", err)
	}
}

// Without the CA, a self-signed certificate is untrusted against the system
// roots, and the RPC says so.
func TestDialAX_TLSWithoutCAFileRejectsSelfSigned(t *testing.T) {
	cert, _ := selfSignedCert(t)
	addr := serveAX(t, &cert)
	err := getTask(t, &flags{axServer: addr, axServerTLS: true})
	if err == nil {
		t.Fatal("TLS without the CA file trusted a self-signed certificate")
	}
	if !strings.Contains(err.Error(), "certificate") {
		t.Errorf("error does not name the certificate problem: %v", err)
	}
}

// The default is unchanged: plaintext gRPC, as AX itself serves.
func TestDialAX_PlaintextByDefault(t *testing.T) {
	addr := serveAX(t, nil)
	if err := getTask(t, &flags{axServer: addr}); err != nil {
		t.Fatalf("plaintext: %v", err)
	}
}

func TestAXTransportCredentials_BadCAFile(t *testing.T) {
	notPEM := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(notPEM, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{
		"missing": filepath.Join(t.TempDir(), "absent.pem"),
		"not PEM": notPEM,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := axTransportCredentials(&flags{axServerTLS: true, axCAFile: path}); err == nil {
				t.Errorf("a %s CA file was accepted", name)
			}
		})
	}
}

func TestClusterLocalHost(t *testing.T) {
	cases := map[string]bool{
		"atenet-router.ate-system.svc.cluster.local":  true,
		"atenet-router.ate-system.svc.cluster.local.": true,
		"atenet-router.ate-system.svc":                true,
		"localhost":                                   true,
		"127.0.0.1":                                   true,
		"::1":                                         true,
		"10.0.0.5":                                    false, // an internal LB in the VPC
		"192.168.1.10":                                false,
		"router.example.com":                          false,
		"atenet-router.ate-system":                    false, // short names go through the search path
		"svc.example.com":                             false,
	}
	for host, want := range cases {
		if got := clusterLocalHost(host); got != want {
			t.Errorf("clusterLocalHost(%q) = %v, want %v", host, got, want)
		}
	}
}

func TestAXRouterPlainHTTPWarning(t *testing.T) {
	cases := map[string]bool{
		defaultAXRouterURL:                 false,
		"http://localhost:8080":            false,
		"http://[::1]:8080":                false,
		"https://router.example.com":       false,
		"https://10.0.0.5":                 false,
		"http://router.example.com":        true,
		"http://10.0.0.5:80":               true,
		"http://atenet-router.ate-system":  true,
		"http://router.example.com:8080/x": true,
	}
	for u, want := range cases {
		if _, got := axRouterPlainHTTPWarning(u); got != want {
			t.Errorf("axRouterPlainHTTPWarning(%q) warns = %v, want %v", u, got, want)
		}
	}
}

// Sanitization: building the sink with a plain-http remote router logs the
// warning, and neither the bearer token nor a password in the URL reaches
// the log. Not parallel: it swaps the standard logger's output.
func TestNewAXSink_PlainHTTPWarningCarriesNoSecret(t *testing.T) {
	const token = "tok-9f8e7d6c5b4a-must-not-leak"
	const password = "hunter2-must-not-leak"
	tmpl := writeTemplate(t, "kind: Task\nspec:\n  image: agent@sha256:abc\n")

	buf, restoreLog := captureLogOutput(t)
	defer restoreLog()

	f := &flags{
		axServer:       "127.0.0.1:1",
		axTaskTemplate: tmpl,
		axRouterURL:    "http://lookout:" + password + "@router.example.com",
		axTaskScope:    axScopeIncident,
	}
	if _, err := newAXSink(f, token); err != nil {
		t.Fatalf("newAXSink: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "WARNING: --ax-router-url") || !strings.Contains(out, "router.example.com") {
		t.Fatalf("no plain-http warning naming the router; log was %q", out)
	}
	if strings.Contains(out, token) || strings.Contains(out, password) {
		t.Errorf("the warning leaked a secret: %q", out)
	}

	// The in-cluster default stays quiet.
	buf.Reset()
	f.axRouterURL = defaultAXRouterURL
	if _, err := newAXSink(f, token); err != nil {
		t.Fatalf("newAXSink: %v", err)
	}
	if strings.Contains(buf.String(), "WARNING") {
		t.Errorf("warned about the in-cluster router: %q", buf.String())
	}
}

func TestRedactedURL(t *testing.T) {
	got := redactedURL("http://lookout:s3cret@router.example.com")
	if strings.Contains(got, "s3cret") || !strings.Contains(got, "router.example.com") {
		t.Errorf("redactedURL = %q", got)
	}
}

// The webhook plain-http warning masks a password embedded in --sink-url.
func TestWebhookPlainHTTPWarningMasksPassword(t *testing.T) {
	const password = "hunter2-must-not-leak"
	warning, ok := webhookPlainHTTPWarning("http://lookout:" + password + "@hooks.example.com")
	if !ok || !strings.Contains(warning, "hooks.example.com") {
		t.Fatalf("no plain-http warning naming the receiver: %q", warning)
	}
	if strings.Contains(warning, password) {
		t.Errorf("the warning leaked the URL password: %q", warning)
	}
	if _, ok := webhookPlainHTTPWarning("https://lookout:" + password + "@hooks.example.com"); ok {
		t.Error("warned about an https receiver")
	}
}

// Flag errors that echo --sink-url or --ax-router-url mask the password too.
func TestURLFlagErrorsMaskPassword(t *testing.T) {
	const password = "hunter2-must-not-leak"
	for _, args := range [][]string{
		{"--sink=webhook", "--dry-run", "--sink-url=https://u:" + password + "@hooks.example.com/"},
		{"--sink=ax", "--dry-run", "--ax-router-url=https://u:" + password + "@router.example.com/"},
	} {
		f, err := parseFlags(args)
		if err != nil {
			t.Fatalf("parseFlags(%v): %v", args, err)
		}
		err = f.validate()
		if err == nil {
			t.Fatalf("validate(%v) accepted a trailing slash", args)
		}
		if strings.Contains(err.Error(), password) {
			t.Errorf("validate(%v) leaked the URL password: %v", args, err)
		}
	}
}
