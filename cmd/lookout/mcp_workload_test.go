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

package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
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

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// workloadPKI writes an in-test CA and a server certificate for
// 127.0.0.1, and returns the --tls-cert-file, --tls-key-file and
// --client-ca-file flags.
func workloadPKI(t *testing.T) []string {
	t.Helper()
	dir := t.TempDir()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(2), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	write := func(name string, typ string, b []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: b}), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	return []string{
		"--tls-cert-file=" + write("tls.crt", "CERTIFICATE", der),
		"--tls-key-file=" + write("tls.key", "PRIVATE KEY", keyDER),
		"--client-ca-file=" + write("ca.crt", "CERTIFICATE", caDER),
	}
}

func writeRules(t *testing.T, yaml string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "rules.yaml")
	if err := os.WriteFile(p, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestMCPWorkloadListenerServes: the real wiring starts the mTLS listener
// with the server built WithWorkloadAuth (Serve would refuse a mismatch
// with an internal error), alone, without --listen.
func TestMCPWorkloadListenerServes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	var stdout, stderr bytes.Buffer
	args := append([]string{
		"--mtls-listen=127.0.0.1:0",
		"--workload-rules=" + writeRules(t, "rules:\n- name: agents\n  identities: [spiffe://example.test/ns/agents/sa/core-agent]\n  tools: [triage]\n"),
	}, workloadPKI(t)...)
	if code := mcpMain(ctx, args, &stdout, &stderr); code != 0 || strings.Contains(stderr.String(), "internal") {
		t.Fatalf("exit %d, stderr: %s", code, stderr.String())
	}
	for _, want := range []string{"mTLS (workload callers)", "1 rule(s)"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("banner lacks %q: %s", want, stderr.String())
		}
	}
	if stdout.Len() != 0 {
		t.Errorf("serving wrote to stdout: %q", stdout.String())
	}
}

// TestMCPWorkloadImpersonationNeedsTheGKEBuild: a rule naming a service
// account is refused at startup by a build that cannot impersonate,
// rather than reading as lookout at call time.
func TestMCPWorkloadImpersonationNeedsTheGKEBuild(t *testing.T) {
	if cloud.CanImpersonate() {
		t.Skip("this build can impersonate")
	}
	var stdout, stderr bytes.Buffer
	args := append([]string{
		"--mtls-listen=127.0.0.1:0",
		"--workload-rules=" + writeRules(t, "rules:\n- name: agents\n  identities: [spiffe://example.test/ns/agents/sa/core-agent]\n  tools: [triage]\n  service_account: reader@example-project.iam.gserviceaccount.com\n"),
	}, workloadPKI(t)...)
	if code := mcpMain(t.Context(), args, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "-tags gke") {
		t.Errorf("exit %d, stderr: %s", code, stderr.String())
	}
}

// TestMCPWorkloadRulesFollowTheClusterMode: clusters are required on a
// multi-cluster endpoint's rules and refused on a single-cluster one's.
func TestMCPWorkloadRulesFollowTheClusterMode(t *testing.T) {
	var stdout, stderr bytes.Buffer
	args := append([]string{
		"--mtls-listen=127.0.0.1:0",
		"--workload-rules=" + writeRules(t, "rules:\n- name: agents\n  identities: [x]\n  tools: [triage]\n  clusters: ['*']\n"),
	}, workloadPKI(t)...)
	if code := mcpMain(t.Context(), args, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "multi-cluster endpoint") {
		t.Errorf("exit %d, stderr: %s", code, stderr.String())
	}
}
