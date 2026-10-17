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
	"flag"
	"fmt"
	"io"
	"log/slog"

	"github.com/go-steer/k8s-lookout/internal/mcpserver"
	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// workloadFlags are the `lookout mcp` flags of the mTLS listener for
// workload callers (#701).
type workloadFlags struct {
	listen, certFile, keyFile, clientCAFile, spiffeTrustDomain, spiffeDir, rules *string
}

func registerWorkloadFlags(fs *flag.FlagSet) workloadFlags {
	return workloadFlags{
		listen:            fs.String("mtls-listen", "", "serve workload callers over mTLS on this host:port"),
		certFile:          fs.String("tls-cert-file", "", "mTLS listener: lookout's certificate (PEM)"),
		keyFile:           fs.String("tls-key-file", "", "mTLS listener: lookout's private key (PEM)"),
		clientCAFile:      fs.String("client-ca-file", "", "mTLS listener: the CA bundle workload certificates must chain to (PEM)"),
		spiffeTrustDomain: fs.String("spiffe-trust-domain", "", "mTLS listener: verify workloads' X509-SVIDs in this SPIFFE trust domain"),
		spiffeDir:         fs.String("spiffe-credentials-dir", "", "mTLS listener: directory holding lookout's X509-SVID and trust bundle (GKE layout)"),
		rules:             fs.String("workload-rules", "", "mTLS listener: YAML rules granting each workload its tools, clusters and identity"),
	}
}

const usageHint = "\nRun 'lookout mcp --help' for usage."

// problem checks the workload flags against the rest, returning a usage
// problem or "". The workload flags configure the mTLS listener and mean
// nothing without it.
func (w workloadFlags) problem(listen string, allowNonLoopback bool, accessLog string) string {
	if *w.listen == "" {
		for _, f := range []struct {
			name string
			set  bool
		}{
			{"--tls-cert-file", *w.certFile != ""},
			{"--tls-key-file", *w.keyFile != ""},
			{"--client-ca-file", *w.clientCAFile != ""},
			{"--spiffe-trust-domain", *w.spiffeTrustDomain != ""},
			{"--spiffe-credentials-dir", *w.spiffeDir != ""},
			{"--workload-rules", *w.rules != ""},
		} {
			if f.set {
				return f.name + " configures the mTLS listener for workload callers; pass --mtls-listen=<host:port> as well." + usageHint
			}
		}
		return ""
	}
	switch {
	case *w.rules == "":
		return "--mtls-listen needs --workload-rules: without rules no workload may call anything" + usageHint
	case *w.spiffeDir != "" && *w.spiffeTrustDomain == "":
		return "--spiffe-credentials-dir needs --spiffe-trust-domain: a PEM bundle does not say which trust domain its anchors vouch for" + usageHint
	case *w.listen == listen:
		return "--mtls-listen and --listen must be different addresses: each listener has its own way in" + usageHint
	}
	// The certificate is this listener's authentication; a routable bind
	// still needs the opt-in and the access log.
	if err := (mcpserver.BindPolicy{
		Flag:             "--mtls-listen",
		Listen:           *w.listen,
		AllowNonLoopback: allowNonLoopback,
		HasAuthToken:     true,
		HasAccessLog:     accessLog != "",
	}).Validate(); err != nil {
		return err.Error()
	}
	return ""
}

// build loads the rules and credentials. It returns nil and 0 when the
// mTLS listener is not configured, and nil and the exit code on failure.
func (w workloadFlags) build(reg *checks.Registry, multiCluster bool, stderr io.Writer) (*mcpserver.WorkloadAuth, int) {
	if *w.listen == "" {
		return nil, 0
	}
	wa, err := mcpserver.NewWorkloadAuth(mcpserver.WorkloadOptions{
		CertFile:          *w.certFile,
		KeyFile:           *w.keyFile,
		ClientCAFile:      *w.clientCAFile,
		SPIFFETrustDomain: *w.spiffeTrustDomain,
		SPIFFEDir:         *w.spiffeDir,
		RulesFile:         *w.rules,
		Registry:          reg,
		MultiCluster:      multiCluster,
		Logger:            slog.New(slog.NewTextHandler(stderr, nil)),
	})
	if err != nil {
		fmt.Fprintf(stderr, "lookout mcp: %v%s\n", err, usageHint)
		return nil, 2
	}
	// Refused at startup, not at the first call: a rule that reads as a
	// service account must never quietly read as lookout instead.
	if wa.NeedsImpersonation() && !cloud.CanImpersonate() {
		_ = wa.Close()
		fmt.Fprintf(stderr, "lookout mcp: --workload-rules: a rule names a service_account, and %v\n", cloud.ErrNoImpersonator)
		return nil, 2
	}
	return wa, 0
}
