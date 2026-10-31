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
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-steer/purser/authn/mtls"
	"github.com/go-steer/purser/client"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Agent Substrate's own conventions for reaching the router over mTLS
// (issue #809, docs/ax-sink-design.md "The router"). The paths are where
// Substrate's components and upstream ax mount the two projected volumes;
// lookout's flags default to them so a manifest that copies Substrate's
// volume stanza needs no flags at all.
const (
	// DefaultRouterCredentialBundle is the credentialBundlePath file of a
	// podCertificate projection from the podidentity.podcert.ate.dev/identity
	// signer, mounted at /run/podidentity.podcert.ate.dev: one PKCS#8
	// private key followed by the certificate chain, leaf first. The leaf
	// carries the pod's SPIFFE ID, which the router matches against its
	// ingress allowlist.
	DefaultRouterCredentialBundle = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"
	// DefaultRouterTrustBundle is a clusterTrustBundle projection of the
	// servicedns.podcert.ate.dev/identity signer (label
	// podcert.ate.dev/canarying: live), mounted at /run/servicedns-ca: the
	// authorities the router's serving certificate chains to.
	DefaultRouterTrustBundle = "/run/servicedns-ca/trust-bundle.pem"
	// DefaultRouterServerName is the only DNS SAN on the router's serving
	// certificate. The servicedns signer names a certificate after the
	// Service it serves, <service>.<namespace>.svc, without the
	// cluster-domain suffix, so dialing the usual
	// atenet-router.ate-system.svc.cluster.local needs the name set
	// explicitly or hostname verification fails.
	DefaultRouterServerName = "atenet-router.ate-system.svc"
)

// RouterTLS configures mutual TLS to Agent Substrate's router, which
// since Substrate v0.6 accepts nothing else.
type RouterTLS struct {
	// CredentialBundlePath is the client's key and certificate chain in
	// the podCertificate credentialBundlePath layout. Required. Re-read as
	// the kubelet rotates it: a pod certificate lives at most 24 hours and
	// is refreshed 30 minutes before it expires.
	CredentialBundlePath string
	// TrustBundlePath is the PEM bundle the router's certificate must chain
	// to. Required, and re-read as it rotates, so a canaried servicedns CA
	// needs no restart. Never the system roots: the router's CA is
	// cluster-internal, and trusting the web PKI for it would be both
	// useless and wrong.
	TrustBundlePath string
	// ServerName is verified against the router's certificate and sent as
	// SNI. Empty verifies the host in the router URL instead.
	//
	// The servicedns CA issues a certificate to every Service-backed pod
	// in the cluster, each for its own Service's name. Chaining to that CA
	// therefore proves only "some Service in this cluster"; the name check
	// is what proves "the router".
	ServerName string
	// ReloadInterval is how often both files are re-read. Zero is purser's
	// default, 30 seconds: well inside the 30-minute refresh margin.
	ReloadInterval time.Duration
	// OnReloadError is told when a re-read fails; the previous credential
	// stays in use until it expires. Nil discards the report.
	OnReloadError func(error)
}

// NewRouterHTTPClient returns an HTTP client for Config.HTTPClient that
// authenticates to the router with the pod's certificate and verifies the
// router against the trust bundle, both read fresh on every handshake.
// Close stops the two file reloaders; the client fails every new
// handshake after that.
//
// Both files are read once here, so a missing mount or a malformed bundle
// fails at startup, naming the file, rather than as the first incident's
// open error two minutes later. The client otherwise matches the shared
// sink client (inject.NewSinkHTTPClient): 10s timeout, otelhttp, its own
// connection pool.
func NewRouterHTTPClient(cfg RouterTLS) (c *http.Client, closeFn func() error, err error) {
	if cfg.CredentialBundlePath == "" {
		return nil, nil, errors.New("ax sink: router mTLS needs a client credential bundle path")
	}
	if cfg.TrustBundlePath == "" {
		return nil, nil, errors.New("ax sink: router mTLS needs a trust bundle path")
	}
	creds, err := mtls.NewCertificateFileSource(mtls.CertificateFileOptions{
		CredentialBundlePath: cfg.CredentialBundlePath,
		ReloadInterval:       cfg.ReloadInterval,
		OnError:              cfg.OnReloadError,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("ax sink: router client certificate: %w", err)
	}
	roots, err := mtls.NewCertPoolFileSource(mtls.CertPoolFileOptions{
		Path:           cfg.TrustBundlePath,
		ReloadInterval: cfg.ReloadInterval,
		OnError:        cfg.OnReloadError,
	})
	if err != nil {
		_ = creds.Close()
		return nil, nil, fmt.Errorf("ax sink: router trust bundle: %w", err)
	}
	tlsCfg, err := client.NewPKI(client.PKIOptions{
		GetClientCertificate: creds.GetClientCertificate,
		RootCAsSource:        roots,
		ServerName:           cfg.ServerName,
	})
	if err != nil {
		_ = creds.Close()
		_ = roots.Close()
		return nil, nil, fmt.Errorf("ax sink: router TLS: %w", err)
	}
	closeFn = func() error { return errors.Join(creds.Close(), roots.Close()) }
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: otelhttp.NewTransport(client.Transport(tlsCfg)),
	}, closeFn, nil
}
