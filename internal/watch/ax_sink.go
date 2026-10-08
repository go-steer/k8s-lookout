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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"

	"github.com/go-steer/k8s-lookout/internal/axapi"
	"github.com/go-steer/k8s-lookout/internal/axsink"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// newAXSink builds the ax sink from flags (docs/ax-sink-design.md). The AX
// API is plaintext gRPC unless --ax-server-tls; the router URL's scheme
// decides TLS for the session calls, which carry the bearer token.
func newAXSink(f *flags, token string) (*axsink.Sink, error) {
	tmpl, err := loadAXTaskTemplate(f.axTaskTemplate)
	if err != nil {
		return nil, err
	}
	conn, err := dialAX(f)
	if err != nil {
		return nil, err
	}
	if warning, ok := axRouterPlainHTTPWarning(f.axRouterURL); ok {
		log.Print(warning)
	}
	return axsink.New(axsink.Config{
		Client:         axapi.NewAXClient(conn),
		Template:       tmpl,
		RouterURL:      f.axRouterURL,
		BearerToken:    token,
		AssertedCaller: f.owner,
		Scope:          axsink.Scope(f.axTaskScope),
	})
}

// dialAX opens the AX API connection. gRPC connects lazily, so a TLS
// failure (an untrusted certificate, a name mismatch) surfaces on the
// first RPC as that incident's open error, not here; an unreadable CA
// file fails here, at startup.
func dialAX(f *flags) (*grpc.ClientConn, error) {
	creds, err := axTransportCredentials(f)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(f.axServer, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("connecting to AX at %s: %w", f.axServer, err)
	}
	return conn, nil
}

// axTransportCredentials is plaintext by default, which is what AX itself
// serves. With --ax-server-tls the certificate is verified against the
// system roots, or only against --ax-ca-file's CAs when that is set, and
// against the host in --ax-server (gRPC takes the server name from it).
func axTransportCredentials(f *flags) (credentials.TransportCredentials, error) {
	if !f.axServerTLS {
		return insecure.NewCredentials(), nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if f.axCAFile != "" {
		pem, err := os.ReadFile(f.axCAFile)
		if err != nil {
			return nil, fmt.Errorf("reading --ax-ca-file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("--ax-ca-file %s holds no PEM certificates", f.axCAFile)
		}
		cfg.RootCAs = pool
	}
	return credentials.NewTLS(cfg), nil
}

// axRouterPlainHTTPWarning returns the startup warning for a plain-http
// router outside the cluster: every session call carries the --token-env
// bearer token, so over http the token crosses the network in the clear.
// The URL is printed redacted (no userinfo password); the token is never
// in reach of this function.
func axRouterPlainHTTPWarning(routerURL string) (string, bool) {
	u, err := url.Parse(routerURL)
	if err != nil || u.Scheme != "http" || clusterLocalHost(u.Hostname()) {
		return "", false
	}
	return fmt.Sprintf("WARNING: --ax-router-url %s is plain http to a host outside the cluster — the --token-env bearer token and incident payloads cross the network unencrypted; use an https router URL (docs/ax-sink-design.md, Cross-cluster)", redactedURL(routerURL)), true
}

// redactedURL is raw with any userinfo password masked, for log lines.
func redactedURL(raw string) string { return inject.RedactedURL(raw) }

// webhookPlainHTTPWarning returns the startup warning for a plain-http
// --sink-url, with any password in the URL masked.
func webhookPlainHTTPWarning(sinkURL string) (string, bool) {
	if !strings.HasPrefix(sinkURL, "http://") {
		return "", false
	}
	return fmt.Sprintf("sink: webhook receiver %s uses plain http — incident payloads and the bearer token ride unencrypted; use https for anything beyond a trusted network", redactedURL(sinkURL)), true
}

// clusterLocalHost reports whether traffic to host stays inside the pod's
// cluster: loopback, or a fully qualified Service name (*.svc,
// *.svc.cluster.local). Private IPs are not cluster-local: an internal
// load balancer in a shared VPC, the cross-cluster case, has one, and
// nothing in the address tells it apart from a ClusterIP. Short names
// (router, router.ns) aren't either: they resolve through the search path,
// which may be the corporate DNS as well as the cluster's.
func clusterLocalHost(host string) bool {
	h := strings.TrimSuffix(strings.ToLower(host), ".")
	if h == "localhost" || strings.HasSuffix(h, ".localhost") {
		return true
	}
	if ip := net.ParseIP(h); ip != nil {
		return ip.IsLoopback()
	}
	return strings.HasSuffix(h, ".svc") || strings.HasSuffix(h, ".svc.cluster.local")
}

// loadAXTaskTemplate reads an AX Task manifest. Unknown fields are errors, so
// a typo in the template fails at startup rather than on the first incident.
func loadAXTaskTemplate(path string) (*axapi.Task, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading AX task template: %w", err)
	}
	js, err := yaml.YAMLToJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("AX task template %s: %w", path, err)
	}
	var t axapi.Task
	if err := protojson.Unmarshal(js, &t); err != nil {
		return nil, fmt.Errorf("AX task template %s: %w", path, err)
	}
	if t.GetKind() != "" && t.GetKind() != "Task" {
		return nil, fmt.Errorf("AX task template %s: kind is %q, want Task", path, t.GetKind())
	}
	if t.GetSpec() == nil {
		return nil, fmt.Errorf("AX task template %s: spec is required", path)
	}
	return &t, nil
}
