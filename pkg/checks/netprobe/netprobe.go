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

// Package netprobe implements `lookout net probe` (DESIGN.md §5):
// active DNS/TCP/HTTP checks for hypothesis CONFIRMATION — "is this
// Service name resolvable", "does this port accept connections",
// "what does this endpoint actually return" — bending read-only in
// letter, not spirit: packets are sent, but nothing in the cluster
// is mutated and no Kubernetes API is touched at all.
//
// Vantage point: the probes run from WHEREVER this lookout process
// runs. Inside a pod, "from inside the cluster" semantics (cluster
// DNS, Service VIPs, NetworkPolicies as the pod experiences them)
// come for free; on an operator's laptop the answers describe the
// laptop's network, which is a different — sometimes exactly wanted,
// sometimes misleading — vantage. No pod is ever spawned to probe
// from; if the in-cluster view is needed, run lookout in-cluster
// (the MCP surface of a deployed sentinel is the usual route).
//
// Because a probe target is not a Kubernetes object, the §4.2
// scoping flags are meaningless here and are REJECTED as usage
// errors rather than silently ignored — a caller passing
// --namespace almost certainly believes it changes the vantage
// point, and it does not.
package netprobe

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func init() {
	checks.Register(New(Deps{}))
}

// Resolver is the DNS seam: net.DefaultResolver in production, a
// fake in tests (§13 — no live DNS in CI assertions beyond
// localhost).
type Resolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// Deps are the injected seams. Zero values give production behavior.
type Deps struct {
	// Resolver defaults to net.DefaultResolver.
	Resolver Resolver
	// Dial defaults to a net.Dialer's DialContext.
	Dial func(ctx context.Context, network, addr string) (net.Conn, error)
	// Transport defaults to http.DefaultTransport. Injected so tests
	// exercise TLS failure classes against httptest servers.
	Transport http.RoundTripper
	// Now defaults to time.Now; the latency fields are measured with
	// it, so the checktest fake clock makes them deterministic.
	Now func() time.Time
}

func (d Deps) resolver() Resolver {
	if d.Resolver != nil {
		return d.Resolver
	}
	return net.DefaultResolver
}

func (d Deps) dial() func(ctx context.Context, network, addr string) (net.Conn, error) {
	if d.Dial != nil {
		return d.Dial
	}
	return (&net.Dialer{}).DialContext
}

func (d Deps) transport() http.RoundTripper {
	if d.Transport != nil {
		return d.Transport
	}
	return http.DefaultTransport
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// New builds the `net probe` command around deps.
func New(deps Deps) checks.Command {
	return checks.Command{
		Name:    "net probe",
		MCPName: "k8s_net_probe",
		Summary: "Actively confirm a network hypothesis — resolve DNS names, open TCP connections, GET HTTP(S) URLs — from wherever lookout runs (in a pod = the in-cluster view); zero cluster mutation, no pods spawned.",
		Flags: []emit.FlagSpec{
			{Name: "dns", Type: emit.FlagString, Default: "",
				Help: "comma-separated names to resolve (e.g. api.prod.svc.cluster.local,db.example.com)"},
			{Name: "tcp", Type: emit.FlagString, Default: "",
				Help: "comma-separated host:port endpoints to connect to (e.g. api.prod.svc:8080,10.0.0.5:5432)"},
			{Name: "http", Type: emit.FlagString, Default: "",
				Help: "comma-separated http(s) URLs to GET; redirects are reported (3xx), not followed, and response bodies are never read into findings"},
			{Name: "probe-timeout", Type: emit.FlagDuration, Default: "5s",
				Help: "per-probe timeout; raise --timeout too when probing many slow targets (it caps the whole invocation)"},
		},
		Output: []checks.OutputField{
			{Name: "ips", Doc: "probe.dns: resolved addresses, sorted, comma-separated"},
			{Name: "latency", Doc: "how long the probe took: DNS resolution / TCP connect / full HTTP exchange"},
			{Name: "status", Doc: "probe.http: HTTP status code of the (unfollowed) response"},
			{Name: "content_length", Doc: "probe.http: Content-Length the server declared (body is discarded unread; omitted when unknown)"},
			{Name: "error_class", Doc: "failed probes: nxdomain|timeout|refused|unreachable|reset|cert|http_4xx|http_5xx|error"},
		},
		Examples: []string{
			"lookout net probe --dns=api.prod.svc.cluster.local",
			"lookout net probe --tcp=db.prod.svc:5432 --probe-timeout=2s",
			"lookout net probe --http=https://api.prod.svc/healthz --format=json",
			"lookout net probe --dns=api.prod.svc --tcp=api.prod.svc:8080 --http=http://api.prod.svc:8080/readyz",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return run(ctx, deps, inv)
		},
	}
}

func run(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	// The §4.2 scoping flags do not apply to an active probe — see
	// the package comment. Rejecting beats ignoring.
	switch {
	case !inv.Scope.Workload.IsZero():
		return 0, emit.UsageErrorf("--workload does not apply: net probe probes network targets from where lookout runs, not Kubernetes objects (resolve the workload's Service and pass --dns/--tcp/--http)")
	case inv.Scope.Namespace != "" || inv.Scope.AllNamespaces:
		return 0, emit.UsageErrorf("--namespace/-A do not apply: net probe is not namespace-scoped — the vantage point is wherever lookout runs")
	case inv.Scope.Since != 0:
		return 0, emit.UsageErrorf("--since does not apply: net probe measures now, not a window")
	}
	dns := splitTargets(inv.Flags.String("dns"))
	tcp := splitTargets(inv.Flags.String("tcp"))
	httpTargets := splitTargets(inv.Flags.String("http"))
	if len(dns)+len(tcp)+len(httpTargets) == 0 {
		return 0, emit.UsageErrorf("nothing to probe: pass at least one of --dns=<name,...>, --tcp=<host:port,...>, --http=<url,...>")
	}
	for _, t := range tcp {
		if _, _, err := net.SplitHostPort(t); err != nil {
			return 0, emit.UsageErrorf("--tcp target %q is not host:port: %v", t, err)
		}
	}
	for _, t := range httpTargets {
		u, err := url.Parse(t)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return 0, emit.UsageErrorf("--http target %q is not an absolute http(s) URL", t)
		}
	}
	timeout := inv.Flags.Duration("probe-timeout")
	if timeout <= 0 {
		return 0, emit.UsageErrorf("--probe-timeout must be positive, got %s", timeout)
	}

	scanned := 0
	emitOne := func(f emit.Finding) error {
		scanned++
		return inv.Out.Emit(f)
	}
	for _, name := range dns {
		if err := emitOne(probeDNS(ctx, deps, name, timeout)); err != nil {
			return scanned, err
		}
	}
	for _, addr := range tcp {
		if err := emitOne(probeTCP(ctx, deps, addr, timeout)); err != nil {
			return scanned, err
		}
	}
	for _, target := range httpTargets {
		if err := emitOne(probeHTTP(ctx, deps, target, timeout)); err != nil {
			return scanned, err
		}
	}
	return scanned, nil
}

func splitTargets(s string) []string {
	var out []string
	for _, t := range strings.Split(s, ",") {
		if t = strings.TrimSpace(t); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func probeDNS(ctx context.Context, deps Deps, name string, timeout time.Duration) emit.Finding {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := deps.now()
	ips, err := deps.resolver().LookupHost(ctx, name)
	latency := deps.now().Sub(start)
	if err != nil {
		return failure("probe.dns", name, classifyDNS(err), err, latency)
	}
	sortedIPs := append([]string(nil), ips...)
	sort.Strings(sortedIPs)
	return emit.Finding{
		Kind:     "probe.dns",
		Severity: emit.SeverityInfo,
		Name:     name,
		Message:  fmt.Sprintf("resolved to %d address(es)", len(sortedIPs)),
		Details: []emit.Field{
			{Key: "ips", Value: strings.Join(sortedIPs, ",")},
			{Key: "latency", Value: fmtLatency(latency)},
		},
	}
}

func probeTCP(ctx context.Context, deps Deps, addr string, timeout time.Duration) emit.Finding {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := deps.now()
	conn, err := deps.dial()(ctx, "tcp", addr)
	latency := deps.now().Sub(start)
	if err != nil {
		return failure("probe.tcp", addr, classifyNet(err), err, latency)
	}
	_ = conn.Close()
	return emit.Finding{
		Kind:     "probe.tcp",
		Severity: emit.SeverityInfo,
		Name:     addr,
		Message:  "connection accepted",
		Details:  []emit.Field{{Key: "latency", Value: fmtLatency(latency)}},
	}
}

func probeHTTP(ctx context.Context, deps Deps, target string, timeout time.Duration) emit.Finding {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client := &http.Client{
		Transport: deps.transport(),
		// A redirect IS the answer being probed for; report the 3xx
		// rather than chasing it to some other server's status.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return failure("probe.http", target, classError, err, 0)
	}
	start := deps.now()
	resp, err := client.Do(req)
	latency := deps.now().Sub(start)
	if err != nil {
		return failure("probe.http", target, classifyHTTP(err), err, latency)
	}
	// The body is never read: status + headers are the finding; no
	// payload bytes cross into any output surface.
	_ = resp.Body.Close()

	f := emit.Finding{
		Kind:     "probe.http",
		Severity: emit.SeverityInfo,
		Name:     target,
		Message:  "GET " + resp.Status,
		Details: []emit.Field{
			{Key: "status", Value: strconv.Itoa(resp.StatusCode)},
			{Key: "latency", Value: fmtLatency(latency)},
		},
	}
	if resp.ContentLength >= 0 {
		f.Details = append(f.Details, emit.Field{Key: "content_length", Value: strconv.FormatInt(resp.ContentLength, 10)})
	}
	switch {
	case resp.StatusCode >= 500:
		f.Severity = emit.SeverityCritical
		f.Details = append(f.Details, emit.Field{Key: "error_class", Value: classHTTP5xx})
	case resp.StatusCode >= 400:
		f.Severity = emit.SeverityWarning
		f.Details = append(f.Details, emit.Field{Key: "error_class", Value: classHTTP4xx})
	}
	return f
}

// Error classes. Severity policy, applied uniformly: a DEFINITIVE
// negative answer (the name does not exist, the peer refused/reset,
// the route is absent, the certificate fails verification, the
// server answered 5xx) is critical — the hypothesis is confirmed
// broken. An INDETERMINATE outcome (timeout: could be a
// NetworkPolicy, an overloaded server, or this vantage point; 4xx:
// the endpoint is reachable and serving, the request itself was
// turned away) is warning.
const (
	classNXDomain    = "nxdomain"
	classTimeout     = "timeout"
	classRefused     = "refused"
	classUnreachable = "unreachable"
	classReset       = "reset"
	classCert        = "cert"
	classHTTP4xx     = "http_4xx"
	classHTTP5xx     = "http_5xx"
	classError       = "error"
)

func classSeverity(class string) string {
	switch class {
	case classTimeout, classHTTP4xx:
		return emit.SeverityWarning
	default:
		return emit.SeverityCritical
	}
}

func failure(kind, name, class string, err error, latency time.Duration) emit.Finding {
	f := emit.Finding{
		Kind:     kind,
		Severity: classSeverity(class),
		Name:     name,
		Message:  err.Error(),
		Details:  []emit.Field{{Key: "error_class", Value: class}},
	}
	if latency > 0 {
		f.Details = append(f.Details, emit.Field{Key: "latency", Value: fmtLatency(latency)})
	}
	return f
}

func classifyDNS(err error) string {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		if dnsErr.IsNotFound {
			return classNXDomain
		}
		if dnsErr.IsTimeout {
			return classTimeout
		}
	}
	if isTimeout(err) {
		return classTimeout
	}
	return classError
}

func classifyNet(err error) string {
	switch {
	case isTimeout(err):
		return classTimeout
	case errors.Is(err, syscall.ECONNREFUSED):
		return classRefused
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return classReset
	case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return classUnreachable
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return classifyDNS(err)
	}
	return classError
}

// classifyHTTP distinguishes TLS certificate failures (their own
// class — a hypothesis about certs is common enough to deserve a
// machine-matchable answer) before falling back to the transport
// classes.
func classifyHTTP(err error) string {
	var certVerify *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameErr x509.HostnameError
	var certInvalid x509.CertificateInvalidError
	var recordHeader tls.RecordHeaderError
	switch {
	case errors.As(err, &certVerify),
		errors.As(err, &unknownAuthority),
		errors.As(err, &hostnameErr),
		errors.As(err, &certInvalid):
		return classCert
	case errors.As(err, &recordHeader):
		// https:// against a plaintext port — a handshake-level
		// mismatch, reported as reset (the TLS layer aborted).
		return classReset
	}
	return classifyNet(err)
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// fmtLatency truncates to the millisecond: sub-millisecond precision
// is noise to an agent reader (same rationale as the summary line's
// elapsed).
func fmtLatency(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	return d.Truncate(time.Millisecond).String()
}
