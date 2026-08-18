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

package netprobe

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// fakeResolver answers from a table; unknown names are NXDOMAIN.
type fakeResolver struct {
	hosts map[string][]string
	errs  map[string]error
}

func (r fakeResolver) LookupHost(_ context.Context, host string) ([]string, error) {
	if err, ok := r.errs[host]; ok {
		return nil, err
	}
	if ips, ok := r.hosts[host]; ok {
		return ips, nil
	}
	return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
}

// fakeDeps pins the clock (each Now call advances 100ms, so every
// latency renders as exactly 100ms) and the resolver.
func fakeDeps(r Resolver) Deps {
	return Deps{
		Resolver: r,
		Now:      checktest.FakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), 100*time.Millisecond),
	}
}

func testResolver() fakeResolver {
	return fakeResolver{
		hosts: map[string][]string{
			"api.prod.svc.cluster.local": {"10.8.0.12", "10.8.0.7"},
		},
		errs: map[string]error{
			"slow.example": &net.DNSError{Err: "i/o timeout", Name: "slow.example", IsTimeout: true},
		},
	}
}

// record is one parsed logfmt finding line.
type record map[string]string

func runRecords(t *testing.T, deps Deps, args ...string) ([]record, record) {
	t.Helper()
	res := checktest.Run(t, New(deps), args...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	var recs []record
	for _, line := range lines[:len(lines)-1] {
		recs = append(recs, parseLogfmtLine(t, line))
	}
	return recs, parseLogfmtLine(t, lines[len(lines)-1])
}

func parseLogfmtLine(t *testing.T, line string) record {
	t.Helper()
	rec := record{}
	rest := line
	for rest != "" {
		eq := strings.IndexByte(rest, '=')
		if eq <= 0 {
			t.Fatalf("bad logfmt line %q", line)
		}
		key := rest[:eq]
		rest = rest[eq+1:]
		var val string
		if strings.HasPrefix(rest, `"`) {
			q, err := strconv.QuotedPrefix(rest)
			if err != nil {
				t.Fatalf("bad quoted value in %q: %v", line, err)
			}
			val, err = strconv.Unquote(q)
			if err != nil {
				t.Fatal(err)
			}
			rest = rest[len(q):]
		} else if sp := strings.IndexByte(rest, ' '); sp >= 0 {
			val, rest = rest[:sp], rest[sp:]
		} else {
			val, rest = rest, ""
		}
		rec[key] = val
		rest = strings.TrimPrefix(rest, " ")
	}
	return rec
}

// --- registration + contract ------------------------------------------------

func TestRegisteredInDefaultRegistry(t *testing.T) {
	c, ok := checks.Lookup("net probe")
	if !ok {
		t.Fatal("net probe is not registered in the default registry")
	}
	if c.MCPName != "k8s_net_probe" {
		t.Errorf("MCP name = %q, want k8s_net_probe", c.MCPName)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("registered command invalid: %v", err)
	}
}

func TestContract(t *testing.T) {
	checktest.VerifyContract(t, New(fakeDeps(testResolver())),
		"--dns=api.prod.svc.cluster.local,missing.prod.svc")
}

// --- usage errors -----------------------------------------------------------

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		nil, // nothing to probe
		{"--dns=x", "--workload=Deployment/prod/api"},
		{"--dns=x", "--namespace=prod"},
		{"--dns=x", "-A"},
		{"--dns=x", "--since=5m"},
		{"--tcp=no-port"},
		{"--http=ftp://files.example"},
		{"--http=/relative/path"},
		{"--dns=x", "--probe-timeout=0s"},
	} {
		res := checktest.Run(t, New(fakeDeps(testResolver())), args...)
		if res.Code != emit.ExitUsage {
			t.Errorf("args %v: exit = %d, want %d (stderr: %s)", args, res.Code, emit.ExitUsage, res.Stderr)
		}
		if res.Stdout != "" {
			t.Errorf("args %v: usage error must keep stdout clean, got %q", args, res.Stdout)
		}
	}
}

// --- DNS --------------------------------------------------------------------

func TestDNS(t *testing.T) {
	recs, summary := runRecords(t, fakeDeps(testResolver()),
		"--dns=api.prod.svc.cluster.local,missing.prod.svc,slow.example")
	if summary["scanned"] != "3" {
		t.Errorf("scanned = %s, want 3 (one per probe)", summary["scanned"])
	}
	if len(recs) != 3 {
		t.Fatalf("findings = %d, want 3", len(recs))
	}
	ok, nx, to := recs[0], recs[1], recs[2]

	if ok["kind"] != "probe.dns" || ok["severity"] != "info" {
		t.Errorf("success envelope = %v", ok)
	}
	if ok["ips"] != "10.8.0.12,10.8.0.7" {
		t.Errorf("ips = %q, want sorted 10.8.0.12,10.8.0.7", ok["ips"])
	}
	if ok["latency"] != "100ms" {
		t.Errorf("latency = %q, want the fake clock's 100ms", ok["latency"])
	}

	if nx["error_class"] != "nxdomain" || nx["severity"] != "critical" {
		t.Errorf("nxdomain must be a critical finding, got %v", nx)
	}
	if to["error_class"] != "timeout" || to["severity"] != "warning" {
		t.Errorf("dns timeout must be a warning finding, got %v", to)
	}
}

// --- TCP --------------------------------------------------------------------

func TestTCPConnect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	recs, _ := runRecords(t, fakeDeps(testResolver()), "--tcp="+ln.Addr().String())
	if len(recs) != 1 {
		t.Fatalf("findings = %d, want 1", len(recs))
	}
	f := recs[0]
	if f["kind"] != "probe.tcp" || f["severity"] != "info" || f["name"] != ln.Addr().String() {
		t.Errorf("tcp success = %v", f)
	}
	if f["latency"] == "" {
		t.Error("tcp success must carry latency")
	}
}

func TestTCPRefused(t *testing.T) {
	// Grab a port that is then closed again: connecting must be
	// refused (nothing listens).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	recs, _ := runRecords(t, fakeDeps(testResolver()), "--tcp="+addr)
	f := recs[0]
	if f["error_class"] != "refused" || f["severity"] != "critical" {
		t.Errorf("refused connect = %v, want error_class=refused severity=critical", f)
	}
}

func TestTCPTimeout(t *testing.T) {
	deps := fakeDeps(testResolver())
	deps.Dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return nil, context.DeadlineExceeded
	}
	recs, _ := runRecords(t, deps, "--tcp=10.255.255.1:443", "--probe-timeout=50ms")
	f := recs[0]
	if f["error_class"] != "timeout" || f["severity"] != "warning" {
		t.Errorf("timeout = %v, want error_class=timeout severity=warning", f)
	}
}

// --- HTTP -------------------------------------------------------------------

func httpStatusServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("probe used %s, want GET only", r.Method)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestHTTPStatusClasses(t *testing.T) {
	ok := httpStatusServer(t, http.StatusOK, "healthy")
	notFound := httpStatusServer(t, http.StatusNotFound, "nope")
	boom := httpStatusServer(t, http.StatusServiceUnavailable, "overloaded")

	recs, summary := runRecords(t, fakeDeps(testResolver()),
		"--http="+ok.URL+","+notFound.URL+","+boom.URL)
	if summary["scanned"] != "3" {
		t.Errorf("scanned = %s, want 3", summary["scanned"])
	}
	okF, nfF, boomF := recs[0], recs[1], recs[2]

	if okF["kind"] != "probe.http" || okF["severity"] != "info" || okF["status"] != "200" {
		t.Errorf("200 = %v", okF)
	}
	if okF["content_length"] != "7" {
		t.Errorf("content_length = %q, want 7 (len(\"healthy\"))", okF["content_length"])
	}
	if _, leaked := okF["body"]; leaked || strings.Contains(okF["message"], "healthy") {
		t.Errorf("response body leaked into the finding: %v", okF)
	}
	if nfF["status"] != "404" || nfF["severity"] != "warning" || nfF["error_class"] != "http_4xx" {
		t.Errorf("404 = %v, want warning/http_4xx", nfF)
	}
	if boomF["status"] != "503" || boomF["severity"] != "critical" || boomF["error_class"] != "http_5xx" {
		t.Errorf("503 = %v, want critical/http_5xx", boomF)
	}
}

func TestHTTPRedirectIsReportedNotFollowed(t *testing.T) {
	followed := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		followed = true
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirect.Close()

	recs, _ := runRecords(t, fakeDeps(testResolver()), "--http="+redirect.URL)
	f := recs[0]
	if f["status"] != "302" || f["severity"] != "info" {
		t.Errorf("redirect = %v, want the 302 itself", f)
	}
	if followed {
		t.Error("probe followed the redirect; it must report the 3xx instead")
	}
}

func TestHTTPCertErrorIsDistinct(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	// Default transport does NOT trust httptest's self-signed CA —
	// exactly the failure under test.
	recs, _ := runRecords(t, fakeDeps(testResolver()), "--http="+srv.URL)
	f := recs[0]
	if f["error_class"] != "cert" || f["severity"] != "critical" {
		t.Errorf("self-signed TLS = %v, want error_class=cert severity=critical", f)
	}
	if f["status"] != "" {
		t.Errorf("cert failure has no HTTP status, got %q", f["status"])
	}
}

func TestHTTPRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	recs, _ := runRecords(t, fakeDeps(testResolver()), "--http=http://"+addr+"/healthz")
	f := recs[0]
	if f["error_class"] != "refused" || f["severity"] != "critical" {
		t.Errorf("refused GET = %v, want error_class=refused severity=critical", f)
	}
}

// --- mixed + golden ---------------------------------------------------------

// TestGolden pins a fully deterministic multi-probe run: fake
// resolver + fake clock, DNS success and both DNS failure classes.
func TestGolden(t *testing.T) {
	res := checktest.Run(t, New(fakeDeps(testResolver())),
		"--dns=api.prod.svc.cluster.local,missing.prod.svc,slow.example")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/probe-dns.golden", res.Stdout)
}

// TestProbeOrderIsInputOrder: findings stream dns → tcp → http, each
// group in flag order, so output is deterministic for agents diffing
// runs.
func TestProbeOrderIsInputOrder(t *testing.T) {
	srv := httpStatusServer(t, http.StatusOK, "")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	recs, _ := runRecords(t, fakeDeps(testResolver()),
		"--http="+srv.URL,
		"--tcp="+ln.Addr().String(),
		"--dns=api.prod.svc.cluster.local,missing.prod.svc")
	kinds := make([]string, len(recs))
	for i, r := range recs {
		kinds[i] = r["kind"]
	}
	want := "probe.dns,probe.dns,probe.tcp,probe.http"
	if got := strings.Join(kinds, ","); got != want {
		t.Errorf("probe order = %s, want %s", got, want)
	}
}
