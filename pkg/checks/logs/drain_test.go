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

package logs

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestTokenizeMask(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"handled 200 in 15ms", "handled <*> in <*>"},
		{"user 550e8400-e29b-41d4-a716-446655440000 logged in", "user <*> logged in"},
		{"conn from 10.4.2.19:8443 closed", "conn from <*> closed"},
		{"at 2026-07-24T09:15:04Z retry", "at <*> retry"},
		{"txn 0xDEADBEEF committed", "txn <*> committed"},
		{"id=8231 status=200 path=/api/cart", "id=<*> status=<*> path=/api/cart"},
		{"(0x7f3a2b9014) done.", "(<*>) done."},
		{"read 12.5MiB in 3.4s", "read <*> in <*>"},
		{"cache hit ratio 99.7%", "cache hit ratio <*>"},
		// Words stay words: no digits, no masking.
		{"error reading config facade", "error reading config facade"},
		{"2026/07/24 09:15:04 started", "<*> <*> started"},
	}
	for _, tt := range tests {
		if got := strings.Join(tokenizeMask(tt.in), " "); got != tt.want {
			t.Errorf("tokenizeMask(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestDrainMergesVaryingParameters(t *testing.T) {
	tree := newDrainTree()
	pod := podKey{"prod", "api-1"}
	for i := 0; i < 100; i++ {
		tree.add(pod, entry{text: fmt.Sprintf("GET /api/user/u%dx 200 %dms region europe-west1", i, i)})
	}
	if len(tree.all) != 1 {
		t.Fatalf("got %d clusters, want 1: %v", len(tree.all), templates(tree))
	}
	c := tree.all[0]
	if c.count != 100 {
		t.Errorf("count = %d, want 100", c.count)
	}
	got := strings.Join(c.template, " ")
	// The numeric latency pre-masks; the /api/user/uNx path token is
	// not shape-maskable and must decay to a wildcard via merging.
	want := "GET <*> <*> <*> region europe-west1"
	if got != want {
		t.Errorf("template = %q, want %q", got, want)
	}
}

func TestDrainKeepsDistinctTemplatesApart(t *testing.T) {
	tree := newDrainTree()
	pod := podKey{"prod", "api-1"}
	lines := []string{
		"connection to database established",
		"connection to database lost retrying",
		"cache warmed with 1500 entries",
		"listening on 0.0.0.0:8080",
	}
	for _, l := range lines {
		for i := 0; i < 3; i++ {
			tree.add(pod, entry{text: l})
		}
	}
	if len(tree.all) != 4 {
		t.Fatalf("got %d clusters, want 4: %v", len(tree.all), templates(tree))
	}
	for _, c := range tree.all {
		if c.count != 3 {
			t.Errorf("cluster %q count = %d, want 3", strings.Join(c.template, " "), c.count)
		}
	}
}

func TestDrainTracksPodsSpreadAndTimes(t *testing.T) {
	tree := newDrainTree()
	t0 := time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC)
	for i := 0; i < 4; i++ {
		pod := podKey{"prod", fmt.Sprintf("api-%d", i)}
		tree.add(pod, entry{ts: t0.Add(time.Duration(i) * time.Minute), text: "ERROR upstream timeout after 250ms"})
	}
	if len(tree.all) != 1 {
		t.Fatalf("got %d clusters, want 1", len(tree.all))
	}
	c := tree.all[0]
	if len(c.pods) != 4 {
		t.Errorf("pods = %d, want 4", len(c.pods))
	}
	if !c.first.Equal(t0) || !c.last.Equal(t0.Add(3*time.Minute)) {
		t.Errorf("first/last = %v/%v, want %v/%v", c.first, c.last, t0, t0.Add(3*time.Minute))
	}
	if c.level != levelError {
		t.Errorf("level = %d, want levelError", c.level)
	}
}

func TestGuessLevel(t *testing.T) {
	tests := []struct {
		line string
		want int
	}{
		{`{"level":"error","msg":"boom"}`, levelError},
		{"level=warn msg=slow", levelWarn},
		{"ts=1 severity=INFO event=start", levelInfo},
		{"E0724 09:15:04.123456 1 controller.go:117] sync failed", levelError},
		{"W0724 09:15:04.123456 1 reflector.go:324] watch closed", levelWarn},
		{"[ERROR] failed to connect", levelError},
		{"2026-07-24 WARN slow response", levelWarn},
		{"FATAL: could not bind port", levelFatal},
		{"DEBUG cache miss", levelDebug},
		{"GET /api/error-pages 200", levelUnknown},
		{"plain message with no level", levelUnknown},
	}
	for _, tt := range tests {
		if got := guessLevel(tt.line); got != tt.want {
			t.Errorf("guessLevel(%q) = %d, want %d", tt.line, got, tt.want)
		}
	}
}

func TestSplitTimestamp(t *testing.T) {
	// Kubelet prefix (what the fetcher requests): parsed AND stripped.
	ts, rest := splitTimestamp("2026-07-24T09:15:04.123456789Z ERROR boom")
	if rest != "ERROR boom" {
		t.Errorf("kubelet prefix not stripped: %q", rest)
	}
	if want := time.Date(2026, 7, 24, 9, 15, 4, 123456789, time.UTC); !ts.Equal(want) {
		t.Errorf("ts = %v, want %v", ts, want)
	}
	// App-style prefix: parsed, kept in text (tokenizer masks it).
	ts, rest = splitTimestamp("2026-07-24 09:15:04 starting up")
	if ts.IsZero() || rest != "2026-07-24 09:15:04 starting up" {
		t.Errorf("app prefix: ts=%v rest=%q", ts, rest)
	}
	// No timestamp: zero time, text untouched.
	ts, rest = splitTimestamp("plain line")
	if !ts.IsZero() || rest != "plain line" {
		t.Errorf("plain: ts=%v rest=%q", ts, rest)
	}
}

func TestProbeNoise(t *testing.T) {
	noisy := []string{
		`10.4.0.1 - - [24/Jul/2026:09:15:04 +0000] "GET /healthz HTTP/1.1" 200 2 "-" "kube-probe/1.33"`,
		"GET /readyz 200 0.2ms",
		`{"method":"GET","path":"/health/live","status":200}`,
		"HEAD /-/healthy 200", // Prometheus-style — via kube-probe UA in practice; path form here
		"I0724 09:15:04.1 1 healthz.go:60] served /livez",
	}
	for _, l := range noisy {
		if !isProbeNoise(l) {
			t.Errorf("isProbeNoise(%q) = false, want true", l)
		}
	}
	clean := []string{
		"GET /api/orders/123 200 15ms",
		"GET /health-records/42 200",    // '-records' must not match /health
		"user liveness-checker created", // not a request line... but see pattern 3 scope
		"ERROR probe of upstream failed",
	}
	for _, l := range clean {
		if isProbeNoise(l) {
			t.Errorf("isProbeNoise(%q) = true, want false", l)
		}
	}
}

// TestCompressionClaim is the §5 mission gate at engine level: a 10k
// line corpus (20 rotating INFO templates with varying parameters,
// interleaved probe noise, an error template, a repeated Go panic and
// a repeated Java stack) must distill to a handful of clusters —
// far under the 40-template emission cap.
func TestCompressionClaim(t *testing.T) {
	eng := newEngine(true)
	corpus := syntheticCorpus(10000, 4)
	rawBytes := 0
	for pod, lines := range corpus {
		s := eng.stream("prod", pod)
		for _, l := range lines {
			rawBytes += len(l) + 1
			s.add(l)
		}
		s.close()
	}
	if eng.lines != 10000 {
		t.Fatalf("lines fed = %d, want 10000", eng.lines)
	}
	results := eng.results()
	if len(results) > 40 {
		t.Fatalf("10k lines produced %d clusters, want <= 40", len(results))
	}
	// Exactly the planted populations: 20 INFO templates + 1 error
	// template + 1 go panic + 1 java exception. Probe noise stripped.
	if len(results) != 23 {
		for _, r := range results {
			t.Logf("cluster count=%d stack=%v template=%q", r.count, r.stack, r.template)
		}
		t.Fatalf("got %d clusters, want 23", len(results))
	}
	if eng.probes == 0 {
		t.Error("no probe noise stripped")
	}
	// Error-ish clusters must sort first.
	sawInfo := false
	for _, r := range results {
		if !r.errorish() {
			sawInfo = true
		} else if sawInfo {
			t.Fatal("error-ish cluster sorted after an info cluster")
		}
	}
	t.Logf("compression: %d lines (%d bytes raw) -> %d clusters, probe lines stripped: %d",
		eng.lines, rawBytes, len(results), eng.probes)
}

// syntheticCorpus builds a deterministic nLines-line corpus spread
// over nPods pods: ~72%% INFO traffic over 20 templates, ~15%% probe
// noise, ~3%% errors, plus periodic Go panic and Java stack blocks.
func syntheticCorpus(nLines, nPods int) map[string][]string {
	goPanic := []string{
		"panic: runtime error: invalid memory address or nil pointer dereference",
		"[signal SIGSEGV: segmentation violation code=0x1 addr=0x0 pc=0x6bce54]",
		"goroutine 1 [running]:",
		"main.(*Server).handle(0xc000123456, {0x8f3b20, 0xc00012a000})",
		"\t/app/server.go:42 +0x1a4",
		"main.dispatch({0x8f3b20, 0xc00012a000})",
		"\t/app/dispatch.go:17 +0x64",
		"main.main()",
		"\t/app/main.go:12 +0x38",
		"goroutine 18 [select]:",
		"database/sql.(*DB).connectionOpener(0xc0000b2000, {0x8f4a10, 0xc0000a6040})",
		"\t/usr/local/go/src/database/sql/sql.go:1246 +0x87",
		"created by database/sql.OpenDB",
		"\t/usr/local/go/src/database/sql/sql.go:824 +0x14d",
	}
	javaStack := []string{
		"java.net.SocketTimeoutException: Read timed out",
		"\tat java.base/java.net.SocketInputStream.socketRead0(Native Method)",
		"\tat java.base/java.net.SocketInputStream.read(SocketInputStream.java:168)",
		"\tat com.shop.billing.ChargeClient.call(ChargeClient.java:88)",
		"\tat com.shop.billing.ChargeService.charge(ChargeService.java:41)",
		"\tat com.shop.api.OrderController.submit(OrderController.java:133)",
		"\tat jakarta.servlet.http.HttpServlet.service(HttpServlet.java:614)",
		"Caused by: java.io.IOException: connection reset",
		"\tat java.base/sun.nio.ch.SocketChannelImpl.throwConnectionReset(SocketChannelImpl.java:394)",
		"\t... 12 more",
	}
	out := make(map[string][]string, nPods)
	base := time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC)
	pod := func(i int) string { return fmt.Sprintf("api-%d", i%nPods) }
	n := 0
	emitLine := func(p, text string) {
		out[p] = append(out[p], fmt.Sprintf("%s %s", base.Add(time.Duration(n)*10*time.Millisecond).Format(time.RFC3339Nano), text))
		n++
	}
	for n < nLines {
		switch {
		case n%100 == 97 && n+len(goPanic) < nLines:
			p := pod(n)
			for _, l := range goPanic {
				emitLine(p, l)
			}
		case n%100 == 61 && n+len(javaStack) < nLines:
			p := pod(n)
			for _, l := range javaStack {
				emitLine(p, l)
			}
		case n%7 == 3:
			emitLine(pod(n), fmt.Sprintf(`10.4.0.9 - - "GET /healthz HTTP/1.1" 200 2 "-" "kube-probe/1.33" %d`, n))
		case n%29 == 11:
			emitLine(pod(n), fmt.Sprintf("ERROR charge failed order=%d attempt=%d: upstream timeout after %dms", 1000+n, n%3, 200+n%50))
		default:
			emitLine(pod(n), fmt.Sprintf("INFO %s completed route=/v1/objects/%d status=200 bytes=%d dur=%dms", corpusHandlers[n%len(corpusHandlers)], n, 512+n, n%40))
		}
	}
	return out
}

// corpusHandlers are 20 digit-free handler names, so each yields a
// distinct branch token and therefore a distinct template.
var corpusHandlers = []string{
	"getcart", "addcart", "checkout", "ship", "refund",
	"login", "logout", "search", "browse", "pay",
	"invoice", "email", "export", "import", "sync",
	"prune", "audit", "index", "backup", "restore",
}

func templates(tree *drainTree) []string {
	var out []string
	for _, c := range tree.all {
		out = append(out, strings.Join(c.template, " "))
	}
	return out
}
