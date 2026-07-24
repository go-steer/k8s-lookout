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
	"reflect"
	"strings"
	"testing"
)

// feedLines pushes raw (timestamp-less) lines through a fresh
// engine stream and returns it.
func feedLines(t *testing.T, lines ...string) *engine {
	t.Helper()
	eng := newEngine(true)
	s := eng.stream("prod", "api-1")
	for _, l := range lines {
		s.add(l)
	}
	s.close()
	return eng
}

func TestGoPanicCollapsesToTopFrames(t *testing.T) {
	lines := []string{
		"INFO starting worker",
		"panic: send on closed channel",
		"",
		"goroutine 7 [running]:",
		"main.(*worker).flush(0xc0001a2000)",
		"\t/app/worker.go:88 +0x9c",
		"main.(*worker).loop(0xc0001a2000)",
		"\t/app/worker.go:61 +0x45",
		"main.startWorkers.func1()",
		"\t/app/main.go:31 +0x28",
		"created by main.startWorkers",
		"\t/app/main.go:29 +0x7a",
		"goroutine 1 [chan receive]:",
		"main.main()",
		"\t/app/main.go:22 +0x118",
		"INFO next run scheduled",
	}
	eng := feedLines(t, lines...)
	if len(eng.stackOrder) != 1 {
		t.Fatalf("stack clusters = %d, want 1", len(eng.stackOrder))
	}
	c := eng.stackOrder[0]
	if c.lang != "go" || c.count != 1 {
		t.Errorf("lang=%q count=%d, want go/1", c.lang, c.count)
	}
	wantFrames := []string{"main.(*worker).flush", "main.(*worker).loop", "main.startWorkers.func1", "main.main"}
	if !reflect.DeepEqual(c.frames, wantFrames) {
		t.Errorf("frames = %v, want %v", c.frames, wantFrames)
	}
	// The two INFO lines around the dump cluster normally.
	if len(eng.tree.all) != 2 {
		t.Errorf("template clusters = %d, want 2 (%v)", len(eng.tree.all), templates(eng.tree))
	}
}

func TestRepeatedPanicsClusterByFrames(t *testing.T) {
	block := func(addr string) []string {
		return []string{
			"panic: runtime error: invalid memory address " + addr,
			"goroutine 12 [running]:",
			"svc/cache.(*Store).Get(0x0)",
			"\t/app/cache/store.go:51 +0x18",
			"svc/api.handleGet({0x8f3b20, 0xc00012a000})",
			"\t/app/api/get.go:33 +0x92",
		}
	}
	var lines []string
	lines = append(lines, block("0x18")...)
	lines = append(lines, "INFO recovered restart")
	lines = append(lines, block("0x40")...)
	eng := feedLines(t, lines...)
	if len(eng.stackOrder) != 1 {
		t.Fatalf("stack clusters = %d, want 1 (same top frames must merge)", len(eng.stackOrder))
	}
	c := eng.stackOrder[0]
	if c.count != 2 {
		t.Errorf("count = %d, want 2", c.count)
	}
	// The differing addresses pre-mask, so the head template is shared.
	if !strings.Contains(c.template, wildcard) {
		t.Errorf("head template %q should mask the address", c.template)
	}
}

func TestJavaStackDetectionAndFrames(t *testing.T) {
	lines := []string{
		"2026-07-24 09:15:04 INFO charge requested",
		"java.net.SocketTimeoutException: Read timed out",
		"\tat java.base/java.net.SocketInputStream.socketRead0(Native Method)",
		"\tat com.shop.billing.ChargeClient.call(ChargeClient.java:88)",
		"\tat com.shop.billing.ChargeService.charge(ChargeService.java:41)",
		"\tat com.shop.api.OrderController.submit(OrderController.java:133)",
		"\tat jakarta.servlet.http.HttpServlet.service(HttpServlet.java:614)",
		"\tat org.eclipse.jetty.server.Server.handle(Server.java:516)",
		"Caused by: java.io.IOException: connection reset",
		"\tat java.base/sun.nio.ch.SocketChannelImpl.read(SocketChannelImpl.java:394)",
		"\t... 12 more",
		"INFO retrying charge",
	}
	eng := feedLines(t, lines...)
	if len(eng.stackOrder) != 1 {
		t.Fatalf("stack clusters = %d, want 1", len(eng.stackOrder))
	}
	c := eng.stackOrder[0]
	if c.lang != "java" {
		t.Errorf("lang = %q, want java", c.lang)
	}
	// Top-5 cap: the sixth+ frame lines are consumed but not kept.
	wantFrames := []string{
		"java.base/java.net.SocketInputStream.socketRead0",
		"com.shop.billing.ChargeClient.call",
		"com.shop.billing.ChargeService.charge",
		"com.shop.api.OrderController.submit",
		"jakarta.servlet.http.HttpServlet.service",
	}
	if !reflect.DeepEqual(c.frames, wantFrames) {
		t.Errorf("frames = %v, want %v", c.frames, wantFrames)
	}
}

func TestJavaHeaderWithoutFramesIsReleasedAsNormalLine(t *testing.T) {
	eng := feedLines(t,
		"ERROR TimeoutException: upstream slow", // header-shaped but no `at` follows
		"INFO continuing",
	)
	if len(eng.stackOrder) != 0 {
		t.Fatalf("stack clusters = %d, want 0", len(eng.stackOrder))
	}
	if len(eng.tree.all) != 2 {
		t.Errorf("template clusters = %d, want 2 (%v)", len(eng.tree.all), templates(eng.tree))
	}
}

func TestPythonTracebackTopFramesInnermostFirst(t *testing.T) {
	lines := []string{
		"Traceback (most recent call last):",
		`  File "/app/main.py", line 90, in <module>`,
		"    run()",
		`  File "/app/svc/web.py", line 44, in run`,
		"    handle(req)",
		`  File "/app/svc/handlers.py", line 71, in handle`,
		"    charge(order)",
		`  File "/app/svc/billing.py", line 12, in charge`,
		"    conn.execute(q)",
		`  File "/app/svc/db.py", line 41, in execute`,
		"    raise OperationalError(msg)",
		"psycopg2.OperationalError: connection timed out",
		"INFO worker restarting",
	}
	eng := feedLines(t, lines...)
	if len(eng.stackOrder) != 1 {
		t.Fatalf("stack clusters = %d, want 1", len(eng.stackOrder))
	}
	c := eng.stackOrder[0]
	if c.lang != "python" {
		t.Errorf("lang = %q, want python", c.lang)
	}
	// Innermost (deepest) frame first, capped at 5 of the 5 present.
	wantFrames := []string{
		"db.py:41:execute",
		"billing.py:12:charge",
		"handlers.py:71:handle",
		"web.py:44:run",
		"main.py:90:<module>",
	}
	if !reflect.DeepEqual(c.frames, wantFrames) {
		t.Errorf("frames = %v, want %v", c.frames, wantFrames)
	}
	// The head is the exception line, not the Traceback marker.
	if !strings.HasPrefix(c.sample, "psycopg2.OperationalError") {
		t.Errorf("sample = %q, want the exception line", c.sample)
	}
	if len(eng.tree.all) != 1 {
		t.Errorf("template clusters = %d, want 1 (%v)", len(eng.tree.all), templates(eng.tree))
	}
}

func TestStreamEndFlushesOpenTrace(t *testing.T) {
	eng := feedLines(t,
		"panic: deadline exceeded",
		"goroutine 1 [running]:",
		"main.run()",
		"\t/app/main.go:10 +0x20",
		// stream ends mid-dump — close() must still complete the trace
	)
	if len(eng.stackOrder) != 1 {
		t.Fatalf("stack clusters = %d, want 1", len(eng.stackOrder))
	}
	if got := eng.stackOrder[0].frames; !reflect.DeepEqual(got, []string{"main.run"}) {
		t.Errorf("frames = %v, want [main.run]", got)
	}
}
