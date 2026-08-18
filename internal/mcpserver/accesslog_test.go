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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// logEpoch is the instant every fake clock in this file starts from.
var logEpoch = time.Date(2026, 8, 18, 14, 3, 21, 0, time.UTC)

// stepClock advances a fixed step per read, so a recorded line's ts
// and dur are exact rather than "some plausible number". Each tool
// call reads twice: once when it arrives, once when Record measures.
func stepClock(step time.Duration) func() time.Time {
	var reads int
	return func() time.Time {
		t := logEpoch.Add(time.Duration(reads) * step)
		reads++
		return t
	}
}

// testLog returns an AccessLog over a buffer with a deterministic
// clock, plus the buffer.
func testLog(t *testing.T, step time.Duration) (*AccessLog, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	l := NewAccessLog(&buf)
	l.clock = stepClock(step)
	return l, &buf
}

// records splits a log buffer into parsed key=value maps. The fields
// this log writes never need quoting (tool names are [a-z0-9_],
// everything else is a number or an RFC3339 stamp), so a plain split
// is the whole parser — and a value that ever did need quoting would
// show up here as a broken field, which is the point.
func records(t *testing.T, buf *bytes.Buffer) []map[string]string {
	t.Helper()
	var out []map[string]string
	for _, line := range strings.Split(strings.TrimRight(buf.String(), "\n"), "\n") {
		if line == "" {
			continue
		}
		rec := map[string]string{}
		for _, tok := range strings.Split(line, " ") {
			k, v, ok := strings.Cut(tok, "=")
			if !ok {
				t.Fatalf("malformed access-log field %q in line %q", tok, line)
			}
			rec[k] = v
		}
		out = append(out, rec)
	}
	return out
}

// TestAccessLogRecordsEveryOutcome drives all four handler exits over
// a real client session: success, runtime failure, a usage error from
// inside emit.Run, and an argument the schema layer rejects before
// emit.Run ever runs. The last one is the one an early return would
// have made invisible.
func TestAccessLogRecordsEveryOutcome(t *testing.T) {
	log, buf := testLog(t, 5*time.Millisecond)
	cs := connect(t, New(testRegistry(t), "test", WithAccessLog(log)))

	if _, err := callTool(t, cs, "k8s_triage_demo", map[string]any{"count": 1}); err != nil {
		t.Fatalf("tools/call: %v", err)
	}
	if _, err := callTool(t, cs, "k8s_triage_demo", map[string]any{"fail": true}); err != nil {
		t.Fatalf("a runtime failure must not be a protocol error: %v", err)
	}
	if _, err := callTool(t, cs, "k8s_triage_demo", map[string]any{"since": "-5s"}); err == nil {
		t.Fatal("expected a usage error from the shared flag parser")
	}
	if _, err := callTool(t, cs, "k8s_triage_demo", map[string]any{"bogus": 1}); err == nil {
		t.Fatal("expected an unknown argument to be rejected")
	}

	got := records(t, buf)
	wantExit := []string{"0", "1", "2", "2"}
	if len(got) != len(wantExit) {
		t.Fatalf("logged %d calls, want %d:\n%s", len(got), len(wantExit), buf.String())
	}
	for i, rec := range got {
		if rec["tool"] != "k8s_triage_demo" {
			t.Errorf("line %d: tool = %q, want k8s_triage_demo", i, rec["tool"])
		}
		if rec["exit"] != wantExit[i] {
			t.Errorf("line %d: exit = %q, want %q", i, rec["exit"], wantExit[i])
		}
		// Two clock reads per call, so every call spans one step and
		// the next one starts a step later.
		if want := logEpoch.Add(time.Duration(2*i) * 5 * time.Millisecond).Format(time.RFC3339); rec["ts"] != want {
			t.Errorf("line %d: ts = %q, want %q", i, rec["ts"], want)
		}
		if rec["dur"] != "5ms" {
			t.Errorf("line %d: dur = %q, want 5ms", i, rec["dur"])
		}
		if rec["bytes"] == "" || rec["bytes"] == "0" {
			t.Errorf("line %d: bytes = %q, want the size of the response text", i, rec["bytes"])
		}
	}
}

// TestAccessLogCarriesNeitherArgumentsNorPayload is the guardrail on
// what a line is allowed to say. The §6.5 sanitizer covers the tool
// result; the access log stays out of that business entirely rather
// than becoming a second surface the guarantee has to hold on.
func TestAccessLogCarriesNeitherArgumentsNorPayload(t *testing.T) {
	log, buf := testLog(t, time.Millisecond)
	cs := connect(t, New(testRegistry(t), "test", WithAccessLog(log)))

	const marker = "sensitive-marker-value"
	res, err := callTool(t, cs, "k8s_fake_spec", map[string]any{"target": "Pod/prod/" + marker})
	if err != nil {
		t.Fatalf("tools/call: %v", err)
	}
	// The command echoes its target, so the marker is genuinely in the
	// response this line is describing.
	if !strings.Contains(text(t, res), marker) {
		t.Fatal("test setup: the marker never reached the tool result")
	}
	if strings.Contains(buf.String(), marker) {
		t.Errorf("an argument value reached the access log:\n%s", buf.String())
	}
	if strings.Contains(buf.String(), "kind=") {
		t.Errorf("a finding reached the access log:\n%s", buf.String())
	}
}

// TestAccessLogNilIsSilent covers the default: no --access-log means
// a nil *AccessLog threaded through the same handler code, not a
// second code path.
func TestAccessLogNilIsSilent(t *testing.T) {
	var l *AccessLog
	l.Record("k8s_triage_demo", 0, l.now(), 42) // must not panic

	cs := connect(t, New(testRegistry(t), "test"))
	if _, err := callTool(t, cs, "k8s_triage_demo", map[string]any{"count": 1}); err != nil {
		t.Fatalf("tools/call without an access log: %v", err)
	}
}

// TestOpenAccessLogAppendsAndIsPrivate pins the two file properties an
// operator depends on: a restarted server adds to the record instead
// of erasing it, and the file is not world-readable.
func TestOpenAccessLogAppendsAndIsPrivate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp-access.log")

	for i, tool := range []string{"k8s_first", "k8s_second"} {
		l, closer, err := OpenAccessLog(path)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		l.clock = stepClock(time.Millisecond)
		l.Record(tool, 0, logEpoch, 1)
		if err := closer.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != 2 {
		t.Errorf("access log has %d lines after two runs, want 2:\n%s", lines, raw)
	}
	if !strings.Contains(string(raw), "tool=k8s_first") || !strings.Contains(string(raw), "tool=k8s_second") {
		t.Errorf("the second run did not append:\n%s", raw)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("access log mode = %O, want 600", perm)
	}
}

func TestOpenAccessLogReportsAnUnusablePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "no-such-dir", "mcp-access.log")
	if _, _, err := OpenAccessLog(path); err == nil {
		t.Fatal("expected an error for a path whose directory does not exist")
	} else if !strings.Contains(err.Error(), "--access-log=") {
		t.Errorf("error %q does not name the flag that caused it", err)
	}
}

// TestAccessLogIsConcurrencySafe: the streamable-HTTP transport can
// have several tool calls in flight, and a half-interleaved line is
// worse than no line at all.
func TestAccessLogIsConcurrencySafe(t *testing.T) {
	var buf bytes.Buffer
	log := NewAccessLog(&buf)
	// A stateless clock: stepClock counts reads, which would itself be
	// the race this test is looking for.
	log.clock = func() time.Time { return logEpoch }

	const calls = 64
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Record("k8s_triage_demo", 0, logEpoch, 128)
		}()
	}
	wg.Wait()

	got := records(t, &buf)
	if len(got) != calls {
		t.Fatalf("logged %d lines, want %d", len(got), calls)
	}
	for i, rec := range got {
		if len(rec) != 5 {
			t.Fatalf("line %d has %d fields, want 5 (ts tool exit dur bytes): %v", i, len(rec), rec)
		}
	}
}
