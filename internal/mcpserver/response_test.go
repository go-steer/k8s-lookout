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
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/internal/fleet"
	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// floodCommand emits, worst first, 3 critical, 20 warning and --info
// info findings, each numbered by seq=, and notes source=live — the
// shape of a big critical-first answer. newestLast declares it a
// timeline instead.
func floodCommand(newestLast bool) checks.Command {
	return checks.Command{
		Name:       "triage flood",
		MCPName:    "k8s_triage_flood",
		Summary:    "Synthetic oversize answer for the response-limit tests; test scaffolding only.",
		NewestLast: newestLast,
		Flags: []emit.FlagSpec{
			{Name: "info", Type: emit.FlagInt, Default: "200", Help: "info findings to emit after the worse ones"},
			{Name: "huge", Type: emit.FlagInt, Default: "0", Help: "pad the message of the finding a cut keeps first (the first, or for a timeline the last) to this many bytes"},
		},
		Kinds: []checks.KindField{
			checks.Kind("flood.critical", "synthetic", emit.SeverityCritical),
			checks.Kind("flood.warning", "synthetic", emit.SeverityWarning),
			checks.Kind("flood.info", "synthetic", emit.SeverityInfo),
		},
		Output: []checks.OutputField{
			{Name: "seq", Doc: "emission order"},
			{Name: "source", Doc: "summary-line note: always live"},
		},
		Run: func(_ context.Context, inv emit.Invocation) (int, error) {
			n, hugeAt := 0, 0
			if newestLast {
				hugeAt = 23 + inv.Flags.Int("info") - 1
			}
			emitN := func(count int, kind, severity string) error {
				for range count {
					msg := "a synthetic finding with a message long enough to weigh something"
					if n == hugeAt && inv.Flags.Int("huge") > 0 {
						msg = strings.Repeat("x", inv.Flags.Int("huge"))
					}
					if err := inv.Out.Emit(emit.Finding{
						Kind: kind, Severity: severity, Namespace: "prod", KindOfObject: "Pod",
						Name: "api-" + strconv.Itoa(n), Reason: "Synthetic", Message: msg,
						Details: []emit.Field{{Key: "seq", Value: strconv.Itoa(n)}},
					}); err != nil {
						return err
					}
					n++
				}
				return nil
			}
			if err := emitN(3, "flood.critical", emit.SeverityCritical); err != nil {
				return 0, err
			}
			if err := emitN(20, "flood.warning", emit.SeverityWarning); err != nil {
				return 0, err
			}
			if err := emitN(inv.Flags.Int("info"), "flood.info", emit.SeverityInfo); err != nil {
				return 0, err
			}
			return n + 7, inv.Out.Note("source", "live")
		},
	}
}

// stream runs c the way the MCP handler does and returns its stdout.
func stream(t *testing.T, c checks.Command, args ...string) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	rc := c.RunConfig(&stdout, &stderr)
	rc.Now = checktest.FakeClock(time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC), 1200*time.Millisecond)
	if code := emit.Run(t.Context(), rc, args); code != emit.ExitData {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	return stdout.String()
}

// splitStream returns a stream's finding lines (newline included) and
// its summary line.
func splitStream(s string) ([]string, string) {
	lines := strings.SplitAfter(s, "\n")
	lines = lines[:len(lines)-1]
	return lines[:len(lines)-1], lines[len(lines)-1]
}

// summaryFields reads a summary line in either format into a map.
func summaryFields(t *testing.T, line string) map[string]string {
	t.Helper()
	s, ok := parseSummary(line)
	if !ok {
		t.Fatalf("not a summary line: %q", line)
	}
	out := map[string]string{}
	for _, p := range s.pairs {
		out[p.key] = p.value
	}
	return out
}

func summaryKeys(t *testing.T, line string) []string {
	t.Helper()
	s, ok := parseSummary(line)
	if !ok {
		t.Fatalf("not a summary line: %q", line)
	}
	var keys []string
	for _, p := range s.pairs {
		keys = append(keys, p.key)
	}
	return keys
}

func formats() []emit.Format { return []emit.Format{emit.FormatLogfmt, emit.FormatJSON} }

// The common case: a critical-first answer over budget keeps its head,
// whole lines only, and the summary says what was left out.
func TestFitResponse_KeepsTheHeadOfAWorstFirstAnswer(t *testing.T) {
	c := floodCommand(false)
	for _, format := range formats() {
		t.Run(string(format), func(t *testing.T) {
			full := stream(t, c, "--format="+string(format))
			limit := len(full) / 3
			got, dropped := fitResponse(c, nil, full, limit)
			if len(got) > limit {
				t.Fatalf("cut answer is %d bytes, over the %d-byte limit", len(got), limit)
			}
			lines, summary := splitStream(got)
			all, _ := splitStream(full)
			if len(lines) == 0 || dropped != len(all)-len(lines) {
				t.Fatalf("kept %d of %d lines, reported %d dropped", len(lines), len(all), dropped)
			}
			for i, line := range lines {
				if line != all[i] {
					t.Fatalf("line %d is not the original's line %d:\n got %q\nwant %q", i, i, line, all[i])
				}
			}
			// The kept prefix holds all 23 critical and warning findings,
			// so every dropped one is info.
			if len(lines) < 23 {
				t.Fatalf("budget too small for this test: kept %d lines", len(lines))
			}
			f := summaryFields(t, summary)
			if f["findings"] != strconv.Itoa(len(lines)) || f["scanned"] != "230" || f["elapsed"] != "1.2s" || f["source"] != "live" {
				t.Errorf("summary lost or miscounted a key: %q", summary)
			}
			if f["response_truncated"] != strconv.Itoa(dropped) {
				t.Errorf("response_truncated = %q, want %d", f["response_truncated"], dropped)
			}
			if want := "critical:0,warning:0,info:" + strconv.Itoa(dropped); f["response_truncated_severity"] != want {
				t.Errorf("response_truncated_severity = %q, want %q", f["response_truncated_severity"], want)
			}
			if want := "kept the first " + strconv.Itoa(len(lines)) + " of 223 findings"; !strings.Contains(f["response_hint"], want) {
				t.Errorf("response_hint = %q, want it to say %q", f["response_hint"], want)
			}
			// Notes stay in place, ahead of the response_* keys.
			if keys := strings.Join(summaryKeys(t, summary), ","); keys != "scanned,findings,elapsed,source,response_truncated,response_truncated_severity,response_hint" {
				t.Errorf("summary keys = %s", keys)
			}
			if err := checktest.Verify(c, got, format); err != nil {
				t.Errorf("cut answer breaks the output contract: %v\n%s", err, got)
			}
		})
	}
}

// A timeline keeps its newest end, and the severity breakdown counts
// what fell off the old end.
func TestFitResponse_KeepsTheTailOfANewestLastAnswer(t *testing.T) {
	c := floodCommand(true)
	for _, format := range formats() {
		t.Run(string(format), func(t *testing.T) {
			full := stream(t, c, "--format="+string(format))
			limit := len(full) / 3
			got, dropped := fitResponse(c, nil, full, limit)
			if len(got) > limit {
				t.Fatalf("cut answer is %d bytes, over the %d-byte limit", len(got), limit)
			}
			lines, summary := splitStream(got)
			all, _ := splitStream(full)
			offset := len(all) - len(lines)
			for i, line := range lines {
				if line != all[offset+i] {
					t.Fatalf("kept line %d is not the original's line %d", i, offset+i)
				}
			}
			f := summaryFields(t, summary)
			if want := "critical:3,warning:20,info:" + strconv.Itoa(dropped-23); f["response_truncated_severity"] != want {
				t.Errorf("response_truncated_severity = %q, want %q", f["response_truncated_severity"], want)
			}
			if !strings.Contains(f["response_hint"], "kept the newest "+strconv.Itoa(len(lines))+" of 223") {
				t.Errorf("response_hint = %q, want it to name the newest end", f["response_hint"])
			}
			if err := checktest.Verify(c, got, format); err != nil {
				t.Errorf("cut answer breaks the output contract: %v\n%s", err, got)
			}
		})
	}
}

func TestFitResponse_AnAnswerThatFitsIsUntouched(t *testing.T) {
	c := floodCommand(false)
	full := stream(t, c)
	if got, dropped := fitResponse(c, nil, full, len(full)); got != full || dropped != 0 {
		t.Errorf("an answer exactly at the limit was changed (%d dropped)", dropped)
	}
	if got, dropped := fitResponse(c, nil, full, len(full)-1); got == full || dropped == 0 {
		t.Error("an answer one byte over the limit was not cut")
	}
}

func TestFitResponse_ZeroDisablesTheLimit(t *testing.T) {
	c := floodCommand(false)
	full := stream(t, c)
	if got, dropped := fitResponse(c, nil, full, 0); got != full || dropped != 0 {
		t.Errorf("limit 0 changed the answer (%d dropped)", dropped)
	}
}

// A line is never split. When the first line to keep does not fit, the
// answer is the summary alone, saying everything was left out, rather
// than a partial line or a later line out of order.
func TestFitResponse_ALineLargerThanTheBudgetLeavesTheSummaryAlone(t *testing.T) {
	for _, newestLast := range []bool{false, true} {
		c := floodCommand(newestLast)
		for _, format := range formats() {
			full := stream(t, c, "--format="+string(format), "--info=0", "--huge=4000")
			got, dropped := fitResponse(c, nil, full, 1000)
			lines, summary := splitStream(got)
			if len(lines) != 0 || dropped != 23 {
				t.Fatalf("newestLast=%v %s: kept %d lines, dropped %d; want the summary alone:\n%s", newestLast, format, len(lines), dropped, got)
			}
			f := summaryFields(t, summary)
			if f["findings"] != "0" || f["response_truncated"] != "23" || f["response_truncated_severity"] != "critical:3,warning:20,info:0" {
				t.Errorf("newestLast=%v %s: summary %q", newestLast, format, summary)
			}
			if err := checktest.Verify(c, got, format); err != nil {
				t.Errorf("newestLast=%v %s: %v", newestLast, format, err)
			}
		}
	}
}

// exemptEven annotates every finding whose name ends in an even digit.
type exemptEven struct{}

func (exemptEven) Exempt(_, _, name string) (string, string, bool) {
	last := name[len(name)-1]
	return "reviewed", "", (last-'0')%2 == 0
}

// exempt= counts the findings returned, not the ones emitted, so it can
// never claim more than findings= accounts for.
func TestFitResponse_ExemptCountsOnlyTheReturnedFindings(t *testing.T) {
	c := floodCommand(false)
	for _, format := range formats() {
		var buf bytes.Buffer
		w, err := emit.NewWriter(&buf, format, emit.WithExemptions(exemptEven{}))
		if err != nil {
			t.Fatal(err)
		}
		for i := range 40 {
			if err := w.Emit(emit.Finding{Kind: "flood.info", Severity: emit.SeverityInfo, Name: "api-" + strconv.Itoa(i), Message: "padding padding padding"}); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Summary(40, time.Second); err != nil {
			t.Fatal(err)
		}
		full := buf.String()
		got, _ := fitResponse(c, nil, full, len(full)/2)
		lines, summary := splitStream(got)
		want := 0
		for _, l := range lines {
			if strings.Contains(l, "exempt_reason") {
				want++
			}
		}
		if f := summaryFields(t, summary); f["exempt"] != strconv.Itoa(want) {
			t.Errorf("%s: exempt = %q, want %d (the returned lines it covers)", format, f["exempt"], want)
		}
		if err := checktest.Verify(c, got, format); err != nil {
			t.Errorf("%s: %v", format, err)
		}
	}
}

// Output that is not a finding stream — no summary line, or one whose
// count does not match its lines — is never rewritten: returning it
// as it was is the old behavior, and a guess could return something
// invalid.
func TestFitResponse_LeavesNonStreamsAlone(t *testing.T) {
	c := floodCommand(false)
	for name, out := range map[string]string{
		"help text":        strings.Repeat("usage: lookout triage flood [flags]\n", 50),
		"no final newline": strings.Repeat("kind=flood.info severity=info\n", 50) + "scanned=50 findings=50 elapsed=1s",
		"count mismatch":   strings.Repeat("kind=flood.info severity=info\n", 50) + "scanned=50 findings=49 elapsed=1s\n",
		"json lookalike":   strings.Repeat(`{"kind":"flood.info"}`+"\n", 50) + `{"scanned":50,"findings":50,"elapsed":{"s":1}}` + "\n",
	} {
		if got, dropped := fitResponse(c, nil, out, 100); got != out || dropped != 0 {
			t.Errorf("%s: rewritten (%d dropped):\n%s", name, dropped, got)
		}
	}
}

// The hint names the arguments that would narrow THIS call: the ones it
// already narrowed by are left out, a target already names one
// workload, and since is offered only to a command that declares
// UsesSince (#756).
func TestNarrowingHintIsTailoredToTheCall(t *testing.T) {
	plain := floodCommand(false)
	timed := floodCommand(false)
	timed.UsesSince = true
	positional := fakeSpecCommand()
	timedPositional := fakeSpecCommand()
	timedPositional.UsesSince = true
	for _, tc := range []struct {
		c    checks.Command
		args []string
		want string
	}{
		{timed, nil, "namespace, workload or since"},
		{timed, []string{"--A=true"}, "namespace, workload or since"},
		{timed, []string{"--namespace=prod"}, "workload or since"},
		{timed, []string{"--workload=Deployment/prod/api", "--since=2h"}, "a shorter since"},
		{timedPositional, []string{"--namespace=prod", "Pod/api"}, "since"},
		// A command that ignores --since is never told to use it, even
		// when the call passed it.
		{plain, nil, "namespace or workload"},
		{plain, []string{"--namespace=prod"}, "workload"},
		{plain, []string{"--since=2h"}, "namespace or workload"},
		{positional, []string{"--namespace=prod", "Pod/api"}, ""},
	} {
		if got := narrowing(tc.c, tc.args); got != tc.want {
			t.Errorf("narrowing(%s UsesSince=%v, %v) = %q, want %q", tc.c.MCPName, tc.c.UsesSince, tc.args, got, tc.want)
		}
	}
}

// With nothing left to offer, the hint says so rather than naming an
// argument that would not help.
func TestResponseHintWithNothingLeftToNarrow(t *testing.T) {
	got := responseHint(fakeSpecCommand(), []string{"--namespace=prod", "Pod/api"}, 10, 50, 1000)
	if want := "kept the first 10 of 50 findings to fit the server's 1000-byte response limit; the call already uses every argument that would narrow it"; got != want {
		t.Errorf("responseHint = %q, want %q", got, want)
	}
}

// End to end: the server cuts by default, the client receives a valid
// answer in the format it asked for, and the access log records the
// bytes actually returned plus how many findings were left out.
func TestCallTool_OversizeAnswerIsCutAndLogged(t *testing.T) {
	reg := checks.NewRegistry()
	reg.Register(floodCommand(false))
	log, buf := testLog(t, time.Millisecond)
	cs := connect(t, New(reg, "test", WithAccessLog(log)))

	res, err := callTool(t, cs, "k8s_triage_flood", map[string]any{"info": 2000, "format": "json"})
	if err != nil || res.IsError {
		t.Fatalf("tools/call: %v %v", err, res)
	}
	got := text(t, res)
	if len(got) > DefaultMaxResponseBytes {
		t.Fatalf("answer is %d bytes, over the default %d", len(got), DefaultMaxResponseBytes)
	}
	if err := checktest.Verify(floodCommand(false), got, emit.FormatJSON); err != nil {
		t.Fatalf("cut answer breaks the output contract: %v", err)
	}
	_, summary := splitStream(got)
	var m map[string]any
	if err := json.Unmarshal([]byte(summary), &m); err != nil {
		t.Fatalf("summary is not JSON: %v", err)
	}
	if _, ok := m["findings"].(float64); !ok {
		t.Errorf("findings is not a JSON number in %s", summary)
	}
	dropped, _ := m["response_truncated"].(string)
	if dropped == "" || dropped == "0" {
		t.Fatalf("no response_truncated in %s", summary)
	}

	recs := records(t, buf)
	if len(recs) != 1 || recs[0]["bytes"] != strconv.Itoa(len(got)) || recs[0]["truncated"] != dropped {
		t.Errorf("access log = %v, want bytes=%d truncated=%s", recs, len(got), dropped)
	}
}

func TestCallTool_MaxResponseBytesZeroReturnsEverything(t *testing.T) {
	reg := checks.NewRegistry()
	reg.Register(floodCommand(false))
	log, buf := testLog(t, time.Millisecond)
	cs := connect(t, New(reg, "test", WithAccessLog(log), WithMaxResponseBytes(0)))

	res, err := callTool(t, cs, "k8s_triage_flood", map[string]any{"info": 2000})
	if err != nil || res.IsError {
		t.Fatalf("tools/call: %v %v", err, res)
	}
	got := text(t, res)
	if len(got) <= DefaultMaxResponseBytes || strings.Contains(got, "response_truncated") {
		t.Fatalf("limit 0 still cut the answer (%d bytes)", len(got))
	}
	if recs := records(t, buf); len(recs) != 1 || recs[0]["truncated"] != "" {
		t.Errorf("an uncut answer logged truncated=: %v", recs)
	}
}

// clusterList renders a k8s_list_clusters result of n clusters (out of
// scanned listable) and two discovery scopes, one of them failed.
func clusterList(n, scanned int) string {
	entries := make([]clusterEntry, n)
	for i := range entries {
		ref := gref("proj-"+strconv.Itoa(i%7), "us-central1", "cluster-"+strconv.Itoa(i))
		entries[i] = clusterEntry{ref: ref, name: ref.Name, canonical: ref.Project + "/" + ref.Location + "/" + ref.Name, access: accessRead}
	}
	scopes := []fleet.ScopeResult{
		{Scope: "folders/1", Via: "asset", Clusters: n},
		{Scope: "projects/missing", Via: "container", Err: errors.New("permission denied")},
	}
	return renderClusters(entries, scopes, scanned, 1200*time.Millisecond)
}

// An oversize cluster list keeps its first clusters (readable ones come
// first), whole lines only, every scope line and the summary, which
// says how many were left out and points at filter (#755).
func TestFitClusterList_KeepsTheFirstClusters(t *testing.T) {
	full := clusterList(200, 250)
	all, _ := splitStream(full)
	entries, scopes := all[:200], all[200:]
	for _, filtered := range []bool{false, true} {
		limit := len(full) / 3
		got, dropped := fitClusterList(full, filtered, limit)
		if len(got) > limit {
			t.Fatalf("cut list is %d bytes, over the %d-byte limit", len(got), limit)
		}
		lines, summary := splitStream(got)
		kept := len(lines) - len(scopes)
		if kept <= 0 || dropped != 200-kept {
			t.Fatalf("kept %d clusters, reported %d dropped", kept, dropped)
		}
		for i := range kept {
			if lines[i] != entries[i] {
				t.Fatalf("line %d is not the original's line %d:\n got %q\nwant %q", i, i, lines[i], entries[i])
			}
		}
		if !slices.Equal(lines[kept:], scopes) {
			t.Errorf("scope lines changed:\n got %q\nwant %q", lines[kept:], scopes)
		}
		f := summaryFields(t, summary)
		if f["scanned"] != "250" || f["findings"] != "0" || f["elapsed"] != "1.2s" || f["clusters"] != strconv.Itoa(kept) {
			t.Errorf("summary lost or miscounted a key: %q", summary)
		}
		if f["response_truncated"] != strconv.Itoa(dropped) {
			t.Errorf("response_truncated = %q, want %d", f["response_truncated"], dropped)
		}
		narrow := "narrow the call with filter"
		if filtered {
			narrow = "narrow the call with a narrower filter"
		}
		if want := "kept the first " + strconv.Itoa(kept) + " of 200 clusters to fit the server's " + strconv.Itoa(limit) + "-byte response limit; " + narrow; f["response_hint"] != want {
			t.Errorf("response_hint = %q, want %q", f["response_hint"], want)
		}
		if keys := strings.Join(summaryKeys(t, summary), ","); keys != "scanned,findings,elapsed,clusters,response_truncated,response_hint" {
			t.Errorf("summary keys = %s", keys)
		}
	}
}

func TestFitClusterList_AListThatFitsIsUntouched(t *testing.T) {
	full := clusterList(40, 40)
	if got, dropped := fitClusterList(full, false, len(full)); got != full || dropped != 0 {
		t.Errorf("a list exactly at the limit was changed (%d dropped)", dropped)
	}
	if got, dropped := fitClusterList(full, false, len(full)-1); got == full || dropped == 0 {
		t.Error("a list one byte over the limit was not cut")
	}
}

func TestFitClusterList_ZeroDisablesTheLimit(t *testing.T) {
	full := clusterList(200, 200)
	if got, dropped := fitClusterList(full, false, 0); got != full || dropped != 0 {
		t.Errorf("limit 0 changed the list (%d dropped)", dropped)
	}
}

// When not even the scope lines and summary leave room for a cluster,
// they are returned alone, saying every cluster was left out.
func TestFitClusterList_TooSmallKeepsScopesAndSummary(t *testing.T) {
	full := clusterList(5, 5)
	got, dropped := fitClusterList(full, false, 50)
	lines, summary := splitStream(got)
	if dropped != 5 || len(lines) != 2 || !strings.HasPrefix(lines[0], "scope=") {
		t.Fatalf("dropped %d, kept:\n%s", dropped, got)
	}
	if f := summaryFields(t, summary); f["clusters"] != "0" || f["response_truncated"] != "5" {
		t.Errorf("summary %q", summary)
	}
}

// Output not shaped like renderClusters' is returned as it was.
func TestFitClusterList_LeavesOtherShapesAlone(t *testing.T) {
	line := "cluster=a project=p location=l canonical=p/l/a dns_endpoint=true\n"
	for name, out := range map[string]string{
		"count mismatch":    strings.Repeat(line, 50) + "scanned=50 findings=0 elapsed=1s clusters=49\n",
		"no clusters key":   strings.Repeat(line, 50) + "scanned=50 findings=0 elapsed=1s\n",
		"scope before rows": "scope=p via=container clusters=50\n" + strings.Repeat(line, 50) + "scanned=50 findings=0 elapsed=1s clusters=50\n",
		"no final newline":  strings.Repeat(line, 50) + "scanned=50 findings=0 elapsed=1s clusters=50",
	} {
		if got, dropped := fitClusterList(out, false, 200); got != out || dropped != 0 {
			t.Errorf("%s: rewritten (%d dropped):\n%s", name, dropped, got)
		}
	}
}
