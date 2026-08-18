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

// Package checktest is the §13 contract-test scaffold for read-path
// commands. Every command's test suite calls VerifyContract, which
// runs the command in both formats and round-trips the emitted
// findings against the command's declared output-field glossary —
// so a check cannot silently emit a field its metadata (and
// therefore its --help, MCP schema, and skill docs) does not
// declare, and metadata edits that break emitted output fail in the
// same place.
package checktest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// Result captures one command invocation.
type Result struct {
	Code   int
	Stdout string
	Stderr string
}

// Run executes a command under the real §4.2 runner with a fake
// clock (each Now() call advances 100ms, so elapsed is always
// "100ms" and output is golden-testable).
func Run(t *testing.T, c checks.Command, args ...string) Result {
	t.Helper()
	return RunStdin(t, c, "", args...)
}

// RunStdin is Run with stdin wired to a fixed string, for the
// commands that consume a piped report (`--report -`). An empty
// stdin still reads as EOF, never as the test process's terminal.
func RunStdin(t *testing.T, c checks.Command, stdin string, args ...string) Result {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cfg := c.RunConfig(&stdout, &stderr)
	cfg.Stdin = strings.NewReader(stdin)
	cfg.Now = FakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), 100*time.Millisecond)
	code := emit.Run(context.Background(), cfg, args)
	return Result{Code: code, Stdout: stdout.String(), Stderr: stderr.String()}
}

// FakeClock returns a Now func that starts at base and advances by
// step on every call.
func FakeClock(base time.Time, step time.Duration) func() time.Time {
	var mu sync.Mutex
	n := 0
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		ts := base.Add(time.Duration(n) * step)
		n++
		return ts
	}
}

// VerifyContract runs the command in logfmt and JSON and verifies
// both outputs against its declared schema. args must not include
// --format.
func VerifyContract(t *testing.T, c checks.Command, args ...string) {
	t.Helper()
	VerifyContractStdin(t, c, "", args...)
}

// VerifyContractStdin is VerifyContract for commands that consume a
// piped report. The same stdin is replayed for each format, so the
// two runs see identical input.
func VerifyContractStdin(t *testing.T, c checks.Command, stdin string, args ...string) {
	t.Helper()
	for _, format := range []emit.Format{emit.FormatLogfmt, emit.FormatJSON} {
		res := RunStdin(t, c, stdin, append([]string{"--format=" + string(format)}, args...)...)
		if res.Code != emit.ExitData {
			t.Fatalf("lookout %s (%s): exit %d, stderr: %s", c.Name, format, res.Code, res.Stderr)
		}
		if err := Verify(c, res.Stdout, format); err != nil {
			t.Errorf("lookout %s (%s): output contract violated: %v\noutput:\n%s",
				c.Name, format, err, res.Stdout)
		}
	}
}

// Verify checks one successful stdout stream against the command's
// declared schema:
//
//   - the last line is the mandatory summary, leading with exactly
//     the keys scanned/findings/elapsed; any §6.6-style note keys
//     after them (emit.Writer.Note) must be declared in the output
//     glossary like Details keys;
//   - the summary's findings count equals the number of finding
//     lines;
//   - every finding key is either an envelope field or declared in
//     the command's Output glossary;
//   - every finding's kind= is declared in the command's Kinds ledger
//     (issue #278) and its severity= is one the ledger allows for that
//     kind.
//
// It deliberately does not require every declared field or kind to
// appear — most are conditional on what the scan found. Declaring a
// kind no run produces is honest; emitting one no declaration covers
// is the drift this catches.
func Verify(c checks.Command, stdout string, format emit.Format) error {
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	if len(lines) == 0 || lines[len(lines)-1] == "" {
		return fmt.Errorf("no summary line (stdout %q)", stdout)
	}
	summary, findings := lines[len(lines)-1], lines[:len(lines)-1]

	declared := map[string]bool{}
	for _, k := range emit.EnvelopeFields() {
		declared[k] = true
	}
	// Writer-owned summary notes (exempt=) are implicitly declared for
	// the same reason envelope fields are: the Writer appends them, so
	// no command can be held responsible for glossing them.
	for _, k := range emit.SummaryNoteFields() {
		declared[k] = true
	}
	for _, f := range c.Output {
		declared[f.Name] = true
	}

	reported, notes, err := parseSummary(summary, format)
	if err != nil {
		return fmt.Errorf("summary line %q: %w", summary, err)
	}
	for _, k := range notes {
		if !declared[k] {
			return fmt.Errorf("summary line %q: note key %q is not declared in the output glossary", summary, k)
		}
	}
	if reported != len(findings) {
		return fmt.Errorf("summary reports findings=%d but %d finding lines precede it", reported, len(findings))
	}
	for _, line := range findings {
		fields, err := lineFields(line, format)
		if err != nil {
			return fmt.Errorf("finding line %q: %w", line, err)
		}
		if len(fields) == 0 {
			return fmt.Errorf("finding line %q: empty record", line)
		}
		var kind, severity string
		for _, f := range fields {
			if !declared[f.Key] {
				return fmt.Errorf("finding line %q: field %q is not an envelope field and not declared in the output glossary", line, f.Key)
			}
			switch f.Key {
			case "kind":
				kind = f.Value
			case "severity":
				severity = f.Value
			}
		}
		if err := verifyKind(c, kind, severity); err != nil {
			return fmt.Errorf("finding line %q: %w", line, err)
		}
	}
	return nil
}

// verifyKind holds one emitted finding to the command's Kinds ledger.
// The ledger is the vocabulary the command's docs, glossary page and
// consumers are generated from, so a kind that never reaches it is a
// claim nobody downstream can read.
func verifyKind(c checks.Command, kind, severity string) error {
	if kind == "" {
		return fmt.Errorf("no kind= field")
	}
	k, ok := c.EmitsKind(kind)
	if !ok {
		return fmt.Errorf("kind %q is not declared in the Kinds ledger (issue #278); add checks.Kind(%q, <one-line claim>, %s) to `%s`",
			kind, kind, severityConst(severity), c.Name)
	}
	for _, s := range k.Severity {
		if s == severity {
			return nil
		}
	}
	return fmt.Errorf("kind %q emitted at severity %q, but its ledger entry declares only %s — widen the declaration or fix the emitter",
		kind, severity, strings.Join(k.Severity, "|"))
}

// severityConst spells a severity back as the emit constant, so the
// failure message is the line the contributor can paste.
func severityConst(severity string) string {
	switch severity {
	case emit.SeverityCritical:
		return "emit.SeverityCritical"
	case emit.SeverityWarning:
		return "emit.SeverityWarning"
	case emit.SeverityInfo:
		return "emit.SeverityInfo"
	}
	return "<severity>"
}

// parseSummary validates the summary line's shape and returns its
// findings count plus any note keys following the mandatory three.
func parseSummary(line string, format emit.Format) (findings int, notes []string, err error) {
	switch format {
	case emit.FormatJSON:
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			return 0, nil, err
		}
		if m["scanned"] == nil || m["findings"] == nil || m["elapsed"] == nil {
			return 0, nil, fmt.Errorf("want the keys scanned/findings/elapsed, got %v", m)
		}
		f, ok := m["findings"].(float64)
		if !ok {
			return 0, nil, fmt.Errorf("findings is not a number: %v", m["findings"])
		}
		for k := range m {
			if k != "scanned" && k != "findings" && k != "elapsed" {
				notes = append(notes, k)
			}
		}
		return int(f), notes, nil
	default:
		pairs, err := parseLogfmt(line)
		if err != nil {
			return 0, nil, err
		}
		if len(pairs) < 3 || pairs[0].Key != "scanned" || pairs[1].Key != "findings" || pairs[2].Key != "elapsed" {
			return 0, nil, fmt.Errorf("want scanned=<n> findings=<n> elapsed=<d>, got %q", line)
		}
		for _, p := range pairs[3:] {
			notes = append(notes, p.Key)
		}
		findings, err = strconv.Atoi(pairs[1].Value)
		return findings, notes, err
	}
}

// lineFields returns the fields of one finding line. Order is
// preserved for logfmt and arbitrary for JSON; nothing here depends on
// it, only on the set of keys and on kind/severity's values.
func lineFields(line string, format emit.Format) ([]emit.Field, error) {
	if format == emit.FormatJSON {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			return nil, err
		}
		fields := make([]emit.Field, 0, len(m))
		for k, v := range m {
			fields = append(fields, emit.Field{Key: k, Value: fmt.Sprint(v)})
		}
		return fields, nil
	}
	return parseLogfmt(line)
}

// parseLogfmt splits one emitted logfmt line back into ordered
// pairs. It understands exactly what emit's encoder produces:
// space-separated key=value with strconv-quoted values.
func parseLogfmt(line string) ([]emit.Field, error) {
	var out []emit.Field
	rest := line
	for rest != "" {
		eq := strings.IndexByte(rest, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("no key=value at %q", rest)
		}
		key := rest[:eq]
		rest = rest[eq+1:]
		var val string
		if strings.HasPrefix(rest, `"`) {
			// Quoted value: find its closing quote via the
			// tokenizer strconv provides.
			q, err := strconv.QuotedPrefix(rest)
			if err != nil {
				return nil, fmt.Errorf("bad quoted value after %s=: %w", key, err)
			}
			val, err = strconv.Unquote(q)
			if err != nil {
				return nil, err
			}
			rest = rest[len(q):]
		} else if sp := strings.IndexByte(rest, ' '); sp >= 0 {
			val, rest = rest[:sp], rest[sp:]
		} else {
			val, rest = rest, ""
		}
		out = append(out, emit.Field{Key: key, Value: val})
		if rest != "" {
			if !strings.HasPrefix(rest, " ") {
				return nil, fmt.Errorf("expected space after %s=%s", key, val)
			}
			rest = rest[1:]
		}
	}
	return out, nil
}
