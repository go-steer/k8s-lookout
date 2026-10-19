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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// DefaultMaxResponseBytes is the default --max-response-bytes (#747).
//
// MCP clients cap what they will hand the model: Claude Code's default
// is 25,000 tokens, about 100,000 characters at its four-characters-a-
// token estimate, and an answer over a client's cap is cut wherever the
// client cuts it, summary line and all, or refused. lookout's logfmt
// runs at roughly 3.4 to 3.8 bytes a token (measured on the golden
// outputs with an open tokenizer), so 100,000 bytes is about the 25k
// mark: an answer this size reaches the model whole, and anything
// larger is cut here instead, deliberately and visibly.
const DefaultMaxResponseBytes = 100000

// WithMaxResponseBytes caps the size of a successful tool result
// (#747); 0 disables the cap. A negative value is ignored, leaving the
// default. See fitResponse for how an oversize answer is cut.
func WithMaxResponseBytes(n int) Option {
	return func(c *config) {
		if n >= 0 {
			c.maxResponseBytes = n
		}
	}
}

// fitResponse cuts one successful tool result to at most limit bytes
// and returns it with the number of findings it dropped (0 when the
// result is returned as it was).
//
// The cut is deliberate rather than arbitrary. A client that is handed
// more than it accepts truncates mid-line or rejects the answer, and
// the model never learns what it lost. Here the result loses whole
// finding lines only, from the end that matters least: the tail for
// most commands, which emit the worst first, and the head for a
// checks.Command.NewestLast timeline, whose newest entries are last.
// The summary line is always kept and rewritten so the §4.2 contract
// still holds on what is returned:
//
//   - findings= is the number of finding lines actually returned;
//   - exempt=, when present, counts only the returned lines, so it
//     never claims more than findings= accounts for;
//   - every other note is kept as it was, in place;
//   - response_truncated=<n> counts the dropped findings,
//     response_truncated_severity=critical:<n>,warning:<n>,info:<n>
//     says how bad they were, and response_hint says which end was
//     kept and which arguments would narrow the call.
//
// Both output formats are line-oriented with one record per line, so
// the cut works the same on --format=json, and the rewritten summary
// stays in the stream's format. A stream that does not end in a
// parseable summary line whose count matches its lines (nothing lookout
// writes, but nothing here assumes so) is returned untouched rather
// than risk returning something invalid. So is one too small to cut:
// when not even the summary fits, the summary alone is returned, over
// the budget, because an answer without one is void.
func fitResponse(c checks.Command, args []string, out string, limit int) (string, int) {
	if limit <= 0 || len(out) <= limit || !strings.HasSuffix(out, "\n") {
		return out, 0
	}
	lines := strings.SplitAfter(out, "\n")
	lines = lines[:len(lines)-1] // SplitAfter leaves "" after the final newline
	sum, ok := parseSummary(lines[len(lines)-1])
	findings := lines[:len(lines)-1]
	if !ok || len(findings) == 0 || sum.findings() != len(findings) {
		return out, 0
	}
	recs := make([]record, len(findings))
	for i, line := range findings {
		if recs[i], ok = parseRecord(line, sum.json); !ok {
			return out, 0
		}
	}

	// Budget the lines against the longest summary this cut could
	// produce: every count it carries is at most the number of
	// findings, so rendering each as that number bounds the real one.
	total := len(findings)
	all := map[string]int{}
	for _, r := range recs {
		all[severityBucket(r.severity)] = total
	}
	hint := responseHint(c, args, total, total, limit)
	budget := limit - len(sum.cut(total, total, total, all, hint))

	keep, size := 0, 0
	for keep < total {
		i := keep
		if c.NewestLast {
			i = total - 1 - keep
		}
		if size+len(findings[i]) > budget {
			break
		}
		size += len(findings[i])
		keep++
	}
	if keep == total {
		return out, 0
	}
	kept, keptRecs := findings[:keep], recs[:keep]
	droppedRecs := recs[keep:]
	if c.NewestLast {
		kept, keptRecs = findings[total-keep:], recs[total-keep:]
		droppedRecs = recs[:total-keep]
	}
	exempt := 0
	for _, r := range keptRecs {
		if r.exempt {
			exempt++
		}
	}
	bySeverity := map[string]int{}
	for _, r := range droppedRecs {
		bySeverity[severityBucket(r.severity)]++
	}
	var b strings.Builder
	b.Grow(size + len(lines[len(lines)-1]) + 256)
	for _, line := range kept {
		b.WriteString(line)
	}
	b.WriteString(sum.cut(keep, total-keep, exempt, bySeverity, responseHint(c, args, keep, total, limit)))
	return b.String(), total - keep
}

// record is what the cut needs to know about one finding line.
type record struct {
	severity string
	exempt   bool
}

// parseRecord reads one finding line in the stream's format.
func parseRecord(line string, isJSON bool) (record, bool) {
	if isJSON {
		var f struct {
			Severity     string `json:"severity"`
			ExemptReason string `json:"exempt_reason"`
		}
		if err := json.Unmarshal([]byte(line), &f); err != nil {
			return record{}, false
		}
		return record{severity: f.Severity, exempt: f.ExemptReason != ""}, true
	}
	pairs, err := emit.ParseLogfmt(line)
	if err != nil || len(pairs) == 0 {
		return record{}, false
	}
	var r record
	for _, p := range pairs {
		switch p.Key {
		case "severity":
			r.severity = p.Value
		case "exempt_reason":
			r.exempt = p.Value != ""
		}
	}
	return r, true
}

// severityBucket maps a finding's severity to its bucket in
// response_truncated_severity. Every declared kind carries one of the
// three levels (the contract tests hold commands to it); "other" exists
// so a count can never silently go missing if one did not.
func severityBucket(s string) string {
	if emit.ValidSeverity(s) {
		return s
	}
	return "other"
}

// summaryPair is one key of a summary line. number marks a JSON value
// written bare (scanned, findings) rather than as a string.
type summaryPair struct {
	key, value string
	number     bool
}

// summary is a parsed §4.2 summary line in either format.
type summary struct {
	json  bool
	pairs []summaryPair
}

// parseSummary parses the last line of a stream as a summary line:
// scanned, findings and elapsed first, notes after.
func parseSummary(line string) (summary, bool) {
	var s summary
	if strings.HasPrefix(line, "{") {
		s.json = true
		pairs, ok := parseJSONObject(line)
		if !ok {
			return summary{}, false
		}
		s.pairs = pairs
	} else {
		fields, err := emit.ParseLogfmt(line)
		if err != nil {
			return summary{}, false
		}
		for _, f := range fields {
			s.pairs = append(s.pairs, summaryPair{key: f.Key, value: f.Value})
		}
	}
	if len(s.pairs) < 3 || s.pairs[0].key != "scanned" || s.pairs[1].key != "findings" || s.pairs[2].key != "elapsed" {
		return summary{}, false
	}
	if _, err := strconv.Atoi(s.pairs[1].value); err != nil {
		return summary{}, false
	}
	return s, true
}

func (s summary) findings() int {
	n, _ := strconv.Atoi(s.pairs[1].value)
	return n
}

// cut renders the summary of a cut stream: findings= set to the lines
// returned, exempt= (when present) to the returned lines it covers, the
// remaining notes as they were, and the response_* notes last.
func (s summary) cut(returned, dropped, exempt int, bySeverity map[string]int, hint string) string {
	pairs := make([]summaryPair, 0, len(s.pairs)+3)
	for _, p := range s.pairs {
		switch p.key {
		case "findings":
			p.value = strconv.Itoa(returned)
		case "exempt":
			p.value = strconv.Itoa(exempt)
		}
		pairs = append(pairs, p)
	}
	sev := make([]string, 0, 4)
	for _, level := range emit.Severities() {
		sev = append(sev, fmt.Sprintf("%s:%d", level, bySeverity[level]))
	}
	if n := bySeverity["other"]; n > 0 {
		sev = append(sev, fmt.Sprintf("other:%d", n))
	}
	pairs = append(pairs,
		summaryPair{key: "response_truncated", value: strconv.Itoa(dropped)},
		summaryPair{key: "response_truncated_severity", value: strings.Join(sev, ",")},
		summaryPair{key: "response_hint", value: hint},
	)
	if !s.json {
		fields := make([]emit.Field, len(pairs))
		for i, p := range pairs {
			fields[i] = emit.Field{Key: p.key, Value: p.value}
		}
		return string(emit.EncodeLogfmt(fields))
	}
	var b bytes.Buffer
	b.WriteByte('{')
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte(',')
		}
		k, _ := json.Marshal(p.key) // string marshaling cannot fail
		b.Write(k)
		b.WriteByte(':')
		if p.number {
			b.WriteString(p.value)
		} else {
			v, _ := json.Marshal(p.value)
			b.Write(v)
		}
	}
	b.WriteString("}\n")
	return b.String()
}

// parseJSONObject decodes one flat JSON object keeping its key order,
// which a map would lose: the summary's keys are ordered, and the
// rewritten line keeps them in place.
func parseJSONObject(line string) ([]summaryPair, bool) {
	dec := json.NewDecoder(strings.NewReader(line))
	dec.UseNumber()
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return nil, false
	}
	var out []summaryPair
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return nil, false
		}
		key, ok := t.(string)
		if !ok {
			return nil, false
		}
		t, err = dec.Token()
		if err != nil {
			return nil, false
		}
		switch v := t.(type) {
		case string:
			out = append(out, summaryPair{key: key, value: v})
		case json.Number:
			out = append(out, summaryPair{key: key, value: v.String(), number: true})
		default: // nested values and literals are not summary shapes
			return nil, false
		}
	}
	if t, err := dec.Token(); err != nil || t != json.Delim('}') {
		return nil, false
	}
	if _, err := dec.Token(); err == nil {
		return nil, false // trailing data after the object
	}
	return out, true
}

// responseHint is the response_hint note: which end of the stream was
// kept, why, and what would narrow the call. It names the MCP arguments
// the model can pass (namespace, workload, since; every tool takes
// them), leaving out the ones this call has already narrowed by.
func responseHint(c checks.Command, args []string, kept, total, limit int) string {
	end := "first"
	if c.NewestLast {
		end = "newest"
	}
	return fmt.Sprintf("kept the %s %d of %d findings to fit the server's %d-byte response limit; narrow the call with %s",
		end, kept, total, limit, narrowing(c, args))
}

// narrowing lists the arguments that would shrink this call's answer.
func narrowing(c checks.Command, args []string) string {
	set := func(flag string) bool {
		for _, a := range args {
			if v, ok := strings.CutPrefix(a, "--"+flag+"="); ok && v != "" {
				return true
			}
		}
		return false
	}
	target := false
	if c.Positional != nil {
		for _, a := range args {
			if !strings.HasPrefix(a, "-") {
				target = true
			}
		}
	}
	var opts []string
	if !set("namespace") && !set("workload") && !target {
		opts = append(opts, "namespace")
	}
	if !set("workload") && !target {
		opts = append(opts, "workload")
	}
	if set("since") {
		opts = append(opts, "a shorter since")
	} else {
		opts = append(opts, "since")
	}
	if len(opts) == 1 {
		return opts[0]
	}
	return strings.Join(opts[:len(opts)-1], ", ") + " or " + opts[len(opts)-1]
}
