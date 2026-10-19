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

	keep, size := fitLines(findings, budget, c.NewestLast)
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

// fitLines counts how many whole lines, taken from the head (or from
// the tail when fromEnd is set) and never split, fit in budget bytes,
// and how many bytes they take.
func fitLines(lines []string, budget int, fromEnd bool) (keep, size int) {
	for keep < len(lines) {
		i := keep
		if fromEnd {
			i = len(lines) - 1 - keep
		}
		if size+len(lines[i]) > budget {
			break
		}
		size += len(lines[i])
		keep++
	}
	return keep, size
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

// count returns the integer value of key, and whether the summary
// carries it as one.
func (s summary) count(key string) (int, bool) {
	for _, p := range s.pairs {
		if p.key == key {
			n, err := strconv.Atoi(p.value)
			return n, err == nil
		}
	}
	return 0, false
}

// cut renders the summary of a cut stream: findings= set to the lines
// returned, exempt= (when present) to the returned lines it covers, the
// remaining notes as they were, and the response_* notes last.
func (s summary) cut(returned, dropped, exempt int, bySeverity map[string]int, hint string) string {
	sev := make([]string, 0, 4)
	for _, level := range emit.Severities() {
		sev = append(sev, fmt.Sprintf("%s:%d", level, bySeverity[level]))
	}
	if n := bySeverity["other"]; n > 0 {
		sev = append(sev, fmt.Sprintf("other:%d", n))
	}
	return s.rewrite(map[string]int{"findings": returned, "exempt": exempt},
		summaryPair{key: "response_truncated", value: strconv.Itoa(dropped)},
		summaryPair{key: "response_truncated_severity", value: strings.Join(sev, ",")},
		summaryPair{key: "response_hint", value: hint},
	)
}

// rewrite renders the summary with the keys in set that it already
// carries given new counts (a key it lacks is not added), every other
// pair as it was and in place, and notes appended last.
func (s summary) rewrite(set map[string]int, notes ...summaryPair) string {
	pairs := make([]summaryPair, 0, len(s.pairs)+len(notes))
	for _, p := range s.pairs {
		if n, ok := set[p.key]; ok {
			p.value = strconv.Itoa(n)
		}
		pairs = append(pairs, p)
	}
	pairs = append(pairs, notes...)
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
// the model can pass (namespace and workload, which every tool takes,
// and since for a command that declares checks.Command.UsesSince),
// leaving out the ones this call has already narrowed by.
func responseHint(c checks.Command, args []string, kept, total, limit int) string {
	end := "first"
	if c.NewestLast {
		end = "newest"
	}
	return keptHint(end, kept, total, "findings", limit, narrowing(c, args))
}

// keptHint is the wording every response_hint shares: what was kept,
// of how many, under which limit, and what would narrow the call.
// An empty narrow means no argument the call has not already used
// would help, and the hint says that instead of naming one.
func keptHint(end string, kept, total int, what string, limit int, narrow string) string {
	next := "narrow the call with " + narrow
	if narrow == "" {
		next = "the call already uses every argument that would narrow it"
	}
	return fmt.Sprintf("kept the %s %d of %d %s to fit the server's %d-byte response limit; %s",
		end, kept, total, what, limit, next)
}

// narrowing lists the arguments that would shrink this call's answer.
// since is offered only to a command that declares UsesSince: every
// tool accepts the argument, but one that ignores it would return the
// same oversize answer to a model that followed the hint (#756).
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
	if c.UsesSince {
		if set("since") {
			opts = append(opts, "a shorter since")
		} else {
			opts = append(opts, "since")
		}
	}
	return orList(opts)
}

// orList joins options as prose: "a", "a or b", "a, b or c"; "" for
// none.
func orList(opts []string) string {
	switch len(opts) {
	case 0:
		return ""
	case 1:
		return opts[0]
	}
	return strings.Join(opts[:len(opts)-1], ", ") + " or " + opts[len(opts)-1]
}

// fitClusterList cuts a k8s_list_clusters result (renderClusters) to at
// most limit bytes, the way fitResponse cuts a command's, and returns
// it with the number of cluster lines it dropped (#755).
//
// The shape differs from a finding stream, so fitResponse cannot take
// it as it is: one line per cluster (readable cluster-wide first, so
// the head is the end worth keeping), then one line per discovery
// scope, then a summary line whose findings= is 0 (an inventory, not a
// judgment) and whose clusters= counts the cluster lines. Only cluster
// lines are dropped, whole, from the tail. The scope lines are kept:
// there are a handful, and they carry the discovery errors that explain
// a short list. The summary is kept with clusters= set to the lines
// returned and scanned= (every cluster the caller could list) as it
// was, and gains response_truncated=<n> (the clusters dropped) and a
// response_hint that points at filter. There is no
// response_truncated_severity: clusters have no severity.
//
// As with fitResponse, output not of this shape is returned untouched,
// and when not even the scope lines and summary fit, they are returned
// alone, over the budget.
func fitClusterList(out string, filtered bool, limit int) (string, int) {
	if limit <= 0 || len(out) <= limit || !strings.HasSuffix(out, "\n") {
		return out, 0
	}
	lines := strings.SplitAfter(out, "\n")
	lines = lines[:len(lines)-1] // SplitAfter leaves "" after the final newline
	sum, ok := parseSummary(lines[len(lines)-1])
	if !ok {
		return out, 0
	}
	body := lines[:len(lines)-1]
	total := 0
	for total < len(body) && strings.HasPrefix(body[total], "cluster=") {
		total++
	}
	entries, scopes := body[:total], body[total:]
	for _, line := range scopes {
		if !strings.HasPrefix(line, "scope=") {
			return out, 0
		}
	}
	listed, ok := sum.count("clusters")
	if !ok || listed != total || total == 0 {
		return out, 0
	}

	scopeBytes := 0
	for _, line := range scopes {
		scopeBytes += len(line)
	}
	narrow := "filter"
	if filtered {
		narrow = "a narrower filter"
	}
	render := func(kept, dropped int) string {
		return sum.rewrite(map[string]int{"clusters": kept},
			summaryPair{key: "response_truncated", value: strconv.Itoa(dropped)},
			summaryPair{key: "response_hint", value: keptHint("first", kept, total, "clusters", limit, narrow)},
		)
	}
	// As in fitResponse: every count the cut summary carries is at
	// most total, so rendering each as total bounds the real one.
	longest := len(render(total, total))
	keep, size := fitLines(entries, limit-scopeBytes-longest, false)
	if keep == total {
		return out, 0
	}
	var b strings.Builder
	b.Grow(size + scopeBytes + longest)
	for _, line := range entries[:keep] {
		b.WriteString(line)
	}
	for _, line := range scopes {
		b.WriteString(line)
	}
	b.WriteString(render(keep, total-keep))
	return b.String(), total - keep
}
