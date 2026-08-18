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

package findings

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// maxReportLine caps one report record. The emit path caps messages
// well below this; the bound exists so a malformed or hostile stream
// cannot make the differ allocate without limit.
const maxReportLine = 1 << 20 // 1 MiB

// ObservationOf reduces one report finding to a diff Observation,
// composing its subject key.
//
// The reason is canonicalized with the MESSAGELESS
// engine.CanonicalReason, matching engine.ScanFingerprint exactly:
// report findings derive their reasons from object STATUS (a container
// waiting.reason is already the specific ImagePullBackOff, never
// kubelet's generic BackOff spelling), so the message-aware variant
// would change nothing — and choosing differently here would split one
// incident across two subject keys the moment the two paths disagreed.
func ObservationOf(f emit.Finding, cluster string) Observation {
	reason := engine.CanonicalReason(f.Reason)
	return Observation{
		SubjectKey:   SubjectKey(cluster, f.Namespace, f.KindOfObject, f.Name, reason),
		Fingerprint:  f.Fingerprint,
		Cluster:      cluster,
		Namespace:    f.Namespace,
		KindOfObject: f.KindOfObject,
		Name:         f.Name,
		Reason:       reason,
		Severity:     f.Severity,
		Message:      f.Message,
	}
}

// ParseReport reads a §4.2 finding stream — the output of `lookout
// health`, `lookout state pods`, or any other read-path command — and
// returns its findings.
//
// Both wire formats are accepted, detected per line, because the
// obvious invocation is a pipe and logfmt is the DEFAULT format:
// requiring `--format=json` upstream would make
// `lookout health | lookout findings diff --report -` silently parse
// nothing. A line beginning with `{` is JSON; anything else is logfmt.
//
// Skipped without error: blank lines, and the mandatory terminating
// summary line (§4.2 `scanned=… findings=… elapsed=…`), which is
// identified by carrying no `kind` — the one field every finding must
// have and the summary never does. Being tolerant here is deliberate:
// a report is an OUTPUT contract being re-read as input, and the
// summary is part of it.
//
// A line that parses but carries no `kind` and no summary shape is a
// hard error rather than a skip — silently dropping records from a
// transition surface would report false `resolved` transitions, which
// is precisely the failure this package exists to prevent.
func ParseReport(r io.Reader, cluster string) ([]Observation, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxReportLine)

	var out []Observation
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		fields, err := parseRecord(text)
		if err != nil {
			return nil, fmt.Errorf("report line %d: %w", line, err)
		}
		if _, ok := fields["kind"]; !ok {
			if isSummary(fields) {
				continue
			}
			return nil, fmt.Errorf("report line %d: record has no kind and is not the summary line: %q", line, truncateForError(text))
		}
		out = append(out, ObservationOf(emit.Finding{
			Kind:         fields["kind"],
			Severity:     fields["severity"],
			Namespace:    fields["namespace"],
			KindOfObject: fields["kind_of_object"],
			Name:         fields["name"],
			Reason:       fields["reason"],
			Message:      fields["message"],
			Fingerprint:  fields["fingerprint"],
		}, cluster))
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read report: %w", err)
	}
	return out, nil
}

// isSummary reports whether a kind-less record is the §4.2 summary
// line, which always carries scanned and findings.
func isSummary(fields map[string]string) bool {
	_, scanned := fields["scanned"]
	_, found := fields["findings"]
	return scanned && found
}

// parseRecord decodes one report line in whichever format it is in.
func parseRecord(text string) (map[string]string, error) {
	if strings.HasPrefix(text, "{") {
		return parseJSONRecord(text)
	}
	return parseLogfmtRecord(text)
}

// parseJSONRecord decodes a --format=json line. Values are decoded
// through json.RawMessage and stringified, because the summary line
// encodes scanned/findings as NUMBERS while every finding field is a
// string — decoding straight into map[string]string would fail on the
// one line we most need to skip.
func parseJSONRecord(text string) (map[string]string, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return nil, fmt.Errorf("malformed JSON record: %w", err)
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		var s string
		if err := json.Unmarshal(v, &s); err == nil {
			out[k] = s
			continue
		}
		out[k] = strings.TrimSpace(string(v))
	}
	return out, nil
}

// parseLogfmtRecord decodes a logfmt line, the exact inverse of
// emit.EncodeLogfmt: space-separated key=value, values quoted with Go
// syntax when they contain a space, `=`, `"`, or a control character.
func parseLogfmtRecord(text string) (map[string]string, error) {
	out := make(map[string]string, 8)
	for i := 0; i < len(text); {
		if text[i] == ' ' {
			i++
			continue
		}
		eq := strings.IndexByte(text[i:], '=')
		if eq < 0 {
			return nil, fmt.Errorf("malformed logfmt record: no `=` in %q", truncateForError(text[i:]))
		}
		key := text[i : i+eq]
		i += eq + 1
		if key == "" {
			return nil, fmt.Errorf("malformed logfmt record: empty key in %q", truncateForError(text))
		}
		if i < len(text) && text[i] == '"' {
			// Quoted value: find the closing quote, honoring escapes,
			// then unquote with the same encoder Go's strconv.Quote used.
			end := i + 1
			for end < len(text) {
				if text[end] == '\\' {
					end += 2
					continue
				}
				if text[end] == '"' {
					break
				}
				end++
			}
			if end >= len(text) {
				return nil, fmt.Errorf("malformed logfmt record: unterminated quoted value for key %q", key)
			}
			v, err := strconv.Unquote(text[i : end+1])
			if err != nil {
				return nil, fmt.Errorf("malformed logfmt record: bad quoted value for key %q: %w", key, err)
			}
			out[key] = v
			i = end + 1
			continue
		}
		sp := strings.IndexByte(text[i:], ' ')
		if sp < 0 {
			out[key] = text[i:]
			break
		}
		out[key] = text[i : i+sp]
		i += sp + 1
	}
	return out, nil
}

// truncateForError bounds a quoted fragment in an error message: a
// malformed 1 MiB line should not become a 1 MiB error string.
func truncateForError(s string) string {
	const max = 120
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
