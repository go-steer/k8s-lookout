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

package emit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// Format selects the finding encoding. Both formats emit one record
// per line and the same terminating summary; logfmt is the default
// because it is the more token-dense of the two.
type Format string

const (
	FormatLogfmt Format = "logfmt"
	FormatJSON   Format = "json"
)

// ParseFormat maps a --format flag value to a Format.
func ParseFormat(s string) (Format, error) {
	switch Format(s) {
	case FormatLogfmt, FormatJSON:
		return Format(s), nil
	}
	return "", fmt.Errorf("unknown format %q (want logfmt or json)", s)
}

// Sanitizer rewrites a finding before it is encoded. Every Writer
// passes every finding through exactly one Sanitizer — this is the
// §6.5 seam: no output path exists that bypasses it.
type Sanitizer func(Finding) Finding

// DefaultSanitizer is applied by every Writer unless overridden with
// WithSanitizer (tests only — production surfaces must not weaken
// it). It is the real §6.5 finding sanitizer: secret masking on
// every Emit, on every surface, not opt-in per check.
var DefaultSanitizer Sanitizer = SanitizeFinding

// Writer encodes sanitized findings to one output stream and counts
// them for the summary line. Not safe for concurrent use; checks are
// single-goroutine emitters by design (deterministic output order).
type Writer struct {
	out      io.Writer
	format   Format
	sanitize Sanitizer
	findings int
}

// WriterOption customizes a Writer.
type WriterOption func(*Writer)

// WithSanitizer overrides the sanitizer. Only tests should relax it;
// every production surface runs DefaultSanitizer (SanitizeFinding).
func WithSanitizer(s Sanitizer) WriterOption {
	return func(w *Writer) { w.sanitize = s }
}

// NewWriter returns a Writer emitting the given format to out.
func NewWriter(out io.Writer, format Format, opts ...WriterOption) (*Writer, error) {
	if _, err := ParseFormat(string(format)); err != nil {
		return nil, err
	}
	w := &Writer{out: out, format: format, sanitize: DefaultSanitizer}
	for _, opt := range opts {
		opt(w)
	}
	return w, nil
}

// Emit sanitizes, validates, and writes one finding as one line.
func (w *Writer) Emit(f Finding) error {
	f = w.sanitize(f)
	if err := f.validate(); err != nil {
		return err
	}
	var line []byte
	switch w.format {
	case FormatJSON:
		line = encodeJSON(f.pairs())
	default:
		line = encodeLogfmt(f.pairs())
	}
	if _, err := w.out.Write(line); err != nil {
		return err
	}
	w.findings++
	return nil
}

// Findings reports how many findings have been emitted so far.
func (w *Writer) Findings() int { return w.findings }

// Summary writes the mandatory terminating line of every successful
// invocation: `scanned=<n> findings=<n> elapsed=<d>` (§4.2). In JSON
// format it is a JSON object with exactly those keys, so a
// line-oriented consumer handles both formats identically. The
// findings count comes from the Writer itself — it cannot drift from
// what was actually emitted.
func (w *Writer) Summary(scanned int, elapsed time.Duration) error {
	var err error
	switch w.format {
	case FormatJSON:
		_, err = fmt.Fprintf(w.out, "{\"scanned\":%d,\"findings\":%d,\"elapsed\":%q}\n",
			scanned, w.findings, elapsed.String())
	default:
		_, err = fmt.Fprintf(w.out, "scanned=%d findings=%d elapsed=%s\n",
			scanned, w.findings, elapsed.String())
	}
	return err
}

// encodeLogfmt renders ordered pairs as one logfmt line. Values are
// quoted only when they contain characters that would break
// splitting on spaces — token density is the point of the default
// format.
func encodeLogfmt(pairs []Field) []byte {
	var b bytes.Buffer
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(p.Key)
		b.WriteByte('=')
		if logfmtNeedsQuoting(p.Value) {
			b.WriteString(strconv.Quote(p.Value))
		} else {
			b.WriteString(p.Value)
		}
	}
	b.WriteByte('\n')
	return b.Bytes()
}

func logfmtNeedsQuoting(v string) bool {
	if strings.ContainsAny(v, " =\"") {
		return true
	}
	for _, r := range v {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

// encodeJSON renders ordered pairs as one flat JSON object per line.
// Hand-assembled rather than marshaling a map so key order matches
// the logfmt encoding exactly — ordered records are part of the
// contract, not a logfmt quirk.
func encodeJSON(pairs []Field) []byte {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte(',')
		}
		// Keys match keyPattern (validated), so only the value
		// needs JSON escaping.
		b.WriteByte('"')
		b.WriteString(p.Key)
		b.WriteString(`":`)
		v, _ := json.Marshal(p.Value) // string marshaling cannot fail
		b.Write(v)
	}
	b.WriteString("}\n")
	return b.Bytes()
}
