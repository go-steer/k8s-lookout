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

// Exempter answers whether a reviewed exemption covers a finding
// (issue #234). It is deliberately narrow — three strings in, an
// annotation out — so the loader (pkg/exempt) stays free of any
// dependency on this package and the Writer stays free of YAML.
//
// The contract is ANNOTATE, never drop: a true result adds the reason
// and expiry to the finding and increments the exemption count on the
// summary line. Nothing is withheld. A consumer that wants the quiet
// view filters on data it can see; lookout does not hide.
type Exempter interface {
	Exempt(kind, namespace, name string) (reason, expires string, ok bool)
}

// Writer encodes sanitized findings to one output stream and counts
// them for the summary line. Not safe for concurrent use; checks are
// single-goroutine emitters by design (deterministic output order).
type Writer struct {
	out      io.Writer
	format   Format
	sanitize Sanitizer
	exempt   Exempter
	findings int
	exempted int
	notes    []Field
}

// WriterOption customizes a Writer.
type WriterOption func(*Writer)

// WithSanitizer overrides the sanitizer. Only tests should relax it;
// every production surface runs DefaultSanitizer (SanitizeFinding).
func WithSanitizer(s Sanitizer) WriterOption {
	return func(w *Writer) { w.sanitize = s }
}

// WithExemptions makes the Writer annotate findings covered by a
// reviewed exemption and report the count in the summary line. Passing
// a nil Exempter is the same as not passing one at all.
//
// This sits on the Writer for the same reason the sanitizer does: it
// is the one place every finding passes through, so no command can
// forget to apply it and no output path can bypass it.
func WithExemptions(e Exempter) WriterOption {
	return func(w *Writer) { w.exempt = e }
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

// Emit sanitizes, annotates, validates, and writes one finding as one
// line.
//
// Exemption annotation happens AFTER sanitizing, so a check cannot
// launder a secret through an exemption reason, and BEFORE validation,
// so a malformed annotation fails in the same place a malformed
// finding does.
func (w *Writer) Emit(f Finding) error {
	f = w.sanitize(f)
	exempted := false
	if w.exempt != nil {
		if reason, expires, ok := w.exempt.Exempt(f.Kind, f.Namespace, f.Name); ok {
			f.ExemptReason, f.ExemptExpires = reason, expires
			exempted = true
		}
	}
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
	// Both counters move only on a finding that actually reached the
	// stream, so exempt=<n> can never claim more than findings=<n>
	// accounts for.
	w.findings++
	if exempted {
		w.exempted++
	}
	return nil
}

// Findings reports how many findings have been emitted so far.
func (w *Writer) Findings() int { return w.findings }

// Exempted reports how many emitted findings carried an exemption
// annotation.
func (w *Writer) Exempted() int { return w.exempted }

// Note records one summary-line annotation, appended after the
// mandatory scanned/findings/elapsed keys in the order first set
// (setting a key again replaces its value in place). This is the
// §6.6 "say so in the summary line" seam: graph-backed commands
// stamp source=live|history (and the resolved at=) so a consumer
// always knows WHICH topology answered — the line every stream ends
// with is the one place that cannot be missed. Keys follow the same
// charset contract as finding keys and must not shadow the summary's
// own keys; note keys a command emits belong in its output glossary
// like any Details key (the §13 contract tests enforce both).
func (w *Writer) Note(key, value string) error {
	if !keyPattern.MatchString(key) {
		return fmt.Errorf("summary note key %q does not match %s", key, keyPattern)
	}
	switch key {
	case "scanned", "findings", "elapsed":
		return fmt.Errorf("summary note key %q shadows a mandatory summary key", key)
	}
	for _, reserved := range SummaryNoteFields() {
		if key == reserved {
			return fmt.Errorf("summary note key %q is owned by the Writer and must not be set by a check", key)
		}
	}
	for i := range w.notes {
		if w.notes[i].Key == key {
			w.notes[i].Value = value
			return nil
		}
	}
	w.notes = append(w.notes, Field{Key: key, Value: value})
	return nil
}

// Summary writes the mandatory terminating line of every successful
// invocation: `scanned=<n> findings=<n> elapsed=<d>` (§4.2), plus any
// recorded Note annotations after those three keys. In JSON format it
// is a JSON object with the same keys in the same order, so a
// line-oriented consumer handles both formats identically. The
// findings count comes from the Writer itself — it cannot drift from
// what was actually emitted.
//
// When an exemption file was supplied, `exempt=<n>` is appended last,
// ALWAYS — including exempt=0. That is the point of the §6.6 seam:
// "this scan ran with exemptions in effect, and none of them fired" is
// a fact a reader needs, and it is not the same fact as "no exemptions
// were configured", which emits no key at all.
func (w *Writer) Summary(scanned int, elapsed time.Duration) error {
	pairs := make([]Field, 0, 4+len(w.notes))
	pairs = append(pairs,
		Field{Key: "scanned", Value: strconv.Itoa(scanned)},
		Field{Key: "findings", Value: strconv.Itoa(w.findings)},
		Field{Key: "elapsed", Value: elapsed.String()},
	)
	pairs = append(pairs, w.notes...)
	if w.exempt != nil {
		pairs = append(pairs, Field{Key: "exempt", Value: strconv.Itoa(w.exempted)})
	}
	var line []byte
	switch w.format {
	case FormatJSON:
		line = encodeJSONSummary(pairs, scanned, w.findings)
	default:
		line = encodeLogfmt(pairs)
	}
	_, err := w.out.Write(line)
	return err
}

// encodeJSONSummary renders the summary pairs as one JSON object,
// keeping scanned/findings numeric (they always were) and everything
// else a string.
func encodeJSONSummary(pairs []Field, scanned, findings int) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "{\"scanned\":%d,\"findings\":%d", scanned, findings)
	for _, p := range pairs[2:] {
		v, _ := json.Marshal(p.Value) // string marshaling cannot fail
		fmt.Fprintf(&b, ",%q:%s", p.Key, v)
	}
	b.WriteString("}\n")
	return b.Bytes()
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
