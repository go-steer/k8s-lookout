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

// Multi-line stack-trace handling (DESIGN.md §5): Go panics, Java
// exceptions, and Python tracebacks are detected as blocks, collapsed
// to their top frames, and clustered by those frames — a 60-line
// goroutine dump becomes one finding with five frames, and two panics
// through the same call path become one finding with count=2.
//
// The detector is a per-stream state machine (traces never interleave
// within one container stream; streams are fed separately). Lines
// that only *might* start a trace (a Java exception header) are held
// until the next line confirms (`at ...`) or refutes them, in which
// case they are released to the normal clustering path in order.

import (
	"strings"
	"time"
)

// maxFrames is the frame budget per collapsed trace: top-5, per the
// design row.
const maxFrames = 5

// trace is one completed stack-trace block.
type trace struct {
	lang   string // go|java|python
	head   string // the message line (panic:/exception/error line)
	frames []string
	first  time.Time
	last   time.Time
}

// stackCluster groups identical traces (same language + top frames).
type stackCluster struct {
	lang     string
	frames   []string
	template string
	count    int
	pods     map[podKey]struct{}
	first    time.Time
	last     time.Time
	sample   string
}

// Detector states.
const (
	sIdle = iota
	sJavaCand
	sGo
	sJava
	sPy
)

type stackDetector struct {
	state int
	held  entry  // the unconfirmed Java exception header (sJavaCand)
	cur   *trace // the trace being collected (sGo/sJava/sPy)
}

// feed consumes one line. It returns any lines that turned out not
// to be part of a trace (to be clustered normally, in order) and a
// completed trace when one just ended.
func (d *stackDetector) feed(en entry) (release []entry, done *trace) {
	switch d.state {
	case sJavaCand:
		if isJavaAt(en.text) {
			d.cur = &trace{lang: "java", head: d.held.text, first: d.held.ts, last: d.held.ts}
			d.state = sJava
			d.consumeJava(en)
			return nil, nil
		}
		rel := []entry{d.held}
		d.state = sIdle
		more, tr := d.feed(en) // reprocess from idle; cannot complete a trace
		return append(rel, more...), tr

	case sGo:
		if d.consumeGo(en) {
			return nil, nil
		}
		done = d.finish()
		release, _ = d.feed(en) // from idle: may start a new block, never completes one
		return release, done

	case sJava:
		if d.consumeJava(en) {
			return nil, nil
		}
		done = d.finish()
		release, _ = d.feed(en)
		return release, done

	case sPy:
		return nil, d.consumePy(en)
	}

	// sIdle
	switch {
	case strings.HasPrefix(en.text, "panic:") || strings.HasPrefix(en.text, "fatal error:"):
		d.cur = &trace{lang: "go", head: en.text, first: en.ts, last: en.ts}
		d.state = sGo
		return nil, nil
	case strings.HasPrefix(en.text, "Traceback (most recent call last):"):
		d.cur = &trace{lang: "python", head: en.text, first: en.ts, last: en.ts}
		d.state = sPy
		return nil, nil
	case isJavaExcHeader(en.text):
		d.held = en
		d.state = sJavaCand
		return nil, nil
	}
	return []entry{en}, nil
}

// flush ends the stream: any held candidate is released, any open
// trace is completed as-is.
func (d *stackDetector) flush() (release []entry, done *trace) {
	switch d.state {
	case sJavaCand:
		release = []entry{d.held}
		d.state = sIdle
	case sGo, sJava, sPy:
		done = d.finish()
	}
	return release, done
}

// finish closes the current trace and resets to idle.
func (d *stackDetector) finish() *trace {
	tr := d.cur
	d.cur = nil
	d.state = sIdle
	if tr != nil && tr.lang == "python" {
		// Python prints frames outermost-first; the interesting
		// "top" frames are the innermost — reverse, then cap.
		for i, j := 0, len(tr.frames)-1; i < j; i, j = i+1, j-1 {
			tr.frames[i], tr.frames[j] = tr.frames[j], tr.frames[i]
		}
	}
	if tr != nil && len(tr.frames) > maxFrames {
		tr.frames = tr.frames[:maxFrames]
	}
	return tr
}

func (d *stackDetector) touch(en entry) {
	if !en.ts.IsZero() {
		if d.cur.first.IsZero() {
			d.cur.first = en.ts
		}
		d.cur.last = en.ts
	}
}

// --- Go ---------------------------------------------------------------

// consumeGo reports whether a line belongs to a Go panic dump:
// goroutine headers, tab-indented file:line frames, "created by",
// the "[signal ...]" line, blank separators between goroutine
// blocks, and package-qualified call lines (`main.(*Server).run(...)`)
// whose function names become the frames.
func (d *stackDetector) consumeGo(en entry) bool {
	t := en.text
	switch {
	case t == "":
	case strings.HasPrefix(t, "goroutine ") && strings.Contains(t, "["):
	case strings.HasPrefix(t, "\t"):
	case strings.HasPrefix(t, "created by "):
	case strings.HasPrefix(t, "[signal "):
	case strings.HasPrefix(t, "exit status "):
	default:
		name, ok := goCallName(t)
		if !ok {
			return false
		}
		if len(d.cur.frames) < maxFrames {
			d.cur.frames = append(d.cur.frames, name)
		}
	}
	d.touch(en)
	return true
}

// goCallName extracts the function name from a Go stack call line.
// The name is everything before the final argument list's opening
// paren and is dot-qualified with no spaces: `main.(*Server).run(...)`
// → `main.(*Server).run`.
func goCallName(t string) (string, bool) {
	if !strings.HasSuffix(t, ")") {
		return "", false
	}
	i := strings.LastIndexByte(t, '(')
	if i <= 0 {
		return "", false
	}
	name := t[:i]
	if strings.ContainsAny(name, " \t") || !strings.Contains(name, ".") {
		return "", false
	}
	return name, true
}

// --- Java -------------------------------------------------------------

// isJavaExcHeader matches lines that may start a Java stack trace:
// "Exception in thread ..." or a (possibly package-qualified) class
// ending in Exception/Error, optionally with a message. Confirmation
// still requires a following `at ...` frame line.
func isJavaExcHeader(t string) bool {
	if strings.HasPrefix(t, "Exception in thread ") {
		return true
	}
	if t == "" || t[0] == ' ' || t[0] == '\t' {
		return false
	}
	first, _, _ := strings.Cut(t, " ")
	first = strings.TrimSuffix(first, ":")
	return strings.HasSuffix(first, "Exception") || strings.HasSuffix(first, "Error")
}

// isJavaAt matches a Java frame line: `at pkg.Class.method(File.java:42)`.
func isJavaAt(t string) bool {
	tr := strings.TrimLeft(t, " \t")
	return strings.HasPrefix(tr, "at ") && strings.HasSuffix(tr, ")")
}

// consumeJava reports whether a line continues a Java trace: frame
// lines, "Caused by:", "... N more", and "Suppressed:" entries.
// Frames are taken from the outermost exception's top only.
func (d *stackDetector) consumeJava(en entry) bool {
	t := strings.TrimLeft(en.text, " \t")
	switch {
	case isJavaAt(en.text):
		if len(d.cur.frames) < maxFrames {
			if name, ok := javaFrameName(t); ok {
				d.cur.frames = append(d.cur.frames, name)
			}
		}
	case strings.HasPrefix(t, "Caused by: "):
	case strings.HasPrefix(t, "Suppressed: "):
	case strings.HasPrefix(t, "... ") && strings.HasSuffix(t, " more"):
	default:
		return false
	}
	d.touch(en)
	return true
}

// javaFrameName extracts `pkg.Class.method` from a trimmed `at ...(...)`
// line.
func javaFrameName(tr string) (string, bool) {
	body := strings.TrimPrefix(tr, "at ")
	i := strings.IndexByte(body, '(')
	if i <= 0 {
		return "", false
	}
	return strings.TrimSpace(body[:i]), true
}

// --- Python -----------------------------------------------------------

// consumePy consumes lines of a Python traceback. Indented lines are
// `File "...", line N, in func` frames or source echoes; the first
// non-indented line is the exception message, which terminates (and
// belongs to) the trace.
func (d *stackDetector) consumePy(en entry) *trace {
	t := en.text
	if t == "" || strings.HasPrefix(t, "  ") {
		if name, ok := pyFrameName(t); ok {
			d.cur.frames = append(d.cur.frames, name)
		}
		d.touch(en)
		return nil
	}
	d.cur.head = t
	d.touch(en)
	return d.finish()
}

// pyFrameName renders `  File "/app/svc/db.py", line 41, in connect`
// as `db.py:41:connect`.
func pyFrameName(t string) (string, bool) {
	tr := strings.TrimLeft(t, " ")
	if !strings.HasPrefix(tr, `File "`) {
		return "", false
	}
	rest := tr[len(`File "`):]
	file, rest, ok := strings.Cut(rest, `"`)
	if !ok {
		return "", false
	}
	if i := strings.LastIndexByte(file, '/'); i >= 0 {
		file = file[i+1:]
	}
	name := file
	if _, after, ok := strings.Cut(rest, "line "); ok {
		line, afterLine, _ := strings.Cut(after, ",")
		name += ":" + strings.TrimSpace(line)
		if _, fn, ok := strings.Cut(afterLine, "in "); ok {
			name += ":" + strings.TrimSpace(fn)
		}
	}
	return name, true
}
