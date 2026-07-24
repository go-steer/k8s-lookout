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

// Per-line parsing: timestamp extraction and log-level guessing.

import (
	"regexp"
	"strings"
	"time"
)

// Guessed log levels, ranked so a cluster keeps the worst level it
// has seen. These are heuristics over line content — a *guess*, as
// the output glossary says — never a promise.
const (
	levelUnknown = iota
	levelDebug
	levelInfo
	levelWarn
	levelError
	levelFatal
)

var levelNames = map[int]string{
	levelDebug: "debug",
	levelInfo:  "info",
	levelWarn:  "warn",
	levelError: "error",
	levelFatal: "fatal",
}

// splitTimestamp splits a leading timestamp off a raw log line. The
// fetcher requests kubelet timestamps (PodLogOptions.Timestamps), so
// the dominant case is an RFC3339Nano first token, which is both
// parsed and *stripped* — it would otherwise defeat clustering and
// burn tokens. App-emitted "2006-01-02 15:04:05" style prefixes are
// parsed but left in the text (the tokenizer masks them); anything
// else yields a zero time.
func splitTimestamp(raw string) (time.Time, string) {
	if sp := strings.IndexByte(raw, ' '); sp > 0 {
		if ts, err := time.Parse(time.RFC3339Nano, raw[:sp]); err == nil {
			return ts, raw[sp+1:]
		}
	}
	for _, layout := range []string{"2006-01-02 15:04:05", "2006/01/02 15:04:05"} {
		if len(raw) >= len(layout) {
			if ts, err := time.Parse(layout, raw[:len(layout)]); err == nil {
				return ts, raw
			}
		}
	}
	return time.Time{}, raw
}

// reLevelKV matches structured level fields: level=error,
// "level":"warn", severity=INFO, lvl=dbg — the common logfmt/JSON
// spellings.
var reLevelKV = regexp.MustCompile(`(?i)"?\b(?:level|lvl|severity)"?\s*[:=]\s*"?([a-zA-Z]+)`)

// reKlog matches the klog/glog header token: I0724, W0724, E0724,
// F0724.
var reKlog = regexp.MustCompile(`^[IWEF]\d{4}$`)

// guessLevel guesses a line's log level: a structured level field
// wins; otherwise the first few tokens are checked for level words
// (bare, bracketed, or a klog header).
func guessLevel(text string) int {
	if m := reLevelKV.FindStringSubmatch(text); m != nil {
		if lvl := levelWord(m[1]); lvl != levelUnknown {
			return lvl
		}
	}
	fields := strings.Fields(text)
	for i, f := range fields {
		if i >= 8 {
			break
		}
		if i == 0 && reKlog.MatchString(f) {
			switch f[0] {
			case 'F':
				return levelFatal
			case 'E':
				return levelError
			case 'W':
				return levelWarn
			case 'I':
				return levelInfo
			}
		}
		if lvl := levelWord(strings.Trim(f, `[](){}<>:"',`)); lvl != levelUnknown {
			return lvl
		}
	}
	return levelUnknown
}

// levelWord maps one word to a level. Only whole-word spellings the
// major loggers actually emit; substring matching would tag every
// "/error" URL path.
func levelWord(w string) int {
	switch strings.ToUpper(w) {
	case "FATAL", "PANIC", "CRITICAL", "CRIT":
		return levelFatal
	case "ERROR", "ERR", "ERRO", "SEVERE":
		return levelError
	case "WARN", "WARNING":
		return levelWarn
	case "INFO", "NOTICE":
		return levelInfo
	case "DEBUG", "TRACE", "FINE", "FINER", "FINEST":
		return levelDebug
	}
	return levelUnknown
}
