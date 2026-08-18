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
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// AccessLog records one line per tool call (issue #281). Until this
// existed the server logged nothing at all: an agent misbehaving
// against lookout left no evidence that a call had even happened, so
// debugging one meant adding print statements to the server.
//
// What a line carries is deliberately narrow — when, which tool, how
// it ended, how long it took, how big the answer was:
//
//	ts=2026-08-18T14:03:21Z tool=k8s_scan exit=0 dur=1.204s bytes=4096
//
// Not the arguments, and not the response body. The §6.5 sanitizer
// guarantees no secret value reaches an output surface; a log that
// copied payloads would be a second place that guarantee has to hold,
// audited by nobody. Tool, outcome, and size answer the operational
// questions (what was called, did it work, what did it cost) without
// becoming a second data path.
//
// The encoding is emit.EncodeLogfmt — the same one findings use — so
// an access log is readable by the same parser as everything else
// lookout writes, and appending to a file that is being rotated
// underneath us is safe.
//
// Safe for concurrent use: the HTTP transport can have several calls
// in flight, and a half-interleaved line is worse than no line.
type AccessLog struct {
	mu    sync.Mutex
	out   io.Writer
	clock func() time.Time
}

// NewAccessLog writes records to out. A nil out yields a nil
// *AccessLog, which records nothing — the no-log default is the same
// code path as the logging one, minus the write.
func NewAccessLog(out io.Writer) *AccessLog {
	if out == nil {
		return nil
	}
	return &AccessLog{out: out, clock: time.Now}
}

// now is the log's clock, and works on a nil *AccessLog so the
// handler can time a call the same way whether logging is on or off.
// Tests substitute a fake so both the timestamp and the duration in a
// recorded line are deterministic.
func (l *AccessLog) now() time.Time {
	if l == nil {
		return time.Now()
	}
	return l.clock()
}

// OpenAccessLog appends to path, creating it if needed, and returns
// the log plus the file to close.
//
// Append rather than truncate: the log outlives one server process,
// and a supervisor that restarts `lookout mcp` must not erase the
// evidence from the run that made it restart. Mode 0600 because the
// tool names alone say which clusters an operator has been reading.
func OpenAccessLog(path string) (*AccessLog, io.Closer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("--access-log=%s: %v", path, err)
	}
	return NewAccessLog(f), f, nil
}

// Record writes one call that began at start; the duration is
// measured against the clock at the moment Record runs. A nil
// *AccessLog is a no-op, so callers never branch on whether logging
// is configured.
//
// ts is when the call arrived, not when it finished — a slow tool
// should sort next to the calls it was concurrent with, which is the
// question an operator is asking when they read this file.
//
// A write failure is dropped on purpose. The alternative is failing a
// tool call that already succeeded because its audit line could not be
// persisted, and on stdio there is nowhere to report the failure that
// is not the protocol stream itself.
func (l *AccessLog) Record(tool string, exit int, start time.Time, bytes int) {
	if l == nil {
		return
	}
	dur := l.clock().Sub(start)
	l.mu.Lock()
	defer l.mu.Unlock()
	line := emit.EncodeLogfmt([]emit.Field{
		{Key: "ts", Value: start.UTC().Format(time.RFC3339)},
		{Key: "tool", Value: tool},
		{Key: "exit", Value: fmt.Sprint(exit)},
		// Truncated to milliseconds for the same reason the summary
		// line's elapsed= is: a short token, and nobody tunes an agent
		// on nanoseconds.
		{Key: "dur", Value: dur.Truncate(time.Millisecond).String()},
		{Key: "bytes", Value: fmt.Sprint(bytes)},
	})
	_, _ = l.out.Write(line)
}
