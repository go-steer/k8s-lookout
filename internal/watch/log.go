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

package watch

import (
	"io"
	"log/slog"
	"strings"
)

// NewLogger constructs a structured *slog.Logger configured for the sentinel
// watch path.
//
// Supported levels (case-insensitive): "debug", "info" (default), "warn", "error".
// Supported formats (case-insensitive): "text" (default) and "json".
// If w is nil, output is discarded rather than panicking.
func NewLogger(level, format string, w io.Writer) *slog.Logger {
	if w == nil {
		w = io.Discard
	}
	opts := &slog.HandlerOptions{
		Level: parseLogLevel(level),
	}
	if strings.EqualFold(strings.TrimSpace(format), "json") {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// AttrCluster returns the canonical structured log attribute naming a cluster.
func AttrCluster(cluster string) slog.Attr {
	return slog.String("cluster", cluster)
}

// AttrSessionID returns the canonical structured log attribute naming an incident session ID.
func AttrSessionID(sessionID string) slog.Attr {
	return slog.String("session_id", sessionID)
}

// AttrFingerprint returns the canonical structured log attribute naming a signal fingerprint.
func AttrFingerprint(fingerprint string) slog.Attr {
	return slog.String("fingerprint", fingerprint)
}

// AttrKind returns the canonical structured log attribute naming a signal or finding kind.
func AttrKind(kind string) slog.Attr {
	return slog.String("kind", kind)
}
