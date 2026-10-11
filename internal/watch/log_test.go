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
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestNewLogger_LevelsAndFormats(t *testing.T) {
	tests := []struct {
		name      string
		level     string
		format    string
		wantDebug bool
		wantInfo  bool
		wantWarn  bool
		wantError bool
	}{
		{
			name:      "debug level emits all",
			level:     "debug",
			format:    "text",
			wantDebug: true,
			wantInfo:  true,
			wantWarn:  true,
			wantError: true,
		},
		{
			name:      "info level filters debug",
			level:     "info",
			format:    "text",
			wantDebug: false,
			wantInfo:  true,
			wantWarn:  true,
			wantError: true,
		},
		{
			name:      "warn level filters info and debug",
			level:     "warn",
			format:    "json",
			wantDebug: false,
			wantInfo:  false,
			wantWarn:  true,
			wantError: true,
		},
		{
			name:      "error level emits only errors",
			level:     "ERROR",
			format:    "JSON",
			wantDebug: false,
			wantInfo:  false,
			wantWarn:  false,
			wantError: true,
		},
		{
			name:      "default fallback level and format",
			level:     "",
			format:    "",
			wantDebug: false,
			wantInfo:  true,
			wantWarn:  true,
			wantError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			l := NewLogger(tc.level, tc.format, &buf)
			l.Debug("msg-debug")
			l.Info("msg-info")
			l.Warn("msg-warn")
			l.Error("msg-error")

			out := buf.String()
			assertContains(t, out, "msg-debug", tc.wantDebug)
			assertContains(t, out, "msg-info", tc.wantInfo)
			assertContains(t, out, "msg-warn", tc.wantWarn)
			assertContains(t, out, "msg-error", tc.wantError)
		})
	}
}

func assertContains(t *testing.T, haystack, needle string, want bool) {
	t.Helper()
	got := strings.Contains(haystack, needle)
	if got != want {
		t.Errorf("contains(%q) = %v, want %v in output:\n%s", needle, got, want, haystack)
	}
}

func TestNewLogger_JSONAndStructuredAttrs(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger("info", "json", &buf)
	l.Info("signal dispatched",
		AttrCluster("prod-us-central1"),
		AttrSessionID("sess-42"),
		AttrFingerprint("sha256:abcdef0123"),
		AttrKind("pod.crashloop"),
	)

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("expected valid JSON line, got err=%v: %s", err, buf.String())
	}
	wantFields := map[string]string{
		"msg":         "signal dispatched",
		"cluster":     "prod-us-central1",
		"session_id":  "sess-42",
		"fingerprint": "sha256:abcdef0123",
		"kind":        "pod.crashloop",
	}
	for k, want := range wantFields {
		if got, _ := rec[k].(string); got != want {
			t.Errorf("json field %q = %q, want %q", k, got, want)
		}
	}
}

func TestNewLogger_NilWriterIsSafe(t *testing.T) {
	l := NewLogger("debug", "text", nil)
	l.Info("discarded safely", AttrCluster("local"))
}
