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

package version

import (
	"strings"
	"testing"
)

func TestFormatVersion(t *testing.T) {
	t.Parallel()
	got := formatVersion("lookout", "v0.12.0", "0123456789abcdef", "2026-07-30T12:00:00Z", false)
	want := "lookout v0.12.0 (commit 01234567, built 2026-07-30T12:00:00Z)"
	if got != want {
		t.Errorf("formatVersion = %q, want %q", got, want)
	}
	dirty := formatVersion("lookout", "v0.13.0-dev", "abc", "unknown", true)
	if !strings.Contains(dirty, "commit abc, modified") {
		t.Errorf("dirty format = %q, want short-sha + modified marker", dirty)
	}
}

func TestResolveBuildInfo_LdflagsWin(t *testing.T) {
	t.Parallel()
	v, c, d, dirty := resolveBuildInfo("v0.12.0", "deadbeef", "2026-07-30")
	if v != "v0.12.0" || c != "deadbeef" || d != "2026-07-30" || dirty {
		t.Errorf("injected values must be authoritative; got %s %s %s dirty=%v", v, c, d, dirty)
	}
}

func TestString_LeadsWithProgAndVersion(t *testing.T) {
	t.Parallel()
	fields := strings.Fields(String("lookout"))
	if len(fields) < 2 || fields[0] != "lookout" || !strings.HasPrefix(fields[1], "v") {
		t.Errorf("String() = %q, want 'lookout v...' leading tokens", String("lookout"))
	}
}
