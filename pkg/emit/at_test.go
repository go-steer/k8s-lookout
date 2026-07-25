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
	"context"
	"strings"
	"testing"
	"time"
)

func TestParseAt(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		in      string
		want    time.Time
		wantErr bool
	}{
		{in: "", want: time.Time{}},
		{in: "20m", want: now.Add(-20 * time.Minute)},
		{in: "1h30m", want: now.Add(-90 * time.Minute)},
		{in: "0s", want: now},
		{in: "2026-07-25T10:00:00Z", want: time.Date(2026, 7, 25, 10, 0, 0, 0, time.UTC)},
		{in: "2026-07-25T10:00:00+02:00", want: time.Date(2026, 7, 25, 8, 0, 0, 0, time.UTC)},
		{in: "-20m", wantErr: true},
		{in: "yesterday", wantErr: true},
		{in: "2026-07-25", wantErr: true}, // date without time is not RFC3339
	} {
		got, err := ParseAt(tc.in, now)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseAt(%q): want error, got %v", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseAt(%q): %v", tc.in, err)
			continue
		}
		if !got.Equal(tc.want) {
			t.Errorf("ParseAt(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// runGraphBacked executes Run with GraphBacked set.
func runGraphBacked(t *testing.T, check CheckFunc, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errBuf bytes.Buffer
	code = Run(context.Background(), RunConfig{
		Name:        "lookout test graph-check",
		Check:       check,
		GraphBacked: true,
		Stdout:      &out,
		Stderr:      &errBuf,
		Now:         fixedClock(),
	}, args)
	return code, out.String(), errBuf.String()
}

// TestAtFlag_RejectedOnLiveOnlyCommands: commands that do not
// declare GraphBacked reject --at/--store as unknown flags (§4.2:
// only graph-backed commands accept them) — a usage error, not a
// silently ignored time.
func TestAtFlag_RejectedOnLiveOnlyCommands(t *testing.T) {
	for _, arg := range []string{"--at=20m", "--store=/tmp/x.db"} {
		code, stdout, stderr := runOnce(t, emitOne, nil, arg)
		if code != ExitUsage {
			t.Errorf("%s on a live-only command: exit %d, want %d (stderr %q)", arg, code, ExitUsage, stderr)
		}
		if stdout != "" {
			t.Errorf("%s: stdout must stay clean on usage error, got %q", arg, stdout)
		}
	}
}

// TestAtFlag_RequiresStore: --at without --store is a usage error
// that NAMES the requirement (§6.6: history is served from a
// sentinel's store; live-only otherwise).
func TestAtFlag_RequiresStore(t *testing.T) {
	code, stdout, stderr := runGraphBacked(t, emitOne, "--at=20m")
	if code != ExitUsage {
		t.Fatalf("exit %d, want %d", code, ExitUsage)
	}
	if !strings.Contains(stderr, "--at requires --store") {
		t.Errorf("stderr must name the --store requirement, got %q", stderr)
	}
	if stdout != "" {
		t.Errorf("stdout must stay clean, got %q", stdout)
	}
}

// TestAtFlag_BadSyntaxIsUsageError.
func TestAtFlag_BadSyntaxIsUsageError(t *testing.T) {
	code, _, stderr := runGraphBacked(t, emitOne, "--at=yesterday", "--store=/tmp/x.db")
	if code != ExitUsage {
		t.Fatalf("exit %d, want %d (stderr %q)", code, ExitUsage, stderr)
	}
	if !strings.Contains(stderr, "RFC3339") {
		t.Errorf("diagnostic must teach the accepted syntaxes, got %q", stderr)
	}
}

// TestAtFlag_ResolvesIntoScope: the full plumbing — duration-ago and
// RFC3339 --at values land in Scope.At (anchored on RunConfig.Now),
// --store in Scope.Store, and a graph-backed command without --at
// runs live (zero At) with --store still visible.
func TestAtFlag_ResolvesIntoScope(t *testing.T) {
	var got Scope
	capture := func(ctx context.Context, inv Invocation) (int, error) {
		got = inv.Scope
		return 0, nil
	}

	// fixedClock's first call anchors --at; base is 2026-01-01T00:00:00Z.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if code, _, stderr := runGraphBacked(t, capture, "--at=20m", "--store=/tmp/x.db"); code != ExitData {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	if want := base.Add(-20 * time.Minute); !got.At.Equal(want) {
		t.Errorf("Scope.At = %v, want %v", got.At, want)
	}
	if got.Store != "/tmp/x.db" {
		t.Errorf("Scope.Store = %q", got.Store)
	}

	if code, _, stderr := runGraphBacked(t, capture, "--at=2026-07-25T10:00:00Z", "--store=/tmp/x.db"); code != ExitData {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	if want := time.Date(2026, 7, 25, 10, 0, 0, 0, time.UTC); !got.At.Equal(want) {
		t.Errorf("Scope.At = %v, want %v", got.At, want)
	}

	if code, _, stderr := runGraphBacked(t, capture, "--store=/tmp/x.db"); code != ExitData {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	if !got.At.IsZero() {
		t.Errorf("no --at must mean live (zero At), got %v", got.At)
	}
	if got.Store != "/tmp/x.db" {
		t.Errorf("--store without --at must still reach the check, got %q", got.Store)
	}
}

// TestValidateGraphBackedSpecs: graph-backed commands cannot shadow
// the §6.6 flag names.
func TestValidateGraphBackedSpecs(t *testing.T) {
	if err := ValidateGraphBackedSpecs([]FlagSpec{{Name: "depth", Type: FlagInt, Default: "2", Help: "x"}}); err != nil {
		t.Errorf("legal spec rejected: %v", err)
	}
	for _, name := range []string{"at", "store"} {
		if err := ValidateGraphBackedSpecs([]FlagSpec{{Name: name, Type: FlagString, Help: "x"}}); err == nil {
			t.Errorf("shadowing --%s must be rejected", name)
		}
	}
	// Live-only commands may keep using the names (nothing else
	// claims them there).
	if err := ValidateSpecs([]FlagSpec{{Name: "at", Type: FlagString, Help: "x"}}); err != nil {
		t.Errorf("live-only command using 'at' as its own flag: %v", err)
	}
}
