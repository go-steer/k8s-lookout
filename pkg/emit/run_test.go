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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// fixedClock advances 100ms per call, so elapsed is always 100ms.
func fixedClock() func() time.Time {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	n := 0
	return func() time.Time {
		ts := base.Add(time.Duration(n) * 100 * time.Millisecond)
		n++
		return ts
	}
}

// runOnce executes Run against a check with captured streams.
func runOnce(t *testing.T, check CheckFunc, flags []FlagSpec, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errBuf bytes.Buffer
	code = Run(context.Background(), RunConfig{
		Name:   "lookout test check",
		Flags:  flags,
		Check:  check,
		Stdout: &out,
		Stderr: &errBuf,
		Now:    fixedClock(),
	}, args)
	return code, out.String(), errBuf.String()
}

func emitOne(ctx context.Context, inv Invocation) (int, error) {
	err := inv.Out.Emit(Finding{Kind: "test.finding", Name: "obj-1"})
	return 3, err
}

func TestRunSuccessEndsWithSummary(t *testing.T) {
	code, stdout, stderr := runOnce(t, emitOne, nil)
	if code != ExitData {
		t.Fatalf("exit = %d, stderr: %s", code, stderr)
	}
	want := "kind=test.finding name=obj-1\nscanned=3 findings=1 elapsed=100ms\n"
	if stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
	if stderr != "" {
		t.Errorf("stderr should be empty on success, got %q", stderr)
	}
}

func TestRunJSONFormat(t *testing.T) {
	code, stdout, _ := runOnce(t, emitOne, nil, "--format=json")
	if code != ExitData {
		t.Fatalf("exit = %d", code)
	}
	want := `{"kind":"test.finding","name":"obj-1"}` + "\n" +
		`{"scanned":3,"findings":1,"elapsed":"100ms"}` + "\n"
	if stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
}

func TestRunRuntimeErrorKeepsStdoutClean(t *testing.T) {
	boom := func(ctx context.Context, inv Invocation) (int, error) {
		return 0, errors.New("informer sync failed")
	}
	code, stdout, stderr := runOnce(t, boom, nil)
	if code != ExitRuntime {
		t.Fatalf("exit = %d, want %d", code, ExitRuntime)
	}
	if stdout != "" {
		t.Errorf("stdout must carry no summary on failure, got %q", stdout)
	}
	if !strings.Contains(stderr, "informer sync failed") {
		t.Errorf("stderr should carry the diagnostic, got %q", stderr)
	}
}

func TestRunUsageErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"unknown flag", []string{"--nonesuch"}},
		{"namespace and -A conflict", []string{"--namespace=prod", "-A"}},
		{"malformed workload", []string{"--workload=Deployment/api"}},
		{"bad format", []string{"--format=yaml"}},
		{"non-positive timeout", []string{"--timeout=0s"}},
		{"negative since", []string{"--since=-5m"}},
		{"stray positional argument", []string{"positional"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, stdout, stderr := runOnce(t, emitOne, nil, tt.args...)
			if code != ExitUsage {
				t.Errorf("exit = %d, want %d (stderr: %q)", code, ExitUsage, stderr)
			}
			if stdout != "" {
				t.Errorf("stdout must stay clean on usage errors, got %q", stdout)
			}
			if stderr == "" {
				t.Error("usage errors must be diagnosed on stderr")
			}
		})
	}
}

func TestRunHelpGoesToStdout(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := Run(context.Background(), RunConfig{
		Name:   "lookout test check",
		Check:  emitOne,
		Help:   "generated help text\n",
		Stdout: &out,
		Stderr: &errBuf,
	}, []string{"--help"})
	if code != ExitData {
		t.Fatalf("exit = %d", code)
	}
	if out.String() != "generated help text\n" {
		t.Errorf("stdout = %q", out.String())
	}

	// Without generated help, a minimal flag listing is produced.
	out.Reset()
	code = Run(context.Background(), RunConfig{
		Name:   "lookout test check",
		Check:  emitOne,
		Stdout: &out,
		Stderr: &errBuf,
	}, []string{"--help"})
	if code != ExitData {
		t.Fatalf("exit = %d", code)
	}
	if !strings.Contains(out.String(), "-timeout") {
		t.Errorf("fallback help should list common flags, got %q", out.String())
	}
}

// TestRunEnforcesTimeout is the --timeout contract test: a check
// that honors ctx must be cancelled and reported as a runtime
// failure.
func TestRunEnforcesTimeout(t *testing.T) {
	hang := func(ctx context.Context, inv Invocation) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	var out, errBuf bytes.Buffer
	start := time.Now()
	code := Run(context.Background(), RunConfig{
		Name:   "lookout test check",
		Check:  hang,
		Stdout: &out,
		Stderr: &errBuf,
	}, []string{"--timeout=50ms"})
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("timeout not enforced: took %s", elapsed)
	}
	if code != ExitRuntime {
		t.Fatalf("exit = %d, want %d", code, ExitRuntime)
	}
	if !strings.Contains(errBuf.String(), "timed out after 50ms") {
		t.Errorf("stderr should name the timeout, got %q", errBuf.String())
	}
	if out.String() != "" {
		t.Errorf("no summary may be written on timeout, got %q", out.String())
	}
}

func TestRunScopePlumbing(t *testing.T) {
	var got Scope
	capture := func(ctx context.Context, inv Invocation) (int, error) {
		got = inv.Scope
		return 0, nil
	}
	code, _, stderr := runOnce(t, capture, nil,
		"--namespace=prod", "--workload=Deployment/prod/api", "--since=30m")
	if code != ExitData {
		t.Fatalf("exit = %d, stderr: %s", code, stderr)
	}
	want := Scope{
		Namespace: "prod",
		Workload:  WorkloadRef{"Deployment", "prod", "api"},
		Since:     30 * time.Minute,
	}
	if got != want {
		t.Errorf("scope = %+v, want %+v", got, want)
	}

	code, _, _ = runOnce(t, capture, nil, "-A")
	if code != ExitData || !got.AllNamespaces {
		t.Errorf("-A not plumbed: exit=%d scope=%+v", code, got)
	}
}

func TestRunCommandSpecificFlags(t *testing.T) {
	specs := []FlagSpec{{Name: "limit", Type: FlagInt, Default: "5", Help: "h"}}
	var limit int
	capture := func(ctx context.Context, inv Invocation) (int, error) {
		limit = inv.Flags.Int("limit")
		return 0, nil
	}
	if code, _, stderr := runOnce(t, capture, specs, "--limit=9"); code != ExitData {
		t.Fatalf("exit = %d, stderr: %s", code, stderr)
	}
	if limit != 9 {
		t.Errorf("limit = %d, want 9", limit)
	}
	if code, _, _ := runOnce(t, capture, specs); code != ExitData {
		t.Fatal("default run failed")
	}
	if limit != 5 {
		t.Errorf("default limit = %d, want 5", limit)
	}
}

// runOnceArgs is runOnce with a MaxArgs allowance.
func runOnceArgs(t *testing.T, check CheckFunc, maxArgs int, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errBuf bytes.Buffer
	code = Run(context.Background(), RunConfig{
		Name:    "lookout test check",
		Check:   check,
		MaxArgs: maxArgs,
		Stdout:  &out,
		Stderr:  &errBuf,
		Now:     fixedClock(),
	}, args)
	return code, out.String(), errBuf.String()
}

// TestRunPositionalArgs covers MaxArgs: positionals reach the check
// via Invocation.Args, may be interspersed with flags kubectl-style,
// and one past the allowance is a usage error naming the excess.
func TestRunPositionalArgs(t *testing.T) {
	var gotArgs []string
	var gotNS string
	capture := func(ctx context.Context, inv Invocation) (int, error) {
		gotArgs = inv.Args
		gotNS = inv.Scope.Namespace
		return 0, nil
	}

	code, _, stderr := runOnceArgs(t, capture, 1, "Pod/prod/api", "--namespace=prod")
	if code != ExitData {
		t.Fatalf("exit = %d, stderr: %s", code, stderr)
	}
	if len(gotArgs) != 1 || gotArgs[0] != "Pod/prod/api" {
		t.Errorf("Args = %v, want [Pod/prod/api]", gotArgs)
	}
	if gotNS != "prod" {
		t.Errorf("flag after the positional was not parsed: namespace = %q", gotNS)
	}

	code, stdout, stderr := runOnceArgs(t, capture, 1, "one", "--namespace=prod", "two")
	if code != ExitUsage {
		t.Fatalf("exit = %d, want %d", code, ExitUsage)
	}
	if stdout != "" {
		t.Errorf("stdout must stay clean, got %q", stdout)
	}
	if !strings.Contains(stderr, `unexpected argument "two"`) {
		t.Errorf("stderr should name the excess positional, got %q", stderr)
	}
}

// TestRunUsageErrorFromCheck: a check may classify its own failure as
// user error (UsageErrorf) and get the §4.2 usage path — exit 2,
// stderr diagnostic with the --help pointer, no summary line.
func TestRunUsageErrorFromCheck(t *testing.T) {
	boom := func(ctx context.Context, inv Invocation) (int, error) {
		return 0, UsageErrorf("--diff requires a sentinel store; lands in M3 (§6.6)")
	}
	code, stdout, stderr := runOnce(t, boom, nil)
	if code != ExitUsage {
		t.Fatalf("exit = %d, want %d", code, ExitUsage)
	}
	if stdout != "" {
		t.Errorf("stdout must carry no summary, got %q", stdout)
	}
	if !strings.Contains(stderr, "lands in M3") || !strings.Contains(stderr, "--help") {
		t.Errorf("stderr = %q", stderr)
	}
	if !IsUsageError(UsageErrorf("x")) {
		t.Error("IsUsageError(UsageErrorf(...)) = false")
	}
	if IsUsageError(errors.New("x")) {
		t.Error("IsUsageError(plain error) = true")
	}
}

// --kubeconfig and --context reach the client constructors through
// the context, not through the check's signature, so the assertion is
// on what kube.SelectionFrom sees inside the check.
func TestRunClusterSelectionRidesTheContext(t *testing.T) {
	var got kube.Selection
	capture := func(ctx context.Context, inv Invocation) (int, error) {
		got = kube.SelectionFrom(ctx)
		return 0, nil
	}

	code, stdout, stderr := runOnce(t, capture, nil, "--kubeconfig=/tmp/kc", "--context=prod")
	if code != ExitData {
		t.Fatalf("exit = %d, stderr: %s", code, stderr)
	}
	want := kube.Selection{Kubeconfig: "/tmp/kc", Context: "prod"}
	if got != want {
		t.Errorf("selection = %+v, want %+v", got, want)
	}
	if !strings.Contains(stdout, "context=prod") {
		t.Errorf("summary line does not record the cluster: %q", stdout)
	}

	// Unflagged, nothing is carried and nothing is claimed.
	got = kube.Selection{}
	code, stdout, _ = runOnce(t, capture, nil)
	if code != ExitData || !got.IsZero() {
		t.Errorf("bare invocation carried %+v", got)
	}
	if strings.Contains(stdout, "context=") {
		t.Errorf("unflagged summary claims a context: %q", stdout)
	}
}

// context= is Writer-owned: a check that tries to set it is a bug,
// and the guard is the same one that protects exempt=.
func TestRunContextNoteIsReserved(t *testing.T) {
	var noteErr error
	sneak := func(ctx context.Context, inv Invocation) (int, error) {
		noteErr = inv.Out.Note("context", "somewhere-else")
		return 0, nil
	}
	if code, _, stderr := runOnce(t, sneak, nil); code != ExitData {
		t.Fatalf("exit = %d, stderr: %s", code, stderr)
	}
	if noteErr == nil {
		t.Error("a check was allowed to set the reserved context note")
	}
}
