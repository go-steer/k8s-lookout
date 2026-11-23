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
	"io"
	"log"
	"os"
	"strings"
	"testing"
)

// removedLeewayFlags lists all 23 --topology-* and --compute-class-* flags
// retired in #842 Phase 2 in favor of github.com/go-steer/leeway.
var removedLeewayFlags = []string{
	"compute-class-dwell",
	"compute-class-infer",
	"compute-class-last-rank-ceiling",
	"compute-class-rank0-floor",
	"compute-class-tier-c-signals",
	"compute-class-window",
	"topology-baseline-band",
	"topology-baseline-half-life",
	"topology-capacity-ratio",
	"topology-cluster-defaults",
	"topology-domain-unavailable-keys",
	"topology-dwell",
	"topology-keys",
	"topology-learn-baselines",
	"topology-max-node-groups",
	"topology-node-group-keys",
	"topology-per-domain-collapse-states",
	"topology-per-domain-exclude-namespaces",
	"topology-per-domain-max-keys",
	"topology-per-domain-min-drift",
	"topology-per-domain-namespaces",
	"topology-per-domain-series",
	"topology-tier-c-signals",
}

// TestRemovedLeewayFlags_AbsentFromInventoryAndRejected (#842 Phase 2):
// none of the 23 retired --topology-* or --compute-class-* flags appear in
// FlagInventory(), and passing any of them fails flag parsing.
func TestRemovedLeewayFlags_AbsentFromInventoryAndRejected(t *testing.T) {
	t.Parallel()

	if len(removedLeewayFlags) != 23 {
		t.Fatalf("removedLeewayFlags has %d entries, want 23", len(removedLeewayFlags))
	}
	seen := make(map[string]bool, len(removedLeewayFlags))
	for _, name := range removedLeewayFlags {
		if seen[name] {
			t.Fatalf("duplicate entry %q in removedLeewayFlags", name)
		}
		seen[name] = true
	}

	for _, fl := range FlagInventory() {
		if strings.HasPrefix(fl.Name, "topology-") || strings.HasPrefix(fl.Name, "compute-class-") {
			t.Errorf("retired leeway flag --%s still present in FlagInventory()", fl.Name)
		}
	}

	for _, name := range removedLeewayFlags {
		arg := "--" + name + "=1"
		if _, err := parseFlags([]string{arg, "--dry-run"}); err == nil {
			t.Errorf("parseFlags accepted retired flag %s", arg)
		}
	}
}

// TestRemovedLeewaySources_RejectedNonZeroZeroStdout (#842 Phase 2, DESIGN.md §4.2):
// explicitly naming topology-drift or compute-class in --sources, or passing a
// retired --topology-* / --compute-class-* flag to Main(), exits non-zero
// with diagnostics on stderr and 0 bytes on stdout.
func TestRemovedLeewaySources_RejectedNonZeroZeroStdout(t *testing.T) {
	// Not t.Parallel(): redirects process-global os.Stdout, os.Stderr, and log output.
	runMainCapturingStdio := func(args []string) (stdoutStr, stderrStr string, code int) {
		t.Helper()
		origStdout, origStderr := os.Stdout, os.Stderr
		origLogWriter := log.Writer()
		origLogFlags := log.Flags()

		outR, outW, err := os.Pipe()
		if err != nil {
			t.Fatalf("stdout pipe: %v", err)
		}
		errR, errW, err := os.Pipe()
		if err != nil {
			t.Fatalf("stderr pipe: %v", err)
		}
		os.Stdout = outW
		os.Stderr = errW
		log.SetOutput(errW)
		defer func() {
			os.Stdout = origStdout
			os.Stderr = origStderr
			log.SetOutput(origLogWriter)
			log.SetFlags(origLogFlags)
		}()

		var outBuf, errBuf bytes.Buffer
		outDone := make(chan struct{})
		errDone := make(chan struct{})
		go func() {
			_, _ = io.Copy(&outBuf, outR)
			close(outDone)
		}()
		go func() {
			_, _ = io.Copy(&errBuf, errR)
			close(errDone)
		}()

		code = Main(args)
		_ = outW.Close()
		_ = errW.Close()
		<-outDone
		<-errDone
		_ = outR.Close()
		_ = errR.Close()
		return outBuf.String(), errBuf.String(), code
	}

	cases := [][]string{
		{"--sources=topology-drift", "--dry-run"},
		{"--sources=compute-class", "--dry-run"},
		{"--sources=k8s-events,topology-drift", "--dry-run"},
		{"--topology-dwell=60s", "--dry-run"},
		{"--compute-class-infer=true", "--dry-run"},
	}

	for _, args := range cases {
		stdout, stderr, code := runMainCapturingStdio(args)
		if code == 0 {
			t.Errorf("Main(%v) exit code = 0, want non-zero (stderr: %s)", args, stderr)
		}
		if stdout != "" {
			t.Errorf("Main(%v) wrote non-empty stdout %q; diagnostics belong on stderr", args, stdout)
		}
		if strings.TrimSpace(stderr) == "" {
			t.Errorf("Main(%v) wrote empty stderr on error", args)
		}
	}
}
