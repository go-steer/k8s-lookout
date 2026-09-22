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

package main

import (
	"bufio"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The smoke test. docs/leeway-design.md §14 makes "built and
// smoke-tested in CI" a Phase 8 exit criterion, on the grounds that a
// binary nobody builds is a binary that does not work — and cmd/leeway
// is exactly the artifact that would rot unnoticed, because nothing in
// the sentinel's own test suite touches it.
//
// It lives here, as a Go test, rather than as a shell step in the
// workflow, so that `go test ./...` covers it: local presubmits, CI and
// a contributor's editor all run the same check, and there is no second
// place for the command line to drift out of date.
//
// What it proves is deliberately modest and deliberately end-to-end:
// the binary compiles, starts against a cluster it cannot reach, binds
// and serves its endpoints, answers /readyz honestly while its
// informers are still failing to list, and exits 0 on SIGTERM. What it
// does not prove is any behaviour of the sources — those have their own
// suites, against fakes, where the assertions can be about placement
// rather than about process plumbing.

// unreachableKubeconfig writes a kubeconfig pointing at a port nothing
// is listening on. Port 1 is reserved and unbindable without privilege,
// so this cannot accidentally reach a real apiserver on a developer's
// machine — which is the failure mode that would make this test pass
// for the wrong reason, or worse, watch somebody's cluster.
func unreachableKubeconfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kubeconfig")
	const cfg = `apiVersion: v1
kind: Config
clusters:
  - name: smoke
    cluster:
      server: https://127.0.0.1:1
contexts:
  - name: smoke
    context:
      cluster: smoke
      user: smoke
current-context: smoke
users:
  - name: smoke
    user:
      token: smoke
`
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatalf("writing kubeconfig: %v", err)
	}
	return path
}

// buildBinary compiles cmd/leeway the way a release does, once per test
// binary. Compiling rather than calling realMain in-process is the
// point: an unbuildable main package is the failure this is here to
// catch, and an in-process call would still pass with a broken build
// tag or a cgo dependency that does not cross-compile.
var buildBinary = sync.OnceValues(func() (string, error) {
	dir, err := os.MkdirTemp("", "leeway-smoke")
	if err != nil {
		return "", err
	}
	bin := filepath.Join(dir, "leeway")
	out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput()
	if err != nil {
		return "", errWithOutput{err, out}
	}
	return bin, nil
})

type errWithOutput struct {
	err error
	out []byte
}

func (e errWithOutput) Error() string { return e.err.Error() + "\n" + string(e.out) }

func binary(t *testing.T) string {
	t.Helper()
	bin, err := buildBinary()
	if err != nil {
		t.Fatalf("go build ./cmd/leeway: %v", err)
	}
	return bin
}

func TestSmoke_TheBinaryPrintsItsVersion(t *testing.T) {
	out, err := exec.Command(binary(t), "--version").Output()
	if err != nil {
		t.Fatalf("--version: %v", err)
	}
	if got := string(out); !strings.HasPrefix(got, "leeway ") {
		t.Errorf("--version printed %q, want it to start with the program name", got)
	}
}

func TestSmoke_AnUnknownFlagIsAUsageError(t *testing.T) {
	// §4.2: 2 is usage, 1 is runtime. A binary that conflates them makes
	// a typo in a manifest indistinguishable from a cluster outage,
	// which is the distinction a CrashLoopBackOff investigation starts
	// from.
	err := exec.Command(binary(t), "--not-a-flag").Run()
	if code := exitCode(t, err); code != 2 {
		t.Errorf("--not-a-flag exited %d, want 2 (usage)", code)
	}
}

func TestSmoke_AnUnknownSourceIsARuntimeError(t *testing.T) {
	err := exec.Command(binary(t), "--sources=rollout",
		"--kubeconfig="+unreachableKubeconfig(t), "--metrics-addr=127.0.0.1:0").Run()
	if code := exitCode(t, err); code != 1 {
		t.Errorf("--sources=rollout exited %d, want 1 (runtime)", code)
	}
}

// TestSmoke_ExplicitlyNamingAnUnavailableSourceIsFatal pins the
// asymmetry the default relies on. compute-class is in the default set
// and is skipped on a cluster that does not serve the GKE CRD — which
// is every cluster this test could run on, and every kwok cluster #478
// will run the scale tier on. Naming it has to mean something
// different, or the skip quietly swallows the case where an operator
// pointed this at their GKE cluster and got the wrong kubeconfig.
func TestSmoke_ExplicitlyNamingAnUnavailableSourceIsFatal(t *testing.T) {
	cmd := exec.Command(binary(t), "--sources=compute-class",
		"--kubeconfig="+unreachableKubeconfig(t), "--metrics-addr=127.0.0.1:0")
	out, err := cmd.CombinedOutput()
	if code := exitCode(t, err); code != 1 {
		t.Errorf("--sources=compute-class exited %d, want 1 (runtime)\n%s", code, head(string(out)))
	}
	if !strings.Contains(string(out), "is not served") {
		t.Errorf("the failure does not say the CRD is missing:\n%s", head(string(out)))
	}
}

func TestSmoke_ItServesItsEndpointsAndExitsCleanlyOnSIGTERM(t *testing.T) {
	bin := binary(t)
	cmd := exec.Command(bin,
		// No --sources: the default set is what a deployment runs, so
		// it is what this exercises. compute-class drops out on the
		// discovery gate and topology-drift carries the process.
		"--kubeconfig="+unreachableKubeconfig(t),
		"--cluster-name=smoke",
		// Port 0: the harness must not lose a race with whatever else
		// is on the machine, and the binary prints what it bound.
		"--metrics-addr=127.0.0.1:0",
		// A backstop, not the exit path under test. If SIGTERM handling
		// is what broke, this is what stops the suite hanging for ten
		// minutes to tell us so.
		"--exit-after=90s",
	)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting %s: %v", bin, err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	addr, logs := watchStderr(t, stderr)

	base := "http://" + addr
	t.Run("healthz is up", func(t *testing.T) {
		body, code := get(t, base+"/healthz")
		if code != http.StatusOK || strings.TrimSpace(body) != "ok" {
			t.Errorf("/healthz = %d %q, want 200 ok", code, body)
		}
	})

	t.Run("readyz refuses while the informers cannot list", func(t *testing.T) {
		// The honest answer, and the one this test exists to pin: the
		// apiserver is unreachable, so no source has drained its initial
		// LIST, so nothing on /metrics is a reading yet. A /readyz that
		// said ok here would let a rollout route past a blind process.
		body, code := get(t, base+"/readyz")
		if code != http.StatusServiceUnavailable {
			t.Errorf("/readyz = %d %q, want 503 against an unreachable apiserver", code, body)
		}
		if !strings.Contains(body, "still listing") {
			t.Errorf("/readyz body = %q, want it to name what is not ready", body)
		}
	})

	t.Run("metrics carries the standalone identity", func(t *testing.T) {
		body, code := get(t, base+"/metrics")
		if code != http.StatusOK {
			t.Fatalf("/metrics = %d", code)
		}
		// The info gauge is the one series guaranteed present before any
		// cluster data arrives, which is what makes it the anchor: it
		// proves the registry, the cluster-label wrapper and the handler
		// are all wired, with no dependency on a source having observed
		// anything.
		if !strings.Contains(body, `lookout_leeway_standalone_info{`) {
			t.Errorf("/metrics does not carry lookout_leeway_standalone_info:\n%s", head(body))
		}
		if !strings.Contains(body, `cluster="smoke"`) {
			t.Errorf("/metrics does not carry the --cluster-name label:\n%s", head(body))
		}
	})

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Errorf("exited %d on SIGTERM, want 0 (clean shutdown)\nstderr:\n%s",
			exitCode(t, err), head(logs()))
	}
}

// watchStderr drains the process's stderr, returning the address it
// reported binding and a snapshot of everything it said. Draining
// matters as much as parsing: an unreachable apiserver makes client-go
// log steadily, and a pipe nobody reads fills up and wedges the process
// under test.
func watchStderr(t *testing.T, r io.Reader) (addr string, logs func() string) {
	t.Helper()
	found := make(chan string, 1)
	var mu sync.Mutex
	var seen strings.Builder

	go func() {
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for sc.Scan() {
			line := sc.Text()
			mu.Lock()
			seen.WriteString(line)
			seen.WriteByte('\n')
			mu.Unlock()
			if _, rest, ok := strings.Cut(line, "/readyz on "); ok {
				select {
				case found <- strings.TrimSpace(rest):
				default:
				}
			}
		}
	}()

	logs = func() string {
		mu.Lock()
		defer mu.Unlock()
		return seen.String()
	}
	select {
	case addr = <-found:
		return addr, logs
	case <-time.After(30 * time.Second):
		t.Fatalf("the binary never reported a listener address\nstderr:\n%s", head(logs()))
		return "", logs
	}
}

func get(t *testing.T, url string) (string, int) {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading %s: %v", url, err)
	}
	return string(body), resp.StatusCode
}

func exitCode(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		t.Fatalf("not an exit error: %v", err)
	}
	return ee.ExitCode()
}

// head bounds what a failure prints. client-go against an unreachable
// apiserver produces thousands of lines, and a test failure that buries
// its own message is a test failure nobody reads.
func head(s string) string {
	const max = 4000
	if len(s) <= max {
		return s
	}
	return s[:max] + "\n… (truncated)"
}
