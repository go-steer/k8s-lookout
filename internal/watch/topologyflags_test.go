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
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources/computeclass"
	"github.com/go-steer/k8s-lookout/pkg/sources/topologydrift"
)

// TestTopologyKeysDefault_IsTheSourcesOwn keeps the flag's default and
// the source's DefaultTopologyKeys from drifting into two different
// answers to "the standard labels". The flag renders the source's list;
// this is what fails if someone hardcodes the strings instead.
func TestTopologyKeysDefault_IsTheSourcesOwn(t *testing.T) {
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	want := topologydrift.DefaultTopologyKeys
	if got := topologyKeysFrom(f.topologyKeys); !slices.Equal(got, want) {
		t.Errorf("--topology-keys default parses to %q, want %q", got, want)
	}
	if f.topologyPerDomain {
		t.Errorf("--topology-per-domain-series defaults on; the 480k-series posture must be opt-in")
	}
}

func TestTopologyKeysFrom_ParsesAndTrims(t *testing.T) {
	got := topologyKeysFrom(" rack , cell,,  ")
	want := []leeway.TopologyKey{"rack", "cell"}
	if !slices.Equal(got, want) {
		t.Errorf("topologyKeysFrom = %q, want %q", got, want)
	}
}

// TestTopologyKeysEmpty_IsRejected pins the deliberate choice not to
// default an emptied flag: the source's normalize would substitute
// zone/region, handing an operator who cleared the flag the exact
// opposite of what they asked for, silently.
func TestTopologyKeysEmpty_IsRejected(t *testing.T) {
	for _, arg := range []string{"--topology-keys=", "--topology-keys=  , ,"} {
		f, err := parseFlags([]string{arg, "--dry-run"})
		if err != nil {
			t.Fatalf("parseFlags(%s): %v", arg, err)
		}
		err = f.validate()
		if err == nil {
			t.Errorf("validate with %s: want an error", arg)
			continue
		}
		if !strings.Contains(err.Error(), "--topology-keys") {
			t.Errorf("error %q does not name the flag", err)
		}
	}
}

// TestPerDomainCardinalityFlags_Defaults pins §8.4's shipped posture.
//
// Three of the four controls have to be right by default, because the operator
// who most needs them is the one who has not read this far: the states are
// collapsed, the axes are capped, and the namespace lists are empty so that
// nothing is silently missing from a cluster nobody configured.
func TestPerDomainCardinalityFlags_Defaults(t *testing.T) {
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if !f.topologyCollapse {
		t.Error("--topology-per-domain-collapse-states defaults off; the four-state label set is twice the series for a distinction no dashboard draws")
	}
	if got, want := f.topologyMaxKeys, topologydrift.DefaultPerDomainMaxKeys; got != want {
		t.Errorf("--topology-per-domain-max-keys default = %d, want the source's %d", got, want)
	}
	if f.topologyDomainNS != "" || f.topologyDomainNotNS != "" {
		t.Errorf("namespace lists default to %q/%q, want empty: a default that hid a namespace would be a silent gap",
			f.topologyDomainNS, f.topologyDomainNotNS)
	}
}

// TestPerDomainMaxKeysZero_IsRejected is the same hazard as the emptied
// --topology-keys above: Config.normalize() reads a zero as "unset" and would
// hand back the default of 4, so an operator who typed 0 meaning "no
// breakdown" would get the largest one the cap allows.
func TestPerDomainMaxKeysZero_IsRejected(t *testing.T) {
	f, err := parseFlags([]string{"--topology-per-domain-max-keys=0", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	err = f.validate()
	if err == nil {
		t.Fatal("validate accepted --topology-per-domain-max-keys=0")
	}
	if !strings.Contains(err.Error(), "--topology-per-domain-max-keys") {
		t.Errorf("error %q does not name the flag", err)
	}
	// And the two values that do mean something are accepted.
	for _, arg := range []string{"--topology-per-domain-max-keys=1", "--topology-per-domain-max-keys=-1"} {
		f, err := parseFlags([]string{arg, "--dry-run"})
		if err != nil {
			t.Fatalf("parseFlags(%s): %v", arg, err)
		}
		if err := f.validate(); err != nil {
			t.Errorf("validate(%s) = %v, want accepted", arg, err)
		}
	}
}

// TestLeewayDeprecation_FlagsAndSourcesMatrix (#842, docs/cli-stability-policy.md
// Rule 3): every explicit use of topology-drift, compute-class, --topology-*, or
// --compute-class-* produces a single-line deprecation warning pointing callers
// to github.com/go-steer/leeway, while default --sources=auto and unrelated
// invocations stay completely silent.
func TestLeewayDeprecation_FlagsAndSourcesMatrix(t *testing.T) {
	t.Parallel()

	silentCases := [][]string{
		nil,
		{"--dry-run"},
		{"--sources=auto", "--dry-run"},
		{"--sources=k8s-events,object-state,rollout,capacity,gateway", "--dry-run"},
	}
	for _, args := range silentCases {
		f, err := parseFlags(args)
		if err != nil {
			t.Fatalf("parseFlags(%v): %v", args, err)
		}
		if warning, ok := f.deprecationWarning(); ok {
			t.Errorf("parseFlags(%v) unexpectedly produced deprecation warning: %q", args, warning)
		}
	}

	warnCases := []struct {
		args       []string
		wantTokens []string
	}{
		{
			args:       []string{"--sources=topology-drift", "--dry-run"},
			wantTokens: []string{"--sources=topology-drift", "github.com/go-steer/leeway"},
		},
		{
			args:       []string{"--sources=compute-class", "--dry-run"},
			wantTokens: []string{"--sources=compute-class", "github.com/go-steer/leeway"},
		},
		{
			args:       []string{"--sources=k8s-events,topology-drift,compute-class", "--dry-run"},
			wantTokens: []string{"--sources=topology-drift,compute-class", "github.com/go-steer/leeway"},
		},
		{
			// Passing a deprecated flag at its exact default value still warns.
			args:       []string{"--topology-dwell=10m", "--dry-run"},
			wantTokens: []string{"--topology-dwell", "github.com/go-steer/leeway"},
		},
		{
			args:       []string{"--compute-class-infer=true", "--dry-run"},
			wantTokens: []string{"--compute-class-infer", "github.com/go-steer/leeway"},
		},
		{
			args:       []string{"--sources=topology-drift", "--topology-dwell=60s", "--compute-class-window=30m", "--dry-run"},
			wantTokens: []string{"--sources=topology-drift", "--compute-class-window", "--topology-dwell", "github.com/go-steer/leeway"},
		},
	}

	// Exhaustively check every --topology-* and --compute-class-* flag from FlagInventory.
	for _, fl := range FlagInventory() {
		if !isDeprecatedLeewayFlag(fl.Name) {
			continue
		}
		if !strings.HasPrefix(fl.Help, "Deprecated (use github.com/go-steer/leeway): ") {
			t.Errorf("flag --%s help does not start with deprecation notice: %q", fl.Name, fl.Help)
		}
		argVal := fl.Default
		if fl.Name == "topology-cluster-defaults" {
			argVal = "none"
		}
		arg := "--" + fl.Name + "=" + argVal
		warnCases = append(warnCases, struct {
			args       []string
			wantTokens []string
		}{
			args:       []string{arg, "--dry-run"},
			wantTokens: []string{"--" + fl.Name, "github.com/go-steer/leeway"},
		})
	}

	for _, tc := range warnCases {
		f, err := parseFlags(tc.args)
		if err != nil {
			t.Fatalf("parseFlags(%v): %v", tc.args, err)
		}
		if err := f.validate(); err != nil {
			t.Fatalf("validate(%v): %v", tc.args, err)
		}
		warning, ok := f.deprecationWarning()
		if !ok {
			t.Errorf("parseFlags(%v): expected deprecation warning, got none", tc.args)
			continue
		}
		if strings.Contains(warning, "\n") {
			t.Errorf("parseFlags(%v): deprecation warning must be single-line, got %q", tc.args, warning)
		}
		for _, tok := range tc.wantTokens {
			if !strings.Contains(warning, tok) {
				t.Errorf("parseFlags(%v): warning %q missing %q", tc.args, warning, tok)
			}
		}
	}
}

// TestLeewayDeprecation_EmitsToStderrNeverStdoutAndExplicitSourcesStillBuild
// verifies both halves of the Phase 1 contract (#842):
//  1. Main() writes the single-line deprecation warning to stderr and 0 bytes
//     to stdout (DESIGN.md §4.2 output contract).
//  2. Explicitly naming topology-drift and compute-class in --sources keeps
//     both sources constructing and registering during the deprecation window.
func TestLeewayDeprecation_EmitsToStderrNeverStdoutAndExplicitSourcesStillBuild(t *testing.T) {
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

	missingKubeconfig := filepath.Join(t.TempDir(), "missing-kubeconfig")

	stdout, stderr, _ := runMainCapturingStdio([]string{
		"--sources=topology-drift,compute-class",
		"--topology-dwell=60s",
		"--dry-run",
		"--kubeconfig=" + missingKubeconfig,
	})
	if stdout != "" {
		t.Errorf("stdout must remain empty on deprecation warning, got: %q", stdout)
	}
	var warnLines []string
	for _, line := range strings.Split(strings.TrimSpace(stderr), "\n") {
		if strings.Contains(line, "deprecated leeway watch configuration") {
			warnLines = append(warnLines, line)
		}
	}
	if len(warnLines) != 1 {
		t.Fatalf("expected exactly 1 deprecation warning line on stderr, got %d:\n%s", len(warnLines), stderr)
	}
	for _, want := range []string{"--sources=topology-drift,compute-class", "--topology-dwell", "github.com/go-steer/leeway"} {
		if !strings.Contains(warnLines[0], want) {
			t.Errorf("stderr deprecation line %q missing %q", warnLines[0], want)
		}
	}

	// Control: explicit non-leeway sources emit zero deprecation lines and zero stdout.
	stdoutClean, stderrClean, _ := runMainCapturingStdio([]string{
		"--sources=k8s-events",
		"--dry-run",
		"--kubeconfig=" + missingKubeconfig,
	})
	if stdoutClean != "" {
		t.Errorf("stdout must remain empty, got: %q", stdoutClean)
	}
	if strings.Contains(stderrClean, "deprecated leeway") {
		t.Errorf("unexpected deprecation warning on non-leeway run:\n%s", stderrClean)
	}

	// Explicit --sources=topology-drift,compute-class still constructs both sources.
	f, err := parseFlags([]string{"--sources=k8s-events,topology-drift,compute-class", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	bs, err := buildSources(f, "", fake.NewSimpleClientset(), dyn, nil, nil)
	if err != nil {
		t.Fatalf("buildSources(explicit leeway sources): %v", err)
	}
	if bs.topoDrift == nil {
		t.Error("topology-drift must still construct when explicitly named in --sources")
	}
	if bs.compClass == nil {
		t.Error("compute-class must still construct when explicitly named in --sources")
	}
	for _, name := range []string{topologydrift.Name, computeclass.Name} {
		if _, ok := bs.registry.Lookup(name); !ok {
			t.Errorf("source %q not registered when explicitly named in --sources", name)
		}
	}
}
