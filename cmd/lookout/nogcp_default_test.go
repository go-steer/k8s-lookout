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

//go:build !gke && !allproviders

package main

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// gcpSymbolMarkers are the package-path fragments that must not
// appear in the default binary's symbol table (DESIGN.md §2: the
// vanilla-Kubernetes build has ZERO GCP SDK linkage). pkg/cloud/gke
// is the provider package itself; the google.golang.org/api paths
// are the SDK surfaces the M4 capabilities link under the tags.
var gcpSymbolMarkers = []string{
	"k8s-lookout/pkg/cloud/gke",
	"google.golang.org/api/compute",
	"google.golang.org/api/logging",
	"google.golang.org/api/container",
	"cloud.google.com/go/logging",
}

// TestDefaultBinaryHasNoGCPSymbols is the `go tool nm` conformance
// check as a test: build the untagged binary, walk its symbol
// table, and fail on any GCP linkage. Registry-level conformance
// (TestDefaultBinaryLinksNoCloudProvider) catches an accidental
// blank import; this catches ANY path by which a GCP package gains
// a symbol in the vanilla build.
func TestDefaultBinaryHasNoGCPSymbols(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the full binary; skipped in -short mode")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("go binary not on PATH: %v", err)
	}

	bin := filepath.Join(t.TempDir(), "lookout-default")
	build := exec.Command(goBin, "build", "-o", bin, ".")
	build.Env = os.Environ()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build (default tags): %v\n%s", err, out)
	}

	nm := exec.Command(goBin, "tool", "nm", bin)
	stdout, err := nm.StdoutPipe()
	if err != nil {
		t.Fatalf("nm stdout pipe: %v", err)
	}
	if err := nm.Start(); err != nil {
		t.Fatalf("go tool nm: %v", err)
	}
	var leaked []string
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		for _, marker := range gcpSymbolMarkers {
			if strings.Contains(line, marker) {
				leaked = append(leaked, line)
				break
			}
		}
		if len(leaked) > 10 {
			break // enough evidence
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scanning nm output: %v", err)
	}
	if err := nm.Wait(); err != nil {
		t.Fatalf("go tool nm: %v", err)
	}
	if len(leaked) > 0 {
		t.Errorf("default (untagged) binary links GCP symbols — the §2 provider boundary leaked into the vanilla build:\n%s",
			strings.Join(leaked, "\n"))
	}
}
