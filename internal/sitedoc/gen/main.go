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

// Command gen regenerates the docs-site Reference section under
// docs/site/src/content/docs/reference/ from the pkg/checks registry,
// the sentinel flag/metric inventories, and the signal-schema v1
// ledger. Run it via dev/tools/gen-site-docs (re-runnable; output is
// deterministic). Stale pages — files no current declaration
// generates — are deleted, so the reference directory always mirrors
// the generator exactly.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-steer/k8s-lookout/internal/sitedoc"
	"github.com/go-steer/k8s-lookout/pkg/checks"

	// Read-path command implementations register themselves into
	// the default registry from their init functions — the same
	// set cmd/lookout mounts (cmd/lookout/checks.go).
	_ "github.com/go-steer/k8s-lookout/pkg/checks/audit"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/bundle"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/cloudcheck"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/delta"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/events"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/findings"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/health"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/inventory"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/logs"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/netprobe"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/perf"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/stab"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/state"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/top"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/triage"
)

func main() {
	root := flag.String("root", ".", "repository root (the directory containing docs/site/)")
	flag.Parse()
	if err := run(*root); err != nil {
		fmt.Fprintln(os.Stderr, "gen-site-docs:", err)
		os.Exit(1)
	}
}

func run(root string) error {
	files := sitedoc.GenerateAll(checks.Default())
	for _, rel := range sitedoc.SortedPaths(files) {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, []byte(files[rel]), 0o644); err != nil {
			return err
		}
		fmt.Println("wrote", rel)
	}
	// Delete stale pages: reference/ is generated-only.
	dir := filepath.Join(root, filepath.FromSlash(sitedoc.Dir))
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		rel := sitedoc.Dir + "/" + e.Name()
		if _, keep := files[rel]; keep {
			continue
		}
		if !strings.HasSuffix(e.Name(), ".md") && !strings.HasSuffix(e.Name(), ".mdx") {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
		fmt.Println("removed stale", rel)
	}
	return nil
}
