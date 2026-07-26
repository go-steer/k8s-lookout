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

// Command gen regenerates the skill reference stubs under
// skills/*/references/ from the pkg/checks registry. Run it via
// dev/tools/gen-skill-refs (re-runnable; output is deterministic).
// Stale stubs — files for commands a skill no longer references —
// are deleted, so the references directories always mirror
// skilldoc.SkillCommands exactly.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-steer/k8s-lookout/internal/skilldoc"
	"github.com/go-steer/k8s-lookout/pkg/checks"

	// Read-path command implementations register themselves into
	// the default registry from their init functions — the same
	// set cmd/lookout mounts.
	_ "github.com/go-steer/k8s-lookout/pkg/checks/bundle"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/delta"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/events"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/health"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/logs"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/netprobe"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/perf"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/stab"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/state"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/triage"
)

func main() {
	root := flag.String("root", ".", "repository root (the directory containing skills/)")
	flag.Parse()
	if err := run(*root); err != nil {
		fmt.Fprintln(os.Stderr, "gen-skill-refs:", err)
		os.Exit(1)
	}
}

func run(root string) error {
	files, err := skilldoc.GenerateAll(checks.Default())
	if err != nil {
		return err
	}
	// Write every generated stub.
	for rel, content := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return err
		}
		fmt.Println("wrote", rel)
	}
	// Delete stale stubs in the managed references directories.
	for skill := range skilldoc.SkillCommands {
		dir := filepath.Join(root, "skills", skill, "references")
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			rel := "skills/" + skill + "/references/" + e.Name()
			if _, keep := files[rel]; keep || !strings.HasSuffix(e.Name(), ".md") {
				continue
			}
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
			fmt.Println("removed stale", rel)
		}
	}
	return nil
}
