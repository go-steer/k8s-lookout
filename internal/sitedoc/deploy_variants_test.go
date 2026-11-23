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

package sitedoc

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// discoverDeployVariantDirs walks repoRoot to find every canonical Kustomize /
// Helm directory under `deploy/` (any directory carrying `kustomization.yaml`
// or `Chart.yaml`) plus every top-level `deploy-*` compatibility alias
// directory (#852).
func discoverDeployVariantDirs(t *testing.T, repoRoot string) []string {
	t.Helper()
	seen := map[string]bool{}

	deployRoot := filepath.Join(repoRoot, "deploy")
	err := filepath.WalkDir(deployRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() {
			return nil
		}
		if d.Name() == "templates" {
			return filepath.SkipDir
		}
		hasKust := fileExists(filepath.Join(path, "kustomization.yaml"))
		hasChart := fileExists(filepath.Join(path, "Chart.yaml"))
		if hasKust || hasChart {
			rel, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			seen[filepath.ToSlash(rel)+"/"] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking deploy/: %v", err)
	}

	entries, err := os.ReadDir(repoRoot)
	if err != nil {
		t.Fatalf("reading repo root: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "deploy-") {
			seen[e.Name()+"/"] = true
		}
	}

	out := make([]string, 0, len(seen))
	for dir := range seen {
		out = append(out, dir)
	}
	slices.Sort(out)
	return out
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

var deployTableRowRe = regexp.MustCompile("^\\|\\s*(?:\\[`([^`]+)`\\]\\([^)]+\\)|`([^`]+)`)\\s*\\|")

func parseRepoMapDeployVariantsTable(t *testing.T, markdown string) []string {
	t.Helper()
	lines := strings.Split(markdown, "\n")
	inSection := false
	var got []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "## ") {
			if trimmed == "## Deployment variants" {
				inSection = true
				continue
			}
			if inSection {
				break
			}
		}
		if !inSection {
			continue
		}
		m := deployTableRowRe.FindStringSubmatch(trimmed)
		if m == nil {
			continue
		}
		cell := m[1]
		if cell == "" {
			cell = m[2]
		}
		got = append(got, cell)
	}
	slices.Sort(got)
	return got
}

// TestDeploymentVariantsHaveREADMEAndMatchRepoMapTable enforces #852 item 6:
//  1. Every deployment/component/alias directory discovered in the repo has a
//     non-empty README.md.
//  2. The `## Deployment variants` table in `docs/repo-map.md` matches the
//     live directory listing 1:1 (neither missing a directory nor retaining a
//     removed one).
func TestDeploymentVariantsHaveREADMEAndMatchRepoMapTable(t *testing.T) {
	t.Parallel()
	repoRoot := filepath.Clean(filepath.Join("..", ".."))

	liveDirs := discoverDeployVariantDirs(t, repoRoot)
	if len(liveDirs) == 0 {
		t.Fatal("discoverDeployVariantDirs found 0 directories")
	}

	for _, dir := range liveDirs {
		readme := filepath.Join(repoRoot, filepath.FromSlash(strings.TrimSuffix(dir, "/")), "README.md")
		data, err := os.ReadFile(readme)
		if err != nil {
			t.Errorf("deployment variant %s is missing README.md (%v)", dir, err)
			continue
		}
		if len(strings.TrimSpace(string(data))) == 0 {
			t.Errorf("deployment variant %s has an empty README.md", dir)
		}
	}

	repoMapBytes, err := os.ReadFile(filepath.Join(repoRoot, "docs", "repo-map.md"))
	if err != nil {
		t.Fatalf("reading docs/repo-map.md: %v", err)
	}
	tableDirs := parseRepoMapDeployVariantsTable(t, string(repoMapBytes))
	if !slices.Equal(tableDirs, liveDirs) {
		t.Errorf("docs/repo-map.md ## Deployment variants table drifted from live deploy* directories:\n  table: %v\n   live: %v", tableDirs, liveDirs)
	}
}

func TestParseRepoMapDeployVariantsTable_DetectsDrift(t *testing.T) {
	t.Parallel()
	sample := "# Title\n\n## Deployment variants\n\n| Directory | Kind |\n| --- | --- |\n| [`deploy/`](../deploy/README.md) | alias |\n| `deploy/base/` | base |\n\n## Next section\n| `ignored/` | other |\n"
	got := parseRepoMapDeployVariantsTable(t, sample)
	want := []string{"deploy/", "deploy/base/"}
	if !slices.Equal(got, want) {
		t.Fatalf("parseRepoMapDeployVariantsTable = %v, want %v", got, want)
	}
}
