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

package all_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"

	_ "github.com/go-steer/k8s-lookout/pkg/checks/all"
)

// modPath is the module path; check packages live under modPath +
// "/pkg/checks/".
const modPath = "github.com/go-steer/k8s-lookout"

// TestAllImportsEveryRegisteringPackage is the drift gate for
// pkg/checks/all: the source tree, not a hand-kept list, decides what
// belongs in it. Any package under pkg/checks/ that calls
// checks.Register in non-test code must be blank-imported by all.go,
// and all.go must not import anything that does not. Without this a
// new check package compiles, tests green, and is simply absent from
// `lookout --help` and the generated reference until somebody notices.
func TestAllImportsEveryRegisteringPackage(t *testing.T) {
	registering := registeringPackages(t)
	imported := blankImports(t, "all.go")

	for _, pkg := range registering {
		if !imported[pkg] {
			t.Errorf("package %s calls checks.Register but pkg/checks/all/all.go does not import it\n"+
				"\tadd:\t_ %q", pkg, pkg)
		}
	}
	have := map[string]bool{}
	for _, pkg := range registering {
		have[pkg] = true
	}
	for pkg := range imported {
		if !have[pkg] {
			t.Errorf("pkg/checks/all/all.go imports %s, which registers no commands — drop the import", pkg)
		}
	}
}

// TestRegistryIsPopulated is the end-to-end version of the same
// claim: importing all must actually yield a non-empty registry with
// every §4.1 group mounted. It catches the case the static check
// cannot — a package imported and present but whose init no longer
// registers.
func TestRegistryIsPopulated(t *testing.T) {
	reg := checks.Default()
	groups := reg.Groups()
	if len(groups) == 0 {
		t.Fatal("no groups registered after importing pkg/checks/all")
	}
	for _, group := range groups {
		if checks.GroupSummary(group) == "" {
			t.Errorf("group %q has no summary", group)
		}
	}
	if len(reg.TopLevel()) == 0 {
		t.Error("no top-level commands registered after importing pkg/checks/all")
	}
}

// registeringPackages walks pkg/checks/ for non-test files containing
// a checks.Register call and returns their import paths, sorted.
func registeringPackages(t *testing.T) []string {
	t.Helper()
	const root = ".." // pkg/checks
	seen := map[string]bool{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if !strings.Contains(string(src), "checks.Register(") {
			return nil
		}
		rel, err := filepath.Rel(root, filepath.Dir(p))
		if err != nil {
			return err
		}
		if rel == "." {
			// pkg/checks itself defines Register; it cannot import
			// the packages that call it.
			return nil
		}
		seen[path.Join(modPath, "pkg/checks", filepath.ToSlash(rel))] = true
		return nil
	})
	if err != nil {
		t.Fatalf("walking pkg/checks: %v", err)
	}
	if len(seen) == 0 {
		t.Fatal("found no packages calling checks.Register — the walk is broken, not the tree")
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// blankImports returns the set of blank-imported paths in a file.
func blankImports(t *testing.T, file string) map[string]bool {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parsing %s: %v", file, err)
	}
	out := map[string]bool{}
	for _, imp := range f.Imports {
		if imp.Name == nil || imp.Name.Name != "_" {
			continue
		}
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			t.Fatalf("bad import path %s in %s: %v", imp.Path.Value, file, err)
		}
		out[p] = true
	}
	return out
}
