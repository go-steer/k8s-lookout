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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"

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

// TestUATInvocationsCoversEveryRegisteredCommand is the hermetic presubmit
// twin of examples/uat-cases/00-contract.sh: every command advertised by
// `lookout mcp --list-tools` (!c.Hidden && !c.Privileged) must have an entry
// in UAT_INVOCATION in examples/uat-invocations.sh, no stale entries may
// remain, and every command listed in UAT_SCOPE_REJECTERS must exit 2
// (ExitUsage) when passed -A (#798).
func TestUATInvocationsCoversEveryRegisteredCommand(t *testing.T) {
	uatFile := filepath.Join(repoRoot, "examples", "uat-invocations.sh")
	raw, err := os.ReadFile(uatFile)
	if err != nil {
		t.Fatalf("reading %s: %v", uatFile, err)
	}
	src := string(raw)

	invRe := regexp.MustCompile(`(?m)^\s*\["([^"]+)"\]="([^"]*)"`)
	invocations := map[string]string{}
	for _, m := range invRe.FindAllStringSubmatch(src, -1) {
		invocations[m[1]] = m[2]
	}
	if len(invocations) == 0 {
		t.Fatalf("parsed zero UAT_INVOCATION entries from %s", uatFile)
	}

	reg := checks.Default()
	advertised := map[string]checks.Command{}
	for _, c := range reg.All() {
		if c.Hidden || c.Privileged {
			continue
		}
		advertised[c.Name] = c
		if _, ok := invocations[c.Name]; !ok {
			t.Errorf("command %q is advertised by `lookout mcp --list-tools` but has no UAT_INVOCATION entry in examples/uat-invocations.sh", c.Name)
		}
	}
	for name := range invocations {
		if _, ok := advertised[name]; !ok {
			t.Errorf("examples/uat-invocations.sh has stale UAT_INVOCATION entry %q", name)
		}
	}

	blockRe := regexp.MustCompile(`(?ms)UAT_SCOPE_REJECTERS=\((.*?)\)`)
	blockMatch := blockRe.FindStringSubmatch(src)
	if len(blockMatch) < 2 {
		t.Fatalf("could not locate UAT_SCOPE_REJECTERS block in %s", uatFile)
	}
	strRe := regexp.MustCompile(`"([^"]+)"`)
	for _, m := range strRe.FindAllStringSubmatch(blockMatch[1], -1) {
		name := m[1]
		c, ok := advertised[name]
		if !ok {
			t.Errorf("UAT_SCOPE_REJECTERS names unknown command %q", name)
			continue
		}
		var args []string
		for _, tok := range strings.Fields(invocations[name]) {
			if !strings.Contains(tok, "$") {
				args = append(args, tok)
			}
		}
		args = append(args, "-A")
		res := checktest.Run(t, c, args...)
		if res.Code != emit.ExitUsage {
			t.Errorf("UAT_SCOPE_REJECTERS command %q with %v exited %d, want %d (ExitUsage); stderr=%s",
				name, args, res.Code, emit.ExitUsage, strings.TrimSpace(res.Stderr))
		}
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
		switch rel {
		case ".":
			// pkg/checks itself defines Register; it cannot import
			// the packages that call it.
			return nil
		case "all":
			// This package registers `scan` directly — see all.go on
			// why that one command cannot register itself. It plainly
			// does not need to blank-import itself.
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
