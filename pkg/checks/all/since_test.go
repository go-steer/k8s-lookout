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
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/scan"
)

// sinceReaders maps every file under pkg/checks that reads
// `<x>.Scope.Since` to the command whose answer that read shapes. A
// file whose read does not make its command honour --since is in
// sinceIgnored instead, with the reason.
//
// When TestUsesSinceMatchesTheSource fails, update this table AND the
// command's UsesSince together: a new read means the command now
// honours --since (set UsesSince: true on it), a removed one that it no
// longer does. The `lookout mcp` response-limit hint offers since only
// to commands that declare it (#756), so a stale declaration either
// hides a narrowing that works or offers one that returns the same
// oversize answer.
var sinceReaders = map[string]string{
	"perf/perf.go":           "perf probe",
	"cloudcheck/stockout.go": "cloud stockout",
	"logs/logs.go":           "triage logs",
	"events/events.go":       "triage events",
	"triage/changes.go":      "triage changes",
	"bundle/bundle.go":       "bundle", // its events and logs sections
}

// sinceIgnored lists the files that read Scope.Since without their
// command declaring UsesSince, and why.
var sinceIgnored = map[string]string{
	// TestScanStagesIgnoreSince keeps this reason true.
	"scan/scan.go": "scan forwards --since to every stage, but no stage a bare scan runs honours it; " +
		"only the opt-in perf and cloud groups do, and offering since on a cut scan would send a model back for the same answer",
}

// TestUsesSinceMatchesTheSource keeps checks.Command.UsesSince honest
// by reading the source rather than running every command: a command
// honours --since exactly when its code reads the invocation's
// Scope.Since. It checks both directions:
//
//   - every file that reads Scope.Since is in sinceReaders or
//     sinceIgnored, so a new reader cannot ship without someone
//     deciding which command it belongs to, and every listed file
//     still reads it;
//   - the commands in the default registry that declare UsesSince are
//     exactly sinceReaders' commands.
//
// The sweep matches the selector chain `.Scope.Since`. A command that
// copied the Scope into a variable first would slip past it; nothing
// does today, and the netprobe refusal (`s.Since != 0` on a Scope
// parameter) is correctly not a reader.
func TestUsesSinceMatchesTheSource(t *testing.T) {
	readers := scopeSinceReaders(t)

	for _, file := range readers {
		_, reads := sinceReaders[file]
		_, ignored := sinceIgnored[file]
		if !reads && !ignored {
			t.Errorf("pkg/checks/%s reads Scope.Since but is not in sinceReaders: add it with the command it belongs to, and set UsesSince on that command", file)
		}
	}
	for file := range sinceIgnored {
		if !slices.Contains(readers, file) {
			t.Errorf("sinceIgnored lists pkg/checks/%s, which no longer reads Scope.Since: remove it", file)
		}
	}
	var want []string
	for file, cmd := range sinceReaders {
		if !slices.Contains(readers, file) {
			t.Errorf("sinceReaders lists pkg/checks/%s, which no longer reads Scope.Since: remove it, and UsesSince from %q if nothing else reads it for that command", file, cmd)
		}
		want = append(want, cmd)
	}
	sort.Strings(want)
	want = slices.Compact(want)

	var got []string
	for _, c := range checks.Default().All() {
		if c.UsesSince {
			got = append(got, c.Name)
		}
	}
	sort.Strings(got)
	if !slices.Equal(got, want) {
		t.Errorf("commands declaring UsesSince = %v, want %v (the commands whose code reads Scope.Since, per sinceReaders)", got, want)
	}
}

// TestScanStagesIgnoreSince is the condition scan's sinceIgnored entry
// rests on. If a default stage starts honouring --since, so does a bare
// scan: declare UsesSince on scan and move scan/scan.go to
// sinceReaders.
func TestScanStagesIgnoreSince(t *testing.T) {
	reg := checks.Default()
	for _, name := range scan.Stages() {
		if c, ok := reg.Lookup(name); ok && c.UsesSince {
			t.Errorf("scan stage %q declares UsesSince, so a bare scan honours --since: set UsesSince on scan and move scan/scan.go from sinceIgnored to sinceReaders", name)
		}
	}
}

// scopeSinceReaders returns the non-test files under pkg/checks,
// relative to it, that contain the selector chain `.Scope.Since`.
func scopeSinceReaders(t *testing.T) []string {
	t.Helper()
	const root = ".." // pkg/checks
	var out []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			return err
		}
		found := false
		ast.Inspect(f, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "Since" {
				if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "Scope" {
					found = true
				}
			}
			return !found
		})
		if found {
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking pkg/checks: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("found no Scope.Since reads under pkg/checks — the walk is broken, not the tree")
	}
	sort.Strings(out)
	return out
}
