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

package sources

import (
	"os/exec"
	"strings"
	"testing"
)

const modulePath = "github.com/go-steer/k8s-lookout"

// layeringRules are the import-graph invariants of the repo: which
// packages may reach which. They are one Go compiler short of being
// enforced — everything here is the same module, so nothing but this
// test stops `pkg/sources/rollout` importing `internal/watch` and
// welding the sources to the sentinel forever.
//
// docs/leeway-design.md §2.4 names four disciplines the standalone
// binary depends on and observes that they are "cheap to keep working
// and expensive to retrofit". Two of them are import-graph properties:
// discipline 1 (pkg/leeway never reaches client-go) and discipline 4
// (sources import pkg/* only). This is where they stop being
// conventions. The other two — informers and the meter passed in, not
// built in — are API shape, not import graph, and cmd/leeway compiling
// is what checks those.
//
// Each rule is checked against the TRANSITIVE closure, not the offending
// package's own import block. A direct-import check is trivially
// satisfiable by accident: one innocuous helper that itself imports the
// sentinel would put the whole thing back on the path while every file
// in sight still looked clean.
var layeringRules = []struct {
	// name is what the failure is called, not what it does.
	name string
	// pattern is the `go list` pattern for the packages under the rule.
	pattern string
	// forbidden are import-path prefixes those packages must not reach.
	forbidden []string
	// why is printed on failure. A layering test that says only "this is
	// forbidden" gets the rule deleted by the next person who needs the
	// import; one that says what breaks gets the design reconsidered.
	why string
}{
	{
		name:      "pkg/... never reaches internal/...",
		pattern:   modulePath + "/pkg/...",
		forbidden: []string{modulePath + "/internal"},
		why: "§2.4 discipline 4: the sources and the engine are the embeddable half of this repo — " +
			"cmd/leeway runs them, and #416 anticipates pkg/leeway being vendored somewhere that is not " +
			"lookout at all. internal/ is the sentinel: its inject path, its store, its flag surface. One " +
			"import from pkg/ into it makes every one of those a transitive dependency of the subsystem, " +
			"and Go's internal/ rule will not catch it because this is all one module. The dependency runs " +
			"one way: internal/watch wires up pkg/sources, never the reverse.",
	},
	{
		name:      "pkg/leeway stays pure",
		pattern:   modulePath + "/pkg/leeway",
		forbidden: []string{"k8s.io/client-go", "k8s.io/metrics", "k8s.io/kubectl"},
		why: "§2.4 discipline 1 and NFR-10: the scoring engine is pure functions over plain structs, " +
			"which is what lets it be tested without a cluster and embedded in something that has no " +
			"client-go. Also checked inside the package by TestPkgLeeway_ImportsNothingThatTouchesACluster, " +
			"which mutation-tests its own matcher; the duplication is deliberate, so that deleting one " +
			"file does not silently retire the rule.",
	},
	{
		name:      "cmd/leeway is the subsystem, not a second sentinel",
		pattern:   modulePath + "/cmd/leeway",
		forbidden: []string{modulePath + "/internal/watch", modulePath + "/pkg/store"},
		why: "the standalone binary earns its keep by being the thing that FAILS when a discipline slips, " +
			"and it cannot do that if it is allowed to reach for the sentinel the moment something is " +
			"missing. Store-less is a supported posture (§9.1: current state is rebuildable, so a restart " +
			"costs the in-flight dwell timers and nothing else), and reaching into internal/watch for the " +
			"parts that are awkward to duplicate is how this stops being a standalone binary.",
	},
}

func TestLayering_TheImportGraphMatchesTheDesign(t *testing.T) {
	for _, rule := range layeringRules {
		t.Run(rule.name, func(t *testing.T) {
			for pkg, deps := range closures(t, rule.pattern) {
				for _, dep := range deps {
					for _, prefix := range rule.forbidden {
						if underPrefix(dep, prefix) {
							t.Errorf("%s transitively imports %s\n  forbidden prefix: %s\n  why: %s",
								pkg, dep, prefix, rule.why)
						}
					}
				}
			}
		})
	}
}

// closures maps each package matching pattern to its transitive import
// closure. Per-package rather than one merged list so a failure names
// the package that has to change, which is the difference between a
// test somebody fixes and a test somebody deletes.
func closures(t *testing.T, pattern string) map[string][]string {
	t.Helper()
	// Deliberately not skipped when `go` is missing, for the same reason
	// the purity guard is not: a guard that quietly opts out is worse
	// than no guard, because the repo keeps advertising a property
	// nothing is checking.
	out, err := exec.Command("go", "list", "-f", "{{.ImportPath}}\t{{join .Deps \" \"}}", pattern).CombinedOutput()
	if err != nil {
		t.Fatalf("go list %s: %v\n%s", pattern, err, out)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	got := make(map[string][]string, len(lines))
	for _, line := range lines {
		pkg, deps, ok := strings.Cut(strings.TrimSpace(line), "\t")
		if !ok || pkg == "" {
			continue
		}
		got[pkg] = strings.Fields(deps)
	}
	if len(got) == 0 {
		t.Fatalf("go list %s matched no packages — the rule is guarding nothing", pattern)
	}
	return got
}

// underPrefix reports whether dep is prefix or lives beneath it,
// respecting path boundaries: "k8s.io/client-gopher" is not client-go.
func underPrefix(dep, prefix string) bool {
	return dep == prefix || strings.HasPrefix(dep, prefix+"/")
}

// TestUnderPrefix_RespectsPathBoundaries proves the matcher works, so a
// typo in a rule's prefix cannot turn the guard above into a no-op that
// passes forever.
func TestUnderPrefix_RespectsPathBoundaries(t *testing.T) {
	cases := []struct {
		dep, prefix string
		want        bool
	}{
		{modulePath + "/internal", modulePath + "/internal", true},
		{modulePath + "/internal/watch", modulePath + "/internal", true},
		{modulePath + "/internal/watch/sub", modulePath + "/internal", true},
		{modulePath + "/pkg/sources", modulePath + "/internal", false},
		{modulePath + "/internalise", modulePath + "/internal", false},
		{"k8s.io/client-go/kubernetes", "k8s.io/client-go", true},
		{"k8s.io/client-gopher", "k8s.io/client-go", false},
		{"k8s.io/api/core/v1", "k8s.io/client-go", false},
	}
	for _, c := range cases {
		if got := underPrefix(c.dep, c.prefix); got != c.want {
			t.Errorf("underPrefix(%q, %q) = %v, want %v", c.dep, c.prefix, got, c.want)
		}
	}
}

// TestLayeringRules_EveryRuleMatchesSomething is the other half of the
// guard: a rule whose pattern has gone stale — a package renamed, a
// directory moved — would pass silently forever while enforcing nothing.
// closures() fatals on an empty match, so running each pattern once is
// enough to prove every rule still has packages under it.
func TestLayeringRules_EveryRuleMatchesSomething(t *testing.T) {
	for _, rule := range layeringRules {
		if len(rule.forbidden) == 0 {
			t.Errorf("rule %q forbids nothing", rule.name)
		}
		if len(closures(t, rule.pattern)) == 0 {
			t.Errorf("rule %q matches no packages", rule.name)
		}
	}
}
