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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/inject/schema"
)

// coverageMapPage is the ONE hand-written enumeration of the
// sentinel's detection surface. Every other listing of it is
// generated — the `--sources` flag help, the signal-kind catalog, the
// startup summary block — so this page is the only place the surface
// can silently fall behind the source registry, and it did: it was
// three sources (autoscaling, ingress, gateway) and sixteen signal
// kinds stale before this test existed.
//
// The sitedoc drift test cannot cover it: the page is narrative
// (what fails, an example trigger, what it costs to enable), not a
// render of a declaration. So the test asserts the two facts that go
// stale on their own instead — every known source has a row, and the
// prose kind count matches the frozen ledger — and leaves the prose
// to the author.
const coverageMapPage = "docs/site/src/content/docs/detect/sentinel.md"

// repoRoot is the working-tree root relative to this package.
const repoRoot = "../.."

func readCoverageMap(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(coverageMapPage)))
	if err != nil {
		t.Fatalf("read %s: %v", coverageMapPage, err)
	}
	return string(b)
}

// TestCoverageMapNamesEverySource: a source the sentinel can run but
// the coverage map does not name is a source operators do not know
// they have.
func TestCoverageMapNamesEverySource(t *testing.T) {
	prose := readCoverageMap(t)
	for _, name := range knownSources {
		// The source column cell: | `name` |. Matching the cell
		// rather than the bare name keeps a passing mention in the
		// surrounding prose (or in the startup-block sample) from
		// standing in for a row.
		if !strings.Contains(prose, "| `"+name+"` |") {
			t.Errorf("%s: no coverage-map row for source %q — add one to ## The coverage map (what fails, example trigger, source, on by default, extra needs)", coverageMapPage, name)
		}
	}
}

// TestCoverageMapKindCountMatchesLedger pins the one number in the
// prose against the frozen schema ledger the /reference/signal-kinds/
// page renders. An additive v1 kind lands in the ledger and the
// generated catalog on its own; this is what makes it land here too.
func TestCoverageMapKindCountMatchesLedger(t *testing.T) {
	prose := readCoverageMap(t)
	want := fmt.Sprintf("— %d in the frozen schema —", len(schema.Kinds()))
	if !strings.Contains(prose, want) {
		t.Errorf("%s: stale signal-kind count — the frozen ledger holds %d kinds, so the prose must read %q", coverageMapPage, len(schema.Kinds()), want)
	}
}

// TestCoverageMapExplicitOnlyCount keeps the intro paragraph's "three
// of the fourteen" honest: both numbers are derived, and both moved
// the last time a source landed.
func TestCoverageMapExplicitOnlyCount(t *testing.T) {
	// Lowercased: the sentence opens the paragraph, so its first word
	// is capitalized in the prose.
	prose := strings.ToLower(readCoverageMap(t))
	explicitOnly := len(knownSources) - len(autoSourceNames)
	want := fmt.Sprintf("%s of the %s sources are never auto-enabled", numberWord(explicitOnly), numberWord(len(knownSources)))
	if !strings.Contains(prose, want) {
		t.Errorf("%s: the intro must read %q (%d known sources, %d auto candidates)", coverageMapPage, want, len(knownSources), len(autoSourceNames))
	}
}

// numberWord spells the small counts the prose uses; anything the
// table does not cover falls back to digits, which still fails the
// comparison loudly rather than passing on a typo.
func numberWord(n int) string {
	words := []string{"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen"}
	if n >= 0 && n < len(words) {
		return words[n]
	}
	return strconv.Itoa(n)
}
