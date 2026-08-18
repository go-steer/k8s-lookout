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

package checktest

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Golden compares got against the committed file at path, and writes
// got there instead when UPDATE_GOLDEN is set in the environment.
//
// There is exactly one way to accept a golden change in this tree:
//
//	UPDATE_GOLDEN=1 go test ./pkg/checks/...
//
// That single answer is the point. Fourteen packages used to carry
// their own copy of this function and four of them keyed off a
// `-update` test flag instead, so "how do I update the golden" had two
// answers depending on which file you happened to be in — and the
// wrong one silently did nothing.
//
// path is the full path to the golden file, testdata prefix included
// ("testdata/edges-mixed.golden"), so a grep for the file name lands
// on the test that owns it.
//
// A mismatch reports a line diff rather than two full dumps: goldens
// here run to a hundred findings, and the useful information is the
// three lines that moved.
func Golden(t *testing.T, path, got string) {
	t.Helper()
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (run with UPDATE_GOLDEN=1 to create it): %v", err)
	}
	if got == string(want) {
		return
	}
	t.Errorf("output drifted from %s — read the diff, then rerun with UPDATE_GOLDEN=1 if the new output is correct:\n%s",
		path, goldenDiff(string(want), got))
}

// GoldenBytes is Golden for output that is not a string.
func GoldenBytes(t *testing.T, path string, got []byte) {
	t.Helper()
	Golden(t, path, string(got))
}

// goldenDiff renders a line diff of want against got, `-` for lines
// only the golden has and `+` for lines only the new output has.
//
// It aligns on a longest-common-subsequence rather than by line index
// because the common failure here is one finding appearing or
// disappearing: index alignment would report every line after it as
// changed, which is exactly the report that makes people stop reading
// and rerun with UPDATE_GOLDEN=1 without looking.
func goldenDiff(want, got string) string {
	w, g := strings.Split(want, "\n"), strings.Split(got, "\n")

	// lcs[i][j] = length of the longest common subsequence of w[i:]
	// and g[j:]. Goldens are hundreds of lines at most, so the full
	// table is cheap and the walk below stays trivial to read.
	lcs := make([][]int, len(w)+1)
	for i := range lcs {
		lcs[i] = make([]int, len(g)+1)
	}
	for i := len(w) - 1; i >= 0; i-- {
		for j := len(g) - 1; j >= 0; j-- {
			if w[i] == g[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
				continue
			}
			lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
		}
	}

	var b strings.Builder
	i, j := 0, 0
	for i < len(w) && j < len(g) {
		switch {
		case w[i] == g[j]:
			fmt.Fprintf(&b, "  %s\n", w[i])
			i, j = i+1, j+1
		case lcs[i+1][j] >= lcs[i][j+1]:
			fmt.Fprintf(&b, "- %s\n", w[i])
			i++
		default:
			fmt.Fprintf(&b, "+ %s\n", g[j])
			j++
		}
	}
	for ; i < len(w); i++ {
		fmt.Fprintf(&b, "- %s\n", w[i])
	}
	for ; j < len(g); j++ {
		fmt.Fprintf(&b, "+ %s\n", g[j])
	}
	return b.String()
}
