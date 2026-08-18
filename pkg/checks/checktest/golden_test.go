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
	"os"
	"path/filepath"
	"testing"
)

// TestGolden_WritesUnderUpdateGolden covers the whole loop a
// contributor runs: no file, UPDATE_GOLDEN=1 creates it, and a
// second pass with the same output passes.
func TestGolden_WritesUnderUpdateGolden(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "out.golden")
	t.Setenv("UPDATE_GOLDEN", "1")
	Golden(t, path, "kind=demo.finding severity=warning\n")

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("golden not written: %v", err)
	}
	if string(b) != "kind=demo.finding severity=warning\n" {
		t.Errorf("wrote %q", b)
	}

	t.Setenv("UPDATE_GOLDEN", "")
	Golden(t, path, "kind=demo.finding severity=warning\n")
}

// TestGoldenDiff_AlignsOnInsertion is the reason this is an LCS diff
// and not an index-by-index comparison: one finding appearing in the
// middle must report as one added line, not as every line after it
// having changed.
func TestGoldenDiff_AlignsOnInsertion(t *testing.T) {
	want := "a\nb\nc\n"
	got := "a\nNEW\nb\nc\n"
	if diff := goldenDiff(want, got); diff != "  a\n+ NEW\n  b\n  c\n  \n" {
		t.Errorf("insertion diff:\n%s", diff)
	}
}

func TestGoldenDiff_ReportsBothSides(t *testing.T) {
	if diff := goldenDiff("a\nb\n", "a\nz\n"); diff != "  a\n- b\n+ z\n  \n" {
		t.Errorf("substitution diff:\n%s", diff)
	}
	if diff := goldenDiff("a\nb\n", "a\n"); diff != "  a\n- b\n  \n" {
		t.Errorf("deletion diff:\n%s", diff)
	}
	if diff := goldenDiff("", ""); diff != "  \n" {
		t.Errorf("empty diff: %q", diff)
	}
}
