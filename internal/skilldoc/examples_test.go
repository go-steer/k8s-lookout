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

package skilldoc_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// TestSkillDocCommandLinesParse is the §4.4.3 contract test over the
// hand-written skill docs: every command line inside a fenced block
// marked ```lookout — in any SKILL.md, playbook, or generated
// reference under skills/ — must resolve against the pkg/checks
// registry and parse under the real §4.2 runner (command exists,
// flags declared, values well-typed, positional count respected).
// The command is NOT executed: its Run is stubbed, so this validates
// the invocation surface only.
func TestSkillDocCommandLinesParse(t *testing.T) {
	docs := skillDocs(t)
	total := 0
	for _, doc := range docs {
		for _, block := range fencedBlocks(t, doc, "lookout") {
			for _, line := range block.lines {
				line = strings.TrimSpace(line)
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				total++
				if err := validateCommandLine(t, line); err != nil {
					t.Errorf("%s:%d: %q: %v", doc, block.start, line, err)
				}
			}
		}
	}
	if total == 0 {
		t.Fatal("no ```lookout command lines found under skills/ — the contract test is not covering anything")
	}
	t.Logf("validated %d command lines", total)
}

// TestSkillDocGoldenSnippetsMatchFixtures keeps quoted output
// snippets honest: every line inside a fenced block marked
// ```lookout-golden must appear verbatim in one of the checktest
// golden fixtures under pkg/checks/**/testdata/*.golden (elision
// lines starting with "…" are exempt). Skill docs therefore quote
// real command output — when a command's output format changes, its
// golden changes, and this test names the stale doc.
func TestSkillDocGoldenSnippetsMatchFixtures(t *testing.T) {
	golden := goldenLines(t)
	docs := skillDocs(t)
	total := 0
	for _, doc := range docs {
		for _, block := range fencedBlocks(t, doc, "lookout-golden") {
			for _, line := range block.lines {
				if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "…") {
					continue
				}
				total++
				if !golden[line] {
					t.Errorf("%s:%d: line not found in any pkg/checks testdata golden:\n%s", doc, block.start, line)
				}
			}
		}
	}
	if total == 0 {
		t.Fatal("no ```lookout-golden lines found under skills/")
	}
	t.Logf("validated %d output lines against golden fixtures", total)
}

// validateCommandLine parses one documented command line against the
// registry using the production runner with a stubbed Run.
//
// A line may be a PIPELINE of lookout invocations (`lookout health |
// lookout findings diff --report=-`, the canonical shape of the
// §4.2-report-consuming commands); every stage is validated, so a
// stale flag on either side of the pipe fails here. The pipe must be
// its own whitespace-separated token.
func validateCommandLine(t *testing.T, line string) error {
	toks, err := shellSplit(line)
	if err != nil {
		return err
	}
	var stage []string
	for _, tok := range toks {
		if tok == "|" {
			if err := validateInvocation(t, stage); err != nil {
				return err
			}
			stage = nil
			continue
		}
		stage = append(stage, tok)
	}
	return validateInvocation(t, stage)
}

// validateInvocation validates one `lookout …` stage.
func validateInvocation(t *testing.T, toks []string) error {
	if len(toks) < 2 || toks[0] != "lookout" {
		return fmt.Errorf("not a `lookout <command>` line")
	}
	reg := checks.Default()
	var c checks.Command
	var ok bool
	var args []string
	if len(toks) >= 3 && !strings.HasPrefix(toks[2], "-") {
		if c, ok = reg.Lookup(toks[1] + " " + toks[2]); ok {
			args = toks[3:]
		}
	}
	if !ok {
		if c, ok = reg.Lookup(toks[1]); ok {
			args = toks[2:]
		}
	}
	if !ok {
		return fmt.Errorf("no such command in the pkg/checks registry")
	}
	c.Run = func(context.Context, emit.Invocation) (int, error) { return 0, nil }
	res := checktest.Run(t, c, args...)
	if res.Code == emit.ExitUsage {
		return fmt.Errorf("usage error: %s", strings.TrimSpace(res.Stderr))
	}
	return nil
}

// skillDocs returns every markdown file under skills/.
func skillDocs(t *testing.T) []string {
	t.Helper()
	var docs []string
	err := filepath.WalkDir(filepath.Join(repoRoot, "skills"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".md") {
			docs = append(docs, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) == 0 {
		t.Fatal("no markdown files under skills/")
	}
	return docs
}

// goldenLines loads every line of every *.golden fixture under
// pkg/checks into a set.
func goldenLines(t *testing.T) map[string]bool {
	t.Helper()
	set := map[string]bool{}
	err := filepath.WalkDir(filepath.Join(repoRoot, "pkg", "checks"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".golden") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(data), "\n") {
			if line != "" {
				set[line] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(set) == 0 {
		t.Fatal("no golden fixtures found under pkg/checks")
	}
	return set
}

// block is one fenced code block.
type block struct {
	start int // 1-based line number of the opening fence
	lines []string
}

// fencedBlocks extracts the fenced blocks with the given info string
// from a markdown file.
func fencedBlocks(t *testing.T, path, info string) []block {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []block
	var cur *block
	inOther := false
	for i, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case cur != nil:
			if trimmed == "```" {
				out = append(out, *cur)
				cur = nil
			} else {
				cur.lines = append(cur.lines, line)
			}
		case inOther:
			if trimmed == "```" {
				inOther = false
			}
		case strings.HasPrefix(trimmed, "```"):
			if strings.TrimPrefix(trimmed, "```") == info {
				cur = &block{start: i + 1}
			} else {
				inOther = true
			}
		}
	}
	if cur != nil {
		t.Fatalf("%s: unterminated ``` fence opened at line %d", path, cur.start)
	}
	return out
}

// shellSplit splits a documented command line into tokens, honoring
// single and double quotes (no expansion — docs are literal).
func shellSplit(line string) ([]string, error) {
	var toks []string
	var cur strings.Builder
	started := false
	quote := rune(0)
	for _, r := range line {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '\'' || r == '"':
			quote = r
			started = true
		case r == ' ' || r == '\t':
			if started {
				toks = append(toks, cur.String())
				cur.Reset()
				started = false
			}
		default:
			cur.WriteRune(r)
			started = true
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated %c quote", quote)
	}
	if started {
		toks = append(toks, cur.String())
	}
	return toks, nil
}
