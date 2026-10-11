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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/all"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

const repoRoot = "../../.."

// TestAllSkillPlaybookCLIFlagsValid verifies every `lookout <subcmd> ...` CLI
// invocation across ALL `skills/*/SKILL.md` and `skills/playbooks/*.md` files —
// including inline markdown backtick spans (`lookout ...`) as well as fenced
// code blocks — against the registered CLI command and flag specs (#671).
func TestAllSkillPlaybookCLIFlagsValid(t *testing.T) {
	pattern := filepath.Join(repoRoot, "skills", "*", "SKILL.md")
	skillFiles, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}
	playbooks, err := filepath.Glob(filepath.Join(repoRoot, "skills", "playbooks", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	skillFiles = append(skillFiles, playbooks...)
	if len(skillFiles) == 0 {
		t.Fatal("no SKILL.md playbooks found under skills/")
	}

	totalInvocations := 0
	for _, file := range skillFiles {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, cand := range extractLookoutInvocations(string(raw)) {
			totalInvocations++
			if err := verifyLookoutCLIInvocation(t, cand.line); err != nil {
				t.Errorf("%s:%d: %q: %v", file, cand.lineNo, cand.line, err)
			}
		}
	}
	if totalInvocations == 0 {
		t.Fatal("no lookout CLI invocations extracted from skills/*")
	}
}

type invocationCandidate struct {
	lineNo int
	line   string
}

func extractLookoutInvocations(markdown string) []invocationCandidate {
	var out []invocationCandidate
	lines := strings.Split(markdown, "\n")
	inFence := false
	fenceInfo := ""

	for idx, rawLine := range lines {
		trimmed := strings.TrimSpace(rawLine)
		if strings.HasPrefix(trimmed, "```") {
			if !inFence {
				inFence = true
				fenceInfo = strings.TrimSpace(strings.TrimPrefix(trimmed, "```"))
			} else {
				inFence = false
				fenceInfo = ""
			}
			continue
		}
		if inFence {
			if fenceInfo == "lookout" || fenceInfo == "" || fenceInfo == "sh" || fenceInfo == "bash" {
				if strings.HasPrefix(trimmed, "lookout ") && !isProseOrMetaRef(trimmed) {
					out = append(out, invocationCandidate{lineNo: idx + 1, line: trimmed})
				}
			}
			continue
		}
		// Extract inline backtick code spans containing concrete `lookout <cmd> ...` calls.
		for _, span := range inlineBacktickSpans(rawLine) {
			span = strings.TrimSpace(span)
			if strings.HasPrefix(span, "lookout ") && !isProseOrMetaRef(span) {
				out = append(out, invocationCandidate{lineNo: idx + 1, line: span})
			}
		}
	}
	return out
}

func inlineBacktickSpans(line string) []string {
	var spans []string
	inTick := false
	var cur strings.Builder
	for i := 0; i < len(line); i++ {
		if line[i] == '`' {
			if inTick {
				spans = append(spans, cur.String())
				cur.Reset()
				inTick = false
			} else {
				inTick = true
			}
			continue
		}
		if inTick {
			cur.WriteByte(line[i])
		}
	}
	return spans
}

// isProseOrMetaRef skips non-check multicall binaries (`lookout watch`,
// `lookout mcp`), bare group names (`lookout cloud`), or template placeholders
// containing `<window ...>`.
func isProseOrMetaRef(s string) bool {
	if strings.HasPrefix(s, "lookout watch") || strings.HasPrefix(s, "lookout mcp") {
		return true
	}
	if strings.Contains(s, "<window ") || strings.HasSuffix(s, "[flags]") {
		return true
	}
	toks := strings.Fields(s)
	if len(toks) < 2 {
		return true
	}
	// Bare command group names referenced in prose (e.g. `lookout cloud` or `lookout audit`).
	if len(toks) == 2 && (toks[1] == "cloud" || toks[1] == "audit" || toks[1] == "triage" || toks[1] == "state" || toks[1] == "stab" || toks[1] == "net" || toks[1] == "perf" || toks[1] == "findings") {
		return true
	}
	return false
}

func verifyLookoutCLIInvocation(t *testing.T, line string) error {
	toks, err := splitShellWords(line)
	if err != nil {
		return err
	}
	var stage []string
	for _, tok := range toks {
		if tok == "|" {
			if err := verifyLookoutStage(t, stage); err != nil {
				return err
			}
			stage = nil
			continue
		}
		stage = append(stage, tok)
	}
	return verifyLookoutStage(t, stage)
}

func verifyLookoutStage(t *testing.T, toks []string) error {
	if len(toks) < 2 || toks[0] != "lookout" {
		return fmt.Errorf("expected `lookout <command>`, got %v", toks)
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
		return fmt.Errorf("unknown command in %q", strings.Join(toks, " "))
	}
	c.Run = func(context.Context, emit.Invocation) (int, error) { return 0, nil }
	res := checktest.Run(t, c, args...)
	if res.Code == emit.ExitUsage {
		return fmt.Errorf("invalid CLI flags/args: %s", strings.TrimSpace(res.Stderr))
	}
	return nil
}

func splitShellWords(line string) ([]string, error) {
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
		return nil, fmt.Errorf("unterminated quote %c", quote)
	}
	if started {
		toks = append(toks, cur.String())
	}
	return toks, nil
}
