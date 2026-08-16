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

package sitedoc_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
)

// handWritten are the two prose tables that enumerate the command
// surface by hand. Everything else that lists commands is generated
// (--help, the MCP schemas, skills/*/references/, this site's
// Reference section), so these two are the only places the surface can
// silently fall behind the registry — and both did, by nine commands,
// before this test existed.
//
// The check is one-directional: every registered command must have a
// row. A row for a command that no longer exists is caught by the
// link checker (its /reference/ page disappears with it).
var handWritten = []struct {
	path string
	// token is the substring a row for c must contain — the first
	// cell, which is the command name in README and the tool name on
	// the MCP page.
	token func(c checks.Command) string
	hint  string
}{
	{
		path:  "README.md",
		token: func(c checks.Command) string { return "| `" + c.Name + "` " },
		hint:  "add a row to README.md ## Command surface",
	},
	{
		path:  "docs/site/src/content/docs/getting-started/mcp.md",
		token: func(c checks.Command) string { return "| `" + c.MCPName + "` " },
		hint:  "add a row to the MCP page's ## The tools table",
	},
}

func TestHandWrittenTablesCoverTheCommandSurface(t *testing.T) {
	for _, table := range handWritten {
		b, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(table.path)))
		if err != nil {
			t.Errorf("%s: %v", table.path, err)
			continue
		}
		prose := string(b)
		for _, c := range checks.All() {
			if c.Hidden {
				continue
			}
			if !strings.Contains(prose, table.token(c)) {
				t.Errorf("%s: no row for %q — %s", table.path, c.Name, table.hint)
			}
		}
	}
}
