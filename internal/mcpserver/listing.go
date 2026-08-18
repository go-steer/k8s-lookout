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

package mcpserver

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-steer/k8s-lookout/pkg/checks"
)

// ToolListing renders what a selection would advertise, one line per
// tool, with the bytes of tools/list JSON each contributes and the
// total.
//
// The cost is the point of the profile flags, so it is printed rather
// than described: a reader comparing `--list-tools` against
// `--profile=triage --list-tools` sees the actual number the model
// pays on every turn, not an assurance that it went down.
func ToolListing(reg *checks.Registry, tools map[string]bool) string {
	var b strings.Builder
	cmds := Advertised(reg, tools)
	for _, c := range cmds {
		fmt.Fprintf(&b, "%-26s %7d  %s\n", c.MCPName, toolBytes(c), c.Name)
	}
	fmt.Fprintf(&b, "%-26s %7d  advertised on every model call\n",
		fmt.Sprintf("%d tools", len(cmds)), SchemaBytes(reg, tools))
	return b.String()
}

// SchemaBytes is the total tools/list payload a selection advertises.
// It is the number the profile flags exist to move, so it is
// available to anything that wants to assert on it rather than
// buried in the listing's formatting.
func SchemaBytes(reg *checks.Registry, tools map[string]bool) int {
	total := 0
	for _, c := range Advertised(reg, tools) {
		total += toolBytes(c)
	}
	return total
}

// toolBytes is the serialized size of one tool as it appears in a
// tools/list response — name, micro-skill description, input schema,
// annotations. Marshaling the real mcp.Tool rather than measuring the
// pieces means the number tracks the wire shape even if the SDK
// changes it.
func toolBytes(c checks.Command) int {
	tool := &mcp.Tool{
		Name:        c.MCPName,
		Description: toolDescription(c),
		InputSchema: inputSchema(c),
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: !c.Writes},
	}
	raw, err := json.Marshal(tool)
	if err != nil {
		// Every field is a string, a bool, or a schema the same
		// marshaler already produced for the server; there is no
		// input that reaches here and fails.
		return 0
	}
	return len(raw)
}
