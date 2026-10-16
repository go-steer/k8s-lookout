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
	return toolListing(reg, tools, false)
}

// MultiClusterToolListing is ToolListing for a multi-cluster endpoint
// (#700): every schema carries the cluster argument, and
// k8s_list_clusters is advertised too.
func MultiClusterToolListing(reg *checks.Registry, tools map[string]bool) string {
	return toolListing(reg, tools, true)
}

func toolListing(reg *checks.Registry, tools map[string]bool, multi bool) string {
	var b strings.Builder
	cmds := Advertised(reg, tools)
	total, count := 0, len(cmds)
	if multi {
		n := marshaledBytes(listClustersTool())
		fmt.Fprintf(&b, "%-26s %7d  %s\n", listClustersName, n, listClustersCommand.Name)
		total += n
		count++
	}
	for _, c := range cmds {
		n := toolBytes(c, multi)
		fmt.Fprintf(&b, "%-26s %7d  %s\n", c.MCPName, n, c.Name)
		total += n
	}
	fmt.Fprintf(&b, "%-26s %7d  advertised on every model call\n",
		fmt.Sprintf("%d tools", count), total)
	return b.String()
}

// SchemaBytes is the total tools/list payload a selection advertises.
// It is the number the profile flags exist to move, so it is
// available to anything that wants to assert on it rather than
// buried in the listing's formatting.
func SchemaBytes(reg *checks.Registry, tools map[string]bool) int {
	total := 0
	for _, c := range Advertised(reg, tools) {
		total += toolBytes(c, false)
	}
	return total
}

// toolBytes is the serialized size of one tool as it appears in a
// tools/list response — name, micro-skill description, input schema,
// annotations. Marshaling the real mcp.Tool rather than measuring the
// pieces means the number tracks the wire shape even if the SDK
// changes it.
func toolBytes(c checks.Command, multi bool) int {
	return marshaledBytes(&mcp.Tool{
		Name:        c.MCPName,
		Description: toolDescription(c),
		InputSchema: toolSchema(c, multi),
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: !c.Writes},
	})
}

func marshaledBytes(tool *mcp.Tool) int {
	raw, err := json.Marshal(tool)
	if err != nil {
		// Every field is a string, a bool, or a schema the same
		// marshaler already produced for the server; there is no
		// input that reaches here and fails.
		return 0
	}
	return len(raw)
}
