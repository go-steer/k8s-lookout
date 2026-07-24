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

// Package mcpserver serves the registered read-path checks as MCP
// tools (DESIGN.md §4.3): every non-hidden checks.Command becomes one
// tool whose name is the command's MCPName, whose description is the
// command's §4.4.1 micro-skill metadata, and whose input schema is
// derived mechanically from the same FlagSpecs that generate --help.
// New commands appear on this surface by registering — no wiring here.
//
// It lives under internal/ (not pkg/) deliberately: like
// internal/watch it implements a lookout subcommand, it is not one of
// lookout's contracts (those are the inject wire shape, the signal
// schema, and the CLI/MCP output contract — not this package's Go
// API), and the package name would otherwise collide with the SDK's
// own mcp package at every import site.
//
// Tool calls run the command in-process through the same emit.Run
// envelope as the CLI: identical flag parsing, --timeout enforcement,
// summary line, and the §6.5 sanitizer on every emitted finding —
// nothing on this surface bypasses it. Exit codes map per §4.2:
// 0 → the stdout payload as the tool result text, 1 → a tool error
// carrying the stderr diagnostics, 2 → a JSON-RPC invalid-params
// error.
package mcpserver

import (
	"bytes"
	"context"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// New builds the MCP server for a registry: one tool per non-hidden
// registered command, picked up automatically from the registry state
// at construction time.
func New(reg *checks.Registry, version string) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "lookout", Version: version}, nil)
	readOnly := true
	for _, c := range reg.All() {
		if c.Hidden {
			continue
		}
		server.AddTool(&mcp.Tool{
			Name:        c.MCPName,
			Description: toolDescription(c),
			InputSchema: inputSchema(c),
			// The whole surface is the read path (§4.3); write
			// verbs are not part of this design.
			Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
		}, handler(c))
	}
	return server
}

// handler adapts one command to an MCP tool handler: map the
// arguments back to argv, run in-process under emit.Run with captured
// buffers, and translate the §4.2 exit code.
func handler(c checks.Command) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args, err := argv(c, req.Params.Arguments)
		if err != nil {
			return nil, invalidParams(err.Error())
		}
		var stdout, stderr bytes.Buffer
		switch code := emit.Run(ctx, c.RunConfig(&stdout, &stderr), args); code {
		case emit.ExitData:
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: stdout.String()}},
			}, nil
		case emit.ExitUsage:
			return nil, invalidParams(diagnostics(c, &stderr))
		default: // emit.ExitRuntime
			// A tool error, not a protocol error: the model must
			// see the diagnostics to self-correct (MCP spec).
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: diagnostics(c, &stderr)}},
			}, nil
		}
	}
}

// invalidParams wraps a message as the JSON-RPC invalid-params error
// (-32602); the SDK returns *jsonrpc.Error values to the client
// verbatim instead of converting them to tool errors.
func invalidParams(msg string) error {
	return &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: msg}
}

// diagnostics returns the captured stderr, trimmed; if a failure
// preceded any diagnostic output, it still names the command rather
// than returning an empty error.
func diagnostics(c checks.Command, stderr *bytes.Buffer) string {
	if s := strings.TrimSpace(stderr.String()); s != "" {
		return s
	}
	return "lookout " + c.Name + ": failed without diagnostics"
}
