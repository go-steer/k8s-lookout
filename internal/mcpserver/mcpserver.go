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

// Option adjusts how a server is built. Options are variadic rather
// than parameters so that adding the next one does not touch every
// call site.
type Option func(*config)

type config struct {
	// tools, when non-nil, is the set of MCP tool names to advertise
	// (ResolveTools). nil means every non-hidden command — the
	// default, and what every caller that does not care gets.
	tools map[string]bool
	// log, when non-nil, receives one line per tool call. A nil log
	// is the no-op default; AccessLog's methods tolerate it.
	log *AccessLog
}

// WithTools restricts the advertised surface to the named tools.
// A nil or empty set is ignored: the way to advertise nothing is not
// to run a server.
func WithTools(names map[string]bool) Option {
	return func(c *config) {
		if len(names) > 0 {
			c.tools = names
		}
	}
}

// WithAccessLog records one line per tool call to l (issue #281).
// A nil log leaves the server silent, which is the default.
func WithAccessLog(l *AccessLog) Option {
	return func(c *config) { c.log = l }
}

// New builds the MCP server for a registry: one tool per non-hidden
// registered command, picked up automatically from the registry state
// at construction time. WithTools narrows that to a profile or an
// explicit list (issue #280).
func New(reg *checks.Registry, version string, opts ...Option) *mcp.Server {
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "lookout", Version: version}, nil)
	for _, c := range Advertised(reg, cfg.tools) {
		server.AddTool(&mcp.Tool{
			Name:        c.MCPName,
			Description: toolDescription(c),
			InputSchema: inputSchema(c),
			// Most of this surface is the read path (§4.3) and stays
			// ReadOnlyHint:true. A command that mutates state (c.Writes
			// — the §9.4 triage-status upsert) is advertised
			// non-read-only so a convention-following client does not
			// auto-approve it as a read (issue #105).
			Annotations: &mcp.ToolAnnotations{ReadOnlyHint: !c.Writes},
		}, handler(c, cfg.log))
	}
	return server
}

// Advertised returns the commands a server built with this selection
// serves, in registry order. It is exported because the tool list is
// worth inspecting without standing a server up — `lookout mcp
// --list-tools` prints it, and the profile tests assert on it.
func Advertised(reg *checks.Registry, tools map[string]bool) []checks.Command {
	var out []checks.Command
	for _, c := range reg.All() {
		if c.Hidden {
			continue
		}
		if tools != nil && !tools[c.MCPName] {
			continue
		}
		out = append(out, c)
	}
	return out
}

// handler adapts one command to an MCP tool handler: map the
// arguments back to argv, run in-process under emit.Run with captured
// buffers, and translate the §4.2 exit code.
//
// Every return path goes through record, including the one that never
// reaches emit.Run — a client that keeps sending arguments the schema
// rejects is exactly the thing an access log is for, and it would be
// the one call shape invisible in the log if the argv error returned
// early.
func handler(c checks.Command, log *AccessLog) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		start := log.now()
		record := func(exit int, text string) {
			log.Record(c.MCPName, exit, start, len(text))
		}
		args, err := argv(c, req.Params.Arguments)
		if err != nil {
			record(emit.ExitUsage, err.Error())
			return nil, invalidParams(err.Error())
		}
		var stdout, stderr bytes.Buffer
		switch code := emit.Run(ctx, c.RunConfig(&stdout, &stderr), args); code {
		case emit.ExitData:
			record(emit.ExitData, stdout.String())
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: stdout.String()}},
			}, nil
		case emit.ExitUsage:
			msg := diagnostics(c, &stderr)
			record(emit.ExitUsage, msg)
			return nil, invalidParams(msg)
		default: // emit.ExitRuntime
			// A tool error, not a protocol error: the model must
			// see the diagnostics to self-correct (MCP spec).
			msg := diagnostics(c, &stderr)
			record(emit.ExitRuntime, msg)
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: msg}},
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
