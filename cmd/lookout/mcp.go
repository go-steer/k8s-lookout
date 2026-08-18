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

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/go-steer/k8s-lookout/internal/mcpserver"
	"github.com/go-steer/k8s-lookout/internal/version"
	"github.com/go-steer/k8s-lookout/pkg/checks"
)

// lookout mcp registers in the multicall root like watch — it is a
// server, not a check, so it does not belong in the pkg/checks
// registry it serves. Every non-hidden registered command becomes an
// MCP tool automatically (§4.3); commands added later appear here
// with no changes to this file.
func init() {
	register(command{
		name:    "mcp",
		summary: "serve the read-path checks as MCP tools (stdio, or --listen for localhost HTTP)",
		run:     runMCP,
	})
}

const mcpHelp = `Usage: lookout mcp [flags]

Serve every registered read-path check as an MCP tool (§4.3): tool
names are the commands' MCP names (e.g. triage delta →
k8s_triage_delta), input schemas mirror the CLI flags plus a "target"
property where a command takes a positional argument, and tool
results carry the same payload the CLI prints — logfmt findings
terminated by the scanned=/findings=/elapsed= summary line, sanitized
by §6.5.

Flags:
  --listen=<host:port>  serve streamable HTTP on a loopback address
                        (e.g. 127.0.0.1:8383) instead of stdio.
                        Non-loopback binds are refused: this server
                        has no auth story, so it never listens on a
                        routable interface (§4.3).
  --profile=<name>      advertise only one curated tool surface
                        instead of all of them (see below).
  --tools=<selection>   adjust the surface tool by tool, left to
                        right: "all" adds every tool, a profile name
                        adds its members, a tool name adds one, and a
                        "-" prefix removes. Combines with --profile,
                        which is evaluated first — so
                        "--profile=triage --tools=-k8s_triage_logs"
                        is the triage surface minus the logs tool.
  --list-tools          print the tools this selection would
                        advertise, with the JSON schema bytes each
                        costs on every model call, and exit.
  --access-log=<path>   append one logfmt line per tool call:
                        ts, tool, exit code, duration, response
                        bytes. Created if absent, appended if not,
                        mode 0600. Deliberately not the arguments or
                        the response body — the log is an operational
                        record, not a second copy of cluster data.

Profiles:
%s
Every tool advertised is paid for on every model call, in tokens and
in the model's accuracy at choosing among near-identical options — the
full surface is well over a hundred kilobytes of JSON schema. Serve
the smallest surface that answers the questions the agent asks.

With no flags the transport is stdio: JSON-RPC on stdin/stdout, the
transport a daemon uses when it spawns "lookout mcp" as a child
process. Diagnostics go to stderr only.
`

func runMCP(ctx context.Context, args []string) int {
	return mcpMain(ctx, args, os.Stdout, os.Stderr)
}

// mcpMain is runMCP with injectable streams for tests.
func mcpMain(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("lookout mcp", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	listen := fs.String("listen", "", "loopback host:port for streamable HTTP; empty for stdio")
	profile := fs.String("profile", "", "advertise one curated tool surface instead of all of them")
	tools := fs.String("tools", "", "adjust the advertised tools: all,<profile>,<tool>,-<tool>, left to right")
	listTools := fs.Bool("list-tools", false, "print the selected tools and their schema cost, then exit")
	accessLog := fs.String("access-log", "", "append one line per tool call to this path")
	reg := checks.Default()
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			fmt.Fprintf(stdout, mcpHelp, mcpserver.ProfileHelp(reg))
			return 0
		}
		fmt.Fprintf(stderr, "lookout mcp: %v\nRun 'lookout mcp --help' for usage.\n", err)
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "lookout mcp: unexpected argument %q\nRun 'lookout mcp --help' for usage.\n", fs.Arg(0))
		return 2
	}
	if *listen != "" {
		if err := mcpserver.ValidateLoopback(*listen); err != nil {
			fmt.Fprintf(stderr, "lookout mcp: %v\n", err)
			return 2
		}
	}
	selected, err := mcpserver.ResolveTools(reg, *profile, *tools)
	if err != nil {
		fmt.Fprintf(stderr, "lookout mcp: %v\nRun 'lookout mcp --help' for usage.\n", err)
		return 2
	}
	if *listTools {
		fmt.Fprint(stdout, mcpserver.ToolListing(reg, selected))
		return 0
	}

	var log *mcpserver.AccessLog
	if *accessLog != "" {
		l, closer, err := mcpserver.OpenAccessLog(*accessLog)
		if err != nil {
			// A log that cannot be opened is a usage error, not a
			// degraded mode: an operator who asked to be able to
			// answer "what did the agent call" must not find out
			// afterwards that nothing was recorded.
			fmt.Fprintf(stderr, "lookout mcp: %v\n", err)
			return 2
		}
		defer func() { _ = closer.Close() }()
		log = l
	}

	server := mcpserver.New(reg, version.Semver(),
		mcpserver.WithTools(selected), mcpserver.WithAccessLog(log))
	if err := mcpserver.Serve(ctx, server, *listen, nil); err != nil {
		fmt.Fprintf(stderr, "lookout mcp: %v\n", err)
		return 1
	}
	return 0
}
