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
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			fmt.Fprint(stdout, mcpHelp)
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

	server := mcpserver.New(checks.Default(), version.Version)
	if err := mcpserver.Serve(ctx, server, *listen, nil); err != nil {
		fmt.Fprintf(stderr, "lookout mcp: %v\n", err)
		return 1
	}
	return 0
}
