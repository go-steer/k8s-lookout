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

// Command lookout is the single multicall binary of k8s-lookout
// (DESIGN.md §4.1): deterministic, token-dense reads of Kubernetes/GKE
// clusters for agent-driven troubleshooting, plus the resident
// per-cluster sentinel, `lookout watch`.
//
// Exit codes follow the §4.2 CLI contract: 0 data, 1 runtime error,
// 2 usage error. Diagnostics go to stderr only; stdout is pure payload.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/go-steer/k8s-lookout/internal/version"
)

// command is one lookout subcommand. run receives the args after the
// subcommand name and returns the process exit code per the §4.2
// contract (0 data / 1 runtime / 2 usage).
type command struct {
	name    string
	summary string
	run     func(ctx context.Context, args []string) int
}

// commands is the multicall registry. Subcommands register themselves
// in their own file's init() so each milestone lands as an isolated
// diff (M0: watch; M1: triage, state, bundle, health, mcp; …).
var commands = map[string]command{}

func register(c command) {
	if _, dup := commands[c.name]; dup {
		panic("duplicate lookout subcommand: " + c.name)
	}
	commands[c.name] = c
}

func init() {
	register(command{
		name:    "version",
		summary: "print the lookout version",
		run: func(_ context.Context, _ []string) int {
			fmt.Println(version.String("lookout"))
			return 0
		},
	})
}

func usage(w *os.File) {
	fmt.Fprintf(w, "Usage: lookout <command> [flags]\n\nCommands:\n")
	names := make([]string, 0, len(commands))
	for name := range commands {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(w, "  %-10s %s\n", name, commands[name].summary)
	}
	fmt.Fprintf(w, "\nRun 'lookout <command> --help' for command-specific flags.\n")
}

func main() {
	os.Exit(realMain(os.Args[1:]))
}

func realMain(args []string) int {
	if len(args) == 0 {
		usage(os.Stderr)
		return 2
	}
	switch args[0] {
	case "-h", "--help", "help":
		usage(os.Stdout)
		return 0
	case "-version", "--version":
		// Flag spelling of the version subcommand (#146: operators
		// and scripts expect --version to work on any binary).
		fmt.Println(version.String("lookout"))
		return 0
	}
	cmd, ok := commands[args[0]]
	if !ok {
		fmt.Fprintf(os.Stderr, "lookout: unknown command %q\n\n", args[0])
		usage(os.Stderr)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return cmd.run(ctx, args[1:])
}
