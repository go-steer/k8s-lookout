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
	"fmt"
	"io"
	"os"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"

	// Read-path command implementations register themselves into
	// the default registry from their init functions.
	_ "github.com/go-steer/k8s-lookout/pkg/checks/bundle"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/cloudcheck"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/delta"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/events"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/health"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/logs"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/netprobe"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/state"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/top"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/triage"
)

// Read-path commands resolve through the pkg/checks registry: each
// §4.1 group ("triage", "state", …) mounts as one multicall command
// whose subcommands are looked up at invocation time, and top-level
// checks ("bundle", "health") mount directly. Nothing is mounted
// until a check registers itself — commands appear in `lookout
// --help` in the same change that implements them.
func init() {
	for _, c := range checkCommands(checks.Default()) {
		register(c)
	}
}

// checkCommands builds the multicall commands for a registry.
func checkCommands(reg *checks.Registry) []command {
	var out []command
	for _, g := range reg.Groups() {
		group := g
		out = append(out, command{
			name:    group,
			summary: checks.GroupSummary(group),
			run: func(ctx context.Context, args []string) int {
				return runGroup(ctx, reg, group, args, os.Stdout, os.Stderr)
			},
		})
	}
	for _, tl := range reg.TopLevel() {
		c := tl
		out = append(out, command{
			name:    c.Name,
			summary: c.Summary,
			run: func(ctx context.Context, args []string) int {
				return emit.Run(ctx, c.RunConfig(os.Stdout, os.Stderr), args)
			},
		})
	}
	return out
}

// runGroup dispatches `lookout <group> <sub>` through the registry,
// following the §4.2 exit-code contract for the dispatch itself:
// help on stdout exits 0, misuse lists the group on stderr and
// exits 2.
func runGroup(ctx context.Context, reg *checks.Registry, group string, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, checks.GroupHelp(reg, group))
		return emit.ExitUsage
	}
	switch args[0] {
	case "-h", "--help", "help":
		fmt.Fprint(stdout, checks.GroupHelp(reg, group))
		return emit.ExitData
	}
	c, ok := reg.Lookup(group + " " + args[0])
	if !ok {
		fmt.Fprintf(stderr, "lookout %s: unknown command %q\n\n", group, args[0])
		fmt.Fprint(stderr, checks.GroupHelp(reg, group))
		return emit.ExitUsage
	}
	cfg := c.RunConfig(stdout, stderr)
	return emit.Run(ctx, cfg, args[1:])
}
