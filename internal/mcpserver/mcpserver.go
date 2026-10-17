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
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

const defaultMaxConcurrentCalls = 8

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
	// Operator-pinned host file paths and cluster selection applied to
	// every tool invocation instead of exposed in MCP input schemas.
	kubeconfig     string
	context        string
	exemptions     string
	store          string
	report         string
	maxConcurrency int
	// google, when non-nil, is Google-token mode: every call runs as its
	// caller, and perCaller bounds each caller's calls in flight.
	google    *GoogleAuth
	perCaller *callerLimit
	// workloads, when non-nil, admits workload callers from the mTLS
	// listener (#701), each limited to what the --workload-rules grant.
	workloads *WorkloadAuth
	// clusters, when non-nil, is multi-cluster mode (#700): every tool
	// takes a cluster argument, resolved here per call.
	clusters *Clusters
	// localCluster is the canonical name of the cluster lookout runs in,
	// the one cluster a Local command may target; "" when unknown.
	localCluster string
	// maxPerCluster caps one cluster's calls in flight; 0 derives it from
	// the global limit and the size of the cluster set.
	maxPerCluster int
	perCluster    *callerLimit
}

// WithLocalCluster names the cluster lookout runs in (project/location/
// name), the only one a Local command such as net probe may target on a
// multi-cluster endpoint. Without it, Local commands are refused there.
func WithLocalCluster(canonical string) Option {
	return func(c *config) { c.localCluster = canonical }
}

// WithMaxConcurrencyPerCluster caps each cluster's calls in flight on a
// multi-cluster endpoint, so one slow or wedged API server cannot hold
// every global slot. Zero (the default) is the global limit divided by
// the number of clusters, at least 2.
func WithMaxConcurrencyPerCluster(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxPerCluster = n
		}
	}
}

// clusterLimit is the per-cluster cap for a set of n clusters.
func (c config) clusterLimit(n int) int {
	if c.maxPerCluster > 0 {
		return c.maxPerCluster
	}
	return max(2, c.maxConcurrency/max(1, n))
}

// WithClusters serves several clusters: every tool gains a required
// cluster argument, and k8s_list_clusters names the clusters a caller
// may pass (#700). The server pins no kubeconfig or context in this
// mode; each call reaches its cluster through the directory.
func WithClusters(c *Clusters) Option {
	return func(cfg *config) { cfg.clusters = c }
}

// WithGoogleAuth runs every tool call as its caller, with the caller's own
// Google token (#699). Serve it with ServeOptions.Google set to the same g,
// which puts the token on each request.
func WithGoogleAuth(g *GoogleAuth) Option {
	return func(c *config) { c.google = g }
}

// WithWorkloadAuth applies w's rules to workload callers, who arrive on
// the mTLS listener (#701). Serve it with ServeOptions.Workloads set to
// the same w.
func WithWorkloadAuth(w *WorkloadAuth) Option {
	return func(c *config) { c.workloads = w }
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

// WithSelection pins --kubeconfig and --context for all MCP tool calls.
// Empty values leave the corresponding flag unset.
func WithSelection(kubeconfig, contextName string) Option {
	return func(c *config) {
		c.kubeconfig = kubeconfig
		c.context = contextName
	}
}

// WithExemptions pins --exemptions=<path> for all MCP tool calls.
func WithExemptions(path string) Option {
	return func(c *config) { c.exemptions = path }
}

// WithStore pins --store=<path> for MCP tool calls that accept a store.
func WithStore(path string) Option {
	return func(c *config) { c.store = path }
}

// WithReport pins --report=<path> for MCP tool calls that accept a report.
func WithReport(path string) Option {
	return func(c *config) { c.report = path }
}

// WithMaxConcurrency bounds concurrent MCP tool executions (default 8).
func WithMaxConcurrency(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxConcurrency = n
		}
	}
}

// New builds the MCP server for a registry: one tool per non-hidden
// registered command, picked up automatically from the registry state
// at construction time. WithTools narrows that to a profile or an
// explicit list (issue #280).
func New(reg *checks.Registry, version string, opts ...Option) *mcp.Server {
	cfg := config{maxConcurrency: defaultMaxConcurrentCalls}
	for _, o := range opts {
		o(&cfg)
	}
	sem := make(chan struct{}, cfg.maxConcurrency)
	if cfg.google != nil || cfg.workloads != nil {
		cfg.perCaller = newCallerLimit(defaultMaxConcurrentPerCaller)
	}
	if cfg.clusters != nil {
		cfg.perCluster = newCallerLimit(cfg.maxConcurrency)
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "lookout", Version: version}, nil)
	multi := cfg.clusters != nil
	if multi {
		server.AddTool(listClustersTool(), listClustersHandler(cfg, sem))
	}
	for _, c := range Advertised(reg, cfg.tools) {
		server.AddTool(&mcp.Tool{
			Name:        c.MCPName,
			Description: toolDescription(c),
			InputSchema: toolSchema(c, multi),
			// Most of this surface is the read path (§4.3) and stays
			// ReadOnlyHint:true. A command that mutates state (c.Writes
			// — the §9.4 triage-status upsert) is advertised
			// non-read-only so a convention-following client does not
			// auto-approve it as a read (issue #105).
			Annotations: &mcp.ToolAnnotations{ReadOnlyHint: !c.Writes},
		}, handler(c, cfg, sem))
	}
	if cfg.google != nil {
		// Serve checks this before putting the Google HTTP layer in front
		// of a server: only this one reads each caller's token.
		cfg.google.server = server
	}
	if cfg.workloads != nil {
		// Likewise for the mTLS listener: only this server applies the
		// workload rules.
		cfg.workloads.server = server
	}
	return server
}

// Advertised returns the commands a server built with this selection
// serves, in registry order. It is exported because the tool list is
// worth inspecting without standing a server up — `lookout mcp
// --list-tools` prints it, and the profile tests assert on it.
//
// A Privileged command (`net probe-from`, DESIGN §5 amendment of
// 2026-10-07) is served only when the selection names it explicitly,
// which only EnablePrivileged does: the nil "everything" selection
// leaves it out, and ResolveTools never puts it in.
func Advertised(reg *checks.Registry, tools map[string]bool) []checks.Command {
	var out []checks.Command
	for _, c := range reg.All() {
		if c.Hidden {
			continue
		}
		if c.Privileged && !tools[c.MCPName] {
			continue
		}
		if tools != nil && !tools[c.MCPName] {
			continue
		}
		out = append(out, c)
	}
	return out
}

// EnablePrivileged adds one Privileged tool to a selection. It is
// called only for an operator flag on `lookout mcp` (e.g.
// --probe-from-image); no client-supplied selection reaches it. A nil
// selection — the full default surface — is first expanded to that
// surface, so enabling the tool adds it rather than replacing
// everything else.
func EnablePrivileged(reg *checks.Registry, tools map[string]bool, mcpName string) (map[string]bool, error) {
	var found bool
	for _, c := range reg.All() {
		if c.MCPName == mcpName && c.Privileged && !c.Hidden {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("%q is not a registered privileged tool", mcpName)
	}
	out := map[string]bool{}
	if tools == nil {
		for _, c := range Advertised(reg, nil) {
			out[c.MCPName] = true
		}
	} else {
		for name := range tools {
			out[name] = true
		}
	}
	out[mcpName] = true
	return out, nil
}

// errReader rejects any read attempt immediately so no MCP tool call
// (such as k8s_findings_diff where --report defaults to "-") can ever
// read os.Stdin and block or corrupt the stdio JSON-RPC stream.
type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, errors.New("stdin is not available on the MCP transport; pass explicit arguments instead")
}

func commandAcceptsStore(c checks.Command) bool {
	if c.GraphBacked {
		return true
	}
	for _, f := range c.Flags {
		if f.Name == "store" {
			return true
		}
	}
	return false
}

func commandAcceptsReport(c checks.Command) bool {
	for _, f := range c.Flags {
		if f.Name == "report" {
			return true
		}
	}
	return false
}

func pinnedArgs(c checks.Command, cfg config) []string {
	var out []string
	if cfg.kubeconfig != "" {
		out = append(out, "--kubeconfig="+cfg.kubeconfig)
	}
	if cfg.context != "" {
		out = append(out, "--context="+cfg.context)
	}
	if cfg.exemptions != "" {
		out = append(out, "--exemptions="+cfg.exemptions)
	}
	// On a multi-cluster endpoint --store and --report are stems, and the
	// handler pins each call's cluster's own file instead.
	if cfg.clusters != nil {
		return out
	}
	if cfg.store != "" && commandAcceptsStore(c) {
		out = append(out, "--store="+cfg.store)
	}
	if cfg.report != "" && commandAcceptsReport(c) {
		out = append(out, "--report="+cfg.report)
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
func handler(c checks.Command, cfg config, sem chan struct{}) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		start := cfg.log.now()
		remote := remoteAddrFromContext(ctx)
		caller, cluster := "", ""
		record := func(exit int, text string) {
			cfg.log.RecordFor(caller, cluster, c.MCPName, exit, start, len(text), remote)
		}
		toolError := func(msg string) *mcp.CallToolResult {
			record(emit.ExitRuntime, msg)
			return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: msg}}}
		}
		// A workload caller (#701) arrived on the mTLS listener with a
		// verified client certificate; the rules say what it may call.
		var wl *workloadCall
		if wc, ok := workloadCallerOf(req); ok {
			caller = wc.Identity
			if cfg.workloads == nil {
				// Unreachable through Serve, which refuses the mismatch;
				// never run such a call as lookout without its rules.
				return toolError("lookout " + c.Name + ": internal: a workload caller reached a server built without --workload-rules"), nil
			}
			call, err := cfg.workloads.allowTool(wc, c)
			if err != nil {
				return toolError("lookout " + c.Name + ": " + err.Error()), nil
			}
			wl = call
			release, ok := cfg.perCaller.acquire(caller)
			if !ok {
				return toolError(fmt.Sprintf("lookout %s: %s already has %d calls in flight; retry when one finishes",
					c.Name, caller, cfg.perCaller.max)), nil
			}
			defer release()
			// Records the call writes (triage status recorded_by, acks)
			// name the workload.
			ctx = kube.WithCallerName(ctx, caller)
		} else if cfg.google != nil {
			// Read the caller from the request, not the context: the
			// token is the one this HTTP request carried
			// (mcpauth.TokenSource).
			id, ts, err := cfg.google.caller(req, c)
			caller = id.Email
			if err != nil {
				return toolError("lookout " + c.Name + ": " + err.Error()), nil
			}
			release, ok := cfg.perCaller.acquire(caller)
			if !ok {
				return toolError(fmt.Sprintf("lookout %s: %s already has %d calls in flight; retry when one finishes",
					c.Name, caller, cfg.perCaller.max)), nil
			}
			defer release()
			ctx = kube.WithCallerName(kube.WithCallerToken(ctx, ts), caller)
		}
		rawArgs := req.Params.Arguments
		var clusterArgs []string
		endpoint := ""
		if cfg.clusters != nil {
			rest, name, err := takeCluster(rawArgs)
			if err != nil {
				record(emit.ExitUsage, err.Error())
				return nil, invalidParams(err.Error())
			}
			entry, err := cfg.clusters.Choose(ctx, caller, name)
			var choice *choiceError
			if errors.As(err, &choice) {
				// Ask the person when the client can be asked (a session
				// that advertised elicitation), and read their answer on
				// the retry; otherwise the error names the candidates, so
				// the model can choose rather than guess.
				if picked, answered := clusterAnswer(req); answered {
					if picked != "" {
						entry, err = cfg.clusters.Choose(ctx, caller, picked)
					}
				} else if len(choice.candidates) > 0 && clientCanBeAsked(req.Session) {
					return askForCluster(c.Name, choice), nil
				}
			}
			if errors.As(err, &choice) {
				record(emit.ExitUsage, choice.msg)
				return nil, invalidParams(choice.msg)
			}
			if err != nil {
				return toolError("lookout " + c.Name + ": " + err.Error()), nil
			}
			cluster = entry.canonical
			if wl != nil {
				if err := cfg.workloads.allowCluster(wl, entry); err != nil {
					return toolError("lookout " + c.Name + ": " + err.Error()), nil
				}
				endpoint = entry.ref.Endpoint
			}
			if c.Local && entry.canonical != cfg.localCluster {
				where := "a cluster this server cannot identify"
				if cfg.localCluster != "" {
					where = cfg.localCluster
				}
				return toolError(fmt.Sprintf("lookout %s measures from where lookout runs, which is %s, not %s: its answer would describe the wrong network. lookout net probe-from probes from a pod inside the cluster", c.Name, where, entry.canonical)), nil
			}
			releaseCluster, ok := cfg.perCluster.acquireUpTo(entry.canonical, cfg.clusterLimit(entry.setSize))
			if !ok {
				return toolError(fmt.Sprintf("lookout %s: cluster %s already has %d calls in flight on this endpoint; retry when one finishes",
					c.Name, entry.name, cfg.clusterLimit(entry.setSize))), nil
			}
			defer releaseCluster()
			// Each cluster's store and report are its own files, named the
			// way a multi-cluster watch names them; a cluster whose name
			// repeats in the set has none.
			if entry.storeName != "" {
				if cfg.store != "" && commandAcceptsStore(c) {
					clusterArgs = append(clusterArgs, "--store="+emit.PerClusterPath(cfg.store, entry.storeName))
				}
				if cfg.report != "" && commandAcceptsReport(c) {
					clusterArgs = append(clusterArgs, "--report="+emit.PerClusterPath(cfg.report, entry.storeName))
				}
			}
			restCfg, err := cfg.clusters.Config(ctx, entry)
			if err != nil {
				return toolError(fmt.Sprintf("lookout %s: cluster %s: %v", c.Name, entry.name, err)), nil
			}
			ctx = kube.WithTarget(ctx, kube.Target{
				Name:     entry.ref.Name,
				Project:  entry.ref.Project,
				Location: entry.ref.Location,
				Config:   restCfg,
			})
			// A command that labels what it stores by cluster (findings
			// diff) is given the canonical name, which does not change
			// with who is asking.
			if commandHasFlag(c, "cluster") {
				clusterArgs = append(clusterArgs, "--cluster="+entry.canonical)
			}
			rawArgs = rest
		}
		if wl != nil {
			// After the cluster is chosen: discovery and the cluster's
			// cached config are lookout's, and only the reads this call
			// makes run as the rule's service account, if it names one.
			var err error
			if ctx, err = cfg.workloads.readAs(ctx, wl, endpoint); err != nil {
				return toolError("lookout " + c.Name + ": " + err.Error()), nil
			}
		}
		args, err := argv(c, rawArgs)
		if err != nil {
			msg := err.Error()
			if cfg.clusters != nil {
				msg = withClusterAccepted(msg)
			}
			record(emit.ExitUsage, msg)
			return nil, invalidParams(msg)
		}
		if (cfg.google != nil || cfg.clusters != nil) && slices.ContainsFunc(args, func(a string) bool { return strings.HasPrefix(a, "--store-cluster") }) {
			// Which cluster's store a call reads is the operator's choice:
			// the caller access check asks the cluster this server reads,
			// so it cannot vouch for rows from another cluster's store
			// (namespace names repeat across clusters). On a multi-cluster
			// endpoint the cluster argument already says which.
			msg := "store_cluster is not accepted here: the store a call reads is fixed by the server's operator"
			record(emit.ExitUsage, msg)
			return nil, invalidParams(msg)
		}
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		case <-ctx.Done():
			msg := "lookout " + c.Name + ": " + ctx.Err().Error()
			record(emit.ExitRuntime, msg)
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: msg}},
			}, nil
		}
		// Server-supplied flags go before the caller's: a positional
		// target ends the caller's argv, and flags after it would not
		// parse.
		fullArgs := append(append(pinnedArgs(c, cfg), clusterArgs...), args...)
		var stdout, stderr bytes.Buffer
		rc := c.RunConfig(&stdout, &stderr)
		rc.Stdin = errReader{}
		switch code := emit.Run(ctx, rc, fullArgs); code {
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

// diagnostics returns the captured stderr, trimmed and sanitized; if a
// failure preceded any diagnostic output, it still names the command
// rather than returning an empty error.
func diagnostics(c checks.Command, stderr *bytes.Buffer) string {
	if s := strings.TrimSpace(stderr.String()); s != "" {
		return emit.MaskString(s)
	}
	return "lookout " + c.Name + ": failed without diagnostics"
}
