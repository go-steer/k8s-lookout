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

// The multi-cluster surface (#700): the cluster argument every tool
// gains, and k8s_list_clusters, which names what it can be.
//
// The argument is a plain string checked on the server, not an enum: an
// enum would repeat the whole cluster list in every tool's schema, paid
// on every model call, growing with the fleet (design, Part 1). It is
// required, because the set is discovered (and, in Google-token mode,
// per caller), so the schema cannot know at startup whether there will
// be one cluster or fifty.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/go-steer/purser"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-steer/k8s-lookout/internal/fleet"
	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

const (
	clusterProperty  = "cluster"
	listClustersName = "k8s_list_clusters"
	filterProperty   = "filter"
)

// toolSchema is a command's input schema, plus the cluster argument on
// a multi-cluster endpoint. In that mode the server, not the caller,
// decides two things the command would otherwise take as arguments:
// which store file a call reads (store_cluster), and the cluster label
// findings diff binds rows to (its own cluster flag), which the server
// sets to the canonical name.
func toolSchema(c checks.Command, multi bool) *jsonschema.Schema {
	s := inputSchema(c)
	if !multi {
		return s
	}
	delete(s.Properties, "store_cluster")
	delete(s.Properties, clusterProperty)
	s.Properties[clusterProperty] = &jsonschema.Schema{
		Type: "string",
		Description: "the cluster to read, as k8s_list_clusters names it: its name, " +
			"name@project when names repeat across projects, or project/location/name. " +
			"Leave it out when the user did not name one: the server picks their default " +
			"or only readable cluster, or answers with the clusters to choose from. Never invent a name",
	}
	return s
}

// takeCluster removes the cluster argument from a call's arguments,
// returning the rest for argv and the name ("" when absent).
func takeCluster(raw json.RawMessage) (json.RawMessage, string, error) {
	if len(raw) == 0 {
		return raw, "", nil
	}
	var args map[string]json.RawMessage
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, "", fmt.Errorf("arguments must be a JSON object: %v", err)
	}
	val, ok := args[clusterProperty]
	if !ok {
		return raw, "", nil
	}
	name, err := decodeString(clusterProperty, val)
	if err != nil {
		return nil, "", err
	}
	delete(args, clusterProperty)
	rest, err := json.Marshal(args)
	if err != nil {
		return nil, "", err
	}
	return rest, strings.TrimSpace(name), nil
}

// withClusterAccepted adds cluster to the accepted-arguments list of an
// unknown-argument error: argv never saw it (takeCluster took it out
// first), and a list without it would tell a model the argument is not
// wanted.
func withClusterAccepted(msg string) string {
	const accepts = "(accepts: "
	if i := strings.Index(msg, accepts); i >= 0 {
		return msg[:i] + accepts + clusterProperty + ", " + msg[i+len(accepts):]
	}
	return msg
}

func commandHasFlag(c checks.Command, name string) bool {
	return slices.ContainsFunc(c.Flags, func(f emit.FlagSpec) bool { return f.Name == name })
}

func listClustersTool() *mcp.Tool {
	return &mcp.Tool{
		Name: listClustersName,
		Description: "List the clusters this endpoint serves, and the name to pass as the cluster argument of " +
			"every other tool. Call it when the user asks which clusters exist or names one you cannot match. " +
			"One line per cluster: its name, project, location, canonical name, access (read: the user can " +
			"read it cluster-wide; limited: not cluster-wide, though they may read some namespaces, so pass " +
			"the namespace they work in and expect cluster-wide checks to be refused; unreachable: lookout " +
			"could not reach it), and whether its control plane has a DNS endpoint. Clusters readable " +
			"cluster-wide come first. Then one line per discovery scope.",
		InputSchema: &jsonschema.Schema{
			Type: "object",
			Properties: map[string]*jsonschema.Schema{
				filterProperty: {
					Type:        "string",
					Description: "only clusters whose name, project or location contains this text (case-insensitive)",
				},
			},
		},
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}
}

// listClustersCommand stands in for a registered command where the
// caller and access-log plumbing wants one.
var listClustersCommand = checks.Command{Name: "list clusters", MCPName: listClustersName}

func listClustersHandler(cfg config, sem chan struct{}) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		start := cfg.log.now()
		remote := remoteAddrFromContext(ctx)
		caller := ""
		record := func(exit int, text string) {
			cfg.log.RecordFor(caller, "", listClustersName, exit, start, len(text), remote)
		}
		toolError := func(msg string) *mcp.CallToolResult {
			record(emit.ExitRuntime, msg)
			return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: emit.MaskString(msg)}}}
		}
		var workload *purser.Caller
		if wc, ok := workloadCallerOf(req); ok {
			caller = wc.Identity
			if cfg.workloads == nil || !cfg.workloads.matchesAny(wc) {
				return toolError("lookout list clusters: " + caller + " is not granted any cluster by the operator's --workload-rules"), nil
			}
			release, ok := cfg.perCaller.acquire(caller)
			if !ok {
				return toolError(fmt.Sprintf("lookout list clusters: %s already has %d calls in flight; retry when one finishes", caller, cfg.perCaller.max)), nil
			}
			defer release()
			workload = &wc
		} else if cfg.google != nil {
			id, ts, err := cfg.google.caller(req, listClustersCommand)
			caller = id.Email
			if err != nil {
				return toolError("lookout list clusters: " + err.Error()), nil
			}
			release, ok := cfg.perCaller.acquire(caller)
			if !ok {
				return toolError(fmt.Sprintf("lookout list clusters: %s already has %d calls in flight; retry when one finishes", caller, cfg.perCaller.max)), nil
			}
			defer release()
			// Discovery runs as the caller: they see what they can read.
			ctx = kube.WithCallerToken(ctx, ts)
		}
		filter, err := listClustersFilter(req.Params.Arguments)
		if err != nil {
			record(emit.ExitUsage, err.Error())
			return nil, invalidParams(err.Error())
		}
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		case <-ctx.Done():
			return toolError("lookout list clusters: " + ctx.Err().Error()), nil
		}
		entries, scopes, err := cfg.clusters.List(ctx, caller, filter)
		if err != nil {
			return toolError("lookout list clusters: " + err.Error()), nil
		}
		// A workload sees only the clusters its rules let it call.
		allowed := func(es []clusterEntry) []clusterEntry {
			if workload == nil {
				return es
			}
			var out []clusterEntry
			for _, e := range es {
				if cfg.workloads.listsCluster(*workload, e) {
					out = append(out, e)
				}
			}
			return out
		}
		entries = allowed(entries)
		total := len(entries)
		if filter != "" {
			all, _, err := cfg.clusters.List(ctx, caller, "")
			if err == nil {
				total = len(allowed(all))
			}
		}
		// The same --max-response-bytes cut as every command's result
		// (#755): a large organization's list, or a broad filter, can
		// exceed what a client accepts.
		out, dropped := fitClusterList(renderClusters(entries, scopes, total, cfg.log.now().Sub(start)), filter != "", cfg.maxResponseBytes)
		cfg.log.record(callRecord{caller: caller, tool: listClustersName, exit: emit.ExitData,
			start: start, bytes: len(out), remote: remote, truncated: dropped})
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: out}}}, nil
	}
}

func listClustersFilter(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var args map[string]json.RawMessage
	if err := json.Unmarshal(raw, &args); err != nil {
		return "", fmt.Errorf("arguments must be a JSON object: %v", err)
	}
	for name := range args {
		if name != filterProperty {
			return "", fmt.Errorf("unknown argument %q for tool %s (accepts: %s)", name, listClustersName, filterProperty)
		}
	}
	val, ok := args[filterProperty]
	if !ok {
		return "", nil
	}
	return decodeString(filterProperty, val)
}

// renderClusters is the §4.2 shape: payload lines, then the summary
// line. It is an inventory, not a judgment, so it carries no findings.
func renderClusters(entries []clusterEntry, scopes []fleet.ScopeResult, total int, elapsed time.Duration) string {
	var b bytes.Buffer
	for _, e := range entries {
		fields := []emit.Field{
			{Key: "cluster", Value: e.name},
			{Key: "project", Value: e.ref.Project},
			{Key: "location", Value: e.ref.Location},
			{Key: "canonical", Value: e.canonical},
		}
		if e.access != "" {
			fields = append(fields, emit.Field{Key: "access", Value: e.access})
		}
		b.Write(emit.EncodeLogfmt(append(fields, emit.Field{Key: "dns_endpoint", Value: fmt.Sprint(e.ref.Endpoint != "")})))
	}
	for _, s := range scopes {
		fields := []emit.Field{
			{Key: "scope", Value: s.Scope},
			{Key: "via", Value: s.Via},
			{Key: "clusters", Value: fmt.Sprint(s.Clusters)},
		}
		if s.Err != nil {
			fields = append(fields, emit.Field{Key: "error", Value: s.Err.Error()})
		}
		if s.Fallback != "" {
			fields = append(fields, emit.Field{Key: "fallback", Value: s.Fallback})
		}
		if len(s.Skipped) > 0 {
			var parts []string
			for _, reason := range slices.Sorted(maps.Keys(s.Skipped)) {
				parts = append(parts, fmt.Sprintf("%d %s", s.Skipped[reason], reason))
			}
			fields = append(fields, emit.Field{Key: "skipped", Value: strings.Join(parts, "; ")})
		}
		b.Write(emit.EncodeLogfmt(fields))
	}
	b.Write(emit.EncodeLogfmt([]emit.Field{
		{Key: "scanned", Value: fmt.Sprint(total)},
		{Key: "findings", Value: "0"},
		{Key: "elapsed", Value: elapsed.Truncate(time.Millisecond).String()},
		{Key: "clusters", Value: fmt.Sprint(len(entries))},
	}))
	return emit.MaskString(b.String())
}
